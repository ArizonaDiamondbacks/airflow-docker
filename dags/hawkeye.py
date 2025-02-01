import os
from io import BytesIO
import subprocess
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
import boto3
import fastavro
from fastavro import reader, writer

JSON_PATH = "/opt/airflow/json_files".strip()
BUCKET_NAME = "azd-hawkeyeplayertracking-databricks-east".strip()
MANIFEST_FILE_KEY = "test/manifest.avro".strip()
S3_PATH_URL = "s3://azd-hawkeyeplayertracking-databricks-east/test".strip()

@dag(
    dag_id='hawkeye',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def hawkeye():

    @task
    def list_files(**context):
        """
        List all JSON files in the source directory.
        Returns a list of file paths.
        """
        all_paths = []
        # Use low-level os.walk for maximum performance
        for root, _, files in os.walk(JSON_PATH):
            for filename in files:
                if filename.lower().endswith('.json'):
                    full_path = os.path.join(root, filename)
                    all_paths.append(full_path)

        return all_paths
    
    @task
    def filter_unprocessed(file_list: list):
        """
        (Optional) Reads the existing manifest in S3, 
        filters out already processed files.
        Returns only the unprocessed files.
        """
        s3_client = boto3.client('s3')

        # Read the existing manifest from S3
        try:
            # Get the object from S3
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=MANIFEST_FILE_KEY)
            
            # 2. Read directly without manual decompression
            avro_file = response['Body']
            avro_reader = reader(avro_file)

            # 3. Extract the processed files from the manifest
            processed_files = [record['file_name'] for record in avro_reader]

            # 4. Filter out the processed files
            unprocessed_files = [file for file in file_list if os.path.basename(file) not in processed_files]
            return unprocessed_files
        
        except s3_client.exceptions.NoSuchKey:
            # If the manifest file doesn't exist, all files are unprocessed
            return file_list
        
        except Exception as e:
            print(f"Error reading manifest file: {str(e)}")
            raise
    
    @task
    def process_and_upload_file(file_path: str):
        """
        Runs the Rust 'process' app to create 6 Parquet files,
        then runs the 'upload' app or uses AWS CLI/SDK to upload them,
        plus a log file, to S3. Returns a dict with details if success.
        Raises an exception if fails, causing the task to be marked failed.
        """
        try:
            # 1) Process step
            result = subprocess.run(
                ["/opt/airflow/bins/json_to_parquet", file_path, "/opt/airflow/output"],
                check=True,
                capture_output=True,   # capture stdout and stderr
                text=True              # decode bytes -> string
            )
            print("----- JSON to Parquet Output -----")
            print(result.stdout)
            print("----- JSON to Parquet Errors -----")
            print(result.stderr)

            # 2) Upload step
            result2 = subprocess.run(
                ["/opt/airflow/bins/upload_to_s3", file_path, "/opt/airflow/output", S3_PATH_URL],
                check=True,
                capture_output=True,
                text=True
            )
            print("----- Uploader STDOUT -----")
            print(result2.stdout)
            print("----- Uploader STDERR -----")
            print(result2.stderr)

            # Return the details if successful
            return {
                "file_name": os.path.basename(file_path),
                "status": "success",
                "timestamp": datetime.now(timezone.utc).isoformat()
                
            }
        except subprocess.CalledProcessError as e:
            # Log any captured output we have
            print(f"Command '{e.cmd}' failed with exit code {e.returncode}")
            if e.stdout:
                print("=== STDOUT ===")
                print(e.stdout)
            if e.stderr:
                print("=== STDERR ===")
                print(e.stderr)
            # This task will be marked as FAILED, no XCom returned
            raise RuntimeError(f"Error processing/uploading {file_path}") from e
        
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def update_manifest(processed_file_list: list):
        """
        Aggregator: runs once after *all* parallel tasks are done (success or fail).
        - Gathers the XCom from each successful subtask in processed_records_list
        - Reads the old manifest from S3
        - Appends the new records
        - Writes the updated manifest to S3
        """
        manifest_schema = {
            "name": "ManifestRecord",
            "type": "record",
            "fields": [
                {"name": "file_name",  "type": "string"},
                {"name": "status",     "type": "string"},
                {"name": "timestamp",  "type": "string"},
            ]
        }

        parsed_schema = fastavro.parse_schema(manifest_schema)

        if not processed_file_list:
            # processed_file_list is None or empty
            new_entries = []
        else:
            # processed_file_list is a non-empty list (though some may be None if tasks failed)
            new_entries = [r for r in processed_file_list if r is not None]

        if not new_entries:
            print("No new entries to update, either none were unprocessed or all failed.")
            return
        
        # 1) Read the existing manifest from S3
        s3_client = boto3.client('s3')
        try:
            # Get the object from S3
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=MANIFEST_FILE_KEY)
            
            avro_file = response['Body']
            avro_reader = reader(avro_file)
            
            old_records = [record for record in avro_reader]
            
        except s3_client.exceptions.NoSuchKey:
            # If the manifest file doesn't exist, start fresh
            old_records = []

        # 2) Append the new records
        updated_records  = old_records + new_entries

        # 3) Write updated records to an Avro file in memory
    #    We'll use a BytesIO buffer, then upload that buffer to S3
        buffer = BytesIO()
        fastavro.writer(buffer, parsed_schema, updated_records, codec='null')
        buffer.seek(0)  # Important: go back to start of file-like object

        # 4) Upload the Avro file to S3, overwriting the old manifest
        s3_client.upload_fileobj(buffer, BUCKET_NAME, MANIFEST_FILE_KEY)

        print(f"Manifest updated with {len(new_entries)} new records. Total size: {len(updated_records)} records.")
    
    # DAG orchestration
    all_files = list_files()
    unprocessed = filter_unprocessed(all_files)

    # Map the process-and-upload step over each unprocessed file
    processed_records = process_and_upload_file.expand(file_path=unprocessed)

    # Aggregator updates the manifest once all tasks are done
    update_manifest(processed_records)

hawkeye_dag = hawkeye()
        
