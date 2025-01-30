from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from pathlib import Path
import time

import boto3

from fastavro import reader
import zstandard
import fastavro
import io

# Define paths
JSON_PATH = "/opt/airflow/json_files"
OUTPUT_FILE = "/opt/airflow/logs/file_paths.json"
OUTPUT_PATH = "/opt/airflow/output"

def list_json_paths(**context):
    """Ultra-efficient file listing with progress tracking"""
    start_time = time.time()
    all_paths = []
    file_count = 0
    
    # Use low-level os.walk for maximum performance
    for root, _, files in os.walk(JSON_PATH):
        for filename in files:
            if filename.lower().endswith('.json'):
                full_path = os.path.join(root, filename)
                all_paths.append(full_path)
                file_count += 1
                
                # Progress reporting every 1000 files
                if file_count % 1000 == 0:
                    print(f"Found {file_count} files...")
    
    # Write all paths in one operation
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(all_paths, f, indent=2)
    
    print(f"Found {file_count} files in {time.time() - start_time:.2f} seconds")
    return file_count

def read_s3(bucket_name, key, **context):
    """Read JSON file from S3"""
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        json_content = json.loads(response['Body'].read().decode('utf-8'))
        
        # Write the JSON content to output
        if not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)
            
        output_file = os.path.join(OUTPUT_PATH, f"s3_{key.replace('/', '_')}")
        with open(output_file, "w") as f:
            json.dump(json_content, f, indent=2)
            
        return json_content
    except Exception as e:
        print(f"Error reading S3 file: {str(e)}")
        raise

def download_from_s3(bucket_name, key, **context):
    """Download file from S3 to local output directory"""
    s3_client = boto3.client('s3')
    
    try:
        if not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)
            
        # Keep original filename from S3 key
        filename = os.path.basename(key)
        output_file = os.path.join(OUTPUT_PATH, filename)
        
        # Download file directly
        s3_client.download_file(bucket_name, key, output_file)
        print(f"Successfully downloaded {key} to {output_file}")
            
        return output_file
    except Exception as e:
        print(f"Error downloading S3 file: {str(e)}")
        raise

def read_zstd_avro_from_s3(bucket_name, key, **context):
    """Correct implementation using fastavro's built-in codec handling"""
    s3_client = boto3.client('s3')
    
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        
        # 2. Read directly without manual decompression
        avro_file = response['Body']
        avro_reader = reader(avro_file)
        
        print(f"Detected codec: {avro_reader.codec}")  # Should show 'zstandard'
        
        # Read records
        records = [record for record in avro_reader]
        
        # Save output (same as before)
        output_file = os.path.join(OUTPUT_PATH, f"{os.path.basename(key)}.json")
        with open(output_file, 'w') as f:
            json.dump(records, f, indent=2)
            
        return records
        
    except Exception as e:
        print(f"Error processing Avro file: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0  # Disable retries for testing
}

with DAG('read_s3',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    list_json_files = PythonOperator(
        task_id='read_zstd_avro',
        python_callable=read_zstd_avro_from_s3,
        op_kwargs={
            'bucket_name': 'azd-trackmanplayertracking-databricks-east',
            'key': 'test/log/log_751801-2fe57c6f-9f1d-4606-a1c7-ea4b6137bafa.avro'
        }
    )