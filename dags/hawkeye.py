from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from pathlib import Path

# Define paths
JSON_PATH = "/opt/airflow/json_files"
OUTPUT_PATH = "/opt/airflow/logs/json_metadata"

def get_json_files_metadata(**context):
    """Get metadata of JSON files without reading their full content"""
    # Ensure output directory exists
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    
    metadata_list = []
    
    # Use Path for better path handling
    json_path = Path(JSON_PATH)
    
    # Walk through directory
    for file_path in json_path.rglob("*.json"):
        # Get file stats
        stats = file_path.stat()
        
        metadata = {
            "filename": file_path.name,
            "relative_path": str(file_path.relative_to(json_path)),
            "size_bytes": stats.st_size,
            "modified_time": datetime.fromtimestamp(stats.st_mtime).isoformat(),
            "created_time": datetime.fromtimestamp(stats.st_ctime).isoformat()
        }
        metadata_list.append(metadata)
    
    # Write metadata to output file
    output_file = Path(OUTPUT_PATH) / "json_files_metadata.json"
    with output_file.open('w') as f:
        json.dump(metadata_list, f, indent=2)
    
    return len(metadata_list)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1
}

with DAG('json_files_metadata',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    list_json_files = PythonOperator(
        task_id='get_json_files_metadata',
        python_callable=get_json_files_metadata
    )