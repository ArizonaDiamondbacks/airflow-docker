from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from pathlib import Path
import time

# Define paths
JSON_PATH = "/opt/airflow/json_files"
OUTPUT_FILE = "/opt/airflow/logs/file_paths.json"

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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0  # Disable retries for testing
}

with DAG('json_files_metadata',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    list_json_files = PythonOperator(
        task_id='get_json_files_metadata',
        python_callable=list_json_paths
    )