FROM apache/airflow:2.10.4-python3.9

USER airflow
RUN pip install --no-cache-dir \
    boto3 \
    s3fs 

USER airflow