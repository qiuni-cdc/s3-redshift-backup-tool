FROM apache/airflow:2.8.1

# Pin exact versions to match QA
RUN pip install --no-cache-dir \
    mysql-connector-python==8.0.29 \
    boto3==1.33.13
