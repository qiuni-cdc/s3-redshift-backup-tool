"""
Reset watermark for local Docker testing.
Deletes the watermark so the next DAG run triggers "first run" behavior
(15-minute lookback instead of incremental from last watermark).

Usage:
    docker exec -it airflow_poc-airflow-worker-1 python /opt/airflow/sync_tool/airflow_poc/reset_watermark_fact_tables.py
"""
import boto3
import os
from dotenv import load_dotenv

os.chdir('/opt/airflow/sync_tool')
load_dotenv()

s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID_QA'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY_QA'),
    region_name='us-west-2'
)

BUCKET = 'redshift-dw-qa-uniuni-com'

# Watermark keys for all 3 tables in the order_tracking pipeline
WATERMARK_KEYS = [
    'watermarks/v2/us_prod_ro_docker_local_kuaisong_ecs_order_info_redshift_default.json',
    'watermarks/v2/us_prod_ro_docker_local_kuaisong_uni_tracking_info_redshift_default.json',
    'watermarks/v2/us_prod_ro_docker_local_kuaisong_uni_tracking_spath_redshift_default.json',
]

print("Resetting watermarks for order_tracking_local_test pipeline...")
print(f"Bucket: {BUCKET}")
print("-" * 60)

for key in WATERMARK_KEYS:
    try:
        s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"  Deleted: {key}")
    except Exception as e:
        print(f"  Skip (not found): {key}")

print("-" * 60)
print("Done! Next DAG run will use 15-minute lookback (first run behavior).")
