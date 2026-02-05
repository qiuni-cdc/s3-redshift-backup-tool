import boto3
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

os.chdir('/opt/airflow/sync_tool')
load_dotenv()

s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID_QA'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY_QA'),
    region_name='us-west-2'
)

# Set watermark to 1 hour ago - sync will process 15 mins of data from this point
watermark_unix = int((datetime.now() - timedelta(hours=1)).timestamp())
watermark_data = {
    'timestamp': str(watermark_unix),
    'metadata': {'initial_setup': True, 'format': 'unix'}
}

s3.put_object(
    Bucket='redshift-dw-qa-uniuni-com',
    Key='watermark/last_run_timestamp_US_PROD_RO_DOCKER_LOCAL:kuaisong.ecs_order_info.json',
    Body=json.dumps(watermark_data)
)
print(f'Watermark set to Unix: {watermark_unix} ({datetime.fromtimestamp(watermark_unix)})')
