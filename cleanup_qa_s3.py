
import boto3
import sys
import os

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config.settings import AppConfig

def cleanup_s3():
    try:
        config = AppConfig()
        s3 = boto3.resource(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )
        
        bucket_name = 'redshift-dw-qa-uniuni-com'
        prefix = 'order_tracking_local_test/incremental/'
        
        print(f"Cleaning up s3://{bucket_name}/{prefix} ...")
        
        bucket = s3.Bucket(bucket_name)
        # Delete all objects with the prefix
        bucket.objects.filter(Prefix=prefix).delete()
        
        print("Cleanup complete.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    cleanup_s3()
