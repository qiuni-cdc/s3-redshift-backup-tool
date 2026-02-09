
import boto3
import sys
import os

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Use Env vars directly to avoid Pydantic validation in partial environments
def cleanup_s3():
    try:
        # Load .env if present (for local testing)
        from dotenv import load_dotenv
        load_dotenv()

        access_key = os.environ.get('S3_ACCESS_KEY')
        secret_key = os.environ.get('S3_SECRET_KEY')
        region = os.environ.get('S3_REGION', 'us-west-2')
        
        if not access_key or not secret_key:
            print("Error: S3_ACCESS_KEY or S3_SECRET_KEY env vars not set")
            return

        s3 = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        
        bucket_name = 'redshift-dw-qa-uniuni-com'
        prefix = 'order_tracking_hybrid_dbt/incremental/'
        
        bucket = s3.Bucket(bucket_name)

        # Cleanup incremental data
        print(f"Cleaning up s3://{bucket_name}/{prefix} ...")
        bucket.objects.filter(Prefix=prefix).delete()

        # Cleanup watermarks
        watermark_prefix = 'order_tracking_hybrid_dbt/watermarks/'
        print(f"Cleaning up s3://{bucket_name}/{watermark_prefix} ...")
        bucket.objects.filter(Prefix=watermark_prefix).delete()
        
        print("Cleanup complete.")

        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    cleanup_s3()
