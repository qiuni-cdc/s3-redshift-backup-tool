#!/usr/bin/env python3
"""
Simple script to delete all S3 lock files

Usage:
    python delete_all_s3_locks.py
"""

import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

import boto3
import yaml

# Configuration
LOCK_PREFIX = 'watermarks/v2/locks/'

def delete_all_locks():
    """Delete all lock files from S3"""

    # Load S3 credentials from project config
    print("📋 Loading S3 credentials from config...")

    try:
        from src.core.configuration_manager import ConfigurationManager

        config_manager = ConfigurationManager()

        # Load pipeline config to get S3 settings
        pipeline_path = Path("config/pipelines/us_dw_unidw_2_settlement_dws_pipeline_direct.yml")
        with open(pipeline_path, 'r') as f:
            pipeline_config = yaml.safe_load(f)

        pipeline_info = pipeline_config.get('pipeline', {})
        source_name = pipeline_info.get('source')
        target_name = pipeline_info.get('target')
        s3_config_name = pipeline_info.get('s3_config')

        # Create app config
        config = config_manager.create_app_config(
            source_connection=source_name,
            target_connection=target_name,
            s3_config_name=s3_config_name
        )

        # Initialize S3 client with credentials
        s3 = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )

        bucket = config.s3.bucket_name

    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        sys.exit(1)

    print(f"🔍 Scanning for locks in s3://{bucket}/{LOCK_PREFIX}")

    # List all lock files
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=LOCK_PREFIX
    )

    locks = response.get('Contents', [])

    if not locks:
        print("✅ No locks found")
        return

    print(f"🔒 Found {len(locks)} lock file(s)\n")

    # Delete each lock
    deleted = 0
    for lock in locks:
        lock_key = lock['Key']
        filename = lock_key.replace(LOCK_PREFIX, '')

        try:
            s3.delete_object(Bucket=bucket, Key=lock_key)
            print(f"✅ Deleted: {filename}")
            deleted += 1
        except Exception as e:
            print(f"❌ Failed to delete {filename}: {e}")

    print(f"\n✅ Successfully deleted {deleted}/{len(locks)} lock file(s)")

if __name__ == '__main__':
    delete_all_locks()
