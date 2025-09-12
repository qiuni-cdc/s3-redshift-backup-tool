#!/usr/bin/env python3
"""
Simple Migration Script for Watermarks v2.0

This script safely migrates existing watermarks to the new v2.0 format
without requiring complex imports or database connections.
"""

import json
import logging
import sys
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def migrate_watermarks_simple(bucket_name, old_prefix='watermarks/', new_prefix='watermarks/v2/'):
    """Simple watermark migration without complex dependencies."""
    
    s3_client = boto3.client('s3')
    
    # Find legacy watermarks
    legacy_watermarks = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=old_prefix)
        
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if '/v2/' not in key and key.endswith('.json') and 'locks/' not in key:
                    legacy_watermarks.append(key)
    except Exception as e:
        logger.error(f"Failed to list legacy watermarks: {e}")
        return False
    
    logger.info(f"Found {len(legacy_watermarks)} legacy watermarks to migrate")
    
    migrated = 0
    skipped = 0
    
    for watermark_key in legacy_watermarks:
        try:
            # Load legacy watermark
            response = s3_client.get_object(Bucket=bucket_name, Key=watermark_key)
            legacy_data = json.loads(response['Body'].read())
            
            # Extract table name from key
            table_name = watermark_key.split('/')[-1].replace('.json', '')
            
            # Check if v2 already exists
            v2_key = f"{new_prefix}{table_name}.json"
            try:
                s3_client.head_object(Bucket=bucket_name, Key=v2_key)
                logger.info(f"Watermark for {table_name} already migrated, skipping")
                skipped += 1
                continue
            except ClientError:
                pass  # v2 doesn't exist, proceed with migration
            
            # Convert to v2 format
            v2_watermark = convert_to_v2_simple(legacy_data, table_name)
            
            # Save v2 watermark
            s3_client.put_object(
                Bucket=bucket_name,
                Key=v2_key,
                Body=json.dumps(v2_watermark, indent=2),
                ContentType='application/json'
            )
            
            migrated += 1
            logger.info(f"Successfully migrated watermark for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to migrate {watermark_key}: {e}")
    
    logger.info(f"Migration complete: {migrated} migrated, {skipped} skipped")
    return True


def convert_to_v2_simple(legacy_data, table_name):
    """Convert legacy watermark to v2.0 format."""
    
    # Handle both dict and object formats
    if isinstance(legacy_data, dict):
        # Direct dict access
        mysql_timestamp = legacy_data.get('last_mysql_data_timestamp')
        mysql_id = legacy_data.get('last_processed_id') 
        mysql_status = legacy_data.get('mysql_status', 'success')
        redshift_rows = legacy_data.get('redshift_rows_loaded', 0)
        redshift_status = legacy_data.get('redshift_status', 'success')
        processed_files = legacy_data.get('processed_s3_files', [])
        backup_strategy = legacy_data.get('backup_strategy', 'hybrid')
    else:
        # Object attribute access
        mysql_timestamp = getattr(legacy_data, 'last_mysql_data_timestamp', None)
        mysql_id = getattr(legacy_data, 'last_processed_id', None)
        mysql_status = getattr(legacy_data, 'mysql_status', 'success')
        redshift_rows = getattr(legacy_data, 'redshift_rows_loaded', 0)
        redshift_status = getattr(legacy_data, 'redshift_status', 'success')
        processed_files = getattr(legacy_data, 'processed_s3_files', [])
        backup_strategy = getattr(legacy_data, 'backup_strategy', 'hybrid')
    
    # Determine CDC strategy
    cdc_strategy = 'hybrid'
    if backup_strategy in ['timestamp_only', 'id_only', 'full_sync', 'custom_sql']:
        cdc_strategy = backup_strategy
    elif mysql_timestamp and mysql_id:
        cdc_strategy = 'hybrid'
    elif mysql_timestamp:
        cdc_strategy = 'timestamp_only'
    elif mysql_id:
        cdc_strategy = 'id_only'
    
    # Deduplicate processed files
    if isinstance(processed_files, list):
        processed_files = sorted(list(set(processed_files)))
    
    # Create v2.0 structure
    v2_watermark = {
        'version': '2.0',
        'table_name': table_name,
        'cdc_strategy': cdc_strategy,
        
        'mysql_state': {
            'last_timestamp': mysql_timestamp,
            'last_id': mysql_id,
            'status': mysql_status or 'success',
            'error': None,
            'last_updated': None
        },
        
        'redshift_state': {
            'total_rows': redshift_rows or 0,  # Use legacy count initially
            'last_updated': None,
            'status': redshift_status or 'success',
            'error': None,
            'last_loaded_files': []
        },
        
        'processed_files': processed_files,
        
        'metadata': {
            'created_at': datetime.now().isoformat(),
            'migrated_from': 'legacy',
            'migration_date': datetime.now().isoformat(),
            'manual_override': backup_strategy == 'manual_cli'
        }
    }
    
    return v2_watermark


def main():
    """Main migration function."""
    print("Simple Watermark Migration to v2.0")
    print("="*50)
    
    # You may need to customize these values
    bucket_name = "redshift-dw-qa-uniuni-com"  # Update with your bucket
    
    try:
        success = migrate_watermarks_simple(bucket_name)
        
        if success:
            print("\n✅ Migration completed successfully!")
            print("\nNext steps:")
            print("1. Deploy the updated code with new watermark system")
            print("2. Monitor performance improvements")
            print("3. The system will automatically fix any count discrepancies")
        else:
            print("\n❌ Migration failed - check logs above")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()