#!/usr/bin/env python3
"""
Check processed files in watermark vs actual S3 files
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
from src.core.s3_watermark_manager import S3WatermarkManager
from src.config.settings import AppConfig

def check_processed_files(table_name: str):
    """Check processed files in watermark vs S3"""
    
    print("=" * 80)
    print("Checking Processed Files in Watermark")
    print("=" * 80)
    
    try:
        # Initialize components
        config = AppConfig()
        watermark_manager = S3WatermarkManager(config)
        
        # Get watermark
        print(f"Getting watermark for: {table_name}")
        watermark = watermark_manager.get_table_watermark(table_name)
        
        if not watermark:
            print("❌ No watermark found")
            return
            
        # Show watermark status for debugging
        print(f"\n🔍 Watermark Status:")
        print(f"  MySQL Status: {getattr(watermark, 'mysql_status', 'None')}")
        print(f"  Redshift Status: {getattr(watermark, 'redshift_status', 'None')}")
        print(f"  MySQL Rows: {getattr(watermark, 'mysql_rows_extracted', 0)}")
        print(f"  Redshift Rows: {getattr(watermark, 'redshift_rows_loaded', 0)}")
        print(f"  Last Data Timestamp: {getattr(watermark, 'last_mysql_data_timestamp', 'None')}")
        print(f"  Last Extraction Time: {getattr(watermark, 'last_mysql_extraction_time', 'None')}")
        print(f"  Backup Strategy: {getattr(watermark, 'backup_strategy', 'None')}")
        
        # Handle incomplete watermarks (common after --redshift-only operations)
        if (not getattr(watermark, 'last_mysql_data_timestamp', None) and 
            not getattr(watermark, 'last_mysql_extraction_time', None)):
            print("⚠️  Incomplete watermark detected (missing MySQL timestamps)")
            print("   This is common after --redshift-only operations")
            print("   Continuing with processed files analysis...")
            
        # Check processed files
        processed_files = getattr(watermark, 'processed_s3_files', None) or []
        print(f"\n📋 Processed Files in Watermark: {len(processed_files)}")
        
        if processed_files:
            print("\nProcessed files list (first 10):")
            for i, file_path in enumerate(processed_files[:10], 1):
                print(f"  {i}. {file_path}")
            if len(processed_files) > 10:
                print(f"     ... and {len(processed_files) - 10} more files")
        else:
            print("  No processed files recorded in watermark")
        
        # Get actual S3 files
        print(f"\n📁 Checking actual S3 files...")
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )
        
        bucket = config.s3.bucket_name
        
        # Build search prefix - extract unscoped table name for S3 search
        unscoped_table_name = table_name.split(':')[-1] if ':' in table_name else table_name
        safe_table_name = unscoped_table_name.replace('.', '_').lower()
        prefix = f"incremental/"
        
        print(f"S3 Bucket: {bucket}")
        print(f"Search Prefix: {prefix}")
        print(f"Table Pattern: {safe_table_name}")
        
        # List S3 files
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        all_s3_files = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet') and safe_table_name in key:
                        s3_uri = f"s3://{bucket}/{key}"
                        all_s3_files.append({
                            'uri': s3_uri,
                            'key': key,
                            'size': obj['Size'],
                            'modified': obj['LastModified']
                        })
        
        print(f"\n📊 Actual S3 Files: {len(all_s3_files)}")
        
        if not all_s3_files:
            print("❌ No S3 files found for this table")
            return
            
        # Compare processed vs actual
        s3_uris = [f['uri'] for f in all_s3_files]
        processed_set = set(processed_files)
        s3_set = set(s3_uris)
        
        # Files in processed but not in S3 (should be rare)
        phantom_processed = processed_set - s3_set
        
        # Files in S3 but not in processed (these should be loaded)
        unprocessed_files = s3_set - processed_set
        
        # Files in both (correctly marked as processed)
        correctly_processed = processed_set & s3_set
        
        print(f"\n🔍 ANALYSIS:")
        print(f"  Total S3 files: {len(all_s3_files)}")
        print(f"  Files marked as processed: {len(processed_files)}")
        print(f"  Files correctly processed: {len(correctly_processed)}")
        print(f"  Files not yet processed: {len(unprocessed_files)}")
        print(f"  Phantom processed files: {len(phantom_processed)}")
        
        # Show unprocessed files (these should be loaded)
        if unprocessed_files:
            print(f"\n🔄 UNPROCESSED FILES ({len(unprocessed_files)} files):")
            unprocessed_list = sorted(list(unprocessed_files))
            for i, file_uri in enumerate(unprocessed_list[:20], 1):
                file_name = file_uri.split('/')[-1]
                print(f"  {i}. {file_name}")
            if len(unprocessed_list) > 20:
                print(f"     ... and {len(unprocessed_list) - 20} more files")
                
            print(f"\n💡 These {len(unprocessed_files)} files should be loaded in the next --redshift-only run")
            print(f"🔧 This explains the 371 vs {len(all_s3_files)} files discrepancy!")
            
            # Estimate missing rows
            if len(correctly_processed) > 0:
                avg_rows_per_file = 18550000 // len(correctly_processed)  # Based on loaded data
                estimated_missing_rows = len(unprocessed_files) * avg_rows_per_file
                print(f"📊 Estimated missing rows: {estimated_missing_rows:,} ({len(unprocessed_files)} files × ~{avg_rows_per_file:,} rows/file)")
        else:
            print(f"\n✅ All {len(all_s3_files)} S3 files are marked as processed")
            print("   No files should be loaded in the next --redshift-only run")
        
        # Show phantom files (shouldn't exist)
        if phantom_processed:
            print(f"\n👻 PHANTOM PROCESSED FILES ({len(phantom_processed)} files):")
            for i, file_uri in enumerate(list(phantom_processed)[:10], 1):
                print(f"  {i}. {file_uri}")
            print("   These files are marked as processed but don't exist in S3")
        
        # Show recent files for debugging
        print(f"\n📅 MOST RECENT S3 FILES (last 10):")
        sorted_files = sorted(all_s3_files, key=lambda x: x['modified'], reverse=True)
        for i, file_info in enumerate(sorted_files[:10], 1):
            file_name = file_info['key'].split('/')[-1]
            is_processed = file_info['uri'] in processed_set
            status = "✅ processed" if is_processed else "🔄 unprocessed"
            print(f"  {i}. {file_name} - {status}")
            print(f"      Modified: {file_info['modified']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Use the full scoped table name as it appears in watermarks
    table_name = "US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool" 
    check_processed_files(table_name)