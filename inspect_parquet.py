#!/usr/bin/env python3
"""
Inspect S3 parquet files to check row counts and contents
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from src.config.settings import AppConfig

def inspect_s3_parquet_files(table_name: str):
    """Inspect S3 parquet files for a table"""
    
    print(f"=" * 80)
    print(f"Inspecting S3 Parquet Files for: {table_name}")
    print(f"=" * 80)
    
    try:
        # Initialize S3 client
        config = AppConfig()
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )
        
        bucket = config.s3.bucket_name
        
        # Build S3 prefix for the table
        safe_table_name = table_name.replace('.', '_').replace(':', '_').lower()
        prefix = f"incremental/"
        
        print(f"S3 Bucket: {bucket}")
        print(f"Search Prefix: {prefix}")
        print(f"Table Pattern: {safe_table_name}")
        
        # List objects
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        parquet_files = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet') and safe_table_name in key:
                        parquet_files.append({
                            'key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
        
        if not parquet_files:
            print("❌ No parquet files found for this table")
            return
        
        print(f"\nFound {len(parquet_files)} parquet files:")
        print("-" * 80)
        
        total_rows = 0
        total_size_mb = 0
        
        for i, file_info in enumerate(parquet_files, 1):
            key = file_info['key']
            size_bytes = file_info['size']
            size_mb = size_bytes / (1024 * 1024)
            total_size_mb += size_mb
            
            print(f"\n{i}. {key}")
            print(f"   Size: {size_mb:.2f} MB ({size_bytes:,} bytes)")
            print(f"   Modified: {file_info['last_modified']}")
            
            try:
                # Download and inspect parquet file
                print("   📊 Inspecting contents...")
                
                response = s3_client.get_object(Bucket=bucket, Key=key)
                parquet_data = response['Body'].read()
                
                # Read parquet file
                parquet_file = pq.ParquetFile(BytesIO(parquet_data))
                
                # Get row count
                row_count = parquet_file.metadata.num_rows
                total_rows += row_count
                
                print(f"   📈 Rows: {row_count:,}")
                
                # Get schema info
                schema = parquet_file.schema_arrow
                print(f"   📋 Columns: {len(schema)} fields")
                
                # Show first few columns
                column_names = [field.name for field in schema][:5]
                if len(column_names) < len(schema):
                    column_names.append('...')
                print(f"   🔖 Fields: {', '.join(column_names)}")
                
                # Sample first few rows if small file
                if row_count <= 1000:
                    table = pq.read_table(BytesIO(parquet_data))
                    df = table.to_pandas()
                    print(f"   📄 Sample data (first 3 rows):")
                    for idx, row in df.head(3).iterrows():
                        print(f"      Row {idx+1}: {dict(list(row.items())[:3])}...")
                
            except Exception as e:
                print(f"   ❌ Error inspecting file: {e}")
        
        print(f"\n{'='*80}")
        print(f"📊 SUMMARY")
        print(f"{'='*80}")
        print(f"Total files: {len(parquet_files)}")
        print(f"Total rows: {total_rows:,}")
        print(f"Total size: {total_size_mb:.2f} MB")
        print(f"Average rows per file: {total_rows // len(parquet_files):,}")
        print(f"Average file size: {total_size_mb / len(parquet_files):.2f} MB")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    table_name = "dw_parcel_detail_tool"
    inspect_s3_parquet_files(table_name)