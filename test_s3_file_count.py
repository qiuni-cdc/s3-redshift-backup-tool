#!/usr/bin/env python3
"""
Test script for _count_actual_s3_files() function
Tests the table name pattern matching logic for S3 file counting
"""

import os
import sys
from pathlib import Path
import boto3

# Add src to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from backup.row_based import RowBasedBackupStrategy
from core.s3_manager import S3Manager
from config.settings import AppConfig

def test_table_name_conversion():
    """Test table name conversion logic"""
    print("=== Table Name Conversion Test ===")
    
    test_cases = [
        "US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool",
        "settlement.customers", 
        "DB:schema.table_name",
        "simple_table"
    ]
    
    for table_name in test_cases:
        safe_table_name = table_name.lower().replace(':', '_').replace('.', '_')
        print(f"Original: {table_name}")
        print(f"Safe:     {safe_table_name}")
        print()

def test_actual_s3_file_count():
    """Test the actual S3 file counting with our target table"""
    print("=== S3 File Count Test ===")
    
    try:
        # Initialize configuration
        config = AppConfig.load()
        
        # Create boto3 S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )
        
        # Initialize S3 manager with the client
        s3_manager = S3Manager(config=config, s3_client=s3_client)
        
        # Create RowBasedBackupStrategy instance
        backup = RowBasedBackupStrategy(config=config)
        
        # Set the s3_manager manually since we need it for the test
        backup.s3_manager = s3_manager
        
        # Test with our problematic table
        table_name = "US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool"
        print(f"Testing S3 file count for: {table_name}")
        
        # Call the function we fixed
        file_count = backup._count_actual_s3_files(table_name)
        
        print(f"Result: {file_count} files found")
        print(f"Expected: Should be 46 files (based on s3clean list output)")
        
        if file_count == 46:
            print("✅ SUCCESS: File count matches expected value!")
        elif file_count > 0:
            print(f"⚠️  PARTIAL: Found {file_count} files, expected 46")
        else:
            print("❌ FAILED: No files found, pattern matching still broken")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests"""
    print("Testing S3 file pattern matching fix\n")
    
    test_table_name_conversion()
    test_actual_s3_file_count()

if __name__ == "__main__":
    main()