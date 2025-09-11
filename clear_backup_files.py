#!/usr/bin/env python3
"""
Clear backup S3 files from watermark while preserving processed files.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.s3_watermark_manager import S3WatermarkManager
from src.config.settings import AppConfig

def clear_backup_files(table_name: str):
    """Clear backup S3 files while preserving processed files"""
    
    try:
        config = AppConfig()
        watermark_manager = S3WatermarkManager(config)
        watermark = watermark_manager.get_table_watermark(table_name)
        
        if not watermark:
            print(f"❌ No watermark found for {table_name}")
            return False
            
        backup_files = watermark.backup_s3_files if watermark.backup_s3_files else []
        processed_files = watermark.processed_s3_files if watermark.processed_s3_files else []
        
        print(f"Backup files to clear: {len(backup_files)}")
        print(f"Processed files to keep: {len(processed_files)}")
        
        if len(backup_files) == 0:
            print("✅ No backup files to clear")
            return True
        
        # Clear backup files, keep processed files
        watermark_data = {
            'backup_s3_files': [],
            's3_file_count': len(processed_files)
        }
        
        success = watermark_manager._update_watermark_direct(table_name, watermark_data)
        
        if success:
            print(f"✅ Cleared {len(backup_files)} backup files")
        else:
            print("❌ Failed to update watermark")
            
        return success
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    table_name = sys.argv[1] if len(sys.argv) > 1 else "US_DW_UNIDW_SSH:unidw.dw_parcel_pricing_temp"
    clear_backup_files(table_name)