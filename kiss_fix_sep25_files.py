#!/usr/bin/env python3
"""
KISS Solution: Remove September 25 Files from Blacklist

The simplest solution is to remove the September 25 files from the blacklist
so they can be discovered and loaded to Redshift properly.
"""

from src.config.settings import AppConfig
from src.core.simple_watermark_manager import SimpleWatermarkManager


def kiss_fix_sep25_files():
    """KISS solution: Remove September 25 files from blacklist"""
    print("🔧 KISS Solution: Remove Sep 25 files from blacklist for proper loading")
    print("=" * 70)
    
    config_dict = AppConfig.load().to_dict()
    manager = SimpleWatermarkManager(config_dict)
    table_name = 'US_DW_RO_SSH:settlement.settle_orders'
    
    # Get current watermark
    watermark = manager.get_watermark(table_name)
    processed_files = watermark.get('processed_files', [])
    
    print(f"📋 Current blacklist size: {len(processed_files)} files")
    
    # Find September 25 files
    sep25_files = [f for f in processed_files if '20250925' in f]
    print(f"📅 September 25 files in blacklist: {len(sep25_files)}")
    
    if not sep25_files:
        print("✅ No September 25 files to remove")
        return
    
    print("📄 September 25 files to remove:")
    for file_uri in sep25_files:
        print(f"   - {file_uri}")
    
    # KISS: Simply remove September 25 files from the list
    updated_files = [f for f in processed_files if '20250925' not in f]
    
    print(f"\n🔧 Removing {len(sep25_files)} September 25 files from blacklist")
    print(f"📊 New blacklist size: {len(updated_files)} files")
    
    # Update watermark with cleaned file list
    watermark['processed_files'] = updated_files
    
    # Save the updated watermark
    manager._save_watermark(table_name, watermark)
    
    print("✅ September 25 files removed from blacklist")
    print("🚀 These files will now be discovered and loaded to Redshift")
    
    # Verify the change
    updated_watermark = manager.get_watermark(table_name)
    final_files = updated_watermark.get('processed_files', [])
    final_sep25_files = [f for f in final_files if '20250925' in f]
    
    print(f"\n🔍 Verification:")
    print(f"   Final blacklist size: {len(final_files)} files")
    print(f"   September 25 files remaining: {len(final_sep25_files)}")
    
    if len(final_sep25_files) == 0:
        print("✅ KISS fix successful!")
    else:
        print("❌ KISS fix failed - some September 25 files still in blacklist")


if __name__ == '__main__':
    kiss_fix_sep25_files()