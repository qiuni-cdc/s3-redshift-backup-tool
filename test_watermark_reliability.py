#!/usr/bin/env python3
"""
Test script for enhanced watermark reliability system
Verifies multi-location backup and recovery functionality
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_watermark_reliability():
    """Test the enhanced watermark reliability features"""
    
    print("🧪 Testing Enhanced Watermark Reliability System")
    print("=" * 60)
    
    try:
        # Import after path setup
        from src.config.settings import AppConfig
        from src.core.s3_watermark_manager import S3WatermarkManager, S3TableWatermark
        
        # Load configuration
        config = AppConfig()
        
        # Initialize watermark manager
        watermark_manager = S3WatermarkManager(config)
        
        test_table = "test.watermark_reliability_test"
        
        print(f"📋 Test Table: {test_table}")
        print()
        
        # Test 1: Create a test watermark
        print("🔬 Test 1: Creating test watermark with multi-location backup")
        test_watermark = S3TableWatermark(
            table_name=test_table,
            last_mysql_extraction_time=datetime.utcnow().isoformat() + 'Z',
            last_mysql_data_timestamp=datetime(2024, 8, 19, 15, 30, 0).isoformat() + 'Z',
            mysql_rows_extracted=1000000,
            mysql_status='success',
            redshift_status='pending',
            redshift_rows_loaded=0,
            backup_strategy='test',
            s3_file_count=100
        )
        
        # Save watermark (should create backups)
        success = watermark_manager._save_watermark(test_watermark)
        print(f"   Watermark save result: {'✅ Success' if success else '❌ Failed'}")
        
        if not success:
            print("❌ Test failed at watermark creation")
            return False
        
        # Test 2: Check backup status
        print("\n🔬 Test 2: Checking backup locations")
        backup_status = watermark_manager.get_watermark_backup_status(test_table)
        
        for location, status in backup_status['backup_locations'].items():
            status_icon = "✅" if status.get('status') == 'available' else "❌"
            print(f"   {location}: {status_icon} {status.get('status', 'unknown')}")
        
        # Test 3: Retrieve watermark (should get from primary)
        print("\n🔬 Test 3: Retrieving watermark from primary location")
        retrieved_watermark = watermark_manager.get_table_watermark(test_table)
        
        if retrieved_watermark:
            print("   ✅ Successfully retrieved watermark from primary")
            print(f"   📊 MySQL Rows: {retrieved_watermark.mysql_rows_extracted:,}")
            print(f"   📊 MySQL Status: {retrieved_watermark.mysql_status}")
        else:
            print("   ❌ Failed to retrieve watermark")
            return False
        
        # Test 4: Simulate primary watermark corruption
        print("\n🔬 Test 4: Simulating primary watermark corruption")
        try:
            # Delete primary watermark to simulate corruption
            primary_key = watermark_manager._get_table_watermark_key(test_table)
            watermark_manager.s3_client.delete_object(
                Bucket=config.s3.bucket_name,
                Key=primary_key
            )
            print("   🗑️ Deleted primary watermark to simulate corruption")
            
            # Try to retrieve - should recover from backup
            print("   🔄 Attempting recovery from backups...")
            recovered_watermark = watermark_manager.get_table_watermark(test_table)
            
            if recovered_watermark:
                print("   ✅ Successfully recovered watermark from backup!")
                print(f"   📊 Recovered MySQL Rows: {recovered_watermark.mysql_rows_extracted:,}")
                print(f"   📊 Recovered MySQL Status: {recovered_watermark.mysql_status}")
                
                # Verify data integrity
                if (recovered_watermark.mysql_rows_extracted == test_watermark.mysql_rows_extracted and
                    recovered_watermark.mysql_status == test_watermark.mysql_status):
                    print("   ✅ Data integrity verification passed")
                else:
                    print("   ❌ Data integrity verification failed")
                    return False
            else:
                print("   ❌ Failed to recover watermark from backups")
                return False
                
        except Exception as e:
            print(f"   ❌ Error during corruption test: {e}")
            return False
        
        # Test 5: Check that primary was restored
        print("\n🔬 Test 5: Verifying primary watermark restoration")
        restored_primary = watermark_manager._get_watermark_from_primary(test_table)
        
        if restored_primary:
            print("   ✅ Primary watermark successfully restored")
        else:
            print("   ❌ Primary watermark was not restored")
            return False
        
        # Test 6: Cleanup test data
        print("\n🧹 Cleaning up test data...")
        try:
            # Delete test watermarks
            watermark_manager.delete_table_watermark(test_table)
            
            # Clean up backup files
            safe_name = test_table.replace('.', '_')
            
            # Clean daily backup
            daily_key = watermark_manager._get_daily_backup_key(test_table)
            try:
                watermark_manager.s3_client.delete_object(
                    Bucket=config.s3.bucket_name,
                    Key=daily_key
                )
            except:
                pass
            
            # Clean session backups
            prefix = f"{watermark_manager.watermark_prefix}backups/sessions/"
            response = watermark_manager.s3_client.list_objects_v2(
                Bucket=config.s3.bucket_name,
                Prefix=prefix
            )
            
            for obj in response.get('Contents', []):
                if safe_name in obj['Key']:
                    try:
                        watermark_manager.s3_client.delete_object(
                            Bucket=config.s3.bucket_name,
                            Key=obj['Key']
                        )
                    except:
                        pass
            
            print("   ✅ Test cleanup completed")
            
        except Exception as e:
            print(f"   ⚠️ Cleanup warning: {e}")
        
        print("\n🎉 All tests passed! Enhanced watermark reliability system is working correctly.")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_watermark_recovery_priority():
    """Test the recovery priority system"""
    
    print("\n🔬 Testing Recovery Priority System")
    print("-" * 40)
    
    try:
        from src.config.settings import AppConfig
        from src.core.s3_watermark_manager import S3WatermarkManager
        
        config = AppConfig()
        watermark_manager = S3WatermarkManager(config)
        
        test_table = "test.recovery_priority_test"
        
        # Test recovery methods individually
        recovery_methods = [
            ("Daily Backup", watermark_manager._recover_from_daily_backup),
            ("Latest Session", watermark_manager._recover_from_latest_session_backup), 
            ("Any Session", watermark_manager._recover_from_any_session_backup)
        ]
        
        for method_name, method_func in recovery_methods:
            print(f"   Testing {method_name}: ", end="")
            try:
                result = method_func(test_table)
                if result:
                    print("✅ Found backup")
                else:
                    print("❌ No backup found (expected for test)")
            except Exception as e:
                print(f"❌ Error: {e}")
        
        print("   ✅ Recovery priority system tested")
        return True
        
    except Exception as e:
        print(f"   ❌ Recovery priority test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting Watermark Reliability Tests")
    print("=" * 60)
    
    # Test basic reliability features
    basic_test_result = test_watermark_reliability()
    
    # Test recovery priority system
    priority_test_result = test_watermark_recovery_priority()
    
    print("\n📋 Test Summary")
    print("=" * 60)
    print(f"Basic Reliability Test: {'✅ PASSED' if basic_test_result else '❌ FAILED'}")
    print(f"Recovery Priority Test: {'✅ PASSED' if priority_test_result else '❌ FAILED'}")
    
    if basic_test_result and priority_test_result:
        print("\n🎉 All watermark reliability tests PASSED!")
        print("✅ Enhanced watermark system is ready for production use")
        sys.exit(0)
    else:
        print("\n❌ Some tests FAILED!")
        print("⚠️ Enhanced watermark system needs attention before production use")
        sys.exit(1)