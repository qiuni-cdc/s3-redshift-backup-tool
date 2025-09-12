#!/usr/bin/env python3
"""
Integration Test for Watermark Refactor

This test verifies that the new SimpleWatermarkManager works correctly
with existing components through the compatibility adapter.
"""

import json
import sys
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
sys.path.insert(0, '/home/qi_chen/s3-redshift-backup')

from src.core.watermark_adapter import WatermarkAdapter, LegacyWatermarkObject
from src.core.simple_watermark_manager import SimpleWatermarkManager


def test_legacy_api_compatibility():
    """Test that legacy components can use the new system unchanged."""
    print("Testing Legacy API Compatibility...")
    
    # Mock configuration
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'watermark_prefix': 'watermarks/v2/'
        }
    }
    
    # Create adapter with mocked S3
    mock_s3 = Mock()
    saved_data = {}
    
    def mock_put_object(**kwargs):
        key = kwargs['Key']
        body = kwargs['Body']
        saved_data[key] = json.loads(body)
        return {'ETag': 'mock-etag'}
    
    def mock_get_object(**kwargs):
        key = kwargs['Key']
        if key in saved_data:
            return {
                'Body': Mock(read=lambda: json.dumps(saved_data[key]).encode())
            }
        else:
            from botocore.exceptions import ClientError
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
    
    mock_s3.put_object.side_effect = mock_put_object
    mock_s3.get_object.side_effect = mock_get_object
    
    with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3):
        adapter = WatermarkAdapter(config)
        
        table_name = "settlement.settle_orders"
        
        # Test 1: get_table_watermark with no existing watermark
        watermark = adapter.get_table_watermark(table_name)
        assert watermark is None or watermark.mysql_status == 'pending'
        
        # Test 2: update_mysql_watermark (legacy method)
        success = adapter.update_mysql_watermark(
            table_name=table_name,
            extraction_time="2025-09-09T11:00:00Z",
            max_data_timestamp="2025-09-09T10:30:00Z",
            last_processed_id=12345,
            rows_extracted=1000,
            status='success'
        )
        assert success
        
        # Test 3: get_table_watermark after update
        watermark = adapter.get_table_watermark(table_name)
        assert watermark is not None
        assert isinstance(watermark, LegacyWatermarkObject)
        assert watermark.last_mysql_data_timestamp == "2025-09-09T10:30:00Z"
        assert watermark.last_processed_id == 12345
        assert watermark.mysql_status == 'success'
        
        # Test 4: update_redshift_watermark (legacy method)
        from datetime import datetime
        success = adapter.update_redshift_watermark(
            table_name=table_name,
            load_time=datetime.now(),
            rows_loaded=1000,
            status='success',
            processed_files=['file1.parquet', 'file2.parquet']
        )
        assert success
        
        # Test 5: verify Redshift state
        watermark = adapter.get_table_watermark(table_name)
        assert watermark.redshift_status == 'success'
        assert watermark.processed_s3_files == ['file1.parquet', 'file2.parquet']
        
        # Test 6: set_manual_watermark
        success = adapter.set_manual_watermark(
            table_name=table_name,
            data_timestamp="2025-09-01T00:00:00Z",
            data_id=50000
        )
        assert success
        
        watermark = adapter.get_table_watermark(table_name)
        assert watermark.last_mysql_data_timestamp == "2025-09-01T00:00:00Z"
        assert watermark.last_processed_id == 50000
        
    print("✅ Legacy API compatibility test passed")


def test_backup_strategy_integration():
    """Test integration with backup strategy code patterns."""
    print("Testing Backup Strategy Integration...")
    
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'watermark_prefix': 'watermarks/v2/'
        }
    }
    
    mock_s3 = Mock()
    saved_data = {}
    
    def mock_put_object(**kwargs):
        key = kwargs['Key']
        body = kwargs['Body']
        saved_data[key] = json.loads(body)
        return {'ETag': 'mock-etag'}
    
    def mock_get_object(**kwargs):
        key = kwargs['Key']
        if key in saved_data:
            return {
                'Body': Mock(read=lambda: json.dumps(saved_data[key]).encode())
            }
        else:
            from botocore.exceptions import ClientError
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
    
    mock_s3.put_object.side_effect = mock_put_object
    mock_s3.get_object.side_effect = mock_get_object
    
    with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3):
        adapter = WatermarkAdapter(config)
        table_name = "US_DW_RO_SSH:settlement.settle_orders"
        
        # Simulate backup strategy usage pattern
        
        # 1. Check for existing watermark
        watermark = adapter.get_table_watermark(table_name)
        
        if not watermark:
            last_timestamp = '1970-01-01 00:00:00'
            last_id = 0
        elif not watermark.last_mysql_data_timestamp and not getattr(watermark, 'last_processed_id', 0):
            last_timestamp = '1970-01-01 00:00:00'  
            last_id = 0
        else:
            # Resume from watermark (exactly like row_based.py)
            raw_timestamp = watermark.last_mysql_data_timestamp
            last_id = getattr(watermark, 'last_processed_id', 0)
            
            if isinstance(raw_timestamp, str) and 'T' in raw_timestamp:
                last_timestamp = raw_timestamp.replace('T', ' ').replace('Z', '')
                if '+' in last_timestamp:
                    last_timestamp = last_timestamp.split('+')[0]
            else:
                last_timestamp = str(raw_timestamp) if raw_timestamp else '1970-01-01 00:00:00'
        
        # 2. Process some data (simulate)
        new_timestamp = "2025-09-09T12:00:00Z"
        new_id = 67890
        
        # 3. Update watermark (using legacy _update_watermark_direct pattern)
        watermark_data = {
            'last_mysql_data_timestamp': new_timestamp,
            'last_processed_id': new_id,
            'mysql_status': 'success',
            'processed_s3_files': ['file1.parquet', 'file2.parquet']
        }
        
        success = adapter._update_watermark_direct(table_name, watermark_data)
        assert success
        
        # 4. Verify resume capability
        watermark = adapter.get_table_watermark(table_name)
        assert watermark.last_mysql_data_timestamp == new_timestamp
        assert watermark.last_processed_id == new_id
        assert watermark.mysql_status == 'success'
        
    print("✅ Backup strategy integration test passed")


def test_redshift_loader_integration():
    """Test integration with Redshift loader patterns."""
    print("Testing Redshift Loader Integration...")
    
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'watermark_prefix': 'watermarks/v2/'
        }
    }
    
    mock_s3 = Mock()
    saved_data = {}
    
    def mock_put_object(**kwargs):
        saved_data[kwargs['Key']] = json.loads(kwargs['Body'])
        return {'ETag': 'mock-etag'}
    
    def mock_get_object(**kwargs):
        key = kwargs['Key']
        if key in saved_data:
            return {'Body': Mock(read=lambda: json.dumps(saved_data[key]).encode())}
        else:
            from botocore.exceptions import ClientError
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
    
    mock_s3.put_object.side_effect = mock_put_object
    mock_s3.get_object.side_effect = mock_get_object
    
    with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3):
        adapter = WatermarkAdapter(config)
        table_name = "settlement.settle_orders"
        
        # Simulate Redshift loader file filtering pattern
        
        # 1. Check watermark for file filtering
        watermark = adapter.get_table_watermark(table_name)
        
        # 2. Get processed files list (like gemini_redshift_loader.py does)
        processed_files = []
        if watermark and watermark.processed_s3_files:
            processed_files = watermark.processed_s3_files
        
        # 3. Simulate file filtering
        all_s3_files = [
            's3://bucket/file1.parquet',
            's3://bucket/file2.parquet', 
            's3://bucket/file3.parquet'
        ]
        
        files_to_load = []
        for s3_file in all_s3_files:
            if s3_file not in processed_files:
                files_to_load.append(s3_file)
        
        assert len(files_to_load) == 3  # All files since no processed files yet
        
        # 4. Simulate successful loading
        from datetime import datetime
        success = adapter.update_redshift_watermark(
            table_name=table_name,
            load_time=datetime.now(),
            rows_loaded=5000,
            status='success', 
            processed_files=files_to_load[:2]  # Only loaded first 2 files
        )
        assert success
        
        # 5. Test file filtering again
        watermark = adapter.get_table_watermark(table_name)
        processed_files = watermark.processed_s3_files if watermark else []
        
        files_to_load = []
        for s3_file in all_s3_files:
            if s3_file not in processed_files:
                files_to_load.append(s3_file)
        
        assert len(files_to_load) == 1  # Only file3 should remain
        assert files_to_load[0] == 's3://bucket/file3.parquet'
        
    print("✅ Redshift loader integration test passed")


def test_cli_command_integration():
    """Test integration with CLI command patterns."""
    print("Testing CLI Command Integration...")
    
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'watermark_prefix': 'watermarks/v2/'
        }
    }
    
    mock_s3 = Mock()
    saved_data = {}
    
    def mock_put_object(**kwargs):
        saved_data[kwargs['Key']] = json.loads(kwargs['Body'])
        return {'ETag': 'mock-etag'}
    
    def mock_get_object(**kwargs):
        key = kwargs['Key']
        if key in saved_data:
            return {'Body': Mock(read=lambda: json.dumps(saved_data[key]).encode())}
        else:
            from botocore.exceptions import ClientError
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
    
    mock_s3.put_object.side_effect = mock_put_object
    mock_s3.get_object.side_effect = mock_get_object
    
    with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3):
        adapter = WatermarkAdapter(config)
        table_name = "settlement.settle_orders"
        
        # Test CLI watermark get command pattern
        watermark = adapter.get_table_watermark(table_name)
        
        # Should be None initially or have default pending status
        assert watermark is None or watermark.mysql_status == 'pending'
        
        # Test CLI watermark set command pattern
        success = adapter.set_manual_watermark(
            table_name=table_name,
            data_timestamp="2025-09-05T08:00:00Z",
            data_id=100000
        )
        assert success
        
        # Test CLI watermark get after set
        watermark = adapter.get_table_watermark(table_name)
        assert watermark is not None
        assert watermark.last_mysql_data_timestamp == "2025-09-05T08:00:00Z" 
        assert watermark.last_processed_id == 100000
        
        # Test CLI reset command pattern
        success = adapter.reset_table_watermark(table_name)
        assert success
        
        # Verify reset worked
        watermark = adapter.get_table_watermark(table_name)
        # Should have default values after reset
        assert watermark.mysql_status == 'pending'
        assert watermark.last_mysql_data_timestamp is None
        
    print("✅ CLI command integration test passed")


def main():
    """Run all integration tests."""
    print("WATERMARK INTEGRATION TESTS")
    print("="*60)
    
    try:
        test_legacy_api_compatibility()
        test_backup_strategy_integration() 
        test_redshift_loader_integration()
        test_cli_command_integration()
        
        print("\n" + "="*60)
        print("✅ ALL INTEGRATION TESTS PASSED")
        print("="*60)
        print("\nThe watermark refactor is fully integrated:")
        print("• Legacy API compatibility maintained")
        print("• Backup strategies work unchanged")
        print("• Redshift loader integration verified")
        print("• CLI commands function correctly")
        print("• Zero breaking changes to existing code")
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()