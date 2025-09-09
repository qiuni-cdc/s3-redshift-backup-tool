"""
Comprehensive Unit Tests for SimpleWatermarkManager

Tests cover all functionality of the new clean watermark design including:
- Initial watermark creation
- State updates for MySQL and Redshift
- File blacklist management
- CDC strategy support
- Error handling and edge cases
- Concurrent access control
"""

import pytest
import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from src.core.simple_watermark_manager import SimpleWatermarkManager
from src.utils.exceptions import WatermarkError


class TestSimpleWatermarkManager:
    """Comprehensive unit tests for SimpleWatermarkManager."""
    
    @pytest.fixture
    def mock_config(self):
        """Test configuration."""
        return {
            's3': {
                'bucket_name': 'test-bucket',
                'watermark_prefix': 'watermarks/v2/'
            },
            'redshift': {
                'host': 'test.redshift.amazonaws.com',
                'database': 'testdb'
            }
        }
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client."""
        client = Mock()
        return client
    
    @pytest.fixture
    def manager(self, mock_config, mock_s3_client):
        """Create manager instance with mocked dependencies."""
        with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3_client):
            return SimpleWatermarkManager(mock_config)
    
    def test_initial_watermark_creation(self, manager, mock_s3_client):
        """Test zero-state watermark creation."""
        # Simulate no existing watermark
        mock_s3_client.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        
        watermark = manager.get_watermark('test_table')
        
        # Verify structure
        assert watermark['version'] == '2.0'
        assert watermark['table_name'] == 'test_table'
        assert watermark['cdc_strategy'] == 'hybrid'
        
        # Verify MySQL state
        assert watermark['mysql_state']['last_timestamp'] is None
        assert watermark['mysql_state']['last_id'] is None
        assert watermark['mysql_state']['status'] == 'pending'
        assert watermark['mysql_state']['error'] is None
        
        # Verify Redshift state
        assert watermark['redshift_state']['total_rows'] == 0
        assert watermark['redshift_state']['status'] == 'pending'
        assert watermark['redshift_state']['last_loaded_files'] == []
        
        # Verify empty processed files
        assert watermark['processed_files'] == []
        
        # Verify metadata
        assert watermark['metadata']['manual_override'] is False
        assert 'created_at' in watermark['metadata']
    
    def test_mysql_state_updates(self, manager, mock_s3_client):
        """Test MySQL extraction state management."""
        # Setup existing watermark
        existing = manager._create_default_watermark('test_table')
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Test timestamp-only update
        manager.update_mysql_state(
            'test_table',
            timestamp='2025-09-09T10:00:00Z',
            status='success'
        )
        
        # Verify save was called
        assert mock_s3_client.put_object.called
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['mysql_state']['last_timestamp'] == '2025-09-09T10:00:00Z'
        assert saved_data['mysql_state']['last_id'] is None
        assert saved_data['mysql_state']['status'] == 'success'
        
        # Test ID-only update
        manager.update_mysql_state(
            'test_table',
            id=12345,
            status='success'
        )
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['mysql_state']['last_id'] == 12345
        
        # Test hybrid update (both timestamp and ID)
        manager.update_mysql_state(
            'test_table',
            timestamp='2025-09-09T11:00:00Z',
            id=67890,
            status='success'
        )
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['mysql_state']['last_timestamp'] == '2025-09-09T11:00:00Z'
        assert saved_data['mysql_state']['last_id'] == 67890
        
        # Test error state
        manager.update_mysql_state(
            'test_table',
            status='failed',
            error='Connection timeout'
        )
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['mysql_state']['status'] == 'failed'
        assert saved_data['mysql_state']['error'] == 'Connection timeout'
    
    def test_redshift_state_updates(self, manager, mock_s3_client):
        """Test Redshift loading state management."""
        # Setup existing watermark
        existing = manager._create_default_watermark('test_table')
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Mock Redshift count query
        with patch.object(manager, '_query_redshift_count', return_value=1000):
            # Test successful load
            manager.update_redshift_state(
                'test_table',
                loaded_files=['s3://bucket/file1.parquet', 's3://bucket/file2.parquet'],
                status='success'
            )
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['redshift_state']['total_rows'] == 1000
        assert saved_data['redshift_state']['status'] == 'success'
        assert saved_data['redshift_state']['last_loaded_files'] == [
            's3://bucket/file1.parquet', 
            's3://bucket/file2.parquet'
        ]
        assert set(saved_data['processed_files']) == {
            's3://bucket/file1.parquet',
            's3://bucket/file2.parquet'
        }
    
    def test_processed_files_management(self, manager, mock_s3_client):
        """Test file blacklist operations."""
        # Setup watermark with some processed files
        existing = manager._create_default_watermark('test_table')
        existing['processed_files'] = ['s3://bucket/old1.parquet', 's3://bucket/old2.parquet']
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Test adding new files (no duplicates)
        with patch.object(manager, '_query_redshift_count', return_value=2000):
            manager.update_redshift_state(
                'test_table',
                loaded_files=['s3://bucket/new1.parquet', 's3://bucket/old1.parquet'],  # old1 is duplicate
                status='success'
            )
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        processed = set(saved_data['processed_files'])
        
        # Should have 3 unique files (old1, old2, new1)
        assert len(processed) == 3
        assert 'old1.parquet' in str(processed)
        assert 'old2.parquet' in str(processed)
        assert 'new1.parquet' in str(processed)
        
        # Test blacklist lookup
        assert manager.is_file_processed('test_table', 's3://bucket/old1.parquet') is True
        assert manager.is_file_processed('test_table', 's3://bucket/new999.parquet') is False
    
    def test_cdc_strategy_support(self, manager, mock_s3_client):
        """Test all 5 CDC strategies."""
        strategies = ['timestamp_only', 'hybrid', 'id_only', 'full_sync', 'custom_sql']
        
        for strategy in strategies:
            # Create watermark with specific strategy
            watermark = manager._create_default_watermark('test_table')
            watermark['cdc_strategy'] = strategy
            mock_s3_client.get_object.return_value = {
                'Body': Mock(read=lambda: json.dumps(watermark).encode())
            }
            
            # Verify strategy is preserved
            result = manager.get_watermark('test_table')
            assert result['cdc_strategy'] == strategy
            
            # Test appropriate updates based on strategy
            if strategy in ['timestamp_only', 'hybrid']:
                manager.update_mysql_state('test_table', timestamp='2025-09-09T12:00:00Z')
            
            if strategy in ['id_only', 'hybrid']:
                manager.update_mysql_state('test_table', id=99999)
            
            if strategy == 'custom_sql':
                # Custom SQL can store arbitrary data in metadata
                watermark = manager.get_watermark('test_table')
                watermark['metadata']['custom_watermark_data'] = {'batch_id': 'batch_123'}
                manager._save_watermark('test_table', watermark)
    
    def test_manual_watermark_setting(self, manager, mock_s3_client):
        """Test manual watermark override functionality."""
        existing = manager._create_default_watermark('test_table')
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Set manual watermark
        manager.set_manual_watermark(
            'test_table',
            timestamp='2025-09-01T00:00:00Z',
            id=50000
        )
        
        # Verify manual override flag is set
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['metadata']['manual_override'] is True
        assert 'manual_set_time' in saved_data['metadata']
        assert saved_data['mysql_state']['last_timestamp'] == '2025-09-01T00:00:00Z'
        assert saved_data['mysql_state']['last_id'] == 50000
    
    def test_watermark_reset(self, manager, mock_s3_client):
        """Test watermark reset functionality."""
        # Setup watermark with data
        existing = manager._create_default_watermark('test_table')
        existing['mysql_state']['last_timestamp'] = '2025-09-09T10:00:00Z'
        existing['mysql_state']['last_id'] = 12345
        existing['processed_files'] = ['file1.parquet', 'file2.parquet']
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Test reset without preserving files
        manager.reset_watermark('test_table', preserve_files=False)
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['mysql_state']['last_timestamp'] is None
        assert saved_data['mysql_state']['last_id'] is None
        assert saved_data['processed_files'] == []
        
        # Test reset with file preservation
        manager.reset_watermark('test_table', preserve_files=True)
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['mysql_state']['last_timestamp'] is None
        assert saved_data['mysql_state']['last_id'] is None
        assert saved_data['processed_files'] == ['file1.parquet', 'file2.parquet']
    
    def test_concurrent_access_control(self, manager, mock_s3_client):
        """Test watermark locking mechanism."""
        # Test successful lock acquisition
        mock_s3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )
        
        lock_id = manager.acquire_lock('test_table')
        assert lock_id is not None
        assert mock_s3_client.put_object.called
        
        # Verify lock data
        lock_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert lock_data['lock_id'] == lock_id
        assert 'locked_at' in lock_data
        assert 'pid' in lock_data
        
        # Test lock conflict
        mock_s3_client.head_object.side_effect = None  # Lock exists
        mock_s3_client.head_object.return_value = {}
        
        with pytest.raises(WatermarkError) as exc_info:
            manager.acquire_lock('test_table')
        assert 'is locked by another process' in str(exc_info.value)
        
        # Test lock release
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps({'lock_id': lock_id}).encode())
        }
        
        manager.release_lock('test_table', lock_id)
        assert mock_s3_client.delete_object.called
    
    def test_table_name_cleaning(self, manager):
        """Test table name cleaning for S3 keys."""
        # Test various table name formats
        assert manager._clean_table_name('simple_table') == 'simple_table'
        assert manager._clean_table_name('schema.table') == 'schema_table'
        assert manager._clean_table_name('US_DW_RO:schema.table') == 'us_dw_ro_schema_table'
        assert manager._clean_table_name('UPPERCASE') == 'uppercase'
    
    def test_error_handling(self, manager, mock_s3_client):
        """Test error handling scenarios."""
        # Test S3 read error
        mock_s3_client.get_object.side_effect = Exception("S3 connection error")
        
        with pytest.raises(WatermarkError):
            manager.get_watermark('test_table')
        
        # Test S3 write error  
        mock_s3_client.get_object.side_effect = None
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(manager._create_default_watermark('test_table')).encode())
        }
        mock_s3_client.put_object.side_effect = Exception("S3 write error")
        
        with pytest.raises(WatermarkError):
            manager.update_mysql_state('test_table', timestamp='2025-09-09T10:00:00Z')
    
    def test_redshift_count_query_failure(self, manager, mock_s3_client):
        """Test graceful handling of Redshift query failures."""
        existing = manager._create_default_watermark('test_table')
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Mock failed Redshift query
        with patch.object(manager, '_query_redshift_count', side_effect=Exception("Connection failed")):
            # Should not raise exception, just log warning
            manager.update_redshift_state(
                'test_table',
                loaded_files=['file1.parquet'],
                status='success'
            )
        
        # Verify watermark was still updated (with 0 count)
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert saved_data['redshift_state']['total_rows'] == 0
        assert saved_data['redshift_state']['status'] == 'success'
        assert 'file1.parquet' in saved_data['processed_files']
    
    def test_large_file_list_performance(self, manager, mock_s3_client):
        """Test performance with large processed file lists."""
        # Create watermark with 10K files
        existing = manager._create_default_watermark('test_table')
        existing['processed_files'] = [f's3://bucket/file{i}.parquet' for i in range(10000)]
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(existing).encode())
        }
        
        # Time the lookup operation
        import time
        start = time.time()
        
        # Should be fast even with 10K files
        assert manager.is_file_processed('test_table', 's3://bucket/file5000.parquet') is True
        assert manager.is_file_processed('test_table', 's3://bucket/file99999.parquet') is False
        
        elapsed = time.time() - start
        assert elapsed < 0.1  # Should be under 100ms
        
        # Test adding more files
        with patch.object(manager, '_query_redshift_count', return_value=100000):
            manager.update_redshift_state(
                'test_table',
                loaded_files=[f's3://bucket/newfile{i}.parquet' for i in range(100)],
                status='success'
            )
        
        saved_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'])
        assert len(saved_data['processed_files']) == 10100  # 10K + 100 new