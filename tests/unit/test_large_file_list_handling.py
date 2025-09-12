"""
Test Large File List Handling for SimpleWatermarkManager

This test specifically addresses the bug where watermarks cannot handle
more than 1000 S3 files. The issue likely stems from:

1. S3 ListObjects API limit of 1000 objects per request
2. JSON serialization size limits
3. Memory/performance issues with large lists
"""

import pytest
import json
import time
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from src.core.simple_watermark_manager import SimpleWatermarkManager


class TestLargeFileListHandling:
    """Test watermark handling with 1000+ files."""
    
    @pytest.fixture
    def mock_config(self):
        """Test configuration."""
        return {
            's3': {
                'bucket_name': 'test-bucket',
                'watermark_prefix': 'watermarks/v2/'
            }
        }
    
    @pytest.fixture
    def manager_with_mocked_s3(self, mock_config):
        """Create manager with mocked S3."""
        mock_s3 = Mock()
        with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3):
            manager = SimpleWatermarkManager(mock_config)
            
            # Track saved watermarks
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
                    raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
            
            mock_s3.put_object.side_effect = mock_put_object
            mock_s3.get_object.side_effect = mock_get_object
            
            return manager, mock_s3, saved_data
    
    def test_handles_exactly_1000_files(self, manager_with_mocked_s3):
        """Test with exactly 1000 files (S3 API boundary)."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_1000_files"
        
        # Create list of 1000 files
        files = [f's3://bucket/data/file_{i:06d}.parquet' for i in range(1000)]
        
        # Update watermark with 1000 files
        manager.update_redshift_state(table, loaded_files=files, status='success')
        
        # Verify all files saved
        watermark = manager.get_watermark(table)
        assert len(watermark['processed_files']) == 1000
        assert set(watermark['processed_files']) == set(files)
        
        # Test lookup performance
        start = time.time()
        for i in [0, 499, 999]:  # Test beginning, middle, end
            assert manager.is_file_processed(table, f's3://bucket/data/file_{i:06d}.parquet')
        elapsed = time.time() - start
        assert elapsed < 0.1, f"Lookup took {elapsed}s, should be < 0.1s"
    
    def test_handles_5000_files(self, manager_with_mocked_s3):
        """Test with 5000 files (well above S3 limit)."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_5000_files"
        
        # Simulate incremental loading over multiple sessions
        batch_size = 500
        total_files = 5000
        
        for batch in range(0, total_files, batch_size):
            files = [f's3://bucket/data/file_{i:06d}.parquet' 
                    for i in range(batch, min(batch + batch_size, total_files))]
            manager.update_redshift_state(table, loaded_files=files, status='success')
        
        # Verify all files tracked
        watermark = manager.get_watermark(table)
        assert len(watermark['processed_files']) == 5000
        
        # Test deduplication
        manager.update_redshift_state(table, 
            loaded_files=['s3://bucket/data/file_000001.parquet'],  # Duplicate
            status='success'
        )
        
        watermark = manager.get_watermark(table)
        assert len(watermark['processed_files']) == 5000  # No increase
    
    def test_json_serialization_with_large_lists(self, manager_with_mocked_s3):
        """Test JSON serialization doesn't break with large file lists."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_json_size"
        
        # Create files with longer paths (more realistic)
        files = [
            f's3://my-company-data-bucket/incremental/year=2025/month=09/day={d:02d}/'
            f'hour={h:02d}/settlement_orders_20250909_{h:02d}{m:02d}00_batch_{b}_chunk_{c}.parquet'
            for d in range(1, 10)
            for h in range(0, 24)
            for m in range(0, 60, 15)
            for b in range(1, 3)
            for c in range(1, 5)
        ][:2000]  # 2000 files with long paths
        
        # Update watermark
        manager.update_redshift_state(table, loaded_files=files, status='success')
        
        # Verify JSON serialization works
        watermark = manager.get_watermark(table)
        assert len(watermark['processed_files']) == 2000
        
        # Check JSON size
        json_str = json.dumps(watermark)
        json_size_mb = len(json_str) / (1024 * 1024)
        print(f"JSON size with 2000 long paths: {json_size_mb:.2f} MB")
        
        # S3 object size limit is 5GB, we should be well under that
        assert json_size_mb < 100, f"JSON too large: {json_size_mb} MB"
    
    def test_memory_efficient_file_checking(self, manager_with_mocked_s3):
        """Test memory-efficient file existence checking."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_memory_efficiency"
        
        # Add 10K files
        large_file_list = [f's3://bucket/file_{i:08d}.parquet' for i in range(10000)]
        
        # Update in batches to simulate real usage
        for i in range(0, 10000, 1000):
            batch = large_file_list[i:i+1000]
            manager.update_redshift_state(table, loaded_files=batch, status='success')
        
        # Memory-efficient lookup using set
        watermark = manager.get_watermark(table)
        processed_set = set(watermark['processed_files'])
        
        # Test lookup performance
        start = time.time()
        
        # Check 1000 random files
        import random
        for _ in range(1000):
            file_num = random.randint(0, 20000)
            file_uri = f's3://bucket/file_{file_num:08d}.parquet'
            is_processed = file_uri in processed_set
            
            # Verify correctness
            if file_num < 10000:
                assert is_processed, f"File {file_num} should be processed"
            else:
                assert not is_processed, f"File {file_num} should not be processed"
        
        elapsed = time.time() - start
        print(f"1000 lookups in {elapsed:.3f}s ({elapsed/1000*1000:.1f}ms per lookup)")
        assert elapsed < 1.0, "Lookups should complete in under 1 second"
    
    def test_sorted_file_list_optimization(self, manager_with_mocked_s3):
        """Test that file lists are sorted for consistent JSON and better compression."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_sorting"
        
        # Add files in random order
        files = [f's3://bucket/file_{i:04d}.parquet' for i in range(100)]
        import random
        random.shuffle(files)
        
        manager.update_redshift_state(table, loaded_files=files, status='success')
        
        # Verify files are sorted in storage
        watermark = manager.get_watermark(table)
        assert watermark['processed_files'] == sorted(files)
        
        # This ensures consistent JSON output and better compression
    
    def test_s3_api_pagination_awareness(self, manager_with_mocked_s3):
        """Test handling of S3's 1000-object listing limit."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_pagination"
        
        # The S3 ListObjectsV2 API returns max 1000 objects per request
        # Our watermark should handle file lists much larger than this
        
        # Add 3500 files (3.5x the S3 API limit)
        files = [f's3://bucket/year=2025/month=09/day={d:02d}/file_{i:06d}.parquet' 
                for d in range(1, 8)  # 7 days
                for i in range(500)]  # 500 files per day
        
        # Add all files
        manager.update_redshift_state(table, loaded_files=files, status='success')
        
        # Verify all files stored
        watermark = manager.get_watermark(table)
        assert len(watermark['processed_files']) == 3500
        
        # Simulate querying if specific files are processed
        # (This is what happens during S3 file filtering)
        test_files = [
            's3://bucket/year=2025/month=09/day=01/file_000000.parquet',  # First
            's3://bucket/year=2025/month=09/day=04/file_000250.parquet',  # Middle
            's3://bucket/year=2025/month=09/day=07/file_000499.parquet',  # Last
            's3://bucket/year=2025/month=09/day=08/file_000001.parquet',  # Not exists
        ]
        
        for file_uri in test_files[:3]:
            assert manager.is_file_processed(table, file_uri), f"{file_uri} should be processed"
        
        assert not manager.is_file_processed(table, test_files[3]), "Day 08 file should not exist"
    
    def test_incremental_file_addition_performance(self, manager_with_mocked_s3):
        """Test performance of incrementally adding files over many sessions."""
        manager, mock_s3, saved_data = manager_with_mocked_s3
        table = "test_incremental_performance"
        
        # Simulate 100 sessions, each adding 50 files
        # Total: 5000 files
        session_times = []
        
        for session in range(100):
            files = [f's3://bucket/session_{session:03d}/file_{i:03d}.parquet' 
                    for i in range(50)]
            
            start = time.time()
            manager.update_redshift_state(table, loaded_files=files, status='success')
            elapsed = time.time() - start
            session_times.append(elapsed)
        
        # Check that performance doesn't degrade significantly
        avg_first_10 = sum(session_times[:10]) / 10
        avg_last_10 = sum(session_times[-10:]) / 10
        
        print(f"First 10 sessions avg: {avg_first_10:.3f}s")
        print(f"Last 10 sessions avg: {avg_last_10:.3f}s")
        
        # Performance should not degrade by more than 2x
        assert avg_last_10 < avg_first_10 * 2, "Performance degraded too much"
        
        # Verify final count
        watermark = manager.get_watermark(table)
        assert len(watermark['processed_files']) == 5000


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])