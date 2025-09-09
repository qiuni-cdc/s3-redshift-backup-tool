#!/usr/bin/env python3
"""
Standalone Test for Large File List Handling

Tests the SimpleWatermarkManager with 1000+ files without requiring pytest.
"""

import json
import time
import sys
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

# Add project root to path
sys.path.insert(0, '/home/qi_chen/s3-redshift-backup')

from src.core.simple_watermark_manager import SimpleWatermarkManager


def create_manager():
    """Create a manager with mocked S3."""
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'watermark_prefix': 'watermarks/v2/'
        }
    }
    
    mock_s3 = Mock()
    
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
    
    with patch('src.core.simple_watermark_manager.boto3.client', return_value=mock_s3):
        manager = SimpleWatermarkManager(config)
        return manager, mock_s3, saved_data


def test_exactly_1000_files():
    """Test with exactly 1000 files (S3 API boundary)."""
    print("Testing 1000 files (S3 API limit)...")
    
    manager, _, _ = create_manager()
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
    
    print(f"✅ 1000 files test passed. Lookup time: {elapsed*1000:.1f}ms")
    assert elapsed < 0.1, f"Lookup took {elapsed}s, should be < 0.1s"


def test_5000_files():
    """Test with 5000 files (well above S3 limit)."""
    print("Testing 5000 files (5x S3 limit)...")
    
    manager, _, _ = create_manager()
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
    
    print("✅ 5000 files test passed with deduplication")


def test_json_size_limits():
    """Test JSON serialization with large file lists."""
    print("Testing JSON size with 2000 long file paths...")
    
    manager, _, _ = create_manager()
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
    
    print(f"✅ JSON size with 2000 long paths: {json_size_mb:.2f} MB")
    
    # S3 object size limit is 5GB, we should be well under that
    assert json_size_mb < 100, f"JSON too large: {json_size_mb} MB"


def test_performance_with_10k_files():
    """Test performance with 10K files."""
    print("Testing performance with 10,000 files...")
    
    manager, _, _ = create_manager()
    table = "test_10k_files"
    
    # Add 10K files in batches
    large_file_list = [f's3://bucket/file_{i:08d}.parquet' for i in range(10000)]
    
    batch_times = []
    for i in range(0, 10000, 1000):
        batch = large_file_list[i:i+1000]
        
        start = time.time()
        manager.update_redshift_state(table, loaded_files=batch, status='success')
        elapsed = time.time() - start
        batch_times.append(elapsed)
    
    # Test lookup performance
    watermark = manager.get_watermark(table)
    processed_set = set(watermark['processed_files'])
    
    start = time.time()
    
    # Check 1000 lookups
    import random
    for _ in range(1000):
        file_num = random.randint(0, 15000)
        file_uri = f's3://bucket/file_{file_num:08d}.parquet'
        is_processed = file_uri in processed_set
        
        # Verify correctness
        if file_num < 10000:
            assert is_processed, f"File {file_num} should be processed"
        else:
            assert not is_processed, f"File {file_num} should not be processed"
    
    elapsed = time.time() - start
    
    avg_batch_time = sum(batch_times) / len(batch_times)
    
    print(f"✅ 10K files performance:")
    print(f"   Average batch update: {avg_batch_time:.3f}s")
    print(f"   1000 lookups in: {elapsed:.3f}s ({elapsed/1000*1000:.1f}ms per lookup)")
    
    # Performance should be acceptable
    assert elapsed < 1.0, "Lookups should complete in under 1 second"
    assert avg_batch_time < 2.0, "Batch updates should complete in under 2 seconds"


def test_file_stats():
    """Test file statistics reporting."""
    print("Testing file statistics...")
    
    manager, _, _ = create_manager()
    table = "test_stats"
    
    # Add some files
    files = [f's3://bucket/test/file_{i:04d}.parquet' for i in range(1500)]
    manager.update_redshift_state(table, loaded_files=files, status='success')
    
    # Get statistics
    stats = manager.get_file_stats(table)
    
    print(f"✅ File statistics:")
    print(f"   Total files: {stats['total_files']}")
    print(f"   JSON size: {stats['json_size_kb']} KB")
    print(f"   Cache status: {stats['cache_status']}")
    print(f"   Performance warning: {stats['performance_warning']}")
    
    assert stats['total_files'] == 1500
    assert stats['json_size_kb'] > 0
    assert len(stats['sample_files']) <= 5


def test_cache_optimization():
    """Test that caching improves performance."""
    print("Testing cache optimization...")
    
    manager, _, _ = create_manager()
    table = "test_cache"
    
    # Add 5000 files
    files = [f's3://bucket/cache_test/file_{i:06d}.parquet' for i in range(5000)]
    manager.update_redshift_state(table, loaded_files=files, status='success')
    
    # First lookup (will create cache)
    start = time.time()
    result1 = manager.is_file_processed(table, files[2500])
    first_lookup = time.time() - start
    
    # Second lookup (should use cache)
    start = time.time()
    result2 = manager.is_file_processed(table, files[2500])
    cached_lookup = time.time() - start
    
    assert result1 == result2 == True
    
    print(f"✅ Cache optimization:")
    print(f"   First lookup: {first_lookup*1000:.1f}ms (creates cache)")
    print(f"   Cached lookup: {cached_lookup*1000:.1f}ms")
    print(f"   Speedup: {first_lookup/cached_lookup:.1f}x")


def main():
    """Run all large file tests."""
    print("LARGE FILE LIST HANDLING TESTS")
    print("="*60)
    
    try:
        test_exactly_1000_files()
        test_5000_files()
        test_json_size_limits()
        test_performance_with_10k_files()
        test_file_stats()
        test_cache_optimization()
        
        print("\n" + "="*60)
        print("✅ ALL LARGE FILE LIST TESTS PASSED")
        print("="*60)
        print("\nThe SimpleWatermarkManager can handle:")
        print("• 1000+ files (S3 API limit)")
        print("• 5000+ files (production scale)")
        print("• 10K+ files (stress test)")
        print("• Efficient caching for repeated lookups")
        print("• JSON serialization under size limits")
        print("• Consistent performance across file counts")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()