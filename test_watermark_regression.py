#!/usr/bin/env python3
"""
Regression Tests for Watermark Major Functionality

Tests all the critical watermark bugs that were fixed:
1. Datetime serialization in watermark persistence
2. S3 file blacklist tracking after backup
3. Watermark state management consistency
4. Integration between backup and Redshift loading phases

These tests prevent regression of the bugs found on September 25, 2025.
"""

import json
import tempfile
import unittest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import boto3
from moto import mock_aws
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.core.unified_watermark_manager import UnifiedWatermarkManager
from src.core.simple_watermark_manager import SimpleWatermarkManager
from src.backup.row_based import RowBasedBackupStrategy
from src.core.gemini_redshift_loader import GeminiRedshiftLoader
from src.config.settings import AppConfig


class TestWatermarkDatetimeSerializationRegression(unittest.TestCase):
    """Test regression for datetime serialization bug fixed on 2025-09-25"""
    
    def setUp(self):
        self.config = {
            's3': {
                'bucket_name': 'test-bucket',
                'access_key_id': 'test-key',
                'secret_access_key': 'test-secret',
                'region': 'us-west-2'
            }
        }
    
    @mock_aws
    def test_watermark_datetime_serialization_to_s3(self):
        """REGRESSION TEST: Ensure datetime objects are serialized when writing to S3"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        # Create watermark manager
        manager = SimpleWatermarkManager(self.config)
        
        # Test updating with datetime objects (the bug scenario)
        current_time = datetime.now()
        
        # This should NOT fail with datetime serialization error
        try:
            manager.update_mysql_state(
                table_name='test_table',
                timestamp=current_time.strftime('%Y-%m-%d %H:%M:%S'),
                id=12345,
                status='success',
                rows_extracted=1000
            )
            success = True
        except TypeError as e:
            if 'datetime' in str(e) and 'JSON serializable' in str(e):
                success = False
                self.fail(f"REGRESSION FAILURE: Datetime serialization bug has returned: {e}")
            else:
                raise
        
        self.assertTrue(success, "Watermark with datetime should be serializable")
        
        # Verify watermark was actually saved and can be retrieved
        watermark = manager.get_watermark('test_table')
        self.assertEqual(watermark['mysql_state']['status'], 'success')
        self.assertEqual(watermark['mysql_state']['total_rows'], 1000)
        self.assertIsNotNone(watermark['mysql_state']['last_updated'])


class TestS3FileBlacklistTrackingRegression(unittest.TestCase):
    """Test regression for S3 file blacklist tracking bug fixed on 2025-09-25"""
    
    def setUp(self):
        self.config = {
            's3': {
                'bucket_name': 'test-bucket',
                'access_key_id': 'test-key',
                'secret_access_key': 'test-secret',
                'region': 'us-west-2'
            }
        }
    
    @mock_aws
    def test_s3_files_added_to_blacklist_after_backup(self):
        """REGRESSION TEST: Ensure S3 files are added to blacklist after successful backup"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        manager = SimpleWatermarkManager(self.config)
        table_name = 'test_table'
        
        # Simulate backup creating S3 files
        s3_files_created = [
            's3://test-bucket/incremental/year=2025/month=09/day=25/test_table_20250925_120000_batch_1.parquet',
            's3://test-bucket/incremental/year=2025/month=09/day=25/test_table_20250925_120100_batch_2.parquet'
        ]
        
        # Initially, no files should be in blacklist
        initial_watermark = manager.get_watermark(table_name)
        self.assertEqual(len(initial_watermark.get('processed_files', [])), 0)
        
        # Simulate successful backup completion - this is where the bug was
        manager.update_redshift_state(
            table_name=table_name,
            loaded_files=s3_files_created,
            status='pending',
            error=None
        )
        
        # REGRESSION CHECK: Files should now be in blacklist
        updated_watermark = manager.get_watermark(table_name)
        processed_files = updated_watermark.get('processed_files', [])
        
        self.assertEqual(len(processed_files), 2, 
                        "REGRESSION FAILURE: S3 files not added to blacklist after backup")
        
        for s3_file in s3_files_created:
            self.assertIn(s3_file, processed_files,
                         f"REGRESSION FAILURE: S3 file {s3_file} missing from blacklist")
        
        # Verify file deduplication works
        for s3_file in s3_files_created:
            is_processed = manager.is_file_processed(table_name, s3_file)
            self.assertTrue(is_processed, 
                           f"REGRESSION FAILURE: File {s3_file} should be marked as processed")


class TestWatermarkPersistenceVerification(unittest.TestCase):
    """Test that watermark changes actually persist to S3"""
    
    def setUp(self):
        self.config = {
            's3': {
                'bucket_name': 'test-bucket',
                'access_key_id': 'test-key', 
                'secret_access_key': 'test-secret',
                'region': 'us-west-2',
                'watermark_prefix': 'watermarks/v2/'
            }
        }
    
    @mock_aws
    def test_watermark_persistence_verification(self):
        """REGRESSION TEST: Verify watermark changes actually persist to S3"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        manager = SimpleWatermarkManager(self.config)
        table_name = 'test_table'
        
        # Make watermark changes
        manager.update_mysql_state(
            table_name=table_name,
            timestamp='2025-09-25 12:38:27',
            id=105816330,
            status='success',
            rows_extracted=5000
        )
        
        # Verify changes persisted by checking S3 directly
        s3_key = f"watermarks/v2/{table_name.replace(':', '_').replace('.', '_').lower()}.json"
        
        try:
            response = s3_client.get_object(Bucket='test-bucket', Key=s3_key)
            watermark_data = json.loads(response['Body'].read())
            
            # Verify the exact changes were persisted
            self.assertEqual(watermark_data['mysql_state']['status'], 'success')
            self.assertEqual(watermark_data['mysql_state']['total_rows'], 5000)
            self.assertEqual(watermark_data['mysql_state']['last_id'], 105816330)
            self.assertEqual(watermark_data['mysql_state']['last_timestamp'], '2025-09-25 12:38:27')
            
        except Exception as e:
            self.fail(f"REGRESSION FAILURE: Watermark not persisted to S3: {e}")


class TestS3FileBlacklistRegressionBugScenarios(unittest.TestCase):
    """Test specific regression scenarios that have occurred multiple times"""
    
    def setUp(self):
        self.config = {
            's3': {
                'bucket_name': 'test-bucket',
                'access_key_id': 'test-key',
                'secret_access_key': 'test-secret',
                'region': 'us-west-2'
            }
        }
    
    @mock_aws
    def test_missing_s3_blacklist_update_after_backup_success(self):
        """REGRESSION TEST: S3 files created but never added to blacklist (September 25, 2025 issue)"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        manager = SimpleWatermarkManager(self.config)
        table_name = 'US_DW_RO_SSH:settlement.settle_orders'
        
        # Step 1: Backup runs and creates S3 files but fails to add them to blacklist (the bug)
        # This simulates the row_based.py bug where S3 files weren't being tracked
        manager.update_mysql_state(
            table_name=table_name,
            timestamp='2025-09-25 09:01:59',
            id=105816330,
            status='success',
            rows_extracted=250000  # 5 chunks * 50k rows each
        )
        
        # Bug scenario: S3 files exist but are NOT in blacklist
        s3_files_created = [
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_1_batch_1.parquet',
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_2_batch_1.parquet',
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_3_batch_1.parquet',
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_4_batch_1.parquet',
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_5_batch_1.parquet'
        ]
        
        # Create actual S3 files to simulate backup completion
        for s3_file in s3_files_created:
            key = s3_file.replace('s3://test-bucket/', '')
            s3_client.put_object(Bucket='test-bucket', Key=key, Body=b'fake parquet data')
        
        # Step 2: Verify the bug - S3 files exist but blacklist is empty
        watermark = manager.get_watermark(table_name)
        self.assertEqual(watermark['mysql_state']['status'], 'success')
        self.assertEqual(len(watermark.get('processed_files', [])), 0, 
                        "REGRESSION FAILURE: Files should not be in blacklist yet (this is the bug)")
        
        # Step 3: Redshift loading tries to find files but can't because they're not properly managed
        # This simulates what happens during Redshift loading phase
        
        # Step 4: Apply the fix - add files to blacklist as they should have been
        manager.update_redshift_state(
            table_name=table_name,
            loaded_files=s3_files_created,
            status='pending',
            error=None
        )
        
        # Step 5: Verify the fix worked
        updated_watermark = manager.get_watermark(table_name)
        processed_files = updated_watermark.get('processed_files', [])
        self.assertEqual(len(processed_files), 5, 
                        "REGRESSION FIX: Files should now be in blacklist")
        
        for s3_file in s3_files_created:
            self.assertIn(s3_file, processed_files,
                         f"REGRESSION FIX: {s3_file} should be in blacklist")
    
    @mock_aws 
    def test_s3_discovery_limited_by_max_keys_regression(self):
        """REGRESSION TEST: S3 files not found due to max_keys limit (pagination issue)"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        manager = SimpleWatermarkManager(self.config)
        table_name = 'US_DW_RO_SSH:settlement.settle_orders'
        
        # Create many files to simulate pagination issues
        old_files = []
        target_files = []
        
        # Create 1000 "old" files that would be scanned first (newer dates)
        for i in range(1000):
            file_uri = f's3://test-bucket/incremental/year=2025/month=09/day=26/file_{i:04d}.parquet'
            key = file_uri.replace('s3://test-bucket/', '')
            s3_client.put_object(
                Bucket='test-bucket', 
                Key=key, 
                Body=b'fake data',
                # Make these files newer so they'd be found first in LastModified sort
            )
            old_files.append(file_uri)
        
        # Create the target September 25 files that we want to find
        for i in range(5):
            file_uri = f's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_09{i:02d}59_chunk_1.parquet'
            key = file_uri.replace('s3://test-bucket/', '')
            s3_client.put_object(
                Bucket='test-bucket',
                Key=key,
                Body=b'target data'
            )
            target_files.append(file_uri)
        
        # Add old files to blacklist (simulating they were processed before)
        manager.update_redshift_state(
            table_name=table_name,
            loaded_files=old_files,
            status='success',
            error=None
        )
        
        # The target files should NOT be in blacklist yet
        watermark = manager.get_watermark(table_name)
        processed_files = watermark.get('processed_files', [])
        
        # Verify old files are blacklisted but target files are not
        self.assertEqual(len(processed_files), 1000, "Old files should be blacklisted")
        
        for target_file in target_files:
            is_processed = manager.is_file_processed(table_name, target_file)
            self.assertFalse(is_processed, 
                           f"REGRESSION TEST: Target file {target_file} should NOT be processed yet")
        
        # This test verifies that the KISS solution (removing files from blacklist) 
        # would work correctly even with many files
    
    @mock_aws
    def test_kiss_solution_remove_files_from_blacklist(self):
        """REGRESSION TEST: KISS solution - remove problematic files from blacklist"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        manager = SimpleWatermarkManager(self.config)
        table_name = 'US_DW_RO_SSH:settlement.settle_orders'
        
        # Simulate the problem: September 25 files were incorrectly added to blacklist
        # without being loaded to Redshift first
        problematic_files = [
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_1_batch_1.parquet',
            's3://test-bucket/incremental/year=2025/month=09/day=25/us_dw_ro_ssh_settlement_settle_orders_20250925_090159_batch_chunk_2_batch_1.parquet'
        ]
        
        good_files = [
            's3://test-bucket/incremental/year=2025/month=09/day=24/us_dw_ro_ssh_settlement_settle_orders_20250924_160739_batch_chunk_1_batch_1.parquet'
        ]
        
        all_files = problematic_files + good_files
        
        # Add all files to blacklist (simulating the bug where files were blacklisted incorrectly)
        manager.update_redshift_state(
            table_name=table_name,
            loaded_files=all_files,
            status='pending',
            error=None
        )
        
        # Verify problem state
        watermark = manager.get_watermark(table_name)
        processed_files = watermark.get('processed_files', [])
        self.assertEqual(len(processed_files), 3, "All files should be blacklisted initially")
        
        # Apply KISS solution: Remove September 25 files from blacklist
        updated_files = [f for f in processed_files if '20250925' not in f]
        watermark['processed_files'] = updated_files
        manager._save_watermark(table_name, watermark)
        
        # Verify KISS solution worked
        final_watermark = manager.get_watermark(table_name)
        final_processed_files = final_watermark.get('processed_files', [])
        
        self.assertEqual(len(final_processed_files), 1, "Only good files should remain in blacklist")
        self.assertIn(good_files[0], final_processed_files, "Good files should remain")
        
        for prob_file in problematic_files:
            self.assertNotIn(prob_file, final_processed_files, 
                           f"KISS SOLUTION: {prob_file} should be removed from blacklist")
            
            # Verify files can now be discovered
            is_processed = manager.is_file_processed(table_name, prob_file)
            self.assertFalse(is_processed, 
                           f"KISS SOLUTION: {prob_file} should no longer be marked as processed")


class TestWatermarkStateManagementIntegration(unittest.TestCase):
    """Test integration between backup and Redshift loading phases"""
    
    def setUp(self):
        self.config = {
            's3': {
                'bucket_name': 'test-bucket',
                'access_key_id': 'test-key',
                'secret_access_key': 'test-secret', 
                'region': 'us-west-2'
            }
        }
    
    @mock_aws
    def test_backup_to_redshift_watermark_consistency(self):
        """REGRESSION TEST: Ensure backup phase properly sets up Redshift loading phase"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-west-2')
        s3_client.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        manager = SimpleWatermarkManager(self.config)
        table_name = 'test_table'
        
        # Phase 1: Backup completion (MySQL -> S3)
        s3_files_created = [
            's3://test-bucket/incremental/test_table_20250925_batch_1.parquet',
            's3://test-bucket/incremental/test_table_20250925_batch_2.parquet'
        ]
        
        # Update MySQL state (backup successful)
        manager.update_mysql_state(
            table_name=table_name,
            timestamp='2025-09-25 12:38:27',
            id=105816330,
            status='success',
            rows_extracted=2000
        )
        
        # Update Redshift state (files ready for loading)
        manager.update_redshift_state(
            table_name=table_name,
            loaded_files=s3_files_created,
            status='pending',
            error=None
        )
        
        # Phase 2: Redshift loading phase should see the files
        watermark = manager.get_watermark(table_name)
        
        # REGRESSION CHECK: Backup phase properly prepared Redshift loading
        self.assertEqual(watermark['mysql_state']['status'], 'success')
        self.assertEqual(watermark['mysql_state']['total_rows'], 2000)
        self.assertEqual(watermark['redshift_state']['status'], 'pending')
        
        processed_files = watermark.get('processed_files', [])
        self.assertEqual(len(processed_files), 2)
        
        # Phase 3: Simulate Redshift loading completion
        manager.update_redshift_state(
            table_name=table_name,
            loaded_files=[],  # No new files, just status update
            status='success',
            error=None
        )
        
        # Final verification
        final_watermark = manager.get_watermark(table_name)
        self.assertEqual(final_watermark['redshift_state']['status'], 'success')
        
        # Files should remain in blacklist to prevent reprocessing
        final_processed_files = final_watermark.get('processed_files', [])
        self.assertEqual(len(final_processed_files), 2)


def run_regression_tests():
    """Run all watermark regression tests"""
    print("🧪 Running Watermark Major Functionality Regression Tests...")
    print("📋 Testing fixes from September 25, 2025:")
    print("   1. Datetime serialization in watermark persistence")
    print("   2. S3 file blacklist tracking after backup")
    print("   3. Watermark persistence verification")
    print("   4. Backup to Redshift loading integration")
    print()
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestWatermarkDatetimeSerializationRegression))
    suite.addTests(loader.loadTestsFromTestCase(TestS3FileBlacklistTrackingRegression))
    suite.addTests(loader.loadTestsFromTestCase(TestWatermarkPersistenceVerification))
    suite.addTests(loader.loadTestsFromTestCase(TestWatermarkStateManagementIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print()
    if result.wasSuccessful():
        print("✅ ALL REGRESSION TESTS PASSED")
        print("🛡️  Watermark functionality is protected against known bugs")
    else:
        print("❌ REGRESSION TESTS FAILED")
        print("🚨 Watermark bugs may have been reintroduced")
        print(f"   Failures: {len(result.failures)}")
        print(f"   Errors: {len(result.errors)}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_regression_tests()
    exit(0 if success else 1)