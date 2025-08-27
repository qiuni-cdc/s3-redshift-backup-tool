#!/usr/bin/env python3
"""
Comprehensive tests for backup strategies.

Tests all three backup strategies (sequential, inter-table, intra-table)
with mocked database and S3 connections to validate functionality.
"""

import os
import sys
import unittest
from unittest.mock import Mock, MagicMock, patch, call
import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
import threading
import time

# Add src to path for imports  
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.backup.sequential import SequentialBackupStrategy
from src.backup.inter_table import InterTableBackupStrategy
from src.backup.intra_table import IntraTableBackupStrategy
from src.config.settings import AppConfig
from src.utils.exceptions import BackupError


def create_test_config():
    """Create test configuration with SSH key path"""
    test_key_path = "/mnt/c/Users/Qi Chen/Downloads/chenqi.pem"
    
    os.environ.update({
        'DB_HOST': 'test-host',
        'DB_USER': 'test-user',
        'DB_PASSWORD': 'test-password',
        'DB_DATABASE': 'test-db',
        'SSH_BASTION_HOST': 'test-bastion',
        'SSH_BASTION_USER': 'test-ssh-user',
        'SSH_BASTION_KEY_PATH': test_key_path,
        'S3_BUCKET_NAME': 'test-bucket',
        'S3_ACCESS_KEY': 'test-access',
        'S3_SECRET_KEY': 'test-secret',
        'LOG_LEVEL': 'DEBUG'
    })
    
    return AppConfig.load()


def create_mock_batch_data(num_rows=10):
    """Create mock batch data for testing"""
    return [
        {
            'ID': i,
            'billing_num': f'B{i:03d}',
            'customer_id': 1000 + i,
            'total_fee': 25.50 + i,
            'currency': 'USD',
            'update_at': datetime(2024, 1, 1, 10, i, 0)
        }
        for i in range(1, num_rows + 1)
    ]


class BaseBackupStrategyTest(unittest.TestCase):
    """Base class for backup strategy tests"""
    
    def setUp(self):
        """Set up test environment"""
        self.config = create_test_config()
        self.mock_batch_data = create_mock_batch_data(50)
    
    def create_mock_cursor(self, return_data=None):
        """Create mock database cursor"""
        mock_cursor = MagicMock()
        
        if return_data is None:
            return_data = self.mock_batch_data
        
        # Mock fetchmany to return batches
        batch_size = self.config.backup.batch_size
        batches = [return_data[i:i + batch_size] for i in range(0, len(return_data), batch_size)]
        batches.append([])  # Empty batch to signal end
        
        mock_cursor.fetchmany.side_effect = batches
        mock_cursor.close = MagicMock()
        
        return mock_cursor
    
    def create_mock_db_connection(self, cursor_data=None):
        """Create mock database connection"""
        mock_conn = MagicMock()
        mock_cursor = self.create_mock_cursor(cursor_data)
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn


class TestSequentialBackupStrategy(BaseBackupStrategyTest):
    """Test sequential backup strategy"""
    
    def setUp(self):
        super().setUp()
        self.strategy = SequentialBackupStrategy(self.config)
        
        # Mock dependencies
        self.strategy.watermark_manager = MagicMock()
        self.strategy.watermark_manager.get_last_watermark.return_value = "2024-01-01 09:00:00"
        self.strategy.s3_manager = MagicMock()
        self.strategy.data_validator = MagicMock()
        
        # Mock validation to always pass
        mock_validation_result = MagicMock()
        mock_validation_result.is_valid.return_value = True
        mock_validation_result.errors = []
        mock_validation_result.warnings = []
        self.strategy.data_validator.validate_dataframe.return_value = mock_validation_result
    
    @patch('src.backup.sequential.ConnectionManager')
    def test_single_table_success(self, mock_connection_manager):
        """Test successful single table backup"""
        # Setup mocks
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        with patch.object(self.strategy, 'validate_table_exists', return_value=True), \
             patch.object(self.strategy, 'get_table_row_count', return_value=50), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM test_table"), \
             patch.object(self.strategy, 'process_batch', return_value=True), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute backup
            result = self.strategy.execute(['test_table'])
            
            # Verify results
            self.assertTrue(result)
            self.assertEqual(self.strategy.metrics.tables_processed, 1)
            self.assertGreater(self.strategy.metrics.total_rows, 0)
    
    @patch('src.backup.sequential.ConnectionManager')
    def test_multiple_tables_success(self, mock_connection_manager):
        """Test successful multiple table backup"""
        # Setup mocks
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        tables = ['table1', 'table2', 'table3']
        
        with patch.object(self.strategy, 'validate_table_exists', return_value=True), \
             patch.object(self.strategy, 'get_table_row_count', return_value=50), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM table"), \
             patch.object(self.strategy, 'process_batch', return_value=True), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute backup
            result = self.strategy.execute(tables)
            
            # Verify results
            self.assertTrue(result)
            self.assertEqual(self.strategy.metrics.tables_processed, 3)
    
    @patch('src.backup.sequential.ConnectionManager')
    def test_table_failure_handling(self, mock_connection_manager):
        """Test handling of table processing failures"""
        # Setup mocks
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        with patch.object(self.strategy, 'validate_table_exists', return_value=True), \
             patch.object(self.strategy, 'get_table_row_count', return_value=50), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM table"), \
             patch.object(self.strategy, 'process_batch', side_effect=[True, False, True]), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute backup (should fail on second table)
            result = self.strategy.execute(['table1', 'table2', 'table3'])
            
            # Verify partial failure
            self.assertFalse(result)
            self.assertGreater(self.strategy.metrics.errors, 0)
    
    def test_retry_mechanism(self):
        """Test batch retry mechanism"""
        test_data = create_mock_batch_data(5)
        
        # Mock process_batch to fail twice then succeed
        with patch.object(self.strategy, 'process_batch', side_effect=[False, False, True]):
            result = self.strategy._process_batch_with_retries(
                test_data, 'test_table', 1, '2024-01-01 12:00:00'
            )
            
            self.assertTrue(result)
            # Verify it was called 3 times (2 failures + 1 success)
            self.assertEqual(self.strategy.process_batch.call_count, 3)
    
    def test_strategy_info(self):
        """Test strategy information"""
        info = self.strategy.get_strategy_info()
        
        self.assertIn('name', info)
        self.assertIn('description', info)
        self.assertIn('advantages', info)
        self.assertIn('disadvantages', info)
        self.assertIn('best_for', info)
        self.assertIn('configuration', info)
        self.assertEqual(info['name'], 'Sequential Backup Strategy')
    
    def test_completion_time_estimate(self):
        """Test completion time estimation"""
        tables = ['table1', 'table2', 'table3']
        estimate = self.strategy.estimate_completion_time(tables)
        
        self.assertIn('total_tables', estimate)
        self.assertIn('estimated_duration_minutes', estimate)
        self.assertIn('strategy', estimate)
        self.assertEqual(estimate['total_tables'], 3)
        self.assertEqual(estimate['strategy'], 'sequential')


class TestInterTableBackupStrategy(BaseBackupStrategyTest):
    """Test inter-table parallel backup strategy"""
    
    def setUp(self):
        super().setUp()
        self.strategy = InterTableBackupStrategy(self.config)
        
        # Mock dependencies
        self.strategy.watermark_manager = MagicMock()
        self.strategy.watermark_manager.get_last_watermark.return_value = "2024-01-01 09:00:00"
        self.strategy.s3_manager = MagicMock()
        self.strategy.data_validator = MagicMock()
        
        # Mock validation to always pass
        mock_validation_result = MagicMock()
        mock_validation_result.is_valid.return_value = True
        mock_validation_result.errors = []
        mock_validation_result.warnings = []
        self.strategy.data_validator.validate_dataframe.return_value = mock_validation_result
    
    @patch('src.backup.inter_table.ConnectionManager')
    def test_parallel_table_processing(self, mock_connection_manager):
        """Test parallel processing of multiple tables"""
        # Setup mocks for each thread
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        tables = ['table1', 'table2', 'table3', 'table4']
        
        with patch.object(self.strategy, 'validate_table_exists', return_value=True), \
             patch.object(self.strategy, 'get_table_row_count', return_value=50), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM table"), \
             patch.object(self.strategy, 'process_batch', return_value=True), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute parallel backup
            result = self.strategy.execute(tables)
            
            # Verify results
            self.assertTrue(result)
            self.assertEqual(self.strategy.metrics.tables_processed, 4)
    
    @patch('src.backup.inter_table.ConnectionManager')
    def test_parallel_error_handling(self, mock_connection_manager):
        """Test error handling in parallel processing"""
        # Setup mocks where one table fails
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        def mock_validate_table_exists(cursor, table_name):
            # Fail validation for table2
            return table_name != 'table2'
        
        tables = ['table1', 'table2', 'table3']
        
        with patch.object(self.strategy, 'validate_table_exists', side_effect=mock_validate_table_exists), \
             patch.object(self.strategy, 'get_table_row_count', return_value=50), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM table"), \
             patch.object(self.strategy, 'process_batch', return_value=True), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute backup (should partially fail)
            result = self.strategy.execute(tables)
            
            # Verify partial failure
            self.assertFalse(result)
            self.assertGreater(self.strategy.metrics.errors, 0)
    
    def test_efficiency_calculation(self):
        """Test parallelization efficiency calculation"""
        # Simulate some completed tables
        self.strategy.metrics.per_table_metrics = {
            'table1': {'duration': 10.0, 'rows': 1000},
            'table2': {'duration': 15.0, 'rows': 1500},
            'table3': {'duration': 8.0, 'rows': 800}
        }
        self.strategy.metrics.start_time = time.time() - 20.0  # Total 20 seconds
        self.strategy.metrics.end_time = time.time()
        
        tables = ['table1', 'table2', 'table3']
        efficiency = self.strategy._calculate_efficiency(tables)
        
        self.assertIn('efficiency_percent', efficiency)
        self.assertIn('theoretical_speedup', efficiency)
        self.assertIn('max_possible_speedup', efficiency)
        self.assertGreater(efficiency['theoretical_speedup'], 1.0)
    
    def test_strategy_info(self):
        """Test strategy information"""
        info = self.strategy.get_strategy_info()
        
        self.assertIn('name', info)
        self.assertEqual(info['name'], 'Inter-Table Parallel Backup Strategy')
        self.assertIn('advantages', info)
        self.assertIn('Faster processing for multiple tables', info['advantages'])
    
    def test_completion_time_estimate(self):
        """Test parallel completion time estimation"""
        tables = ['table1', 'table2', 'table3', 'table4']
        estimate = self.strategy.estimate_completion_time(tables)
        
        self.assertIn('parallel_duration_minutes', estimate)
        self.assertIn('estimated_speedup', estimate)
        self.assertEqual(estimate['strategy'], 'inter_table_parallel')
        self.assertGreater(estimate['estimated_speedup'], 1.0)


class TestIntraTableBackupStrategy(BaseBackupStrategyTest):
    """Test intra-table parallel backup strategy"""
    
    def setUp(self):
        super().setUp()
        self.strategy = IntraTableBackupStrategy(self.config)
        
        # Mock dependencies
        self.strategy.watermark_manager = MagicMock()
        self.strategy.watermark_manager.get_last_watermark.return_value = "2024-01-01 09:00:00"
        self.strategy.s3_manager = MagicMock()
        self.strategy.data_validator = MagicMock()
        
        # Mock validation to always pass
        mock_validation_result = MagicMock()
        mock_validation_result.is_valid.return_value = True
        mock_validation_result.errors = []
        mock_validation_result.warnings = []
        self.strategy.data_validator.validate_dataframe.return_value = mock_validation_result
    
    def test_time_chunk_calculation(self):
        """Test time chunk calculation for large tables"""
        with patch.object(self.strategy, 'calculate_time_chunks') as mock_calculate_chunks:
            # Mock time chunks
            mock_calculate_chunks.return_value = [
                ("2024-01-01 09:00:00", "2024-01-01 10:00:00"),
                ("2024-01-01 10:00:00", "2024-01-01 11:00:00"),
                ("2024-01-01 11:00:00", "2024-01-01 12:00:00")
            ]
            
            chunks = mock_calculate_chunks("large_table", "2024-01-01 09:00:00", "2024-01-01 12:00:00", 3)
            
            self.assertEqual(len(chunks), 3)
            self.assertEqual(chunks[0][0], "2024-01-01 09:00:00")
            self.assertEqual(chunks[-1][1], "2024-01-01 12:00:00")
    
    @patch('src.backup.intra_table.ConnectionManager')
    def test_chunk_parallel_processing(self, mock_connection_manager):
        """Test parallel processing of time chunks"""
        # Setup mocks
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        # Mock time chunks
        time_chunks = [
            ("2024-01-01 09:00:00", "2024-01-01 10:00:00"),
            ("2024-01-01 10:00:00", "2024-01-01 11:00:00")
        ]
        
        with patch.object(self.strategy, 'calculate_time_chunks', return_value=time_chunks), \
             patch.object(self.strategy, 'validate_table_exists', return_value=True), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM table"), \
             patch.object(self.strategy, 'process_batch', return_value=True), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute backup
            result = self.strategy.execute(['large_table'])
            
            # Verify results
            self.assertTrue(result)
            self.assertEqual(self.strategy.metrics.tables_processed, 1)
    
    @patch('src.backup.intra_table.ConnectionManager')  
    def test_chunk_failure_handling(self, mock_connection_manager):
        """Test handling of chunk processing failures"""
        # Setup mocks where one chunk fails
        mock_conn = self.create_mock_db_connection()
        mock_connection_manager.return_value.get_database_connection.return_value.__enter__.return_value = mock_conn
        
        time_chunks = [
            ("2024-01-01 09:00:00", "2024-01-01 10:00:00"),
            ("2024-01-01 10:00:00", "2024-01-01 11:00:00"),
            ("2024-01-01 11:00:00", "2024-01-01 12:00:00")
        ]
        
        # Mock process_batch to fail for middle chunk
        call_count = 0
        def mock_process_batch(*args):
            nonlocal call_count
            call_count += 1
            # Fail on calls 3-4 (second chunk has 2 batches)
            return call_count not in [3, 4]
        
        with patch.object(self.strategy, 'calculate_time_chunks', return_value=time_chunks), \
             patch.object(self.strategy, 'validate_table_exists', return_value=True), \
             patch.object(self.strategy, 'get_incremental_query', return_value="SELECT * FROM table"), \
             patch.object(self.strategy, 'process_batch', side_effect=mock_process_batch), \
             patch.object(self.strategy, 'update_watermarks', return_value=True):
            
            # Execute backup (should fail due to chunk failure)
            result = self.strategy.execute(['large_table'])
            
            # Verify failure
            self.assertFalse(result)
            self.assertGreater(self.strategy.metrics.errors, 0)
    
    def test_single_chunk_fallback(self):
        """Test fallback to single chunk for small time windows"""
        # Mock small time window resulting in single chunk
        time_chunks = [("2024-01-01 09:00:00", "2024-01-01 09:30:00")]
        
        with patch.object(self.strategy, 'calculate_time_chunks', return_value=time_chunks):
            chunks = self.strategy.calculate_time_chunks(
                "small_table", "2024-01-01 09:00:00", "2024-01-01 09:30:00", 4
            )
            
            self.assertEqual(len(chunks), 1)
    
    def test_strategy_info(self):
        """Test strategy information"""
        info = self.strategy.get_strategy_info()
        
        self.assertIn('name', info)
        self.assertEqual(info['name'], 'Intra-Table Parallel Backup Strategy')
        self.assertIn('advantages', info)
        self.assertIn('Handles very large tables efficiently', info['advantages'])
        self.assertIn('configuration', info)
        self.assertIn('num_chunks', info['configuration'])
    
    def test_completion_time_estimate_single_table(self):
        """Test completion time estimation for single table"""
        estimate = self.strategy.estimate_completion_time(['large_table'])
        
        self.assertIn('parallel_duration_minutes', estimate)
        self.assertIn('estimated_speedup', estimate)
        self.assertIn('chunks', estimate)
        self.assertEqual(estimate['strategy'], 'intra_table_parallel')
        self.assertEqual(estimate['total_tables'], 1)
    
    def test_completion_time_estimate_multiple_tables(self):
        """Test completion time estimation for multiple tables"""
        tables = ['table1', 'table2', 'table3']
        estimate = self.strategy.estimate_completion_time(tables)
        
        self.assertIn('total_duration_minutes', estimate)
        self.assertIn('chunks_per_table', estimate)
        self.assertEqual(estimate['total_tables'], 3)
        self.assertEqual(estimate['strategy'], 'intra_table_parallel')


class TestBackupStrategyComparison(BaseBackupStrategyTest):
    """Test comparison between backup strategies"""
    
    def setUp(self):
        super().setUp()
        self.strategies = {
            'sequential': SequentialBackupStrategy(self.config),
            'inter_table': InterTableBackupStrategy(self.config),
            'intra_table': IntraTableBackupStrategy(self.config)
        }
    
    def test_strategy_info_consistency(self):
        """Test that all strategies provide consistent info structure"""
        required_keys = {'name', 'description', 'advantages', 'disadvantages', 'best_for', 'configuration'}
        
        for strategy_name, strategy in self.strategies.items():
            with self.subTest(strategy=strategy_name):
                info = strategy.get_strategy_info()
                
                # Check all required keys are present
                self.assertTrue(required_keys.issubset(info.keys()), 
                              f"Strategy {strategy_name} missing keys: {required_keys - info.keys()}")
                
                # Check types
                self.assertIsInstance(info['advantages'], list)
                self.assertIsInstance(info['disadvantages'], list)
                self.assertIsInstance(info['best_for'], list)
                self.assertIsInstance(info['configuration'], dict)
    
    def test_completion_time_estimates(self):
        """Test completion time estimates for all strategies"""
        tables = ['table1', 'table2', 'table3']
        
        for strategy_name, strategy in self.strategies.items():
            with self.subTest(strategy=strategy_name):
                estimate = strategy.estimate_completion_time(tables)
                
                # Check required keys
                self.assertIn('total_tables', estimate)
                self.assertIn('strategy', estimate)
                self.assertEqual(estimate['total_tables'], 3)
                
                # Check strategy-specific keys
                if strategy_name == 'sequential':
                    self.assertIn('estimated_duration_minutes', estimate)
                elif strategy_name == 'inter_table':
                    self.assertIn('parallel_duration_minutes', estimate)
                    self.assertIn('estimated_speedup', estimate)
                elif strategy_name == 'intra_table':
                    self.assertIn('total_duration_minutes', estimate)


def run_backup_strategy_tests():
    """Run all backup strategy tests"""
    print("🧪 Running Backup Strategy Tests\n")
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestSequentialBackupStrategy,
        TestInterTableBackupStrategy, 
        TestIntraTableBackupStrategy,
        TestBackupStrategyComparison
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print("📊 BACKUP STRATEGY TEST SUMMARY")
    print(f"{'='*60}")
    
    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    passed = total_tests - failures - errors
    
    print(f"Total Tests: {total_tests}")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failures}")
    print(f"💥 Errors: {errors}")
    print(f"Success Rate: {(passed/total_tests)*100:.1f}%")
    
    if failures > 0:
        print(f"\n⚠️  FAILURES:")
        for test, traceback in result.failures:
            print(f"   - {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if errors > 0:
        print(f"\n💥 ERRORS:")
        for test, traceback in result.errors:
            print(f"   - {test}: {traceback.split('Exception:')[-1].strip()}")
    
    success = failures == 0 and errors == 0
    if success:
        print(f"\n🎉 All backup strategy tests passed!")
    else:
        print(f"\n⚠️  Some tests failed. Review the output above.")
    
    return success


if __name__ == "__main__":
    success = run_backup_strategy_tests()
    sys.exit(0 if success else 1)