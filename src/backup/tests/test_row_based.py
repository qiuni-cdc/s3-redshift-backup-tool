"""
Comprehensive unit tests for row-based backup strategy.

Tests the RowBasedBackupStrategy implementation including:
- Chunk processing with timestamp + ID pagination
- Watermark management and resume capabilities
- Error handling and retry logic
- Memory management integration
- Exact row count verification
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timedelta

from src.backup.row_based import RowBasedBackupStrategy
from src.config.settings import AppConfig
from src.utils.exceptions import BackupError, DatabaseError


class TestRowBasedBackupStrategy(unittest.TestCase):
    """Test cases for RowBasedBackupStrategy"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Mock configuration
        self.mock_config = Mock(spec=AppConfig)
        self.mock_config.backup = Mock()
        self.mock_config.backup.target_rows_per_chunk = 5000000
        self.mock_config.backup.batch_size = 10000
        self.mock_config.backup.retry_attempts = 3
        self.mock_config.backup.memory_limit_mb = 4096
        self.mock_config.backup.gc_threshold = 1000
        self.mock_config.backup.memory_check_interval = 10
        
        # Mock dependencies with patch decorators
        self.patches = [
            patch('src.backup.row_based.BaseBackupStrategy.__init__'),
            patch('src.backup.row_based.get_backup_logger'),
        ]
        
        for p in self.patches:
            p.start()
        
        # Create strategy instance
        self.strategy = RowBasedBackupStrategy(self.mock_config)
        
        # Mock required attributes from BaseBackupStrategy
        self.strategy.config = self.mock_config
        self.strategy.logger = Mock()
        self.strategy.logger.logger = Mock()
        self.strategy.logger.set_context = Mock()
        self.strategy.logger.table_started = Mock()
        self.strategy.logger.table_completed = Mock()
        self.strategy.logger.table_failed = Mock()
        self.strategy.logger.error_occurred = Mock()
        
        self.strategy.watermark_manager = Mock()
        self.strategy.memory_manager = Mock()
        self.strategy.database_session = Mock()
        self.strategy.validate_table_exists = Mock(return_value=True)
        self.strategy.process_batch = Mock(return_value=True)
        self.strategy.update_watermarks = Mock()
        self.strategy.force_gc_if_needed = Mock()
        self.strategy.check_memory_usage = Mock(return_value=True)
    
    def tearDown(self):
        """Clean up patches"""
        for p in self.patches:
            p.stop()
    
    def test_init_sets_correct_context(self):
        """Test that initialization sets correct logger context"""
        self.strategy.logger.set_context.assert_called_with(
            strategy="row_based", 
            chunking_type="timestamp_id"
        )
    
    def test_validate_required_columns_success(self):
        """Test successful column validation"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            {'Field': 'ID'}, 
            {'Field': 'update_at'}, 
            {'Field': 'name'}
        ]
        
        result = self.strategy._validate_required_columns(mock_cursor, "test_table")
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_with("DESCRIBE test_table")
        self.strategy.logger.logger.info.assert_called()
    
    def test_validate_required_columns_missing_update_at(self):
        """Test column validation with missing update_at column"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            {'Field': 'ID'}, 
            {'Field': 'name'},
            {'Field': 'created_at'}
        ]
        
        result = self.strategy._validate_required_columns(mock_cursor, "test_table")
        
        self.assertFalse(result)
        self.strategy.logger.logger.error.assert_called()
    
    def test_validate_required_columns_missing_id(self):
        """Test column validation with missing ID column"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            {'Field': 'update_at'}, 
            {'Field': 'name'},
            {'Field': 'created_at'}
        ]
        
        result = self.strategy._validate_required_columns(mock_cursor, "test_table")
        
        self.assertFalse(result)
        self.strategy.logger.logger.error.assert_called()
    
    def test_validate_required_columns_tuple_cursor(self):
        """Test column validation with tuple cursor results"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ('ID', 'int', 'NO', 'PRI', None, 'auto_increment'), 
            ('update_at', 'timestamp', 'NO', '', None, ''),
            ('name', 'varchar(255)', 'YES', '', None, '')
        ]
        
        result = self.strategy._validate_required_columns(mock_cursor, "test_table")
        
        self.assertTrue(result)
    
    def test_get_next_chunk_with_data(self):
        """Test _get_next_chunk with available data"""
        mock_cursor = Mock()
        mock_data = [
            {
                'ID': 1001, 
                'update_at': datetime(2025, 8, 20, 10, 0, 0),
                'name': 'test1'
            },
            {
                'ID': 1002, 
                'update_at': datetime(2025, 8, 20, 10, 5, 0),
                'name': 'test2'
            }
        ]
        mock_cursor.fetchall.return_value = mock_data
        mock_cursor.nextset.side_effect = StopIteration  # No more result sets
        
        chunk_data, last_timestamp, last_id = self.strategy._get_next_chunk(
            mock_cursor, "test_table", "2025-08-20 09:00:00", 1000, 10000
        )
        
        self.assertEqual(len(chunk_data), 2)
        self.assertEqual(last_timestamp, "2025-08-20 10:05:00")
        self.assertEqual(last_id, 1002)
        
        # Verify SQL query construction
        expected_query = """
            SELECT * FROM test_table
            WHERE (update_at > '2025-08-20 09:00:00' 
               OR (update_at = '2025-08-20 09:00:00' AND ID > 1000))
            ORDER BY update_at, ID 
            LIMIT 10000
            """
        mock_cursor.execute.assert_called_once()
        actual_query = mock_cursor.execute.call_args[0][0]
        
        # Normalize whitespace for comparison
        normalized_actual = ' '.join(actual_query.split())
        normalized_expected = ' '.join(expected_query.split())
        self.assertEqual(normalized_actual, normalized_expected)
    
    def test_get_next_chunk_no_data(self):
        """Test _get_next_chunk with no available data"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.nextset.side_effect = StopIteration
        
        chunk_data, last_timestamp, last_id = self.strategy._get_next_chunk(
            mock_cursor, "test_table", "2025-08-20 10:00:00", 1000, 10000
        )
        
        self.assertEqual(len(chunk_data), 0)
        self.assertEqual(last_timestamp, "2025-08-20 10:00:00")
        self.assertEqual(last_id, 1000)
    
    def test_get_next_chunk_database_error(self):
        """Test _get_next_chunk handling database errors"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database connection lost")
        
        with self.assertRaises(DatabaseError):
            self.strategy._get_next_chunk(
                mock_cursor, "test_table", "2025-08-20 10:00:00", 1000, 10000
            )
        
        self.strategy.logger.logger.error.assert_called()
    
    def test_update_chunk_watermark(self):
        """Test chunk watermark update functionality"""
        table_name = "test_table"
        last_timestamp = "2025-08-20 10:30:00"
        last_id = 5000
        rows_processed = 1000
        total_rows = 15000
        
        self.strategy._update_chunk_watermark(
            table_name, last_timestamp, last_id, rows_processed, total_rows
        )
        
        # Verify watermark manager is called with correct parameters
        self.strategy.watermark_manager.update_mysql_stage.assert_called_once()
        call_args = self.strategy.watermark_manager.update_mysql_stage.call_args
        
        self.assertEqual(call_args[1]['table_name'], table_name)
        self.assertEqual(call_args[1]['rows_extracted'], total_rows)
        self.assertEqual(call_args[1]['status'], 'in_progress')
        self.assertIsInstance(call_args[1]['extraction_time'], datetime)
        self.assertIsInstance(call_args[1]['max_data_timestamp'], datetime)
    
    def test_update_chunk_watermark_exception_handling(self):
        """Test chunk watermark update with exception handling"""
        self.strategy.watermark_manager.update_mysql_stage.side_effect = Exception("S3 error")
        
        # Should not raise exception, just log warning
        self.strategy._update_chunk_watermark(
            "test_table", "2025-08-20 10:30:00", 5000, 1000, 15000
        )
        
        self.strategy.logger.logger.warning.assert_called()
    
    def test_process_batch_with_retries_success_first_attempt(self):
        """Test batch processing succeeds on first attempt"""
        batch_data = [{'ID': 1, 'name': 'test'}]
        table_name = "test_table"
        batch_id = "chunk_1_batch_1"
        current_timestamp = "2025-08-20 10:00:00"
        
        self.strategy.process_batch.return_value = True
        
        result = self.strategy._process_batch_with_retries(
            batch_data, table_name, batch_id, current_timestamp
        )
        
        self.assertTrue(result)
        self.strategy.process_batch.assert_called_once_with(
            batch_data, table_name, batch_id, current_timestamp
        )
    
    def test_process_batch_with_retries_success_after_failures(self):
        """Test batch processing succeeds after initial failures"""
        batch_data = [{'ID': 1, 'name': 'test'}]
        table_name = "test_table"
        batch_id = "chunk_1_batch_1"
        current_timestamp = "2025-08-20 10:00:00"
        
        # Fail twice, then succeed
        self.strategy.process_batch.side_effect = [False, False, True]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.strategy._process_batch_with_retries(
                batch_data, table_name, batch_id, current_timestamp, max_retries=3
            )
        
        self.assertTrue(result)
        self.assertEqual(self.strategy.process_batch.call_count, 3)
        self.strategy.logger.logger.warning.assert_called()
    
    def test_process_batch_with_retries_all_attempts_fail(self):
        """Test batch processing when all retry attempts fail"""
        batch_data = [{'ID': 1, 'name': 'test'}]
        table_name = "test_table"
        batch_id = "chunk_1_batch_1"
        current_timestamp = "2025-08-20 10:00:00"
        
        self.strategy.process_batch.return_value = False
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.strategy._process_batch_with_retries(
                batch_data, table_name, batch_id, current_timestamp, max_retries=3
            )
        
        self.assertFalse(result)
        self.assertEqual(self.strategy.process_batch.call_count, 3)
    
    def test_process_batch_with_retries_exception_handling(self):
        """Test batch processing with exception during retry"""
        batch_data = [{'ID': 1, 'name': 'test'}]
        table_name = "test_table"
        batch_id = "chunk_1_batch_1"
        current_timestamp = "2025-08-20 10:00:00"
        
        # Exception on first attempt, success on second
        self.strategy.process_batch.side_effect = [
            Exception("Network error"),
            True
        ]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.strategy._process_batch_with_retries(
                batch_data, table_name, batch_id, current_timestamp, max_retries=3
            )
        
        self.assertTrue(result)
        self.assertEqual(self.strategy.process_batch.call_count, 2)
        self.strategy.logger.logger.warning.assert_called()
    
    @patch('src.backup.row_based.datetime')
    def test_process_single_table_row_based_no_watermark(self, mock_datetime):
        """Test processing table without existing watermark"""
        # Setup mocks
        mock_datetime.now.return_value = datetime(2025, 8, 20, 12, 0, 0)
        mock_datetime.fromisoformat.return_value = datetime(2025, 8, 20, 11, 0, 0)
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # No existing watermark
        self.strategy.watermark_manager.get_table_watermark.return_value = None
        
        # Mock chunk data
        chunk_data = [
            {
                'ID': 1, 
                'update_at': datetime(2025, 8, 20, 11, 0, 0),
                'name': 'test1'
            }
        ]
        
        with patch.object(self.strategy, '_get_next_chunk') as mock_get_chunk:
            mock_get_chunk.side_effect = [
                (chunk_data, "2025-08-20 11:00:00", 1),  # First chunk with data
                ([], "2025-08-20 11:00:00", 1)           # Second chunk - no data
            ]
            
            with patch.object(self.strategy, '_process_batch_with_retries') as mock_process_batch:
                mock_process_batch.return_value = True
                
                result = self.strategy._process_single_table_row_based(
                    mock_conn, "test_table", "2025-08-20 12:00:00", None
                )
        
        self.assertTrue(result)
        
        # Verify watermark started from 1970 (no existing watermark)
        mock_get_chunk.assert_any_call(
            mock_cursor, "test_table", "1970-01-01 00:00:00", 0, 5000000
        )
        
        # Verify final watermark update with success status
        self.strategy.update_watermarks.assert_called()
        final_call = self.strategy.update_watermarks.call_args
        self.assertEqual(final_call[1]['status'], 'success')
    
    def test_process_single_table_row_based_with_watermark_resume(self):
        """Test processing table with existing watermark for resume"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Existing watermark for resume
        mock_watermark = Mock()
        mock_watermark.last_mysql_data_timestamp = "2025-08-20 10:00:00"
        mock_watermark.last_processed_id = 5000
        self.strategy.watermark_manager.get_table_watermark.return_value = mock_watermark
        
        # Mock empty chunk (no more data)
        with patch.object(self.strategy, '_get_next_chunk') as mock_get_chunk:
            mock_get_chunk.return_value = ([], "2025-08-20 10:00:00", 5000)
            
            result = self.strategy._process_single_table_row_based(
                mock_conn, "test_table", "2025-08-20 12:00:00", None
            )
        
        self.assertTrue(result)
        
        # Verify resume from correct watermark position
        mock_get_chunk.assert_called_with(
            mock_cursor, "test_table", "2025-08-20 10:00:00", 5000, 5000000
        )
    
    def test_process_single_table_row_based_validation_failure(self):
        """Test processing table with validation failure"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Validation failure
        self.strategy.validate_table_exists.return_value = False
        
        result = self.strategy._process_single_table_row_based(
            mock_conn, "test_table", "2025-08-20 12:00:00", None
        )
        
        self.assertFalse(result)
        self.strategy.logger.logger.error.assert_called()
    
    def test_process_single_table_row_based_column_validation_failure(self):
        """Test processing table with column validation failure"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(self.strategy, '_validate_required_columns') as mock_validate:
            mock_validate.return_value = False
            
            result = self.strategy._process_single_table_row_based(
                mock_conn, "test_table", "2025-08-20 12:00:00", None
            )
        
        self.assertFalse(result)
    
    def test_process_single_table_row_based_batch_failure(self):
        """Test processing table with batch processing failure"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        self.strategy.watermark_manager.get_table_watermark.return_value = None
        
        chunk_data = [{'ID': 1, 'update_at': datetime(2025, 8, 20, 11, 0, 0), 'name': 'test1'}]
        
        with patch.object(self.strategy, '_get_next_chunk') as mock_get_chunk:
            mock_get_chunk.return_value = (chunk_data, "2025-08-20 11:00:00", 1)
            
            with patch.object(self.strategy, '_process_batch_with_retries') as mock_process_batch:
                mock_process_batch.return_value = False  # Batch processing fails
                
                result = self.strategy._process_single_table_row_based(
                    mock_conn, "test_table", "2025-08-20 12:00:00", None
                )
        
        self.assertFalse(result)
        self.strategy.logger.logger.error.assert_called()
    
    def test_process_single_table_row_based_exception_handling(self):
        """Test processing table with exception and watermark error update"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Force an exception during processing
        with patch.object(self.strategy, '_validate_required_columns') as mock_validate:
            mock_validate.side_effect = Exception("Database error")
            
            result = self.strategy._process_single_table_row_based(
                mock_conn, "test_table", "2025-08-20 12:00:00", None
            )
        
        self.assertFalse(result)
        
        # Verify error watermark update
        self.strategy.update_watermarks.assert_called()
        error_call = self.strategy.update_watermarks.call_args
        self.assertEqual(error_call[1]['status'], 'failed')
        self.assertIn('error_message', error_call[1])
        
        self.strategy.logger.error_occurred.assert_called()
    
    def test_execute_empty_tables_list(self):
        """Test execute with empty tables list"""
        result = self.strategy.execute([])
        
        self.assertFalse(result)
        self.strategy.logger.logger.warning.assert_called_with("No tables specified for backup")
    
    def test_execute_single_table_success(self):
        """Test execute with single table success"""
        mock_db_conn = Mock()
        self.strategy.database_session.return_value.__enter__.return_value = mock_db_conn
        
        with patch.object(self.strategy, '_process_single_table_row_based') as mock_process:
            mock_process.return_value = True
            
            result = self.strategy.execute(["test_table"])
        
        self.assertTrue(result)
        mock_process.assert_called_once()
        self.strategy.logger.table_completed.assert_called()
    
    def test_execute_single_table_failure(self):
        """Test execute with single table failure"""
        mock_db_conn = Mock()
        self.strategy.database_session.return_value.__enter__.return_value = mock_db_conn
        
        with patch.object(self.strategy, '_process_single_table_row_based') as mock_process:
            mock_process.return_value = False
            
            result = self.strategy.execute(["test_table"])
        
        self.assertFalse(result)
        mock_process.assert_called_once()
        self.strategy.logger.table_failed.assert_called()
    
    def test_execute_multiple_tables_mixed_results(self):
        """Test execute with multiple tables having mixed results"""
        mock_db_conn = Mock()
        self.strategy.database_session.return_value.__enter__.return_value = mock_db_conn
        
        with patch.object(self.strategy, '_process_single_table_row_based') as mock_process:
            # First table succeeds, second fails
            mock_process.side_effect = [True, False]
            
            result = self.strategy.execute(["table1", "table2"])
        
        self.assertFalse(result)  # Overall failure due to one table failing
        self.assertEqual(mock_process.call_count, 2)
    
    def test_execute_with_custom_limit(self):
        """Test execute with custom row limit parameter"""
        mock_db_conn = Mock()
        self.strategy.database_session.return_value.__enter__.return_value = mock_db_conn
        
        with patch.object(self.strategy, '_process_single_table_row_based') as mock_process:
            mock_process.return_value = True
            
            result = self.strategy.execute(["test_table"], limit=1000000)
        
        self.assertTrue(result)
        # Verify limit is passed to processing method
        mock_process.assert_called_with(mock_db_conn, "test_table", unittest.mock.ANY, 1000000)
    
    def test_execute_exception_during_processing(self):
        """Test execute with exception during table processing"""
        mock_db_conn = Mock()
        self.strategy.database_session.return_value.__enter__.return_value = mock_db_conn
        
        with patch.object(self.strategy, '_process_single_table_row_based') as mock_process:
            mock_process.side_effect = Exception("Unexpected error")
            
            result = self.strategy.execute(["test_table"])
        
        self.assertFalse(result)
        self.strategy.logger.error_occurred.assert_called()
    
    def test_memory_management_integration(self):
        """Test memory management methods are called correctly"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        self.strategy.watermark_manager.get_table_watermark.return_value = None
        
        # Create multiple chunks to test memory management
        chunk_data = [{'ID': i, 'update_at': datetime(2025, 8, 20, 11, 0, 0), 'name': f'test{i}'} 
                     for i in range(1, 11)]
        
        with patch.object(self.strategy, '_get_next_chunk') as mock_get_chunk:
            mock_get_chunk.side_effect = [
                (chunk_data, "2025-08-20 11:00:00", 10),  # First chunk
                (chunk_data, "2025-08-20 11:01:00", 20),  # Second chunk
                ([], "2025-08-20 11:01:00", 20)           # No more data
            ]
            
            with patch.object(self.strategy, '_process_batch_with_retries') as mock_process_batch:
                mock_process_batch.return_value = True
                
                result = self.strategy._process_single_table_row_based(
                    mock_conn, "test_table", "2025-08-20 12:00:00", None
                )
        
        self.assertTrue(result)
        
        # Verify memory management methods were called
        self.strategy.force_gc_if_needed.assert_called()
        self.strategy.check_memory_usage.assert_called()
    
    def test_cursor_cleanup_on_exception(self):
        """Test proper cursor cleanup when exceptions occur"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Force exception after cursor creation
        self.strategy.validate_table_exists.side_effect = Exception("Test error")
        
        result = self.strategy._process_single_table_row_based(
            mock_conn, "test_table", "2025-08-20 12:00:00", None
        )
        
        self.assertFalse(result)
        
        # Verify cursor cleanup was attempted (nextset and close)
        mock_cursor.nextset.assert_called()
        mock_cursor.close.assert_called()


class TestRowBasedIntegration(unittest.TestCase):
    """Integration tests for row-based strategy end-to-end scenarios"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        self.mock_config = Mock(spec=AppConfig)
        self.mock_config.backup = Mock()
        self.mock_config.backup.target_rows_per_chunk = 1000  # Smaller for testing
        self.mock_config.backup.batch_size = 100
        self.mock_config.backup.retry_attempts = 2
    
    @patch('src.backup.row_based.BaseBackupStrategy.__init__')
    @patch('src.backup.row_based.get_backup_logger')
    def test_complete_table_backup_simulation(self, mock_logger, mock_base_init):
        """Test complete table backup from start to finish"""
        strategy = RowBasedBackupStrategy(self.mock_config)
        
        # Setup mocks
        strategy.config = self.mock_config
        strategy.logger = Mock()
        strategy.logger.logger = Mock()
        strategy.logger.set_context = Mock()
        strategy.logger.table_started = Mock()
        strategy.logger.table_completed = Mock()
        strategy.logger.error_occurred = Mock()
        
        strategy.watermark_manager = Mock()
        strategy.memory_manager = Mock()
        strategy.database_session = Mock()
        strategy.validate_table_exists = Mock(return_value=True)
        strategy.process_batch = Mock(return_value=True)
        strategy.update_watermarks = Mock()
        strategy.force_gc_if_needed = Mock()
        strategy.check_memory_usage = Mock(return_value=True)
        
        # Mock database connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        strategy.database_session.return_value.__enter__.return_value = mock_conn
        
        # No existing watermark (fresh start)
        strategy.watermark_manager.get_table_watermark.return_value = None
        
        # Simulate table with 2500 rows (3 chunks of 1000, 1000, 500)
        chunk1_data = [
            {'ID': i, 'update_at': datetime(2025, 8, 20, 10, i % 60, 0), 'name': f'row{i}'}
            for i in range(1, 1001)
        ]
        chunk2_data = [
            {'ID': i, 'update_at': datetime(2025, 8, 20, 11, i % 60, 0), 'name': f'row{i}'}
            for i in range(1001, 2001)
        ]
        chunk3_data = [
            {'ID': i, 'update_at': datetime(2025, 8, 20, 12, i % 60, 0), 'name': f'row{i}'}
            for i in range(2001, 2501)
        ]
        
        with patch.object(strategy, '_validate_required_columns') as mock_validate:
            mock_validate.return_value = True
            
            with patch.object(strategy, '_get_next_chunk') as mock_get_chunk:
                mock_get_chunk.side_effect = [
                    (chunk1_data, "2025-08-20 10:59:00", 1000),   # Chunk 1
                    (chunk2_data, "2025-08-20 11:59:00", 2000),   # Chunk 2  
                    (chunk3_data, "2025-08-20 12:59:00", 2500),   # Chunk 3
                    ([], "2025-08-20 12:59:00", 2500)             # No more data
                ]
                
                with patch.object(strategy, '_process_batch_with_retries') as mock_process_batch:
                    mock_process_batch.return_value = True
                    
                    result = strategy.execute(["test_table"])
        
        self.assertTrue(result)
        
        # Verify all chunks were processed
        self.assertEqual(mock_get_chunk.call_count, 4)  # 3 data chunks + 1 empty
        
        # Verify batch processing was called for each chunk (10 batches per 1000-row chunk)
        expected_batches = 10 + 10 + 5  # 100 rows per batch
        self.assertEqual(mock_process_batch.call_count, expected_batches)
        
        # Verify progressive watermark updates (one per chunk)
        self.assertEqual(strategy.watermark_manager.update_mysql_stage.call_count, 3)
        
        # Verify final success watermark
        strategy.update_watermarks.assert_called()
        final_call = strategy.update_watermarks.call_args
        self.assertEqual(final_call[1]['status'], 'success')
        
        # Verify memory management was called
        strategy.force_gc_if_needed.assert_called()
        strategy.check_memory_usage.assert_called()


if __name__ == '__main__':
    unittest.main()