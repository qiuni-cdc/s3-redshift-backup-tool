"""
Unit tests for batch size hierarchy resolution.

Tests the proper hierarchy implementation:
1. Table-specific processing.batch_size (highest priority)
2. Pipeline processing.batch_size (fallback)
3. System default from AppConfig (final fallback)
"""

import pytest
from unittest.mock import Mock, patch
from src.utils.validation import resolve_batch_size


class TestBatchSizeHierarchy:
    """Test batch size hierarchy resolution logic"""

    def test_table_specific_batch_size_highest_priority(self):
        """Test that table-specific batch_size has highest priority"""
        table_config = {
            'processing': {
                'batch_size': 250000
            }
        }
        pipeline_config = {
            'processing': {
                'batch_size': 100000
            }
        }

        # Mock app config with different default
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        assert result == 250000

    def test_pipeline_batch_size_fallback(self):
        """Test that pipeline batch_size is used when table-specific is missing"""
        table_config = {
            'processing': {}  # No batch_size specified
        }
        pipeline_config = {
            'processing': {
                'batch_size': 100000
            }
        }

        # Mock app config with different default
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        assert result == 100000

    def test_system_default_final_fallback(self):
        """Test that system default is used when both table and pipeline are missing"""
        table_config = {
            'processing': {}  # No batch_size specified
        }
        pipeline_config = {
            'processing': {}  # No batch_size specified
        }

        # Mock app config
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        assert result == 5000000

    def test_no_processing_config_uses_system_default(self):
        """Test that missing processing config uses system default"""
        table_config = {}  # No processing section
        pipeline_config = {}  # No processing section

        # Mock app config
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        assert result == 5000000

    def test_none_pipeline_config_still_works(self):
        """Test that None pipeline_config doesn't break the function"""
        table_config = {
            'processing': {
                'batch_size': 150000
            }
        }

        # Mock app config
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=None,  # This should be handled gracefully
            app_config=mock_app_config
        )

        assert result == 150000

    def test_none_pipeline_config_falls_back_to_system_default(self):
        """Test that None pipeline_config with no table config uses system default"""
        table_config = {
            'processing': {}  # No batch_size
        }

        # Mock app config
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=None,  # This should be handled gracefully
            app_config=mock_app_config
        )

        assert result == 5000000

    @patch('src.utils.validation.AppConfig')
    def test_auto_loads_app_config_when_none(self, mock_app_config_class):
        """Test that AppConfig is automatically loaded when none provided"""
        # Setup mock
        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000
        mock_app_config_class.return_value = mock_app_config

        table_config = {
            'processing': {}  # No batch_size
        }

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=None,
            app_config=None  # Should auto-load
        )

        # Verify AppConfig was instantiated
        mock_app_config_class.assert_called_once()
        assert result == 5000000

    def test_returns_integer_type(self):
        """Test that function always returns integer type"""
        table_config = {
            'processing': {
                'batch_size': '250000'  # String input
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            app_config=mock_app_config
        )

        assert isinstance(result, int)
        assert result == 250000

    def test_handles_nested_processing_config(self):
        """Test that deeply nested processing configs work correctly"""
        table_config = {
            'other_settings': {},
            'processing': {
                'strategy': 'sequential',
                'batch_size': 75000,
                'retry_attempts': 3
            }
        }
        pipeline_config = {
            'source': 'mysql',
            'processing': {
                'parallel': True,
                'batch_size': 50000
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        assert result == 75000  # Table-specific should win


class TestBatchSizeHierarchyEdgeCases:
    """Test edge cases and error conditions"""

    def test_zero_batch_size_is_preserved(self):
        """Test that zero batch size values are preserved (caller should validate)"""
        table_config = {
            'processing': {
                'batch_size': 0
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            app_config=mock_app_config
        )

        assert result == 0

    def test_negative_batch_size_is_preserved(self):
        """Test that negative batch size values are preserved (caller should validate)"""
        table_config = {
            'processing': {
                'batch_size': -1000
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            app_config=mock_app_config
        )

        assert result == -1000

    def test_very_large_batch_size_is_preserved(self):
        """Test that very large batch sizes are preserved"""
        table_config = {
            'processing': {
                'batch_size': 50000000  # 50M rows
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            app_config=mock_app_config
        )

        assert result == 50000000

    def test_complex_config_structure(self):
        """Test with complex realistic config structures"""
        table_config = {
            'full_name': 'production.user_events',
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'id',
            'processing': {
                'strategy': 'sequential',
                'batch_size': 200000,
                'retry_attempts': 5,
                'timeout_seconds': 600
            },
            'validation': {
                'max_null_percentage': 5.0
            }
        }

        pipeline_config = {
            'name': 'production_sync',
            'source': 'mysql://prod-db',
            'target': 's3://data-lake',
            'processing': {
                'batch_size': 100000,
                'parallel_tables': 4
            },
            'monitoring': {
                'enabled': True
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        assert result == 200000  # Table-specific should win


if __name__ == '__main__':
    pytest.main([__file__])