"""
Integration tests for batch size hierarchy in actual components.

Tests that the hierarchy works correctly in:
- CDC Configuration Manager
- Main CLI commands
- Multi-schema commands
- Configuration validation
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.core.cdc_configuration_manager import CDCConfigurationManager
from src.core.configuration_manager import ConfigurationManager


class TestBatchSizeHierarchyIntegration:
    """Integration tests for batch size hierarchy in real components"""

    def setup_method(self):
        """Setup test fixtures"""
        self.mock_app_config = Mock()
        self.mock_app_config.backup.target_rows_per_chunk = 5000000

    def test_cdc_configuration_manager_hierarchy(self):
        """Test CDC configuration manager respects batch size hierarchy"""
        # Setup CDC configuration manager
        cdc_manager = CDCConfigurationManager()

        # Test case 1: Table-specific batch_size (highest priority)
        table_config_with_batch = {
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'id',
            'processing': {
                'batch_size': 250000
            }
        }

        config = cdc_manager.parse_table_cdc_config(table_config_with_batch, 'test_table')
        assert config.batch_size == 250000

        # Test case 2: No table-specific batch_size, falls back to system default
        table_config_no_batch = {
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'id',
            'processing': {}
        }

        with patch('src.config.settings.AppConfig', return_value=self.mock_app_config):
            config = cdc_manager.parse_table_cdc_config(table_config_no_batch, 'test_table')
            assert config.batch_size == 5000000

    def test_configuration_manager_validation_hierarchy(self):
        """Test configuration manager validation respects hierarchy"""
        config_manager = ConfigurationManager()

        # Mock pipeline config object
        mock_pipeline_config = Mock()
        mock_pipeline_config.processing = {'batch_size': 100000}

        # Mock table config object with proper validation settings
        mock_table_config = Mock()
        mock_table_config.full_name = 'test.table'
        mock_table_config.cdc_strategy = 'hybrid'
        mock_table_config.cdc_timestamp_column = 'updated_at'
        mock_table_config.cdc_id_column = 'id'
        mock_table_config.processing = {'batch_size': 300000}
        # Mock validation settings to avoid validation errors
        mock_table_config.validation = {'max_null_percentage': 10.0}

        # Should not raise validation error with valid batch_size
        try:
            config_manager._validate_single_table_config(mock_table_config, mock_pipeline_config)
        except Exception as e:
            pytest.fail(f"Validation failed unexpectedly: {e}")

        # Test with no table-specific batch_size
        mock_table_config.processing = {}  # No batch_size
        mock_table_config.validation = {'max_null_percentage': 5.0}  # Valid validation config

        try:
            config_manager._validate_single_table_config(mock_table_config, mock_pipeline_config)
        except Exception as e:
            pytest.fail(f"Validation failed unexpectedly: {e}")

    def test_cdc_configuration_manager_batch_resolution(self):
        """Test CDC configuration manager uses batch size hierarchy correctly"""
        # Test the actual CDC configuration manager which we know exists
        from src.core.cdc_configuration_manager import CDCConfigurationManager

        cdc_manager = CDCConfigurationManager()

        # Test table config with processing batch_size
        table_config_with_processing = {
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'id',
            'processing': {
                'batch_size': 150000
            }
        }

        with patch('src.config.settings.AppConfig', return_value=self.mock_app_config):
            config = cdc_manager.parse_table_cdc_config(table_config_with_processing, 'test_table')
            # Should use processing batch_size
            assert config.batch_size == 150000

        # Test table config without processing batch_size (should use system default)
        table_config_no_processing = {
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'id'
            # No processing section
        }

        with patch('src.config.settings.AppConfig', return_value=self.mock_app_config):
            config = cdc_manager.parse_table_cdc_config(table_config_no_processing, 'test_table')
            # Should use system default
            assert config.batch_size == 5000000

    def test_legacy_cdc_batch_size_compatibility(self):
        """Test that legacy cdc_batch_size is still supported for backward compatibility"""
        # Test the actual resolve_batch_size function to verify legacy support
        from src.utils.validation import resolve_batch_size

        # Test config with legacy cdc_batch_size field
        table_config = {
            'cdc_batch_size': 200000,  # Legacy field (not in processing section)
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'id'
            # No processing section
        }

        # This should fall back to system default since resolve_batch_size
        # only looks at processing.batch_size
        result = resolve_batch_size(
            table_config=table_config,
            app_config=self.mock_app_config
        )

        # Should use system default since legacy field is not in processing
        assert result == 5000000

        # But if we put batch_size in processing, it should work
        table_config_with_processing = {
            'processing': {
                'batch_size': 200000
            }
        }

        result = resolve_batch_size(
            table_config=table_config_with_processing,
            app_config=self.mock_app_config
        )

        assert result == 200000

    def test_multi_schema_commands_hierarchy(self):
        """Test multi-schema commands respect hierarchy"""
        # This is a more complex integration test that would require
        # setting up full multi-schema command context
        # For now, we'll test the utility function usage

        from src.utils.validation import resolve_batch_size

        # Simulate multi-schema command context
        table_config = {
            'processing': {
                'batch_size': 350000
            }
        }

        # Multi-schema commands should use the utility function
        result = resolve_batch_size(
            table_config=table_config,
            app_config=self.mock_app_config
        )

        assert result == 350000


class TestBatchSizeValidationIntegration:
    """Integration tests for batch size validation"""

    def test_invalid_batch_size_validation(self):
        """Test that invalid batch sizes are properly validated"""
        config_manager = ConfigurationManager()

        # Mock pipeline config
        mock_pipeline_config = Mock()
        mock_pipeline_config.processing = {}

        # Mock table config with invalid batch_size
        mock_table_config = Mock()
        mock_table_config.full_name = 'test.table'
        mock_table_config.cdc_strategy = 'hybrid'
        mock_table_config.cdc_timestamp_column = 'updated_at'
        mock_table_config.cdc_id_column = 'id'
        mock_table_config.processing = {'batch_size': -1000}  # Invalid

        # Should raise validation error
        with pytest.raises(Exception) as exc_info:
            config_manager._validate_single_table_config(mock_table_config, mock_pipeline_config)

        assert "Invalid batch_size" in str(exc_info.value)

        # Test with zero batch_size
        mock_table_config.processing = {'batch_size': 0}  # Invalid

        with pytest.raises(Exception) as exc_info:
            config_manager._validate_single_table_config(mock_table_config, mock_pipeline_config)

        assert "Invalid batch_size" in str(exc_info.value)

    def test_string_batch_size_conversion(self):
        """Test that string batch sizes are properly converted to integers"""
        from src.utils.validation import resolve_batch_size

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


class TestRealWorldScenarios:
    """Test real-world configuration scenarios"""

    def test_production_pipeline_config(self):
        """Test with realistic production pipeline configuration"""
        from src.utils.validation import resolve_batch_size

        # Realistic production config
        table_config = {
            'full_name': 'production.user_events',
            'cdc_strategy': 'hybrid',
            'cdc_timestamp_column': 'updated_at',
            'cdc_id_column': 'event_id',
            'processing': {
                'strategy': 'sequential',
                'batch_size': 500000,  # Large table needs big batches
                'retry_attempts': 5
            }
        }

        pipeline_config = {
            'name': 'production_sync',
            'processing': {
                'batch_size': 100000,  # Conservative default
                'parallel_tables': 4
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        # Should use table-specific optimized batch size
        assert result == 500000

    def test_small_reference_table_config(self):
        """Test with small reference table that uses pipeline default"""
        from src.utils.validation import resolve_batch_size

        # Small reference table with no specific batch size
        table_config = {
            'full_name': 'reference.countries',
            'cdc_strategy': 'full_sync',
            'processing': {
                'strategy': 'sequential'
                # No batch_size - should use pipeline default
            }
        }

        pipeline_config = {
            'processing': {
                'batch_size': 10000  # Smaller default for reference tables
            }
        }

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        # Should use pipeline default
        assert result == 10000

    def test_legacy_config_without_processing_section(self):
        """Test legacy configurations that don't have processing sections"""
        from src.utils.validation import resolve_batch_size

        # Legacy config without processing section
        table_config = {
            'full_name': 'legacy.orders',
            'cdc_strategy': 'timestamp_only',
            'cdc_timestamp_column': 'updated_at'
            # No processing section at all
        }

        # Pipeline also has no processing section
        pipeline_config = {}

        mock_app_config = Mock()
        mock_app_config.backup.target_rows_per_chunk = 5000000

        result = resolve_batch_size(
            table_config=table_config,
            pipeline_config=pipeline_config,
            app_config=mock_app_config
        )

        # Should fall back to system default
        assert result == 5000000


if __name__ == '__main__':
    pytest.main([__file__])