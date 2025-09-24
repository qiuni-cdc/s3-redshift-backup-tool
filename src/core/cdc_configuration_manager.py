"""
CDC Configuration Manager for v1.2.0

Handles parsing and validation of flexible CDC configurations from pipeline YAML files.
Supports migration from v1.1.0 configurations with backward compatibility.
"""

from typing import Dict, Any, Optional, List
import yaml
from pathlib import Path
from dataclasses import asdict
from datetime import datetime

from src.core.cdc_strategy_engine import CDCConfig, CDCStrategyType, CDCStrategyFactory
from src.utils.logging import get_logger

logger = get_logger(__name__)


class CDCConfigurationManager:
    """Manages CDC configuration parsing and validation"""
    
    def __init__(self):
        self.cdc_factory = CDCStrategyFactory()
    
    def parse_table_cdc_config(self, table_config: Dict[str, Any], table_name: str) -> CDCConfig:
        """
        Parse CDC configuration for a table from YAML config
        
        Supports both v1.1.0 and v1.2.0 configuration formats:
        
        v1.1.0 format (backward compatible):
        tables:
          customers:
            cdc_strategy: "timestamp_only"
            cdc_timestamp_column: "updated_at"
        
        v1.2.0 enhanced format:
        tables:
          customers:
            cdc_strategy: "hybrid"
            cdc_timestamp_column: "last_modified"
            cdc_id_column: "customer_id"
            cdc_ordering: ["last_modified", "customer_id"]
            batch_size: 75000
        """
        try:
            # Extract CDC strategy (default to timestamp_only for v1.0.0/v1.1.0 compatibility)
            strategy_name = table_config.get('cdc_strategy', 'timestamp_only')
            strategy_type = CDCStrategyType(strategy_name)
            
            # Parse configuration based on strategy type
            cdc_config = CDCConfig(
                strategy=strategy_type,
                timestamp_column=table_config.get('cdc_timestamp_column'),
                id_column=table_config.get('cdc_id_column'),
                ordering_columns=table_config.get('cdc_ordering'),
                custom_query=table_config.get('custom_query'),
                batch_size=table_config.get('batch_size', 50000),
                timestamp_format=table_config.get('timestamp_format', 'auto'),
                additional_where=table_config.get('additional_where')  # NEW: additional WHERE clause
            )
            
            # Handle full_sync strategy metadata
            if strategy_type == CDCStrategyType.FULL_SYNC:
                full_sync_mode = table_config.get('full_sync_mode', 'append')
                cdc_config.metadata = {'full_sync_mode': full_sync_mode}
                logger.info(f"Full sync mode for {table_name}: {full_sync_mode}")
            
            # Apply v1.0.0/v1.1.0 compatibility defaults
            if strategy_type == CDCStrategyType.TIMESTAMP_ONLY and not cdc_config.timestamp_column:
                logger.info(f"Applying v1.0.0/v1.1.0 compatibility: using 'updated_at' for {table_name}")
                cdc_config.timestamp_column = 'updated_at'
            
            # Validate configuration
            self._validate_cdc_config(cdc_config, table_name)
            
            # Enhanced logging for full_sync strategies
            log_extra = {
                "table": table_name,
                "strategy": strategy_name,
                "timestamp_column": cdc_config.timestamp_column,
                "id_column": cdc_config.id_column,
                "batch_size": cdc_config.batch_size,
                "timestamp_format": cdc_config.timestamp_format,
                "additional_where": bool(cdc_config.additional_where)  # NEW: show if additional WHERE clause used
            }
            
            # Add full_sync_mode to logs if applicable
            if strategy_type == CDCStrategyType.FULL_SYNC and cdc_config.metadata:
                log_extra["full_sync_mode"] = cdc_config.metadata.get('full_sync_mode')
            
            logger.info(f"Parsed CDC config for {table_name}", extra=log_extra)
            
            return cdc_config
            
        except Exception as e:
            logger.error(f"Failed to parse CDC config for {table_name}: {e}")
            # Fallback to v1.0.0/v1.1.0 compatibility
            logger.info(f"Falling back to v1.0.0/v1.1.0 compatibility mode for {table_name}")
            return CDCConfig(
                strategy=CDCStrategyType.TIMESTAMP_ONLY,
                timestamp_column='updated_at',
                batch_size=50000
            )
    
    def _validate_cdc_config(self, config: CDCConfig, table_name: str) -> None:
        """Validate CDC configuration"""
        try:
            # This will raise ValueError if invalid
            config.__post_init__()
            
            # Additional validation
            if config.batch_size <= 0:
                raise ValueError(f"batch_size must be positive, got: {config.batch_size}")
            
            if config.batch_size > 1000000:
                logger.warning(f"Large batch size for {table_name}: {config.batch_size}")
            
        except ValueError as e:
            raise ValueError(f"Invalid CDC configuration for {table_name}: {e}")
    
    def create_cdc_strategy(self, table_config: Dict[str, Any], table_name: str):
        """Create CDC strategy instance for a table"""
        cdc_config = self.parse_table_cdc_config(table_config, table_name)
        return self.cdc_factory.create_strategy(cdc_config)
    
    def get_supported_strategies(self) -> List[str]:
        """Get list of supported CDC strategies"""
        return self.cdc_factory.get_supported_strategies()
    
    def validate_pipeline_cdc_config(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate CDC configuration for entire pipeline
        Returns validation results with warnings/errors
        """
        results = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'tables_validated': 0,
            'strategies_used': set()
        }
        
        tables = pipeline_config.get('tables', {})
        if not tables:
            results['warnings'].append("No tables configured - pipeline will register tables dynamically")
            return results
        
        for table_name, table_config in tables.items():
            try:
                # Parse and validate CDC config
                cdc_config = self.parse_table_cdc_config(table_config, table_name)
                results['strategies_used'].add(cdc_config.strategy.value)
                results['tables_validated'] += 1
                
                # Strategy-specific warnings
                if cdc_config.strategy == CDCStrategyType.FULL_SYNC:
                    results['warnings'].append(f"{table_name}: full_sync strategy may impact performance")
                
                if cdc_config.strategy == CDCStrategyType.CUSTOM_SQL:
                    results['warnings'].append(f"{table_name}: custom_sql strategy requires manual testing")
                
            except Exception as e:
                results['valid'] = False
                results['errors'].append(f"{table_name}: {str(e)}")
        
        # Convert set to list for JSON serialization
        results['strategies_used'] = list(results['strategies_used'])
        
        logger.info("Pipeline CDC validation completed", extra={
            "valid": results['valid'],
            "tables_validated": results['tables_validated'],
            "strategies_used": results['strategies_used'],
            "warnings": len(results['warnings']),
            "errors": len(results['errors'])
        })
        
        return results
    
    def generate_cdc_config_examples(self) -> Dict[str, Dict[str, Any]]:
        """Generate example CDC configurations for documentation"""
        return {
            'timestamp_only_v1_compatible': {
                'description': 'v1.0.0/v1.1.0 compatible timestamp-only CDC',
                'config': {
                    'cdc_strategy': 'timestamp_only',
                    'cdc_timestamp_column': 'updated_at'
                }
            },
            'timestamp_only_flexible': {
                'description': 'Flexible timestamp-only CDC with custom column',
                'config': {
                    'cdc_strategy': 'timestamp_only', 
                    'cdc_timestamp_column': 'last_modified',
                    'cdc_ordering': ['last_modified'],
                    'batch_size': 75000
                }
            },
            'hybrid_robust': {
                'description': 'Hybrid CDC with timestamp + ID (most robust)',
                'config': {
                    'cdc_strategy': 'hybrid',
                    'cdc_timestamp_column': 'updated_at',
                    'cdc_id_column': 'record_id',
                    'cdc_ordering': ['updated_at', 'record_id'],
                    'batch_size': 100000
                }
            },
            'id_only_append': {
                'description': 'ID-only CDC for append-only tables',
                'config': {
                    'cdc_strategy': 'id_only',
                    'cdc_id_column': 'log_id',
                    'batch_size': 200000
                }
            },
            'full_sync_small': {
                'description': 'Full sync for small dimension tables',
                'config': {
                    'cdc_strategy': 'full_sync',
                    'batch_size': 10000
                }
            },
            'custom_sql_advanced': {
                'description': 'Custom SQL for complex incremental logic',
                'config': {
                    'cdc_strategy': 'custom_sql',
                    'custom_query': """
                        SELECT * FROM {table_name} 
                        WHERE process_date > '{last_timestamp}' 
                        AND status IN ('active', 'pending')
                        ORDER BY process_date, id
                        LIMIT {limit}
                    """,
                    'batch_size': 50000
                }
            }
        }
    
    def migrate_v1_1_to_v1_2_config(self, v1_1_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate v1.1.0 pipeline configuration to v1.2.0 format
        
        This adds explicit CDC configuration to tables that relied on 
        hardcoded 'updated_at' behavior in v1.1.0.
        """
        v1_2_config = v1_1_config.copy()
        
        # Update version
        if 'pipeline' in v1_2_config:
            v1_2_config['pipeline']['version'] = '1.2.0'
        
        # Migrate table configurations
        tables = v1_2_config.get('tables', {})
        for table_name, table_config in tables.items():
            # Add explicit CDC configuration if missing
            if 'cdc_strategy' not in table_config:
                table_config['cdc_strategy'] = 'timestamp_only'
                table_config['cdc_timestamp_column'] = 'updated_at'
                logger.info(f"Migrated {table_name} to explicit timestamp_only CDC")
        
        logger.info("Migrated pipeline configuration from v1.1.0 to v1.2.0", extra={
            "tables_migrated": len(tables),
            "version": "1.2.0"
        })
        
        return v1_2_config


class BackwardCompatibilityManager:
    """Manages backward compatibility with v1.0.0/v1.1.0 systems"""
    
    @staticmethod
    def detect_legacy_table_usage(table_name: str, watermark_data: Dict[str, Any]) -> bool:
        """Detect if table is using legacy v1.0.0/v1.1.0 watermark format"""
        # Legacy indicators:
        # - No 'cdc_strategy' in watermark metadata
        # - Uses 'last_mysql_data_timestamp' and 'last_processed_id' format
        # - Backup strategy is 'sequential' (v1.0.0/v1.1.0 default)
        
        backup_strategy = watermark_data.get('backup_strategy', '')
        has_cdc_metadata = 'cdc_strategy' in watermark_data.get('metadata', {})
        
        is_legacy = (
            backup_strategy in ['sequential', 'inter-table', 'intra-table'] and
            not has_cdc_metadata and
            'last_mysql_data_timestamp' in watermark_data
        )
        
        if is_legacy:
            logger.info(f"Detected legacy v1.0.0/v1.1.0 usage for table: {table_name}")
        
        return is_legacy
    
    @staticmethod
    def create_legacy_compatible_cdc_config(table_name: str) -> CDCConfig:
        """Create v1.0.0/v1.1.0 compatible CDC configuration"""
        logger.info(f"Creating legacy compatible CDC config for {table_name}")
        
        return CDCConfig(
            strategy=CDCStrategyType.TIMESTAMP_ONLY,
            timestamp_column='updated_at',  # v1.0.0/v1.1.0 hardcoded default
            batch_size=50000  # v1.0.0/v1.1.0 default
        )
    
    @staticmethod
    def upgrade_watermark_to_v1_2(legacy_watermark: Dict[str, Any], 
                                  cdc_config: CDCConfig) -> Dict[str, Any]:
        """Upgrade legacy watermark format to v1.2.0 format"""
        # Preserve all legacy fields for compatibility
        upgraded = legacy_watermark.copy()
        
        # Add v1.2.0 CDC metadata
        if 'metadata' not in upgraded:
            upgraded['metadata'] = {}
        
        upgraded['metadata'].update({
            'cdc_strategy': cdc_config.strategy.value,
            'cdc_timestamp_column': cdc_config.timestamp_column,
            'cdc_id_column': cdc_config.id_column,
            'v1_2_migration': True,
            'migration_timestamp': str(datetime.now())
        })
        
        return upgraded