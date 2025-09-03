"""
CDC Backup Integration for v1.2.0

Integrates the CDC Strategy Engine with existing backup strategies.
Provides seamless migration from v1.0.0/v1.1.0 hardcoded behavior to flexible CDC.
"""

from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
from datetime import datetime

from src.core.cdc_strategy_engine import (
    CDCStrategy, CDCConfig, CDCStrategyFactory, 
    CDCStrategyType, WatermarkData, create_legacy_cdc_config
)
from src.core.cdc_configuration_manager import CDCConfigurationManager, BackwardCompatibilityManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


class CDCBackupIntegration:
    """
    Integrates CDC Strategy Engine with backup strategies
    
    This class serves as the bridge between:
    - Legacy backup strategies (SequentialBackupStrategy, etc.)
    - New CDC Strategy Engine
    - Pipeline configuration system
    """
    
    def __init__(self, connection_manager=None, pipeline_config: Optional[Dict[str, Any]] = None):
        self.connection_manager = connection_manager
        self.pipeline_config = pipeline_config or {}
        self.cdc_config_manager = CDCConfigurationManager()
        self.cdc_factory = CDCStrategyFactory()
        self.compatibility_manager = BackwardCompatibilityManager()
        
        # Cache for CDC strategies (one per table)
        self._cdc_strategies: Dict[str, CDCStrategy] = {}
    
    def get_cdc_strategy(self, table_name: str, table_config: Optional[Dict[str, Any]] = None) -> CDCStrategy:
        """
        Get or create CDC strategy for a table
        
        Handles:
        - v1.2.0 explicit configuration
        - v1.1.0 compatibility with pipeline configs
        - v1.0.0 compatibility with hardcoded behavior
        """
        # Check cache first
        if table_name in self._cdc_strategies:
            return self._cdc_strategies[table_name]
        
        cdc_strategy = None
        
        try:
            # 1. Try v1.2.0 explicit table configuration
            if table_config:
                cdc_strategy = self.cdc_config_manager.create_cdc_strategy(table_config, table_name)
                logger.info(f"Created v1.2.0 CDC strategy for {table_name}", extra={
                    "strategy": cdc_strategy.strategy_name,
                    "source": "explicit_config"
                })
            
            # 2. Try pipeline configuration lookup
            elif self.pipeline_config:
                pipeline_tables = self.pipeline_config.get('tables', {})
                
                # Try exact match first
                table_cfg = None
                if table_name in pipeline_tables:
                    table_cfg = pipeline_tables[table_name]
                else:
                    # Try unscoped table name (remove schema prefix)
                    unscoped_table_name = table_name.split('.')[-1] if '.' in table_name else table_name
                    # Try with schema prefix (add back schema)
                    for config_key, config_value in pipeline_tables.items():
                        # Match unscoped names
                        config_unscoped = config_key.split('.')[-1] if '.' in config_key else config_key
                        if config_unscoped == unscoped_table_name:
                            table_cfg = config_value
                            logger.info(f"Found pipeline config for {table_name} using pattern match with {config_key}")
                            break
                
                if table_cfg:
                    cdc_strategy = self.cdc_config_manager.create_cdc_strategy(table_cfg, table_name)
                    logger.info(f"Created CDC strategy from pipeline for {table_name}", extra={
                        "strategy": cdc_strategy.strategy_name,
                        "source": "pipeline_config"
                    })
            
            # 3. Check for legacy usage pattern
            if not cdc_strategy:
                # TODO: Check watermark data to detect legacy usage
                # For now, assume legacy for unknown tables
                legacy_config = self.compatibility_manager.create_legacy_compatible_cdc_config(table_name)
                cdc_strategy = self.cdc_factory.create_strategy(legacy_config)
                logger.info(f"Created legacy compatible CDC strategy for {table_name}", extra={
                    "strategy": cdc_strategy.strategy_name,
                    "source": "legacy_compatibility"
                })
        
        except Exception as e:
            logger.error(f"Failed to create CDC strategy for {table_name}: {e}")
            # Final fallback - v1.0.0 behavior
            legacy_config = create_legacy_cdc_config()
            cdc_strategy = self.cdc_factory.create_strategy(legacy_config)
            logger.info(f"Using fallback legacy CDC strategy for {table_name}")
        
        # Cache the strategy
        self._cdc_strategies[table_name] = cdc_strategy
        return cdc_strategy
    
    def build_incremental_query(self, 
                               table_name: str, 
                               watermark: Dict[str, Any],
                               limit: int,
                               table_config: Optional[Dict[str, Any]] = None) -> str:
        """
        Build incremental query using appropriate CDC strategy
        
        This replaces the hardcoded query building in existing backup strategies.
        """
        try:
            # Get CDC strategy for this table
            cdc_strategy = self.get_cdc_strategy(table_name, table_config)
            
            # Build query using strategy
            query = cdc_strategy.build_query(table_name, watermark, limit)
            
            logger.info(f"Built incremental query for {table_name}", extra={
                "table": table_name,
                "strategy": cdc_strategy.strategy_name,
                "limit": limit,
                "watermark_present": bool(watermark)
            })
            
            return query
            
        except Exception as e:
            logger.error(f"Failed to build incremental query for {table_name}: {e}")
            # Fallback to legacy v1.0.0 behavior
            return self._build_legacy_query(table_name, watermark, limit)
    
    def extract_watermark_from_batch(self,
                                   table_name: str,
                                   batch_data: List[Dict[str, Any]],
                                   table_config: Optional[Dict[str, Any]] = None) -> WatermarkData:
        """
        Extract watermark data from processed batch using appropriate CDC strategy
        """
        try:
            # Get CDC strategy for this table
            cdc_strategy = self.get_cdc_strategy(table_name, table_config)
            
            # Extract watermark using strategy
            watermark_data = cdc_strategy.extract_watermark_data(batch_data)
            
            logger.info(f"Extracted watermark data for {table_name}", extra={
                "table": table_name,
                "strategy": watermark_data.strategy_used,
                "rows_processed": watermark_data.row_count,
                "last_timestamp": watermark_data.last_timestamp,
                "last_id": watermark_data.last_id
            })
            
            return watermark_data
            
        except Exception as e:
            logger.error(f"Failed to extract watermark for {table_name}: {e}")
            # Fallback to basic watermark
            return WatermarkData(
                row_count=len(batch_data),
                strategy_used='fallback'
            )
    
    def validate_table_for_cdc(self,
                              table_name: str,
                              table_schema: Dict[str, str],
                              table_config: Optional[Dict[str, Any]] = None) -> Tuple[bool, List[str]]:
        """
        Validate table schema supports configured CDC strategy
        
        Returns (is_valid, list_of_issues)
        """
        issues = []
        
        try:
            # Get CDC strategy for this table
            cdc_strategy = self.get_cdc_strategy(table_name, table_config)
            
            # Validate using strategy
            is_valid = cdc_strategy.validate_table_schema(table_schema)
            
            if not is_valid:
                issues.append(f"Table schema validation failed for {cdc_strategy.strategy_name} strategy")
            
            # Additional validations
            if cdc_strategy.config.strategy == CDCStrategyType.TIMESTAMP_ONLY:
                timestamp_col = cdc_strategy.config.timestamp_column
                if timestamp_col not in table_schema:
                    issues.append(f"Missing timestamp column: {timestamp_col}")
            
            elif cdc_strategy.config.strategy == CDCStrategyType.HYBRID:
                timestamp_col = cdc_strategy.config.timestamp_column
                id_col = cdc_strategy.config.id_column
                
                if timestamp_col not in table_schema:
                    issues.append(f"Missing timestamp column: {timestamp_col}")
                if id_col not in table_schema:
                    issues.append(f"Missing ID column: {id_col}")
            
            elif cdc_strategy.config.strategy == CDCStrategyType.ID_ONLY:
                id_col = cdc_strategy.config.id_column
                if id_col not in table_schema:
                    issues.append(f"Missing ID column: {id_col}")
            
            logger.info(f"CDC validation for {table_name}", extra={
                "table": table_name,
                "strategy": cdc_strategy.strategy_name,
                "valid": is_valid,
                "issues_count": len(issues)
            })
            
            return len(issues) == 0, issues
            
        except Exception as e:
            issues.append(f"CDC validation error: {str(e)}")
            return False, issues
    
    def convert_watermark_to_legacy_format(self, watermark_data: WatermarkData) -> Dict[str, Any]:
        """
        Convert new WatermarkData format to legacy watermark format
        for backward compatibility with existing S3 watermark storage
        """
        legacy_format = {}
        
        # Map new format to legacy fields
        if watermark_data.last_timestamp:
            legacy_format['last_mysql_data_timestamp'] = watermark_data.last_timestamp
        
        if watermark_data.last_id:
            legacy_format['last_processed_id'] = watermark_data.last_id
        
        if watermark_data.row_count:
            legacy_format['mysql_row_count'] = watermark_data.row_count
        
        # Add v1.2.0 metadata while maintaining compatibility
        legacy_format['metadata'] = {
            'cdc_strategy': watermark_data.strategy_used,
            'v1_2_enhanced': True,
            **watermark_data.additional_data
        }
        
        return legacy_format
    
    def _build_legacy_query(self, table_name: str, watermark: Dict[str, Any], limit: int) -> str:
        """
        Fallback to v1.0.0/v1.1.0 hardcoded query building
        Used when CDC strategy creation fails
        """
        logger.warning(f"Using legacy query building for {table_name}")
        
        last_timestamp = watermark.get('last_mysql_data_timestamp')
        last_id = watermark.get('last_processed_id', 0)
        
        if last_timestamp:
            # Replicate v1.0.0/v1.1.0 hybrid logic (timestamp + ID)
            where_clause = f"""WHERE updated_at > '{last_timestamp}'
                OR (updated_at = '{last_timestamp}' AND ID > {last_id})"""
        else:
            where_clause = ""
        
        query = f"""
        SELECT * FROM {table_name}
        {where_clause}
        ORDER BY updated_at, ID
        LIMIT {limit}
        """.strip()
        
        return query
    
    def get_cdc_strategy_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about CDC strategy for a table"""
        if table_name not in self._cdc_strategies:
            return {"error": "No CDC strategy configured"}
        
        cdc_strategy = self._cdc_strategies[table_name]
        config = cdc_strategy.config
        
        return {
            "strategy": config.strategy.value,
            "timestamp_column": config.timestamp_column,
            "id_column": config.id_column,
            "ordering_columns": config.ordering_columns,
            "batch_size": config.batch_size,
            "custom_query": bool(config.custom_query)
        }
    
    def clear_strategy_cache(self):
        """Clear cached CDC strategies (for testing or reconfiguration)"""
        self._cdc_strategies.clear()
        logger.info("Cleared CDC strategy cache")


# Utility functions for integrating with existing backup strategies
def create_cdc_integration(connection_manager=None, pipeline_config: Optional[Dict[str, Any]] = None) -> CDCBackupIntegration:
    """Factory function to create CDC integration instance"""
    return CDCBackupIntegration(connection_manager, pipeline_config)


def upgrade_legacy_backup_strategy(backup_strategy, pipeline_config: Optional[Dict[str, Any]] = None):
    """
    Upgrade existing backup strategy to use CDC Strategy Engine
    
    This can be used to enhance SequentialBackupStrategy, InterTableBackupStrategy, etc.
    without major refactoring.
    """
    # Add CDC integration to backup strategy
    if not hasattr(backup_strategy, 'cdc_integration'):
        backup_strategy.cdc_integration = create_cdc_integration(
            backup_strategy.connection_manager if hasattr(backup_strategy, 'connection_manager') else None,
            pipeline_config
        )
        logger.info(f"Upgraded {backup_strategy.__class__.__name__} with CDC Strategy Engine")
    
    return backup_strategy