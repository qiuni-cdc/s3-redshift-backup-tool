"""
Watermark Adapter - Compatibility Layer

This adapter allows existing components to work with the new SimpleWatermarkManager
without requiring immediate code changes. It provides a compatibility layer that
translates between the legacy API and the new v2.0 format.

This enables gradual migration while ensuring the new system works immediately.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from src.core.simple_watermark_manager import SimpleWatermarkManager

logger = logging.getLogger(__name__)


class LegacyWatermarkObject:
    """
    Mock legacy watermark object for backward compatibility.
    
    Provides the same interface as the old S3TableWatermark class
    but backed by the new v2.0 SimpleWatermarkManager.
    """
    
    def __init__(self, watermark_data: Dict[str, Any]):
        """Initialize from v2.0 watermark data."""
        self._data = watermark_data
        
        # Map v2.0 structure to legacy fields
        mysql_state = watermark_data.get('mysql_state', {})
        redshift_state = watermark_data.get('redshift_state', {})
        
        # Legacy field mappings
        self.table_name = watermark_data.get('table_name', '')
        
        # MySQL fields (legacy names)
        self.last_mysql_data_timestamp = mysql_state.get('last_timestamp')
        self.last_mysql_extraction_time = mysql_state.get('last_updated')
        self.last_processed_id = mysql_state.get('last_id')
        self.mysql_status = mysql_state.get('status', 'pending')
        
        # Redshift fields (legacy names)
        self.redshift_rows_loaded = redshift_state.get('total_rows', 0)
        self.redshift_status = redshift_state.get('status', 'pending')
        self.redshift_load_time = redshift_state.get('last_updated')
        
        # File management
        self.processed_s3_files = watermark_data.get('processed_files', [])
        
        # Other legacy fields
        self.backup_strategy = watermark_data.get('cdc_strategy', 'hybrid')
        self.last_error = mysql_state.get('error') or redshift_state.get('error')
        
        # Metadata
        self.metadata = watermark_data.get('metadata', {})


class WatermarkAdapter:
    """
    Compatibility adapter for legacy watermark API.
    
    Wraps SimpleWatermarkManager to provide the same interface
    as the legacy S3WatermarkManager.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize adapter with new watermark manager."""
        self.config = config
        self.simple_manager = SimpleWatermarkManager(config)
        self.logger = logger
        
        # Legacy compatibility flag
        self._legacy_mode = True
    
    def get_table_watermark(self, table_name: str) -> Optional[LegacyWatermarkObject]:
        """
        Get watermark using legacy API format.
        
        Returns:
            LegacyWatermarkObject that matches old S3TableWatermark interface
        """
        try:
            watermark_data = self.simple_manager.get_watermark(table_name)
            if not watermark_data:
                return None
                
            return LegacyWatermarkObject(watermark_data)
            
        except Exception as e:
            logger.error(f"Failed to get watermark for {table_name}: {e}")
            return None
    
    def _update_watermark_direct(self, table_name: str, watermark_data: Dict[str, Any]) -> bool:
        """
        Direct watermark update (legacy method).
        
        Translates legacy update format to v2.0 format.
        """
        try:
            # Extract data from legacy format
            mysql_timestamp = watermark_data.get('last_mysql_data_timestamp')
            mysql_id = watermark_data.get('last_processed_id')
            mysql_status = watermark_data.get('mysql_status', 'success')
            mysql_error = watermark_data.get('last_error') if mysql_status == 'failed' else None
            
            # Update MySQL state
            self.simple_manager.update_mysql_state(
                table_name=table_name,
                timestamp=mysql_timestamp,
                id=mysql_id,
                status=mysql_status,
                error=mysql_error
            )
            
            # Update Redshift state if present
            if 'processed_s3_files' in watermark_data:
                # Get last loaded files (difference from previous state)
                current = self.simple_manager.get_watermark(table_name)
                existing_files = set(current.get('processed_files', []))
                new_files = set(watermark_data.get('processed_s3_files', []))
                loaded_files = list(new_files - existing_files)
                
                if loaded_files:
                    redshift_status = watermark_data.get('redshift_status', 'success')
                    redshift_error = watermark_data.get('last_error') if redshift_status == 'failed' else None
                    
                    self.simple_manager.update_redshift_state(
                        table_name=table_name,
                        loaded_files=loaded_files,
                        status=redshift_status,
                        error=redshift_error
                    )
                    
                    # If legacy data includes row count, update it too
                    if 'redshift_rows_loaded' in watermark_data:
                        legacy_count = watermark_data.get('redshift_rows_loaded', 0)
                        if legacy_count > 0:
                            self.simple_manager.update_redshift_count_from_external(
                                table_name, legacy_count
                            )
            
            logger.info(f"Updated watermark via legacy API for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update watermark for {table_name}: {e}")
            return False
    
    def update_mysql_watermark(self, table_name: str, extraction_time: str,
                              max_data_timestamp: str, last_processed_id: int,
                              rows_extracted: int, status: str = 'success',
                              **kwargs) -> bool:
        """Legacy MySQL watermark update method."""
        try:
            self.simple_manager.update_mysql_state(
                table_name=table_name,
                timestamp=max_data_timestamp,
                id=last_processed_id,
                status=status,
                error=kwargs.get('error')
            )
            
            logger.info(f"Updated MySQL watermark for {table_name}: "
                       f"timestamp={max_data_timestamp}, id={last_processed_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update MySQL watermark for {table_name}: {e}")
            return False
    
    def update_redshift_watermark(self, table_name: str, load_time: datetime,
                                 rows_loaded: int, status: str = 'success',
                                 processed_files: Optional[List[str]] = None,
                                 **kwargs) -> bool:
        """Legacy Redshift watermark update method."""
        try:
            # Note: rows_loaded is ignored - we use actual Redshift count
            files = processed_files or []
            
            self.simple_manager.update_redshift_state(
                table_name=table_name,
                loaded_files=files,
                status=status,
                error=kwargs.get('error')
            )
            
            logger.info(f"Updated Redshift watermark for {table_name}: "
                       f"files={len(files)}, status={status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Redshift watermark for {table_name}: {e}")
            return False
    
    def set_manual_watermark(self, table_name: str, data_timestamp: str, 
                            data_id: Optional[int] = None) -> bool:
        """Legacy manual watermark setting."""
        try:
            self.simple_manager.set_manual_watermark(
                table_name=table_name,
                timestamp=data_timestamp,
                id=data_id
            )
            return True
        except Exception as e:
            logger.error(f"Failed to set manual watermark for {table_name}: {e}")
            return False
    
    def get_incremental_start_timestamp(self, table_name: str) -> Optional[str]:
        """Get timestamp for incremental processing."""
        watermark = self.get_table_watermark(table_name)
        if watermark and watermark.last_mysql_data_timestamp:
            return watermark.last_mysql_data_timestamp
        return None
    
    # Additional legacy methods can be added here as needed
    def reset_table_watermark(self, table_name: str) -> bool:
        """Reset watermark to initial state."""
        try:
            self.simple_manager.reset_watermark(table_name)
            return True
        except Exception as e:
            logger.error(f"Failed to reset watermark for {table_name}: {e}")
            return False


def create_watermark_manager(config: Dict[str, Any]) -> WatermarkAdapter:
    """
    Factory function to create watermark manager.
    
    Returns the new adapter which provides legacy compatibility
    while using the improved v2.0 system internally.
    """
    return WatermarkAdapter(config)