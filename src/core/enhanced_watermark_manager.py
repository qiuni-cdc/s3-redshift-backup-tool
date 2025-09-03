"""
Enhanced Watermark Manager for v1.2.0

Extends the existing S3 watermark system to support:
- Multiple CDC column types and strategies
- Flexible watermark data structures
- Backward compatibility with v1.0.0/v1.1.0 watermark format
- Advanced watermark validation and recovery
"""

from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import json
from dataclasses import asdict

from src.core.cdc_strategy_engine import WatermarkData, CDCStrategyType
from src.core.s3_watermark_manager import S3WatermarkManager  # Existing system
from src.utils.logging import get_logger

logger = get_logger(__name__)


class EnhancedWatermarkManager:
    """
    Enhanced watermark manager supporting flexible CDC strategies
    
    Wraps and extends existing S3WatermarkManager with:
    - CDC strategy-aware watermark handling
    - Multi-column watermark support
    - Advanced validation and recovery
    """
    
    def __init__(self, s3_client=None, bucket_name: str = None):
        # Use existing S3 watermark manager as base
        self.base_manager = S3WatermarkManager()
        if s3_client:
            self.base_manager.s3_client = s3_client
        if bucket_name:
            self.base_manager.bucket_name = bucket_name
    
    def save_cdc_watermark(self, 
                          table_name: str,
                          watermark_data: WatermarkData,
                          session_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Save CDC watermark data using enhanced format while maintaining compatibility
        
        Converts new WatermarkData format to legacy S3 watermark format
        but adds v1.2.0 metadata for enhanced features.
        """
        try:
            # Convert to legacy format for S3 storage compatibility
            legacy_watermark = self._watermark_data_to_legacy_format(watermark_data)
            
            # Add session metadata
            if session_metadata:
                legacy_watermark.update(session_metadata)
            
            # Add v1.2.0 enhanced metadata
            if 'metadata' not in legacy_watermark:
                legacy_watermark['metadata'] = {}
                
            legacy_watermark['metadata'].update({
                'watermark_version': '1.2.0',
                'cdc_strategy': watermark_data.strategy_used,
                'enhanced_features': True,
                'save_timestamp': datetime.now().isoformat(),
                **watermark_data.additional_data
            })
            
            # Use existing S3 watermark manager for actual storage
            success = self.base_manager.save_watermark(table_name, legacy_watermark)
            
            if success:
                logger.info(f"Saved v1.2.0 enhanced watermark for {table_name}", extra={
                    "table": table_name,
                    "strategy": watermark_data.strategy_used,
                    "row_count": watermark_data.row_count,
                    "last_timestamp": watermark_data.last_timestamp,
                    "last_id": watermark_data.last_id
                })
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to save enhanced watermark for {table_name}: {e}")
            return False
    
    def load_cdc_watermark(self, table_name: str) -> Optional[WatermarkData]:
        """
        Load watermark data and convert to enhanced WatermarkData format
        
        Handles both v1.0.0/v1.1.0 legacy format and v1.2.0 enhanced format
        """
        try:
            # Load using existing S3 watermark manager
            legacy_watermark = self.base_manager.get_watermark(table_name)
            
            if not legacy_watermark:
                logger.info(f"No watermark found for {table_name}")
                return None
            
            # Convert to enhanced format
            watermark_data = self._legacy_format_to_watermark_data(legacy_watermark)
            
            logger.info(f"Loaded watermark for {table_name}", extra={
                "table": table_name,
                "strategy": watermark_data.strategy_used,
                "row_count": watermark_data.row_count,
                "has_timestamp": watermark_data.last_timestamp is not None,
                "has_id": watermark_data.last_id is not None
            })
            
            return watermark_data
            
        except Exception as e:
            logger.error(f"Failed to load watermark for {table_name}: {e}")
            return None
    
    def validate_watermark_consistency(self, 
                                     table_name: str,
                                     expected_strategy: str,
                                     expected_columns: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate watermark data consistency with current CDC configuration
        
        Returns validation results with recommendations
        """
        results = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'recommendations': [],
            'migration_needed': False
        }
        
        try:
            watermark_data = self.load_cdc_watermark(table_name)
            
            if not watermark_data:
                results['warnings'].append("No watermark data found - first sync")
                return results
            
            # Strategy consistency check
            current_strategy = watermark_data.strategy_used
            if current_strategy != expected_strategy:
                if current_strategy in ['timestamp_only', None] and expected_strategy != 'timestamp_only':
                    results['migration_needed'] = True
                    results['recommendations'].append(
                        f"Consider migrating from {current_strategy} to {expected_strategy}"
                    )
                else:
                    results['errors'].append(
                        f"Strategy mismatch: watermark uses {current_strategy}, config expects {expected_strategy}"
                    )
                    results['valid'] = False
            
            # Column consistency checks
            if expected_strategy in ['timestamp_only', 'hybrid']:
                expected_ts_col = expected_columns.get('timestamp_column')
                watermark_ts_col = watermark_data.additional_data.get('timestamp_column')
                
                if expected_ts_col and watermark_ts_col and expected_ts_col != watermark_ts_col:
                    results['warnings'].append(
                        f"Timestamp column changed: {watermark_ts_col} → {expected_ts_col}"
                    )
            
            if expected_strategy in ['hybrid', 'id_only']:
                expected_id_col = expected_columns.get('id_column')
                watermark_id_col = watermark_data.additional_data.get('id_column')
                
                if expected_id_col and watermark_id_col and expected_id_col != watermark_id_col:
                    results['warnings'].append(
                        f"ID column changed: {watermark_id_col} → {expected_id_col}"
                    )
            
            # Data completeness checks
            if expected_strategy in ['timestamp_only', 'hybrid'] and not watermark_data.last_timestamp:
                results['errors'].append("Missing timestamp data in watermark")
                results['valid'] = False
            
            if expected_strategy in ['hybrid', 'id_only'] and not watermark_data.last_id:
                results['errors'].append("Missing ID data in watermark")
                results['valid'] = False
            
            logger.info(f"Watermark validation for {table_name}", extra={
                "table": table_name,
                "valid": results['valid'],
                "migration_needed": results['migration_needed'],
                "warnings": len(results['warnings']),
                "errors": len(results['errors'])
            })
            
            return results
            
        except Exception as e:
            results['valid'] = False
            results['errors'].append(f"Validation error: {str(e)}")
            return results
    
    def migrate_watermark_strategy(self,
                                  table_name: str,
                                  new_strategy: str,
                                  new_columns: Dict[str, Any],
                                  preserve_data: bool = True) -> bool:
        """
        Migrate watermark from one CDC strategy to another
        
        Handles common migration scenarios:
        - timestamp_only → hybrid (add ID tracking)
        - hybrid → timestamp_only (remove ID tracking)
        - Any strategy → full_sync (reset watermark)
        """
        try:
            current_watermark = self.load_cdc_watermark(table_name)
            
            if not current_watermark:
                logger.info(f"No existing watermark for {table_name} - creating new")
                return True
            
            # Create migrated watermark
            migrated_watermark = WatermarkData(
                strategy_used=new_strategy,
                row_count=current_watermark.row_count if preserve_data else 0
            )
            
            # Strategy-specific migration logic
            if new_strategy == 'full_sync':
                # Full sync doesn't need watermark data
                migrated_watermark.additional_data = {'migration': 'to_full_sync'}
                
            elif new_strategy == 'timestamp_only':
                # Keep timestamp, discard ID
                migrated_watermark.last_timestamp = current_watermark.last_timestamp
                migrated_watermark.additional_data = {
                    'timestamp_column': new_columns.get('timestamp_column', 'updated_at'),
                    'migration': f'from_{current_watermark.strategy_used}'
                }
                
            elif new_strategy == 'hybrid':
                # Keep timestamp, initialize ID
                migrated_watermark.last_timestamp = current_watermark.last_timestamp
                migrated_watermark.last_id = current_watermark.last_id or 0
                migrated_watermark.additional_data = {
                    'timestamp_column': new_columns.get('timestamp_column', 'updated_at'),
                    'id_column': new_columns.get('id_column', 'ID'),
                    'migration': f'from_{current_watermark.strategy_used}'
                }
                
            elif new_strategy == 'id_only':
                # Keep ID, discard timestamp
                migrated_watermark.last_id = current_watermark.last_id or 0
                migrated_watermark.additional_data = {
                    'id_column': new_columns.get('id_column', 'ID'),
                    'migration': f'from_{current_watermark.strategy_used}'
                }
            
            # Save migrated watermark
            success = self.save_cdc_watermark(table_name, migrated_watermark, {
                'migration_timestamp': datetime.now().isoformat(),
                'previous_strategy': current_watermark.strategy_used,
                'preserve_data': preserve_data
            })
            
            if success:
                logger.info(f"Successfully migrated watermark for {table_name}", extra={
                    "table": table_name,
                    "from_strategy": current_watermark.strategy_used,
                    "to_strategy": new_strategy,
                    "preserve_data": preserve_data
                })
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to migrate watermark for {table_name}: {e}")
            return False
    
    def _watermark_data_to_legacy_format(self, watermark_data: WatermarkData) -> Dict[str, Any]:
        """Convert WatermarkData to legacy S3 watermark format"""
        legacy_format = {}
        
        # Map core fields
        if watermark_data.last_timestamp:
            legacy_format['last_mysql_data_timestamp'] = watermark_data.last_timestamp
            legacy_format['last_mysql_extraction_time'] = datetime.now()
        
        if watermark_data.last_id:
            legacy_format['last_processed_id'] = watermark_data.last_id
        
        if watermark_data.row_count:
            legacy_format['mysql_row_count'] = watermark_data.row_count
        
        # Add status fields (required by legacy system)
        legacy_format['mysql_status'] = 'success'
        legacy_format['backup_strategy'] = watermark_data.strategy_used or 'cdc_enhanced'
        
        return legacy_format
    
    def _legacy_format_to_watermark_data(self, legacy_watermark: Dict[str, Any]) -> WatermarkData:
        """Convert legacy S3 watermark format to WatermarkData"""
        
        # Extract core data
        last_timestamp = legacy_watermark.get('last_mysql_data_timestamp')
        last_id = legacy_watermark.get('last_processed_id')
        row_count = legacy_watermark.get('mysql_row_count', 0)
        
        # Determine strategy
        metadata = legacy_watermark.get('metadata', {})
        strategy_used = metadata.get('cdc_strategy')
        
        if not strategy_used:
            # Legacy detection logic
            if last_timestamp and last_id:
                strategy_used = 'hybrid'  # v1.0.0/v1.1.0 used hybrid approach
            elif last_timestamp:
                strategy_used = 'timestamp_only'
            elif last_id:
                strategy_used = 'id_only'
            else:
                strategy_used = 'unknown'
        
        # Additional data
        additional_data = {}
        if 'timestamp_column' in metadata:
            additional_data['timestamp_column'] = metadata['timestamp_column']
        if 'id_column' in metadata:
            additional_data['id_column'] = metadata['id_column']
        
        # Copy all metadata
        additional_data.update(metadata)
        
        return WatermarkData(
            last_timestamp=last_timestamp,
            last_id=last_id,
            row_count=row_count,
            strategy_used=strategy_used,
            additional_data=additional_data
        )
    
    def get_watermark_statistics(self, table_names: List[str]) -> Dict[str, Any]:
        """Get statistics about watermarks across multiple tables"""
        stats = {
            'total_tables': len(table_names),
            'with_watermarks': 0,
            'without_watermarks': 0,
            'strategies_used': {},
            'version_distribution': {'1.0.0': 0, '1.1.0': 0, '1.2.0': 0, 'unknown': 0},
            'migration_candidates': []
        }
        
        for table_name in table_names:
            try:
                watermark_data = self.load_cdc_watermark(table_name)
                
                if watermark_data:
                    stats['with_watermarks'] += 1
                    
                    # Count strategies
                    strategy = watermark_data.strategy_used or 'unknown'
                    stats['strategies_used'][strategy] = stats['strategies_used'].get(strategy, 0) + 1
                    
                    # Detect version
                    metadata = watermark_data.additional_data or {}
                    if metadata.get('watermark_version') == '1.2.0':
                        stats['version_distribution']['1.2.0'] += 1
                    elif metadata.get('enhanced_features'):
                        stats['version_distribution']['1.2.0'] += 1
                    elif strategy == 'hybrid':
                        stats['version_distribution']['1.1.0'] += 1
                    elif strategy == 'timestamp_only':
                        stats['version_distribution']['1.0.0'] += 1
                    else:
                        stats['version_distribution']['unknown'] += 1
                    
                    # Check migration candidates
                    if strategy in ['timestamp_only', 'unknown'] and not metadata.get('watermark_version'):
                        stats['migration_candidates'].append(table_name)
                
                else:
                    stats['without_watermarks'] += 1
                    
            except Exception as e:
                logger.error(f"Error analyzing watermark for {table_name}: {e}")
                stats['without_watermarks'] += 1
        
        return stats