"""
Enhanced S3-based watermark management system with database-like search capabilities.

Provides table-level watermark tracking stored in S3 with rich query functionality.
"""

import boto3
import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

from src.config.settings import AppConfig
from src.utils.exceptions import WatermarkError
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class S3TableWatermark:
    """Data class for S3-stored table watermark"""
    table_name: str
    last_mysql_extraction_time: Optional[str] = None
    last_mysql_data_timestamp: Optional[str] = None
    mysql_rows_extracted: int = 0
    mysql_status: str = 'pending'  # pending, success, failed
    last_redshift_load_time: Optional[str] = None
    redshift_rows_loaded: int = 0
    redshift_status: str = 'pending'
    backup_strategy: str = 'sequential'
    s3_file_count: int = 0
    last_error: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = asdict(self)
        # Ensure metadata is not None
        if result['metadata'] is None:
            result['metadata'] = {}
        # Ensure None values are properly handled
        for key, value in result.items():
            if value is None:
                result[key] = None  # Explicitly set None for JSON serialization
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'S3TableWatermark':
        """Create from dictionary (JSON deserialization)"""
        # Handle missing fields gracefully
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class S3WatermarkManager:
    """
    Manages table-level watermarks stored in S3 with database-like query capabilities.
    
    Provides rich search, filtering, and management features for watermarks.
    """
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.s3_client = self._init_s3_client()
        self.bucket_name = config.s3.bucket_name
        
        # S3 paths
        self.watermark_prefix = "watermark/"
        self.tables_prefix = f"{self.watermark_prefix}tables/"
        self.history_prefix = f"{self.watermark_prefix}history/"
        self.global_prefix = f"{self.watermark_prefix}global/"
    
    def _init_s3_client(self):
        """Initialize S3 client"""
        try:
            client = boto3.client(
                's3',
                aws_access_key_id=self.config.s3.access_key,
                aws_secret_access_key=self.config.s3.secret_key.get_secret_value(),
                region_name=self.config.s3.region
            )
            logger.info("S3 watermark manager initialized")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise WatermarkError(f"S3 client initialization failed: {e}")
    
    def get_table_watermark(self, table_name: str) -> Optional[S3TableWatermark]:
        """
        Get watermark for specific table.
        
        Args:
            table_name: Name of the table (e.g., 'settlement.settlement_claim_detail')
            
        Returns:
            S3TableWatermark object or None if not found
        """
        try:
            s3_key = self._get_table_watermark_key(table_name)
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            watermark = S3TableWatermark.from_dict(data)
            
            logger.debug(f"Retrieved watermark for table {table_name}")
            return watermark
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.debug(f"No watermark found for table {table_name}")
            return None
        except Exception as e:
            logger.error(f"Failed to get watermark for table {table_name}: {e}")
            raise WatermarkError(f"Failed to retrieve watermark: {e}")
    
    def update_mysql_watermark(
        self,
        table_name: str,
        extraction_time: datetime,
        max_data_timestamp: Optional[datetime] = None,
        rows_extracted: int = 0,
        status: str = 'success',
        backup_strategy: str = 'sequential',
        s3_file_count: int = 0,
        error_message: Optional[str] = None
    ) -> bool:
        """Update MySQL extraction watermark"""
        try:
            # Get existing watermark or create new
            watermark = self.get_table_watermark(table_name) or S3TableWatermark(
                table_name=table_name,
                created_at=datetime.utcnow().isoformat() + 'Z'
            )
            
            # Update MySQL-related fields
            watermark.last_mysql_extraction_time = extraction_time.isoformat() + 'Z'
            watermark.last_mysql_data_timestamp = max_data_timestamp.isoformat() + 'Z' if max_data_timestamp else None
            watermark.mysql_rows_extracted = rows_extracted
            watermark.mysql_status = status
            watermark.backup_strategy = backup_strategy
            watermark.s3_file_count = s3_file_count
            watermark.updated_at = datetime.utcnow().isoformat() + 'Z'
            
            if error_message:
                watermark.last_error = error_message
            
            # Save to S3
            success = self._save_watermark(watermark)
            
            if success:
                logger.info(
                    f"Updated MySQL watermark for {table_name}",
                    extraction_time=extraction_time,
                    rows_extracted=rows_extracted,
                    status=status
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to update MySQL watermark for {table_name}: {e}")
            raise WatermarkError(f"Failed to update MySQL watermark: {e}")
    
    def update_redshift_watermark(
        self,
        table_name: str,
        load_time: datetime,
        rows_loaded: int = 0,
        status: str = 'success',
        error_message: Optional[str] = None
    ) -> bool:
        """Update Redshift load watermark"""
        try:
            # Get existing watermark
            watermark = self.get_table_watermark(table_name)
            if not watermark:
                raise WatermarkError(f"No existing watermark for table {table_name}")
            
            # Update Redshift-related fields
            watermark.last_redshift_load_time = load_time.isoformat() + 'Z'
            watermark.redshift_rows_loaded = rows_loaded
            watermark.redshift_status = status
            watermark.updated_at = datetime.utcnow().isoformat() + 'Z'
            
            if error_message:
                watermark.last_error = error_message
            
            success = self._save_watermark(watermark)
            
            if success:
                logger.info(
                    f"Updated Redshift watermark for {table_name}",
                    load_time=load_time,
                    rows_loaded=rows_loaded,
                    status=status
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to update Redshift watermark for {table_name}: {e}")
            raise WatermarkError(f"Failed to update Redshift watermark: {e}")
    
    def list_all_tables(self) -> List[S3TableWatermark]:
        """Get all table watermarks (like SELECT * FROM watermarks)"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.tables_prefix,
                MaxKeys=1000
            )
            
            watermarks = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    try:
                        # Get the watermark data
                        response = self.s3_client.get_object(
                            Bucket=self.bucket_name,
                            Key=obj['Key']
                        )
                        
                        data = json.loads(response['Body'].read().decode('utf-8'))
                        watermark = S3TableWatermark.from_dict(data)
                        watermarks.append(watermark)
                        
                    except Exception as e:
                        logger.warning(f"Failed to read watermark {obj['Key']}: {e}")
                        continue
            
            # Sort by table name
            watermarks.sort(key=lambda w: w.table_name)
            
            logger.info(f"Retrieved {len(watermarks)} table watermarks")
            return watermarks
            
        except Exception as e:
            logger.error(f"Failed to list table watermarks: {e}")
            raise WatermarkError(f"Failed to list watermarks: {e}")
    
    def search_tables(
        self,
        table_pattern: Optional[str] = None,
        mysql_status: Optional[str] = None,
        redshift_status: Optional[str] = None,
        updated_since: Optional[datetime] = None,
        has_errors: Optional[bool] = None
    ) -> List[S3TableWatermark]:
        """
        Search tables with filters (like SQL WHERE clause).
        
        Args:
            table_pattern: Table name pattern (supports wildcards)
            mysql_status: Filter by MySQL status (pending, success, failed)
            redshift_status: Filter by Redshift status 
            updated_since: Only tables updated since this datetime
            has_errors: Only tables with/without errors
            
        Returns:
            List of matching watermarks
        """
        try:
            all_watermarks = self.list_all_tables()
            filtered = []
            
            for watermark in all_watermarks:
                # Apply filters
                if table_pattern:
                    if not self._matches_pattern(watermark.table_name, table_pattern):
                        continue
                
                if mysql_status and watermark.mysql_status != mysql_status:
                    continue
                
                if redshift_status and watermark.redshift_status != redshift_status:
                    continue
                
                if updated_since:
                    if not watermark.updated_at:
                        continue
                    updated_dt = datetime.fromisoformat(watermark.updated_at.replace('Z', '+00:00'))
                    if updated_dt < updated_since:
                        continue
                
                if has_errors is not None:
                    has_error = bool(watermark.last_error)
                    if has_errors != has_error:
                        continue
                
                filtered.append(watermark)
            
            logger.info(f"Search returned {len(filtered)} matching tables")
            return filtered
            
        except Exception as e:
            logger.error(f"Failed to search tables: {e}")
            raise WatermarkError(f"Table search failed: {e}")
    
    def get_sync_summary(self) -> Dict[str, Any]:
        """Get summary statistics (like SQL GROUP BY with COUNT)"""
        try:
            all_watermarks = self.list_all_tables()
            
            summary = {
                'total_tables': len(all_watermarks),
                'mysql_success': 0,
                'mysql_failed': 0,
                'mysql_pending': 0,
                'redshift_success': 0,
                'redshift_failed': 0,
                'redshift_pending': 0,
                'total_mysql_rows': 0,
                'total_redshift_rows': 0,
                'tables_with_errors': 0,
                'last_updated': None
            }
            
            latest_update = None
            
            for watermark in all_watermarks:
                # Count statuses
                if watermark.mysql_status == 'success':
                    summary['mysql_success'] += 1
                elif watermark.mysql_status == 'failed':
                    summary['mysql_failed'] += 1
                else:
                    summary['mysql_pending'] += 1
                
                if watermark.redshift_status == 'success':
                    summary['redshift_success'] += 1
                elif watermark.redshift_status == 'failed':
                    summary['redshift_failed'] += 1
                else:
                    summary['redshift_pending'] += 1
                
                # Sum rows
                summary['total_mysql_rows'] += watermark.mysql_rows_extracted
                summary['total_redshift_rows'] += watermark.redshift_rows_loaded
                
                # Count errors
                if watermark.last_error:
                    summary['tables_with_errors'] += 1
                
                # Track latest update
                if watermark.updated_at:
                    update_dt = datetime.fromisoformat(watermark.updated_at.replace('Z', '+00:00'))
                    if not latest_update or update_dt > latest_update:
                        latest_update = update_dt
            
            if latest_update:
                summary['last_updated'] = latest_update.isoformat()
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get sync summary: {e}")
            return {}
    
    def get_incremental_start_timestamp(self, table_name: str) -> str:
        """Get timestamp for incremental data extraction"""
        watermark = self.get_table_watermark(table_name)
        
        if watermark and watermark.last_mysql_data_timestamp and watermark.mysql_status == 'success':
            # Convert from ISO format back to MySQL format
            dt = datetime.fromisoformat(watermark.last_mysql_data_timestamp.replace('Z', '+00:00'))
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            logger.debug(f"Using incremental start timestamp for {table_name}: {timestamp}")
            return timestamp
        else:
            # Fallback to default
            default_timestamp = "2025-01-01 00:00:00"
            logger.debug(f"Using default start timestamp for {table_name}: {default_timestamp}")
            return default_timestamp
    
    def get_last_watermark(self, table_name: Optional[str] = None) -> str:
        """
        Compatibility method for backup strategies that expect get_last_watermark().
        
        Args:
            table_name: Optional table name (if not provided, returns default watermark)
            
        Returns:
            Last watermark timestamp in MySQL format (YYYY-MM-DD HH:MM:SS)
        """
        if table_name:
            return self.get_incremental_start_timestamp(table_name)
        else:
            # Return a reasonable default for global watermark queries
            default_timestamp = "2025-01-01 00:00:00"
            logger.debug(f"get_last_watermark called without table_name, returning default: {default_timestamp}")
            return default_timestamp
    
    def get_watermark_metadata(self) -> Dict[str, Any]:
        """
        Compatibility method for CLI that expects get_watermark_metadata().
        
        Returns:
            Dictionary with watermark metadata for display purposes
        """
        try:
            summary = self.get_sync_summary()
            
            return {
                'updated_at': summary.get('last_updated'),
                'total_tables': summary.get('total_tables', 0),
                'mysql_success': summary.get('mysql_success', 0),
                'redshift_success': summary.get('redshift_success', 0),
                'backup_strategy': 'table_aware',
                'format_version': '3.0'
            }
        except Exception as e:
            logger.warning(f"Failed to get watermark metadata: {e}")
            return {
                'backup_strategy': 'table_aware',
                'format_version': '3.0',
                'error': str(e)
            }
    
    def backup_watermark(self, table_name: str) -> str:
        """Create backup of table watermark"""
        try:
            watermark = self.get_table_watermark(table_name)
            if not watermark:
                raise WatermarkError(f"No watermark found for table {table_name}")
            
            # Create backup key with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            table_safe_name = table_name.replace('.', '_')
            backup_key = f"{self.history_prefix}{datetime.utcnow().strftime('%Y-%m-%d')}/{table_safe_name}_{timestamp}.json"
            
            # Save backup
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=backup_key,
                Body=json.dumps(watermark.to_dict(), indent=2),
                ContentType='application/json',
                Metadata={
                    'table_name': table_name,
                    'backup_time': datetime.utcnow().isoformat(),
                    'backup_type': 'manual'
                }
            )
            
            logger.info(f"Created watermark backup for {table_name}: {backup_key}")
            return backup_key
            
        except Exception as e:
            logger.error(f"Failed to backup watermark for {table_name}: {e}")
            raise WatermarkError(f"Watermark backup failed: {e}")
    
    def delete_table_watermark(self, table_name: str, create_backup: bool = True) -> bool:
        """Delete table watermark (with optional backup)"""
        try:
            if create_backup:
                backup_key = self.backup_watermark(table_name)
                logger.info(f"Backup created before deletion: {backup_key}")
            
            s3_key = self._get_table_watermark_key(table_name)
            
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.warning(f"Deleted watermark for table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete watermark for {table_name}: {e}")
            raise WatermarkError(f"Watermark deletion failed: {e}")
    
    def _save_watermark(self, watermark: S3TableWatermark) -> bool:
        """Save watermark to S3"""
        try:
            s3_key = self._get_table_watermark_key(watermark.table_name)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(watermark.to_dict(), indent=2),
                ContentType='application/json',
                Metadata={
                    'table_name': watermark.table_name,
                    'mysql_status': watermark.mysql_status,
                    'redshift_status': watermark.redshift_status,
                    'updated_at': watermark.updated_at or ''
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save watermark for {watermark.table_name}: {e}")
            return False
    
    def _get_table_watermark_key(self, table_name: str) -> str:
        """Generate S3 key for table watermark"""
        # Convert settlement.settlement_claim_detail -> settlement_settlement_claim_detail.json
        safe_name = table_name.replace('.', '_')
        return f"{self.tables_prefix}{safe_name}.json"
    
    def _matches_pattern(self, text: str, pattern: str) -> bool:
        """Simple pattern matching with wildcards"""
        if '*' not in pattern:
            return pattern.lower() in text.lower()
        
        # Simple wildcard matching
        pattern = pattern.lower()
        text = text.lower()
        
        if pattern.startswith('*') and pattern.endswith('*'):
            return pattern[1:-1] in text
        elif pattern.startswith('*'):
            return text.endswith(pattern[1:])
        elif pattern.endswith('*'):
            return text.startswith(pattern[:-1])
        else:
            return pattern in text
    
    def set_manual_watermark(
        self,
        table_name: str,
        data_timestamp: datetime
    ) -> bool:
        """
        Set manual watermark for CLI operations (without extraction time).
        
        This creates a completely fresh watermark with only the data timestamp, 
        which enables watermark-based filtering instead of session-based filtering.
        
        Args:
            table_name: Name of the table
            data_timestamp: The data cutoff timestamp
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create a completely fresh watermark (ignore existing)
            watermark = S3TableWatermark(
                table_name=table_name,
                last_mysql_data_timestamp=data_timestamp.isoformat() + 'Z',
                last_mysql_extraction_time=None,  # Explicitly None for manual watermarks
                mysql_rows_extracted=0,
                mysql_status='success',
                redshift_rows_loaded=0, 
                redshift_status='pending',
                backup_strategy='manual_cli',
                s3_file_count=0,
                created_at=datetime.utcnow().isoformat() + 'Z',
                updated_at=datetime.utcnow().isoformat() + 'Z',
                metadata={'manual_watermark': True}
            )
            
            success = self._save_watermark(watermark)
            
            if success:
                logger.info(
                    f"Set manual watermark for {table_name}",
                    data_timestamp=data_timestamp,
                    watermark_type='manual'
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to set manual watermark for {table_name}: {e}")
            raise WatermarkError(f"Failed to set manual watermark: {e}")