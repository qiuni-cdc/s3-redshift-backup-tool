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
    last_processed_id: Optional[int] = None  # Last processed ID for row-based pagination
    mysql_rows_extracted: int = 0
    mysql_status: str = 'pending'  # pending, success, failed, in_progress
    last_redshift_load_time: Optional[str] = None
    redshift_rows_loaded: int = 0
    redshift_status: str = 'pending'  # pending, success, failed, in_progress
    backup_strategy: str = 'sequential'
    s3_file_count: int = 0
    processed_s3_files: Optional[List[str]] = None  # Track loaded S3 files to prevent duplicates
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
        Get watermark for specific table with automatic recovery from backup locations.
        
        Args:
            table_name: Name of the table (e.g., 'settlement.settlement_claim_detail')
            
        Returns:
            S3TableWatermark object or None if not found in any location
        """
        # Try primary location first
        watermark = self._get_watermark_from_primary(table_name)
        if watermark:
            logger.debug(f"Retrieved watermark for table {table_name} from primary location")
            return watermark
        
        # Try recovery from backup locations
        logger.info(f"Primary watermark not found for {table_name}, attempting recovery from backups")
        watermark = self._recover_watermark_from_backups(table_name)
        
        if watermark:
            logger.info(f"Successfully recovered watermark for {table_name} from backup location")
            
            # Restore the recovered watermark to primary location
            try:
                self._restore_primary_watermark(table_name, watermark)
                logger.info(f"Restored watermark for {table_name} to primary location")
            except Exception as e:
                logger.warning(f"Failed to restore watermark to primary location: {e}")
            
            return watermark
        
        logger.debug(f"No watermark found for table {table_name} in any location")
        return None
    
    def _get_watermark_from_primary(self, table_name: str) -> Optional[S3TableWatermark]:
        """Get watermark from primary location"""
        try:
            s3_key = self._get_table_watermark_key(table_name)
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Remove backup metadata for clean watermark object
            if 'backup_metadata' in data:
                del data['backup_metadata']
            
            watermark = S3TableWatermark.from_dict(data)
            
            # Validate the watermark data
            if self._validate_watermark_data(watermark):
                return watermark
            else:
                logger.warning(f"Primary watermark for {table_name} failed validation")
                return None
            
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.warning(f"Failed to get primary watermark for {table_name}: {e}")
            return None
    
    def _recover_watermark_from_backups(self, table_name: str) -> Optional[S3TableWatermark]:
        """Recover watermark from backup locations in priority order"""
        
        # Recovery methods in priority order
        recovery_methods = [
            ("daily backup", self._recover_from_daily_backup),
            ("latest session backup", self._recover_from_latest_session_backup),
            ("any session backup", self._recover_from_any_session_backup)
        ]
        
        for method_name, recovery_method in recovery_methods:
            try:
                logger.debug(f"Trying recovery method: {method_name}")
                watermark = recovery_method(table_name)
                if watermark and self._validate_watermark_data(watermark):
                    logger.info(f"Watermark recovered successfully using {method_name}")
                    return watermark
                else:
                    logger.debug(f"No valid watermark found using {method_name}")
            except Exception as e:
                logger.warning(f"Recovery method {method_name} failed: {e}")
        
        logger.error(f"All recovery methods failed for {table_name}")
        return None
    
    def _recover_from_daily_backup(self, table_name: str) -> Optional[S3TableWatermark]:
        """Recover from today's daily backup"""
        try:
            daily_key = self._get_daily_backup_key(table_name)
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=daily_key
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Remove backup metadata
            if 'backup_metadata' in data:
                del data['backup_metadata']
            
            return S3TableWatermark.from_dict(data)
            
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.debug(f"Daily backup recovery failed: {e}")
            return None
    
    def _recover_from_latest_session_backup(self, table_name: str) -> Optional[S3TableWatermark]:
        """Recover from the most recent session backup"""
        try:
            safe_name = table_name.replace('.', '_')
            prefix = f"{self.watermark_prefix}backups/sessions/"
            
            # List all session backup directories
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            
            session_dirs = []
            for common_prefix in response.get('CommonPrefixes', []):
                session_id = common_prefix['Prefix'].split('/')[-2]
                session_dirs.append(session_id)
            
            # Sort by session ID (which includes timestamp) to get latest
            if session_dirs:
                latest_session = sorted(session_dirs)[-1]
                session_key = self._get_session_backup_key(table_name, latest_session)
                
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=session_key
                )
                
                data = json.loads(response['Body'].read().decode('utf-8'))
                
                # Remove backup metadata
                if 'backup_metadata' in data:
                    del data['backup_metadata']
                
                return S3TableWatermark.from_dict(data)
            
            return None
            
        except Exception as e:
            logger.debug(f"Latest session backup recovery failed: {e}")
            return None
    
    def _recover_from_any_session_backup(self, table_name: str) -> Optional[S3TableWatermark]:
        """Recover from any available session backup"""
        try:
            safe_name = table_name.replace('.', '_')
            prefix = f"{self.watermark_prefix}backups/sessions/"
            
            # List all files in session backup area
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            # Find files that match our table
            matching_files = []
            for obj in response.get('Contents', []):
                if safe_name in obj['Key'] and obj['Key'].endswith('.json'):
                    matching_files.append((obj['Key'], obj['LastModified']))
            
            if matching_files:
                # Get the most recently modified file
                latest_file = max(matching_files, key=lambda x: x[1])
                
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=latest_file[0]
                )
                
                data = json.loads(response['Body'].read().decode('utf-8'))
                
                # Remove backup metadata
                if 'backup_metadata' in data:
                    del data['backup_metadata']
                
                return S3TableWatermark.from_dict(data)
            
            return None
            
        except Exception as e:
            logger.debug(f"Any session backup recovery failed: {e}")
            return None
    
    def _validate_watermark_data(self, watermark: S3TableWatermark) -> bool:
        """Validate that watermark data is consistent and usable"""
        try:
            # Check required fields exist
            if not watermark.table_name:
                return False
            
            # Check that statuses are valid
            valid_statuses = ['pending', 'success', 'failed', 'in_progress']
            if watermark.mysql_status not in valid_statuses:
                return False
            if watermark.redshift_status not in valid_statuses:
                return False
            
            # Check that extracted rows is not negative
            if watermark.mysql_rows_extracted < 0:
                return False
            
            logger.debug(f"Watermark validation passed for {watermark.table_name}")
            return True
            
        except Exception as e:
            logger.warning(f"Watermark validation failed: {e}")
            return False
    
    def _restore_primary_watermark(self, table_name: str, watermark: S3TableWatermark):
        """Restore recovered watermark to primary location"""
        try:
            # Add metadata indicating this is a restored watermark
            watermark_data = watermark.to_dict()
            watermark_data['backup_metadata'] = {
                'restored_at': datetime.utcnow().isoformat() + 'Z',
                'restoration_source': 'backup_recovery',
                'backup_locations': ['primary_restored']
            }
            
            primary_key = self._get_table_watermark_key(table_name)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=primary_key,
                Body=json.dumps(watermark_data, indent=2),
                ContentType='application/json',
                Metadata={
                    'table_name': watermark.table_name,
                    'mysql_status': watermark.mysql_status,
                    'redshift_status': watermark.redshift_status,
                    'updated_at': watermark.updated_at or '',
                    'backup_location': 'primary_restored',
                    'restoration_source': 'backup_recovery'
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to restore primary watermark: {e}")
            raise
    
    def get_watermark_backup_status(self, table_name: str) -> dict:
        """Get status of all watermark backup locations for debugging"""
        safe_name = table_name.replace('.', '_')
        status = {
            'table_name': table_name,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'backup_locations': {}
        }
        
        # Check primary location
        primary_key = self._get_table_watermark_key(table_name)
        status['backup_locations']['primary'] = self._check_backup_location(primary_key)
        
        # Check daily backup
        daily_key = self._get_daily_backup_key(table_name)
        status['backup_locations']['daily'] = self._check_backup_location(daily_key)
        
        # Check session backups
        session_count = 0
        try:
            prefix = f"{self.watermark_prefix}backups/sessions/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            for obj in response.get('Contents', []):
                if safe_name in obj['Key'] and obj['Key'].endswith('.json'):
                    session_count += 1
        except:
            pass
        
        status['backup_locations']['sessions'] = {
            'status': 'available' if session_count > 0 else 'missing',
            'count': session_count
        }
        
        return status
    
    def _check_backup_location(self, s3_key: str) -> dict:
        """Check if a backup location exists and get metadata"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                'status': 'available',
                'last_modified': response['LastModified'].isoformat(),
                'size': response['ContentLength']
            }
        except self.s3_client.exceptions.NoSuchKey:
            return {'status': 'missing'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def cleanup_old_watermark_backups(self, days_to_keep: int = 7) -> dict:
        """Clean up old watermark backup files"""
        from datetime import timedelta
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        cleanup_stats = {
            'cleaned_daily_backups': 0,
            'cleaned_session_backups': 0,
            'errors': []
        }
        
        try:
            # Clean daily backups
            daily_prefix = f"{self.watermark_prefix}backups/daily/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=daily_prefix
            )
            
            for obj in response.get('Contents', []):
                if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                    try:
                        self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                        cleanup_stats['cleaned_daily_backups'] += 1
                    except Exception as e:
                        cleanup_stats['errors'].append(f"Failed to delete {obj['Key']}: {e}")
            
            # Clean session backups
            sessions_prefix = f"{self.watermark_prefix}backups/sessions/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=sessions_prefix
            )
            
            for obj in response.get('Contents', []):
                if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                    try:
                        self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
                        cleanup_stats['cleaned_session_backups'] += 1
                    except Exception as e:
                        cleanup_stats['errors'].append(f"Failed to delete {obj['Key']}: {e}")
            
            logger.info(f"Watermark backup cleanup completed", cleanup_stats=cleanup_stats)
            
        except Exception as e:
            cleanup_stats['errors'].append(f"Cleanup operation failed: {e}")
            logger.error(f"Watermark backup cleanup failed: {e}")
        
        return cleanup_stats
    
    def update_mysql_watermark(
        self,
        table_name: str,
        extraction_time: datetime,
        max_data_timestamp: Optional[datetime] = None,
        last_processed_id: Optional[int] = None,
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
            
            # Update timestamp only if it's newer (forward progress)
            if max_data_timestamp:
                new_timestamp = max_data_timestamp.isoformat() + 'Z'
                if not watermark.last_mysql_data_timestamp or new_timestamp > watermark.last_mysql_data_timestamp:
                    watermark.last_mysql_data_timestamp = new_timestamp
            
            # Update last processed ID for row-based pagination
            if last_processed_id is not None:
                watermark.last_processed_id = last_processed_id
            
            # Make row extraction additive for cumulative progress tracking
            if rows_extracted > 0:
                current_rows = watermark.mysql_rows_extracted or 0
                watermark.mysql_rows_extracted = current_rows + rows_extracted
                logger.info(f"Additive watermark update: {current_rows} + {rows_extracted} = {watermark.mysql_rows_extracted}")
            
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
        processed_files: Optional[List[str]] = None,
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
            
            # Track processed S3 files to prevent re-loading
            if processed_files:
                if not watermark.processed_s3_files:
                    watermark.processed_s3_files = []
                # Add new files to the list (avoid duplicates)
                for file_path in processed_files:
                    if file_path not in watermark.processed_s3_files:
                        watermark.processed_s3_files.append(file_path)
            
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
        
        # A watermark is valid for incremental processing if it has a data timestamp,
        # regardless of mysql_status (data may exist even if status is failed/pending)
        if watermark and watermark.last_mysql_data_timestamp:
            # Convert from ISO format back to MySQL format
            dt = datetime.fromisoformat(watermark.last_mysql_data_timestamp.replace('Z', '+00:00'))
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            logger.debug(f"Using incremental start timestamp for {table_name}: {timestamp} (mysql_status: {watermark.mysql_status})")
            return timestamp
        else:
            # No valid watermark found - require explicit user input
            logger.warning(f"No valid watermark found for {table_name} - missing data timestamp")
            raise ValueError(
                f"No watermark found for table '{table_name}'. "
                f"Please set an initial watermark using: "
                f"python -m src.cli.main watermark set -t {table_name} --timestamp 'YYYY-MM-DD HH:MM:SS'"
            )
    
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
            # No table specified - cannot determine appropriate watermark
            logger.warning("get_last_watermark called without table_name")
            raise ValueError(
                "No table name provided for watermark lookup. "
                "Please specify a table name or use table-specific watermark methods."
            )
    
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
        """Delete table watermark completely including all backup locations"""
        try:
            if create_backup:
                backup_key = self.backup_watermark(table_name)
                logger.info(f"Final backup created before complete deletion: {backup_key}")
            
            deleted_locations = []
            
            # Delete primary location
            try:
                s3_key = self._get_table_watermark_key(table_name)
                logger.info(f"Attempting to delete primary watermark at: {s3_key}")
                response = self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=s3_key
                )
                logger.info(f"Primary watermark delete response: {response}")
                deleted_locations.append("primary")
            except Exception as e:
                logger.error(f"Could not delete primary watermark at {s3_key}: {e}")
                # Don't continue if we can't delete the primary location
                raise
            
            # Delete daily backup
            try:
                daily_key = self._get_daily_backup_key(table_name)
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=daily_key
                )
                deleted_locations.append("daily_backup")
            except Exception as e:
                logger.debug(f"Could not delete daily backup (may not exist): {e}")
            
            # Delete session backups - list and delete all
            try:
                session_prefix = f"{self.history_prefix}{table_name.replace('.', '_')}_"
                
                # List all session backups
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=session_prefix
                )
                
                session_count = 0
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            try:
                                self.s3_client.delete_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                session_count += 1
                            except Exception as e:
                                logger.warning(f"Could not delete session backup {obj['Key']}: {e}")
                
                if session_count > 0:
                    deleted_locations.append(f"{session_count}_session_backups")
                    
            except Exception as e:
                logger.warning(f"Could not delete session backups: {e}")
            
            # Verify deletion by trying to read the primary watermark
            try:
                verify_key = self._get_table_watermark_key(table_name)
                logger.info(f"Verifying deletion by checking: {verify_key}")
                self.s3_client.head_object(Bucket=self.bucket_name, Key=verify_key)
                logger.error(f"VERIFICATION FAILED: Watermark still exists at {verify_key} after deletion!")
                return False
            except self.s3_client.exceptions.NoSuchKey:
                logger.info(f"VERIFICATION SUCCESS: Watermark confirmed deleted at {verify_key}")
            except Exception as e:
                logger.warning(f"Could not verify deletion: {e}")
            
            if deleted_locations:
                logger.warning(f"Completely deleted watermark for table {table_name} from: {', '.join(deleted_locations)}")
                return True
            else:
                logger.error(f"Failed to delete any watermark locations for {table_name}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to delete watermark for {table_name}: {e}")
            raise WatermarkError(f"Watermark deletion failed: {e}")
    
    def force_reset_watermark(self, table_name: str) -> bool:
        """
        Force reset watermark by creating a fresh watermark with epoch start.
        
        This bypasses all backup/recovery logic by directly overwriting the watermark
        with a clean slate starting from epoch time.
        """
        try:
            # Create a completely fresh watermark
            fresh_watermark = S3TableWatermark(
                table_name=table_name,
                last_mysql_extraction_time=None,  # No previous extraction
                last_mysql_data_timestamp='1970-01-01T00:00:00Z',  # Epoch start
                last_processed_id=0,  # Start from beginning
                mysql_rows_extracted=0,  # Reset count
                mysql_status='pending',  # Fresh status
                last_redshift_load_time=None,  # No Redshift loads yet
                redshift_rows_loaded=0,
                redshift_status='pending',
                backup_strategy='row_based',  # Use the new strategy
                s3_file_count=0,
                processed_s3_files=None,
                last_error=None,  # Clear all errors
                created_at=datetime.utcnow().isoformat() + 'Z',
                updated_at=datetime.utcnow().isoformat() + 'Z',
                metadata={'force_reset': True, 'reset_time': datetime.utcnow().isoformat() + 'Z'}
            )
            
            # Force save the fresh watermark (will overwrite everything)
            success = self._save_watermark(fresh_watermark)
            
            if success:
                logger.warning(f"Force reset completed for {table_name} - watermark set to epoch start")
                return True
            else:
                logger.error(f"Force reset failed to save new watermark for {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Force reset failed for {table_name}: {e}")
            raise WatermarkError(f"Force reset failed: {e}")
    
    def _save_watermark(self, watermark: S3TableWatermark) -> bool:
        """Save watermark to S3 with multi-location backup"""
        import time
        
        try:
            # Generate session ID for backup tracking
            session_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            
            # Prepare watermark data with backup metadata
            watermark_data = watermark.to_dict()
            watermark_data['backup_metadata'] = {
                'saved_at': datetime.utcnow().isoformat() + 'Z',
                'session_id': session_id,
                'backup_locations': ['primary', 'daily', 'session']
            }
            
            watermark_json = json.dumps(watermark_data, indent=2)
            
            # Get all save locations
            primary_key = self._get_table_watermark_key(watermark.table_name)
            daily_key = self._get_daily_backup_key(watermark.table_name)
            session_key = self._get_session_backup_key(watermark.table_name, session_id)
            
            # Save to primary location with retry
            primary_success = self._save_with_retry(primary_key, watermark_json, watermark, location="primary")
            
            # Save to backup locations (don't fail if backups fail)
            daily_success = self._save_to_backup_location(daily_key, watermark_json, watermark, location="daily")
            session_success = self._save_to_backup_location(session_key, watermark_json, watermark, location="session")
            
            # Verify primary save integrity
            verification_success = self._verify_watermark_save(primary_key, watermark)
            
            # Log backup results
            backup_results = {
                'primary': primary_success,
                'daily': daily_success, 
                'session': session_success,
                'verification': verification_success
            }
            
            if primary_success and verification_success:
                logger.info(
                    f"Watermark saved successfully for {watermark.table_name}",
                    backup_results=backup_results
                )
            else:
                logger.warning(
                    f"Watermark save had issues for {watermark.table_name}",
                    backup_results=backup_results
                )
            
            # Return success only if primary and verification succeed
            return primary_success and verification_success
            
        except Exception as e:
            logger.error(f"Failed to save watermark for {watermark.table_name}: {e}")
            return False
    
    def _get_table_watermark_key(self, table_name: str) -> str:
        """Generate S3 key for table watermark"""
        # Convert settlement.settlement_claim_detail -> settlement_settlement_claim_detail.json
        safe_name = table_name.replace('.', '_')
        return f"{self.tables_prefix}{safe_name}.json"
    
    def _get_daily_backup_key(self, table_name: str) -> str:
        """Generate S3 key for daily backup watermark"""
        safe_name = table_name.replace('.', '_')
        date_str = datetime.utcnow().strftime("%Y%m%d")
        return f"{self.watermark_prefix}backups/daily/{safe_name}_{date_str}.json"
    
    def _get_session_backup_key(self, table_name: str, session_id: str) -> str:
        """Generate S3 key for session backup watermark"""
        safe_name = table_name.replace('.', '_')
        return f"{self.watermark_prefix}backups/sessions/{session_id}/{safe_name}.json"
    
    def _save_with_retry(self, s3_key: str, watermark_json: str, watermark: S3TableWatermark, location: str, max_retries: int = 3) -> bool:
        """Save watermark to S3 with retry logic"""
        import time
        
        for attempt in range(max_retries):
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=watermark_json,
                    ContentType='application/json',
                    Metadata={
                        'table_name': watermark.table_name,
                        'mysql_status': watermark.mysql_status,
                        'redshift_status': watermark.redshift_status,
                        'updated_at': watermark.updated_at or '',
                        'backup_location': location,
                        'save_attempt': str(attempt + 1)
                    }
                )
                
                # Verify the save by checking object exists
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                logger.debug(f"Successfully saved watermark to {location} location (attempt {attempt + 1})")
                return True
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed to save watermark to {location} location: {e}")
                if attempt < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    sleep_time = 2 ** attempt
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All {max_retries} attempts failed to save watermark to {location} location")
        
        return False
    
    def _save_to_backup_location(self, s3_key: str, watermark_json: str, watermark: S3TableWatermark, location: str) -> bool:
        """Save watermark to backup location (single attempt, don't fail main operation)"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=watermark_json,
                ContentType='application/json',
                Metadata={
                    'table_name': watermark.table_name,
                    'mysql_status': watermark.mysql_status,
                    'redshift_status': watermark.redshift_status,
                    'updated_at': watermark.updated_at or '',
                    'backup_location': location,
                    'backup_type': 'redundant'
                }
            )
            
            logger.debug(f"Successfully saved watermark backup to {location} location")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to save watermark backup to {location} location: {e}")
            return False
    
    def _verify_watermark_save(self, s3_key: str, expected_watermark: S3TableWatermark) -> bool:
        """Verify that saved watermark matches expected data"""
        try:
            # Read back the saved watermark
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            saved_data = json.loads(response['Body'].read())
            
            # Remove backup metadata for comparison
            if 'backup_metadata' in saved_data:
                del saved_data['backup_metadata']
            
            # Verify critical fields match
            expected_data = expected_watermark.to_dict()
            critical_fields = ['table_name', 'last_mysql_data_timestamp', 'mysql_rows_extracted', 'mysql_status']
            
            for field in critical_fields:
                if saved_data.get(field) != expected_data.get(field):
                    logger.error(f"Watermark verification failed for {field}: saved={saved_data.get(field)}, expected={expected_data.get(field)}")
                    return False
            
            logger.debug("Watermark save verification passed")
            return True
            
        except Exception as e:
            logger.error(f"Watermark save verification failed: {e}")
            return False
    
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
    
    def _update_watermark_direct(
        self,
        table_name: str,
        watermark_data: Dict[str, Any]
    ) -> bool:
        """
        Update watermark with direct values (bypassing additive logic).
        
        Used for final absolute watermark updates to prevent double-counting bugs.
        
        Args:
            table_name: Name of the table
            watermark_data: Dictionary of watermark fields to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get existing watermark or create new one
            watermark = self.get_table_watermark(table_name)
            if not watermark:
                watermark = S3TableWatermark(
                    table_name=table_name,
                    created_at=datetime.utcnow().isoformat() + 'Z'
                )
            
            # Update fields directly from the data (no additive logic)
            for field, value in watermark_data.items():
                if hasattr(watermark, field):
                    setattr(watermark, field, value)
            
            # Always update the timestamp
            watermark.updated_at = datetime.utcnow().isoformat() + 'Z'
            
            # Save to S3
            success = self._save_watermark(watermark)
            
            if success:
                logger.info(
                    f"Direct watermark update for {table_name}",
                    updated_fields=list(watermark_data.keys()),
                    absolute_update=True
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to update watermark directly for {table_name}: {e}")
            raise WatermarkError(f"Failed to update watermark directly: {e}")