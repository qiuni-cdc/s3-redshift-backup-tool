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
    backup_s3_files: Optional[List[str]] = None     # Track S3 files created during backup
    processed_s3_files: Optional[List[str]] = None  # Track S3 files loaded to Redshift
    last_error: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    last_session_id: Optional[str] = None  # MySQL session ID for mode detection
    last_redshift_session_id: Optional[str] = None  # Redshift session ID for accumulation logic
    id_range_extracted: Optional[Dict[str, int]] = None  # Track ID ranges for better visibility
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        from datetime import datetime
        
        result = asdict(self)
        # Ensure metadata is not None
        if result['metadata'] is None:
            result['metadata'] = {}
        
        # Handle datetime serialization and None values
        for key, value in result.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat() + 'Z' if value.tzinfo is None else value.isoformat()
            elif value is None:
                result[key] = None  # Explicitly set None for JSON serialization
            elif isinstance(value, datetime):
                # Convert datetime to ISO format string for JSON serialization
                result[key] = value.isoformat() + 'Z' if value.tzinfo is None else value.isoformat()
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'S3TableWatermark':
        """Create from dictionary (JSON deserialization)"""
        from datetime import datetime
        
        # Handle missing fields and datetime deserialization
        processed_data = {}
        for k, v in data.items():
            if k in cls.__dataclass_fields__:
                # Handle datetime deserialization
                if v is not None and isinstance(v, str):
                    # Check if it's a datetime field that needs conversion
                    field_type = cls.__dataclass_fields__[k].type
                    if hasattr(field_type, '__origin__') and field_type.__origin__ is type(Optional[datetime]):
                        # Optional[datetime] field
                        try:
                            processed_data[k] = datetime.fromisoformat(v.replace('Z', '+00:00'))
                        except (ValueError, AttributeError):
                            processed_data[k] = v
                    elif field_type == datetime:
                        # Direct datetime field
                        try:
                            processed_data[k] = datetime.fromisoformat(v.replace('Z', '+00:00'))
                        except (ValueError, AttributeError):
                            processed_data[k] = v
                    else:
                        processed_data[k] = v
                else:
                    processed_data[k] = v
        
        return cls(**processed_data)


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
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update MySQL extraction watermark.
        
        SIMPLIFIED: Always use the provided row count as absolute value.
        No accumulation, no modes, no complexity.
        """
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
            
            # SIMPLIFIED: Always use absolute count
            if rows_extracted > 0:
                watermark.mysql_rows_extracted = rows_extracted
                logger.info(f"Updated watermark with absolute count: {rows_extracted}")
            
            watermark.mysql_status = status
            watermark.backup_strategy = backup_strategy
            
            # CRITICAL FIX: Preserve backup_s3_files and sync s3_file_count
            # If backup_s3_files exists, use its length for s3_file_count
            if hasattr(watermark, 'backup_s3_files') and watermark.backup_s3_files:
                watermark.s3_file_count = len(watermark.backup_s3_files)
                logger.debug(f"Syncing s3_file_count from backup_s3_files: {watermark.s3_file_count}")
            else:
                watermark.s3_file_count = s3_file_count
            
            watermark.updated_at = datetime.utcnow().isoformat() + 'Z'
            
            # Merge metadata if provided (for full_sync_mode and other CDC strategy metadata)
            if metadata:
                existing_metadata = watermark.metadata or {}
                existing_metadata.update(metadata)
                watermark.metadata = existing_metadata
            
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
        error_message: Optional[str] = None,
        mode: str = 'auto',  # NEW: Add mode parameter for accumulation control
        session_id: Optional[str] = None  # NEW: Add session tracking
    ) -> bool:
        """Update Redshift load watermark with accumulation support (FIXES DOUBLE-COUNTING BUG)"""
        try:
            # Get existing watermark
            watermark = self.get_table_watermark(table_name)
            if not watermark:
                raise WatermarkError(f"No existing watermark for table {table_name}")
            
            # Update Redshift-related fields
            watermark.last_redshift_load_time = load_time.isoformat() + 'Z'
            watermark.redshift_status = status
            watermark.updated_at = datetime.utcnow().isoformat() + 'Z'
            
            # CRITICAL FIX: Mode-controlled row count update (prevent double-counting bug)
            if rows_loaded > 0:
                effective_mode = mode
                
                # Auto mode detection based on session ID
                if mode == 'auto':
                    # Same session = absolute (replace), different session = additive (accumulate)
                    last_redshift_session = getattr(watermark, 'last_redshift_session_id', None)
                    if session_id and last_redshift_session == session_id:
                        effective_mode = 'absolute'  # Same session - replace count
                        logger.debug(f"Redshift auto mode: same session '{session_id}', using absolute")
                    else:
                        effective_mode = 'additive'  # Different session - add to total
                        logger.debug(f"Redshift auto mode: different session (last='{last_redshift_session}', current='{session_id}'), using additive")
                
                # Apply the determined mode
                if effective_mode == 'absolute':
                    # Replace existing count (for same session updates)
                    previous_count = watermark.redshift_rows_loaded or 0
                    watermark.redshift_rows_loaded = rows_loaded
                    logger.info(f"Redshift absolute watermark update: replaced {previous_count} with {rows_loaded}")
                    
                elif effective_mode == 'additive':
                    # Add to existing count (for cross-session accumulation) 
                    current_rows = watermark.redshift_rows_loaded or 0
                    watermark.redshift_rows_loaded = current_rows + rows_loaded
                    logger.info(f"Redshift additive watermark update: {current_rows} + {rows_loaded} = {watermark.redshift_rows_loaded}")
                
                else:
                    raise ValueError(f"Invalid watermark mode: {effective_mode}")
                
                # Store session ID for future auto mode detection
                if session_id:
                    watermark.last_redshift_session_id = session_id
            
            # SIMPLIFIED FILE MOVEMENT LOGIC
            if processed_files is not None and status == 'success':
                # Initialize lists if needed
                if not watermark.processed_s3_files:
                    watermark.processed_s3_files = []
                if not hasattr(watermark, 'backup_s3_files') or not watermark.backup_s3_files:
                    watermark.backup_s3_files = []
                
                # For each successfully loaded file:
                # 1. Add to processed_s3_files (if not already there)
                # 2. Remove from backup_s3_files (if present)
                for file_path in processed_files:
                    # Add to processed files
                    if file_path not in watermark.processed_s3_files:
                        watermark.processed_s3_files.append(file_path)
                        logger.info(f"Added to processed files: {file_path.split('/')[-1]}")
                    
                    # Remove from backup files
                    if file_path in watermark.backup_s3_files:
                        watermark.backup_s3_files.remove(file_path)
                        logger.info(f"Removed from backup files: {file_path.split('/')[-1]}")
                
                # Update file count
                backup_count = len(watermark.backup_s3_files)
                processed_count = len(watermark.processed_s3_files)
                watermark.s3_file_count = backup_count + processed_count
                
                logger.info(f"File movement complete: {backup_count} backup, {processed_count} processed files")
                
            elif processed_files is not None and status != 'success':
                logger.warning(f"Redshift status is '{status}', not moving {len(processed_files)} files")
            # If processed_files is None, preserve existing lists unchanged
                
                # MEMORY LEAK FIX: Implement file list rotation to prevent memory exhaustion
                max_tracked_files = 5000  # Reasonable limit for production
                if len(watermark.processed_s3_files) > max_tracked_files:
                    # Keep most recent files, remove oldest
                    files_to_remove = len(watermark.processed_s3_files) - max_tracked_files
                    watermark.processed_s3_files = watermark.processed_s3_files[files_to_remove:]
                    logger.info(f"Rotated processed files list: removed {files_to_remove} oldest entries, "
                              f"kept {len(watermark.processed_s3_files)} recent files")
                
                # Additional cleanup: remove files older than 30 days based on naming pattern
                self._cleanup_old_processed_files(watermark)
            
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
    
    def _cleanup_old_processed_files(self, watermark: S3TableWatermark):
        """
        Clean up processed files list by removing files older than 30 days based on naming patterns.
        
        Args:
            watermark: The watermark object with processed_s3_files list to clean up
        """
        if not watermark.processed_s3_files:
            return
        
        try:
            from datetime import datetime, timedelta
            import re
            
            # Define cutoff date (30 days ago)
            cutoff_date = datetime.now() - timedelta(days=30)
            
            # Track files to remove
            files_to_remove = []
            
            for file_path in watermark.processed_s3_files:
                try:
                    # Extract timestamp from S3 file path patterns
                    # Common patterns: "YYYYMMDD_HHMMSS", "YYYY-MM-DD", "batch_YYYYMMDD"
                    timestamp_patterns = [
                        r'(\d{8})_(\d{6})',        # YYYYMMDD_HHMMSS
                        r'(\d{4}-\d{2}-\d{2})',    # YYYY-MM-DD
                        r'batch_(\d{8})',          # batch_YYYYMMDD
                        r'(\d{4})(\d{2})(\d{2})',  # YYYYMMDD
                    ]
                    
                    file_timestamp = None
                    
                    for pattern in timestamp_patterns:
                        match = re.search(pattern, file_path)
                        if match:
                            if len(match.groups()) == 2:  # YYYYMMDD_HHMMSS
                                date_str = match.group(1)
                                time_str = match.group(2)
                                file_timestamp = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                            elif len(match.groups()) == 1:  # YYYY-MM-DD or batch_YYYYMMDD
                                date_str = match.group(1).replace('-', '')
                                if len(date_str) == 8:  # YYYYMMDD
                                    file_timestamp = datetime.strptime(date_str, "%Y%m%d")
                                elif len(date_str) == 10:  # YYYY-MM-DD
                                    file_timestamp = datetime.strptime(date_str, "%Y-%m-%d")
                            elif len(match.groups()) == 3:  # YYYYMMDD split
                                year, month, day = match.groups()
                                file_timestamp = datetime.strptime(f"{year}{month}{day}", "%Y%m%d")
                            break
                    
                    # If we found a timestamp and it's older than cutoff, mark for removal
                    if file_timestamp and file_timestamp < cutoff_date:
                        files_to_remove.append(file_path)
                        
                except Exception as e:
                    # If we can't parse the timestamp, keep the file to be safe
                    logger.debug(f"Could not parse timestamp from {file_path}: {e}")
                    continue
            
            # Remove old files
            if files_to_remove:
                for file_path in files_to_remove:
                    watermark.processed_s3_files.remove(file_path)
                
                logger.info(f"Time-based cleanup removed {len(files_to_remove)} files older than 30 days "
                          f"({len(watermark.processed_s3_files)} files remaining)")
            else:
                logger.debug("Time-based cleanup: no files older than 30 days found")
                
        except Exception as e:
            logger.warning(f"Time-based processed files cleanup failed: {e}")
    
    def _acquire_watermark_lock(self, lock_key: str, operation_id: str) -> bool:
        """
        Acquire a distributed lock for watermark operations using S3.
        
        Args:
            lock_key: S3 key for the lock file
            operation_id: Unique operation identifier
            
        Returns:
            True if lock acquired successfully, False otherwise
        """
        try:
            # Create lock content with operation metadata
            lock_data = {
                'operation_id': operation_id,
                'acquired_at': datetime.utcnow().isoformat() + 'Z',
                'ttl_seconds': 300,  # 5 minutes TTL to prevent permanent locks
                'process_info': {
                    'pid': getattr(__import__('os'), 'getpid', lambda: 'unknown')(),
                    'hostname': getattr(__import__('socket'), 'gethostname', lambda: 'unknown')()
                }
            }
            
            lock_json = json.dumps(lock_data)
            
            # Use S3 conditional put to implement atomic lock acquisition
            # This prevents race conditions by ensuring only one operation can create the lock
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=lock_key,
                    Body=lock_json,
                    ContentType='application/json',
                    # Ensure lock doesn't exist (atomic check-and-set)
                    IfNoneMatch='*'  # Only create if object doesn't exist
                )
                logger.debug(f"Acquired watermark lock: {lock_key} (operation: {operation_id})")
                return True
                
            except self.s3_client.exceptions.PreconditionFailed:
                # Lock already exists - check if it's expired
                try:
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=lock_key)
                    existing_lock = json.loads(response['Body'].read().decode('utf-8'))
                    
                    # Check if lock is expired (TTL-based cleanup)
                    lock_time = datetime.fromisoformat(existing_lock['acquired_at'].replace('Z', '+00:00'))
                    ttl_seconds = existing_lock.get('ttl_seconds', 300)
                    
                    if (datetime.utcnow().replace(tzinfo=__import__('datetime').timezone.utc) - lock_time).total_seconds() > ttl_seconds:
                        # Lock is expired, try to clean it up and retry
                        logger.info(f"Found expired lock, attempting cleanup: {lock_key}")
                        self._release_watermark_lock(lock_key, existing_lock['operation_id'])
                        
                        # Retry lock acquisition after cleanup
                        try:
                            self.s3_client.put_object(
                                Bucket=self.bucket_name,
                                Key=lock_key,
                                Body=lock_json,
                                ContentType='application/json',
                                IfNoneMatch='*'
                            )
                            logger.debug(f"Acquired watermark lock after cleanup: {lock_key}")
                            return True
                        except:
                            logger.debug(f"Failed to acquire lock after cleanup, another process likely got it: {lock_key}")
                            return False
                    else:
                        logger.debug(f"Active lock exists: {lock_key} (held by operation: {existing_lock.get('operation_id', 'unknown')})")
                        return False
                        
                except Exception as e:
                    logger.debug(f"Could not read existing lock {lock_key}: {e}")
                    return False
                    
        except Exception as e:
            logger.warning(f"Failed to acquire watermark lock {lock_key}: {e}")
            return False
    
    def _release_watermark_lock(self, lock_key: str, operation_id: str):
        """
        Release a distributed lock for watermark operations.
        
        Args:
            lock_key: S3 key for the lock file  
            operation_id: Operation identifier that acquired the lock
        """
        try:
            # Verify we own the lock before releasing it
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=lock_key)
                existing_lock = json.loads(response['Body'].read().decode('utf-8'))
                
                if existing_lock.get('operation_id') != operation_id:
                    logger.warning(f"Cannot release lock {lock_key}: owned by different operation {existing_lock.get('operation_id')}")
                    return
            except self.s3_client.exceptions.NoSuchKey:
                logger.debug(f"Lock {lock_key} already released or expired")
                return
            except Exception as e:
                logger.warning(f"Could not verify lock ownership for {lock_key}: {e}")
                return
            
            # Delete the lock
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=lock_key)
            logger.debug(f"Released watermark lock: {lock_key} (operation: {operation_id})")
            
        except Exception as e:
            logger.warning(f"Failed to release watermark lock {lock_key}: {e}")
    
    def _verify_watermark_save_with_backoff(self, primary_key: str, expected_watermark: S3TableWatermark, operation_id: str) -> bool:
        """
        Verify watermark save with exponential backoff to handle S3 eventual consistency.
        
        Args:
            primary_key: S3 key for the saved watermark
            expected_watermark: Expected watermark data
            operation_id: Operation ID that should match in saved data
            
        Returns:
            True if verification successful, False otherwise
        """
        import time
        
        max_attempts = 5
        base_delay = 0.1  # 100ms initial delay
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Try to read back the saved watermark
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=primary_key)
                saved_data = json.loads(response['Body'].read().decode('utf-8'))
                
                # Check operation ID to ensure we're reading our own save
                saved_operation_id = saved_data.get('backup_metadata', {}).get('operation_id')
                if saved_operation_id != operation_id:
                    logger.warning(f"Verification found different operation ID: expected {operation_id}, got {saved_operation_id}")
                    if attempt < max_attempts:
                        time.sleep(base_delay * (2 ** (attempt - 1)))  # Exponential backoff
                        continue
                    return False
                
                # Verify critical watermark fields
                if (saved_data.get('table_name') == expected_watermark.table_name and
                    saved_data.get('last_mysql_extraction_time') == expected_watermark.last_mysql_extraction_time and
                    saved_data.get('mysql_status') == expected_watermark.mysql_status and
                    saved_data.get('redshift_status') == expected_watermark.redshift_status):
                    
                    logger.debug(f"Watermark verification successful on attempt {attempt}")
                    return True
                else:
                    logger.warning(f"Watermark verification failed: data mismatch on attempt {attempt}")
                    if attempt < max_attempts:
                        time.sleep(base_delay * (2 ** (attempt - 1)))  # Exponential backoff
                        continue
                    return False
                    
            except self.s3_client.exceptions.NoSuchKey:
                logger.debug(f"Watermark not yet available for verification, attempt {attempt}")
                if attempt < max_attempts:
                    time.sleep(base_delay * (2 ** (attempt - 1)))  # Exponential backoff
                    continue
                return False
                
            except Exception as e:
                logger.warning(f"Watermark verification error on attempt {attempt}: {e}")
                if attempt < max_attempts:
                    time.sleep(base_delay * (2 ** (attempt - 1)))  # Exponential backoff
                    continue
                return False
        
        logger.error(f"Watermark verification failed after {max_attempts} attempts")
        return False
    
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
            
            # ENHANCED: Clear ALL backup copies to prevent automatic recovery
            try:
                logger.info(f"Clearing backup copies for {table_name} to prevent recovery")
                self._clear_all_watermark_backups(table_name)
            except Exception as e:
                logger.warning(f"Failed to clear backup copies: {e}")
            
            # Force save the fresh watermark (will overwrite everything)
            success = self._save_watermark(fresh_watermark)
            
            if success:
                logger.warning(f"Force reset completed for {table_name} - watermark set to epoch start")
                logger.warning(f"All backup copies cleared to prevent automatic recovery")
                return True
            else:
                logger.error(f"Force reset failed to save new watermark for {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Force reset failed for {table_name}: {e}")
            raise WatermarkError(f"Force reset failed: {e}")
    
    def _save_watermark(self, watermark: S3TableWatermark) -> bool:
        """
        Save watermark to S3 with atomic operations and race condition protection.
        
        RACE CONDITION FIX: Implements proper synchronization and delayed verification
        to prevent concurrent access issues and S3 eventual consistency problems.
        """
        import time
        import uuid
        
        try:
            # Generate unique operation ID to prevent race conditions
            operation_id = str(uuid.uuid4())
            session_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            
            # RACE CONDITION FIX: Create atomic lock using S3 object with unique key
            lock_key = f"{self.watermark_prefix}locks/{watermark.table_name.replace('.', '_')}.lock"
            if not self._acquire_watermark_lock(lock_key, operation_id):
                logger.warning(f"Could not acquire watermark lock for {watermark.table_name}, concurrent operation detected")
                time.sleep(0.5)  # Short delay for other operation to complete
                # Try one more time with exponential backoff
                if not self._acquire_watermark_lock(lock_key, operation_id):
                    logger.error(f"Failed to acquire watermark lock after retry for {watermark.table_name}")
                    return False
            
            try:
                # Prepare watermark data with operation metadata
                watermark_data = watermark.to_dict()
                watermark_data['backup_metadata'] = {
                    'saved_at': datetime.utcnow().isoformat() + 'Z',
                    'session_id': session_id,
                    'operation_id': operation_id,
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
                
                # RACE CONDITION FIX: Implement delayed verification with exponential backoff
                # to handle S3 eventual consistency issues
                verification_success = self._verify_watermark_save_with_backoff(primary_key, watermark, operation_id)
                
            finally:
                # Always release lock to prevent deadlocks
                self._release_watermark_lock(lock_key, operation_id)
            
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
    
    def _clear_all_watermark_backups(self, table_name: str) -> bool:
        """
        Clear all watermark backup copies for a table to prevent automatic recovery.
        
        This method removes:
        - Daily backup copies
        - Session backup copies  
        - Historical backups
        
        Used during force_reset_watermark to ensure the reset isn't overridden
        by automatic recovery from backup locations.
        
        Args:
            table_name: Name of the table to clear backups for
            
        Returns:
            True if cleanup succeeded, False otherwise
        """
        try:
            safe_name = table_name.replace('.', '_')
            deleted_count = 0
            errors = []
            
            # Clear daily backup
            try:
                daily_key = self._get_daily_backup_key(table_name)
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=daily_key
                )
                deleted_count += 1
                logger.debug(f"Deleted daily backup: {daily_key}")
            except self.s3_client.exceptions.NoSuchKey:
                logger.debug(f"Daily backup not found (already clean): {daily_key}")
            except Exception as e:
                errors.append(f"Daily backup deletion failed: {e}")
                logger.warning(f"Failed to delete daily backup: {e}")
            
            # Clear session backups
            try:
                sessions_prefix = f"{self.watermark_prefix}backups/sessions/"
                
                # List all session backups for this table
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=sessions_prefix
                )
                
                for page in page_iterator:
                    for obj in page.get('Contents', []):
                        # Check if this backup is for our table
                        if safe_name in obj['Key'] and obj['Key'].endswith('.json'):
                            try:
                                self.s3_client.delete_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                deleted_count += 1
                                logger.debug(f"Deleted session backup: {obj['Key']}")
                            except Exception as e:
                                errors.append(f"Session backup deletion failed {obj['Key']}: {e}")
                                logger.warning(f"Failed to delete session backup {obj['Key']}: {e}")
                                
            except Exception as e:
                errors.append(f"Session backup listing failed: {e}")
                logger.warning(f"Failed to list session backups: {e}")
            
            # Clear historical backups
            try:
                history_prefix = f"{self.history_prefix}"
                
                # List all historical backups for this table
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=history_prefix
                )
                
                for page in page_iterator:
                    for obj in page.get('Contents', []):
                        # Check if this backup is for our table
                        if safe_name in obj['Key'] and obj['Key'].endswith('.json'):
                            try:
                                self.s3_client.delete_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                deleted_count += 1
                                logger.debug(f"Deleted historical backup: {obj['Key']}")
                            except Exception as e:
                                errors.append(f"Historical backup deletion failed {obj['Key']}: {e}")
                                logger.warning(f"Failed to delete historical backup {obj['Key']}: {e}")
                                
            except Exception as e:
                errors.append(f"Historical backup listing failed: {e}")
                logger.warning(f"Failed to list historical backups: {e}")
            
            if deleted_count > 0:
                logger.info(f"Cleared {deleted_count} watermark backup copies for {table_name}")
            else:
                logger.debug(f"No watermark backup copies found for {table_name}")
            
            if errors:
                logger.warning(f"Backup cleanup completed with {len(errors)} errors for {table_name}")
                return False
            else:
                return True
                
        except Exception as e:
            logger.error(f"Failed to clear watermark backups for {table_name}: {e}")
            return False
    
    def _get_table_watermark_key(self, table_name: str) -> str:
        """Generate S3 key for table watermark with v1.2.0 multi-schema support"""
        # FIXED: Use consistent scoped table name cleaning (same as S3Manager/RedshiftLoader)
        safe_name = self._clean_table_name_with_scope(table_name)
        return f"{self.tables_prefix}{safe_name}.json"
    
    def _clean_table_name_with_scope(self, table_name: str) -> str:
        """
        Clean table name for watermark keys with v1.2.0 multi-schema support.
        
        Handles both scoped and unscoped table names:
        - 'settlement.settle_orders' → 'settlement_settle_orders'
        - 'US_DW_RO_SSH:settlement.settle_orders' → 'us_dw_ro_ssh_settlement_settle_orders'
        - 'us_dw_pipeline:settlement.settle_orders' → 'us_dw_pipeline_settlement_settle_orders'
        
        Args:
            table_name: Table name (may include scope prefix)
            
        Returns:
            Cleaned table name suitable for watermark keys
        """
        # Handle scoped table names (v1.2.0 multi-schema)
        if ':' in table_name:
            scope, actual_table = table_name.split(':', 1)
            # Clean scope: lowercase, replace special chars with underscores
            clean_scope = scope.lower().replace('-', '_').replace('.', '_')
            # Clean table: replace dots with underscores
            clean_table = actual_table.replace('.', '_').replace('-', '_')
            # Combine: scope_table_name
            return f"{clean_scope}_{clean_table}"
        else:
            # Unscoped table (v1.0.0 compatibility)
            return table_name.replace('.', '_').replace('-', '_')
    
    def _get_daily_backup_key(self, table_name: str) -> str:
        """Generate S3 key for daily backup watermark with v1.2.0 multi-schema support"""
        # FIXED: Use consistent scoped table name cleaning
        safe_name = self._clean_table_name_with_scope(table_name)
        date_str = datetime.utcnow().strftime("%Y%m%d")
        return f"{self.watermark_prefix}backups/daily/{safe_name}_{date_str}.json"
    
    def _get_session_backup_key(self, table_name: str, session_id: str) -> str:
        """Generate S3 key for session backup watermark with v1.2.0 multi-schema support"""
        # FIXED: Use consistent scoped table name cleaning
        safe_name = self._clean_table_name_with_scope(table_name)
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
        data_timestamp: Optional[datetime] = None,
        data_id: Optional[int] = None
    ) -> bool:
        """
        Set manual watermark for CLI operations (timestamp and/or ID).
        
        This creates a completely fresh watermark with the specified timestamp and/or ID, 
        which enables watermark-based filtering for both timestamp and ID-based CDC strategies.
        
        Args:
            table_name: Name of the table
            data_timestamp: The data cutoff timestamp (optional)
            data_id: The starting ID for ID-based CDC (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate that at least one parameter is provided
            if data_timestamp is None and data_id is None:
                raise ValueError("Must provide either data_timestamp or data_id")
            
            # Create metadata to track manual settings
            metadata = {
                'manual_watermark': True,
                'manual_timestamp': data_timestamp is not None,
                'manual_id': data_id is not None
            }
            
            # If ID is provided, add CDC configuration
            if data_id is not None:
                metadata['cdc_config'] = {
                    'id_start_override': data_id,
                    'manual_id_set': True
                }
            
            # Create a completely fresh watermark (ignore existing)
            watermark = S3TableWatermark(
                table_name=table_name,
                last_mysql_data_timestamp=data_timestamp.isoformat() + 'Z' if data_timestamp else None,
                last_mysql_extraction_time=None,  # Explicitly None for manual watermarks
                last_processed_id=data_id if data_id is not None else 0,  # Use provided ID or reset to 0
                mysql_rows_extracted=0,
                mysql_status='success',
                redshift_rows_loaded=0, 
                redshift_status='pending',
                backup_strategy='manual_cli',
                s3_file_count=0,
                processed_s3_files=[],  # CRITICAL FIX: Reset processed files list
                created_at=datetime.utcnow().isoformat() + 'Z',
                updated_at=datetime.utcnow().isoformat() + 'Z',
                metadata=metadata
            )
            
            success = self._save_watermark(watermark)
            
            if success:
                log_data = {
                    'table': table_name,
                    'watermark_type': 'manual'
                }
                if data_timestamp:
                    log_data['data_timestamp'] = data_timestamp
                if data_id is not None:
                    log_data['data_id'] = data_id
                    
                logger.info(
                    f"Set manual watermark for {table_name}",
                    **log_data
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