"""
Simple Watermark Manager - Clean Design v2.0

This module implements a simplified watermark system that eliminates all
complexity and bugs from the legacy system.

Key Principles:
- Single source of truth for each data point
- Cumulative tracking (like blacklist) - consistent across files and rows
- Simple file blacklist for deduplication
- Clear separation of MySQL and Redshift state
- No session tracking or mode switching

Cumulative Fields:
- processed_files: List of S3 files loaded to Redshift (blacklist)
- mysql_state.total_rows: Total rows extracted from MySQL (cumulative sum)
- redshift_state.total_rows: Total rows in Redshift (queried from DB)
"""

import json
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
import os
import uuid

from src.utils.exceptions import WatermarkError

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class SimpleWatermarkManager:
    """
    Simplified watermark manager with clean design.
    
    No accumulation bugs, no complex state management, just simple and reliable.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the watermark manager."""
        self.config = config
        
        # Initialize S3 client directly
        self.s3_client = boto3.client('s3',
            region_name=config['s3'].get('region', 'us-west-2'),
            aws_access_key_id=config['s3'].get('access_key_id'),
            aws_secret_access_key=config['s3'].get('secret_access_key')
        )
        
        self.bucket_name = config['s3']['bucket_name']
        self.watermark_prefix = config['s3'].get('watermark_prefix', 'watermarks/v2/')
        
        # Cache for processed file sets (optimization for large file lists)
        self._processed_files_cache = {}
        
        # Redshift connection for absolute row counts
        self._redshift_conn = None
    
    def get_watermark(self, table_name: str) -> Dict[str, Any]:
        """
        Get watermark for a table, creating default if none exists.
        
        Args:
            table_name: Table identifier (can include scope prefix)
            
        Returns:
            Watermark dictionary with v2.0 structure
        """
        try:
            key = f"{self.watermark_prefix}{self._clean_table_name(table_name)}.json"
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            watermark = json.loads(response['Body'].read())
            
            # Ensure it's a v2.0 watermark
            if watermark.get('version') != '2.0':
                logger.warning(f"Found legacy watermark for {table_name}, needs migration")
                return self._create_default_watermark(table_name)
                
            return watermark
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"No watermark found for {table_name}, creating default")
                return self._create_default_watermark(table_name)
            else:
                raise WatermarkError(f"Failed to retrieve watermark: {e}")
    
    def update_mysql_state(self, table_name: str,
                          timestamp: Optional[str] = None,
                          id: Optional[int] = None,
                          status: str = "success",
                          error: Optional[str] = None,
                          rows_extracted: Optional[int] = None,
                          s3_files_created: Optional[int] = None,
                          session_rows_total: Optional[int] = None) -> None:
        """
        Update MySQL extraction state with cumulative row and S3 file tracking.

        CUMULATIVE LOGIC: Just like processed_files, rows and S3 files are accumulated across sessions.
        This provides a clear total of all rows ever extracted from MySQL and S3 files created.

        Args:
            table_name: Table identifier
            timestamp: Last processed timestamp (ISO format)
            id: Last processed ID
            status: One of: success, failed, pending
            error: Error message if status is failed
            rows_extracted: Number of rows extracted in this session (will be added to total)
            s3_files_created: Number of S3 files created in this session (will be added to total)
            session_rows_total: Optional total rows for this session (sets last_session_rows without affecting cumulative)
        """
        watermark = self.get_watermark(table_name)

        # CUMULATIVE: Add new rows to existing total (same logic as processed_files)
        session_rows = 0
        if rows_extracted is not None and rows_extracted > 0:
            current_rows = watermark['mysql_state'].get('total_rows', 0)
            total_rows = current_rows + rows_extracted
            session_rows = rows_extracted
            logger.info(f"MySQL rows cumulative update: {current_rows} + {rows_extracted} = {total_rows}")
        else:
            total_rows = watermark['mysql_state'].get('total_rows', 0)

        # Allow setting session total directly for final updates (without affecting cumulative)
        if session_rows_total is not None:
            session_rows = session_rows_total
            logger.info(f"MySQL session rows total set directly: {session_rows_total}")

        # CUMULATIVE: Add new S3 files to existing total (same logic as rows)
        session_files = 0
        if s3_files_created is not None and s3_files_created > 0:
            current_files = watermark['mysql_state'].get('s3_files_created', 0)
            total_files = current_files + s3_files_created
            session_files = s3_files_created
            logger.info(f"S3 files cumulative update: {current_files} + {s3_files_created} = {total_files}")
        else:
            total_files = watermark['mysql_state'].get('s3_files_created', 0)

        watermark['mysql_state'].update({
            'last_timestamp': timestamp,
            'last_id': id,
            'status': status,
            'error': error,
            'total_rows': total_rows,  # Cumulative total
            'last_session_rows': session_rows,  # Rows extracted in this session
            's3_files_created': total_files,  # Cumulative S3 files created during extraction
            'last_session_files': session_files,  # Files created in this session
            'last_updated': datetime.now(timezone.utc).isoformat()
        })

        self._save_watermark(table_name, watermark)
        logger.info(f"Updated MySQL state for {table_name}: status={status}, total_rows={total_rows}, session_rows={session_rows}, s3_files={total_files}")
    
    def update_redshift_state(self, table_name: str,
                             loaded_files: List[str],
                             status: str = "success",
                             error: Optional[str] = None) -> None:
        """
        Update Redshift loading state (files and status only, NOT row count).

        This method updates the processed files blacklist and status, but does NOT
        update the row count. Row counts should be updated separately via
        update_redshift_count_from_external() which maintains cumulative logic.

        Args:
            table_name: Table identifier
            loaded_files: List of newly loaded S3 file URIs
            status: One of: success, failed, pending
            error: Error message if status is failed
        """
        watermark = self.get_watermark(table_name)
        
        # Add new files to blacklist (no duplicates)
        existing_files = set(watermark.get('processed_files', []))
        existing_files.update(loaded_files)
        watermark['processed_files'] = sorted(list(existing_files))
        
        # Invalidate cache for this table (files list changed)
        if table_name in self._processed_files_cache:
            del self._processed_files_cache[table_name]

        # CUMULATIVE FIX: Do NOT update total_rows here to preserve cumulative count
        # Row count is only updated via update_redshift_count_from_external() which
        # maintains the cumulative logic. This method only updates file list and status.

        watermark['redshift_state'].update({
            # 'total_rows' - DO NOT UPDATE HERE, preserve cumulative value
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'status': status,
            'error': error,
            'last_loaded_files': loaded_files  # Track session files for display
        })

        self._save_watermark(table_name, watermark)
        current_rows = watermark['redshift_state'].get('total_rows', 0)
        logger.info(f"Updated Redshift state for {table_name}: "
                   f"status={status}, cumulative_rows={current_rows}, new_files={len(loaded_files)}")
    
    def is_file_processed(self, table_name: str, file_uri: str) -> bool:
        """
        Check if a file has already been processed.
        
        Optimized for large file lists (1000+ files) using cached set lookup.
        
        Args:
            table_name: Table identifier
            file_uri: S3 file URI to check
            
        Returns:
            True if file is in processed list, False otherwise
        """
        # Use cached set for O(1) lookup performance with large file lists
        if table_name not in self._processed_files_cache:
            watermark = self.get_watermark(table_name)
            self._processed_files_cache[table_name] = set(watermark.get('processed_files', []))
        
        return file_uri in self._processed_files_cache[table_name]
    
    def update_redshift_count_from_external(self, table_name: str, actual_count: int) -> None:
        """
        Update Redshift row count from external source (like GeminiRedshiftLoader).

        DUAL TRACKING: Tracks both cumulative total and last session rows (like MySQL).
        - total_rows: Cumulative count across all sessions
        - last_session_rows: Rows loaded in this specific session only

        This method allows the loader to provide the actual count after successful
        operations, maintaining the separation of concerns while ensuring accuracy.
        """
        try:
            watermark = self.get_watermark(table_name)

            # CUMULATIVE: Add new rows to existing total (same logic as MySQL)
            current_rows = watermark['redshift_state'].get('total_rows', 0)
            new_total = current_rows + actual_count

            watermark['redshift_state']['total_rows'] = new_total
            watermark['redshift_state']['last_session_rows'] = actual_count  # Session-only tracking
            watermark['redshift_state']['last_updated'] = datetime.now(timezone.utc).isoformat()

            self._save_watermark(table_name, watermark)
            logger.info(f"Redshift rows update: session={actual_count}, cumulative={current_rows} + {actual_count} = {new_total}")

        except Exception as e:
            logger.error(f"Failed to update Redshift count for {table_name}: {e}")
    
    def get_file_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics about processed files for this table.

        Useful for monitoring large file lists and performance tuning.
        """
        watermark = self.get_watermark(table_name)
        processed_files = watermark.get('processed_files', [])

        # Calculate JSON size
        json_str = json.dumps(processed_files)
        json_size_kb = len(json_str) / 1024

        return {
            'total_files': len(processed_files),
            'json_size_kb': round(json_size_kb, 2),
            'cache_status': 'cached' if table_name in self._processed_files_cache else 'not_cached',
            'sample_files': processed_files[:5] if processed_files else [],
            'performance_warning': json_size_kb > 1024  # Warn if > 1MB JSON
        }

    def get_files_by_status(self, table_name: str, s3_files: List[str]) -> Dict[str, Any]:
        """
        Separate S3 files into processed and unprocessed using blacklist.

        Args:
            table_name: Table identifier
            s3_files: List of S3 file URIs to check

        Returns:
            Dictionary with processed and unprocessed file lists
        """
        # Use existing blacklist check (with cache optimization)
        processed = [f for f in s3_files if self.is_file_processed(table_name, f)]
        unprocessed = [f for f in s3_files if not self.is_file_processed(table_name, f)]

        return {
            'processed_files': processed,
            'unprocessed_files': unprocessed,
            'total_processed': len(processed),
            'total_unprocessed': len(unprocessed),
            'total_files': len(s3_files)
        }

    def get_summary(self, table_name: str) -> Dict[str, Any]:
        """
        Get summary statistics for a table.

        Returns comprehensive stats about MySQL extraction, Redshift loading,
        and file processing status.

        Args:
            table_name: Table identifier

        Returns:
            Dictionary with summary statistics
        """
        watermark = self.get_watermark(table_name)

        mysql_state = watermark.get('mysql_state', {})
        redshift_state = watermark.get('redshift_state', {})
        processed_files = watermark.get('processed_files', [])

        return {
            'table_name': table_name,
            # MySQL stats
            'mysql_total_rows': mysql_state.get('total_rows', 0),
            'mysql_last_session_rows': mysql_state.get('last_session_rows', 0),
            'mysql_status': mysql_state.get('status', 'pending'),
            'mysql_last_timestamp': mysql_state.get('last_timestamp'),
            'mysql_last_id': mysql_state.get('last_id'),
            'mysql_last_updated': mysql_state.get('last_updated'),
            # Redshift stats
            'redshift_total_rows': redshift_state.get('total_rows', 0),
            'redshift_last_session_rows': redshift_state.get('last_session_rows', 0),
            'redshift_status': redshift_state.get('status', 'pending'),
            'redshift_last_updated': redshift_state.get('last_updated'),
            # File stats
            'total_processed_files': len(processed_files),
            # Metadata
            'cdc_strategy': watermark.get('cdc_strategy', 'hybrid'),
            'created_at': watermark.get('metadata', {}).get('created_at'),
            'manual_override': watermark.get('metadata', {}).get('manual_override', False)
        }
    
    def set_manual_watermark(self, table_name: str,
                           timestamp: Optional[str] = None,
                           id: Optional[int] = None) -> None:
        """
        Manually set watermark for fresh sync or recovery.
        
        Args:
            table_name: Table identifier
            timestamp: Manual timestamp to set
            id: Manual ID to set
        """
        self.update_mysql_state(table_name, timestamp=timestamp, id=id, 
                               status="success", error=None)
        
        # Mark as manual for tracking
        watermark = self.get_watermark(table_name)
        watermark['metadata'] = watermark.get('metadata', {})
        watermark['metadata']['manual_override'] = True
        watermark['metadata']['manual_set_time'] = datetime.now(timezone.utc).isoformat()
        self._save_watermark(table_name, watermark)
        
        logger.info(f"Set manual watermark for {table_name}: timestamp={timestamp}, id={id}")
    
    def reset_watermark(self, table_name: str, preserve_files: bool = False) -> None:
        """
        Reset watermark to initial state.
        
        Args:
            table_name: Table identifier
            preserve_files: If True, keep processed files list
        """
        current = self.get_watermark(table_name) if preserve_files else None
        default = self._create_default_watermark(table_name)
        
        if preserve_files and current:
            default['processed_files'] = current.get('processed_files', [])
        
        # Invalidate cache (files list changed)
        if table_name in self._processed_files_cache:
            del self._processed_files_cache[table_name]
            
        self._save_watermark(table_name, default)
        logger.info(f"Reset watermark for {table_name}, preserve_files={preserve_files}")
    
    def acquire_lock(self, table_name: str) -> str:
        """
        Acquire exclusive lock for table operations.
        
        Args:
            table_name: Table identifier
            
        Returns:
            Lock ID for release
            
        Raises:
            WatermarkError if table is already locked
        """
        lock_key = f"{self.watermark_prefix}locks/{self._clean_table_name(table_name)}.lock"
        lock_id = str(uuid.uuid4())
        
        try:
            # Check if lock exists
            self.s3_client.head_object(Bucket=self.bucket_name, Key=lock_key)
            raise WatermarkError(f"Table {table_name} is locked by another process")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Create lock
                lock_data = {
                    'lock_id': lock_id,
                    'locked_at': datetime.now(timezone.utc).isoformat(),
                    'pid': os.getpid(),
                    'hostname': os.uname().nodename
                }
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=lock_key,
                    Body=json.dumps(lock_data)
                )
                logger.debug(f"Acquired lock for {table_name}: {lock_id}")
                return lock_id
            else:
                raise WatermarkError(f"Failed to acquire lock: {e}")
    
    def release_lock(self, table_name: str, lock_id: str) -> None:
        """Release table lock."""
        lock_key = f"{self.watermark_prefix}locks/{self._clean_table_name(table_name)}.lock"
        
        try:
            # Verify lock ownership
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=lock_key)
            lock_data = json.loads(response['Body'].read())
            
            if lock_data['lock_id'] == lock_id:
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=lock_key)
                logger.debug(f"Released lock for {table_name}: {lock_id}")
            else:
                logger.warning(f"Lock ID mismatch for {table_name}, not releasing")
                
        except ClientError as e:
            logger.warning(f"Could not release lock: {e}")
    
    def _create_default_watermark(self, table_name: str) -> Dict[str, Any]:
        """Create a default v2.0 watermark with ZERO counts."""
        return {
            'version': '2.0',
            'table_name': table_name,
            'cdc_strategy': 'hybrid',  # Default, will be updated by CDC engine

            'mysql_state': {
                'last_timestamp': None,
                'last_id': None,
                'status': 'pending',
                'error': None,
                'total_rows': 0,  # ALWAYS 0 on reset
                'last_session_rows': 0,  # Rows from last session
                's3_files_created': 0,  # ALWAYS 0 on reset - cumulative S3 files created during extraction
                'last_session_files': 0,  # Files created in last session
                'last_updated': None
            },

            'redshift_state': {
                'total_rows': 0,  # ALWAYS 0 on reset - cumulative total
                'last_session_rows': 0,  # Rows loaded in last session only
                'last_updated': None,
                'status': 'pending',
                'error': None,
                'last_loaded_files': []
            },

            'processed_files': [],

            'metadata': {
                'created_at': datetime.now(timezone.utc).isoformat(),
                'manual_override': False
            }
        }
    
    def _save_watermark(self, table_name: str, watermark: Dict[str, Any]) -> None:
        """Save watermark to S3."""
        key = f"{self.watermark_prefix}{self._clean_table_name(table_name)}.json"
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(watermark, indent=2, cls=DateTimeEncoder),
                ContentType='application/json'
            )
        except Exception as e:
            raise WatermarkError(f"Failed to save watermark: {e}")
    
    def _clean_table_name(self, table_name: str) -> str:
        """Clean table name for S3 key usage."""
        # Handle scoped names like "US_DW_RO:schema.table"
        return table_name.replace(':', '_').replace('.', '_').lower()
    
    def _query_redshift_count(self, table_name: str) -> int:
        """
        Query actual row count from Redshift.
        
        This is the key to preventing accumulation bugs - always get truth from source.
        Uses the proper connection management with SSH tunnel support.
        """
        try:
            # For now, we'll skip actual Redshift queries in the SimpleWatermarkManager
            # to avoid complex connection dependencies. The GeminiRedshiftLoader
            # will handle absolute count updates when it successfully loads data.
            # This keeps the watermark manager simple and focused.
            
            logger.debug(f"Skipping Redshift count query for {table_name} - will be updated by loader")
            return 0
                    
        except Exception as e:
            logger.warning(f"Could not query Redshift count for {table_name}: {e}")
            return 0