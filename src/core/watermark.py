"""
High-watermark management system for incremental backups.

This module manages high-watermark timestamps stored in S3, enabling incremental
data processing by tracking the last successful backup timestamp.
"""

import io
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError, BotoCoreError

from src.config.settings import AppConfig
from src.utils.exceptions import WatermarkError, S3Error
from src.utils.logging import get_logger


logger = get_logger(__name__)


class WatermarkManager:
    """
    Manages high-watermark timestamps for incremental backup operations.
    
    The watermark system stores the timestamp of the last successful backup
    in S3, enabling incremental processing by determining the starting point
    for the next backup operation.
    """
    
    def __init__(self, config: AppConfig, s3_client=None):
        self.config = config
        self.s3_client = s3_client
        self.bucket_name = config.s3.bucket_name
        self.watermark_key = config.s3.high_watermark_key.lstrip('/')
        
        # Default watermark for initial runs
        self.default_watermark = "1970-01-01 00:00:00"
        
        # Watermark metadata
        self._watermark_metadata = {}
    
    def _ensure_s3_client(self):
        """Ensure S3 client is available"""
        if self.s3_client is None:
            raise WatermarkError("S3 client not initialized")
    
    def get_last_watermark(self, table_name: Optional[str] = None) -> str:
        """
        Get the last watermark timestamp.
        
        Args:
            table_name: Optional table-specific watermark
        
        Returns:
            Last watermark timestamp as string (YYYY-MM-DD HH:MM:SS)
        
        Raises:
            WatermarkError: If watermark retrieval fails
        """
        self._ensure_s3_client()
        
        try:
            # Determine watermark key (global or table-specific)
            if table_name:
                watermark_key = f"{self.watermark_key.rstrip('.txt')}_{table_name}.json"
            else:
                watermark_key = self.watermark_key
            
            logger.debug("Retrieving watermark", watermark_key=watermark_key)
            
            # Try to read watermark from S3
            try:
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=watermark_key
                )
                
                # Read watermark content
                content = response['Body'].read().decode('utf-8').strip()
                
                # Handle JSON format (new format with metadata)
                if watermark_key.endswith('.json'):
                    watermark_data = json.loads(content)
                    watermark = watermark_data.get('timestamp', self.default_watermark)
                    self._watermark_metadata = watermark_data.get('metadata', {})
                else:
                    # Handle plain text format (legacy)
                    watermark = content
                    self._watermark_metadata = {}
                
                # Validate watermark format
                self._validate_watermark_format(watermark)
                
                logger.info(
                    "Watermark retrieved successfully",
                    watermark=watermark,
                    table_name=table_name,
                    metadata_keys=list(self._watermark_metadata.keys())
                )
                
                return watermark
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    # Watermark file doesn't exist, return default
                    logger.info(
                        "Watermark file not found, using default",
                        watermark_key=watermark_key,
                        default_watermark=self.default_watermark
                    )
                    return self.default_watermark
                else:
                    raise
            
        except json.JSONDecodeError as e:
            logger.error("Invalid watermark JSON format", error=str(e))
            raise WatermarkError(f"Invalid watermark format: {e}")
        
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to retrieve watermark", error=str(e))
            raise WatermarkError(f"Watermark retrieval failed: {e}")
        
        except Exception as e:
            logger.error("Unexpected watermark retrieval error", error=str(e))
            raise WatermarkError(f"Unexpected error retrieving watermark: {e}")
    
    def update_watermark(
        self,
        timestamp: str,
        table_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        atomic: bool = True
    ) -> bool:
        """
        Update the watermark timestamp.
        
        Args:
            timestamp: New watermark timestamp (YYYY-MM-DD HH:MM:SS)
            table_name: Optional table-specific watermark
            metadata: Additional metadata to store with watermark
            atomic: Whether to perform atomic update (safer but slower)
        
        Returns:
            True if update successful
        
        Raises:
            WatermarkError: If watermark update fails
        """
        self._ensure_s3_client()
        
        try:
            # Validate timestamp format
            self._validate_watermark_format(timestamp)
            
            # Determine watermark key
            if table_name:
                watermark_key = f"{self.watermark_key.rstrip('.txt')}_{table_name}.json"
            else:
                watermark_key = self.watermark_key
            
            # Prepare watermark data
            watermark_data = {
                'timestamp': timestamp,
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'table_name': table_name,
                'metadata': metadata or {}
            }
            
            # Add system metadata
            watermark_data['metadata'].update({
                'update_source': 'backup_system',
                'format_version': '2.0',
                'bucket': self.bucket_name
            })
            
            logger.debug(
                "Updating watermark",
                watermark_key=watermark_key,
                old_watermark=self.get_last_watermark(table_name) if not atomic else "unknown",
                new_watermark=timestamp
            )
            
            if atomic:
                # Atomic update: read current, compare, then update
                old_watermark = self.get_last_watermark(table_name)
                
                # Validate that new watermark is not going backwards
                if self._compare_timestamps(timestamp, old_watermark) < 0:
                    raise WatermarkError(
                        f"New watermark ({timestamp}) is earlier than current ({old_watermark})",
                        watermark_value=timestamp
                    )
            
            # Upload watermark to S3
            if watermark_key.endswith('.json'):
                # JSON format with metadata
                content = json.dumps(watermark_data, indent=2)
                content_type = 'application/json'
            else:
                # Plain text format (legacy)
                content = timestamp
                content_type = 'text/plain'
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=watermark_key,
                Body=content.encode('utf-8'),
                ContentType=content_type,
                Metadata={
                    'watermark-timestamp': timestamp,
                    'table-name': table_name or 'global',
                    'update-time': datetime.now(timezone.utc).isoformat()
                }
            )
            
            # Update local metadata cache
            self._watermark_metadata = watermark_data['metadata']
            
            logger.info(
                "Watermark updated successfully",
                watermark_key=watermark_key,
                new_watermark=timestamp,
                table_name=table_name
            )
            
            return True
            
        except WatermarkError:
            # Re-raise watermark errors as-is
            raise
        
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to update watermark", error=str(e))
            raise WatermarkError(f"Watermark update failed: {e}", watermark_value=timestamp)
        
        except Exception as e:
            logger.error("Unexpected watermark update error", error=str(e))
            raise WatermarkError(f"Unexpected error updating watermark: {e}", watermark_value=timestamp)
    
    def backup_watermark(self, backup_suffix: str = None) -> str:
        """
        Create a backup of the current watermark.
        
        Args:
            backup_suffix: Optional suffix for backup key
        
        Returns:
            S3 key of the backup watermark
        """
        self._ensure_s3_client()
        
        try:
            # Generate backup key
            timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            if backup_suffix:
                backup_key = f"{self.watermark_key}_backup_{backup_suffix}_{timestamp_str}"
            else:
                backup_key = f"{self.watermark_key}_backup_{timestamp_str}"
            
            # Copy current watermark to backup location
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': self.watermark_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=backup_key,
                MetadataDirective='COPY'
            )
            
            logger.info(
                "Watermark backup created",
                original_key=self.watermark_key,
                backup_key=backup_key
            )
            
            return backup_key
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to backup watermark", error=str(e))
            raise WatermarkError(f"Watermark backup failed: {e}")
    
    def restore_watermark(self, backup_key: str) -> bool:
        """
        Restore watermark from backup.
        
        Args:
            backup_key: S3 key of the backup watermark
        
        Returns:
            True if restore successful
        """
        self._ensure_s3_client()
        
        try:
            logger.warning(
                "Restoring watermark from backup",
                backup_key=backup_key,
                current_key=self.watermark_key
            )
            
            # Copy backup to current watermark location
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': backup_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=self.watermark_key,
                MetadataDirective='REPLACE',
                Metadata={
                    'restored-from': backup_key,
                    'restore-time': datetime.now(timezone.utc).isoformat()
                }
            )
            
            logger.info("Watermark restored successfully", backup_key=backup_key)
            return True
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to restore watermark", error=str(e))
            raise WatermarkError(f"Watermark restore failed: {e}")
    
    def list_watermark_backups(self) -> list:
        """
        List available watermark backups.
        
        Returns:
            List of backup information dictionaries
        """
        self._ensure_s3_client()
        
        try:
            # List objects with backup prefix
            backup_prefix = f"{self.watermark_key}_backup_"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=backup_prefix
            )
            
            backups = []
            for obj in response.get('Contents', []):
                backups.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag'].strip('"')
                })
            
            # Sort by last modified (newest first)
            backups.sort(key=lambda x: x['last_modified'], reverse=True)
            
            logger.info(f"Found {len(backups)} watermark backups")
            return backups
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to list watermark backups", error=str(e))
            raise WatermarkError(f"Failed to list backups: {e}")
    
    def get_watermark_metadata(self) -> Dict[str, Any]:
        """Get metadata associated with current watermark"""
        return self._watermark_metadata.copy()
    
    def _validate_watermark_format(self, timestamp: str):
        """
        Validate watermark timestamp format.
        
        Args:
            timestamp: Timestamp string to validate
        
        Raises:
            WatermarkError: If format is invalid
        """
        try:
            # Try to parse the timestamp
            datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            raise WatermarkError(
                f"Invalid watermark format '{timestamp}'. Expected: YYYY-MM-DD HH:MM:SS",
                watermark_value=timestamp
            )
    
    def _compare_timestamps(self, ts1: str, ts2: str) -> int:
        """
        Compare two timestamp strings.
        
        Args:
            ts1: First timestamp
            ts2: Second timestamp
        
        Returns:
            -1 if ts1 < ts2, 0 if equal, 1 if ts1 > ts2
        """
        try:
            dt1 = datetime.strptime(ts1, '%Y-%m-%d %H:%M:%S')
            dt2 = datetime.strptime(ts2, '%Y-%m-%d %H:%M:%S')
            
            if dt1 < dt2:
                return -1
            elif dt1 > dt2:
                return 1
            else:
                return 0
        except ValueError:
            # If parsing fails, do string comparison
            if ts1 < ts2:
                return -1
            elif ts1 > ts2:
                return 1
            else:
                return 0
    
    def calculate_time_window(
        self,
        current_timestamp: Optional[str] = None,
        table_name: Optional[str] = None
    ) -> tuple:
        """
        Calculate the time window for incremental backup.
        
        Args:
            current_timestamp: Current timestamp (defaults to now)
            table_name: Optional table-specific watermark
        
        Returns:
            Tuple of (start_timestamp, end_timestamp)
        """
        if current_timestamp is None:
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        last_watermark = self.get_last_watermark(table_name)
        
        logger.debug(
            "Calculated backup time window",
            start_time=last_watermark,
            end_time=current_timestamp,
            table_name=table_name
        )
        
        return (last_watermark, current_timestamp)
    
    def health_check(self) -> Dict[str, str]:
        """
        Perform health check on watermark system.
        
        Returns:
            Health status dictionary
        """
        health = {}
        
        try:
            # Test watermark retrieval
            watermark = self.get_last_watermark()
            health['watermark_retrieval'] = 'OK'
            health['current_watermark'] = watermark
        except Exception as e:
            health['watermark_retrieval'] = f'ERROR: {str(e)}'
        
        try:
            # Test S3 bucket access
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            health['s3_access'] = 'OK'
        except Exception as e:
            health['s3_access'] = f'ERROR: {str(e)}'
        
        return health