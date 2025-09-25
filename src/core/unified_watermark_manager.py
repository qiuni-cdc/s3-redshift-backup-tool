"""
Unified Watermark Manager - KISS Implementation

A simple, reliable watermark manager that fixes all persistence and scoping issues.
Follows KISS principles: one way to update, always verify, handle scoping correctly.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

from fix_watermark_scoping import TableNameResolver

logger = logging.getLogger(__name__)


class UnifiedWatermarkManager:
    """
    Simple, unified watermark manager that actually works.
    
    Key features:
    - Handles table name scoping correctly
    - Single update method (no confusion)
    - Always verifies persistence
    - Clear separation of concerns
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize with S3 configuration."""
        s3_config = config.get('s3', {})
        self.bucket_name = s3_config.get('bucket_name')
        self.watermark_prefix = s3_config.get('watermark_prefix', 'watermarks/v2/')
        self.region = s3_config.get('region', 'us-west-2')
        
        # Create S3 client
        self.s3_client = boto3.client('s3', region_name=self.region)
        
        # Table name resolver for consistent scoping
        self.resolver = TableNameResolver()
        
        logger.info(f"UnifiedWatermarkManager initialized: bucket={self.bucket_name}, prefix={self.watermark_prefix}")
    
    def get_watermark(self, table_name: str) -> Dict[str, Any]:
        """
        Get watermark for a table, handling scoping correctly.
        
        Args:
            table_name: Table name (with or without scope)
            
        Returns:
            Watermark dict or default if not found
        """
        # Try scoped name first (for v1.2.0 pipelines)
        scoped_key = self._get_s3_key(table_name, use_scope=True)
        watermark = self._read_from_s3(scoped_key)
        
        if watermark:
            logger.debug(f"Found watermark with scoped key: {scoped_key}")
            return watermark
        
        # Fallback to unscoped name
        unscoped_key = self._get_s3_key(table_name, use_scope=False)
        watermark = self._read_from_s3(unscoped_key)
        
        if watermark:
            logger.debug(f"Found watermark with unscoped key: {unscoped_key}")
            return watermark
        
        # Return default if not found
        logger.debug(f"No watermark found for {table_name}, returning default")
        return self._create_default_watermark(table_name)
    
    def update_watermark(self, 
                        table_name: str,
                        last_id: Optional[int] = None,
                        last_timestamp: Optional[str] = None,
                        rows_processed: int = 0,
                        status: str = 'success',
                        error: Optional[str] = None) -> bool:
        """
        Update watermark with verification - THE ONLY UPDATE METHOD.
        
        Args:
            table_name: Table name (with or without scope)
            last_id: Last processed ID
            last_timestamp: Last processed timestamp
            rows_processed: Rows processed in this session
            status: Status (success/failed/in_progress)
            error: Error message if failed
            
        Returns:
            True if update succeeded and was verified
        """
        try:
            # Get current watermark
            current = self.get_watermark(table_name)
            
            # Update MySQL state
            mysql_state = current.get('mysql_state', {})
            if last_id is not None:
                mysql_state['last_id'] = last_id
            if last_timestamp is not None:
                mysql_state['last_timestamp'] = last_timestamp
            
            mysql_state['status'] = status
            mysql_state['error'] = error
            mysql_state['last_updated'] = datetime.now(timezone.utc).isoformat()
            
            # Handle row counts correctly (accumulate, don't overwrite)
            if rows_processed > 0:
                current_total = mysql_state.get('total_rows', 0)
                mysql_state['total_rows'] = current_total + rows_processed
                logger.debug(f"Accumulating rows: {current_total} + {rows_processed} = {mysql_state['total_rows']}")
            
            current['mysql_state'] = mysql_state
            
            # Save with proper scoping
            success = self._save_with_verification(table_name, current)
            
            if success:
                logger.info(f"Watermark updated successfully for {table_name}: "
                          f"id={last_id}, timestamp={last_timestamp}, "
                          f"total_rows={mysql_state['total_rows']}")
            else:
                logger.error(f"Watermark update FAILED for {table_name}")
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to update watermark for {table_name}: {e}")
            return False
    
    def set_manual_watermark(self, table_name: str, last_id: int) -> bool:
        """Set manual watermark for testing or recovery."""
        logger.info(f"Setting manual watermark for {table_name}: id={last_id}")
        
        # Create fresh watermark
        watermark = self._create_default_watermark(table_name)
        watermark['mysql_state']['last_id'] = last_id
        watermark['mysql_state']['status'] = 'success'
        watermark['mysql_state']['last_updated'] = datetime.now(timezone.utc).isoformat()
        watermark['metadata']['manual_override'] = True
        
        return self._save_with_verification(table_name, watermark)
    
    def _save_with_verification(self, table_name: str, watermark: Dict[str, Any]) -> bool:
        """
        Save watermark and verify it was actually persisted.
        
        This is the KEY to fixing persistence issues - always verify!
        """
        # Determine the correct key based on table name format
        if ':' in table_name:
            # Already scoped, use as-is
            key = self._get_s3_key(table_name, use_scope=True)
        else:
            # Try to save with scope if we're in a pipeline context
            # For now, save to both locations to ensure compatibility
            unscoped_key = self._get_s3_key(table_name, use_scope=False)
            scoped_key = self._get_s3_key(table_name, use_scope=True)
            
            # Save to unscoped location
            if not self._write_to_s3(unscoped_key, watermark):
                return False
                
            # If table name has no scope, we're done
            if unscoped_key == scoped_key:
                return self._verify_write(unscoped_key, watermark)
            
            # Also save to scoped location for v1.2.0 compatibility
            key = scoped_key
        
        # Write to S3
        if not self._write_to_s3(key, watermark):
            return False
        
        # Verify the write succeeded
        return self._verify_write(key, watermark)
    
    def _verify_write(self, key: str, expected_watermark: Dict[str, Any]) -> bool:
        """Verify watermark was actually written to S3."""
        try:
            # Read back immediately
            actual = self._read_from_s3(key)
            
            if not actual:
                logger.error(f"Verification failed: watermark not found at {key}")
                return False
            
            # Verify key fields match
            expected_id = expected_watermark.get('mysql_state', {}).get('last_id')
            actual_id = actual.get('mysql_state', {}).get('last_id')
            
            if expected_id != actual_id:
                logger.error(f"Verification failed: last_id mismatch. "
                           f"Expected {expected_id}, got {actual_id}")
                return False
            
            logger.debug(f"Watermark verified successfully at {key}")
            return True
            
        except Exception as e:
            logger.error(f"Verification failed for {key}: {e}")
            return False
    
    def _get_s3_key(self, table_name: str, use_scope: bool) -> str:
        """Get S3 key for watermark storage."""
        watermark_name = self.resolver.get_watermark_key(table_name, use_scope)
        # Clean for S3 key usage
        clean_name = watermark_name.replace(':', '_').replace('.', '_').lower()
        return f"{self.watermark_prefix}{clean_name}.json"
    
    def _read_from_s3(self, key: str) -> Optional[Dict[str, Any]]:
        """Read watermark from S3."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            content = response['Body'].read()
            return json.loads(content)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logger.error(f"Failed to read from S3 {key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to parse watermark from {key}: {e}")
            return None
    
    def _write_to_s3(self, key: str, watermark: Dict[str, Any]) -> bool:
        """Write watermark to S3."""
        try:
            def datetime_serializer(obj):
                """JSON serializer for datetime objects."""
                if isinstance(obj, datetime):
                    return obj.strftime('%Y-%m-%d %H:%M:%S')
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(watermark, indent=2, default=datetime_serializer),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            logger.error(f"Failed to write to S3 {key}: {e}")
            return False
    
    def _create_default_watermark(self, table_name: str) -> Dict[str, Any]:
        """Create default watermark structure."""
        return {
            'version': '2.0',
            'table_name': table_name,
            'mysql_state': {
                'last_timestamp': None,
                'last_id': None,
                'status': 'pending',
                'error': None,
                'total_rows': 0,
                'last_updated': None
            },
            'redshift_state': {
                'total_rows': 0,
                'last_updated': None,
                'status': 'pending',
                'error': None
            },
            'metadata': {
                'created_at': datetime.now(timezone.utc).isoformat(),
                'manual_override': False
            }
        }