"""
S3 manager for handling parquet file uploads with partitioning strategy.

This module provides comprehensive S3 operations including parquet file uploads,
partitioned storage, progress tracking, and error handling for large datasets.
"""

import io
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# PyArrow compatibility fix for metadata
def write_parquet_safe(table, buffer, **kwargs):
    """Safe parquet writing with metadata compatibility"""
    try:
        # Try with metadata first
        return table.write_to_parquet(buffer, **kwargs)
    except TypeError as e:
        if "metadata" in str(e):
            # Remove metadata and try again
            kwargs_no_metadata = {k: v for k, v in kwargs.items() if k != "metadata"}
            return table.write_to_parquet(buffer, **kwargs_no_metadata)
        raise
import pyarrow.parquet as pq
import pandas as pd
from tqdm import tqdm

from src.config.settings import AppConfig
from src.utils.exceptions import S3Error, ValidationError
from src.utils.logging import get_logger


logger = get_logger(__name__)


class S3Manager:
    """
    Manages S3 operations for the backup system.
    
    Handles parquet file uploads, partitioned storage, progress tracking,
    and provides utilities for S3 data management.
    """
    
    def __init__(self, config: AppConfig, s3_client=None):
        self.config = config
        self.s3_client = s3_client
        self.bucket_name = config.s3.bucket_name
        self.incremental_path = config.s3.incremental_path.strip('/')
        
        # Upload statistics
        self._upload_stats = {
            'total_files': 0,
            'total_bytes': 0,
            'failed_uploads': 0
        }
    
    def _ensure_s3_client(self):
        """Ensure S3 client is available"""
        if self.s3_client is None:
            raise S3Error("S3 client not initialized")
    
    def generate_s3_key(
        self, 
        table_name: str, 
        timestamp: str, 
        batch_id: int,
        partition_strategy: str = "datetime"
    ) -> str:
        """
        Generate S3 key with partitioning strategy.
        
        Args:
            table_name: Name of the table
            timestamp: Timestamp for the backup
            batch_id: Batch identifier
            partition_strategy: Partitioning strategy ('datetime', 'table', 'hybrid')
        
        Returns:
            S3 key with proper partitioning
        """
        try:
            # Parse timestamp
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp.replace(' ', 'T'))
            else:
                dt = timestamp
            
            # Clean table name for use in path
            clean_table_name = table_name.replace('.', '_').replace('-', '_')
            
            # Generate timestamp string for filename
            timestamp_str = dt.strftime('%Y%m%d_%H%M%S')
            
            if partition_strategy == "datetime":
                # Partition by year/month/day/hour
                key = (
                    f"{self.incremental_path}/"
                    f"year={dt.year}/"
                    f"month={dt.month:02d}/"
                    f"day={dt.day:02d}/"
                    f"hour={dt.hour:02d}/"
                    f"{clean_table_name}_{timestamp_str}_batch_{batch_id:04d}.parquet"
                )
            elif partition_strategy == "table":
                # Partition by table first, then datetime
                key = (
                    f"{self.incremental_path}/"
                    f"table={clean_table_name}/"
                    f"year={dt.year}/"
                    f"month={dt.month:02d}/"
                    f"day={dt.day:02d}/"
                    f"{timestamp_str}_batch_{batch_id:04d}.parquet"
                )
            elif partition_strategy == "hybrid":
                # Hybrid approach: year/month/table/day/hour
                key = (
                    f"{self.incremental_path}/"
                    f"year={dt.year}/"
                    f"month={dt.month:02d}/"
                    f"table={clean_table_name}/"
                    f"day={dt.day:02d}/"
                    f"hour={dt.hour:02d}/"
                    f"{timestamp_str}_batch_{batch_id:04d}.parquet"
                )
            else:
                raise ValidationError(f"Unknown partition strategy: {partition_strategy}")
            
            logger.debug(
                "Generated S3 key",
                table_name=table_name,
                batch_id=batch_id,
                partition_strategy=partition_strategy,
                s3_key=key
            )
            
            return key
            
        except Exception as e:
            logger.error("Failed to generate S3 key", error=str(e))
            raise S3Error(f"S3 key generation failed: {e}")
    
    def upload_parquet(
        self, 
        table: pa.Table, 
        s3_key: str,
        compression: str = "snappy",
        metadata: Optional[Dict[str, str]] = None,
        show_progress: bool = False
    ) -> bool:
        """
        Upload PyArrow table as parquet to S3.
        
        Args:
            table: PyArrow table to upload
            s3_key: S3 key for the upload
            compression: Compression algorithm (snappy, gzip, lz4, brotli)
            metadata: Additional metadata to include
            show_progress: Whether to show upload progress
        
        Returns:
            True if upload successful, False otherwise
        """
        self._ensure_s3_client()
        
        try:
            logger.debug(
                "Starting parquet upload",
                s3_key=s3_key,
                rows=len(table),
                columns=len(table.column_names),
                compression=compression
            )
            
            # Create parquet buffer
            buffer = io.BytesIO()
            
            # Set up parquet writer options optimized for Redshift compatibility
            writer_options = {
                'compression': compression,
                'use_dictionary': False,  # Disable dictionary for Redshift compatibility
                'write_statistics': False,  # Disable statistics that might cause metadata issues
                'data_page_size': 1024 * 1024,  # 1MB pages
                'use_deprecated_int96_timestamps': False,
                'store_schema': False,  # Don't store schema metadata that might reference S3
                'write_batch_size': 1000  # Smaller batch size for better compatibility
            }
            
            # Create a clean table without problematic metadata
            # Remove any existing metadata that might reference S3 paths
            clean_table = table.replace_schema_metadata(None)
            
            # Write parquet to buffer with Redshift-compatible parameters
            try:
                pq.write_table(clean_table, buffer, **writer_options)
            except TypeError as e:
                # Fallback to minimal options if there are still compatibility issues
                minimal_options = {
                    'compression': compression,
                    'use_dictionary': False,
                    'write_statistics': False
                }
                pq.write_table(clean_table, buffer, **minimal_options)
            
            # Get buffer size for statistics
            buffer_size = buffer.tell()
            buffer.seek(0)
            
            # Prepare S3 upload parameters with minimal metadata for Redshift compatibility
            upload_params = {
                'Bucket': self.bucket_name,
                'Key': s3_key,
                'Body': buffer.getvalue(),
                'ContentType': 'application/parquet',
                # Minimal S3 metadata to avoid conflicts with Redshift
                'Metadata': {
                    'rows': str(len(table)),
                    'cols': str(len(table.column_names)),
                    'comp': compression
                }
            }
            
            # Upload to S3 with simplified approach
            self.s3_client.put_object(**upload_params)
            
            # Update statistics
            self._upload_stats['total_files'] += 1
            self._upload_stats['total_bytes'] += buffer_size
            
            logger.info(
                "Parquet upload successful",
                s3_key=s3_key,
                file_size_mb=round(buffer_size / 1024 / 1024, 2),
                rows=len(table),
                compression=compression
            )
            
            return True
            
        except (ClientError, BotoCoreError) as e:
            self._upload_stats['failed_uploads'] += 1
            logger.error("S3 upload failed", s3_key=s3_key, error=str(e))
            raise S3Error(f"Upload failed for {s3_key}: {e}", s3_key=s3_key, bucket=self.bucket_name)
        
        except Exception as e:
            self._upload_stats['failed_uploads'] += 1
            logger.error("Unexpected upload error", s3_key=s3_key, error=str(e))
            raise S3Error(f"Unexpected upload error: {e}", s3_key=s3_key)
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        s3_key: str,
        schema: Optional[pa.Schema] = None,
        use_schema_alignment: bool = True,
        **kwargs
    ) -> bool:
        """
        Upload pandas DataFrame as parquet to S3 with Feature 1 schema alignment.
        
        Args:
            df: Pandas DataFrame to upload
            s3_key: S3 key for the upload
            schema: Optional PyArrow schema for validation and alignment
            use_schema_alignment: Enable Feature 1 schema alignment (default: True)
            **kwargs: Additional arguments passed to upload_parquet
        
        Returns:
            True if upload successful
        """
        try:
            # Feature 1: Schema Alignment for Redshift Compatibility
            if schema and use_schema_alignment:
                logger.debug("Using Feature 1 schema alignment", s3_key=s3_key)
                # Apply Feature 1 schema alignment for perfect Redshift compatibility
                table = self.align_dataframe_to_redshift_schema(df, schema)
            elif schema:
                logger.debug("Using legacy schema validation", s3_key=s3_key)
                # Legacy approach - basic schema validation only
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                logger.debug("No schema provided - using automatic inference", s3_key=s3_key)
                # No schema - use automatic inference
                table = pa.Table.from_pandas(df, preserve_index=False)
            
            # Skip DataFrame metadata for Redshift compatibility
            # Remove any existing metadata parameter to avoid issues
            kwargs.pop('metadata', None)
            
            return self.upload_parquet(table, s3_key, **kwargs)
            
        except Exception as e:
            logger.error("DataFrame upload failed", error=str(e), 
                        use_schema_alignment=use_schema_alignment,
                        has_schema=schema is not None)
            raise S3Error(f"DataFrame upload failed: {e}")
    
    def list_backup_files(
        self,
        table_name: Optional[str] = None,
        date_prefix: Optional[str] = None,
        max_keys: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        List backup files in S3.
        
        Args:
            table_name: Filter by table name
            date_prefix: Filter by date prefix (YYYY/MM/DD format)
            max_keys: Maximum number of keys to return
        
        Returns:
            List of file information dictionaries
        """
        self._ensure_s3_client()
        
        try:
            # Build prefix for filtering
            prefix = f"{self.incremental_path}/"
            
            if date_prefix:
                # Add date-based filtering
                date_parts = date_prefix.split('/')
                if len(date_parts) >= 1:
                    prefix += f"year={date_parts[0]}/"
                if len(date_parts) >= 2:
                    prefix += f"month={date_parts[1]}/"
                if len(date_parts) >= 3:
                    prefix += f"day={date_parts[2]}/"
            
            logger.debug("Listing S3 objects", prefix=prefix, max_keys=max_keys)
            
            # List objects
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                
                # Filter by table name if specified
                if table_name and table_name not in key:
                    continue
                
                files.append({
                    'key': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag'].strip('"')
                })
            
            logger.info(f"Found {len(files)} backup files", prefix=prefix)
            return files
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Failed to list S3 objects", error=str(e))
            raise S3Error(f"Failed to list backup files: {e}")
    
    def delete_backup_files(
        self,
        keys: List[str],
        confirm: bool = False
    ) -> Dict[str, int]:
        """
        Delete backup files from S3.
        
        Args:
            keys: List of S3 keys to delete
            confirm: Confirmation flag to prevent accidental deletion
        
        Returns:
            Dictionary with deletion statistics
        """
        if not confirm:
            raise ValidationError("Deletion requires explicit confirmation")
        
        self._ensure_s3_client()
        
        try:
            logger.warning(f"Deleting {len(keys)} files from S3", keys=keys[:5])
            
            # Batch delete (S3 supports up to 1000 keys per request)
            batch_size = 1000
            deleted_count = 0
            error_count = 0
            
            for i in range(0, len(keys), batch_size):
                batch_keys = keys[i:i + batch_size]
                delete_objects = [{'Key': key} for key in batch_keys]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': delete_objects}
                )
                
                deleted_count += len(response.get('Deleted', []))
                error_count += len(response.get('Errors', []))
                
                # Log any errors
                for error in response.get('Errors', []):
                    logger.error(
                        "Failed to delete object",
                        key=error['Key'],
                        error_code=error['Code'],
                        error_message=error['Message']
                    )
            
            stats = {
                'deleted': deleted_count,
                'errors': error_count,
                'total_requested': len(keys)
            }
            
            logger.info("Batch deletion completed", **stats)
            return stats
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Batch deletion failed", error=str(e))
            raise S3Error(f"Batch deletion failed: {e}")
    
    def get_file_metadata(self, s3_key: str) -> Dict[str, Any]:
        """
        Get metadata for a specific S3 file.
        
        Args:
            s3_key: S3 key of the file
        
        Returns:
            File metadata dictionary
        """
        self._ensure_s3_client()
        
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            return {
                'content_length': response['ContentLength'],
                'content_type': response.get('ContentType'),
                'last_modified': response['LastModified'],
                'etag': response['ETag'].strip('"'),
                'metadata': response.get('Metadata', {}),
                'storage_class': response.get('StorageClass', 'STANDARD')
            }
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise S3Error(f"File not found: {s3_key}", s3_key=s3_key)
            else:
                raise S3Error(f"Failed to get metadata: {e}", s3_key=s3_key)
    
    def download_parquet(self, s3_key: str) -> pa.Table:
        """
        Download parquet file from S3 as PyArrow table.
        
        Args:
            s3_key: S3 key of the parquet file
        
        Returns:
            PyArrow table
        """
        self._ensure_s3_client()
        
        try:
            logger.debug("Downloading parquet file", s3_key=s3_key)
            
            # Download file to buffer
            buffer = io.BytesIO()
            self.s3_client.download_fileobj(self.bucket_name, s3_key, buffer)
            buffer.seek(0)
            
            # Read parquet from buffer
            table = pq.read_table(buffer)
            
            logger.info(
                "Parquet download successful",
                s3_key=s3_key,
                rows=len(table),
                columns=len(table.column_names)
            )
            
            return table
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Parquet download failed", s3_key=s3_key, error=str(e))
            raise S3Error(f"Download failed for {s3_key}: {e}", s3_key=s3_key)
    
    def get_upload_stats(self) -> Dict[str, Any]:
        """Get upload statistics"""
        return {
            **self._upload_stats,
            'success_rate': (
                (self._upload_stats['total_files'] - self._upload_stats['failed_uploads']) /
                max(self._upload_stats['total_files'], 1) * 100
            ),
            'total_size_mb': round(self._upload_stats['total_bytes'] / 1024 / 1024, 2)
        }
    
    def reset_stats(self):
        """Reset upload statistics"""
        self._upload_stats = {
            'total_files': 0,
            'total_bytes': 0,
            'failed_uploads': 0
        }
    
    def align_dataframe_to_redshift_schema(self, df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
        """
        Feature 1: Align DataFrame to Redshift-compatible schema with robust error handling.
        
        This function ensures perfect schema compatibility by:
        1. Reordering columns to match target schema
        2. Adding missing columns as nulls
        3. Removing extra columns not in schema
        4. Type casting with fallback to nulls for incompatible data
        
        Args:
            df: Source DataFrame to align
            schema: Target PyArrow schema for Redshift compatibility
            
        Returns:
            PyArrow Table aligned to target schema
            
        Raises:
            S3Error: If schema alignment fails completely
        """
        import time
        start_time = time.time()
        
        try:
            logger.debug("Starting Feature 1 schema alignment", 
                        source_columns=len(df.columns), 
                        target_columns=len(schema))
            
            # Get target column names and types
            target_columns = [field.name for field in schema]
            source_columns = df.columns.tolist()
            
            # Performance tracking
            columns_casted = 0
            columns_missing = 0
            columns_nullified = 0
            
            # Step 1: Reorder and select columns
            aligned_df = pd.DataFrame()
            
            for field in schema:
                col_name = field.name
                target_type = field.type
                
                if col_name in df.columns:
                    # Column exists - perform type conversion
                    try:
                        if pa.types.is_integer(target_type):
                            aligned_df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                        elif pa.types.is_floating(target_type):
                            aligned_df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('float64')
                        elif pa.types.is_decimal(target_type):
                            # Special handling for decimal types - convert to float for Redshift compatibility
                            aligned_df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('float64')
                        elif pa.types.is_string(target_type):
                            aligned_df[col_name] = df[col_name].astype('string')
                        elif pa.types.is_timestamp(target_type):
                            aligned_df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                        elif pa.types.is_date(target_type):
                            aligned_df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
                        else:
                            # Default: preserve as-is
                            aligned_df[col_name] = df[col_name]
                        
                        columns_casted += 1
                        
                    except Exception as e:
                        logger.warning(f"Type conversion failed for column {col_name}", error=str(e))
                        aligned_df[col_name] = None
                        columns_nullified += 1
                else:
                    # Column missing - add as nulls
                    aligned_df[col_name] = None
                    columns_missing += 1
            
            # Step 2: Convert to PyArrow Table with target schema
            table = pa.table(aligned_df, schema=schema)
            
            alignment_time = time.time() - start_time
            
            # Log performance metrics
            logger.info("Schema alignment successful",
                       casted=columns_casted,
                       columns=len(schema), 
                       missing=columns_missing,
                       nullified=columns_nullified,
                       rows=len(df),
                       duration_seconds=round(alignment_time, 3))
            
            return table
            
        except Exception as e:
            error_msg = f"Feature 1 schema alignment failed: {str(e)}"
            logger.error("Schema alignment error", error=error_msg, 
                        source_columns=len(df.columns) if df is not None else 0,
                        target_columns=len(schema))
            raise S3Error(error_msg)