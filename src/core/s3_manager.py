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
            
            # Use optimized upload method
            self._upload_with_optimization(upload_params, buffer_size)
            
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
    
    def _upload_with_optimization(self, upload_params: Dict[str, Any], file_size: int):
        """
        Upload to S3 with performance optimizations based on file size.
        
        Args:
            upload_params: S3 upload parameters
            file_size: Size of the file in bytes
        """
        from boto3.s3.transfer import TransferConfig
        
        # Get S3 optimization settings from config
        multipart_threshold = getattr(self.config.s3, 'multipart_threshold', 104857600)  # 100MB
        max_concurrency = getattr(self.config.s3, 'max_concurrency', 10)
        
        if file_size >= multipart_threshold:
            # Use optimized multipart upload for large files
            logger.debug(
                "Using optimized multipart upload",
                file_size_mb=round(file_size / 1024 / 1024, 2),
                threshold_mb=round(multipart_threshold / 1024 / 1024, 2),
                max_concurrency=max_concurrency
            )
            
            # Create transfer config for optimization
            transfer_config = TransferConfig(
                multipart_threshold=getattr(self.config.s3, 'multipart_threshold', 104857600),
                multipart_chunksize=getattr(self.config.s3, 'multipart_chunksize', 52428800),
                max_concurrency=max_concurrency,
                max_bandwidth=getattr(self.config.s3, 'max_bandwidth', None),
                use_threads=True
            )
            
            # Use transfer manager for optimized upload
            from boto3.s3.transfer import create_transfer_manager
            transfer_manager = create_transfer_manager(self.s3_client, transfer_config)
            
            # Extract body for transfer manager
            body = upload_params.pop('Body')
            bucket = upload_params.pop('Bucket')
            key = upload_params.pop('Key')
            
            # Create upload future
            future = transfer_manager.upload(
                fileobj=BytesIO(body) if isinstance(body, bytes) else body,
                bucket=bucket,
                key=key,
                extra_args=upload_params
            )
            
            # Wait for completion with timeout
            try:
                future.result(timeout=300)  # 5 minute timeout
                logger.debug("Optimized multipart upload completed", s3_key=key)
            except Exception as e:
                logger.error("Multipart upload failed", s3_key=key, error=str(e))
                raise
            finally:
                transfer_manager._shutdown()
        else:
            # Use regular upload for smaller files
            logger.debug(
                "Using standard upload",
                file_size_mb=round(file_size / 1024 / 1024, 2),
                threshold_mb=round(multipart_threshold / 1024 / 1024, 2)
            )
            self.s3_client.put_object(**upload_params)
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        s3_key: str,
        schema: Optional[pa.Schema] = None,
        use_schema_alignment: bool = True,
        compression: str = "snappy",
        **kwargs
    ) -> bool:
        """
        Upload DataFrame using PoC-compatible approach.
        
        Uses the exact approach from the working PoC for Redshift COPY compatibility.
        
        Args:
            df: Pandas DataFrame to upload
            s3_key: S3 key for the upload
            schema: Optional PyArrow schema from PoC
            use_schema_alignment: Enable PoC alignment (default: True)
            compression: Compression method
            **kwargs: Additional arguments
        
        Returns:
            True if upload successful
        """
        try:
            logger.info(
                "Starting PoC-compatible DataFrame upload",
                s3_key=s3_key,
                rows=len(df),
                columns=len(df.columns),
                use_schema_alignment=use_schema_alignment,
                compression=compression
            )
            
            # Use PoC schema alignment if enabled and schema provided
            if use_schema_alignment and schema:
                logger.info("Applying PoC schema alignment")
                df = self._align_dataframe_to_poc_schema(df, schema)
            
            # Convert DataFrame to PyArrow table
            if schema:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            else:
                table = pa.Table.from_pandas(df, preserve_index=False)
            
            # Upload with PoC-compatible settings
            return self._upload_table_to_s3_poc(table, s3_key, compression)
            
        except Exception as e:
            logger.error(
                "PoC DataFrame upload failed",
                s3_key=s3_key,
                error=str(e),
                error_type=type(e).__name__
            )
            return False
    
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
    
    def _align_dataframe_to_poc_schema(self, df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        """
        Align DataFrame using the proven PoC approach.
        
        This method replicates the exact schema alignment from the working PoC:
        1. Use explicit Decimal conversion for decimal types
        2. Simple type conversion without complex type checking
        3. Direct pandas to PyArrow conversion
        
        Args:
            df: Source DataFrame
            schema: Target PyArrow schema
            
        Returns:
            Aligned DataFrame compatible with PoC approach
        """
        try:
            logger.debug("Starting PoC schema alignment", target_fields=len(schema))
            
            # Import decimal for proper decimal handling
            import decimal
            
            # Create copy of dataframe
            aligned_df = df.copy()
            
            # Apply PoC type conversions
            for field in schema:
                col_name = field.name
                target_type = field.type
                
                if col_name not in aligned_df.columns:
                    # Add missing column
                    aligned_df[col_name] = None
                    continue
                
                # PoC conversion logic
                if pa.types.is_decimal(target_type):
                    # Use exact PoC decimal conversion
                    precision = target_type.precision
                    scale = target_type.scale
                    
                    def convert_to_decimal(value):
                        if pd.isna(value) or value is None:
                            return None
                        try:
                            # Convert to Decimal with proper scale
                            decimal_val = decimal.Decimal(str(float(value)))
                            quantizer = decimal.Decimal('0.' + '0' * scale)
                            return decimal_val.quantize(quantizer)
                        except (ValueError, decimal.InvalidOperation):
                            return None
                    
                    aligned_df[col_name] = aligned_df[col_name].apply(convert_to_decimal)
                
                elif pa.types.is_timestamp(target_type):
                    # Simple timestamp conversion
                    aligned_df[col_name] = pd.to_datetime(aligned_df[col_name], errors='coerce')
                
                elif pa.types.is_integer(target_type):
                    # Simple integer conversion
                    aligned_df[col_name] = pd.to_numeric(aligned_df[col_name], errors='coerce').astype('Int64')
                
                elif pa.types.is_string(target_type):
                    # Simple string conversion
                    aligned_df[col_name] = aligned_df[col_name].astype(str)
                    aligned_df[col_name] = aligned_df[col_name].replace('nan', None)
            
            # Reorder columns to match schema
            schema_columns = [field.name for field in schema]
            aligned_df = aligned_df[schema_columns]
            
            logger.info(
                "PoC schema alignment completed",
                original_columns=len(df.columns),
                aligned_columns=len(aligned_df.columns)
            )
            
            return aligned_df
            
        except Exception as e:
            logger.error(
                "PoC schema alignment failed",
                error=str(e),
                schema_fields=len(schema)
            )
            raise
    
    def _upload_table_to_s3_poc(self, table: pa.Table, s3_key: str, compression: str = "snappy") -> bool:
        """
        Upload PyArrow table to S3 using PoC-compatible settings.
        
        Uses the exact parquet generation settings from the working PoC
        to ensure Redshift COPY compatibility.
        
        Args:
            table: PyArrow table to upload
            s3_key: S3 key for the file
            compression: Compression method
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create parquet file in memory
            parquet_buffer = BytesIO()
            
            # PoC-compatible parquet settings for Redshift
            writer_options = {
                'compression': compression,
                'use_dictionary': False,       # PoC setting
                'write_statistics': False,     # PoC setting
                'store_schema': False         # PoC setting
            }
            
            # Write with PoC settings
            pq.write_table(
                table,
                parquet_buffer,
                **writer_options
            )
            
            # Get parquet data
            parquet_data = parquet_buffer.getvalue()
            file_size = len(parquet_data)
            
            logger.info(
                "Generated PoC-compatible parquet data",
                s3_key=s3_key,
                size_bytes=file_size,
                writer_options=writer_options
            )
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_data,
                ContentType='application/octet-stream'
            )
            
            # Update statistics
            self._upload_stats['total_files'] += 1
            self._upload_stats['total_bytes'] += file_size
            
            logger.info(
                "Successfully uploaded PoC-compatible file to S3",
                s3_key=s3_key,
                bucket=self.bucket_name,
                size_bytes=file_size
            )
            
            return True
            
        except Exception as e:
            self._upload_stats['failed_uploads'] += 1
            logger.error(
                "PoC S3 upload failed",
                s3_key=s3_key,
                error=str(e)
            )
            return False