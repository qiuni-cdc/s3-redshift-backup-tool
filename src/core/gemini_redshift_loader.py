"""
Gemini Redshift Loader - Direct Parquet COPY Implementation

This module implements direct parquet COPY loading to Redshift using the Gemini solution
with dynamic schema discovery and Feature 1 schema alignment. This replaces the old
CSV conversion approach with a more efficient direct parquet method.
"""

import time
from typing import List, Dict, Any, Optional
from datetime import datetime
import psycopg2
from contextlib import contextmanager

from src.config.settings import AppConfig
from src.core.flexible_schema_manager import FlexibleSchemaManager
from src.core.connections import ConnectionManager
from src.core.s3_watermark_manager import S3WatermarkManager
from src.core.column_mapper import ColumnMapper
from src.utils.exceptions import BackupError, DatabaseError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class GeminiRedshiftLoader:
    """
    Gemini-based Redshift loader using direct parquet COPY.
    
    This loader uses the Gemini solution approach:
    1. Dynamic schema discovery from MySQL INFORMATION_SCHEMA
    2. Direct parquet COPY commands (no CSV conversion)
    3. Automatic table creation with proper DDL
    4. Integration with table watermark system
    """
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection_manager = ConnectionManager(config)
        self.schema_manager = FlexibleSchemaManager(self.connection_manager)
        self.watermark_manager = S3WatermarkManager(config)
        self.column_mapper = ColumnMapper()
        self.logger = logger
        
    def _test_connection(self) -> bool:
        """
        Test Redshift connection for CLI verification.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self._redshift_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result is not None
        except Exception as e:
            self.logger.error(f"Redshift connection test failed: {e}")
            return False
        
    def load_table_data(self, table_name: str, cdc_strategy=None) -> bool:
        """
        Load S3 parquet data to Redshift using Gemini direct COPY approach.
        
        Args:
            table_name: Name of the table to load
            cdc_strategy: CDC strategy instance (optional, for full_sync replace mode)
            
        Returns:
            True if loading successful, False otherwise
        """
        load_start_time = datetime.now()
        
        try:
            logger.info(f"Starting Gemini Redshift load for {table_name}")
            
            # Set Redshift status to pending
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_start_time,
                status='pending'
            )
            
            # Step 1: Get dynamic schema using unified schema manager
            logger.debug(f"Discovering schema for {table_name}")
            pyarrow_schema, redshift_ddl = self.schema_manager.get_table_schema(table_name)
            
            # Step 2: Create or update Redshift table
            redshift_table_name = self._get_redshift_table_name(table_name)
            table_created = self._ensure_redshift_table(redshift_table_name, redshift_ddl)
            
            if not table_created:
                logger.error(f"Failed to create/verify Redshift table: {redshift_table_name}")
                self._set_error_status(table_name, "Table creation failed")
                return False
            
            # Step 2.5: Check if we need to truncate table before loading (full_sync replace mode)
            if cdc_strategy and hasattr(cdc_strategy, 'requires_truncate_before_load'):
                if cdc_strategy.requires_truncate_before_load():
                    self._truncate_table_before_load(redshift_table_name)
                    logger.info(f"Truncated table {redshift_table_name} for full_sync replace mode")
            
            # Step 3: Get S3 parquet files for this table
            s3_files = self._get_s3_parquet_files(table_name, cdc_strategy)
            
            if not s3_files:
                logger.warning(f"No S3 parquet files found for {table_name}")
                # This might be OK if no new data, set success
                # Pass None to preserve existing processed files list
                self._set_success_status(table_name, load_start_time, 0, None)
                return True
            
            logger.info(f"Found {len(s3_files)} S3 parquet files for {table_name}")
            
            # Step 4: Execute direct parquet COPY commands
            total_rows_loaded = 0
            successful_files = 0
            failed_files = []
            successful_file_list = []
            
            # Enhanced logging for load analysis
            logger.info(f"🔄 Starting to process {len(s3_files)} files for {table_name}")
            file_size_info = []  # Track file sizes for failure analysis
            
            with self._redshift_connection() as conn:
                for i, s3_file in enumerate(s3_files, 1):
                    file_name = s3_file.split('/')[-1]
                    logger.info(f"📄 Processing file {i}/{len(s3_files)}: {file_name}")
                    
                    try:
                        # Get file size for diagnostics
                        file_size = self._get_s3_file_size(s3_file)
                        file_size_info.append({'file': s3_file, 'size_mb': file_size})
                        
                        start_time = datetime.now()
                        rows_loaded = self._copy_parquet_file(conn, redshift_table_name, s3_file, table_name)
                        duration = (datetime.now() - start_time).total_seconds()
                        
                        total_rows_loaded += rows_loaded
                        successful_files += 1
                        successful_file_list.append(s3_file)
                        
                        # Enhanced success logging
                        rate = rows_loaded / duration if duration > 0 else 0
                        logger.info(f"✅ {file_name}: {rows_loaded:,} rows in {duration:.1f}s ({rate:,.0f} rows/sec)")
                        
                    except Exception as e:
                        # Enhanced failure logging with diagnostics
                        import traceback
                        error_details = {
                            'file': s3_file,
                            'file_name': file_name,
                            'error': str(e),
                            'error_type': type(e).__name__,
                            'file_size_mb': file_size if 'file_size' in locals() else 'unknown',
                            'position_in_batch': f"{i}/{len(s3_files)}",
                            'stack_trace': traceback.format_exc()
                        }
                        failed_files.append(error_details)
                        
                        # Detailed failure logging
                        logger.error(f"❌ {file_name}: {error_details['error_type']} - {error_details['error']}")
                        if 'file_size' in locals() and file_size:
                            logger.error(f"   File size: {file_size:.1f} MB")
                        logger.error(f"   Position: {error_details['position_in_batch']}")
                        
                        # Log stack trace at debug level for deeper investigation
                        logger.debug(f"Stack trace for failed file {file_name}:\n{error_details['stack_trace']}")
                        
                        # Continue with other files rather than failing completely
                        continue
            
            if successful_files == 0:
                logger.error(f"All COPY operations failed for {table_name}")
                self._set_error_status(table_name, "All COPY operations failed")
                return False
            
            # Step 5: Log detailed results and failed files
            if failed_files:
                logger.warning(f"Failed to load {len(failed_files)} files for {table_name}:")
                
                # Group failures by error type for better analysis
                from collections import defaultdict
                failures_by_type = defaultdict(list)
                for failed in failed_files:
                    failures_by_type[failed['error_type']].append(failed)
                
                for error_type, files in failures_by_type.items():
                    logger.error(f"🔴 {error_type} errors ({len(files)} files):")
                    for failed in files[:5]:  # Show first 5 of each type
                        size_info = f" ({failed['file_size_mb']:.1f} MB)" if failed['file_size_mb'] != 'unknown' else ""
                        logger.error(f"  ❌ {failed['file_name']}{size_info}: {failed['error'][:100]}...")
                    if len(files) > 5:
                        logger.error(f"     ... and {len(files) - 5} more {error_type} errors")
                
                # Calculate failure statistics
                total_failed_size = sum(f['file_size_mb'] for f in failed_files if f['file_size_mb'] != 'unknown')
                total_successful_size = sum(info['size_mb'] for info in file_size_info if info['file'] in successful_file_list)
                
                logger.error(f"📊 FAILURE ANALYSIS for {table_name}:")
                logger.error(f"  Failed files: {len(failed_files)}/{len(s3_files)} ({len(failed_files)/len(s3_files)*100:.1f}%)")
                if total_failed_size > 0:
                    logger.error(f"  Failed data size: {total_failed_size:.1f} MB")
                if total_successful_size > 0:
                    logger.error(f"  Successful data size: {total_successful_size:.1f} MB")
                
                # Also log failed files as a structured summary
                failed_file_names = [f['file'] for f in failed_files]
                logger.error(f"FAILED_FILES_SUMMARY for {table_name}: {failed_file_names}")
            
            # Update success status with only successfully processed files
            self._set_success_status(table_name, load_start_time, total_rows_loaded, successful_file_list)
            
            if failed_files:
                logger.info(f"Gemini Redshift load completed for {table_name}: {total_rows_loaded} rows from {successful_files}/{len(s3_files)} files (⚠️  {len(failed_files)} files failed)")
            else:
                logger.info(f"Gemini Redshift load completed for {table_name}: {total_rows_loaded} rows from {successful_files}/{len(s3_files)} files (✅ all files successful)")
            
            return True
            
        except Exception as e:
            logger.error(f"Gemini Redshift load failed for {table_name}: {e}")
            self._set_error_status(table_name, str(e))
            return False
    
    def _truncate_table_before_load(self, redshift_table: str) -> None:
        """
        Truncate table before loading for full_sync replace mode.
        
        Args:
            redshift_table: Redshift table name to truncate
        """
        try:
            with self._redshift_connection() as connection:
                with connection.cursor() as cursor:
                    # Construct full table name with schema
                    full_table_name = f"{self.config.redshift.schema}.{redshift_table}"
                    
                    logger.info(f"Truncating table {full_table_name} for full_sync replace mode")
                    cursor.execute(f"TRUNCATE TABLE {full_table_name}")
                    connection.commit()
                    
                    logger.info(f"Successfully truncated table {full_table_name}")
                    
        except Exception as e:
            error_msg = f"Failed to truncate table {redshift_table}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def _get_redshift_table_name(self, mysql_table_name: str) -> str:
        """Convert MySQL table name to Redshift table name with v1.2.0 scoped support"""
        # FIXED: Extract actual table name without scope prefix for Redshift
        # Redshift tables don't include the scope prefix, just the table name
        
        # Handle scoped table names (v1.2.0 multi-schema)
        if ':' in mysql_table_name:
            # Extract table name after the scope prefix
            # Example: 'US_DW_RO_SSH:settlement.settle_orders' → 'settlement.settle_orders'
            _, actual_table = mysql_table_name.split(':', 1)
            table_name = actual_table
        else:
            # Unscoped table (v1.0.0 compatibility)
            table_name = mysql_table_name
        
        # For Redshift, we only need the table name part (after schema)
        # Example: 'settlement.settle_orders' → 'settle_orders'
        if '.' in table_name:
            _, table_only = table_name.rsplit('.', 1)
            return table_only
        else:
            return table_name
    
    def _ensure_redshift_table(self, table_name: str, ddl: str) -> bool:
        """Create or verify Redshift table exists with correct schema"""
        try:
            with self._redshift_connection() as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                """, (self.config.redshift.schema, table_name))
                
                table_exists = cursor.fetchone()[0] > 0
                
                if not table_exists:
                    logger.info(f"Creating Redshift table: {table_name}")
                    cursor.execute(ddl)
                    conn.commit()
                    logger.info(f"Successfully created table: {table_name}")
                else:
                    logger.debug(f"Redshift table already exists: {table_name}")
                
                cursor.close()
                return True
                
        except Exception as e:
            logger.error(f"Failed to ensure Redshift table {table_name}: {e}")
            return False
    
    def _get_s3_parquet_files(self, table_name: str, cdc_strategy=None) -> List[str]:
        """SIMPLIFIED: Get all S3 files, exclude processed files, load remaining"""
        try:
            # Get watermark to check for processed files
            watermark = self.watermark_manager.get_table_watermark(table_name)
            
            if not watermark:
                logger.warning(f"No watermark found for {table_name}")
                return []
            
            # Allow loading with in_progress or success status (files may exist from interrupted backup)
            if watermark.mysql_status not in ['success', 'in_progress']:
                logger.warning(f"MySQL backup status is {watermark.mysql_status} for {table_name}, skipping")
                return []
            
            if watermark.mysql_status == 'in_progress':
                logger.info(f"MySQL backup is in_progress for {table_name}, checking for existing S3 files")
            
            logger.info(f"SIMPLIFIED LOGIC: Finding all S3 files for {table_name}, excluding processed files")
            
            # Get S3 client
            s3_client = self.connection_manager.get_s3_client()
            
            # Build S3 prefix for this table's data
            clean_table_name = self._clean_table_name_with_scope(table_name)
            base_prefix = f"{self.config.s3.incremental_path.strip('/')}/"
            
            # Try table-specific partition first, then general prefix
            table_partition_prefix = f"{base_prefix}table={clean_table_name}/"
            
            # Check if table partition exists
            try:
                table_partition_response = s3_client.list_objects_v2(
                    Bucket=self.config.s3.bucket_name,
                    Prefix=table_partition_prefix,
                    MaxKeys=1
                )
                has_table_partition = len(table_partition_response.get('Contents', [])) > 0
            except Exception as e:
                logger.warning(f"Failed to check table partition: {e}")
                has_table_partition = False
            
            # Use appropriate prefix
            prefix = table_partition_prefix if has_table_partition else base_prefix
            logger.info(f"Scanning S3 prefix: {prefix}")
            
            # Get all S3 objects
            try:
                paginator = s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=self.config.s3.bucket_name,
                    Prefix=prefix
                )
                
                all_objects = []
                for page in page_iterator:
                    if 'Contents' in page:
                        all_objects.extend(page['Contents'])
                
                
            except Exception as e:
                logger.error(f"S3 listing failed: {e}")
                return []
            
            # Get processed files to exclude
            processed_files = set()
            if watermark and watermark.processed_s3_files:
                processed_files = set(watermark.processed_s3_files)
                logger.info(f"Will exclude {len(processed_files)} already processed files")
            
            # Find all parquet files for this table, excluding processed ones
            files_to_load = []
            
            for obj in all_objects:
                key = obj['Key']
                
                # Must be parquet file
                if not key.endswith('.parquet'):
                    continue
                
                # Must match table name (if using general prefix)
                if not has_table_partition and clean_table_name not in key:
                    continue
                
                # Build S3 URI
                s3_uri = f"s3://{self.config.s3.bucket_name}/{key}"
                
                # Skip if already processed
                if s3_uri in processed_files:
                    logger.debug(f"Skipping processed file: {key}")
                    continue
                
                # Include this file
                files_to_load.append(s3_uri)
                logger.debug(f"Will load file: {key}")
            
            logger.info(f"SIMPLIFIED RESULT: {len(files_to_load)} files to load (excluded {len(processed_files)} processed files)")
            
            # Show files to load
            for i, file_uri in enumerate(files_to_load, 1):
                file_name = file_uri.split('/')[-1]
                logger.info(f"  {i}. {file_name}")
            
            return files_to_load
            
        except Exception as e:
            logger.error(f"Failed to get S3 parquet files for {table_name}: {e}")
            return []
    
    def _get_s3_file_size(self, s3_uri: str) -> float:
        """Get S3 file size in MB for diagnostics"""
        try:
            import boto3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config.s3.access_key,
                aws_secret_access_key=self.config.s3.secret_key.get_secret_value(),
                region_name=self.config.s3.region
            )
            
            # Extract bucket and key from S3 URI
            bucket = self.config.s3.bucket_name
            key = s3_uri.replace(f"s3://{bucket}/", "")
            
            response = s3_client.head_object(Bucket=bucket, Key=key)
            size_bytes = response['ContentLength']
            return size_bytes / (1024 * 1024)  # Convert to MB
            
        except Exception as e:
            logger.debug(f"Failed to get file size for {s3_uri}: {e}")
            return 0.0

    def _copy_parquet_file(self, conn, table_name: str, s3_uri: str, full_table_name: str = None) -> int:
        """Execute COPY command for a single parquet file"""
        cursor = None
        try:
            cursor = conn.cursor()
            
            # Check if table has column mappings
            column_list = ""
            if full_table_name and self.column_mapper.has_mapping(full_table_name):
                # Get the parquet schema to know source columns
                # For now, we'll rely on Redshift's automatic column matching
                # In a full implementation, we'd read the parquet schema
                logger.info(f"Table {full_table_name} has column mappings - using automatic matching")
            
            # Build COPY command for direct parquet loading
            copy_command = f"""
                COPY {self.config.redshift.schema}.{table_name}{column_list}
                FROM '{s3_uri}'
                ACCESS_KEY_ID '{self.config.s3.access_key}'
                SECRET_ACCESS_KEY '{self.config.s3.secret_key.get_secret_value()}'
                FORMAT AS PARQUET;
            """
            
            logger.debug(f"Executing COPY command for {s3_uri}")
            cursor.execute(copy_command)
            
            # Get number of rows loaded
            cursor.execute("SELECT pg_last_copy_count()")
            rows_loaded = cursor.fetchone()[0]
            
            conn.commit()
            cursor.close()
            
            return rows_loaded
            
        except Exception as e:
            # Rollback the failed transaction to clean up the connection state
            try:
                conn.rollback()
                logger.debug(f"Rolled back failed transaction for {s3_uri}")
            except:
                pass  # Ignore rollback errors
            
            if cursor:
                try:
                    cursor.close()
                except:
                    pass  # Ignore cursor close errors
                    
            logger.error(f"COPY command failed for {s3_uri}: {e}")
            raise
    
    @contextmanager
    def _redshift_connection(self):
        """Context manager for Redshift database connections"""
        try:
            # Use Redshift SSH tunnel if configured
            if hasattr(self.config, 'redshift_ssh') and self.config.redshift_ssh.bastion_host:
                with self.connection_manager.redshift_ssh_tunnel() as local_port:
                    conn = psycopg2.connect(
                        host='localhost',
                        port=local_port,
                        database=self.config.redshift.database,
                        user=self.config.redshift.user,
                        password=self.config.redshift.password.get_secret_value()
                    )
                    logger.debug("Connected to Redshift via SSH tunnel")
                    conn.autocommit = True
                    yield conn
                    conn.close()
            else:
                # Direct connection
                conn = psycopg2.connect(
                    host=self.config.redshift.host,
                    port=self.config.redshift.port,
                    database=self.config.redshift.database,
                    user=self.config.redshift.user,
                    password=self.config.redshift.password.get_secret_value()
                )
                logger.debug("Connected to Redshift directly")
                conn.autocommit = True
                yield conn
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            raise DatabaseError(f"Redshift connection failed: {e}")
    
    def _set_success_status(self, table_name: str, load_time: datetime, rows_loaded: int, processed_files: List[str] = None):
        """Set successful load status in watermark with session tracking (FIXES ACCUMULATION BUG)"""
        try:
            # Generate consistent session ID for this loading operation to prevent double-counting
            import uuid
            session_id = f"redshift_load_{uuid.uuid4().hex[:8]}"
            
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_time,
                rows_loaded=rows_loaded,
                status='success',
                processed_files=processed_files,  # Pass None if no files, not empty list
                mode='auto',  # Use auto mode for intelligent accumulation
                session_id=session_id  # Track this loading session to prevent double-counting
            )
        except Exception as e:
            logger.warning(f"Failed to update success watermark for {table_name}: {e}")
    
    def _set_error_status(self, table_name: str, error_message: str):
        """Set error status in watermark"""
        try:
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=datetime.now(),
                rows_loaded=0,
                status='failed',
                error_message=error_message
            )
        except Exception as e:
            logger.warning(f"Failed to update error watermark for {table_name}: {e}")
    
    def verify_row_counts(self, tables: List[str]) -> bool:
        """Verify row counts match between S3 data and Redshift"""
        try:
            all_verified = True
            
            with self._redshift_connection() as conn:
                cursor = conn.cursor()
                
                for table_name in tables:
                    redshift_table_name = self._get_redshift_table_name(table_name)
                    
                    # Get Redshift row count
                    cursor.execute(f"SELECT COUNT(*) FROM {self.config.redshift.schema}.{redshift_table_name}")
                    redshift_count = cursor.fetchone()[0]
                    
                    # Get expected count from watermark
                    watermark = self.watermark_manager.get_table_watermark(table_name)
                    expected_count = watermark.redshift_rows_loaded if watermark else 0
                    
                    if redshift_count == expected_count:
                        logger.info(f"Row count verified for {table_name}: {redshift_count} rows")
                    else:
                        logger.error(f"Row count mismatch for {table_name}: Redshift={redshift_count}, Expected={expected_count}")
                        all_verified = False
                
                cursor.close()
            
            return all_verified
            
        except Exception as e:
            logger.error(f"Row count verification failed: {e}")
            return False
    
    def _clean_table_name_with_scope(self, table_name: str) -> str:
        """
        Clean table name for S3 path generation with v1.2.0 multi-schema support.
        
        Handles both scoped and unscoped table names:
        - 'settlement.settle_orders' → 'settlement_settle_orders'
        - 'US_DW_RO_SSH:settlement.settle_orders' → 'us_dw_ro_ssh_settlement_settle_orders'
        - 'us_dw_pipeline:settlement.settle_orders' → 'us_dw_pipeline_settlement_settle_orders'
        
        Args:
            table_name: Table name (may include scope prefix)
            
        Returns:
            Cleaned table name suitable for S3 path matching
        """
        # Handle scoped table names (v1.2.0 multi-schema)
        if ':' in table_name:
            scope, actual_table = table_name.split(':', 1)
            # Clean scope: lowercase, replace special chars with underscores
            clean_scope = scope.lower().replace('-', '_').replace('.', '_')
            # Clean table: lowercase and replace dots with underscores
            clean_table = actual_table.lower().replace('.', '_').replace('-', '_')
            # Combine: scope_table_name
            return f"{clean_scope}_{clean_table}"
        else:
            # Unscoped table (v1.0.0 compatibility)
            return table_name.lower().replace('.', '_').replace('-', '_')