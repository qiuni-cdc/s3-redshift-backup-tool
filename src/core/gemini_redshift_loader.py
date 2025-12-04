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
from src.core.watermark_adapter import create_watermark_manager
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
    
    def __init__(self, config: AppConfig, connection_registry=None):
        self.config = config
        self.connection_manager = ConnectionManager(config)
        
        # Use provided connection registry (with active SSH tunnels) or create new one
        if connection_registry is None:
            try:
                from src.core.connection_registry import ConnectionRegistry
                connection_registry = ConnectionRegistry()
            except Exception as e:
                logger.warning(f"Failed to load connection registry: {e}")
        
        self.schema_manager = FlexibleSchemaManager(self.connection_manager, connection_registry=connection_registry)
        self.watermark_manager = create_watermark_manager(config.to_dict())
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
        
    def load_table_data(self, table_name: str, cdc_strategy=None, table_config=None) -> bool:
        """
        Load S3 parquet data to Redshift using Gemini direct COPY approach.
        
        Args:
            table_name: Name of the table to load
            cdc_strategy: CDC strategy instance (optional, for full_sync replace mode)
            table_config: Table configuration with target_name mapping (optional)
            
        Returns:
            True if loading successful, False otherwise
        """
        load_start_time = datetime.now()

        try:
            logger.info(f"Starting Gemini Redshift load for {table_name}")
            
            # Set Redshift status to pending using v2.0 API
            self.watermark_manager.simple_manager.update_redshift_state(
                table_name=table_name,
                loaded_files=[],
                status='pending',
                error=None
            )
            
            # Step 1: Get dynamic schema using unified schema manager
            logger.debug(f"Discovering schema for {table_name}")
            pyarrow_schema, redshift_ddl = self.schema_manager.get_table_schema(table_name)

            # Step 2: Create or update Redshift table
            redshift_table_name = self._get_redshift_table_name(table_name, table_config)
            corrected_ddl = self._fix_ddl_table_name(redshift_ddl, redshift_table_name, table_name)
            table_created = self._ensure_redshift_table(redshift_table_name, corrected_ddl)
            
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
            # CRITICAL FIX: Only update watermark AFTER actual successful loading
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
            
            # CRITICAL FIX: Check if ANY files were actually loaded
            if successful_files == 0:
                logger.error(f"❌ All COPY operations failed for {table_name} - NO DATA LOADED")
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
    
    def _get_redshift_table_name(self, mysql_table_name: str, table_config=None) -> str:
        """Convert MySQL table name to Redshift table name with v1.2.0 scoped support and target name mapping"""
        
        # NEW FEATURE: Check if table_config specifies a target_name
        if table_config and hasattr(table_config, 'target_name') and table_config.target_name:
            target_name = table_config.target_name
            logger.info(f"✅ Table mapping: {mysql_table_name} → {target_name} (custom mapping)")
            return target_name
        
        # EXISTING LOGIC: Extract actual table name without scope prefix for Redshift
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
            final_name = table_only
        else:
            final_name = table_name
        
        logger.info(f"✅ Table mapping: {mysql_table_name} → {final_name} (default mapping)")
        return final_name
    
    def _fix_ddl_table_name(self, ddl: str, target_table_name: str, source_table_name: str) -> str:
        """Fix DDL to use the correct target table name for custom table name mappings"""
        import re
        
        # Extract source table name parts for DDL matching
        if ':' in source_table_name:
            _, clean_source = source_table_name.split(':', 1)
        else:
            clean_source = source_table_name
            
        if '.' in clean_source:
            _, source_table_only = clean_source.rsplit('.', 1)
        else:
            source_table_only = clean_source
        
        # Pattern to match CREATE TABLE statements with optional schema prefix
        # Handles: "CREATE TABLE schema.table" or "CREATE TABLE IF NOT EXISTS schema.table" or "CREATE TABLE table"
        pattern = r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(?:(\w+)\.)?(\w+)'

        # Find the current table name in DDL
        match = re.search(pattern, ddl, re.IGNORECASE)
        if match:
            schema_in_ddl = match.group(1)  # May be None if no schema prefix
            current_table_in_ddl = match.group(2)

            # Only replace if the current table name matches our source (avoid incorrect replacements)
            if current_table_in_ddl == source_table_only or current_table_in_ddl == clean_source.replace('.', '_'):
                # Replace with target table name, removing any schema prefix
                corrected_ddl = re.sub(
                    r'(CREATE TABLE(?:\s+IF NOT EXISTS)?\s+)(?:\w+\.)?(\w+)',
                    rf'\1{target_table_name}',
                    ddl,
                    flags=re.IGNORECASE
                )
                logger.debug(f"DDL table name corrected: {current_table_in_ddl} → {target_table_name}")
                return corrected_ddl
        
        # If no replacement needed or pattern not found, return original DDL
        return ddl
    
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
            logger.info(f"🔍 DEBUG: Starting _get_s3_parquet_files for table_name={table_name}")

            # Get watermark to check for processed files
            watermark = self.watermark_manager.get_table_watermark(table_name)

            if not watermark:
                logger.warning(f"No watermark found for {table_name}")
                return []

            logger.info(f"🔍 DEBUG: Watermark - mysql_status={watermark.mysql_status}, redshift_status={watermark.redshift_status}")

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

            logger.info(f"🔍 DEBUG: clean_table_name={clean_table_name}")
            logger.info(f"🔍 DEBUG: base_prefix={base_prefix}")

            # Try table-specific partition first, then general prefix
            table_partition_prefix = f"{base_prefix}table={clean_table_name}/"

            logger.info(f"🔍 DEBUG: Checking table partition: {table_partition_prefix}")

            # Check if table partition exists
            try:
                table_partition_response = s3_client.list_objects_v2(
                    Bucket=self.config.s3.bucket_name,
                    Prefix=table_partition_prefix,
                    MaxKeys=1
                )
                has_table_partition = len(table_partition_response.get('Contents', [])) > 0
                logger.info(f"🔍 DEBUG: has_table_partition={has_table_partition}")
                if has_table_partition:
                    logger.info(f"🔍 DEBUG: Found table partition, first file: {table_partition_response.get('Contents', [{}])[0].get('Key', 'N/A')}")
            except Exception as e:
                logger.warning(f"Failed to check table partition: {e}")
                has_table_partition = False

            # Simple strategy selection based on partition discovery

            # Strategy selection based on partition discovery
            if has_table_partition:
                # Use table-specific prefix for maximum efficiency
                logger.info(f"Using table partition strategy for efficient file discovery")
                prefix = table_partition_prefix
                max_keys = 1000  # Reasonable limit for table-specific files
            else:
                # Priority search for today's files first
                from datetime import datetime
                today = datetime.now()
                today_prefix = f"{base_prefix}year={today.year}/month={today.month:02d}/day={today.day:02d}/"
                logger.info(f"Prioritizing today's files with prefix: {today_prefix}")
                logger.info(f"🔍 DEBUG: No table partition found, falling back to date-based search")
                prefix = today_prefix
                max_keys = 1000  # Start with today's files only

            logger.info(f"🔍 DEBUG: Final search prefix={prefix}, max_keys={max_keys}")
            logger.debug(f"Using S3 prefix: {prefix} (max_keys: {max_keys})")

            # Execute S3 listing with chosen strategy
            try:
                all_objects = []
                continuation_token = None
                total_scanned = 0

                # FIXED: Implement proper pagination to find ALL files, not just first 1000
                while True:
                    list_params = {
                        'Bucket': self.config.s3.bucket_name,
                        'Prefix': prefix
                    }

                    # Add pagination token if we have one
                    if continuation_token:
                        list_params['ContinuationToken'] = continuation_token

                    # For general prefix, limit each page size
                    if not has_table_partition:
                        list_params['MaxKeys'] = 1000  # Page size, not total limit

                    response = s3_client.list_objects_v2(**list_params)

                    # Add objects from this page
                    page_objects = response.get('Contents', [])
                    all_objects.extend(page_objects)
                    total_scanned += len(page_objects)

                    # Check if we have more pages
                    if response.get('IsTruncated', False) and total_scanned < max_keys:
                        continuation_token = response.get('NextContinuationToken')
                        logger.debug(f"S3 listing paginating... scanned {total_scanned} objects so far")
                    else:
                        break

                # Trim to max_keys if needed
                if not has_table_partition and len(all_objects) > max_keys:
                    # Sort by LastModified descending to prioritize newer files
                    all_objects.sort(key=lambda x: x.get('LastModified', ''), reverse=True)
                    all_objects = all_objects[:max_keys]
                    logger.info(f"S3 scan found {total_scanned} objects, limited to newest {max_keys}")

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
            import traceback
            logger.error(f"Full error traceback: {traceback.format_exc()}")
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

            # Get source columns from schema manager to build explicit column list
            # This allows Redshift table to have extra columns (like inserted_at with DEFAULT)
            column_list = ""
            if full_table_name and hasattr(self, 'schema_manager'):
                try:
                    # Get the schema from MySQL source (what's actually in the Parquet file)
                    pyarrow_schema, _ = self.schema_manager.get_table_schema(full_table_name)
                    if pyarrow_schema:
                        # Build column list from source schema
                        source_columns = [field.name for field in pyarrow_schema]

                        # Try to find column mappings - check both scoped and unscoped table names
                        # E.g., "us_dw_unidw_direct:unidw.dw_parcel_detail_tool_temp" vs "unidw.dw_parcel_detail_tool_temp"
                        unscoped_table_name = full_table_name.split(':', 1)[1] if ':' in full_table_name else full_table_name

                        # Use existing ColumnMapper method - tries scoped first, then unscoped
                        mapped_list = self.column_mapper.get_copy_column_list(full_table_name, source_columns)
                        if not mapped_list:
                            mapped_list = self.column_mapper.get_copy_column_list(unscoped_table_name, source_columns)

                        if mapped_list:
                            column_list = f" {mapped_list}"
                            logger.info(f"Using explicit column list with mappings: {len(source_columns)} columns")
                        else:
                            column_list = f" ({', '.join(source_columns)})"
                            logger.info(f"Using explicit column list from source: {len(source_columns)} columns")
                except Exception as e:
                    logger.warning(f"Failed to build column list from schema: {e}, using default matching")
                    column_list = ""

            # Build COPY command for direct parquet loading
            copy_command = f"""
                COPY {self.config.redshift.schema}.{table_name}{column_list}
                FROM '{s3_uri}'
                ACCESS_KEY_ID '{self.config.s3.access_key}'
                SECRET_ACCESS_KEY '{self.config.s3.secret_key.get_secret_value()}'
                FORMAT AS PARQUET;
            """

            logger.info(f"Executing COPY command for {s3_uri}")

            # Execute COPY command
            cursor.execute(copy_command)

            # CRITICAL FIX: Get actual number of rows loaded and verify it's not zero
            cursor.execute("SELECT pg_last_copy_count()")
            rows_loaded = cursor.fetchone()[0]

            # Commit the transaction
            conn.commit()

            # Log actual result
            if rows_loaded == 0:
                logger.warning(f"⚠️  COPY command executed but loaded 0 rows from {s3_uri}")
            else:
                logger.info(f"✅ COPY command loaded {rows_loaded} rows from {s3_uri}")

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
            # Use Redshift SSH tunnel if configured and not None
            if (hasattr(self.config, 'redshift_ssh') and
                self.config.redshift_ssh is not None and
                self.config.redshift_ssh.host and
                self.config.redshift_ssh.host not in ['None', '', 'null']):
                with self.connection_manager.redshift_ssh_tunnel() as local_port:
                    conn = psycopg2.connect(
                        host='localhost',
                        port=local_port,
                        database=self.config.redshift.database,
                        user=self.config.redshift.user,
                        password=self.config.redshift.password.get_secret_value(),
                        options=f'-c search_path={self.config.redshift.schema}'
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
                    password=self.config.redshift.password.get_secret_value(),
                    options=f'-c search_path={self.config.redshift.schema}'
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
            # Update with v2.0 API
            self.watermark_manager.simple_manager.update_redshift_state(
                table_name=table_name,
                loaded_files=processed_files or [],
                status='success'
            )
            
            # After successful state update, also update the row count externally
            if rows_loaded > 0:
                self.watermark_manager.simple_manager.update_redshift_count_from_external(
                    table_name=table_name,
                    actual_count=rows_loaded
                )
        except Exception as e:
            logger.warning(f"Failed to update success watermark for {table_name}: {e}")
            
        except Exception as e:
            logger.error(f"Failed to get S3 parquet files for {table_name}: {e}")
            import traceback
            logger.error(f"Full error traceback: {traceback.format_exc()}")
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

            # Get source columns from schema manager to build explicit column list
            # This allows Redshift table to have extra columns (like inserted_at with DEFAULT)
            column_list = ""
            if full_table_name and hasattr(self, 'schema_manager'):
                try:
                    # Get the schema from MySQL source (what's actually in the Parquet file)
                    pyarrow_schema, _ = self.schema_manager.get_table_schema(full_table_name)
                    if pyarrow_schema:
                        # Build column list from source schema
                        source_columns = [field.name for field in pyarrow_schema]

                        # Try to find column mappings - check both scoped and unscoped table names
                        # E.g., "us_dw_unidw_direct:unidw.dw_parcel_detail_tool_temp" vs "unidw.dw_parcel_detail_tool_temp"
                        unscoped_table_name = full_table_name.split(':', 1)[1] if ':' in full_table_name else full_table_name

                        # Use existing ColumnMapper method - tries scoped first, then unscoped
                        mapped_list = self.column_mapper.get_copy_column_list(full_table_name, source_columns)
                        if not mapped_list:
                            mapped_list = self.column_mapper.get_copy_column_list(unscoped_table_name, source_columns)

                        if mapped_list:
                            column_list = f" {mapped_list}"
                            logger.info(f"Using explicit column list with mappings: {len(source_columns)} columns")
                        else:
                            column_list = f" ({', '.join(source_columns)})"
                            logger.info(f"Using explicit column list from source: {len(source_columns)} columns")
                except Exception as e:
                    logger.warning(f"Failed to build column list from schema: {e}, using default matching")
                    column_list = ""

            # Build COPY command for direct parquet loading
            copy_command = f"""
                COPY {self.config.redshift.schema}.{table_name}{column_list}
                FROM '{s3_uri}'
                ACCESS_KEY_ID '{self.config.s3.access_key}'
                SECRET_ACCESS_KEY '{self.config.s3.secret_key.get_secret_value()}'
                FORMAT AS PARQUET;
            """

            logger.info(f"Executing COPY command for {s3_uri}")

            # Execute COPY command
            cursor.execute(copy_command)

            # CRITICAL FIX: Get actual number of rows loaded and verify it's not zero
            cursor.execute("SELECT pg_last_copy_count()")
            rows_loaded = cursor.fetchone()[0]

            # Commit the transaction
            conn.commit()

            # Log actual result
            if rows_loaded == 0:
                logger.warning(f"⚠️  COPY command executed but loaded 0 rows from {s3_uri}")
            else:
                logger.info(f"✅ COPY command loaded {rows_loaded} rows from {s3_uri}")

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
            # Use Redshift SSH tunnel if configured and not None
            if (hasattr(self.config, 'redshift_ssh') and
                self.config.redshift_ssh is not None and
                self.config.redshift_ssh.host and
                self.config.redshift_ssh.host not in ['None', '', 'null']):
                with self.connection_manager.redshift_ssh_tunnel() as local_port:
                    conn = psycopg2.connect(
                        host='localhost',
                        port=local_port,
                        database=self.config.redshift.database,
                        user=self.config.redshift.user,
                        password=self.config.redshift.password.get_secret_value(),
                        options=f'-c search_path={self.config.redshift.schema}'
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
                    password=self.config.redshift.password.get_secret_value(),
                    options=f'-c search_path={self.config.redshift.schema}'
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
            
            # Update with v2.0 API - track loaded files and get actual count
            self.watermark_manager.simple_manager.update_redshift_state(
                table_name=table_name,
                loaded_files=processed_files or [],
                status='success',
                error=None
            )

            # After successful state update, also update the row count externally
            if rows_loaded > 0:
                self.watermark_manager.simple_manager.update_redshift_count_from_external(
                    table_name=table_name,
                    actual_count=rows_loaded
                )
        except Exception as e:
            logger.warning(f"Failed to update success watermark for {table_name}: {e}")
    
    def _set_error_status(self, table_name: str, error_message: str):
        """Set error status in watermark"""
        try:
            # Set error status using v2.0 API
            self.watermark_manager.simple_manager.update_redshift_state(
                table_name=table_name,
                loaded_files=[],
                status='failed',
                error=error_message
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
                    redshift_table_name = self._get_redshift_table_name(table_name, None)
                    
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