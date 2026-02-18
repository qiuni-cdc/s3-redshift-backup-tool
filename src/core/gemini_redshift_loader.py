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
import re

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

        # Build S3 path with target-based isolation (v2.1) - same logic as S3Manager
        isolation_prefix = config.s3.isolation_prefix.strip('/') if config.s3.isolation_prefix else ''
        base_path = config.s3.incremental_path.strip('/')

        if isolation_prefix:
            self.s3_data_path = f"{isolation_prefix}/{base_path}"
        else:
            self.s3_data_path = base_path

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
        
    def load_table_data(self, table_name: str, cdc_strategy=None, table_config=None, execution_timestamp: Optional[datetime] = None) -> bool:
        """
        Load S3 parquet data to Redshift using Gemini direct COPY approach.
        
        Args:
            table_name: Name of the table to load
            cdc_strategy: CDC strategy instance (optional, for full_sync replace mode)
            table_config: Table configuration with target_name mapping (optional)
            execution_timestamp: Optional timestamp of the extraction execution (for accurate partition targeting)
            
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

            # Determine target schema (default to config, override with table_config)
            target_schema = self.config.redshift.schema
            if table_config and hasattr(table_config, 'target_schema') and table_config.target_schema:
                target_schema = table_config.target_schema
                logger.info(f"Using custom target schema: {target_schema}")

            # Step 2: Create or update Redshift table
            redshift_table_name = self._get_redshift_table_name(table_name, table_config)
            corrected_ddl = self._fix_ddl_table_name(redshift_ddl, redshift_table_name, table_name)
            
            # Update DDL with correct schema if needed (basic string replacement)
            # This is a simple heuristic - for robust DDL parsing we'd need more logic
            # But _ensure_redshift_table handles the actual creation in the correct schema context
            
            table_created = self._ensure_redshift_table(redshift_table_name, corrected_ddl, schema=target_schema)
            
            if not table_created:
                logger.error(f"Failed to create/verify Redshift table: {target_schema}.{redshift_table_name}")
                self._set_error_status(table_name, "Table creation failed")
                return False
            
            # Step 2.5: Check if we need to truncate table before loading (full_sync replace mode)
            if cdc_strategy and hasattr(cdc_strategy, 'requires_truncate_before_load'):
                if cdc_strategy.requires_truncate_before_load():
                    self._truncate_table_before_load(redshift_table_name, schema=target_schema)
                    logger.info(f"Truncated table {target_schema}.{redshift_table_name} for full_sync replace mode")
            
            # Step 3: Get S3 parquet files for this table
            s3_files = self._get_s3_parquet_files(table_name, cdc_strategy, execution_timestamp)
            
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
            
            with self._redshift_connection(schema=target_schema) as conn:
                for i, s3_file in enumerate(s3_files, 1):
                    file_name = s3_file.split('/')[-1]
                    logger.info(f"📄 Processing file {i}/{len(s3_files)}: {file_name}")
                    
                    try:
                        # Get file size for diagnostics
                        file_size = self._get_s3_file_size(s3_file)
                        file_size_info.append({'file': s3_file, 'size_mb': file_size})
                        
                        start_time = datetime.now()
                        rows_loaded = self._copy_parquet_file(conn, redshift_table_name, s3_file, table_name, schema=target_schema)
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
            
            # Get table schema/DDL
            # Note: For v1.2.0, table_name might be scoped (e.g. "conn:schema.table")
            # FlexibleSchemaManager needs to handle this or we strip it here
            try:
                # Get schema and DDL
                # For basic schema retrieval, use the full name - manager handles scoping
                flexible_schema, redshift_ddl = self.schema_manager.get_table_schema(table_name)
                
                if not redshift_ddl:
                    raise Exception("Failed to generate DDL")
                
                # Fix DDL table name to match target Redshift table name
                # (DDL comes with source table name, but we might want a different target name)
                corrected_ddl = self._fix_ddl_table_name(redshift_ddl, redshift_table_name, table_name)
                
                # Ensure table exists
                table_created = self._ensure_redshift_table(redshift_table_name, corrected_ddl, schema=target_schema)

            except Exception as e:
                logger.error(f"Schema/DDL generation failed: {e}")
                # Log traceback for debugging
                import traceback
                logger.error(traceback.format_exc())
                return False

            # Step 3: Load data
            total_rows = 0
            loaded_files = []
            failed = False
            
            # Use a single connection for the batch of files
            try:
                with self._redshift_connection(schema=target_schema) as conn:
                    # If this is a full sync with REPLACE mode, and we have files to load,
                    # we should TRUNCATE the table before loading the first file.
                    # This ensures we replace the entire dataset.
                    if cdc_strategy and hasattr(cdc_strategy, 'get_sync_mode') and cdc_strategy.get_sync_mode() == 'replace':
                        logger.info(f"Full sync REPLACE mode detected. Truncating {redshift_table_name} before loading.")
                        with conn.cursor() as cursor:
                            cursor.execute(f"TRUNCATE TABLE {target_schema}.{redshift_table_name}")
                            logger.info(f"Table {redshift_table_name} truncated.")

                    for s3_uri in s3_files:
                        try:
                            # Load file - pass explicit table name to ensure we load to correct target
                            # Also pass full_table_name for schema lookup (needed for column list generation)
                            rows = self._copy_parquet_file(
                                conn, 
                                redshift_table_name, 
                                s3_uri, 
                                full_table_name=table_name,
                                schema=target_schema
                            )
                            total_rows += rows
                            loaded_files.append(s3_uri)
                            
                        except Exception as e:
                            logger.error(f"Failed to load file {s3_uri}: {e}")
                            failed = True
                            self._set_error_status(table_name, str(e))
                            break
            except Exception as conn_err:
                logger.error(f"Redshift connection error during load: {conn_err}")
                failed = True
                self._set_error_status(table_name, str(conn_err))
            
            if failed:
                return False
            
            # Step 4: Update status
            current_time = datetime.now()
            self._set_success_status(table_name, current_time, total_rows, loaded_files)
            
            logger.info(
                "Redshift load complete",
                table=table_name,
                redshift_table=redshift_table_name,
                files=len(s3_files),
                rows=total_rows
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Redshift load failed for {table_name}: {e}")
            self._set_error_status(table_name, str(e))
            return False
    
    def _truncate_table_before_load(self, redshift_table: str, schema: str = None) -> None:
        """
        Truncate table before loading for full_sync replace mode.
        
        Args:
            redshift_table: Redshift table name to truncate
            schema: Optional schema name override
        """
        target_schema = schema or self.config.redshift.schema
        try:
            with self._redshift_connection(schema=target_schema) as connection:
                with connection.cursor() as cursor:
                    # Construct full table name with schema
                    full_table_name = f"{target_schema}.{redshift_table}"
                    
                    logger.info(f"Truncating table {full_table_name} for full_sync replace mode")
                    cursor.execute(f"TRUNCATE TABLE {full_table_name}")
                    connection.commit()
                    
                    logger.info(f"Successfully truncated table {full_table_name}")
                    
        except Exception as e:
            error_msg = f"Failed to truncate table {redshift_table}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def _get_redshift_table_name(self, mysql_table_name: str, table_config=None) -> str:
        """
        Determine Redshift table name from MySQL table name or config.
        Priority:
        1. Configured target_name
        2. MySQL table name (cleaned)
        """
        # 1. Use target_name from config if available (Priority 1)
        if table_config and hasattr(table_config, 'target_name') and table_config.target_name:
             target = table_config.target_name
             logger.debug(f"🔍 Using configured target_name: {target}")
             return target
        
        # 2. Use MySQL table name (Priority 2)
        # Handle scoped names: "schema:table" -> "table" (we typically use schema in search path)
        if ':' in mysql_table_name:
            _, clean_name = mysql_table_name.split(':', 1)
            # Remove schema prefix if present locally
            if '.' in clean_name:
                _, clean_name = clean_name.rsplit('.', 1)
            logger.debug(f"Using derived table name from {mysql_table_name} -> {clean_name}")
            return clean_name
            
        if '.' in mysql_table_name:
            _, clean_name = mysql_table_name.rsplit('.', 1)
            logger.debug(f"Using derived table name from {mysql_table_name} -> {clean_name}")
            return clean_name
            
        return mysql_table_name
    
    def _fix_ddl_table_name(self, ddl: str, target_table_name: str, source_table_name: str) -> str:
        """
        Fix table name in DDL to match target Redshift table name.
        Uses regex to safely replace the table name in CREATE TABLE statement.
        """
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

        logger.info(f"🔍 DEBUG: Fix DDL - source='{source_table_name}', clean='{clean_source}', table_only='{source_table_only}', target='{target_table_name}'")
             
        # Pattern to match CREATE TABLE statements with optional schema prefix
        # Handles: "CREATE TABLE schema.table" or "CREATE TABLE IF NOT EXISTS schema.table" or "CREATE TABLE table"
        # Also handles backticks: `schema`.`table` or `table`
        pattern = r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(?:`?(\w+)`?\.)?`?(\w+)`?'
        
        match = re.search(pattern, ddl, re.IGNORECASE)
        if match:
            # Check what we found in the DDL
            schema_in_ddl = match.group(1)
            current_table_in_ddl = match.group(2)
            
            logger.info(f"🔍 DEBUG: Found in DDL - schema='{schema_in_ddl}', table='{current_table_in_ddl}'")

            # Only replace if the current table name matches our source (avoid incorrect replacements)
            # Check if DDL table name matches source table name (e.g. "uni_tracking_spath")
            if current_table_in_ddl == source_table_only or current_table_in_ddl == clean_source.replace('.', '_'):
                # Replace the whole match with "CREATE TABLE [IF NOT EXISTS] target_name"
                # We strip the schema from DDL because we rely on search_path or explicit schema in ensuring
                
                corrected_ddl = re.sub(
                    r'(CREATE TABLE(?:\s+IF NOT EXISTS)?\s+)(?:`?\w+`?\.)?`?(\w+)`?',
                    rf'\1{target_table_name}',
                    ddl,
                    flags=re.IGNORECASE
                )
                logger.info(f"🔍 DEBUG: DDL fixed. Replaced '{current_table_in_ddl}' with '{target_table_name}'")
                return corrected_ddl
            else:
                logger.warning(f"⚠️ DEBUG: DDL table name '{current_table_in_ddl}' does not match source '{source_table_only}'. execution might fail.")
        else:
            logger.warning("⚠️ DEBUG: Could not find CREATE TABLE pattern in DDL")
            
        return ddl
    
    def _ensure_redshift_table(self, table_name: str, ddl: str, schema: str = None) -> bool:
        """Create Redshift table if not exists, or add missing columns if table exists (schema evolution)."""
        valid_schema = schema or self.config.redshift.schema
        try:
            with self._redshift_connection(schema=valid_schema) as conn:
                cursor = conn.cursor()

                # Check if table exists
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, (valid_schema, table_name))

                table_exists = cursor.fetchone()[0] > 0

                if not table_exists:
                    logger.info(f"Creating Redshift table: {table_name}")
                    logger.info(f"DDL to execute:\n{ddl}")
                    cursor.execute(ddl)
                    logger.info(f"Successfully created table: {table_name}")
                else:
                    logger.info(f"Redshift table already exists: {table_name} - checking for missing columns")
                    self._add_missing_columns(cursor, table_name, ddl, valid_schema)

                cursor.close()
                return True

        except Exception as e:
            logger.error(f"Failed to ensure Redshift table {table_name}: {e}")
            return False

    def _parse_ddl_columns(self, ddl: str) -> dict:
        """Parse column name → type definition from a CREATE TABLE DDL string."""
        columns = {}
        # Match everything between the first '(' and the closing ')' before DISTSTYLE/SORTKEY/';'
        match = re.search(r'CREATE TABLE[^(]*\((.*?)\)\s*(?:DISTSTYLE|SORTKEY|;|$)', ddl, re.DOTALL | re.IGNORECASE)
        if not match:
            return columns
        for line in match.group(1).split('\n'):
            line = line.strip().rstrip(',')
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                col_name = parts[0].strip('`"')
                col_type = parts[1].strip()
                columns[col_name] = col_type
        return columns

    def _add_missing_columns(self, cursor, table_name: str, ddl: str, schema: str) -> None:
        """Add columns present in DDL but absent from the existing Redshift table."""
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table_name))
        existing = {row[0].lower() for row in cursor.fetchall()}

        ddl_columns = self._parse_ddl_columns(ddl)
        added = []
        for col_name, col_def in ddl_columns.items():
            if col_name.lower() not in existing:
                # Strip NOT NULL — existing rows will be NULL for the new column
                nullable_def = col_def.replace(' NOT NULL', '')
                alter_sql = f"ALTER TABLE {schema}.{table_name} ADD COLUMN {col_name} {nullable_def}"
                logger.info(f"Schema evolution: adding missing column {col_name} {nullable_def} to {table_name}")
                cursor.execute(alter_sql)
                added.append(col_name)

        if added:
            logger.info(f"Added {len(added)} missing column(s) to {schema}.{table_name}: {added}")
        else:
            logger.info(f"No missing columns for {schema}.{table_name}")
    
    def _get_s3_parquet_files(self, table_name: str, cdc_strategy=None, execution_timestamp: Optional[datetime] = None) -> List[str]:
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

            # Build S3 prefix for this table's data (v2.1: uses isolation_prefix)
            clean_table_name = self._clean_table_name_with_scope(table_name)
            base_prefix = f"{self.s3_data_path}/"

            logger.info(f"🔍 DEBUG: clean_table_name={clean_table_name}")
            logger.info(f"🔍 DEBUG: base_prefix={base_prefix} (includes isolation_prefix)")

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

            # Strategy selection based on partition discovery
            if has_table_partition:
                # Use table-specific partition
                logger.info(f"Using table partition strategy for efficient file discovery")
                prefix = table_partition_prefix
                max_keys = 20000  # High limit for table-specific files
                pagination_strategy = 'table_partition'
            else:
                # Priority search for today's files first
                from datetime import datetime, timedelta
                today = datetime.max  # Placeholder, will be overwritten
                # We need to be careful with 'today' import overshadowing
                import datetime as dt_module
                
                # Use extraction time from watermark if available (handles backfills & midnight crossing)
                # Fallback to current time for legacy objects or missing attributes
                # Priority: 
                # 1. Explicit execution_timestamp (passed from CLI for backfills)
                # 2. Watermark mysql_last_synced_time (normal incremental flow)
                # 3. Current time (fallback)
                
                if execution_timestamp:
                    target_date = execution_timestamp
                    logger.info(f"Using provided execution timestamp for partition targeting: {target_date}")
                else:
                    extraction_ts = getattr(watermark, 'mysql_last_synced_time', None)
                    
                    if isinstance(extraction_ts, (int, float)):
                        target_date = dt_module.datetime.fromtimestamp(extraction_ts)
                    elif isinstance(extraction_ts, dt_module.datetime):
                        target_date = extraction_ts
                    else:
                        target_date = dt_module.datetime.now()

                today_prefix = f"{base_prefix}year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/"
                logger.info(f"Prioritizing extraction date files with prefix: {today_prefix} (Target Date: {target_date})")
                logger.info(f"🔍 DEBUG: No table partition found, falling back to date-based search. WARNING: This scans shared prefixes!")
                prefix = today_prefix
                max_keys = 50000  # CRITICAL FIX: Increased from 1000 to scan significantly more files in shared folder
                pagination_strategy = 'datetime'

            logger.info(f"🔍 DEBUG: Final search prefix={prefix}, max_keys={max_keys}, strategy={pagination_strategy}")
            
            # Execute S3 listing with chosen strategy
            try:
                all_objects = []
                continuation_token = None
                total_scanned = 0
                
                # FIXED: Implement proper pagination to find ALL files matching criteria
                while True:
                    list_params = {
                        'Bucket': self.config.s3.bucket_name,
                        'Prefix': prefix,
                        'MaxKeys': 1000  # API batch size, not total limit
                    }

                    # Add pagination token if we have one
                    if continuation_token:
                        list_params['ContinuationToken'] = continuation_token

                    response = s3_client.list_objects_v2(**list_params)

                    # Add objects from this page
                    page_objects = response.get('Contents', [])
                    all_objects.extend(page_objects)
                    total_scanned += len(page_objects)
                    
                    if not continuation_token:
                        logger.info(f"🔍 Found {len(page_objects)} files in first page.")

                    # Stop if we hit our safeguard limit
                    if total_scanned >= max_keys:
                        logger.warning(f"Hit max_keys limit ({max_keys}) while scanning S3. Some files might be missed.")
                        break

                    # Check if we have more pages
                    if response.get('IsTruncated', False):
                        continuation_token = response.get('NextContinuationToken')
                        # Log periodically
                        if total_scanned % 5000 < 1000:
                            logger.info(f"S3 listing paginating... scanned {total_scanned} objects so far")
                    else:
                        break
                
                logger.info(f"S3 scan complete. Scanned {total_scanned} objects total (before filtering).")

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

    def _copy_parquet_file(self, conn, table_name: str, s3_uri: str, full_table_name: str = None, schema: str = None) -> int:
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
            # Logic: Use provided schema -> try to get from connection -> use config default
            
            target_schema = schema or self.config.redshift.schema
            
            # If schema not explicitly passed, try to get from connection options
            # (though we prefer explicit passing via Argument)
            if not schema:
                try:
                    # Parse search_path from connection dsn/options if available
                    if hasattr(conn, 'get_dsn_parameters'):
                        options = conn.get_dsn_parameters().get('options', '')
                        if 'search_path=' in options:
                            target_schema = options.split('search_path=')[1].split()[0]
                except:
                    pass

            copy_command = f"""
                COPY {target_schema}.{table_name}{column_list}
                FROM '{s3_uri}'
                ACCESS_KEY_ID '{self.config.s3.access_key}'
                SECRET_ACCESS_KEY '{self.config.s3.secret_key.get_secret_value()}'
                FORMAT AS PARQUET;
            """

            logger.info(f"Executing COPY command for {s3_uri}")

            # Set timeout to prevent indefinite hanging (10 minutes = 600000 milliseconds)
            cursor.execute("SET statement_timeout = 600000")

            try:
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

                return rows_loaded

            finally:
                # Always reset timeout to unlimited
                try:
                    cursor.execute("SET statement_timeout = 0")
                except:
                    pass  # Ignore errors during timeout reset

                if cursor:
                    cursor.close()

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
    def _redshift_connection(self, schema: str = None):
        """Context manager for Redshift database connections"""
        target_schema = schema or self.config.redshift.schema
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
                        options=f'-c search_path={target_schema}',
                        keepalives=1,
                        keepalives_idle=60,
                        keepalives_interval=10,
                        keepalives_count=3
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
                    options=f'-c search_path={target_schema}'
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
        
        Matches logic in S3Manager to ensure path consistency.
        
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
            # Clean table: replace dots with underscores (preserve case for consistency with S3Manager)
            clean_table = actual_table.replace('.', '_').replace('-', '_')
            # Combine: scope_table_name
            return f"{clean_scope}_{clean_table}"
        else:
            # Unscoped table (v1.0.0 compatibility)
            return table_name.replace('.', '_').replace('-', '_')



