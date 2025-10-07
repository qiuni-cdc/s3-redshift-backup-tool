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
        lock_id = None
        
        try:
            # Acquire lock before loading to prevent concurrent loads
            logger.debug(f"Acquiring lock for Redshift load of {table_name}")
            lock_id = self.watermark_manager.simple_manager.acquire_lock(table_name)
            logger.info(f"Lock acquired for Redshift load of {table_name}: {lock_id}")
            
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
            
            # FIXED: Update DDL to use correct target table name if custom mapping is used
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
                # This might be OK if no new data, set success with ACTUAL zero rows
                self._set_success_status(table_name, load_start_time, 0, [])
                return True
            
            logger.info(f"Found {len(s3_files)} S3 parquet files for {table_name}")
            
            # Step 4: Execute direct parquet COPY commands
            # CRITICAL FIX: Only update watermark AFTER actual successful loading
            total_rows_loaded = 0
            successful_files = 0
            loaded_file_uris = []
            
            with self._redshift_connection() as conn:
                for s3_file in s3_files:
                    try:
                        logger.info(f"Loading file {s3_file}")
                        rows_loaded = self._copy_parquet_file(conn, redshift_table_name, s3_file, table_name)
                        total_rows_loaded += rows_loaded
                        successful_files += 1
                        loaded_file_uris.append(s3_file)
                        logger.info(f"✅ Successfully loaded {rows_loaded} rows from {s3_file}")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to load S3 file {s3_file}: {e}")
                        # Continue with other files rather than failing completely
                        continue
            
            # CRITICAL FIX: Check if ANY files were actually loaded
            if successful_files == 0:
                logger.error(f"❌ All COPY operations failed for {table_name} - NO DATA LOADED")
                self._set_error_status(table_name, "All COPY operations failed")
                return False
            
            # Redshift load lifecycle monitoring
            watermark = self.watermark_manager.get_table_watermark(table_name)
            blacklist_size = len(watermark.processed_s3_files) if watermark and watermark.processed_s3_files else 0
            
            logger.info(
                "Redshift Load Monitoring",
                table_name=table_name,
                files_discovered=len(s3_files),
                files_successfully_loaded=successful_files,
                files_failed=len(s3_files) - successful_files,
                total_rows_loaded=total_rows_loaded,
                blacklist_size_before=blacklist_size,
                blacklist_size_after=blacklist_size + successful_files
            )
            
            # Update watermark with loaded files (this adds them to blacklist)
            self._set_success_status(table_name, load_start_time, total_rows_loaded, loaded_file_uris)
            
            # Final monitoring summary
            logger.info(f"✅ Load completed: {table_name} - {total_rows_loaded} rows loaded, {successful_files} files processed")
            logger.info(f"✅ Gemini Redshift load completed for {table_name}: {total_rows_loaded} rows from {successful_files}/{len(s3_files)} files")
            return True
            
        except Exception as e:
            logger.error(f"Gemini Redshift load failed for {table_name}: {e}")
            self._set_error_status(table_name, str(e))
            return False
        
        finally:
            # Always release lock, even if error occurs
            if lock_id:
                try:
                    self.watermark_manager.simple_manager.release_lock(table_name, lock_id)
                    logger.debug(f"Released lock for Redshift load of {table_name}: {lock_id}")
                except Exception as lock_error:
                    logger.error(f"Failed to release lock for {table_name}: {lock_error}")
    
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
        
        # Pattern to match CREATE TABLE statements
        # Handles both "CREATE TABLE table_name" and "CREATE TABLE IF NOT EXISTS table_name"
        pattern = r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(?:public\.)?(\w+)'
        
        # Find the current table name in DDL
        match = re.search(pattern, ddl, re.IGNORECASE)
        if match:
            current_table_in_ddl = match.group(1)
            
            # Only replace if the current table name matches our source (avoid incorrect replacements)
            if current_table_in_ddl == source_table_only or current_table_in_ddl == clean_source.replace('.', '_'):
                # Replace with target table name, preserving schema
                corrected_ddl = re.sub(
                    r'(CREATE TABLE(?:\s+IF NOT EXISTS)?\s+)(?:public\.)?(\w+)',
                    rf'\1public.{target_table_name}',
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
        """Get list of S3 parquet files for the table using pure blacklist deduplication (no timestamp filtering)"""
        try:
            # Get watermark for processed files blacklist
            watermark = self.watermark_manager.get_table_watermark(table_name)
            
            if not watermark or watermark.mysql_status != 'success':
                logger.warning(f"No successful MySQL backup found for {table_name}")
                return []
            
            # PURE BLACKLIST APPROACH: Only use processed files list for deduplication
            # No timestamp filtering - eliminates all timing bugs
            logger.info(f"Using pure blacklist approach for file filtering (no timestamp cutoff)")
            
            # Get S3 client
            s3_client = self.connection_manager.get_s3_client()
            
            # Build S3 prefix for this table's data using same logic as S3Manager
            # FIXED: Use table-specific prefix to avoid scanning all files
            clean_table_name = self._clean_table_name_with_scope(table_name)
            
            # ENHANCED: Use more efficient prefix strategies to minimize file scanning
            # Strategy 1: Try to find table-specific partition directories first
            base_prefix = f"{self.config.s3.incremental_path.strip('/')}/"
            
            # Check if table partition strategy is used (most efficient)
            table_partition_prefix = f"{base_prefix}table={clean_table_name}/"
            
            logger.debug(f"Trying table-specific partition prefix: {table_partition_prefix}")
            
            # First try the most specific prefix (table partition)
            try:
                table_partition_response = s3_client.list_objects_v2(
                    Bucket=self.config.s3.bucket_name,
                    Prefix=table_partition_prefix,
                    MaxKeys=1  # Just check if this partition exists
                )
                
                has_table_partition = len(table_partition_response.get('Contents', [])) > 0
                logger.debug(f"Table partition exists: {has_table_partition}")
                
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
                prefix = today_prefix
                max_keys = 1000  # Start with today's files only
            
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
                
                total_objects = len(all_objects)
                logger.info(f"S3 prefix scan returned {total_objects} total objects")
                
                # Replace response Contents with our paginated results
                response['Contents'] = all_objects
                
            except Exception as e:
                logger.error(f"S3 listing failed: {e}")
                return []
            
            parquet_files = []
            filtered_files = []
            
            # Get list of already processed files to prevent duplicates
            processed_files = []
            if watermark and watermark.processed_s3_files:
                processed_files = watermark.processed_s3_files
                logger.info(f"Found {len(processed_files)} previously processed files to exclude")
            else:
                logger.info(f"No previously processed files to exclude")
            
            # Convert to set for O(1) lookup performance
            processed_files_set = set(processed_files) if processed_files else set()
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                
                # ENHANCED: More efficient filtering - check parquet first, then table name
                if not key.endswith('.parquet'):
                    continue
                    
                # For table partition strategy, we already know files match
                # For general prefix, we need to filter by table name
                if not has_table_partition and clean_table_name not in key:
                    continue
                
                parquet_files.append(key)
                s3_uri = f"s3://{self.config.s3.bucket_name}/{key}"
                logger.debug(f"S3 URI: {s3_uri}")
                
                # Check if file was already processed using blacklist
                if s3_uri in processed_files_set:
                    logger.debug(f"❌ EXCLUDING file (already processed): {key}")
                    continue
                
                # Include files not in blacklist
                filtered_files.append(s3_uri)
                logger.debug(f"✅ INCLUDING file (not in processed list): {key}")
            
            logger.info(f"FILTERING SUMMARY: Found {len(parquet_files)} total files, filtered to {len(filtered_files)} files for loading")
            
            # Show filtering results
            logger.info(f"Filtering results: {len(filtered_files)}/{len(parquet_files)} files eligible for loading")
            if len(filtered_files) == 0 and len(parquet_files) > 0:
                logger.warning(f"All {len(parquet_files)} files were already processed")
            
            return filtered_files
            
        except Exception as e:
            logger.error(f"Failed to get S3 parquet files for {table_name}: {e}")
            import traceback
            logger.error(f"Full error traceback: {traceback.format_exc()}")
            return []
    
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
            
            logger.info(f"Executing COPY command for {s3_uri}")
            
            # Execute COPY command
            cursor.execute(copy_command)
            
            # CRITICAL FIX: Get actual number of rows loaded and verify it's not zero
            cursor.execute("SELECT pg_last_copy_count()")
            rows_loaded = cursor.fetchone()[0]
            
            # Commit the transaction
            conn.commit()
            cursor.close()
            
            # Log actual result
            if rows_loaded == 0:
                logger.warning(f"⚠️  COPY command executed but loaded 0 rows from {s3_uri}")
            else:
                logger.info(f"✅ COPY command loaded {rows_loaded} rows from {s3_uri}")
            
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
            
            # Update with v2.0 API - track loaded files and get actual count
            self.watermark_manager.simple_manager.update_redshift_state(
                table_name=table_name,
                loaded_files=processed_files or [],
                status='success',
                error=None
            )
            
            # Update the actual count from Redshift for accuracy
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
            # Clean table: replace dots with underscores
            clean_table = actual_table.replace('.', '_').replace('-', '_')
            # Combine: scope_table_name
            return f"{clean_scope}_{clean_table}"
        else:
            # Unscoped table (v1.0.0 compatibility)
            return table_name.replace('.', '_').replace('-', '_')