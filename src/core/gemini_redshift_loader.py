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
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection_manager = ConnectionManager(config)
        self.schema_manager = FlexibleSchemaManager(self.connection_manager)
        self.watermark_manager = create_watermark_manager(config.model_dump())
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
            
            # CRITICAL FIX: Only update watermark with ACTUAL loaded rows and files
            logger.info(f"✅ Successfully loaded {total_rows_loaded} rows from {successful_files}/{len(s3_files)} files")
            self._set_success_status(table_name, load_start_time, total_rows_loaded, loaded_file_uris)
            
            logger.info(f"Gemini Redshift load completed for {table_name}: {total_rows_loaded} rows from {successful_files}/{len(s3_files)} files")
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
        """Get list of S3 parquet files for the table"""
        try:
            # Get watermark to determine what data needs to be loaded
            watermark = self.watermark_manager.get_table_watermark(table_name)
            
            if not watermark or watermark.mysql_status != 'success':
                logger.warning(f"No successful MySQL backup found for {table_name}")
                return []
                
            # Determine cutoff time for incremental loading using MySQL extraction time
            # This is more reliable than Redshift status which may timeout
            cutoff_time = None
            logger.debug(f"Watermark check: redshift_status='{watermark.redshift_status}', mysql_extraction_time='{watermark.last_mysql_extraction_time}'")
            
            # Determine cutoff time for incremental loading
            cutoff_time = None
            session_start = None
            session_end = None
            
            # Debug watermark detection
            logger.info(f"DEBUG: Watermark detection:")
            logger.info(f"DEBUG:   backup_strategy = '{watermark.backup_strategy}'")
            logger.info(f"DEBUG:   has_mysql_data_timestamp = {bool(watermark.last_mysql_data_timestamp)}")
            logger.info(f"DEBUG:   has_mysql_extraction_time = {bool(watermark.last_mysql_extraction_time)}")
            
            # Check for manual watermark first (priority over extraction time for CLI operations)
            if (watermark.last_mysql_data_timestamp and 
                watermark.backup_strategy == 'manual_cli' and 
                not watermark.last_mysql_extraction_time):
                # Pure manual watermark - use data timestamp for filtering
                from datetime import datetime, timezone
                data_timestamp_dt = datetime.fromisoformat(watermark.last_mysql_data_timestamp.replace('Z', '+00:00'))
                cutoff_time = data_timestamp_dt
                logger.info(f"Using manual watermark-based incremental Redshift loading: files after {cutoff_time}")
                logger.info(f"CUTOFF DEBUG: Manual cutoff set to {cutoff_time}")
            elif watermark.last_mysql_extraction_time:
                # Session-based filtering for actual backup operations
                from datetime import datetime, timezone, timedelta
                mysql_extraction_dt = datetime.fromisoformat(watermark.last_mysql_extraction_time.replace('Z', '+00:00'))
                
                # CRITICAL FIX: For full_sync replace mode, use a much narrower time window
                # to avoid loading accumulated files from previous test runs
                if (cdc_strategy and hasattr(cdc_strategy, 'strategy_name') and 
                    cdc_strategy.strategy_name == 'full_sync' and
                    hasattr(cdc_strategy, 'sync_mode') and 
                    cdc_strategy.sync_mode == 'replace'):
                    
                    # Very narrow window for full_sync replace: only files from this exact session
                    session_start = mysql_extraction_dt - timedelta(minutes=30)
                    session_end = mysql_extraction_dt + timedelta(hours=1)
                    logger.info(f"Using NARROW session window for full_sync replace mode: files from {session_start} to {session_end}")
                else:
                    # Standard wide window for incremental operations
                    session_start = mysql_extraction_dt - timedelta(hours=2)
                    session_end = mysql_extraction_dt + timedelta(hours=10)
                    logger.info(f"Using standard session-based incremental Redshift loading: files from {session_start} to {session_end}")
                
                # Ensure both times have timezone info
                if session_start.tzinfo is None:
                    session_start = session_start.replace(tzinfo=timezone.utc)
                if session_end.tzinfo is None:
                    session_end = session_end.replace(tzinfo=timezone.utc)
            elif watermark.last_mysql_data_timestamp:
                # Fallback: use data timestamp if no extraction time
                from datetime import datetime, timezone
                data_timestamp_dt = datetime.fromisoformat(watermark.last_mysql_data_timestamp.replace('Z', '+00:00'))
                cutoff_time = data_timestamp_dt
                logger.info(f"Using fallback watermark-based incremental Redshift loading: files after {cutoff_time}")
            else:
                logger.info(f"Full Redshift loading: no watermark or extraction time found")
            
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
            
            # OPTIMIZATION: For session-based loading, try date-specific prefixes first
            # This helps find recent files without scanning thousands of old files
            if session_start and not has_table_partition:
                try:
                    # Get dates to scan from session window
                    from datetime import timedelta
                    current_date = session_start.date()
                    end_date = session_end.date() + timedelta(days=1)  # Include end date
                    
                    date_prefixes = []
                    while current_date <= end_date:
                        year = current_date.year
                        month = current_date.month
                        day = current_date.day
                        date_prefix = f"{base_prefix}year={year}/month={month:02d}/day={day:02d}/"
                        date_prefixes.append(date_prefix)
                        current_date += timedelta(days=1)
                    
                    logger.info(f"Optimizing S3 scan with {len(date_prefixes)} date-specific prefixes for session window")
                    
                    # Try date-specific prefixes first (much more efficient)
                    all_date_objects = []
                    for date_prefix in date_prefixes:
                        try:
                            date_response = s3_client.list_objects_v2(
                                Bucket=self.config.s3.bucket_name,
                                Prefix=date_prefix,
                                MaxKeys=1000
                            )
                            date_objects = date_response.get('Contents', [])
                            if date_objects:
                                logger.debug(f"Found {len(date_objects)} objects in {date_prefix}")
                                all_date_objects.extend(date_objects)
                        except Exception as e:
                            logger.warning(f"Failed to scan date prefix {date_prefix}: {e}")
                    
                    if all_date_objects:
                        # Use date-specific results instead of general scan
                        response = {'Contents': all_date_objects}
                        logger.info(f"Date-optimized scan found {len(all_date_objects)} objects in session window")
                        # Skip the general pagination scan
                        prefix = None  # Signal to skip general scan
                        
                except Exception as e:
                    logger.warning(f"Date-based optimization failed, falling back to general scan: {e}")
            
            # Strategy selection based on partition discovery
            if has_table_partition:
                # Use table-specific prefix for maximum efficiency
                logger.info(f"Using table partition strategy for efficient file discovery")
                prefix = table_partition_prefix
                max_keys = 1000  # Reasonable limit for table-specific files
            else:
                # Fallback to general prefix but with stricter filtering
                logger.info(f"Using general prefix with enhanced filtering")
                prefix = base_prefix
                max_keys = 2000  # Limit scan size to avoid performance issues
            
            # Skip general scan if we already have date-optimized results
            if prefix is None and 'Contents' in locals() and response.get('Contents'):
                logger.info(f"Using date-optimized results, skipping general S3 scan")
                all_objects = response.get('Contents', [])
                total_objects = len(all_objects)
            else:
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
                
                # CRITICAL: Skip files that were already processed
                if s3_uri in processed_files:
                    logger.debug(f"Skipping file (already processed): {key}")
                    continue
                
                logger.debug(f"File not in processed list, checking time filters...")
                
                # Apply timestamp filtering for incremental loading
                file_modified_time = obj['LastModified']
                if file_modified_time.tzinfo is None:
                    file_modified_time = file_modified_time.replace(tzinfo=timezone.utc)
                
                # CRITICAL FIX: Extract timestamp from filename as primary source
                # S3 LastModified can be significantly delayed due to processing
                filename_timestamp = None
                try:
                    import re
                    # Extract timestamp from filename pattern: *_YYYYMMDD_HHMMSS_*
                    timestamp_match = re.search(r'_(\d{8}_\d{6})_', key)
                    if timestamp_match:
                        timestamp_str = timestamp_match.group(1)
                        filename_timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                        filename_timestamp = filename_timestamp.replace(tzinfo=timezone.utc)
                        logger.debug(f"Extracted filename timestamp: {filename_timestamp} from {key}")
                except Exception as e:
                    logger.debug(f"Could not extract filename timestamp from {key}: {e}")
                
                # Use filename timestamp if available, fallback to LastModified
                effective_timestamp = filename_timestamp if filename_timestamp else file_modified_time
                
                if session_start and session_end:
                    # Session-based filtering: use effective timestamp (filename preferred)
                    if session_start <= effective_timestamp <= session_end:
                        filtered_files.append(s3_uri)
                        timestamp_source = "filename" if filename_timestamp else "S3_LastModified"
                        logger.debug(f"Including file (current session): {key} - {timestamp_source} {effective_timestamp}")
                    else:
                        timestamp_source = "filename" if filename_timestamp else "S3_LastModified" 
                        logger.debug(f"Skipping file (different session): {key} - {timestamp_source} {effective_timestamp}")
                elif cutoff_time:
                    # CRITICAL FIX: Watermark-based filtering with proper logic
                    # Files should be included if they are AFTER the cutoff time
                    is_after_cutoff = effective_timestamp > cutoff_time
                    timestamp_source = "filename" if filename_timestamp else "S3_LastModified"
                    
                    if is_after_cutoff:
                        filtered_files.append(s3_uri)
                        logger.info(f"✅ INCLUDING file: {key}")
                        logger.info(f"   Timestamp ({timestamp_source}): {effective_timestamp}")
                        logger.info(f"   Cutoff time: {cutoff_time}")
                        logger.info(f"   After cutoff: {is_after_cutoff}")
                    else:
                        logger.info(f"❌ EXCLUDING file: {key}")
                        logger.info(f"   Timestamp ({timestamp_source}): {effective_timestamp}")
                        logger.info(f"   Cutoff time: {cutoff_time}")
                        logger.info(f"   After cutoff: {is_after_cutoff}")
                else:
                    # No cutoff - include all files (full load)
                    filtered_files.append(s3_uri)
                    logger.debug(f"Including file (full load): {key}")
            
            logger.info(f"FILTERING SUMMARY: Found {len(parquet_files)} total files, filtered to {len(filtered_files)} files for loading")
            
            # ENHANCED DEBUG: Always show filtering details
            logger.info(f"FILTERING CRITERIA:")
            if session_start and session_end:
                logger.info(f"  Session window: {session_start} to {session_end}")
            elif cutoff_time:
                logger.info(f"  Cutoff time: {cutoff_time} (files must be AFTER this time)")
            else:
                logger.info(f"  No filtering (full load)")
            
            # ENHANCED DEBUG: Always show why files were filtered (not just when zero)
            if len(parquet_files) > 0:
                if len(filtered_files) == 0:
                    logger.error(f"❌ ALL {len(parquet_files)} FILES WERE FILTERED OUT - INVESTIGATING...")
                    logger.info(f"WATERMARK DEBUG:")
                    logger.info(f"  last_mysql_extraction_time = {watermark.last_mysql_extraction_time}")
                    logger.info(f"  last_mysql_data_timestamp = {watermark.last_mysql_data_timestamp}")
                    logger.info(f"  backup_strategy = {watermark.backup_strategy}")
                    logger.info(f"  Calculated cutoff_time = {cutoff_time}")
                else:
                    logger.info(f"✅ FILTERING SUCCESS: {len(filtered_files)}/{len(parquet_files)} files passed filtering")
                
                # P2 FIX: Show first few files that were rejected, prioritizing recent ones
                # Use dynamic date detection instead of hardcoded dates
                from datetime import datetime, timedelta
                
                # Look for files from the last 7 days instead of hardcoded dates
                recent_date_patterns = []
                for days_back in range(7):
                    date = datetime.now() - timedelta(days=days_back)
                    recent_date_patterns.append(date.strftime('%Y%m%d'))
                
                recent_files = []
                for f in parquet_files:
                    if any(pattern in f for pattern in recent_date_patterns):
                        recent_files.append(f)
                        if len(recent_files) >= 5:
                            break
                
                all_files = parquet_files[:3] if not recent_files else recent_files
                
                for i, obj_key in enumerate(all_files):
                    # Need to get the object info again for debugging
                    try:
                        obj_response = s3_client.list_objects_v2(
                            Bucket=self.config.s3.bucket_name,
                            Prefix=obj_key,
                            MaxKeys=1
                        )
                        if obj_response.get('Contents'):
                            obj = obj_response['Contents'][0]
                            file_time = obj['LastModified']
                            if file_time.tzinfo is None:
                                file_time = file_time.replace(tzinfo=timezone.utc)
                            
                            # Also extract filename timestamp for debugging
                            filename_timestamp = None
                            try:
                                import re
                                timestamp_match = re.search(r'_(\d{8}_\d{6})_', obj_key)
                                if timestamp_match:
                                    timestamp_str = timestamp_match.group(1)
                                    filename_timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                                    filename_timestamp = filename_timestamp.replace(tzinfo=timezone.utc)
                            except:
                                pass
                            
                            effective_timestamp = filename_timestamp if filename_timestamp else file_time
                            
                            logger.info(f"DEBUG: File {i+1}: {obj_key}")
                            logger.info(f"DEBUG:   S3 LastModified: {file_time}")
                            if filename_timestamp:
                                logger.info(f"DEBUG:   Filename timestamp: {filename_timestamp}")
                                logger.info(f"DEBUG:   Using timestamp: FILENAME (more reliable)")
                            else:
                                logger.info(f"DEBUG:   Using timestamp: S3 LASTMODIFIED (fallback)")
                            logger.info(f"DEBUG:   Effective timestamp: {effective_timestamp}")
                            logger.info(f"DEBUG:   Cutoff time:  {cutoff_time}")
                            
                            # Check session window if applicable
                            if session_start and session_end:
                                logger.info(f"DEBUG:   Session window: {session_start} to {session_end}")
                                logger.info(f"DEBUG:   In session: {session_start <= effective_timestamp <= session_end}")
                            elif cutoff_time:
                                logger.info(f"DEBUG:   After cutoff: {effective_timestamp > cutoff_time}")
                    except Exception as e:
                        logger.warning(f"DEBUG: Failed to get file info for {obj_key}: {e}")
            
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
            
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_time,
                rows_loaded=rows_loaded,
                status='success',
                processed_files=processed_files or [],
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
            # Clean table: replace dots with underscores
            clean_table = actual_table.replace('.', '_').replace('-', '_')
            # Combine: scope_table_name
            return f"{clean_scope}_{clean_table}"
        else:
            # Unscoped table (v1.0.0 compatibility)
            return table_name.replace('.', '_').replace('-', '_')