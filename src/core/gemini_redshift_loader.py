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
from src.config.dynamic_schemas import DynamicSchemaManager
from src.core.connections import ConnectionManager
from src.core.s3_watermark_manager import S3WatermarkManager
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
        self.schema_manager = DynamicSchemaManager(self.connection_manager)
        self.watermark_manager = S3WatermarkManager(config)
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
        
    def load_table_data(self, table_name: str) -> bool:
        """
        Load S3 parquet data to Redshift using Gemini direct COPY approach.
        
        Args:
            table_name: Name of the table to load
            
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
            
            # Step 1: Get dynamic schema
            logger.debug(f"Discovering schema for {table_name}")
            pyarrow_schema, redshift_ddl = self.schema_manager.get_schemas(table_name)
            
            # Step 2: Create or update Redshift table
            redshift_table_name = self._get_redshift_table_name(table_name)
            table_created = self._ensure_redshift_table(redshift_table_name, redshift_ddl)
            
            if not table_created:
                logger.error(f"Failed to create/verify Redshift table: {redshift_table_name}")
                self._set_error_status(table_name, "Table creation failed")
                return False
            
            # Step 3: Get S3 parquet files for this table
            s3_files = self._get_s3_parquet_files(table_name)
            
            if not s3_files:
                logger.warning(f"No S3 parquet files found for {table_name}")
                # This might be OK if no new data, set success
                self._set_success_status(table_name, load_start_time, 0)
                return True
            
            logger.info(f"Found {len(s3_files)} S3 parquet files for {table_name}")
            
            # Step 4: Execute direct parquet COPY commands
            total_rows_loaded = 0
            successful_files = 0
            
            with self._redshift_connection() as conn:
                for s3_file in s3_files:
                    try:
                        rows_loaded = self._copy_parquet_file(conn, redshift_table_name, s3_file)
                        total_rows_loaded += rows_loaded
                        successful_files += 1
                        logger.debug(f"Loaded {rows_loaded} rows from {s3_file}")
                        
                    except Exception as e:
                        logger.error(f"Failed to load S3 file {s3_file}: {e}")
                        # Continue with other files rather than failing completely
                        continue
            
            if successful_files == 0:
                logger.error(f"All COPY operations failed for {table_name}")
                self._set_error_status(table_name, "All COPY operations failed")
                return False
            
            # Step 5: Update success status
            self._set_success_status(table_name, load_start_time, total_rows_loaded)
            
            logger.info(f"Gemini Redshift load completed for {table_name}: {total_rows_loaded} rows from {successful_files}/{len(s3_files)} files")
            return True
            
        except Exception as e:
            logger.error(f"Gemini Redshift load failed for {table_name}: {e}")
            self._set_error_status(table_name, str(e))
            return False
    
    def _get_redshift_table_name(self, mysql_table_name: str) -> str:
        """Convert MySQL table name to Redshift table name"""
        # Remove database prefix if present (e.g. "settlement.table_name" -> "table_name")
        if '.' in mysql_table_name:
            table_name = mysql_table_name.split('.')[-1]
        else:
            table_name = mysql_table_name
        
        # Redshift table names: remove special characters, use underscores
        redshift_name = table_name.replace('-', '_').replace(' ', '_')
        
        return redshift_name
    
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
    
    def _get_s3_parquet_files(self, table_name: str) -> List[str]:
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
            elif watermark.last_mysql_extraction_time:
                # Session-based filtering for actual backup operations
                from datetime import datetime, timezone, timedelta
                mysql_extraction_dt = datetime.fromisoformat(watermark.last_mysql_extraction_time.replace('Z', '+00:00'))
                # Create a very wide time window to handle timezone differences and processing delays
                session_start = mysql_extraction_dt - timedelta(hours=2)
                session_end = mysql_extraction_dt + timedelta(hours=10)
                # Ensure both times have timezone info
                if session_start.tzinfo is None:
                    session_start = session_start.replace(tzinfo=timezone.utc)
                if session_end.tzinfo is None:
                    session_end = session_end.replace(tzinfo=timezone.utc)
                logger.info(f"Using session-based incremental Redshift loading: files from {session_start} to {session_end}")
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
            
            # Build S3 prefix for this table's data
            # Need to search the partitioned structure that backup system uses
            clean_table_name = table_name.replace('.', '_').replace('-', '_')
            prefix = f"{self.config.s3.incremental_path.strip('/')}/"
            
            logger.debug(f"Searching for {clean_table_name} files with prefix: {prefix}")
            
            # List objects with this prefix
            response = s3_client.list_objects_v2(
                Bucket=self.config.s3.bucket_name,
                Prefix=prefix
            )
            
            parquet_files = []
            filtered_files = []
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                # Filter for parquet files that match our table name
                if key.endswith('.parquet') and clean_table_name in key:
                    parquet_files.append(key)
                    
                    # Apply timestamp filtering for incremental loading
                    file_modified_time = obj['LastModified']
                    if file_modified_time.tzinfo is None:
                        file_modified_time = file_modified_time.replace(tzinfo=timezone.utc)
                    
                    if session_start and session_end:
                        # Session-based filtering: narrow time window around extraction
                        if session_start <= file_modified_time <= session_end:
                            s3_uri = f"s3://{self.config.s3.bucket_name}/{key}"
                            filtered_files.append(s3_uri)
                            logger.debug(f"Including file (current session): {key} - modified {file_modified_time}")
                        else:
                            logger.debug(f"Skipping file (different session): {key} - modified {file_modified_time}")
                    elif cutoff_time:
                        # Watermark-based filtering: files after watermark timestamp
                        if file_modified_time > cutoff_time:
                            s3_uri = f"s3://{self.config.s3.bucket_name}/{key}"
                            filtered_files.append(s3_uri)
                            logger.debug(f"Including file (after watermark): {key} - modified {file_modified_time}")
                        else:
                            logger.debug(f"Skipping file (before watermark): {key} - modified {file_modified_time}")
                    else:
                        # No cutoff - include all files (full load)
                        s3_uri = f"s3://{self.config.s3.bucket_name}/{key}"
                        filtered_files.append(s3_uri)
                        logger.debug(f"Including file (full load): {key}")
            
            logger.info(f"Found {len(parquet_files)} total files, filtered to {len(filtered_files)} files for loading")
            
            # Debug logging to understand filtering
            if len(parquet_files) > 0 and len(filtered_files) == 0:
                logger.info("DEBUG: No files passed filtering - investigating...")
                logger.info(f"DEBUG: watermark.last_mysql_extraction_time = {watermark.last_mysql_extraction_time}")
                logger.info(f"DEBUG: watermark.last_mysql_data_timestamp = {watermark.last_mysql_data_timestamp}")
                logger.info(f"DEBUG: cutoff_time = {cutoff_time}")
                
                # Show first few files that were rejected, especially new ones
                recent_files = [f for f in parquet_files if '20250813' in f or '20250814' in f][:5]
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
                            logger.info(f"DEBUG: File {i+1}: {obj_key}")
                            logger.info(f"DEBUG:   LastModified: {file_time}")
                            logger.info(f"DEBUG:   Cutoff time:  {cutoff_time}")
                            if cutoff_time:
                                logger.info(f"DEBUG:   After cutoff: {file_time > cutoff_time}")
                            # Check session window if applicable
                            if session_start and session_end:
                                logger.info(f"DEBUG:   Session window: {session_start} to {session_end}")
                                logger.info(f"DEBUG:   In session: {session_start <= file_time <= session_end}")
                            elif cutoff_time:
                                logger.info(f"DEBUG:   After cutoff: {file_time > cutoff_time}")
                    except Exception as e:
                        logger.warning(f"DEBUG: Failed to get file info for {obj_key}: {e}")
            
            return filtered_files
            
        except Exception as e:
            logger.error(f"Failed to get S3 parquet files for {table_name}: {e}")
            return []
    
    def _copy_parquet_file(self, conn, table_name: str, s3_uri: str) -> int:
        """Execute COPY command for a single parquet file"""
        try:
            cursor = conn.cursor()
            
            # Build COPY command for direct parquet loading
            copy_command = f"""
                COPY {self.config.redshift.schema}.{table_name}
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
                yield conn
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            raise DatabaseError(f"Redshift connection failed: {e}")
    
    def _set_success_status(self, table_name: str, load_time: datetime, rows_loaded: int):
        """Set successful load status in watermark"""
        try:
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_time,
                rows_loaded=rows_loaded,
                status='success'
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