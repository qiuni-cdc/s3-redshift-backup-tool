"""
Redshift loader for S3 to Redshift data loading operations.

This module handles loading S3 backup files into Redshift tables,
with support for the existing CSV conversion approach.
"""

import boto3
import psycopg2
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import time
import pandas as pd
import pyarrow.parquet as pq
from io import StringIO, BytesIO
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder

from src.config.settings import AppConfig
from src.core.watermark_adapter import create_watermark_manager
from src.utils.exceptions import BackupError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class RedshiftLoader:
    """
    Handles loading S3 backup data into Redshift tables.

    Uses the proven CSV conversion approach for reliable loading.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.watermark_manager = create_watermark_manager(config.to_dict())
        self.s3_client = None
        self._init_s3_client()

        # Build S3 path with target-based isolation (v2.1) - same logic as S3Manager
        isolation_prefix = config.s3.isolation_prefix.strip('/') if config.s3.isolation_prefix else ''
        base_path = config.s3.incremental_path.strip('/')

        if isolation_prefix:
            self.s3_data_path = f"{isolation_prefix}/{base_path}"
        else:
            self.s3_data_path = base_path
        
    def _init_s3_client(self):
        """Initialize S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config.s3.access_key,
                aws_secret_access_key=self.config.s3.secret_key.get_secret_value(),
                region_name=self.config.s3.region
            )
            logger.info("S3 client initialized for Redshift loader")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise BackupError(f"S3 client initialization failed: {e}")
    
    @contextmanager
    def redshift_connection(self):
        """Get Redshift connection through SSH tunnel if needed"""
        tunnel = None
        conn = None
        
        try:
            # Setup SSH tunnel if Redshift needs it
            if hasattr(self.config, 'redshift_ssh') and self.config.redshift_ssh is not None and self.config.redshift_ssh.host:
                tunnel = SSHTunnelForwarder(
                    (self.config.redshift_ssh.host, 22),
                    ssh_username=self.config.redshift_ssh.username,
                    ssh_pkey=self.config.redshift_ssh.private_key_path,
                    remote_bind_address=(self.config.redshift.host, self.config.redshift.port),
                    local_bind_address=('127.0.0.1', 0)
                )
                tunnel.start()
                local_port = tunnel.local_bind_port
                host = '127.0.0.1'
                logger.info(f"Redshift SSH tunnel established on port {local_port}")
            else:
                # Direct connection
                host = self.config.redshift.host
                local_port = self.config.redshift.port
            
            # Connect to Redshift
            conn = psycopg2.connect(
                host=host,
                port=local_port,
                database=self.config.redshift.database,
                user=self.config.redshift.user,
                password=self.config.redshift.password.get_secret_value()
            )
            
            logger.info("Redshift connection established")
            yield conn
            
        except Exception as e:
            logger.error(f"Redshift connection failed: {e}")
            raise BackupError(f"Failed to connect to Redshift: {e}")
        finally:
            if conn:
                conn.close()
            if tunnel:
                tunnel.stop()
    
    def load_table_data(self, table_name: str, since_time: Optional[datetime] = None) -> bool:
        """
        Load S3 backup data for a table into Redshift.
        
        Args:
            table_name: Name of the table to load
            since_time: Load files created since this time (if None, load recent files)
            
        Returns:
            True if load successful
        """
        load_start_time = datetime.now()
        
        try:
            # Set Redshift status to pending
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_start_time,
                rows_loaded=0,
                status='pending'
            )
            
            logger.info(f"Starting Redshift load for table: {table_name}")
            
            # Find S3 files to load
            s3_files = self._find_s3_backup_files(table_name, since_time)
            
            if not s3_files:
                logger.info(f"No S3 files found to load for {table_name}")
                # Still mark as success since there's nothing to load
                self.watermark_manager.update_redshift_watermark(
                    table_name=table_name,
                    load_time=load_start_time,
                    rows_loaded=0,
                    status='success'
                )
                return True
            
            logger.info(f"Found {len(s3_files)} S3 files to load for {table_name}")
            
            # Process files and load to Redshift
            total_rows_loaded = 0
            
            for s3_file in s3_files:
                rows_loaded = self._load_single_file(table_name, s3_file)
                total_rows_loaded += rows_loaded
                logger.info(f"Loaded {rows_loaded} rows from {s3_file['key']}")
            
            # Update watermark with success
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_start_time,
                rows_loaded=total_rows_loaded,
                status='success'
            )
            
            logger.info(
                f"Redshift load completed for {table_name}: {total_rows_loaded} total rows"
            )
            
            return True
            
        except Exception as e:
            # Set error status
            self.watermark_manager.update_redshift_watermark(
                table_name=table_name,
                load_time=load_start_time,
                rows_loaded=0,
                status='failed',
                error_message=str(e)
            )
            
            logger.error(f"Redshift load failed for {table_name}: {e}")
            return False
    
    def _find_s3_backup_files(self, table_name: str, since_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Find S3 backup files for the table"""
        try:
            # Default to files from last 24 hours if no since_time provided
            if since_time is None:
                since_time = datetime.now() - timedelta(hours=24)
            
            # Generate search prefix based on table name (v2.1: uses isolation_prefix)
            table_prefix = table_name.replace('.', '_')
            s3_prefix = self.s3_data_path

            logger.debug(f"Searching S3 for files with prefix: {s3_prefix} and pattern: {table_prefix} (includes isolation_prefix)")
            
            # List objects in S3
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.s3.bucket_name,
                Prefix=s3_prefix,
                MaxKeys=1000
            )
            
            if 'Contents' not in response:
                return []
            
            # Filter files for this table and time range
            matching_files = []
            for obj in response['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified'].replace(tzinfo=None)  # Remove timezone for comparison
                
                # Check if file matches table pattern and time range
                if (table_prefix in key.lower() and 
                    'parquet' in key.lower() and 
                    last_modified >= since_time):
                    
                    matching_files.append({
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': last_modified
                    })
            
            # Sort by modification time (oldest first for consistent loading)
            matching_files.sort(key=lambda x: x['last_modified'])
            
            logger.info(f"Found {len(matching_files)} matching S3 files for {table_name}")
            return matching_files
            
        except Exception as e:
            logger.error(f"Failed to find S3 files for {table_name}: {e}")
            raise BackupError(f"S3 file discovery failed: {e}")
    
    def _load_single_file(self, table_name: str, s3_file: Dict[str, Any]) -> int:
        """
        Load a single S3 parquet file into Redshift using CSV conversion.
        
        Returns:
            Number of rows loaded
        """
        try:
            # Step 1: Download parquet file from S3
            logger.debug(f"Downloading S3 file: {s3_file['key']}")
            
            response = self.s3_client.get_object(
                Bucket=self.config.s3.bucket_name,
                Key=s3_file['key']
            )
            parquet_data = response['Body'].read()
            
            # Step 2: Convert parquet to CSV (following your proven approach)
            parquet_buffer = BytesIO(parquet_data)
            table = pq.read_table(parquet_buffer)
            df = table.to_pandas()
            
            if len(df) == 0:
                logger.info(f"Skipping empty file: {s3_file['key']}")
                return 0
            
            # Convert to CSV with proper formatting for Redshift
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, sep='|', na_rep='\\N', date_format='%Y-%m-%d %H:%M:%S')
            csv_data = csv_buffer.getvalue()
            
            # Step 3: Upload CSV to S3 temporary location
            csv_key = s3_file['key'].replace('.parquet', '.csv').replace('incremental/', 'temp_csv/')
            
            self.s3_client.put_object(
                Bucket=self.config.s3.bucket_name,
                Key=csv_key,
                Body=csv_data.encode('utf-8'),
                ContentType='text/csv'
            )
            
            # Step 4: Load CSV into Redshift using COPY command
            redshift_table_name = self._get_redshift_table_name(table_name)
            rows_loaded = self._execute_redshift_copy(redshift_table_name, csv_key, len(df))
            
            # Step 5: Clean up temporary CSV file
            self.s3_client.delete_object(
                Bucket=self.config.s3.bucket_name,
                Key=csv_key
            )
            
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Failed to load file {s3_file['key']}: {e}")
            raise BackupError(f"File loading failed: {e}")
    
    def _execute_redshift_copy(self, redshift_table_name: str, csv_s3_key: str, expected_rows: int) -> int:
        """Execute Redshift COPY command for CSV file"""
        try:
            with self.redshift_connection() as conn:
                cursor = conn.cursor()
                
                # Construct COPY command (following your proven CSV approach)
                copy_command = f"""
                COPY {redshift_table_name}
                FROM 's3://{self.config.s3.bucket_name}/{csv_s3_key}'
                ACCESS_KEY_ID '{self.config.s3.access_key}'
                SECRET_ACCESS_KEY '{self.config.s3.secret_key.get_secret_value()}'
                DELIMITER '|'
                IGNOREHEADER 1
                NULL AS '\\\\N'
                DATEFORMAT 'YYYY-MM-DD HH:MI:SS'
                TRUNCATECOLUMNS
                COMPUPDATE OFF
                STATUPDATE OFF;
                """
                
                logger.debug(f"Executing COPY command for {redshift_table_name}")
                cursor.execute(copy_command)
                
                # Get row count from Redshift system tables
                cursor.execute("SELECT pg_last_copy_count();")
                actual_rows = cursor.fetchone()[0]
                
                conn.commit()
                cursor.close()
                
                if actual_rows != expected_rows:
                    logger.warning(
                        f"Row count mismatch for {redshift_table_name}: "
                        f"expected {expected_rows}, loaded {actual_rows}"
                    )
                
                logger.info(f"Successfully loaded {actual_rows} rows into {redshift_table_name}")
                return actual_rows
                
        except Exception as e:
            logger.error(f"COPY command failed for {redshift_table_name}: {e}")
            raise BackupError(f"Redshift COPY failed: {e}")
    
    def _get_redshift_table_name(self, mysql_table_name: str) -> str:
        """Convert MySQL table name to Redshift table name"""
        # Convert settlement.settlement_claim_detail -> settlement_claim_detail
        # Remove schema prefix to get just the table name
        if '.' in mysql_table_name:
            table_name = mysql_table_name.split('.', 1)[1]  # Get everything after first dot
        else:
            table_name = mysql_table_name
        
        schema = self.config.redshift.schema or 'public'
        return f"{schema}.{table_name}"
    
    def create_redshift_table_if_needed(self, table_name: str, sample_s3_file: str) -> bool:
        """
        Create Redshift table based on S3 data schema if it doesn't exist.
        
        This is a placeholder for table creation logic.
        """
        try:
            redshift_table_name = self._get_redshift_table_name(table_name)
            
            with self.redshift_connection() as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{redshift_table_name.split('.')[-1]}' 
                    AND table_schema = '{redshift_table_name.split('.')[0]}'
                """)
                
                table_exists = cursor.fetchone()[0] > 0
                
                if table_exists:
                    logger.info(f"Redshift table already exists: {redshift_table_name}")
                    return True
                else:
                    logger.info(f"Redshift table creation needed: {redshift_table_name}")
                    # TODO: Implement table creation based on S3 schema
                    # This would involve reading the parquet schema and generating CREATE TABLE SQL
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to check/create Redshift table {table_name}: {e}")
            return False
    
    def verify_row_counts(self, table_names: List[str]) -> bool:
        """
        Verify that row counts match between MySQL extraction and Redshift loading.
        
        Args:
            table_names: List of table names to verify
            
        Returns:
            True if all row counts match
        """
        try:
            all_match = True
            
            for table_name in table_names:
                watermark = self.watermark_manager.get_table_watermark(table_name)
                
                if watermark:
                    mysql_rows = watermark.mysql_rows_extracted
                    redshift_rows = watermark.redshift_rows_loaded
                    
                    if mysql_rows == redshift_rows:
                        logger.info(
                            f"Row count verification passed for {table_name}: {mysql_rows} rows"
                        )
                    else:
                        logger.error(
                            f"Row count mismatch for {table_name}: "
                            f"MySQL={mysql_rows}, Redshift={redshift_rows}"
                        )
                        all_match = False
                else:
                    logger.warning(f"No watermark found for verification: {table_name}")
                    all_match = False
            
            return all_match
            
        except Exception as e:
            logger.error(f"Row count verification failed: {e}")
            return False