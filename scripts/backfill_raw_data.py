#!/usr/bin/env python3
"""
Backfill Raw Data Script

Loads historical data from MySQL to Redshift raw tables day-by-day.
This is useful for performance testing with large datasets.

Usage:
    python scripts/backfill_raw_data.py \
        --start-date 2025-12-13 \
        --end-date 2026-02-13 \
        --table ecs_order_info

Features:
- Day-by-day loading with progress tracking
- Automatic commit after each day
- Resume capability (skips already loaded days)
- Detailed logging and statistics
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any
# Standard MySQL type codes (approximate) - we use raw codes to avoid dependency on specific connector version
# 253=VAR_STRING, 254=STRING, 15=VARCHAR, 252=BLOB, 246=NEWDECIMAL, 3=LONG, 8=LONGLONG, 1=TINY, 2=SHORT, 9=INT24, 7=TIMESTAMP, 12=DATETIME, 10=DATE
import time
from pathlib import Path
import boto3
import gzip
import csv
from io import BytesIO, StringIO
from io import BytesIO, StringIO

# Add project root to path (works in both Docker and host)
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

from src.config.settings import AppConfig
from src.core.connections import ConnectionManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


class RawDataBackfiller:
    """Backfill raw tables with historical data from MySQL"""
    
    # Table configuration mapping
    TABLE_CONFIG = {
        'ecs_order_info': {
            'mysql_table': 'uniods.dw_ecs_order_info',
            'redshift_table': 'settlement_public.ecs_order_info_raw',
            'timestamp_column': 'add_time',
            'id_column': 'order_id',
            'mysql_connection': 'US_DW_UNIODS_SSH',
            'batch_size': 50000,
            'excluded_columns': ['id'],
            'column_defaults': {}, # Will use dynamic defaults
            'auto_defaults': True  # Enable dynamic defaults
        },
        'uni_tracking_info': {
            'mysql_table': 'uniods.dw_uni_tracking_info',
            'redshift_table': 'settlement_public.uni_tracking_info_raw',
            'timestamp_column': 'update_time',
            'id_column': 'order_id',
            'mysql_connection': 'US_DW_UNIODS_SSH',
            'batch_size': 50000,
            'excluded_columns': ['id'],
            'auto_defaults': True
        },
        'uni_tracking_spath': {
            'mysql_table': 'uniods.dw_uni_tracking_spath',
            'redshift_table': 'settlement_public.uni_tracking_spath_raw',
            'timestamp_column': 'pathTime',
            'id_column': 'id',
            'mysql_connection': 'US_DW_UNIODS_SSH',
            'batch_size': 100000,
            'auto_defaults': True
        }
    }
    
    
    
    def __init__(self):
        """Initialize backfiller with configuration (reads from environment variables)"""
        self.config = AppConfig()
        self.connection_manager = ConnectionManager(self.config)
        
        
        # S3 Client and Credentials
        self.s3_client = None
        self.bucket_name = os.environ.get('S3_BUCKET_NAME')
        self.aws_ak = None
        self.aws_sk = None
        
    def _init_s3(self):
        if self.s3_client:
            return

        # 0. Emergency: manually load .env if present (fixes missing Docker env vars)
        try:
            from dotenv import load_dotenv
            # Search paths for .env
            search_paths = [
                Path.cwd() / '.env',
                Path.cwd() / 'airflow_poc' / '.env',
                Path(__file__).parent.parent / '.env', # Project root
                Path(__file__).parent.parent / 'airflow_poc' / '.env'
            ]
            
            for p in search_paths:
                if p.exists():
                    logger.info(f"Loading environment from {p}")
                    load_dotenv(p)
                    break
        except ImportError:
            logger.warning("python-dotenv not installed, skipping manual .env load")

        # 1. Try Airflow Connection Direct Inspection (most reliable for Airflow Context)
        try:
            from airflow.hooks.base import BaseHook
            conn = BaseHook.get_connection('aws_default')
            if conn.login and conn.password:
                self.aws_ak = conn.login
                self.aws_sk = conn.password
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_ak,
                    aws_secret_access_key=self.aws_sk,
                    region_name=os.environ.get('S3_REGION', 'us-west-2')
                )
                logger.info(f"Initialized S3 from Airflow Connection 'aws_default' (found key: {self.aws_ak[:4]}****)")
                return
        except Exception as e:
            logger.debug(f"Could not use Airflow Connection: {e}")

        # 2. Fallback to Env Vars / Boto3 (for non-Airflow contexts)
        # Check standard AND fallback keys
        ak = os.environ.get('AWS_ACCESS_KEY_ID') or os.environ.get('S3_ACCESS_KEY') or os.environ.get('AWS_ACCESS_KEY_ID_QA')
        sk = os.environ.get('AWS_SECRET_ACCESS_KEY') or os.environ.get('S3_SECRET_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY_QA')
        
        self.s3_client = boto3.client(
            's3',
            region_name=os.environ.get('S3_REGION', 'us-west-2'),
            aws_access_key_id=ak,
            aws_secret_access_key=sk
        )
        
        if ak and sk:
            self.aws_ak = ak
            self.aws_sk = sk
            logger.info(f"Using AWS Access Key from Env: {ak[:4]}****")
        else:
            logger.warning("AWS credentials not found in Env or Airflow Connection!")

        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME env var is required for S3 mode")
            
        logger.info(f"Initialized S3 client for bucket: {self.bucket_name}")
        
    def get_day_range_unix(self, date_dt: datetime) -> tuple:
        """
        Get Unix timestamp boundaries for a given date.
        
        Returns:
            (start_unix, end_unix): Start of day (00:00:00) and end of day (23:59:59)
        """
        start_of_day = date_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
        return int(start_of_day.timestamp()), int(end_of_day.timestamp())
    
    def check_day_loaded(self, redshift_conn, table_config: Dict, date_dt: datetime) -> bool:
        """Check if data for this day already exists in Redshift"""
        start_unix, end_unix = self.get_day_range_unix(date_dt)
        
        query = f"""
            SELECT COUNT(*) 
            FROM {table_config['redshift_table']}
            WHERE {table_config['timestamp_column']} >= {start_unix}
              AND {table_config['timestamp_column']} <= {end_unix}
        """
        
        cursor = redshift_conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        
        logger.info(f"   [Check] Found {count} rows in {table_config['redshift_table']} for range {date_dt.strftime('%Y-%m-%d')} ({start_unix}-{end_unix})")
        
        return count > 0

    def load_day_data_s3(self, mysql_conn, redshift_conn, table_config: Dict, date_dt: datetime,
                      skip_if_exists: bool = True, limit: int = None) -> Dict[str, Any]:
        """Load one day of data using S3 COPY strategy"""
        start_time = time.time()
        start_unix, end_unix = self.get_day_range_unix(date_dt)
        date_str = date_dt.strftime('%Y-%m-%d')
        
        # Check if already loaded
        if skip_if_exists and self.check_day_loaded(redshift_conn, table_config, date_dt):
            logger.info(f"📅 {date_str}: Already loaded (checked via SQL), skipping")
            return {'date': date_str, 'status': 'skipped', 'rows': 0, 'duration': 0}
            
        logger.info(f"📅 {date_str}: Starting S3 load (Unix: {start_unix} - {end_unix})")
        
        # Prepare for Redshift Load
        redshift_cursor = redshift_conn.cursor()
        
        # 1. DELETE existing data for this day (Idempotency) - ONCE per day
        # Critical for safe re-runs and partial loads
        try:
            delete_sql = f"""
                DELETE FROM {table_config['redshift_table']} 
                WHERE {table_config['timestamp_column']} >= {start_unix}
                  AND {table_config['timestamp_column']} <= {end_unix}
            """
            logger.info(f"   Cleaning up existing data for {date_str}...")
            redshift_cursor.execute(delete_sql)
            redshift_conn.commit()
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            redshift_conn.rollback()
            raise

        # 2. Process in CHUNKS (e.g. 1 hour) to avoid timeout/memory issues
        CHUNK_HOURS = 1
        chunk_seconds = CHUNK_HOURS * 3600
        current_chunk_start = start_unix
        
        total_day_rows = 0
        chunks_processed = 0
        
        while current_chunk_start <= end_unix:
            chunk_end = min(current_chunk_start + chunk_seconds - 1, end_unix)
            chunk_start_dt = datetime.fromtimestamp(current_chunk_start)
            chunk_end_dt = datetime.fromtimestamp(chunk_end)
            
            logger.info(f"   Processing Chunk: {chunk_start_dt.strftime('%H:%M')} - {chunk_end_dt.strftime('%H:%M')}...")
            
            # --- EXTRACT CHUNK ---
            mysql_cursor = mysql_conn.cursor()
            extract_query = f"""
                SELECT * FROM {table_config['mysql_table']}
                WHERE {table_config['timestamp_column']} >= {current_chunk_start}
                  AND {table_config['timestamp_column']} <= {chunk_end}
                ORDER BY {table_config['timestamp_column']}, {table_config['id_column']}
            """
            if limit and limit < 1000: extract_query += f" LIMIT {limit}" # Only limit small tests
            
            mysql_cursor.execute(extract_query)
            
            # Get metadata (only needed once, but safe to get every time)
            if chunks_processed == 0:
                raw_column_names = [desc[0] for desc in mysql_cursor.description]
                column_types = {desc[0]: desc[1] for desc in mysql_cursor.description}
                excluded_cols = table_config.get('excluded_columns', [])
                column_defaults = table_config.get('column_defaults', {})
                auto_defaults = table_config.get('auto_defaults', False)
                keep_indices = [i for i, col in enumerate(raw_column_names) if col not in excluded_cols]
                column_names = [raw_column_names[i] for i in keep_indices]
            
            rows = mysql_cursor.fetchall()
            chunk_rows = len(rows)
            mysql_cursor.close()
            
            if chunk_rows == 0:
                current_chunk_start += chunk_seconds
                continue
                
            # --- UPLOAD CHUNK ---
            self._init_s3()
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            for row in rows:
                clean_row = []
                for i in keep_indices:
                    col_name = raw_column_names[i]
                    val = row[i]
                    if val is None:
                        if col_name in column_defaults: val = column_defaults[col_name]
                        elif auto_defaults:
                            type_code = column_types.get(col_name)
                            if type_code in (253, 254, 15, 252, 249, 250, 251): val = ''
                            elif type_code in (1, 2, 3, 8, 9): val = 0
                            elif type_code in (4, 5, 246): val = 0.0
                    clean_row.append(val)
                writer.writerow(clean_row)
                
            gz_buffer = BytesIO()
            with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz:
                gz.write(csv_buffer.getvalue().encode('utf-8'))
            gz_buffer.seek(0)
            
            s3_key = f"backfill/temp_{table_config['redshift_table']}_{date_str}_chunk_{current_chunk_start}_{int(time.time())}.csv.gz"
            full_s3_path = f"s3://{self.bucket_name}/{s3_key}"
            
            logger.info(f"      Uploading {len(gz_buffer.getvalue())/1024:.1f} KB...")
            self.s3_client.upload_fileobj(gz_buffer, self.bucket_name, s3_key)
            
            # --- COPY CHUNK ---
            iam_role = os.environ.get('REDSHIFT_IAM_ROLE')
            creds = ""
            if iam_role: creds = f"IAM_ROLE '{iam_role}'"
            else:
                if not self.aws_ak or not self.aws_sk: raise ValueError("Missing AWS credentials")
                creds = f"CREDENTIALS 'aws_access_key_id={self.aws_ak};aws_secret_access_key={self.aws_sk}'"
                
            copy_sql = f"""
                COPY {table_config['redshift_table']} ({', '.join(column_names)})
                FROM '{full_s3_path}'
                {creds}
                GZIP
                CSV DELIMITER ','
                IGNOREHEADER 0
                ACCEPTINVCHARS
                TRUNCATECOLUMNS
                COMPUPDATE OFF
                STATUPDATE OFF
                REGION '{os.environ.get('S3_REGION', 'us-west-2')}'
            """
            
            try:
                redshift_cursor.execute(copy_sql)
                redshift_conn.commit()
                total_day_rows += chunk_rows
                chunks_processed += 1
            except Exception as e:
                logger.error(f"Chunk COPY failed: {e}")
                redshift_conn.rollback()
                raise
            finally:
                try: self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
                except: pass
            
            current_chunk_start += chunk_seconds

        redshift_cursor.close()
        
        duration = time.time() - start_time
        rate = total_day_rows / duration if duration > 0 else 0
        logger.info(f"✅ {date_str}: Loaded {total_day_rows:,} rows via S3 (in {chunks_processed} chunks) in {duration:.1f}s ({rate:,.0f} rows/sec)")
        
        return {'date': date_str, 'status': 'success', 'rows': total_day_rows, 'duration': duration}
    
    def load_day_data(self, mysql_conn, redshift_conn, table_config: Dict, date_dt: datetime,
                      skip_if_exists: bool = True, limit: int = None, use_s3: bool = False) -> Dict[str, Any]:
        """Load one day of data using specified strategy"""
        
        if use_s3:
            return self.load_day_data_s3(mysql_conn, redshift_conn, table_config, date_dt, skip_if_exists, limit)
            
        # ... existing logic ...
        start_unix, end_unix = self.get_day_range_unix(date_dt)
        date_str = date_dt.strftime('%Y-%m-%d')
        
        # Check if already loaded
        if skip_if_exists and self.check_day_loaded(redshift_conn, table_config, date_dt):
            logger.info(f"📅 {date_str}: Already loaded, skipping")
            return {'date': date_str, 'status': 'skipped', 'rows': 0, 'duration': 0}
        
        logger.info(f"📅 {date_str}: Starting load (Unix: {start_unix} - {end_unix})")
        
        # Extract from MySQL
        mysql_cursor = mysql_conn.cursor()
        
        extract_query = f"""
            SELECT * FROM {table_config['mysql_table']}
            WHERE {table_config['timestamp_column']} >= {start_unix}
              AND {table_config['timestamp_column']} <= {end_unix}
            ORDER BY {table_config['timestamp_column']}, {table_config['id_column']}
        """
        
        if limit:
            extract_query += f" LIMIT {limit}"
        
        logger.info(f"   Extracting from MySQL...")
        mysql_cursor.execute(extract_query)
        
        # Get column names and indices to keep
        raw_column_names = [desc[0] for desc in mysql_cursor.description]
        
        # Capture types for dynamic defaults
        # desc[1] is type_code
        column_types = {desc[0]: desc[1] for desc in mysql_cursor.description}
        
        excluded_cols = table_config.get('excluded_columns', [])
        column_defaults = table_config.get('column_defaults', {})
        auto_defaults = table_config.get('auto_defaults', False)
        
        # Calculate indices to keep
        keep_indices = [i for i, col in enumerate(raw_column_names) if col not in excluded_cols]
        column_names = [raw_column_names[i] for i in keep_indices]
        
        logger.info(f"   Columns: {len(column_names)} (excluded: {excluded_cols})")
        
        # Fetch all rows for this day
        rows = mysql_cursor.fetchall()
        total_rows = len(rows)
        mysql_cursor.close()
        
        if total_rows == 0:
            logger.info(f"   No data found for {date_str}")
            return {'date': date_str, 'status': 'no_data', 'rows': 0, 'duration': time.time() - start_time}
        
        logger.info(f"   Extracted {total_rows:,} rows from MySQL")
        
        # Use multi-row INSERT for speed (faster than executemany, no S3 needed)
        # Redshift has query size limits, so we use batches of 2000 rows
        insert_batch_size = 2000  # Rows per single INSERT statement
        columns_str = ', '.join(column_names)
        
        redshift_cursor = redshift_conn.cursor()
        
        logger.info(f"   Loading to Redshift via multi-row INSERTs (batch size: {insert_batch_size})...")
        loaded_rows = 0
        
        try:
            # Prepare template for ONE row: (%s, %s, ...)
            row_template = '(' + ', '.join(['%s'] * len(column_names)) + ')'
            
            for i in range(0, total_rows, insert_batch_size):
                batch = rows[i:i + insert_batch_size]
                
                # Format values for SQL safety
                values_list = []
                for row in batch:
                    # Filter row values and apply defaults
                    filtered_row = []
                    for i in keep_indices:
                        col_name = raw_column_names[i]
                        val = row[i]
                        
                        # Apply default if value is None
                        if val is None:
                            # 1. Manual override
                            if col_name in column_defaults:
                                val = column_defaults[col_name]
                            # 2. Auto defaults based on type
                            elif auto_defaults:
                                type_code = column_types.get(col_name)
                                # String types (VARCHAR, VAR_STRING, STRING, BLOB, TEXT)
                                if type_code in (253, 254, 15, 252, 249, 250, 251):
                                    val = ''
                                # Numeric types (TINY, SHORT, LONG, INT24, LONGLONG)
                                elif type_code in (1, 2, 3, 8, 9):
                                    val = 0
                                # Floating point (FLOAT, DOUBLE, NEWDECIMAL)
                                elif type_code in (4, 5, 246):
                                    val = 0.0
                                # For Date/Time, keeping None is usually safer unless strictly NOT NULL
                                # but usually dates allow NULL. Strings are the main pain point.
                            
                        filtered_row.append(val)
                    
                    # Use mogrify to safely format values (handles quotes, nulls, dates, etc.)
                    formatted_val = redshift_cursor.mogrify(row_template, filtered_row).decode('utf-8')
                    values_list.append(formatted_val)
                
                values_str = ','.join(values_list)
                insert_sql = f"INSERT INTO {table_config['redshift_table']} ({columns_str}) VALUES {values_str}"
                
                redshift_cursor.execute(insert_sql)
                loaded_rows += len(batch)
                
                # Periodic commit/progress report
                if (i + insert_batch_size) % 5000 == 0 or (i + insert_batch_size >= total_rows):
                     logger.info(f"   Progress: {loaded_rows:,} / {total_rows:,} rows")
            
            redshift_conn.commit()
            
        except Exception as e:
            logger.error(f"   INSERT failed: {e}")
            redshift_conn.rollback()
            raise
        finally:
            redshift_cursor.close()
        
        duration = time.time() - start_time
        rate = total_rows / duration if duration > 0 else 0
        
        logger.info(f"✅ {date_str}: Loaded {total_rows:,} rows in {duration:.1f}s ({rate:,.0f} rows/sec)")
        
        return {'date': date_str, 'status': 'success', 'rows': total_rows, 'duration': duration}
    
    def backfill(self, table_name: str, start_unix: int, end_unix: int,
                 skip_if_exists: bool = True, dry_run: bool = False, limit: int = None, use_s3: bool = False):
        """
        Backfill data for a Unix timestamp range
        
        Args:
            table_name: Table to backfill (ecs_order_info, uni_tracking_info, uni_tracking_spath)
            start_unix: Start Unix timestamp
            end_unix: End Unix timestamp
            skip_if_exists: Skip days that already have data
            dry_run: If True, only check what would be loaded
        """
        
        if table_name not in self.TABLE_CONFIG:
            raise ValueError(f"Unknown table: {table_name}. Valid tables: {list(self.TABLE_CONFIG.keys())}")
        
        table_config = self.TABLE_CONFIG[table_name]
        
        # Convert Unix timestamps to datetime for day iteration
        start_dt = datetime.fromtimestamp(start_unix).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = datetime.fromtimestamp(end_unix).replace(hour=0, minute=0, second=0, microsecond=0)
        
        if start_dt > end_dt:
            raise ValueError("Start timestamp must be before end timestamp")
        
        total_days = (end_dt - start_dt).days + 1
        
        logger.info("=" * 80)
        logger.info(f"🚀 BACKFILL STARTED")
        logger.info(f"   Table: {table_name}")
        logger.info(f"   MySQL: {table_config['mysql_table']}")
        logger.info(f"   Redshift: {table_config['redshift_table']}")
        logger.info(f"   Date Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')} ({total_days} days)")
        logger.info(f"   Skip Existing: {skip_if_exists}")
        logger.info(f"   Dry Run: {dry_run}")
        logger.info("=" * 80)
        
        if dry_run:
            logger.info("DRY RUN MODE - No data will be loaded")
            return
        
        # Get connections using the correct project APIs
        logger.info("Connecting to databases...")
        
        # Optimize MySQL connection for high throughput (backfill specific)
        os.environ['MYSQL_COMPRESS'] = 'true'
        
        mysql_connection_name = table_config['mysql_connection']
        
        # Iteration logic with ROBUST CONNECTION HANDLING per day
        # This ensures lost connections don't kill the whole process
        results = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y-%m-%d')
            
            try:
                # Open fresh connections for EACH day to handle timeouts/drops gracefully
                with self.connection_manager.database_session(mysql_connection_name) as mysql_conn:
                    if not mysql_conn.is_connected():
                         # Force reconnect if needed (though database_session should handle it)
                         mysql_conn.reconnect(attempts=3, delay=2)
                         
                    # Use ConnectionRegistry for Redshift
                    with self.connection_manager.connection_registry.get_redshift_connection('redshift_default_direct') as redshift_conn:
                        
                        result = self.load_day_data(
                            mysql_conn,
                            redshift_conn,
                            table_config,
                            current_dt,
                            skip_if_exists,
                            limit,
                            use_s3=use_s3
                        )
                        results.append(result)
            
            except Exception as e:
                logger.error(f"❌ {date_str}: Failed - {e}")
                results.append({
                    'date': date_str,
                    'status': 'error',
                    'rows': 0,
                    'duration': 0,
                    'error': str(e)
                })
                logger.warning(f"Skipping {date_str} due to error, continuing...")
            
            current_dt += timedelta(days=1)
        
        # Print summary
        logger.info("=" * 80)
        logger.info("📊 BACKFILL SUMMARY")
        logger.info("=" * 80)
        
        total_rows = sum(r['rows'] for r in results)
        total_duration = sum(r['duration'] for r in results)
        success_count = sum(1 for r in results if r['status'] == 'success')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        no_data_count = sum(1 for r in results if r['status'] == 'no_data')
        error_count = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Total Days Processed: {len(results)}")
        logger.info(f"  ✅ Success: {success_count}")
        logger.info(f"  ⏭️  Skipped: {skipped_count}")
        logger.info(f"  📭 No Data: {no_data_count}")
        logger.info(f"  ❌ Errors: {error_count}")
        logger.info(f"Total Rows Loaded: {total_rows:,}")
        logger.info(f"Total Duration: {total_duration:.1f}s")
        
        if total_duration > 0:
            avg_rate = total_rows / total_duration
            logger.info(f"Average Rate: {avg_rate:,.0f} rows/sec")
        
        logger.info("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Backfill raw tables with historical data from MySQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load 2 months using dates
  python scripts/backfill_raw_data.py \\
      --start-date 2025-12-17 \\
      --end-date 2026-02-17 \\
      --table ecs_order_info

  # Load using Unix timestamps
  python scripts/backfill_raw_data.py \\
      --start-unix 1734393600 \\
      --end-unix 1739750400 \\
      --table ecs_order_info

  # Load all three tables
  python scripts/backfill_raw_data.py \\
      --start-date 2025-12-17 \\
      --end-date 2026-02-17 \\
      --table ecs_order_info,uni_tracking_info,uni_tracking_spath

  # Dry run (check what would be loaded)
  python scripts/backfill_raw_data.py \\
      --start-date 2025-12-17 \\
      --end-date 2026-02-17 \\
      --table ecs_order_info \\
      --dry-run
        """
    )
    
    # Accept either Unix timestamps or dates
    parser.add_argument('--start-unix', type=int, help='Start Unix timestamp')
    parser.add_argument('--end-unix', type=int, help='End Unix timestamp')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    
    parser.add_argument('--table', required=True,
                       help='Table name(s) to backfill (comma-separated): ecs_order_info, uni_tracking_info, uni_tracking_spath')
    parser.add_argument('--force', action='store_true',
                       help='Force reload even if data exists')
    parser.add_argument('--dry-run', action='store_true',
                       help='Dry run - show what would be loaded without loading')
    
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--limit', type=int, help='Limit rows per day (for testing)')
    parser.add_argument('--use-s3', action='store_true', help='Use S3 COPY loading (faster)')
    
    args = parser.parse_args()
    
    # Determine start and end Unix timestamps
    if args.start_unix and args.end_unix:
        start_unix = args.start_unix
        end_unix = args.end_unix
    elif args.start_date and args.end_date:
        start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
        start_unix = int(start_dt.timestamp())
        end_unix = int(end_dt.timestamp())
    else:
        parser.error("Must provide either (--start-unix and --end-unix) or (--start-date and --end-date)")
    
    # Parse table names
    tables = [t.strip() for t in args.table.split(',')]
    
    # Create backfiller (config read from environment variables)
    backfiller = RawDataBackfiller()
    
    # Process each table
    for table in tables:
        try:
            backfiller.backfill(
                table_name=table,
                start_unix=start_unix,
                end_unix=end_unix,
                skip_if_exists=not args.force,
                dry_run=args.dry_run,
                limit=args.limit,
                use_s3=args.use_s3
            )
        except Exception as e:
            logger.error(f"Failed to backfill {table}: {e}")
            import traceback
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    main()
