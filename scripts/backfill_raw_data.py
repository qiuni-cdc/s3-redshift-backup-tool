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
import time
from pathlib import Path

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
            'mysql_table': 'kuaisong.ecs_order_info',
            'redshift_table': 'settlement_public.ecs_order_info_raw',
            'timestamp_column': 'add_time',
            'id_column': 'order_id',
            'mysql_connection': 'US_PROD_RO_SSH',
            'batch_size': 50000
        },
        'uni_tracking_info': {
            'mysql_table': 'kuaisong.uni_tracking_info',
            'redshift_table': 'settlement_public.uni_tracking_info_raw',
            'timestamp_column': 'update_time',
            'id_column': 'order_id',
            'mysql_connection': 'US_PROD_RO_SSH',
            'batch_size': 50000
        },
        'uni_tracking_spath': {
            'mysql_table': 'kuaisong.uni_tracking_spath',
            'redshift_table': 'settlement_public.uni_tracking_spath_raw',
            'timestamp_column': 'pathTime',
            'id_column': 'id',
            'mysql_connection': 'US_PROD_RO_SSH',
            'batch_size': 100000
        }
    }
    
    def __init__(self):
        """Initialize backfiller with configuration (reads from environment variables)"""
        self.config = AppConfig()
        self.connection_manager = ConnectionManager(self.config)
        
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
        
        return count > 0
    
    def load_day_data(self, mysql_conn, redshift_conn, table_config: Dict, date_dt: datetime,
                      skip_if_exists: bool = True) -> Dict[str, Any]:
        """Load one day of data from MySQL to Redshift"""
        
        start_time = time.time()
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
        
        logger.info(f"   Extracting from MySQL...")
        mysql_cursor.execute(extract_query)
        
        # Get column names
        column_names = [desc[0] for desc in mysql_cursor.description]
        
        # Fetch all rows for this day
        rows = mysql_cursor.fetchall()
        total_rows = len(rows)
        mysql_cursor.close()
        
        if total_rows == 0:
            logger.info(f"   No data found for {date_str}")
            return {'date': date_str, 'status': 'no_data', 'rows': 0, 'duration': time.time() - start_time}
        
        logger.info(f"   Extracted {total_rows:,} rows from MySQL")
        
        # Load to Redshift in batches
        redshift_cursor = redshift_conn.cursor()
        batch_size = table_config['batch_size']
        loaded_rows = 0
        
        # Build INSERT statement
        placeholders = ', '.join(['%s'] * len(column_names))
        insert_query = f"""
            INSERT INTO {table_config['redshift_table']} 
            ({', '.join(column_names)})
            VALUES ({placeholders})
        """
        
        logger.info(f"   Loading to Redshift in batches of {batch_size:,}...")
        
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            redshift_cursor.executemany(insert_query, batch)
            loaded_rows += len(batch)
            
            if i + batch_size < total_rows:
                logger.info(f"   Progress: {loaded_rows:,} / {total_rows:,} rows")
        
        # Commit this day's data
        redshift_conn.commit()
        redshift_cursor.close()
        
        duration = time.time() - start_time
        rate = total_rows / duration if duration > 0 else 0
        
        logger.info(f"✅ {date_str}: Loaded {total_rows:,} rows in {duration:.1f}s ({rate:,.0f} rows/sec)")
        
        return {'date': date_str, 'status': 'success', 'rows': total_rows, 'duration': duration}
    
    def backfill(self, table_name: str, start_unix: int, end_unix: int,
                 skip_if_exists: bool = True, dry_run: bool = False):
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
        
        mysql_connection_name = table_config['mysql_connection']
        
        # Use ConnectionManager.database_session for MySQL (handles SSH tunnel)
        with self.connection_manager.database_session(mysql_connection_name) as mysql_conn:
            logger.info(f"✅ MySQL connected via {mysql_connection_name}")
            
            # Use ConnectionRegistry for Redshift (handles SSH tunnel, no S3 dependency)
            with self.connection_manager.connection_registry.get_redshift_connection('redshift_default') as redshift_conn:
                logger.info("✅ Redshift connected")
                
                # Process each day
                results = []
                current_dt = start_dt
                
                while current_dt <= end_dt:
                    try:
                        result = self.load_day_data(
                            mysql_conn,
                            redshift_conn,
                            table_config,
                            current_dt,
                            skip_if_exists
                        )
                        results.append(result)
                        
                    except Exception as e:
                        date_str = current_dt.strftime('%Y-%m-%d')
                        logger.error(f"❌ {date_str}: Failed - {e}")
                        results.append({
                            'date': date_str,
                            'status': 'error',
                            'rows': 0,
                            'duration': 0,
                            'error': str(e)
                        })
                        
                        # Continue to next day on error (don't block the whole backfill)
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
                dry_run=args.dry_run
            )
        except Exception as e:
            logger.error(f"Failed to backfill {table}: {e}")
            import traceback
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    main()
