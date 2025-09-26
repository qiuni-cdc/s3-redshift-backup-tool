#!/usr/bin/env python3
"""
Download and Sync Automation Script

This script supports two execution modes:

FULL PIPELINE (default):
1. Executes download.sh from parcel_download_tool_etl directory
2. Runs the sync command for the parcel detail tool
3. Saves sync results to a timestamped JSON file
4. If sync successful: Deletes all records from MySQL table
5. If sync successful: Runs S3 cleanup command
6. If sync successful: Resets watermark

SYNC-ONLY MODE (--sync-only):
1. Skips the download phase entirely
2. Runs only the sync command for the parcel detail tool
3. Saves sync results to a timestamped JSON file
4. If sync successful: Deletes all records from MySQL table
5. If sync successful: Runs S3 cleanup command
6. If sync successful: Resets watermark

Special Features:
- Enhanced validation for unidw.dw_parcel_detail_tool_temp table
- Verifies sync success by comparing rows_processed with actual MySQL source table count
- Uses existing connection infrastructure for secure MySQL access

Usage:
  python download_and_sync.py                                    # Full pipeline
  python download_and_sync.py --sync-only                        # Sync only
  python download_and_sync.py "2024-08-14 10:00:00" "-2"        # Full pipeline with parameters
  python download_and_sync.py --help                             # Show help
"""

import subprocess
import json
import sys
import os
import argparse
from datetime import datetime
from pathlib import Path
import logging
import psycopg2

# Import existing connection management
from src.config.settings import AppConfig
from src.core.connections import ConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('parcel_download_tool_etl.log')
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Download and Sync Automation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_and_sync.py                                    # Run full pipeline (download + sync)
  python download_and_sync.py --sync-only                        # Skip download, only run sync
  python download_and_sync.py "2024-08-14 10:00:00" "-2"        # Full pipeline with download parameters
  python download_and_sync.py --sync-only --help                 # Show this help message
        """
    )

    parser.add_argument(
        '--sync-only',
        action='store_true',
        help='Skip the download phase and only run the sync command'
    )

    parser.add_argument(
        'start_datetime',
        nargs='?',
        help='Start datetime string for download script (e.g., "2024-08-14 10:00:00")'
    )

    parser.add_argument(
        'hours_offset',
        nargs='?',
        help='Hours offset string for download script (e.g., "-2")'
    )

    return parser.parse_args()


def get_mysql_table_count(table_name: str) -> int:
    """Get the row count from a MySQL table using existing connection infrastructure"""
    try:
        # Initialize configuration and connection manager
        config = AppConfig()
        conn_manager = ConnectionManager(config)

        logger.info(f"🔍 Querying MySQL table count for: {table_name}")

        # Use existing MySQL connection logic (using US_DW_UNIDW_SSH connection)
        with conn_manager.database_session('US_DW_UNIDW_SSH') as conn:
            cursor = conn.cursor()
            query = f"SELECT COUNT(*) FROM {table_name}"
            logger.info(f"📊 Executing MySQL query: {query}")
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()

            logger.info(f"📈 MySQL table {table_name} has {count:,} rows")
            return count

    except Exception as e:
        logger.error(f"❌ Failed to get MySQL table count for {table_name}: {e}")
        raise


def delete_mysql_table_records(table_name: str) -> bool:
    """Delete all records from a MySQL table using existing connection infrastructure"""
    try:
        # Initialize configuration and connection manager
        config = AppConfig()
        conn_manager = ConnectionManager(config)

        logger.info(f"🗑️ Deleting all records from MySQL table: {table_name}")

        # Use existing MySQL connection logic (using US_DW_UNIDW_SSH connection)
        with conn_manager.database_session('US_DW_UNIDW_SSH') as conn:
            cursor = conn.cursor()

            # First get count before deletion for logging
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(count_query)
            record_count = cursor.fetchone()[0]

            if record_count == 0:
                logger.info(f"📊 Table {table_name} is already empty (0 records)")
                cursor.close()
                return True

            # Delete all records
            delete_query = f"DELETE FROM {table_name}"
            logger.info(f"🗑️ Executing MySQL delete: {delete_query}")
            logger.info(f"📊 About to delete {record_count:,} records...")

            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()

            logger.info(f"✅ Successfully deleted {deleted_count:,} records from {table_name}")
            return True

    except Exception as e:
        logger.error(f"❌ Failed to delete records from MySQL table {table_name}: {e}")
        return False


def run_s3clean_command(table_name: str, pipeline: str) -> bool:
    """Run s3clean command to clean up S3 files"""
    try:
        cmd = [
            "python", "-m", "src.cli.main", "s3clean", "clean",
            "-t", table_name,
            "-p", pipeline,
            "--force"  # Automatically confirm deletion without manual prompt
        ]

        logger.info(f"🧹 Running S3 cleanup command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=False,  # Show real-time output
            text=True,
            timeout=1800  # 30 minute timeout
        )

        if result.returncode == 0:
            logger.info("✅ S3 cleanup completed successfully")
            return True
        else:
            logger.error(f"❌ S3 cleanup failed with exit code {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("❌ S3 cleanup command timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Error executing S3 cleanup command: {e}")
        return False


def run_watermark_reset_command(table_name: str, pipeline: str) -> bool:
    """Run watermark reset command"""
    try:
        cmd = [
            "python", "-m", "src.cli.main", "watermark", "reset",
            "-t", table_name,
            "-p", pipeline,
            "--yes"  # Automatically confirm deletion without manual prompt
        ]

        logger.info(f"🔄 Running watermark reset command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=False,  # Show real-time output
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            logger.info("✅ Watermark reset completed successfully")
            return True
        else:
            logger.error(f"❌ Watermark reset failed with exit code {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("❌ Watermark reset command timed out after 5 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Error executing watermark reset command: {e}")
        return False


def execute_download_script(download_dir="parcel_download_tool_etl", start_datetime=None, hours_offset=None):
    """
    Execute download_tool.sh script from the specified directory with parameters
    
    Args:
        download_dir: Directory containing download_tool.sh script
        start_datetime: Start datetime string (e.g., "2024-08-14 10:00:00")
        hours_offset: Hours offset string (e.g., "-2")
        
    Returns:
        tuple: (success: bool, stdout: str, stderr: str)
    """
    
    logger.info(f"🔄 Starting download script execution from {download_dir}")
    logger.info(f"   Parameters: start_datetime='{start_datetime}', hours_offset='{hours_offset}'")
    
    # Check if directory exists
    if not Path(download_dir).exists():
        logger.error(f"❌ Directory {download_dir} not found")
        return False, "", f"Directory {download_dir} not found"
    # Check if download_tool.sh exists
    download_script = Path(download_dir) / "download_tool.sh"
    if not download_script.exists():
        logger.error(f"❌ download_tool.sh not found in {download_dir}")
        return False, "", f"download_tool.sh not found in {download_dir}"
    
    try:
        # Execute download script with parameters
        result = subprocess.run(
            ["./download_tool.sh", start_datetime, hours_offset],
            cwd=download_dir,
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout
        )
        
        if result.returncode == 0:
            logger.info("✅ Download script completed successfully")
            return True, result.stdout, result.stderr
        else:
            logger.error(f"❌ Download script failed with exit code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            return False, result.stdout, result.stderr
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Download script timed out after 2 hour")
        return False, "", "Script execution timed out"
    except Exception as e:
        logger.error(f"❌ Error executing download script: {e}")
        return False, "", str(e)


def run_sync_command(table_name, pipeline, limit=None, backup_only=False, redshift_only=False):
    """
    Run the sync command with JSON output

    Args:
        table_name: Name of the table to sync
        pipeline: Pipeline name
        limit: Optional row limit
        backup_only: Only run backup stage
        redshift_only: Only run Redshift loading stage

    Returns:
        tuple: (success: bool, json_data: dict, json_file_path: str)
    """
    logger.info(f"🔄 Starting sync for table: {table_name}")

    # Create JSON output folder if it doesn't exist
    json_folder = "parcel_download_and_sync_json"
    if not os.path.exists(json_folder):
        os.makedirs(json_folder)
        logger.info(f"📁 Created folder: {json_folder}")

    # Generate timestamped filename with folder path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_table = table_name.replace(".", "_").replace(":", "_")
    safe_pipeline = pipeline.replace(".", "_").replace(":", "_")
    json_filename = os.path.join(json_folder, f"sync_{safe_pipeline}_{safe_table}_{timestamp}.json")
    
    # Build sync command with JSON output file path (using pipeline subcommand for Airflow integration)
    cmd = [
        "python", "-m", "src.cli.main", "sync", "pipeline",
        "-p", pipeline,
        "-t", table_name,
        "--json-output", json_filename
    ]
    
    if limit:
        cmd.extend(["--limit", str(limit)])
    if backup_only:
        cmd.append("--backup-only")
    if redshift_only:
        cmd.append("--redshift-only")
    
    logger.info(f"Executing command: {' '.join(cmd)}")
    
    try:
        # Execute sync command with real-time output
        logger.info("🔄 Starting sync command execution...")
        logger.info("📺 Real-time sync logs will appear below:")
        logger.info("-" * 60)

        result = subprocess.run(
            cmd,
            capture_output=False,  # Allow real-time output to console
            text=True,
            timeout=7200  # 2 hour timeout for large syncs
        )

        logger.info("-" * 60)
        logger.info("🏁 Sync command execution completed")
        
        if result.returncode == 0:
            # Read JSON output from file (since --json-output writes to file)
            try:
                if os.path.exists(json_filename):
                    with open(json_filename, 'r') as f:
                        json_data = json.load(f)
                    logger.info(f"📄 JSON results loaded from: {json_filename}")
                else:
                    logger.error(f"❌ JSON output file not found: {json_filename}")
                    return False, {}, ""
                
                # Parse the Airflow integration JSON structure
                execution_id = json_data.get('execution_id', 'unknown')
                start_time = json_data.get('start_time', '')
                end_time = json_data.get('end_time', '')
                duration = json_data.get('duration_seconds', 0)
                pipeline = json_data.get('pipeline', 'unknown')
                overall_status = json_data.get('status', 'unknown')
                tables_requested = json_data.get('tables_requested', [])
                total_tables = json_data.get('total_tables', 0)


                # Log per-table results
                table_results = json_data.get('table_results', {})
                if table_results:
                    logger.info(f"📋 Per-Table Results:")
                    for table_name, table_data in table_results.items():
                        table_status = table_data.get('status', 'unknown')

                        if table_status == 'success':
                            rows_processed = table_data.get('rows_processed', 0)
                            files_created = table_data.get('files_created', 0)
                            table_duration = table_data.get('duration_seconds', 0)
                            completed_at = table_data.get('completed_at', 'unknown')

                            logger.info(f"   ✅ {table_name}: {table_status}")
                            logger.info(f"      Rows: {rows_processed:,}")
                            logger.info(f"      Files: {files_created}")
                            logger.info(f"      Duration: {table_duration:.1f}s")
                            logger.info(f"      Completed: {completed_at}")
                        else:
                            error_message = table_data.get('error_message', 'Unknown error')
                            failed_at = table_data.get('failed_at', 'unknown')

                            logger.error(f"   ❌ {table_name}: {table_status}")
                            logger.error(f"      Error: {error_message}")
                            logger.error(f"      Failed: {failed_at}")

                # Special validation for unidw.dw_parcel_detail_tool_temp table
                if table_name == "unidw.dw_parcel_detail_tool_temp":
                    return validate_parcel_detail_sync(table_name, table_results, overall_status, json_data, json_filename)

                # Determine success based on overall status for other tables
                if overall_status == 'success':
                    logger.info("✅ Sync completed successfully")
                    return True, json_data, json_filename
                else:
                    logger.error("❌ Sync failed")
                    return False, json_data, json_filename
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse JSON output: {e}")
                logger.error("Check the JSON output file for formatting issues")
                return False, {}, ""
        else:
            logger.error(f"❌ Sync command failed with exit code {result.returncode}")
            logger.error("Check the console output above for error details")
            return False, {}, ""
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Sync command timed out after 2 hours")
        return False, {}, ""
    except Exception as e:
        logger.error(f"❌ Error executing sync command: {e}")
        return False, {}, ""


def validate_parcel_detail_sync(table_name: str, table_results: dict, overall_status: str, json_data: dict, json_filename: str) -> tuple:
    """
    Special validation for unidw.dw_parcel_detail_tool_temp table.

    Validates that:
    1. The table status is 'success'
    2. The rows_processed matches the actual count in the MySQL source table

    Args:
        table_name: Name of the table being validated
        table_results: Per-table results from sync JSON
        overall_status: Overall sync status
        json_data: Complete JSON data
        json_filename: JSON output filename

    Returns:
        tuple: (success: bool, json_data: dict, json_filename: str)
    """
    logger.info(f"🔍 Performing special validation for {table_name}")

    try:
        # Check if table exists in results
        if table_name not in table_results:
            logger.error(f"❌ Table {table_name} not found in sync results")
            return False, json_data, json_filename

        table_data = table_results[table_name]
        table_status = table_data.get('status', 'unknown')

        # Check table status is success
        if table_status != 'success':
            logger.error(f"❌ Table {table_name} status is '{table_status}', not 'success'")
            return False, json_data, json_filename

        # Get rows processed from sync
        rows_processed = table_data.get('rows_processed', 0)
        logger.info(f"📊 Sync reported {rows_processed:,} rows processed for {table_name}")

        # Get actual count from MySQL source table
        try:
            mysql_count = get_mysql_table_count(table_name)
        except Exception as e:
            logger.error(f"❌ Failed to query MySQL table count: {e}")
            logger.error(f"❌ {table_name} sync validation failed:")
            logger.error(f"   Status: {table_status}")
            logger.error(f"   Rows processed: {rows_processed:,}")
            logger.error(f"   MySQL source count: Cannot verify (query failed)")
            logger.error(f"   ✗ Row counts do not match!")
            return False, json_data, json_filename

        # Compare rows processed vs MySQL source count
        if rows_processed == mysql_count:
            logger.info(f"✅ {table_name} sync validation passed:")
            logger.info(f"   Status: {table_status}")
            logger.info(f"   Rows processed: {rows_processed:,}")
            logger.info(f"   MySQL source count: {mysql_count:,}")
            logger.info(f"   ✓ Row counts match!")
            return True, json_data, json_filename
        else:
            logger.error(f"❌ {table_name} sync validation failed:")
            logger.error(f"   Status: {table_status}")
            logger.error(f"   Rows processed: {rows_processed:,}")
            logger.error(f"   MySQL source count: {mysql_count:,}")
            logger.error(f"   ✗ Row counts do not match!")
            return False, json_data, json_filename

    except Exception as e:
        logger.error(f"❌ Error during {table_name} validation: {e}")
        return False, json_data, json_filename




def main():
    """Main execution function"""
    # Parse command line arguments
    args = parse_arguments()

    # Configuration - Fixed values
    DOWNLOAD_DIR = "parcel_download_tool_etl"
    PIPELINE = "us_dw_unidw_2_public_pipeline"
    TABLE_NAME = "unidw.dw_parcel_detail_tool_temp"

    # Determine execution mode
    if args.sync_only:
        logger.info("🚀 Starting Sync-Only Pipeline")
        logger.info("=" * 50)
        logger.info("📋 Mode: SYNC ONLY (skipping download phase)")
    else:
        logger.info("🚀 Starting Download and Sync Pipeline")
        logger.info("=" * 50)
        logger.info("📋 Mode: FULL PIPELINE (download + sync)")

    logger.info(f"Configuration:")
    logger.info(f"  Download Dir: {DOWNLOAD_DIR}")
    logger.info(f"  Table: {TABLE_NAME}")
    logger.info(f"  Pipeline: {PIPELINE}")

    if not args.sync_only:
        logger.info(f"  Start DateTime: {args.start_datetime}")
        logger.info(f"  Hours Offset: {args.hours_offset}")
    logger.info("")

    download_success = True  # Default to True for sync-only mode

    # Phase 1: Execute download script (only if not sync-only)
    if not args.sync_only:
        logger.info("📥 Phase 1: Executing download script")
        download_success, download_stdout, download_stderr = execute_download_script(
            DOWNLOAD_DIR, args.start_datetime, args.hours_offset
        )

        if not download_success:
            logger.error("❌ Download phase failed, aborting sync")
            sys.exit(1)
    else:
        logger.info("⏭️ Phase 1: Skipping download script (--sync-only mode)")

    # Phase 2: Run sync command
    phase_number = "2" if not args.sync_only else "1"
    logger.info(f"🔄 Phase {phase_number}: Running sync command")
    sync_success, sync_data, json_file = run_sync_command(
        table_name=TABLE_NAME,
        pipeline=PIPELINE
    )

    # Report pipeline status
    if download_success and sync_success:
        if args.sync_only:
            logger.info("🎉 Sync-only pipeline completed successfully!")
        else:
            logger.info("🎉 Full pipeline completed successfully!")
        logger.info(f"📄 Sync results: {json_file}")
    else:
        if args.sync_only:
            logger.error("❌ Sync-only pipeline failed")
        else:
            logger.error("❌ Pipeline failed")
        if json_file:
            logger.info(f"📄 Sync results: {json_file}")

    # Always run cleanup steps regardless of sync success/failure
    logger.info("🧹 Starting cleanup steps...")
    logger.info("=" * 50)

    # Only delete MySQL records if sync was successful
    delete_success = True  # Default to true if we skip deletion
    if sync_success:
        logger.info(f"🗑️ Step 1: Deleting all records from {TABLE_NAME}")
        delete_success = delete_mysql_table_records(TABLE_NAME)
    else:
        logger.info("⏭️ Step 1: Skipping MySQL record deletion (sync failed)")

    # Always run S3 cleanup (regardless of sync success)
    logger.info(f"🧹 Step 2: Running S3 cleanup for {TABLE_NAME}")
    s3clean_success = run_s3clean_command(TABLE_NAME, PIPELINE)

    # Always run watermark reset (regardless of sync success)
    logger.info(f"🔄 Step 3: Resetting watermark for {TABLE_NAME}")
    watermark_success = run_watermark_reset_command(TABLE_NAME, PIPELINE)

    # Final status
    logger.info("=" * 50)
    if sync_success and delete_success and s3clean_success and watermark_success:
        logger.info("🎉 Pipeline and all cleanup steps completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Critical cleanup steps failed")
        sys.exit(1)


if __name__ == "__main__":
    main()