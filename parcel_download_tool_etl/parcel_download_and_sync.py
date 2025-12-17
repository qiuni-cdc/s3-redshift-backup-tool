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
  python parcel_download_and_sync.py                                              # Full pipeline (default: -0.5 hours)
  python parcel_download_and_sync.py --sync-only                                  # Sync only
  python parcel_download_and_sync.py -p us_dw_unidw_2_settlement_dws_pipeline    # Custom pipeline
  python parcel_download_and_sync.py --pipeline my_pipeline --sync-only          # Custom pipeline, sync only
  python parcel_download_and_sync.py -d "2024-08-14 10:00:00" --hours "-2"       # Full pipeline with datetime
  python parcel_download_and_sync.py --date "" --hours "-1"                       # Use current time, 1 hour ago
  python parcel_download_and_sync.py --help                                       # Show help
"""

import subprocess
import json
import sys
import os
import argparse
from datetime import datetime
from pathlib import Path
import logging

# Project root directory (parent of parcel_download_tool_etl)
# Get the absolute path to this script
SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Verify the project structure
if not (PROJECT_ROOT / 'src').exists():
    print(f"ERROR: Cannot find 'src' directory at {PROJECT_ROOT}")
    print(f"Script location: {SCRIPT_PATH}")
    print(f"Script directory: {SCRIPT_DIR}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Contents of project root: {list(PROJECT_ROOT.iterdir())}")
    sys.exit(1)

# Add project root to Python path so we can import from src/
sys.path.insert(0, str(PROJECT_ROOT))

# Import existing connection management
try:
    from src.core.connection_registry import ConnectionRegistry
except ModuleNotFoundError as e:
    print(f"ERROR: Failed to import ConnectionRegistry: {e}")
    print(f"Python path: {sys.path}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"src directory exists: {(PROJECT_ROOT / 'src').exists()}")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / 'parcel_download_tool_etl' / 'parcel_download_tool_etl.log')
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
  python parcel_download_and_sync.py                                              # Run full pipeline (default: -0.5 hours)
  python parcel_download_and_sync.py --sync-only                                  # Skip download, only run sync
  python parcel_download_and_sync.py -p us_dw_unidw_2_settlement_dws_pipeline    # Use custom pipeline
  python parcel_download_and_sync.py --pipeline my_pipeline --sync-only          # Custom pipeline, sync only
  python parcel_download_and_sync.py -d "2024-08-14 10:00:00" --hours "-2"       # Full pipeline with specific datetime
  python parcel_download_and_sync.py --date "" --hours "-1"                       # Use current time, 1 hour ago
  python parcel_download_and_sync.py -p my_pipeline -d "" --hours "-2"           # All options combined
  python parcel_download_and_sync.py --help                                       # Show this help message
        """
    )

    parser.add_argument(
        '--sync-only',
        action='store_true',
        help='Skip the download phase and only run the sync command'
    )

    parser.add_argument(
        '-p', '--pipeline',
        type=str,
        default='us_dw_unidw_2_public_pipeline',
        help='Pipeline name to use (default: us_dw_unidw_2_public_pipeline)'
    )

    parser.add_argument(
        '-d', '--date',
        type=str,
        default='',
        help='Start datetime string for download script (e.g., "2024-08-14 10:00:00"). Empty string means use current time.'
    )

    parser.add_argument(
        '--hours',
        type=str,
        default='-1',
        help='Hours offset string for download script (e.g., "-2" for 2 hours ago, "-1" for 1 hour ago). Default: -1'
    )

    return parser.parse_args()


def get_source_connection_from_pipeline(pipeline_name: str) -> str:
    """Get the source connection name from a pipeline configuration"""
    import yaml

    pipeline_path = PROJECT_ROOT / "config" / "pipelines" / f"{pipeline_name}.yml"
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Pipeline configuration not found: {pipeline_path}")

    with open(pipeline_path, 'r') as f:
        pipeline_config = yaml.safe_load(f)

    source_connection = pipeline_config.get('pipeline', {}).get('source')
    if not source_connection:
        raise ValueError(f"Pipeline '{pipeline_name}' missing 'source' configuration")

    return source_connection


def get_target_connection_from_pipeline(pipeline_name: str) -> str:
    """Get the target connection name from a pipeline configuration"""
    import yaml

    pipeline_path = PROJECT_ROOT / "config" / "pipelines" / f"{pipeline_name}.yml"
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Pipeline configuration not found: {pipeline_path}")

    with open(pipeline_path, 'r') as f:
        pipeline_config = yaml.safe_load(f)

    target_connection = pipeline_config.get('pipeline', {}).get('target')
    if not target_connection:
        raise ValueError(f"Pipeline '{pipeline_name}' missing 'target' configuration")

    return target_connection


def get_mysql_table_count(table_name: str, source_connection: str) -> int:
    """Get the row count from a MySQL table using existing connection infrastructure"""
    try:
        # Initialize connection registry (uses project root's connections.yml)
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying MySQL table count for: {table_name}")
        logger.info(f"📡 Using connection: {source_connection}")

        # Use the connection specified by the pipeline
        with conn_registry.get_mysql_connection(source_connection) as conn:
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


def get_mysql_inserted_at_range(table_name: str, source_connection: str) -> tuple:
    """
    Get the min and max inserted_at values from MySQL source table.

    Args:
        table_name: Name of the MySQL table (e.g., 'unidw.dw_parcel_detail_tool_temp')
        source_connection: MySQL connection name from connections.yml

    Returns:
        tuple: (min_inserted_at, max_inserted_at) as ISO format strings
    """
    try:
        # Initialize connection registry (uses project root's connections.yml)
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying MySQL inserted_at range for: {table_name}")
        logger.info(f"📡 Using connection: {source_connection}")

        # Use the connection specified by the pipeline
        with conn_registry.get_mysql_connection(source_connection) as conn:
            cursor = conn.cursor()
            query = f"SELECT MIN(inserted_at), MAX(inserted_at) FROM {table_name}"
            logger.info(f"📊 Executing MySQL query: {query}")
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()

            if result and result[0] and result[1]:
                min_inserted_at = result[0].isoformat() if hasattr(result[0], 'isoformat') else str(result[0])
                max_inserted_at = result[1].isoformat() if hasattr(result[1], 'isoformat') else str(result[1])
                logger.info(f"📈 MySQL inserted_at range: {min_inserted_at} to {max_inserted_at}")
                return min_inserted_at, max_inserted_at
            else:
                logger.warning(f"⚠️  No data found in {table_name}")
                return None, None

    except Exception as e:
        logger.error(f"❌ Failed to get MySQL inserted_at range for {table_name}: {e}")
        raise


def get_redshift_table_count(table_name: str, target_connection: str, min_inserted_at: str, max_inserted_at: str) -> int:
    """
    Get the row count from a Redshift table for rows with inserted_at in the specified range.
    Note: inserted_at in Redshift is synced from the MySQL source table, not the load timestamp.

    Args:
        table_name: Name of the Redshift table (e.g., 'dw_parcel_detail_tool')
        target_connection: Redshift connection name from connections.yml (e.g., 'redshift_settlement_dws')
        min_inserted_at: Minimum inserted_at value from MySQL source (ISO format string)
        max_inserted_at: Maximum inserted_at value from MySQL source (ISO format string)

    Returns:
        int: Count of rows where inserted_at is between min and max
    """
    try:
        # Initialize connection registry (uses project root's connections.yml)
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying Redshift table count for: {table_name}")
        logger.info(f"📡 Using connection: {target_connection}")
        logger.info(f"⏰ inserted_at range: {min_inserted_at} to {max_inserted_at}")

        # Use the Redshift connection specified
        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()
            query = f"""
                SELECT COUNT(distinct id)
                FROM {table_name}
                WHERE inserted_at >= %s AND inserted_at <= %s
            """
            logger.info(f"📊 Executing Redshift query: {query}")
            logger.info(f"📊 Parameters: min_inserted_at={min_inserted_at}, max_inserted_at={max_inserted_at}")
            cursor.execute(query, (min_inserted_at, max_inserted_at))
            count = cursor.fetchone()[0]
            cursor.close()

            logger.info(f"📈 Redshift table {table_name} has {count:,} rows in inserted_at range")
            return count

    except Exception as e:
        logger.error(f"❌ Failed to get Redshift table count for {table_name}: {e}")
        raise


def delete_redshift_records_by_inserted_at(table_name: str, target_connection: str, min_inserted_at: str, max_inserted_at: str) -> bool:
    """
    Delete records from Redshift table based on inserted_at range.

    This is used to clean up partial/incorrect loads before retrying.

    Args:
        table_name: Name of the Redshift table (e.g., 'dw_parcel_detail_tool')
        target_connection: Redshift connection name from connections.yml
        min_inserted_at: Minimum inserted_at value (ISO format string)
        max_inserted_at: Maximum inserted_at value (ISO format string)

    Returns:
        bool: True if deletion successful, False otherwise
    """
    try:
        # Initialize connection registry (uses project root's connections.yml)
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🗑️ Cleaning up Redshift table: {table_name}")
        logger.info(f"📡 Using connection: {target_connection}")
        logger.info(f"⏰ inserted_at range: {min_inserted_at} to {max_inserted_at}")

        # Use the Redshift connection specified
        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()
            try:
                # Delete records in the inserted_at range
                delete_query = f"""
                    DELETE FROM {table_name}
                    WHERE inserted_at >= %s AND inserted_at <= %s
                """
                logger.info(f"🗑️ Executing Redshift delete: DELETE FROM {table_name} WHERE inserted_at BETWEEN {min_inserted_at} AND {max_inserted_at}")

                cursor.execute(delete_query, (min_inserted_at, max_inserted_at))
                deleted_count = cursor.rowcount
                conn.commit()

                logger.info(f"✅ Successfully deleted {deleted_count:,} records from {table_name}")
                return True

            finally:
                # Always close cursor in finally block with error suppression
                try:
                    cursor.close()
                except:
                    pass  # Suppress any errors from cursor cleanup

    except Exception as e:
        logger.error(f"❌ Failed to delete records from Redshift table {table_name}: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def delete_mysql_table_records(table_name: str, source_connection: str) -> bool:
    """Delete all records from a MySQL table using existing connection infrastructure"""
    try:
        # Initialize connection registry (uses project root's connections.yml)
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🗑️ Deleting all records from MySQL table: {table_name}")
        logger.info(f"📡 Using connection: {source_connection}")

        # Use context manager correctly - let it handle all cleanup
        with conn_registry.get_mysql_connection(source_connection) as conn:
            cursor = conn.cursor()
            try:
                # First get count before deletion for logging
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                cursor.execute(count_query)
                record_count = cursor.fetchone()[0]

                if record_count == 0:
                    logger.info(f"📊 Table {table_name} is already empty (0 records)")
                    return True

                # Delete all records
                delete_query = f"DELETE FROM {table_name}"
                logger.info(f"🗑️ Executing MySQL delete: {delete_query}")
                logger.info(f"📊 About to delete {record_count:,} records...")
                logger.warning(f"⚠️  This may take a long time for large tables (20M+ rows could take hours)")

                cursor.execute(delete_query)
                deleted_count = cursor.rowcount
                conn.commit()

                logger.info(f"✅ Successfully deleted {deleted_count:,} records from {table_name}")
                return True

            finally:
                # Always close cursor in finally block with error suppression
                try:
                    cursor.close()
                except:
                    pass  # Suppress any errors from cursor cleanup

    except Exception as e:
        logger.error(f"❌ Failed to delete records from MySQL table {table_name}: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
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
            cwd=PROJECT_ROOT,  # Run from project root where src/ exists
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
            cwd=PROJECT_ROOT,  # Run from project root where src/ exists
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


def run_sync_command(table_name, pipeline, source_connection, target_connection, limit=None, backup_only=False, redshift_only=False):
    """
    Run the sync command with JSON output

    Args:
        table_name: Name of the table to sync
        pipeline: Pipeline name
        source_connection: MySQL source connection name
        target_connection: Redshift target connection name
        limit: Optional row limit
        backup_only: Only run backup stage
        redshift_only: Only run Redshift loading stage

    Returns:
        tuple: (success: bool, json_data: dict, json_file_path: str)
    """
    logger.info(f"🔄 Starting sync for table: {table_name}")

    # Create JSON output folder if it doesn't exist (in parcel_download_tool_etl)
    json_folder = PROJECT_ROOT / "parcel_download_tool_etl" / "parcel_download_and_sync_json"
    if not json_folder.exists():
        json_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Created folder: {json_folder}")

    # Generate timestamped filename with folder path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_table = table_name.replace(".", "_").replace(":", "_")
    safe_pipeline = pipeline.replace(".", "_").replace(":", "_")
    json_filename = str(json_folder / f"sync_{safe_pipeline}_{safe_table}_{timestamp}.json")
    
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
            cwd=PROJECT_ROOT,  # Run from project root where src/ exists
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
                    return validate_parcel_detail_sync(table_name, table_results, overall_status, json_data, json_filename, source_connection, target_connection)

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


def validate_parcel_detail_sync(table_name: str, table_results: dict, overall_status: str, json_data: dict, json_filename: str, source_connection: str, target_connection: str) -> tuple:
    """
    Special validation for unidw.dw_parcel_detail_tool_temp table.

    Validates that:
    1. The table status is 'success'
    2. The rows_processed matches the actual count in the MySQL source table
    3. The rows loaded to Redshift with matching inserted_at range equals MySQL count

    Note: The inserted_at column in Redshift is synced from MySQL, not the load timestamp.
    Validation queries Redshift using the min/max inserted_at values from the MySQL source table.

    Args:
        table_name: Name of the table being validated
        table_results: Per-table results from sync JSON
        overall_status: Overall sync status
        json_data: Complete JSON data
        json_filename: JSON output filename
        source_connection: MySQL source connection name from pipeline configuration
        target_connection: Redshift target connection name from pipeline configuration

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
            mysql_count = get_mysql_table_count(table_name, source_connection)
        except Exception as e:
            logger.error(f"❌ Failed to query MySQL table count: {e}")
            logger.error(f"❌ {table_name} sync validation failed:")
            logger.error(f"   Status: {table_status}")
            logger.error(f"   Rows processed: {rows_processed:,}")
            logger.error(f"   MySQL source count: Cannot verify (query failed)")
            logger.error(f"   ✗ Row counts do not match!")
            return False, json_data, json_filename

        # Get inserted_at range from MySQL source table for Redshift verification
        redshift_count = None
        try:
            # Get min/max inserted_at from MySQL source table
            min_inserted_at, max_inserted_at = get_mysql_inserted_at_range(table_name, source_connection)

            if min_inserted_at and max_inserted_at:
                # Target table in Redshift is 'dw_parcel_detail_tool' (without unidw. prefix)
                # Connection comes from pipeline config (e.g., 'redshift_settlement_dws')
                target_table = 'dw_parcel_detail_tool'

                redshift_count = get_redshift_table_count(
                    table_name=target_table,
                    target_connection=target_connection,
                    min_inserted_at=min_inserted_at,
                    max_inserted_at=max_inserted_at
                )
            else:
                logger.warning(f"⚠️  Cannot get inserted_at range from MySQL - skipping Redshift verification")
        except Exception as e:
            logger.error(f"❌ Failed to query Redshift table count: {e}")
            logger.warning(f"⚠️  Continuing with MySQL validation only")

        # Compare all counts
        validation_passed = True
        validation_details = []

        validation_details.append(f"   Status: {table_status}")
        validation_details.append(f"   Rows processed (MySQL→S3): {rows_processed:,}")
        validation_details.append(f"   MySQL source count: {mysql_count:,}")

        if redshift_count is not None:
            validation_details.append(f"   Redshift rows loaded (inserted_at range): {redshift_count:,}")

        # Check MySQL backup counts match
        if rows_processed != mysql_count:
            validation_details.append(f"   ✗ MySQL backup count mismatch!")
            validation_passed = False
        else:
            validation_details.append(f"   ✓ MySQL backup counts match!")

        # Check Redshift count matches if available
        if redshift_count is not None:
            if redshift_count != mysql_count:
                validation_details.append(f"   ✗ Redshift load count mismatch! (Expected: {mysql_count:,}, Found: {redshift_count:,})")

                # Automatic cleanup: Delete mismatched data from Redshift
                logger.warning(f"⚠️  Redshift count mismatch detected - initiating automatic cleanup")

                target_table = 'dw_parcel_detail_tool'
                cleanup_success = delete_redshift_records_by_inserted_at(
                    table_name=target_table,
                    target_connection=target_connection,
                    min_inserted_at=min_inserted_at,
                    max_inserted_at=max_inserted_at
                )

                if cleanup_success:
                    validation_details.append(f"   ✓ Automatic Redshift cleanup completed")
                else:
                    validation_details.append(f"   ✗ Automatic Redshift cleanup failed")

                validation_passed = False
            else:
                validation_details.append(f"   ✓ Redshift load counts match!")

        # Log results
        if validation_passed:
            logger.info(f"✅ {table_name} sync validation passed:")
            for detail in validation_details:
                logger.info(detail)
            return True, json_data, json_filename
        else:
            logger.error(f"❌ {table_name} sync validation failed:")
            for detail in validation_details:
                logger.error(detail)
            return False, json_data, json_filename

    except Exception as e:
        logger.error(f"❌ Error during {table_name} validation: {e}")
        return False, json_data, json_filename




def main():
    """Main execution function"""
    # Parse command line arguments
    args = parse_arguments()

    # Configuration - Fixed values
    DOWNLOAD_DIR = str(PROJECT_ROOT / "parcel_download_tool_etl")
    TABLE_NAME = "unidw.dw_parcel_detail_tool_temp"

    # Get pipeline from args (with default)
    PIPELINE = args.pipeline

    # Get source connection from pipeline configuration
    try:
        SOURCE_CONNECTION = get_source_connection_from_pipeline(PIPELINE)
        logger.info(f"📡 Source connection from pipeline: {SOURCE_CONNECTION}")
    except Exception as e:
        logger.error(f"❌ Failed to get source connection from pipeline '{PIPELINE}': {e}")
        sys.exit(1)

    # Get target connection from pipeline configuration
    try:
        TARGET_CONNECTION = get_target_connection_from_pipeline(PIPELINE)
        logger.info(f"📡 Target connection from pipeline: {TARGET_CONNECTION}")
    except Exception as e:
        logger.error(f"❌ Failed to get target connection from pipeline '{PIPELINE}': {e}")
        sys.exit(1)

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
        logger.info(f"  Start DateTime: {args.date if args.date else 'current time'}")
        logger.info(f"  Hours Offset: {args.hours}")
    logger.info("")

    download_success = True  # Default to True for sync-only mode

    # Phase 1: Execute download script (only if not sync-only)
    if not args.sync_only:
        logger.info("📥 Phase 1: Executing download script")
        download_success, download_stdout, download_stderr = execute_download_script(
            DOWNLOAD_DIR, args.date, args.hours
        )

        if not download_success:
            logger.error("❌ Download phase failed, aborting sync")
            sys.exit(1)
    else:
        logger.info("⏭️ Phase 1: Skipping download script (--sync-only mode)")

    # # Phase 2: Run sync command
    # phase_number = "2" if not args.sync_only else "1"
    # logger.info(f"🔄 Phase {phase_number}: Running sync command")
    # sync_success, sync_data, json_file = run_sync_command(
    #     table_name=TABLE_NAME,
    #     pipeline=PIPELINE,
    #     source_connection=SOURCE_CONNECTION,
    #     target_connection=TARGET_CONNECTION
    # )

    # # Report pipeline status
    # if download_success and sync_success:
    #     if args.sync_only:
    #         logger.info("🎉 Sync-only pipeline completed successfully!")
    #     else:
    #         logger.info("🎉 Full pipeline completed successfully!")
    #     logger.info(f"📄 Sync results: {json_file}")
    # else:
    #     if args.sync_only:
    #         logger.error("❌ Sync-only pipeline failed")
    #     else:
    #         logger.error("❌ Pipeline failed")
    #     if json_file:
    #         logger.info(f"📄 Sync results: {json_file}")

    # # Always run cleanup steps regardless of sync success/failure
    # logger.info("🧹 Starting cleanup steps...")
    # logger.info("=" * 50)

    # # Only delete MySQL records if sync was successful
    # delete_success = True  # Default to true if we skip deletion
    # if sync_success:
    #     logger.info(f"🗑️ Step 1: Deleting all records from {TABLE_NAME}")
    #     delete_success = delete_mysql_table_records(TABLE_NAME, SOURCE_CONNECTION)
    # else:
    #     logger.info("⏭️ Step 1: Skipping MySQL record deletion (sync failed)")

    # # Always run S3 cleanup (regardless of sync success)
    # logger.info(f"🧹 Step 2: Running S3 cleanup for {TABLE_NAME}")
    # s3clean_success = run_s3clean_command(TABLE_NAME, PIPELINE)

    # # Always run watermark reset (regardless of sync success)
    # logger.info(f"🔄 Step 3: Resetting watermark for {TABLE_NAME}")
    # watermark_success = run_watermark_reset_command(TABLE_NAME, PIPELINE)

    # # Final status
    # logger.info("=" * 50)
    # if sync_success and delete_success and s3clean_success and watermark_success:
    #     logger.info("🎉 Pipeline and all cleanup steps completed successfully!")
    #     sys.exit(0)
    # else:
    #     logger.error("❌ Critical cleanup steps failed")
    #     sys.exit(1)

    # Final status - Download phase only
    logger.info("=" * 50)
    if download_success:
        logger.info("🎉 Download phase completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Download phase failed")
        sys.exit(1)


if __name__ == "__main__":
    main()