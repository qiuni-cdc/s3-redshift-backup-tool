#!/usr/bin/env python3
"""
Settle Orders Download and Sync Automation Script

This script executes the complete settle_orders ETL pipeline:

1. Sync incremental data from settlement.settle_orders to Redshift settlement_ods.temp_settle_orders
2. Data validation: Verify incremental record count matches temp_settle_orders row count
3. Deduplicate temp_settle_orders to settlement_ods.latest_settle_orders (drop if exists)
4. Merge latest_settle_orders into settlement_dws.settle_orders using uni_sn as primary key
5. Data validation: Verify distinct uni_sn count in settlement_dws.settle_orders
6. Cleanup: Delete records from temp_settle_orders after successful merge
7. S3 cleanup

Error Handling and Recovery:
On any step failure, the script will automatically:
- Extract max(created_at) and max(id) from settlement_dws.settle_orders
- Reset watermark to initial state (clear all fields)
- Set watermark to max values (no adjustment needed)
- Clean temp_settle_orders table
- Clean S3 files

This ensures the pipeline can be safely retried after failure without data loss or duplication.

IMPORTANT: Using max values from DWS table with exclusive comparison (>):
- WHERE created_at > watermark_timestamp
- WHERE id > watermark_id
This ensures only new records (greater than existing max) are synced in the next run.

Usage:
  python settle_orders_download_and_sync.py                                                # Default pipeline
  python settle_orders_download_and_sync.py -p us_dw_settlement_2_settlement_ods_pipeline_direct  # Custom pipeline
  python settle_orders_download_and_sync.py --help                                         # Show help
"""

import subprocess
import json
import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Project root directory
SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
PROJECT_ROOT = SCRIPT_DIR.parent  # Go up one level from settle_orders_etl to project root

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
        logging.FileHandler(PROJECT_ROOT / 'settle_orders_etl' / 'settle_orders_etl.log')
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Settle Orders Download and Sync Automation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python settle_orders_download_and_sync.py                                                # Use default pipeline
  python settle_orders_download_and_sync.py -p us_dw_settlement_2_settlement_ods_pipeline_direct  # Custom pipeline
  python settle_orders_download_and_sync.py --help                                         # Show this help message
        """
    )

    parser.add_argument(
        '-p', '--pipeline',
        type=str,
        default='us_dw_settlement_2_settlement_ods_pipeline_direct',
        help='Pipeline name to use (default: us_dw_settlement_2_settlement_ods_pipeline_direct)'
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


def get_redshift_table_count(table_name: str, target_connection: str) -> int:
    """Get the row count from a Redshift table"""
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying Redshift table count for: {table_name}")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()
            query = f"SELECT COUNT(*) FROM {table_name}"
            logger.info(f"📊 Executing Redshift query: {query}")
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()

            logger.info(f"📈 Redshift table {table_name} has {count:,} rows")
            return count

    except Exception as e:
        logger.error(f"❌ Failed to get Redshift table count for {table_name}: {e}")
        raise


def get_redshift_distinct_count(table_name: str, column_name: str, target_connection: str, created_at_min: str = None) -> int:
    """
    Get the distinct count of a column from a Redshift table with optional filters

    Args:
        table_name: Name of the Redshift table
        column_name: Column to count distinct values
        target_connection: Redshift connection name
        created_at_min: Optional minimum created_at timestamp for filtering
    """
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying Redshift distinct count for: {table_name}.{column_name}")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()

            if created_at_min:
                query = f"""
                    SELECT COUNT(DISTINCT {column_name})
                    FROM {table_name}
                    WHERE created_at >= %s
                """
                logger.info(f"📊 Executing Redshift query: {query}")
                logger.info(f"📊 Parameters: created_at_min={created_at_min}")
                cursor.execute(query, (created_at_min,))
            else:
                query = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}"
                logger.info(f"📊 Executing Redshift query: {query}")
                cursor.execute(query)

            count = cursor.fetchone()[0]
            cursor.close()

            logger.info(f"📈 Redshift table {table_name} has {count:,} distinct {column_name} values")
            return count

    except Exception as e:
        logger.error(f"❌ Failed to get Redshift distinct count for {table_name}.{column_name}: {e}")
        raise


def get_redshift_min_created_at(table_name: str, target_connection: str) -> str:
    """Get the minimum created_at value from a Redshift table (for Step 5 validation)"""
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying Redshift min(created_at) for: {table_name}")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()
            query = f"SELECT MIN(created_at) FROM {table_name}"
            logger.info(f"📊 Executing Redshift query: {query}")
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()

            if result and result[0]:
                min_created_at = result[0].isoformat() if hasattr(result[0], 'isoformat') else str(result[0])
                logger.info(f"📈 Minimum created_at in {table_name}: {min_created_at}")
                return min_created_at
            else:
                logger.warning(f"⚠️  No data found in {table_name}")
                return None

    except Exception as e:
        logger.error(f"❌ Failed to get min created_at for {table_name}: {e}")
        raise


def get_redshift_max_created_at_and_id(table_name: str, target_connection: str) -> tuple:
    """Get the maximum created_at and id values from a Redshift table (for cleanup)"""
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔍 Querying Redshift max(created_at) and max(id) for: {table_name}")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()
            query = f"SELECT MAX(created_at), MAX(id) FROM {table_name}"
            logger.info(f"📊 Executing Redshift query: {query}")
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()

            if result and result[0] and result[1]:
                max_created_at = result[0].isoformat() if hasattr(result[0], 'isoformat') else str(result[0])
                max_id = result[1]
                logger.info(f"📈 Maximum created_at in {table_name}: {max_created_at}")
                logger.info(f"📈 Maximum id in {table_name}: {max_id}")
                return max_created_at, max_id
            else:
                logger.warning(f"⚠️  No data found in {table_name}")
                return None, None

    except Exception as e:
        logger.error(f"❌ Failed to get max created_at and id for {table_name}: {e}")
        raise


def deduplicate_to_latest(target_connection: str) -> bool:
    """
    Deduplicate temp_settle_orders to latest_settle_orders

    Drops and recreates settlement_ods.latest_settle_orders with only the latest
    record for each unique uni_sn based on created_at timestamp.
    """
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔄 Deduplicating temp_settle_orders to latest_settle_orders")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()

            # Drop latest_settle_orders if exists
            drop_query = "DROP TABLE IF EXISTS settlement_ods.latest_settle_orders"
            logger.info(f"🗑️ Executing: {drop_query}")
            cursor.execute(drop_query)

            # Create latest_settle_orders with deduplicated data
            # Note: We exclude the 'rn' column to match the schema of settlement_dws.settle_orders
            create_query = """
                CREATE TABLE settlement_ods.latest_settle_orders AS
                SELECT t.*
                FROM settlement_ods.temp_settle_orders t
                INNER JOIN (
                    SELECT id, uni_sn,
                           ROW_NUMBER() OVER (PARTITION BY uni_sn ORDER BY created_at DESC, id DESC) as rn
                    FROM settlement_ods.temp_settle_orders
                ) ranked ON t.id = ranked.id
                WHERE ranked.rn = 1
            """
            logger.info(f"✨ Creating deduplicated latest_settle_orders table")
            cursor.execute(create_query)

            conn.commit()
            cursor.close()

            logger.info(f"✅ Successfully deduplicated to latest_settle_orders")
            return True

    except Exception as e:
        logger.error(f"❌ Failed to deduplicate to latest_settle_orders: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def merge_into_settle_orders(target_connection: str) -> bool:
    """
    Merge latest_settle_orders into settlement_dws.settle_orders

    Uses DELETE + INSERT pattern (Redshift best practice):
    1. DELETE matching records (WHEN MATCHED)
    2. INSERT all records (WHEN MATCHED + WHEN NOT MATCHED)

    Equivalent to:
    MERGE INTO settlement_dws.settle_orders t
    USING settlement_ods.latest_settle_orders s
    ON t.uni_sn = s.uni_sn
    WHEN MATCHED THEN UPDATE SET ... (all fields)
    WHEN NOT MATCHED THEN INSERT ...
    """
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔄 Merging latest_settle_orders into settlement_dws.settle_orders")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()

            logger.info(f"📋 Starting MERGE operation (DELETE + INSERT pattern)")

            # Step 1: DELETE matching records (WHEN MATCHED)
            delete_query = """
                DELETE FROM settlement_dws.settle_orders
                USING settlement_ods.latest_settle_orders
                WHERE settlement_dws.settle_orders.uni_sn = settlement_ods.latest_settle_orders.uni_sn
            """
            logger.info(f"🔄 WHEN MATCHED: Deleting matching records")
            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            logger.info(f"   Deleted {deleted_count:,} matching records (to be updated)")

            # Step 2: INSERT all records (WHEN MATCHED + WHEN NOT MATCHED)
            insert_query = """
                INSERT INTO settlement_dws.settle_orders
                SELECT * FROM settlement_ods.latest_settle_orders
            """
            logger.info(f"🔄 WHEN MATCHED + NOT MATCHED: Inserting all records")
            cursor.execute(insert_query)
            inserted_count = cursor.rowcount
            logger.info(f"   Inserted {inserted_count:,} total records")

            conn.commit()
            cursor.close()

            logger.info(f"✅ Successfully merged latest_settle_orders into settle_orders")
            logger.info(f"   Records updated: {deleted_count:,}")
            logger.info(f"   Records inserted (new): {inserted_count - deleted_count:,}")
            logger.info(f"   Total records affected: {inserted_count:,}")
            return True

    except Exception as e:
        logger.error(f"❌ Failed to merge into settle_orders: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def rollback_merge_operation(target_connection: str) -> bool:
    """
    Rollback merge operation by deleting records that were inserted from latest_settle_orders

    This is called when merge validation fails to clean up the bad merge.
    """
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🔄 Rolling back merge operation")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()

            # Delete records that match latest_settle_orders
            rollback_query = """
                DELETE FROM settlement_dws.settle_orders
                USING settlement_ods.latest_settle_orders
                WHERE settlement_dws.settle_orders.uni_sn = settlement_ods.latest_settle_orders.uni_sn
            """
            logger.info(f"🔙 Deleting merged records from settlement_dws.settle_orders")
            cursor.execute(rollback_query)
            deleted_count = cursor.rowcount

            conn.commit()
            cursor.close()

            logger.info(f"✅ Successfully rolled back merge operation")
            logger.info(f"   Deleted {deleted_count:,} records from settlement_dws.settle_orders")
            return True

    except Exception as e:
        logger.error(f"❌ Failed to rollback merge operation: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def delete_redshift_table_records(table_name: str, target_connection: str) -> bool:
    """Delete all records from a Redshift table"""
    try:
        config_path = PROJECT_ROOT / "config" / "connections.yml"
        conn_registry = ConnectionRegistry(config_path=str(config_path))

        logger.info(f"🗑️ Deleting all records from Redshift table: {table_name}")
        logger.info(f"📡 Using connection: {target_connection}")

        with conn_registry.get_redshift_connection(target_connection) as conn:
            cursor = conn.cursor()

            # Get count before deletion
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(count_query)
            record_count = cursor.fetchone()[0]

            if record_count == 0:
                logger.info(f"📊 Table {table_name} is already empty (0 records)")
                cursor.close()
                return True

            # Delete all records
            delete_query = f"DELETE FROM {table_name}"
            logger.info(f"🗑️ Executing Redshift delete: {delete_query}")
            logger.info(f"📊 About to delete {record_count:,} records...")

            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()

            logger.info(f"✅ Successfully deleted {deleted_count:,} records from {table_name}")
            return True

    except Exception as e:
        logger.error(f"❌ Failed to delete records from Redshift table {table_name}: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def reset_watermark_completely(table_name: str, pipeline: str) -> bool:
    """Reset watermark to initial state (clear all fields)"""
    try:
        cmd = [
            "python", "-m", "src.cli.main", "watermark", "reset",
            "-t", table_name,
            "-p", pipeline,
            "--yes"
        ]

        logger.info(f"🔄 Resetting watermark to initial state")
        logger.info(f"📋 Command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode == 0:
            logger.info(f"✅ Watermark reset to initial state")
            return True
        else:
            logger.error(f"❌ Watermark reset failed with exit code {result.returncode}")
            if result.stderr:
                logger.error(f"   Error output: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("❌ Watermark reset command timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Error resetting watermark: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def set_watermark_values(table_name: str, timestamp: str, id_value: int, pipeline: str) -> bool:
    """Set watermark to specific timestamp and id values"""
    try:
        cmd = [
            "python", "-m", "src.cli.main", "watermark", "set",
            "-t", table_name,
            "-p", pipeline,
            "--timestamp", timestamp,
            "--id", str(id_value),
            "--yes"
        ]

        logger.info(f"🔄 Setting watermark values: timestamp={timestamp}, id={id_value}")
        logger.info(f"📋 Command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode == 0:
            logger.info(f"✅ Watermark set to timestamp={timestamp}, id={id_value}")
            return True
        else:
            logger.error(f"❌ Watermark set failed with exit code {result.returncode}")
            if result.stderr:
                logger.error(f"   Error output: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("❌ Watermark set command timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Error setting watermark: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def cleanup_on_failure(table_name: str, pipeline: str, target_connection: str) -> None:
    """
    Comprehensive cleanup on pipeline failure:
    1. Extract max(created_at) and max(id) from settlement_dws.settle_orders
    2. Reset watermark to initial state (clear all fields)
    3. Set watermark to max values (no adjustment needed)
    4. Clean temp_settle_orders table
    5. Clean S3 files

    This ensures the pipeline can safely retry from the correct position with a clean watermark state.

    IMPORTANT: Since incremental sync uses exclusive comparison (> not >=), we set:
    - timestamp = max(created_at) from DWS table
    - id = max(id) from DWS table
    This ensures the next sync will only include records greater than existing max values.
    """
    logger.error("=" * 60)
    logger.error("🚨 PIPELINE FAILED - Starting cleanup procedure")
    logger.error("=" * 60)

    cleanup_steps_completed = []
    cleanup_steps_failed = []

    try:
        # Step 1: Get max(created_at) and max(id) from settlement_dws.settle_orders
        logger.info("📋 Step 1/5: Extracting max(created_at) and max(id) from settlement_dws.settle_orders")
        try:
            max_created_at, max_id = get_redshift_max_created_at_and_id(
                'settlement_dws.settle_orders',
                target_connection
            )

            if max_created_at and max_id:
                logger.info(f"✅ Found max(created_at): {max_created_at}")
                logger.info(f"✅ Found max(id): {max_id}")
                cleanup_steps_completed.append("Extract max(created_at) and max(id)")

                # No adjustment needed - use max values directly
                # Since incremental sync uses WHERE timestamp > X and id > Y (exclusive),
                # setting watermark to max ensures next sync only picks up new records

                # Parse timestamp to ensure correct format
                if isinstance(max_created_at, str):
                    if 'T' in max_created_at:
                        # ISO format: 2025-01-06T10:30:00
                        dt = datetime.fromisoformat(max_created_at.replace('Z', '+00:00'))
                    else:
                        # MySQL format: 2025-01-06 10:30:00
                        dt = datetime.strptime(max_created_at, '%Y-%m-%d %H:%M:%S')
                else:
                    dt = max_created_at

                watermark_timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                watermark_id = max_id

                logger.info(f"📐 Using max values directly (no adjustment needed):")
                logger.info(f"   Watermark: timestamp={watermark_timestamp_str}, id={watermark_id}")
                logger.info(f"   Reason: Next sync uses WHERE timestamp > X AND id > Y (exclusive)")

                # Step 2: Reset watermark to initial state
                logger.info("📋 Step 2/5: Resetting watermark to initial state")
                reset_success = reset_watermark_completely(
                    table_name=table_name,
                    pipeline=pipeline
                )

                if reset_success:
                    logger.info(f"✅ Watermark reset to initial state")
                    cleanup_steps_completed.append("Reset watermark to initial state")
                else:
                    logger.error("❌ Failed to reset watermark")
                    cleanup_steps_failed.append("Reset watermark to initial state")
                    # Continue to try setting values anyway

                # Step 3: Set watermark to max values (no adjustment)
                logger.info("📋 Step 3/5: Setting watermark to max values")
                set_success = set_watermark_values(
                    table_name=table_name,
                    timestamp=watermark_timestamp_str,
                    id_value=watermark_id,
                    pipeline=pipeline
                )

                if set_success:
                    logger.info(f"✅ Watermark set to timestamp={watermark_timestamp_str}, id={watermark_id}")
                    cleanup_steps_completed.append("Set watermark values")
                else:
                    logger.error("❌ Failed to set watermark values")
                    cleanup_steps_failed.append("Set watermark values")

            else:
                logger.warning("⚠️  settlement_dws.settle_orders is empty, skipping watermark reset")
                cleanup_steps_completed.append("Extract max(created_at) and max(id) - table empty")
                cleanup_steps_completed.append("Reset watermark - skipped (no data)")
                cleanup_steps_completed.append("Set watermark - skipped (no data)")

        except Exception as e:
            logger.error(f"❌ Failed to extract max(created_at) and max(id): {e}")
            cleanup_steps_failed.append("Extract max(created_at) and max(id)")

        # Step 4: Clean temp_settle_orders table
        logger.info("📋 Step 4/5: Cleaning temp_settle_orders table")
        try:
            temp_cleanup_success = delete_redshift_table_records(
                'settlement_ods.temp_settle_orders',
                target_connection
            )

            if temp_cleanup_success:
                logger.info("✅ temp_settle_orders cleaned")
                cleanup_steps_completed.append("Clean temp table")
            else:
                logger.error("❌ Failed to clean temp_settle_orders")
                cleanup_steps_failed.append("Clean temp table")
        except Exception as e:
            logger.error(f"❌ Failed to clean temp_settle_orders: {e}")
            cleanup_steps_failed.append("Clean temp table")

        # Step 5: Clean S3 files
        logger.info("📋 Step 5/5: Cleaning S3 files")
        try:
            s3_cleanup_success = run_s3clean_command(table_name, pipeline)

            if s3_cleanup_success:
                logger.info("✅ S3 files cleaned")
                cleanup_steps_completed.append("Clean S3 files")
            else:
                logger.error("❌ Failed to clean S3 files")
                cleanup_steps_failed.append("Clean S3 files")
        except Exception as e:
            logger.error(f"❌ Failed to clean S3 files: {e}")
            cleanup_steps_failed.append("Clean S3 files")

    except Exception as e:
        logger.error(f"❌ Cleanup procedure failed with unexpected error: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")

    # Report cleanup results
    logger.error("=" * 60)
    logger.error("🧹 CLEANUP SUMMARY")
    logger.error("=" * 60)

    if cleanup_steps_completed:
        logger.info("✅ Completed cleanup steps:")
        for step in cleanup_steps_completed:
            logger.info(f"   - {step}")

    if cleanup_steps_failed:
        logger.error("❌ Failed cleanup steps:")
        for step in cleanup_steps_failed:
            logger.error(f"   - {step}")
        logger.error("⚠️  Manual intervention may be required")
    else:
        logger.info("✅ All cleanup steps completed successfully")
        logger.info("✅ Pipeline is ready for retry")

    logger.error("=" * 60)


def run_s3clean_command(table_name: str, pipeline: str) -> bool:
    """Run s3clean command to clean up S3 files"""
    try:
        cmd = [
            "python", "-m", "src.cli.main", "s3clean", "clean",
            "-t", table_name,
            "-p", pipeline,
            "--force"
        ]

        logger.info(f"🧹 Running S3 cleanup command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=False,
            text=True,
            timeout=1800
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


def run_sync_command(table_name, pipeline):
    """
    Run the sync command with JSON output

    Returns:
        tuple: (success: bool, json_data: dict, json_file_path: str)
    """
    logger.info(f"🔄 Starting sync for table: {table_name}")

    # Create JSON output folder if it doesn't exist
    json_folder = PROJECT_ROOT / "settle_orders_etl" / "settle_orders_sync_json"
    if not json_folder.exists():
        json_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Created folder: {json_folder}")

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_table = table_name.replace(".", "_").replace(":", "_")
    safe_pipeline = pipeline.replace(".", "_").replace(":", "_")
    json_filename = str(json_folder / f"sync_{safe_pipeline}_{safe_table}_{timestamp}.json")

    # Build sync command
    cmd = [
        "python", "-m", "src.cli.main", "sync", "pipeline",
        "-p", pipeline,
        "-t", table_name,
        "--json-output", json_filename
    ]

    logger.info(f"Executing command: {' '.join(cmd)}")

    try:
        logger.info("🔄 Starting sync command execution...")
        logger.info("📺 Real-time sync logs will appear below:")
        logger.info("-" * 60)

        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=False,
            text=True,
            timeout=7200  # 2 hour timeout
        )

        logger.info("-" * 60)
        logger.info("🏁 Sync command execution completed")

        if result.returncode == 0:
            try:
                if os.path.exists(json_filename):
                    with open(json_filename, 'r') as f:
                        json_data = json.load(f)
                    logger.info(f"📄 JSON results loaded from: {json_filename}")
                else:
                    logger.error(f"❌ JSON output file not found: {json_filename}")
                    return False, {}, ""

                overall_status = json_data.get('status', 'unknown')

                # Log per-table results
                table_results = json_data.get('table_results', {})
                if table_results:
                    logger.info(f"📋 Per-Table Results:")
                    for table_name, table_data in table_results.items():
                        table_status = table_data.get('status', 'unknown')

                        if table_status == 'success':
                            rows_processed = table_data.get('rows_processed', 0)
                            logger.info(f"   ✅ {table_name}: {table_status}")
                            logger.info(f"      Rows: {rows_processed:,}")
                        else:
                            error_message = table_data.get('error_message', 'Unknown error')
                            logger.error(f"   ❌ {table_name}: {table_status}")
                            logger.error(f"      Error: {error_message}")

                if overall_status == 'success':
                    logger.info("✅ Sync completed successfully")
                    return True, json_data, json_filename
                else:
                    logger.error("❌ Sync failed")
                    return False, json_data, json_filename

            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse JSON output: {e}")
                return False, {}, ""
        else:
            logger.error(f"❌ Sync command failed with exit code {result.returncode}")
            return False, {}, ""

    except subprocess.TimeoutExpired:
        logger.error("❌ Sync command timed out after 2 hours")
        return False, {}, ""
    except Exception as e:
        logger.error(f"❌ Error executing sync command: {e}")
        return False, {}, ""


def main():
    """Main execution function"""
    args = parse_arguments()

    # Configuration
    TABLE_NAME = "settlement.settle_orders"
    PIPELINE = args.pipeline

    # Get source and target connections from pipeline
    try:
        SOURCE_CONNECTION = get_source_connection_from_pipeline(PIPELINE)
        TARGET_CONNECTION = get_target_connection_from_pipeline(PIPELINE)
        logger.info(f"📡 Source connection: {SOURCE_CONNECTION}")
        logger.info(f"📡 Target connection: {TARGET_CONNECTION}")
    except Exception as e:
        logger.error(f"❌ Failed to get connections from pipeline '{PIPELINE}': {e}")
        sys.exit(1)

    logger.info("🚀 Starting Settle Orders ETL Pipeline")
    logger.info("=" * 50)
    logger.info(f"Configuration:")
    logger.info(f"  Table: {TABLE_NAME}")
    logger.info(f"  Pipeline: {PIPELINE}")
    logger.info("")

    try:
        # Step 1: Sync incremental data to temp_settle_orders
        logger.info("📥 Step 1: Syncing incremental data to settlement_ods.temp_settle_orders")
        sync_success, sync_data, json_file = run_sync_command(
            table_name=TABLE_NAME,
            pipeline=PIPELINE
        )

        if not sync_success:
            logger.error("❌ Sync failed, aborting pipeline")
            cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
            sys.exit(1)

        # Step 2: Data validation - verify sync row count
        logger.info("🔍 Step 2: Data validation - verifying row counts")
        try:
            table_results = sync_data.get('table_results', {})
            if TABLE_NAME in table_results:
                rows_processed = table_results[TABLE_NAME].get('rows_processed', 0)
                temp_table_count = get_redshift_table_count('settlement_ods.temp_settle_orders', TARGET_CONNECTION)

                logger.info(f"📊 Rows processed: {rows_processed:,}")
                logger.info(f"📊 Rows in temp_settle_orders: {temp_table_count:,}")

                if rows_processed != temp_table_count:
                    logger.error(f"❌ Row count mismatch! Expected {rows_processed:,}, found {temp_table_count:,}")
                    cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
                    sys.exit(1)

                logger.info("✅ Row count validation passed")
        except Exception as e:
            logger.error(f"❌ Data validation failed: {e}")
            cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
            sys.exit(1)

        # Step 3: Deduplicate to latest_settle_orders
        logger.info("🔄 Step 3: Deduplicating to settlement_ods.latest_settle_orders")
        dedup_success = deduplicate_to_latest(TARGET_CONNECTION)

        if not dedup_success:
            logger.error("❌ Deduplication failed, aborting pipeline")
            cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
            sys.exit(1)

        # Step 4: Merge into settlement_dws.settle_orders
        logger.info("🔄 Step 4: Merging into settlement_dws.settle_orders")
        merge_success = merge_into_settle_orders(TARGET_CONNECTION)

        if not merge_success:
            logger.error("❌ Merge failed, aborting pipeline")
            cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
            sys.exit(1)

        # Step 5: Data validation - verify merge results
        logger.info("🔍 Step 5: Data validation - verifying merge results")
        try:
            latest_count = get_redshift_table_count('settlement_ods.latest_settle_orders', TARGET_CONNECTION)
            min_created_at = get_redshift_min_created_at('settlement_ods.latest_settle_orders', TARGET_CONNECTION)

            dws_distinct_count = get_redshift_distinct_count(
                'settlement_dws.settle_orders',
                'uni_sn',
                TARGET_CONNECTION,
                created_at_min=min_created_at
            )

            logger.info(f"📊 Rows in latest_settle_orders: {latest_count:,}")
            logger.info(f"📊 Distinct uni_sn in settle_orders (after merge): {dws_distinct_count:,}")

            if latest_count != dws_distinct_count:
                logger.error(f"❌ Merge validation failed! Expected {latest_count:,}, found {dws_distinct_count:,}")
                logger.warning("⚠️  Rolling back merge operation...")
                rollback_success = rollback_merge_operation(TARGET_CONNECTION)
                if rollback_success:
                    logger.info("✅ Merge rollback completed successfully")
                else:
                    logger.error("❌ Merge rollback failed - manual intervention required")
                cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
                sys.exit(1)

            logger.info("✅ Merge validation passed")
        except Exception as e:
            logger.error(f"❌ Merge validation failed: {e}")
            logger.warning("⚠️  Rolling back merge operation...")
            rollback_success = rollback_merge_operation(TARGET_CONNECTION)
            if rollback_success:
                logger.info("✅ Merge rollback completed successfully")
            else:
                logger.error("❌ Merge rollback failed - manual intervention required")
            cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
            sys.exit(1)

        # Step 6: Cleanup - delete temp_settle_orders
        logger.info("🧹 Step 6: Cleaning up temp_settle_orders")
        cleanup_success = delete_redshift_table_records('settlement_ods.temp_settle_orders', TARGET_CONNECTION)

        if not cleanup_success:
            logger.warning("⚠️  Failed to cleanup temp_settle_orders")

        # Step 7: S3 cleanup
        logger.info("🧹 Step 7: Running S3 cleanup")
        s3clean_success = run_s3clean_command(TABLE_NAME, PIPELINE)

        # Final status
        logger.info("=" * 50)
        if sync_success and dedup_success and merge_success and cleanup_success and s3clean_success:
            logger.info("🎉 Settle Orders ETL Pipeline completed successfully!")
            logger.info(f"📄 Sync results: {json_file}")
            sys.exit(0)
        else:
            logger.error("❌ Pipeline completed with warnings")
            logger.info(f"📄 Sync results: {json_file}")
            sys.exit(1)

    except Exception as e:
        # Catch any unexpected exceptions
        logger.error("=" * 60)
        logger.error(f"❌ UNEXPECTED ERROR: {e}")
        logger.error("=" * 60)
        import traceback
        logger.error(f"Stack trace:\n{traceback.format_exc()}")

        # Attempt cleanup on unexpected failure
        try:
            cleanup_on_failure(TABLE_NAME, PIPELINE, TARGET_CONNECTION)
        except Exception as cleanup_error:
            logger.error(f"❌ Cleanup also failed: {cleanup_error}")

        sys.exit(1)


if __name__ == "__main__":
    main()
