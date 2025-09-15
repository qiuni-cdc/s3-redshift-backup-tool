"""
Priority 1 & 2: JSON Output and Airflow Task Integration
Minimal, bug-free implementation focusing only on essential features
"""

import json
import time
import uuid
import click
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

# Make boto3 optional for environments without AWS dependencies
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    # Create dummy ClientError for type hints
    class ClientError(Exception):
        pass

# Make logging optional for environments without project dependencies
try:
    from src.utils.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    # Use basic logging if project logging not available
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


class SyncExecutionTracker:
    """Simple, focused tracker for sync execution metadata"""
    
    def __init__(self):
        self.execution_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        self.start_time = datetime.now(timezone.utc)
        self.metadata = {
            "execution_id": self.execution_id,
            "start_time": self.start_time.isoformat(),
            "pipeline": None,
            "tables_requested": [],
            "table_results": {},
            "status": "running"
        }
    
    def set_pipeline_info(self, pipeline: str, tables: List[str]):
        """Set pipeline and tables information"""
        self.metadata.update({
            "pipeline": pipeline,
            "tables_requested": tables,
            "total_tables": len(tables)
        })
        logger.info(f"Tracking execution {self.execution_id} for pipeline {pipeline}")
    
    def record_table_success(self, table: str, rows_processed: int = 0, files_created: int = 0, duration: float = 0):
        """Record successful table processing"""
        self.metadata["table_results"][table] = {
            "status": "success",
            "rows_processed": rows_processed,
            "files_created": files_created,
            "duration_seconds": duration,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }
        logger.info(f"Table {table} completed successfully: {rows_processed} rows")
    
    def record_table_failure(self, table: str, error_message: str):
        """Record failed table processing"""
        self.metadata["table_results"][table] = {
            "status": "failed", 
            "error_message": str(error_message),
            "failed_at": datetime.now(timezone.utc).isoformat()
        }
        logger.error(f"Table {table} failed: {error_message}")
    
    def finalize(self) -> Tuple[str, Dict[str, Any]]:
        """Finalize execution and return status and metadata"""
        end_time = datetime.now(timezone.utc)
        duration = (end_time - self.start_time).total_seconds()
        
        # Calculate summary stats
        results = self.metadata["table_results"]
        success_count = len([r for r in results.values() if r["status"] == "success"])
        failure_count = len([r for r in results.values() if r["status"] == "failed"])
        total_rows = sum(r.get("rows_processed", 0) for r in results.values())
        total_files = sum(r.get("files_created", 0) for r in results.values())
        
        # Determine overall status
        if failure_count == 0:
            overall_status = "success"
        elif success_count > 0:
            overall_status = "partial_success"
        else:
            overall_status = "failed"
        
        # Update metadata
        self.metadata.update({
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "status": overall_status,
            "summary": {
                "success_count": success_count,
                "failure_count": failure_count,
                "total_rows_processed": total_rows,
                "total_files_created": total_files
            }
        })
        
        logger.info(f"Execution {self.execution_id} completed with status: {overall_status}")
        return overall_status, self.metadata
    
    def write_json_output(self, output_path: str) -> bool:
        """Write metadata to JSON file"""
        try:
            # Ensure directory exists
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write JSON with proper formatting
            with open(output_path, 'w') as f:
                json.dump(self.metadata, f, indent=2, default=str)
            
            logger.info(f"JSON metadata written to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write JSON output to {output_path}: {e}")
            return False


class S3CompletionMarker:
    """Simple S3 completion marker for Airflow S3KeySensor"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = None
        self._ensure_s3_client()
    
    def _ensure_s3_client(self):
        """Initialize S3 client if needed"""
        if self.s3_client is None:
            if not BOTO3_AVAILABLE:
                raise ImportError("boto3 is required for S3 completion markers. Install with: pip install boto3")
            try:
                self.s3_client = boto3.client('s3')
                logger.info("S3 client initialized for completion markers")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {e}")
                raise
    
    def create_table_marker(self, table_name: str, status: str, execution_id: str, 
                           prefix: str = "completion_markers") -> bool:
        """Create completion marker for a single table"""
        try:
            # Generate marker key
            date_str = datetime.now().strftime('%Y%m%d')
            clean_table = table_name.replace('.', '_').replace(':', '_')
            marker_key = f"{prefix}/{date_str}/sync_{clean_table}_{status.upper()}"
            
            # Create marker content
            marker_content = {
                "table": table_name,
                "status": status,
                "execution_id": execution_id,
                "completion_time": datetime.now(timezone.utc).isoformat(),
                "marker_created_by": "s3-redshift-sync-v1.2.0"
            }
            
            # Upload marker
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=marker_key,
                Body=json.dumps(marker_content, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Created completion marker: s3://{self.bucket_name}/{marker_key}")
            return True
            
        except ClientError as e:
            logger.error(f"S3 error creating marker for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating marker for {table_name}: {e}")
            return False
    
    def create_pipeline_marker(self, pipeline_name: str, tables: List[str], overall_status: str, 
                              execution_id: str, prefix: str = "completion_markers") -> bool:
        """Create completion marker for entire pipeline"""
        try:
            date_str = datetime.now().strftime('%Y%m%d')
            clean_pipeline = pipeline_name.replace('.', '_').replace(':', '_')
            marker_key = f"{prefix}/{date_str}/pipeline_{clean_pipeline}_{overall_status.upper()}"
            
            marker_content = {
                "pipeline": pipeline_name,
                "tables": tables,
                "status": overall_status,
                "execution_id": execution_id,
                "completion_time": datetime.now(timezone.utc).isoformat(),
                "tables_count": len(tables)
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=marker_key,
                Body=json.dumps(marker_content, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Created pipeline marker: s3://{self.bucket_name}/{marker_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create pipeline marker: {e}")
            return False


class AirflowExitCodes:
    """Enhanced exit codes for Airflow task integration"""
    SUCCESS = 0
    CONFIGURATION_ERROR = 1
    CONNECTION_ERROR = 2  
    PARTIAL_SUCCESS = 3
    DATA_ERROR = 4
    S3_ERROR = 5
    REDSHIFT_ERROR = 6


def determine_exit_code(overall_status: str, table_results: Dict[str, Any]) -> int:
    """Determine appropriate exit code based on execution results"""
    
    if overall_status == "success":
        return AirflowExitCodes.SUCCESS
    elif overall_status == "partial_success":
        return AirflowExitCodes.PARTIAL_SUCCESS
    else:
        # Analyze failure types to determine specific exit code
        error_messages = [
            result.get("error_message", "").lower() 
            for result in table_results.values() 
            if result.get("status") == "failed"
        ]
        
        if any("connection" in msg for msg in error_messages):
            return AirflowExitCodes.CONNECTION_ERROR
        elif any("s3" in msg for msg in error_messages):
            return AirflowExitCodes.S3_ERROR
        elif any("redshift" in msg or "copy" in msg for msg in error_messages):
            return AirflowExitCodes.REDSHIFT_ERROR
        elif any("config" in msg or "validation" in msg for msg in error_messages):
            return AirflowExitCodes.CONFIGURATION_ERROR
        else:
            return AirflowExitCodes.DATA_ERROR


def enhance_sync_with_airflow_integration(
    pipeline: str, 
    tables: List[str],
    sync_executor_func,  # Function that actually performs the sync
    json_output_path: Optional[str] = None,
    s3_completion_bucket: Optional[str] = None,
    completion_prefix: str = "completion_markers"
) -> int:
    """
    Enhance any sync operation with Airflow integration features
    
    Args:
        pipeline: Pipeline name
        tables: List of table names to sync
        sync_executor_func: Function that performs the actual sync (must return success_count, table_results)
        json_output_path: Path for JSON metadata output
        s3_completion_bucket: S3 bucket for completion markers
        completion_prefix: S3 prefix for completion markers
        
    Returns:
        Exit code suitable for Airflow
    """
    
    # Initialize tracking
    tracker = SyncExecutionTracker()
    tracker.set_pipeline_info(pipeline, tables)
    
    s3_marker = None
    if s3_completion_bucket:
        try:
            s3_marker = S3CompletionMarker(s3_completion_bucket)
        except Exception as e:
            logger.warning(f"S3 completion markers disabled due to error: {e}")
    
    try:
        # Execute the actual sync
        click.echo(f"🚀 Starting sync execution: {tracker.execution_id}")
        click.echo(f"📋 Pipeline: {pipeline}")
        click.echo(f"📊 Tables: {', '.join(tables)}")
        
        # Call the sync executor function
        success_count, table_results = sync_executor_func(tracker)
        
        # Record results in tracker
        for table_name, result in table_results.items():
            if result.get("success", False):
                tracker.record_table_success(
                    table_name,
                    rows_processed=result.get("rows_processed", 0),
                    files_created=result.get("files_created", 0),
                    duration=result.get("duration", 0)
                )
            else:
                tracker.record_table_failure(
                    table_name,
                    result.get("error_message", "Unknown error")
                )
        
        # Finalize execution
        overall_status, final_metadata = tracker.finalize()
        
        # Output JSON if requested
        if json_output_path:
            if tracker.write_json_output(json_output_path):
                click.echo(f"📄 JSON metadata: {json_output_path}")
            else:
                click.echo(f"⚠️  Warning: Could not write JSON metadata")
        
        # Create S3 completion markers if requested
        if s3_marker:
            click.echo(f"🏷️  Creating S3 completion markers...")
            
            # Create individual table markers
            for table_name in tables:
                table_status = "SUCCESS" if table_name in table_results and table_results[table_name].get("success") else "FAILED"
                s3_marker.create_table_marker(table_name, table_status, tracker.execution_id, completion_prefix)
            
            # Create pipeline marker
            pipeline_status = "SUCCESS" if overall_status == "success" else "FAILED"
            s3_marker.create_pipeline_marker(pipeline, tables, pipeline_status, tracker.execution_id, completion_prefix)
            
            click.echo(f"✅ Completion markers created in s3://{s3_completion_bucket}/{completion_prefix}")
        
        # Display summary
        summary = final_metadata["summary"]
        click.echo(f"\n📊 EXECUTION SUMMARY")
        click.echo(f"✅ Successful: {summary['success_count']}")
        click.echo(f"❌ Failed: {summary['failure_count']}")
        click.echo(f"📈 Total rows: {summary['total_rows_processed']:,}")
        click.echo(f"📄 Total files: {summary['total_files_created']}")
        click.echo(f"⏱️  Duration: {final_metadata['duration_seconds']:.1f}s")
        
        # Determine exit code
        exit_code = determine_exit_code(overall_status, final_metadata["table_results"])
        
        if exit_code != AirflowExitCodes.SUCCESS:
            click.echo(f"\n⚠️  Exiting with code {exit_code} for Airflow task handling")
        
        return exit_code
        
    except Exception as e:
        logger.error(f"Sync execution failed: {e}")
        
        # Record failure
        for table_name in tables:
            tracker.record_table_failure(table_name, str(e))
        
        overall_status, final_metadata = tracker.finalize()
        
        # Still try to output JSON on failure
        if json_output_path:
            tracker.write_json_output(json_output_path)
        
        click.echo(f"❌ Sync execution failed: {e}")
        return determine_exit_code("failed", final_metadata["table_results"])


def print_exit_codes_help():
    """Print help information about exit codes"""
    click.echo("\n📊 Airflow Integration Exit Codes:")
    click.echo("  0 - SUCCESS: All tables processed successfully")
    click.echo("  1 - CONFIGURATION_ERROR: Invalid pipeline/table configuration")
    click.echo("  2 - CONNECTION_ERROR: Database or network connection failed")
    click.echo("  3 - PARTIAL_SUCCESS: Some tables succeeded, others failed")
    click.echo("  4 - DATA_ERROR: Data validation or processing issues")
    click.echo("  5 - S3_ERROR: S3 upload or access issues")
    click.echo("  6 - REDSHIFT_ERROR: Redshift COPY or SQL execution issues")
    click.echo("")