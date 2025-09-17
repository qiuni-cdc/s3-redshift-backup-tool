"""
Airflow Integration Enhancements for Sync Tool
Critical enhancements for better Airflow/dbt integration

APPROACH COMPARISON:
==================

CLI Enhancement Approach (This Implementation):
- ✅ Immediate deployment (1 day)
- ✅ No changes to core sync logic
- ✅ Tool independence (works with/without Airflow)
- ✅ Simple testing and debugging
- ❌ Process overhead (subprocess calls)
- ❌ Limited native Airflow integration

Native Operator Approach (Future Phase):
- ✅ Better performance (no subprocess)
- ✅ Rich Airflow integration (XComs, callbacks)
- ✅ Resource efficiency (shared connections)
- ❌ Complex implementation (2-3 weeks)
- ❌ Tight coupling with Airflow
- ❌ More complex testing requirements

RECOMMENDATION: Start with CLI Enhancement, evolve to Native Operators
"""

import click
import json
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
import uuid
import boto3
from botocore.exceptions import ClientError

from src.utils.logging import get_logger

logger = get_logger(__name__)

# Enhanced exit codes for Airflow
class ExitCodes:
    SUCCESS = 0
    CONFIGURATION_ERROR = 1
    CONNECTION_ERROR = 2
    PARTIAL_SUCCESS = 3
    DATA_VALIDATION_ERROR = 4
    S3_ERROR = 5
    REDSHIFT_ERROR = 6

class AirflowIntegrationMixin:
    """Mixin to add Airflow integration capabilities to sync commands"""
    
    def __init__(self):
        self.execution_metadata = {}
        self.table_results = {}
        self.s3_client = None
        self.internal_state = {
            "initialized": False,
            "metadata_finalized": False,
            "json_output_written": False,
            "completion_markers_created": False,
            "completion_markers_requested": False,
            "execution_errors": [],
            "validation_errors": []
        }
        
    def initialize_execution_metadata(self, pipeline: str, strategy: str, tables: List[str]):
        """Initialize execution metadata tracking"""
        self.execution_metadata = {
            "execution_id": f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "pipeline": pipeline,
            "strategy": strategy,
            "tables_requested": tables,
            "version": "1.2.0-airflow-enhanced"
        }
        self.internal_state["initialized"] = True
        logger.info(f"Execution metadata initialized: {self.execution_metadata['execution_id']}")
        
    def record_table_result(self, table: str, status: str, **kwargs):
        """Record results for individual table processing"""
        self.table_results[table] = {
            "status": status,
            "start_time": kwargs.get('start_time'),
            "end_time": kwargs.get('end_time'),
            "duration_seconds": kwargs.get('duration_seconds', 0),
            "rows_extracted": kwargs.get('rows_extracted', 0),
            "rows_loaded": kwargs.get('rows_loaded', 0),
            "s3_files_created": kwargs.get('s3_files_created', 0),
            "watermark_updated": kwargs.get('watermark_updated', False),
            "error_message": kwargs.get('error_message'),
            "s3_files": kwargs.get('s3_files', [])
        }
        
    def finalize_execution_metadata(self, overall_status: str, error: Optional[str] = None):
        """Finalize execution metadata"""
        self.execution_metadata.update({
            "end_time": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": (datetime.now(timezone.utc) - datetime.fromisoformat(self.execution_metadata["start_time"].replace('Z', '+00:00'))).total_seconds(),
            "status": overall_status,
            "error": error,
            "tables_processed": len([t for t in self.table_results.values() if t["status"] == "success"]),
            "tables_failed": len([t for t in self.table_results.values() if t["status"] == "failed"]),
            "total_rows_processed": sum(t.get("rows_extracted", 0) for t in self.table_results.values()),
            "total_files_created": sum(t.get("s3_files_created", 0) for t in self.table_results.values())
        })
        self.internal_state["metadata_finalized"] = True
        logger.info(f"Execution metadata finalized with status: {overall_status}")

    def output_json_metadata(self, output_file: str):
        """Output execution metadata as JSON file"""
        try:
            metadata = {
                "execution_metadata": self.execution_metadata,
                "table_results": self.table_results,
                "summary": {
                    "success_count": len([t for t in self.table_results.values() if t["status"] == "success"]),
                    "failure_count": len([t for t in self.table_results.values() if t["status"] == "failed"]),
                    "total_rows": self.execution_metadata.get("total_rows_processed", 0),
                    "total_files": self.execution_metadata.get("total_files_created", 0)
                }
            }
            
            # Ensure output directory exists
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
                
            self.internal_state["json_output_written"] = True
            logger.info(f"Execution metadata written to: {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to write JSON metadata: {e}")
            click.echo(f"⚠️  Warning: Could not write JSON metadata to {output_file}: {e}")

    def create_s3_completion_marker(self, bucket: str, pipeline: str, tables: List[str], 
                                   status: str = "SUCCESS", prefix: str = "completion_markers"):
        """Create S3 completion marker for Airflow S3KeySensor"""
        self.internal_state["completion_markers_requested"] = True
        try:
            if not self.s3_client:
                self.s3_client = boto3.client('s3')
            
            # Create marker for overall execution
            date_str = datetime.now().strftime('%Y%m%d')
            execution_id = self.execution_metadata.get("execution_id", "unknown")
            
            # Individual table markers
            for table in tables:
                table_status = self.table_results.get(table, {}).get("status", "UNKNOWN").upper()
                if table_status == "SUCCESS":
                    table_status = "SUCCESS"
                else:
                    table_status = "FAILED"
                
                # Clean table name for S3 key
                clean_table = table.replace('.', '_').replace(':', '_')
                marker_key = f"{prefix}/{date_str}/sync_{clean_table}_{table_status}"
                
                marker_content = {
                    "completion_time": datetime.now(timezone.utc).isoformat(),
                    "execution_id": execution_id,
                    "pipeline": pipeline,
                    "table": table,
                    "status": table_status,
                    "files_created": self.table_results.get(table, {}).get("s3_files_created", 0),
                    "rows_processed": self.table_results.get(table, {}).get("rows_extracted", 0)
                }
                
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=marker_key,
                    Body=json.dumps(marker_content, indent=2),
                    ContentType='application/json'
                )
                
                logger.info(f"Created S3 completion marker: s3://{bucket}/{marker_key}")
            
            # Overall pipeline marker
            overall_marker_key = f"{prefix}/{date_str}/pipeline_{pipeline.replace('.', '_')}_{status}"
            overall_content = {
                "completion_time": datetime.now(timezone.utc).isoformat(),
                "execution_id": execution_id,
                "pipeline": pipeline,
                "tables": tables,
                "status": status,
                "summary": {
                    "tables_processed": len(tables),
                    "total_files": self.execution_metadata.get("total_files_created", 0),
                    "total_rows": self.execution_metadata.get("total_rows_processed", 0)
                }
            }
            
            self.s3_client.put_object(
                Bucket=bucket,
                Key=overall_marker_key,
                Body=json.dumps(overall_content, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Created pipeline completion marker: s3://{bucket}/{overall_marker_key}")
            self.internal_state["completion_markers_created"] = True
            
        except ClientError as e:
            logger.error(f"Failed to create S3 completion marker: {e}")
            click.echo(f"⚠️  Warning: Could not create S3 completion marker: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating completion marker: {e}")
            click.echo(f"⚠️  Warning: Unexpected error creating completion marker: {e}")

    def determine_exit_code(self) -> int:
        """Determine appropriate exit code based on execution results"""
        if not self.table_results:
            return ExitCodes.CONFIGURATION_ERROR
        
        success_count = len([t for t in self.table_results.values() if t["status"] == "success"])
        failure_count = len([t for t in self.table_results.values() if t["status"] == "failed"])
        
        if failure_count == 0:
            return ExitCodes.SUCCESS
        elif success_count > 0:
            return ExitCodes.PARTIAL_SUCCESS
        else:
            # Analyze failure types to determine specific exit code
            error_types = [t.get("error_message", "") for t in self.table_results.values() if t["status"] == "failed"]
            
            if any("connection" in err.lower() for err in error_types):
                return ExitCodes.CONNECTION_ERROR
            elif any("s3" in err.lower() for err in error_types):
                return ExitCodes.S3_ERROR
            elif any("redshift" in err.lower() or "copy" in err.lower() for err in error_types):
                return ExitCodes.REDSHIFT_ERROR
            elif any("validation" in err.lower() or "schema" in err.lower() for err in error_types):
                return ExitCodes.DATA_VALIDATION_ERROR
            else:
                return ExitCodes.CONFIGURATION_ERROR

    def validate_execution_state(self) -> Tuple[bool, List[str]]:
        """Comprehensive state validation for user debugging"""
        errors = []
        
        # Check initialization
        if not self.internal_state["initialized"]:
            errors.append("❌ Execution metadata not initialized")
        
        # Validate metadata completeness
        required_fields = ["execution_id", "start_time", "pipeline", "tables_requested"]
        missing = [f for f in required_fields if f not in self.execution_metadata]
        if missing:
            errors.append(f"❌ Missing metadata fields: {missing}")
        
        # Check table results consistency
        requested = set(self.execution_metadata.get("tables_requested", []))
        processed = set(self.table_results.keys())
        if requested != processed:
            errors.append(f"❌ Table processing mismatch: requested {requested}, processed {processed}")
        
        # Validate S3 state if enabled
        if self.internal_state.get("completion_markers_requested") and not self.internal_state["completion_markers_created"]:
            errors.append("❌ S3 completion markers requested but not created")
        
        # Check execution errors
        if self.internal_state["execution_errors"]:
            errors.extend([f"❌ Runtime error: {err}" for err in self.internal_state["execution_errors"]])
        
        # Check validation errors
        if self.internal_state["validation_errors"]:
            errors.extend([f"❌ Validation error: {err}" for err in self.internal_state["validation_errors"]])
        
        return len(errors) == 0, errors

    def get_execution_health_report(self) -> Dict[str, Any]:
        """Detailed health report for user inspection"""
        is_valid, errors = self.validate_execution_state()
        
        return {
            "overall_health": "✅ HEALTHY" if is_valid else "❌ ISSUES DETECTED",
            "validation_errors": errors,
            "internal_state": self.internal_state.copy(),
            "execution_progress": {
                "tables_requested": len(self.execution_metadata.get("tables_requested", [])),
                "tables_processed": len(self.table_results),
                "success_count": len([t for t in self.table_results.values() if t["status"] == "success"]),
                "failure_count": len([t for t in self.table_results.values() if t["status"] == "failed"]),
                "skipped_count": len([t for t in self.table_results.values() if t["status"] == "skipped"])
            },
            "resource_usage": {
                "s3_files_created": sum(t.get("s3_files_created", 0) for t in self.table_results.values()),
                "total_rows": sum(t.get("rows_extracted", 0) for t in self.table_results.values()),
                "duration_seconds": self.execution_metadata.get("duration_seconds", 0)
            },
            "airflow_integration_status": {
                "json_output_enabled": "json_output_written" in self.internal_state and self.internal_state["json_output_written"],
                "completion_markers_enabled": self.internal_state.get("completion_markers_requested", False),
                "completion_markers_created": self.internal_state.get("completion_markers_created", False)
            }
        }

    def log_execution_error(self, error: str):
        """Log execution error for state tracking"""
        self.internal_state["execution_errors"].append(error)
        logger.error(f"Execution error logged: {error}")

    def log_validation_error(self, error: str):
        """Log validation error for state tracking"""
        self.internal_state["validation_errors"].append(error)
        logger.warning(f"Validation error logged: {error}")

    def display_health_summary(self):
        """Display concise health summary to user"""
        health_report = self.get_execution_health_report()
        
        click.echo(f"\n🏥 EXECUTION HEALTH: {health_report['overall_health']}")
        
        if health_report['validation_errors']:
            click.echo("⚠️  Issues detected:")
            for error in health_report['validation_errors']:
                click.echo(f"   {error}")
        
        progress = health_report['execution_progress']
        click.echo(f"📊 Progress: {progress['success_count']}/{progress['tables_requested']} tables successful")
        
        if progress['failure_count'] > 0:
            click.echo(f"❌ Failures: {progress['failure_count']}")
        
        if progress['skipped_count'] > 0:
            click.echo(f"⏭️  Skipped: {progress['skipped_count']}")
        
        airflow_status = health_report['airflow_integration_status']
        if airflow_status['json_output_enabled']:
            click.echo("✅ JSON metadata output: Available")
        if airflow_status['completion_markers_created']:
            click.echo("✅ S3 completion markers: Created")

def add_airflow_options():
    """Decorator to add Airflow integration options to commands"""
    def decorator(func):
        func = click.option('--output-json', type=click.Path(), 
                          help='Output execution metadata as JSON file for Airflow monitoring')(func)
        func = click.option('--create-completion-marker', is_flag=True,
                          help='Create S3 completion markers for Airflow S3KeySensor')(func)
        func = click.option('--completion-marker-bucket', type=str,
                          help='S3 bucket for completion markers (defaults to data bucket)')(func)
        func = click.option('--completion-marker-prefix', type=str, default='completion_markers',
                          help='S3 prefix for completion markers')(func)
        func = click.option('--idempotent', is_flag=True,
                          help='Skip processing if completion marker already exists')(func)
        return func
    return decorator

def check_idempotency(bucket: str, pipeline: str, table: str, prefix: str = "completion_markers") -> bool:
    """Check if processing already completed (idempotency check)"""
    try:
        s3_client = boto3.client('s3')
        date_str = datetime.now().strftime('%Y%m%d')
        clean_table = table.replace('.', '_').replace(':', '_')
        marker_key = f"{prefix}/{date_str}/sync_{clean_table}_SUCCESS"
        
        s3_client.head_object(Bucket=bucket, Key=marker_key)
        return True  # Marker exists
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False  # Marker doesn't exist
        else:
            logger.warning(f"Error checking idempotency marker: {e}")
            return False  # Assume not processed
    except Exception as e:
        logger.warning(f"Unexpected error checking idempotency: {e}")
        return False

def print_exit_code_help():
    """Print help about exit codes for Airflow integration"""
    click.echo("")
    click.echo("📊 Exit Codes for Airflow Integration:")
    click.echo("  0 - Success: All tables processed successfully")
    click.echo("  1 - Configuration Error: Invalid pipeline/table configuration")
    click.echo("  2 - Connection Error: Database or S3 connection failed")
    click.echo("  3 - Partial Success: Some tables succeeded, others failed")
    click.echo("  4 - Data Validation Error: Schema or data quality issues")
    click.echo("  5 - S3 Error: S3 upload or access issues")
    click.echo("  6 - Redshift Error: Redshift COPY or SQL execution issues")
    click.echo("")