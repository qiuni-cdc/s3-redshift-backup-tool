#!/usr/bin/env python3
"""
Staged Backup Tool - CLI Wrapper for Date Range Processing

Implements Option A approach: Clean CLI orchestration for common staged backup patterns.
Eliminates MySQL dependency and uses simplified CLI wrapper architecture.

Features:
- Smart watermark reset (preserves existing S3 files) for Airflow integration
- Date range validation and chunking
- JSON output for automation pipelines
- Simplified CLI orchestration without MySQL queries
- Conservative defaults for reliable operation
- S3 file protection to prevent re-processing existing data

Usage:
    # Basic staged backup with smart reset (preserves S3 files)
    python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \
        --start-date '2025-08-01' --end-date '2025-08-15'
    
    # Airflow integration mode with S3 file preservation
    python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \
        --start-date '2025-08-01' --end-date '2025-08-15' \
        --json-output --output-file /tmp/backup_report.json
"""

import click
import subprocess
import json
import time
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


class StagedBackupTool:
    """CLI wrapper for staged backup operations with automatic watermark management."""
    
    def __init__(self, pipeline: str, table: str):
        self.pipeline = pipeline
        self.table = table
        self.start_time = datetime.now()
        self.execution_id = f"staged_backup_{int(time.time())}"
        
    def validate_date_range(self, start_date: str, end_date: str) -> Tuple[bool, str]:
        """Validate date range format and logic."""
        try:
            start_dt = datetime.fromisoformat(start_date.replace('T', ' '))
            end_dt = datetime.fromisoformat(end_date.replace('T', ' '))
            
            if start_dt >= end_dt:
                return False, f"Start date ({start_date}) must be before end date ({end_date})"
                
            # Check if range is reasonable (not too far in future)
            now = datetime.now()
            if start_dt > now + timedelta(days=1):
                return False, f"Start date ({start_date}) is too far in the future"
                
            return True, "Date range is valid"
            
        except ValueError as e:
            return False, f"Invalid date format: {e}"
    
    def run_command(self, cmd: str, timeout: int = 300, show_output: bool = False) -> Tuple[bool, str, str]:
        """Execute CLI command and return success, stdout, stderr."""
        logger.info(f"🔧 Running: {cmd}")
        try:
            if show_output:
                # Stream output in real-time for debugging
                import subprocess
                process = subprocess.Popen(
                    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                    text=True, bufsize=1, universal_newlines=True
                )
                
                stdout_lines = []
                stderr_lines = []
                
                # Read output line by line and display immediately
                while True:
                    stdout_line = process.stdout.readline()
                    stderr_line = process.stderr.readline()
                    
                    if stdout_line:
                        stdout_lines.append(stdout_line)
                        # Show CLI output with prefix for clarity
                        print(f"  CLI: {stdout_line.rstrip()}")
                    
                    if stderr_line:
                        stderr_lines.append(stderr_line)
                        print(f"  ERR: {stderr_line.rstrip()}")
                    
                    if process.poll() is not None:
                        break
                
                # Read any remaining output
                remaining_stdout, remaining_stderr = process.communicate()
                if remaining_stdout:
                    stdout_lines.append(remaining_stdout)
                    for line in remaining_stdout.split('\n'):
                        if line.strip():
                            print(f"  CLI: {line}")
                if remaining_stderr:
                    stderr_lines.append(remaining_stderr)
                    for line in remaining_stderr.split('\n'):
                        if line.strip():
                            print(f"  ERR: {line}")
                
                stdout = ''.join(stdout_lines)
                stderr = ''.join(stderr_lines)
                return process.returncode == 0, stdout, stderr
            else:
                # Standard execution without streaming
                result = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, timeout=timeout
                )
                return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return False, "", f"Command timed out after {timeout}s"
        except Exception as e:
            return False, "", str(e)
    
    def convert_to_mysql_datetime(self, date_str: str) -> str:
        """Convert date string to MySQL datetime format."""
        try:
            # Handle various input formats
            if 'T' in date_str:
                date_str = date_str.replace('T', ' ')
            
            # Parse datetime
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            
            # Return in MySQL format (YYYY-MM-DD HH:MM:SS)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
            
        except Exception as e:
            logger.warning(f"Date conversion failed for '{date_str}': {e}")
            return date_str
    
    def execute_staged_backup(
        self, 
        start_date: str, 
        end_date: str, 
        dry_run: bool = False,
        chunk_size: int = 75000,
        max_chunks: Optional[int] = None,
        verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Execute staged backup using simplified CLI orchestration approach.
        
        Approach:
        1. Auto-reset watermark (for Airflow integration)
        2. Set watermark to start_date
        3. Run sync with conservative chunk limits
        """
        result = {
            "success": False,
            "start_date": start_date,
            "end_date": end_date,
            "chunk_size": chunk_size,
            "max_chunks": max_chunks,
            "commands_executed": [],
            "duration_seconds": 0,
            "error": None
        }
        
        execution_start = time.time()
        
        try:
            if dry_run:
                logger.info("🔍 DRY RUN MODE - Showing commands that would be executed:")
                
                # Show smart-reset command (preserves S3 files)
                reset_cmd = f"python -m src.cli.main watermark reset -p {self.pipeline} -t {self.table} --preserve-files --yes"
                logger.info(f"  1. {reset_cmd}")
                result["commands_executed"].append(f"[DRY RUN] {reset_cmd}")
                
                # Show watermark set command
                formatted_start_date = self.convert_to_mysql_datetime(start_date)
                set_cmd = f"python -m src.cli.main watermark set -p {self.pipeline} -t {self.table} --timestamp '{formatted_start_date}'"
                logger.info(f"  2. {set_cmd}")
                result["commands_executed"].append(f"[DRY RUN] {set_cmd}")
                
                # Show sync command
                if max_chunks:
                    sync_cmd = f"python -m src.cli.main sync pipeline -p {self.pipeline} -t {self.table} --limit {chunk_size} --max-chunks {max_chunks}"
                    estimated_rows = chunk_size * max_chunks
                else:
                    sync_cmd = f"python -m src.cli.main sync pipeline -p {self.pipeline} -t {self.table} --limit {chunk_size}"
                    estimated_rows = f"{chunk_size} per chunk (unlimited)"
                
                logger.info(f"  3. {sync_cmd}")
                result["commands_executed"].append(f"[DRY RUN] {sync_cmd}")
                
                logger.info(f"📊 Estimated processing: {estimated_rows} rows")
                logger.info(f"📅 Date range: {start_date} to {end_date}")
                logger.info("🔒 Dry-run completed safely - no watermarks modified")
                
                result["success"] = True
                
            else:
                logger.info("🚀 Executing staged backup with auto-reset...")
                
                # Step 1: Smart reset watermark (preserves processed S3 files)
                logger.info("Step 1: Smart-resetting watermark (preserving processed S3 files)...")
                reset_cmd = f"python -m src.cli.main watermark reset -p {self.pipeline} -t {self.table} --preserve-files --yes"
                success, output, error = self.run_command(reset_cmd, show_output=verbose)
                result["commands_executed"].append(reset_cmd)
                
                if not success:
                    raise Exception(f"Failed to reset watermark: {error}")
                
                logger.info("✅ Watermark smart-reset completed (S3 files preserved)")
                
                # Step 2: Set watermark to start date
                logger.info(f"Step 2: Setting watermark to {start_date}...")
                formatted_start_date = self.convert_to_mysql_datetime(start_date)
                set_cmd = f"python -m src.cli.main watermark set -p {self.pipeline} -t {self.table} --timestamp '{formatted_start_date}'"
                success, output, error = self.run_command(set_cmd, show_output=verbose)
                result["commands_executed"].append(set_cmd)
                
                if not success:
                    raise Exception(f"Failed to set watermark: {error}")
                
                logger.info("✅ Watermark set completed")
                
                # Step 3: Execute sync with configured limits
                if max_chunks:
                    logger.info(f"Step 3: Syncing with {chunk_size} rows/chunk × {max_chunks} chunks...")
                    sync_cmd = f"python -m src.cli.main sync pipeline -p {self.pipeline} -t {self.table} --limit {chunk_size} --max-chunks {max_chunks}"
                else:
                    logger.info(f"Step 3: Syncing with {chunk_size} rows/chunk (unlimited chunks)...")
                    sync_cmd = f"python -m src.cli.main sync pipeline -p {self.pipeline} -t {self.table} --limit {chunk_size}"
                
                success, output, error = self.run_command(sync_cmd, timeout=1800, show_output=verbose)  # 30 min timeout
                result["commands_executed"].append(sync_cmd)
                
                if not success:
                    raise Exception(f"Sync failed: {error}")
                
                # Parse sync output for actual results
                actual_rows = self.parse_rows_from_sync_output(output)
                if actual_rows > 0:
                    logger.info(f"✅ Sync completed: processed {actual_rows:,} rows")
                else:
                    logger.warning("⚠️  Sync completed but no rows were processed (may be normal if no new data)")
                
                logger.info("✅ Staged backup completed successfully!")
                result["success"] = True
                result["actual_rows_processed"] = actual_rows
                
        except Exception as e:
            logger.error(f"❌ Staged backup failed: {e}")
            result["error"] = str(e)
            
        finally:
            result["duration_seconds"] = time.time() - execution_start
            
        return result
    
    def parse_rows_from_sync_output(self, output: str) -> int:
        """Extract actual row count from sync command output."""
        import re
        import json
        
        # Look for JSON log entries that contain total_rows
        total_rows = 0
        
        # Parse each line for JSON log entries
        for line in output.split('\n'):
            try:
                # Try to parse as JSON (structured logging format)
                if line.strip() and ('{' in line and '}' in line):
                    # Extract JSON part from the line
                    json_start = line.find('{')
                    json_part = line[json_start:]
                    log_entry = json.loads(json_part)
                    
                    # Look for total_rows in backup summary
                    if log_entry.get('event') == 'Sequential backup summary':
                        total_rows = max(total_rows, log_entry.get('total_rows', 0))
                    
                    # Look for per-table metrics
                    per_table = log_entry.get('per_table_metrics', {})
                    for table_metrics in per_table.values():
                        table_rows = table_metrics.get('rows', 0)
                        total_rows = max(total_rows, table_rows)
                        
            except (json.JSONDecodeError, ValueError):
                # If not JSON, try text patterns
                if 'rows processed' in line.lower() or 'total_rows' in line.lower():
                    numbers = re.findall(r'\d+', line)
                    if numbers:
                        total_rows = max(total_rows, int(numbers[-1]))
        
        return total_rows
    
    def create_json_report(self, result: Dict[str, Any], start_date: str, end_date: str) -> Dict[str, Any]:
        """Create comprehensive JSON report for Airflow integration."""
        return {
            "execution_id": self.execution_id,
            "tool": "staged_backup_tool",
            "version": "3.0.0",
            "timestamp": datetime.now().isoformat(),
            "pipeline": self.pipeline,
            "table": self.table,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            },
            "execution": {
                "success": result["success"],
                "duration_seconds": result["duration_seconds"],
                "chunk_size": result["chunk_size"],
                "max_chunks": result["max_chunks"],
                "commands_executed": result["commands_executed"],
                "error": result["error"]
            },
            "airflow_integration": {
                "smart_reset_watermark": True,
                "preserves_s3_files": True,
                "ready_for_downstream": result["success"]
            }
        }


@click.command()
@click.option('--pipeline', '-p', required=True, help='Pipeline name')
@click.option('--table', '-t', required=True, help='Table name')
@click.option('--start-date', required=True, help='Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
@click.option('--end-date', required=True, help='End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
@click.option('--chunk-size', type=int, default=75000, help='Rows per chunk (default: 75000)')
@click.option('--max-chunks', type=int, help='Maximum chunks to process (for row limiting)')
@click.option('--json-output', is_flag=True, help='Generate JSON output for Airflow integration')
@click.option('--output-file', help='Save JSON output to specific file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose CLI output streaming (shows detailed JSON logs from sync operations)')
def main(pipeline, table, start_date, end_date, dry_run, chunk_size, max_chunks, 
         json_output, output_file, verbose):
    """
    Staged Backup Tool - CLI Wrapper for Date Range Processing
    
    Implements simplified CLI orchestration with smart watermark reset that preserves 
    existing S3 files for Airflow integration.
    
    Examples:
        # Basic staged backup (smart-resets watermark, preserves S3 files)
        python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \\
            --start-date '2025-08-01' --end-date '2025-08-15'
        
        # With row limiting (75K rows/chunk × 30 chunks = 2.25M rows max)
        python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \\
            --start-date '2025-08-01' --end-date '2025-08-15' --max-chunks 30
        
        # Airflow integration mode with JSON output and S3 file preservation
        python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \\
            --start-date '2025-08-01' --end-date '2025-08-15' \\
            --json-output --output-file /tmp/backup_report.json
    """
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize tool
    tool = StagedBackupTool(pipeline, table)
    
    logger.info("🚀 Staged Backup Tool v3.0 - CLI Wrapper Mode")
    logger.info(f"📋 Pipeline: {pipeline}")
    logger.info(f"📊 Table: {table}")
    logger.info(f"📅 Date Range: {start_date} to {end_date}")
    logger.info(f"⚙️  Chunk Size: {chunk_size:,} rows")
    if max_chunks:
        logger.info(f"🔢 Max Chunks: {max_chunks} (total: {chunk_size * max_chunks:,} rows)")
    logger.info("🛡️  S3 File Protection: Preserves existing processed files")
    
    try:
        # Step 1: Validate date range
        valid, message = tool.validate_date_range(start_date, end_date)
        if not valid:
            logger.error(f"❌ Invalid date range: {message}")
            sys.exit(1)
        
        logger.info(f"✅ Date range validation: {message}")
        
        # Step 2: Execute staged backup
        logger.info("🚀 Executing staged backup with CLI orchestration...")
        if verbose:
            logger.info("🔍 Verbose mode enabled - showing detailed CLI output...")
        result = tool.execute_staged_backup(
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            chunk_size=chunk_size,
            max_chunks=max_chunks,
            verbose=verbose
        )
        
        # Step 3: Generate JSON output if requested
        report = None
        if json_output or output_file:
            logger.info("📝 Generating JSON report...")
            report = tool.create_json_report(
                result=result,
                start_date=start_date,
                end_date=end_date
            )
            
            # Save to file
            if output_file:
                with open(output_file, 'w') as f:
                    json.dump(report, f, indent=2)
                logger.info(f"📄 JSON report saved to: {output_file}")
            
            # Print JSON to stdout for Airflow
            if json_output and not output_file:
                print(json.dumps(report, indent=2))
        
        # Step 4: Final status
        if result["success"]:
            logger.info("🎉 Staged backup completed successfully!")
            if not dry_run:
                logger.info(f"⏱️  Duration: {result['duration_seconds']:.2f} seconds")
                logger.info("🔄 Watermark was automatically reset and configured")
            
            sys.exit(0)
        else:
            logger.error(f"❌ Staged backup failed: {result['error']}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Tool execution failed: {e}")
        sys.exit(2)


if __name__ == '__main__':
    main()