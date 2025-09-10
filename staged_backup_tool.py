#!/usr/bin/env python3
"""
Staged Backup Tool - CLI Wrapper for Date Range Processing

Implements Option A approach: Clean CLI orchestration for common staged backup patterns.
Eliminates MySQL dependency and uses simplified CLI wrapper architecture.

Features:
- Automatic watermark reset for Airflow integration
- Date range validation and chunking
- JSON output for automation pipelines
- Simplified CLI orchestration without MySQL queries
- Conservative defaults for reliable operation

Usage:
    # Basic staged backup with auto-reset
    python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \
        --start-date '2025-08-01' --end-date '2025-08-15'
    
    # Airflow integration mode
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
    
    def run_command(self, cmd: str, timeout: int = 300) -> Tuple[bool, str, str]:
        """Execute CLI command and return success, stdout, stderr."""
        logger.info(f"🔧 Running: {cmd}")
        try:
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
        max_chunks: Optional[int] = None
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
                
                # Show auto-reset command
                reset_cmd = f"python -m src.cli.main watermark reset -p {self.pipeline} -t {self.table}"
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
                
                # Step 1: Auto-reset watermark (Airflow integration requirement)
                logger.info("Step 1: Auto-resetting watermark for clean state...")
                reset_cmd = f"python -m src.cli.main watermark reset -p {self.pipeline} -t {self.table}"
                success, output, error = self.run_command(reset_cmd)
                result["commands_executed"].append(reset_cmd)
                
                if not success:
                    raise Exception(f"Failed to reset watermark: {error}")
                
                logger.info("✅ Watermark reset completed")
                
                # Step 2: Set watermark to start date
                logger.info(f"Step 2: Setting watermark to {start_date}...")
                formatted_start_date = self.convert_to_mysql_datetime(start_date)
                set_cmd = f"python -m src.cli.main watermark set -p {self.pipeline} -t {self.table} --timestamp '{formatted_start_date}'"
                success, output, error = self.run_command(set_cmd)
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
                
                success, output, error = self.run_command(sync_cmd, timeout=1800)  # 30 min timeout
                result["commands_executed"].append(sync_cmd)
                
                if not success:
                    raise Exception(f"Sync failed: {error}")
                
                logger.info("✅ Staged backup completed successfully!")
                result["success"] = True
                
        except Exception as e:
            logger.error(f"❌ Staged backup failed: {e}")
            result["error"] = str(e)
            
        finally:
            result["duration_seconds"] = time.time() - execution_start
            
        return result
    
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
                "auto_reset_watermark": True,
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
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(pipeline, table, start_date, end_date, dry_run, chunk_size, max_chunks, 
         json_output, output_file, verbose):
    """
    Staged Backup Tool - CLI Wrapper for Date Range Processing
    
    Implements simplified CLI orchestration with automatic watermark reset for Airflow integration.
    
    Examples:
        # Basic staged backup (auto-resets watermark)
        python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \\
            --start-date '2025-08-01' --end-date '2025-08-15'
        
        # With row limiting (75K rows/chunk × 30 chunks = 2.25M rows max)
        python staged_backup_tool.py -p us_dw_hybrid_v1_2 -t settlement.settle_orders \\
            --start-date '2025-08-01' --end-date '2025-08-15' --max-chunks 30
        
        # Airflow integration mode with JSON output
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
    
    try:
        # Step 1: Validate date range
        valid, message = tool.validate_date_range(start_date, end_date)
        if not valid:
            logger.error(f"❌ Invalid date range: {message}")
            sys.exit(1)
        
        logger.info(f"✅ Date range validation: {message}")
        
        # Step 2: Execute staged backup
        logger.info("🚀 Executing staged backup with CLI orchestration...")
        result = tool.execute_staged_backup(
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            chunk_size=chunk_size,
            max_chunks=max_chunks
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