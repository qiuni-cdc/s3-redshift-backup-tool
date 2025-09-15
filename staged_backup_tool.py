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
import os
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
    
    def run_sync_with_progress_monitoring(self, cmd: str, timeout: int = 7200, target_rows: int = 0) -> Tuple[bool, str, str]:
        """
        Run sync command with periodic progress monitoring via watermark status.
        
        This provides real-time visibility into sync progress without relying on stdout streaming.
        """
        import threading
        import time
        import os
        
        logger.info(f"🔧 Running: {cmd}")
        logger.info("📊 Streaming sync process output in real-time...")
        
        # Start the sync process with real-time output streaming  
        # Force unbuffered output using stdbuf
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Use stdbuf to force line buffering for real-time output
        unbuffered_cmd = f"stdbuf -oL -eL {cmd}"
        
        process = subprocess.Popen(
            unbuffered_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
            text=True, bufsize=0, universal_newlines=True, env=env
        )
        
        # Collect output for return
        stdout_lines = []
        stderr_lines = []
        
        def stream_output():
            """Stream stdout and stderr in real-time using non-blocking I/O"""
            import select
            import fcntl
            import os
            
            try:
                # Make pipes non-blocking
                fd_stdout = process.stdout.fileno()
                fd_stderr = process.stderr.fileno()
                
                fcntl.fcntl(fd_stdout, fcntl.F_SETFL, 
                           fcntl.fcntl(fd_stdout, fcntl.F_GETFL) | os.O_NONBLOCK)
                fcntl.fcntl(fd_stderr, fcntl.F_SETFL, 
                           fcntl.fcntl(fd_stderr, fcntl.F_GETFL) | os.O_NONBLOCK)
                
                stdout_buffer = ""
                stderr_buffer = ""
                
                # Stream output until process completes
                while process.poll() is None:
                    # Use select to check for available data
                    ready, _, _ = select.select([fd_stdout, fd_stderr], [], [], 0.1)
                    
                    for fd in ready:
                        try:
                            if fd == fd_stdout:
                                chunk = os.read(fd, 4096).decode('utf-8', errors='replace')
                                if chunk:
                                    stdout_buffer += chunk
                                    # Process complete lines
                                    while '\n' in stdout_buffer:
                                        line, stdout_buffer = stdout_buffer.split('\n', 1)
                                        if line.strip():
                                            stdout_lines.append(line)
                                            logger.info(f"SYNC: {line}")
                                        
                            elif fd == fd_stderr:
                                chunk = os.read(fd, 4096).decode('utf-8', errors='replace')
                                if chunk:
                                    stderr_buffer += chunk
                                    # Process complete lines
                                    while '\n' in stderr_buffer:
                                        line, stderr_buffer = stderr_buffer.split('\n', 1)
                                        if line.strip():
                                            stderr_lines.append(line)
                                            logger.info(f"SYNC: {line}")
                                            
                        except (OSError, IOError):
                            # No more data available
                            pass
                
                # Process any remaining buffered output
                if stdout_buffer.strip():
                    for line in stdout_buffer.split('\n'):
                        if line.strip():
                            stdout_lines.append(line)
                            logger.info(f"SYNC: {line}")
                            
                if stderr_buffer.strip():
                    for line in stderr_buffer.split('\n'):
                        if line.strip():
                            stderr_lines.append(line)
                            logger.info(f"SYNC: {line}")
                            
            except Exception as e:
                logger.warning(f"Output streaming error: {e}")
        
        # Start output streaming thread
        stream_thread = threading.Thread(target=stream_output, daemon=True)
        stream_thread.start()
        
        try:
            # Wait for process to complete with timeout
            process.wait(timeout=timeout)
            success = process.returncode == 0
            
            # Wait for streaming thread to finish
            stream_thread.join(timeout=10)
            
            if success:
                logger.info("✅ Sync command completed successfully")
            else:
                logger.error(f"❌ Sync command failed with return code {process.returncode}")
                
            return success, '\n'.join(stdout_lines), '\n'.join(stderr_lines)
            
        except subprocess.TimeoutExpired:
            process.kill()
            logger.error(f"⏱️  Sync command timed out after {timeout} seconds")
            return False, "", f"Command timed out after {timeout}s"
        except Exception as e:
            process.kill()
            logger.error(f"💥 Sync command failed: {e}")
            return False, "", str(e)
    
    def calculate_row_count_in_date_range(self, start_date: str, end_date: str, dry_run: bool = False, verbose: bool = False) -> int:
        """
        Query MySQL to count exact rows between timestamps using sync tool's connection.
        
        This reuses the same connection infrastructure as the sync command.
        """
        try:
            if dry_run:
                logger.info("🔍 DRY RUN: Would count rows in MySQL")
                return 1963280  # Use known value for dry run
            
            logger.info("🔍 Querying MySQL for exact row count...")
            
            # Convert dates to MySQL format
            mysql_start = self.convert_to_mysql_datetime(start_date)
            mysql_end = self.convert_to_mysql_datetime(end_date)
            
            # Import the same modules the sync tool uses
            from src.core.configuration_manager import ConfigurationManager
            from src.core.connections import ConnectionManager
            from src.config.settings import AppConfig
            
            try:
                # Initialize configuration (same as sync tool)
                config = AppConfig()
                conn_mgr = ConnectionManager(config)
                
                # Use the same connection method as backup strategies
                with conn_mgr.ssh_tunnel() as local_port:
                    
                    # Get MySQL connection through the tunnel
                    with conn_mgr.database_connection(local_port) as mysql_conn:
                        cursor = mysql_conn.cursor()
                        
                        # Build the count query
                        # Extract schema and table name
                        if '.' in self.table:
                            schema, table = self.table.split('.', 1)
                            full_table = f"`{schema}`.`{table}`"
                        else:
                            # Use default schema from connection if not specified
                            full_table = f"`{self.table}`"
                        
                        # Query with created_at (most common CDC column)
                        count_query = f"""
                            SELECT COUNT(*) 
                            FROM {full_table} 
                            WHERE created_at >= %s AND created_at < %s
                        """
                        
                        logger.info(f"   Executing: SELECT COUNT(*) FROM {full_table} WHERE created_at BETWEEN '{mysql_start}' AND '{mysql_end}'")
                        cursor.execute(count_query, (mysql_start, mysql_end))
                        
                        result = cursor.fetchone()
                        row_count = result[0] if result else 0
                        
                        logger.info(f"✅ Found {row_count:,} rows in date range")
                        return row_count
                    
            except Exception as e:
                logger.warning(f"MySQL count failed: {e}")
                logger.info("💡 Falling back to estimation...")
                
                # Fall back to estimation if SQL fails
                start_dt = datetime.fromisoformat(start_date.replace('T', ' '))
                end_dt = datetime.fromisoformat(end_date.replace('T', ' '))
                days = (end_dt - start_dt).days
                estimated_rows_per_day = 500000
                estimated_total = days * estimated_rows_per_day
                
                logger.info(f"   Estimated: {days} days × {estimated_rows_per_day:,}/day = ~{estimated_total:,} rows")
                return estimated_total
                
        except Exception as e:
            logger.warning(f"Row count calculation failed: {e}")
            return -1
    
    def calculate_optimal_chunks(self, total_rows: int, chunk_size: int = 75000) -> Tuple[int, int]:
        """
        Calculate optimal chunk_size and max_chunks for the given row count.
        
        Returns: (optimal_chunk_size, max_chunks)
        """
        if total_rows <= 0:
            return chunk_size, 1
            
        # Calculate number of chunks needed
        chunks_needed = (total_rows + chunk_size - 1) // chunk_size  # Ceiling division
        
        logger.info(f"📊 Row calculation: {total_rows:,} total rows ÷ {chunk_size:,} chunk size = {chunks_needed} chunks needed")
        
        return chunk_size, chunks_needed

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
    
    def get_pipeline_for_sync_mode(self, sync_mode: str, base_pipeline: str) -> str:
        """
        Get the appropriate pipeline configuration based on sync mode.
        
        Uses intelligent pipeline name transformation based on the base pipeline:
        1. If sync_mode matches the base pipeline strategy, use base pipeline as-is
        2. Otherwise, try to find a variant of the base pipeline for the sync_mode
        3. Fall back to base pipeline if no variant exists
        
        Example transformations:
        - us_dw_hybrid_v1_2 + id_only → us_dw_id_only_v1_2
        - settlement_pipeline + full_sync → settlement_full_sync_pipeline (if exists)
        """
        # If hybrid mode or sync_mode matches pipeline name, use original
        if sync_mode == 'hybrid' or sync_mode.lower() in base_pipeline.lower():
            return base_pipeline
        
        # Try to find a pipeline variant by replacing strategy keywords
        potential_pipelines = []
        
        # Strategy 1: Replace strategy keywords in pipeline name
        strategy_replacements = {
            'hybrid': sync_mode,
            'cdc': f'{sync_mode}_cdc'
        }
        
        for old_strategy, new_strategy in strategy_replacements.items():
            if old_strategy in base_pipeline.lower():
                variant = base_pipeline.replace(old_strategy, new_strategy)
                potential_pipelines.append(variant)
                # Also try with underscores
                variant_underscore = base_pipeline.replace(old_strategy.replace('_', ''), f'{sync_mode}_')
                potential_pipelines.append(variant_underscore)
        
        # Strategy 2: Insert sync_mode before version suffix
        if '_v' in base_pipeline:  # e.g., us_dw_hybrid_v1_2 → us_dw_id_only_v1_2
            base_part, version_part = base_pipeline.rsplit('_v', 1)
            # Remove strategy keywords from base part
            clean_base = base_part
            for strategy in ['hybrid', 'cdc', 'full_sync', 'id_only', 'timestamp_only']:
                clean_base = clean_base.replace(f'_{strategy}', '').replace(strategy, '')
            
            variant = f"{clean_base}_{sync_mode}_v{version_part}"
            potential_pipelines.append(variant)
        
        # Strategy 3: Simple append/prepend patterns
        potential_pipelines.extend([
            f"{base_pipeline}_{sync_mode}",
            f"{sync_mode}_{base_pipeline}",
            base_pipeline.replace('pipeline', f'{sync_mode}_pipeline')
        ])
        
        # Check if any potential pipeline exists by attempting to validate it
        for potential in potential_pipelines:
            if self._pipeline_exists(potential):
                logger.info(f"🔄 Sync mode '{sync_mode}' mapped to pipeline: {potential}")
                return potential
        
        # Fallback: use original pipeline
        logger.info(f"ℹ️  No specific pipeline found for sync mode '{sync_mode}', using base pipeline: {base_pipeline}")
        return base_pipeline
    
    def _pipeline_exists(self, pipeline_name: str) -> bool:
        """
        Check if a pipeline configuration exists.
        
        This is a simple file existence check. In production, you might want
        to use the configuration manager to validate pipeline existence.
        """
        import os
        config_path = f"config/pipelines/{pipeline_name}.yml"
        exists = os.path.exists(config_path)
        
        if exists:
            logger.debug(f"✅ Pipeline config found: {config_path}")
        else:
            logger.debug(f"❌ Pipeline config not found: {config_path}")
            
        return exists
    
    def execute_staged_backup(
        self, 
        start_date: str, 
        end_date: str, 
        dry_run: bool = False,
        chunk_size: int = 75000,
        max_chunks: Optional[int] = None,
        verbose: bool = False,
        sync_mode: str = 'hybrid',
        full_sync_mode: str = 'append'
    ) -> Dict[str, Any]:
        """
        Execute staged backup with automatic row count calculation and CDC strategy support.
        
        Supports multiple CDC strategies:
        - hybrid: Timestamp + ID (default, most robust)
        - id_only: ID-only for append-only tables
        - full_sync: Complete table refresh (replace/paginate/append)
        - timestamp_only: Legacy v1.0.0 compatibility
        - custom_sql: User-defined queries
        
        Approach:
        1. Auto-reset watermark (for Airflow integration)
        2. Set watermark to start_date  
        3. Query MySQL to count exact rows in date range
        4. Calculate optimal chunk parameters
        5. Run sync with precise row control
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
            # Step 0: Calculate exact row count in date range (RESTORED FUNCTIONALITY)
            logger.info("🧮 Step 0: Calculating exact row count in date range...")
            total_rows_in_range = self.calculate_row_count_in_date_range(start_date, end_date, dry_run, verbose)
            
            if total_rows_in_range > 0:
                # Calculate optimal chunk parameters based on actual data
                if max_chunks is None:
                    optimal_chunk_size, optimal_max_chunks = self.calculate_optimal_chunks(total_rows_in_range, chunk_size)
                    chunk_size = optimal_chunk_size
                    max_chunks = optimal_max_chunks
                    logger.info(f"🎯 Auto-calculated: {chunk_size:,} rows/chunk × {max_chunks} chunks = {total_rows_in_range:,} total rows")
                else:
                    logger.info(f"📋 User-specified: {chunk_size:,} rows/chunk × {max_chunks} chunks = {chunk_size * max_chunks:,} rows (actual data: {total_rows_in_range:,})")
            elif total_rows_in_range == 0:
                logger.warning("⚠️ No rows found in date range - proceeding with minimal settings")
                chunk_size = 1000
                max_chunks = 1
            else:
                logger.warning("⚠️ Row count calculation failed - using provided parameters")
                # If no max_chunks was provided and row calculation failed, set a reasonable default
                if max_chunks is None:
                    max_chunks = 30  # Conservative default: 75K × 30 = 2.25M rows max
                    logger.info(f"🔧 No max_chunks specified, defaulting to {max_chunks} chunks ({chunk_size * max_chunks:,} rows max)")
            
            # Update result with calculated values
            result["total_rows_in_range"] = total_rows_in_range
            result["chunk_size"] = chunk_size
            result["max_chunks"] = max_chunks
            
            if dry_run:
                logger.info("🔍 DRY RUN MODE - Showing commands that would be executed:")
                
                # Map sync mode to appropriate pipeline configuration
                effective_pipeline = self.get_pipeline_for_sync_mode(sync_mode, self.pipeline)
                
                # Show smart-reset command (preserves S3 files)
                reset_cmd = f"python -m src.cli.main watermark reset -p {effective_pipeline} -t {self.table} --preserve-files --yes"
                logger.info(f"  1. {reset_cmd}")
                result["commands_executed"].append(f"[DRY RUN] {reset_cmd}")
                
                # Show watermark set command
                formatted_start_date = self.convert_to_mysql_datetime(start_date)
                set_cmd = f"python -m src.cli.main watermark set -p {effective_pipeline} -t {self.table} --timestamp '{formatted_start_date}'"
                logger.info(f"  2. {set_cmd}")
                result["commands_executed"].append(f"[DRY RUN] {set_cmd}")
                
                # Show calculated sync command
                if self.pipeline:
                    if max_chunks is not None:
                        sync_cmd = f"python -m src.cli.main sync pipeline -p {effective_pipeline} -t {self.table} --limit {chunk_size} --max-chunks {max_chunks} --json-output /tmp/output.json"
                    else:
                        sync_cmd = f"python -m src.cli.main sync pipeline -p {effective_pipeline} -t {self.table} --limit {chunk_size} --json-output /tmp/output.json"
                else:
                    if max_chunks is not None:
                        sync_cmd = f"python -m src.cli.main sync -t {self.table} --limit {chunk_size} --max-chunks {max_chunks}"
                    else:
                        sync_cmd = f"python -m src.cli.main sync -t {self.table} --limit {chunk_size}"
                
                logger.info(f"  3. {sync_cmd}")
                result["commands_executed"].append(f"[DRY RUN] {sync_cmd}")
                
                logger.info(f"📊 CALCULATED processing: {total_rows_in_range:,} rows ({chunk_size:,} × {max_chunks} chunks)")
                logger.info(f"📅 Date range: {start_date} to {end_date}")
                logger.info("🔒 Dry-run completed safely - no watermarks modified")
                
                result["success"] = True
                
            else:
                logger.info("🚀 Executing staged backup with auto-reset...")
                
                # Map sync mode to appropriate pipeline configuration
                effective_pipeline = self.get_pipeline_for_sync_mode(sync_mode, self.pipeline)
                
                # Step 1: Smart reset watermark (preserves processed S3 files)
                logger.info("Step 1: Smart-resetting watermark (preserving processed S3 files)...")
                reset_cmd = f"python -m src.cli.main watermark reset -p {effective_pipeline} -t {self.table} --preserve-files --yes"
                success, output, error = self.run_command(reset_cmd, show_output=verbose)
                result["commands_executed"].append(reset_cmd)
                
                if not success:
                    raise Exception(f"Failed to reset watermark: {error}")
                
                logger.info("✅ Watermark smart-reset completed (S3 files preserved)")
                
                # Step 2: Set watermark to start date
                logger.info(f"Step 2: Setting watermark to {start_date}...")
                formatted_start_date = self.convert_to_mysql_datetime(start_date)
                set_cmd = f"python -m src.cli.main watermark set -p {effective_pipeline} -t {self.table} --timestamp '{formatted_start_date}'"
                success, output, error = self.run_command(set_cmd, show_output=verbose)
                result["commands_executed"].append(set_cmd)
                
                if not success:
                    raise Exception(f"Failed to set watermark: {error}")
                
                logger.info("✅ Watermark set completed")
                
                # Step 3: Execute sync with CALCULATED limits for precise row control
                expected_total_rows = chunk_size * max_chunks if max_chunks else total_rows_in_range
                logger.info(f"Step 3: Syncing with CALCULATED parameters...")
                logger.info(f"   📊 Target: {total_rows_in_range:,} rows in date range")  
                if max_chunks is not None:
                    logger.info(f"   ⚙️ Chunks: {chunk_size:,} rows/chunk × {max_chunks} chunks = {expected_total_rows:,} total")
                else:
                    logger.info(f"   ⚙️ Chunks: {chunk_size:,} rows/chunk × unlimited chunks = {expected_total_rows:,} total")
                logger.info("   📡 Sync pipeline executing (progress shown at completion)...")
                logger.info("   ⏱️  Timeout: 2 hours max")
                
                if self.pipeline:
                    # Use pipeline mode with JSON output to file
                    json_file = f"/tmp/staged_backup_{int(time.time())}.json"
                    if max_chunks is not None:
                        sync_cmd = f"python -m src.cli.main sync pipeline -p {effective_pipeline} -t {self.table} --limit {chunk_size} --max-chunks {max_chunks} --json-output {json_file}"
                    else:
                        sync_cmd = f"python -m src.cli.main sync pipeline -p {effective_pipeline} -t {self.table} --limit {chunk_size} --json-output {json_file}"
                else:
                    # Use v1.0 mode without JSON output (fallback to text parsing)
                    if max_chunks is not None:
                        sync_cmd = f"python -m src.cli.main sync -t {self.table} --limit {chunk_size} --max-chunks {max_chunks}"
                    else:
                        sync_cmd = f"python -m src.cli.main sync -t {self.table} --limit {chunk_size}"
                    json_file = None
                
                # Run sync with progress monitoring
                success, output, error = self.run_sync_with_progress_monitoring(sync_cmd, timeout=7200, target_rows=total_rows_in_range)
                result["commands_executed"].append(sync_cmd)
                
                if not success:
                    raise Exception(f"Sync failed: {error}")
                
                # Parse output based on whether JSON file was used
                if json_file and self.pipeline:
                    # Read JSON output from file for pipeline mode
                    actual_rows, sync_details = self.parse_json_file_output(json_file)
                    result["sync_details"] = sync_details
                    # Clean up temp file
                    try:
                        import os
                        os.remove(json_file)
                    except:
                        pass  # Ignore cleanup errors
                else:
                    # Fallback to text parsing for v1.0 mode
                    actual_rows = self.parse_rows_from_sync_output_fallback(output)
                    result["sync_details"] = {"parsing_method": "v1.0_text_fallback", "rows_found": actual_rows}
                
                # Final watermark check for accurate results
                logger.info("📊 Checking final watermark for actual sync results...")
                try:
                    watermark_cmd = f"python -m src.cli.main watermark get -p {effective_pipeline} -t {self.table}"
                    watermark_result = subprocess.run(watermark_cmd, shell=True, capture_output=True, text=True, timeout=15)
                    
                    if watermark_result.returncode == 0:
                        # Parse final results from watermark
                        final_extracted = 0
                        final_loaded = 0
                        final_s3_files = 0
                        
                        for line in watermark_result.stdout.split('\n'):
                            if 'Rows Extracted:' in line:
                                try:
                                    final_extracted = int(line.split(':')[1].replace(',', '').strip())
                                except:
                                    pass
                            elif 'Rows Loaded:' in line:
                                try:
                                    final_loaded = int(line.split(':')[1].replace(',', '').strip())
                                except:
                                    pass
                            elif 'S3 Files Created:' in line:
                                try:
                                    final_s3_files = int(line.split(':')[1].replace(',', '').strip())
                                except:
                                    pass
                        
                        logger.info("✅ Staged backup completed successfully!")
                        logger.info(f"📊 FINAL RESULTS:")
                        logger.info(f"   📤 MySQL Extraction: {final_extracted:,} rows extracted")
                        logger.info(f"   💾 S3 Storage: {final_s3_files} files created")
                        logger.info(f"   📥 Redshift Loading: {final_loaded:,} rows loaded")
                        
                        # Update result with actual data
                        result["actual_rows_processed"] = max(final_extracted, final_loaded, actual_rows)
                    else:
                        logger.info("✅ Staged backup completed successfully!")
                        if actual_rows > 0:
                            logger.info(f"📊 Processed {actual_rows:,} rows (from sync output)")
                        else:
                            logger.info("📊 Sync completed (check watermark for detailed results)")
                        result["actual_rows_processed"] = actual_rows
                        
                except Exception as e:
                    logger.info("✅ Staged backup completed successfully!")
                    logger.info(f"📊 Sync process finished (watermark check failed: {e})")
                    result["actual_rows_processed"] = actual_rows
                result["success"] = True
                
        except Exception as e:
            logger.error(f"❌ Staged backup failed: {e}")
            result["error"] = str(e)
            
        finally:
            result["duration_seconds"] = time.time() - execution_start
            
        return result
    
    def parse_json_file_output(self, json_file_path: str) -> Tuple[int, Dict[str, Any]]:
        """Parse JSON output from file (pipeline mode with --json-output file)."""
        import json
        import os
        
        total_rows = 0
        sync_details = {}
        
        try:
            if os.path.exists(json_file_path):
                with open(json_file_path, 'r') as f:
                    json_data = json.load(f)
                
                sync_details = json_data
                
                # Extract row counts from the pipeline JSON structure
                # This follows the schema from the pipeline sync command
                if 'execution_summary' in json_data:
                    execution_summary = json_data['execution_summary']
                    total_rows = execution_summary.get('total_rows_processed', 0)
                
                # Alternative: check per-table metrics
                if total_rows == 0 and 'tables' in json_data:
                    for table_data in json_data['tables'].values():
                        table_rows = table_data.get('total_rows_processed', 0)
                        total_rows += table_rows
                
                return total_rows, sync_details
            else:
                print(f"Warning: JSON output file not found: {json_file_path}")
                
        except Exception as e:
            print(f"Warning: Failed to parse JSON file {json_file_path}: {e}")
        
        # Fallback if JSON file parsing fails
        return 0, {"parsing_method": "json_file_fallback", "error": "File parsing failed"}

    def parse_json_sync_output(self, output: str) -> Tuple[int, Dict[str, Any]]:
        """Extract actual row count and sync details from JSON sync command output."""
        import json
        
        total_rows = 0
        sync_details = {}
        
        try:
            # The sync command with --json-output returns structured JSON
            # Look for the JSON output at the end of the command output
            lines = output.strip().split('\n')
            
            # Find the JSON output (usually the last substantial block)
            json_output = None
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].strip()
                if line and line.startswith('{'):
                    try:
                        # Try to parse as complete JSON
                        potential_json = '\n'.join(lines[i:])
                        json_output = json.loads(potential_json)
                        break
                    except json.JSONDecodeError:
                        # Try just this line
                        try:
                            json_output = json.loads(line)
                            break
                        except json.JSONDecodeError:
                            continue
            
            if json_output:
                sync_details = json_output
                
                # Extract row counts from structured output
                stages = json_output.get('stages', {})
                
                # Check backup stage
                backup_stage = stages.get('backup', {})
                if backup_stage.get('executed') and backup_stage.get('summary'):
                    backup_rows = backup_stage['summary'].get('total_rows', 0)
                    total_rows = max(total_rows, backup_rows)
                
                # Check redshift stage  
                redshift_stage = stages.get('redshift', {})
                if redshift_stage.get('executed') and redshift_stage.get('summary'):
                    redshift_rows = redshift_stage['summary'].get('total_rows', 0)
                    total_rows = max(total_rows, redshift_rows)
                
                # Also check tables_processed for verification
                tables_processed = json_output.get('tables_processed', 0)
                
                return total_rows, sync_details
            
        except Exception as e:
            # Log the parsing error but continue with fallback
            print(f"Warning: Failed to parse JSON sync output: {e}")
        
        # Fallback to old text parsing if JSON parsing fails
        total_rows = self.parse_rows_from_sync_output_fallback(output)
        sync_details = {"parsing_method": "fallback_text", "rows_found": total_rows}
        
        return total_rows, sync_details
    
    def parse_rows_from_sync_output_fallback(self, output: str) -> int:
        """Fallback: Extract actual row count from sync command text output (fragile but functional)."""
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
                "error": result["error"],
                "sync_details": result.get("sync_details", {}),
                "actual_rows_processed": result.get("actual_rows_processed", 0)
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
@click.option('--sync-mode', default='hybrid', 
              type=click.Choice(['hybrid', 'id_only', 'full_sync', 'timestamp_only', 'custom_sql']),
              help='CDC strategy mode (default: hybrid)')
@click.option('--full-sync-mode', default='append',
              type=click.Choice(['replace', 'paginate', 'append']),
              help='Full sync sub-mode (default: append, only used with --sync-mode full_sync)')
def main(pipeline, table, start_date, end_date, dry_run, chunk_size, max_chunks, 
         json_output, output_file, verbose, sync_mode, full_sync_mode):
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
    logger.info(f"🔄 Sync Mode: {sync_mode.upper()}")
    if sync_mode == 'full_sync':
        logger.info(f"📦 Full Sync Mode: {full_sync_mode}")
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
            verbose=verbose,
            sync_mode=sync_mode,
            full_sync_mode=full_sync_mode
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