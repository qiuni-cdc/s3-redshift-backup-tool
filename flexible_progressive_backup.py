#!/usr/bin/env python3
"""
Flexible Progressive Backup Tool for Massive Tables

A configurable automation script for handling massive table backups with 
progressive chunking, monitoring, and error recovery capabilities.

Usage Examples:
    # Basic progressive backup
    python flexible_progressive_backup.py settlement.massive_table

    # Custom chunk size and mode
    python flexible_progressive_backup.py settlement.huge_table --chunk-size 10000000 --mode backup-only

    # Resume from specific timestamp
    python flexible_progressive_backup.py settlement.big_table --start-timestamp "2025-01-01 00:00:00"

    # Full pipeline with monitoring
    python flexible_progressive_backup.py settlement.enormous_table --mode full --monitor-interval 300
"""

import argparse
import subprocess
import time
import json
import sys
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path
import signal

class ProgressiveBackupManager:
    """
    Flexible progressive backup manager for massive tables.
    
    Supports configurable chunk sizes, different backup modes, progress monitoring,
    error recovery, and resume capabilities.
    """
    
    def __init__(self, 
                 table_name: str,
                 chunk_size: int = 5000000,
                 mode: str = "full",
                 start_timestamp: Optional[str] = None,
                 max_chunks: Optional[int] = None,
                 timeout_minutes: int = 120,
                 monitor_interval: int = 300,
                 log_level: str = "info",
                 verbose_output: bool = False):
        
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.mode = mode.lower()
        self.start_timestamp = start_timestamp
        self.max_chunks = max_chunks
        self.timeout_seconds = timeout_minutes * 60
        self.monitor_interval = monitor_interval
        self.log_level = log_level
        self.verbose_output = verbose_output
        
        # Setup logging
        self.session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.log_file = f"progressive_backup_{self._clean_table_name()}_{self.session_id}.log"
        
        # State tracking
        self.chunks_processed = 0
        self.total_rows_processed = 0
        self.start_time = datetime.now()
        self.interrupted = False
        
        # Validate inputs
        self._validate_inputs()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _validate_inputs(self):
        """Validate input parameters"""
        if not self.table_name:
            raise ValueError("Table name is required")
        
        if self.chunk_size <= 0:
            raise ValueError("Chunk size must be positive")
        
        if self.mode not in ['full', 'backup-only', 'redshift-only']:
            raise ValueError("Mode must be 'full', 'backup-only', or 'redshift-only'")
        
        if self.start_timestamp:
            try:
                datetime.fromisoformat(self.start_timestamp.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("Invalid timestamp format. Use 'YYYY-MM-DD HH:MM:SS'")
    
    def _clean_table_name(self) -> str:
        """Clean table name for file naming"""
        return self.table_name.replace('.', '_').replace('-', '_')
    
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown on interruption"""
        self.log(f"Received signal {signum}, initiating graceful shutdown...")
        self.interrupted = True
    
    def log(self, message: str, level: str = "info"):
        """Enhanced logging with timestamps and levels"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        level_symbol = {
            'info': 'ℹ️',
            'success': '✅',
            'warning': '⚠️',
            'error': '❌',
            'debug': '🔍'
        }.get(level, 'ℹ️')
        
        log_entry = f"[{timestamp}] {level_symbol} {message}"
        print(log_entry)
        
        # Write to log file
        with open(self.log_file, 'a') as f:
            f.write(log_entry + '\n')
    
    def get_current_watermark(self) -> Optional[Dict[str, Any]]:
        """Get current watermark status for the table"""
        try:
            cmd = [
                'python', '-m', 'src.cli.main', 'watermark', 'get',
                '-t', self.table_name
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Parse text watermark output
                return self._parse_watermark_text(result.stdout)
            else:
                self.log(f"Failed to get watermark: {result.stderr}", "warning")
                return None
                
        except Exception as e:
            self.log(f"Error getting watermark: {e}", "error")
            return None
    
    def _parse_watermark_text(self, text_output: str) -> Dict[str, Any]:
        """Parse text watermark output into dictionary"""
        watermark = {
            'table_name': self.table_name,
            'mysql_status': 'unknown',
            'redshift_status': 'unknown', 
            'mysql_rows_extracted': 0,
            'redshift_rows_loaded': 0,
            'last_mysql_data_timestamp': None,
            'last_mysql_extraction_time': None
        }
        
        # Use context-aware parsing
        lines = text_output.split('\n')
        in_mysql_section = False
        in_redshift_section = False
        
        for line in lines:
            line = line.strip()
            
            # Detect sections
            if 'MySQL → S3 Backup Stage:' in line:
                in_mysql_section = True
                in_redshift_section = False
                continue
            elif 'S3 → Redshift Loading Stage:' in line:
                in_mysql_section = False
                in_redshift_section = True
                continue
            elif 'Next Incremental Backup:' in line:
                in_mysql_section = False
                in_redshift_section = False
                continue
            
            # Parse based on current section
            if 'Status:' in line:
                status_value = line.split(':')[-1].strip()
                if in_mysql_section:
                    watermark['mysql_status'] = status_value
                elif in_redshift_section:
                    watermark['redshift_status'] = status_value
            elif 'Rows Extracted:' in line:
                try:
                    watermark['mysql_rows_extracted'] = int(line.split(':')[-1].strip().replace(',', ''))
                except:
                    pass
            elif 'Rows Loaded:' in line:
                try:
                    watermark['redshift_rows_loaded'] = int(line.split(':')[-1].strip().replace(',', ''))
                except:
                    pass
            elif 'Last Data Timestamp:' in line:
                try:
                    timestamp_part = line.split(':', 1)[-1].strip()
                    if timestamp_part and timestamp_part != 'None':
                        watermark['last_mysql_data_timestamp'] = timestamp_part
                except:
                    pass
            elif 'Last Extraction Time:' in line:
                try:
                    timestamp_part = line.split(':', 1)[-1].strip()
                    if timestamp_part and timestamp_part != 'None':
                        watermark['last_mysql_extraction_time'] = timestamp_part
                except:
                    pass
            elif 'Will start from:' in line:
                try:
                    timestamp_part = line.split(':', 1)[-1].strip()
                    if timestamp_part and not watermark['last_mysql_data_timestamp']:
                        watermark['last_mysql_data_timestamp'] = timestamp_part
                except:
                    pass
        
        return watermark
    
    def setup_initial_watermark(self):
        """Setup initial watermark if specified"""
        if self.start_timestamp:
            self.log(f"Setting initial watermark to: {self.start_timestamp}")
            
            cmd = [
                'python', '-m', 'src.cli.main', 'watermark', 'set',
                '-t', self.table_name,
                '--timestamp', self.start_timestamp
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log("Initial watermark set successfully", "success")
            else:
                self.log(f"Failed to set initial watermark: {result.stderr}", "error")
                raise RuntimeError("Failed to setup initial watermark")
    
    def run_sync_chunk(self, mode: str = None) -> tuple[bool, bool]:
        """Run a single sync chunk with specified mode
        
        Returns:
            (success, has_more_data) - success indicates if chunk completed,
            has_more_data indicates if there's more data to process
        """
        actual_mode = mode or self.mode
        chunk_start_time = datetime.now()
        
        cmd = [
            'python', '-m', 'src.cli.main', 'sync',
            '-t', self.table_name,
            '--limit', str(self.chunk_size)
        ]
        
        # Add mode-specific flags
        if actual_mode == 'backup-only':
            cmd.append('--backup-only')
        elif actual_mode == 'redshift-only':
            cmd.append('--redshift-only')
        # 'full' mode uses no additional flags
        
        self.log(f"🔄 Running chunk {self.chunks_processed + 1} ({actual_mode} mode):")
        self.log(f"   Command: {' '.join(cmd)}")
        self.log(f"   Expected rows: up to {self.chunk_size:,}")
        self.log(f"   Timeout: {self.timeout_seconds}s")
        
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=self.timeout_seconds
            )
            
            chunk_duration = datetime.now() - chunk_start_time
            
            if result.returncode == 0:
                self.chunks_processed += 1
                
                # Check if sync indicates no more data
                has_more_data = True
                if "No new data to backup" in result.stdout or \
                   "0 rows processed" in result.stdout or \
                   "No more data available" in result.stdout:
                    has_more_data = False
                    self.log("📋 Sync indicates no more data available", "info")
                
                # Try to extract row count from output
                rows_in_chunk = 0
                s3_files_created = 0
                if "rows processed" in result.stdout:
                    try:
                        import re
                        # Extract row count
                        matches = re.findall(r'(\d+)\s+rows processed', result.stdout)
                        if matches:
                            rows_in_chunk = int(matches[-1])
                            self.total_rows_processed += rows_in_chunk
                            
                            # If we got fewer rows than chunk size, likely no more data
                            if rows_in_chunk < self.chunk_size:
                                has_more_data = False
                        
                        # Extract S3 file information
                        s3_matches = re.findall(r'Created S3 file|Uploaded.*\.parquet', result.stdout)
                        s3_files_created = len(s3_matches)
                        
                    except Exception as parse_error:
                        self.log(f"Warning: Could not parse sync output: {parse_error}", "warning")
                
                # Enhanced success logging
                rate_per_min = (rows_in_chunk / chunk_duration.total_seconds() * 60) if chunk_duration.total_seconds() > 0 else 0
                
                self.log(f"✅ Chunk {self.chunks_processed} completed successfully:", "success")
                self.log(f"   📊 Rows processed: {rows_in_chunk:,}")
                self.log(f"   📁 S3 files created: {s3_files_created}")
                self.log(f"   ⏱️  Duration: {chunk_duration}")
                self.log(f"   🚀 Rate: {rate_per_min:,.0f} rows/minute")
                self.log(f"   📈 Total processed: {self.total_rows_processed:,}")
                self.log(f"   🔄 More data available: {'Yes' if has_more_data else 'No'}")
                
                # Log detailed output in debug mode or verbose mode
                if self.log_level == 'debug' or self.verbose_output:
                    self.log("🔍 Full sync output:", "debug")
                    for line in result.stdout.split('\n'):
                        if line.strip():
                            self.log(f"     {line.strip()}", "debug")
                
                return True, has_more_data
            else:
                self.log(f"❌ Chunk {self.chunks_processed + 1} failed:", "error")
                self.log(f"   ⏱️  Duration: {chunk_duration}")
                self.log(f"   🔍 Error output: {result.stderr}", "error")
                
                # Log stdout for additional context
                if result.stdout:
                    self.log(f"   📋 Additional output: {result.stdout}", "warning")
                
                return False, True  # Failed, but assume more data exists
                
        except subprocess.TimeoutExpired:
            chunk_duration = datetime.now() - chunk_start_time
            self.log(f"⏰ Chunk {self.chunks_processed + 1} timed out:", "error")
            self.log(f"   ⏱️  Duration: {chunk_duration}")
            self.log(f"   🚨 Timeout after: {self.timeout_seconds}s")
            return False, True
        except Exception as e:
            chunk_duration = datetime.now() - chunk_start_time
            self.log(f"💥 Unexpected error in chunk {self.chunks_processed + 1}:", "error")
            self.log(f"   ⏱️  Duration: {chunk_duration}")
            self.log(f"   🔍 Error: {e}", "error")
            return False, True
    
    def monitor_progress(self):
        """Monitor and log current progress with enhanced metrics"""
        watermark = self.get_current_watermark()
        elapsed_time = datetime.now() - self.start_time
        
        self.log(f"📊 === PROGRESS UPDATE ===")
        self.log(f"   ⏰ Session Time: {elapsed_time}")
        self.log(f"   🔢 Chunks Processed: {self.chunks_processed}")
        self.log(f"   📈 Script Total Rows: {self.total_rows_processed:,}")
        
        if watermark:
            mysql_rows = watermark.get('mysql_rows_extracted', 0)
            redshift_rows = watermark.get('redshift_rows_loaded', 0)
            mysql_status = watermark.get('mysql_status', 'unknown')
            redshift_status = watermark.get('redshift_status', 'unknown')
            last_mysql_timestamp = watermark.get('last_mysql_data_timestamp', 'None')
            last_extraction_time = watermark.get('last_mysql_extraction_time', 'None')
            
            self.log(f"   🗄️  MySQL Status: {mysql_status}")
            self.log(f"   📊 MySQL Rows: {mysql_rows:,}")
            self.log(f"   📅 Last Data Timestamp: {last_mysql_timestamp}")
            self.log(f"   🕐 Last Extraction: {last_extraction_time}")
            self.log(f"   🏭 Redshift Status: {redshift_status}")
            self.log(f"   📊 Redshift Rows: {redshift_rows:,}")
            
            # Performance metrics
            if mysql_rows > 0 and elapsed_time.total_seconds() > 0:
                watermark_rate = mysql_rows / (elapsed_time.total_seconds() / 60)
                self.log(f"   🚀 Watermark Rate: {watermark_rate:,.0f} rows/minute")
            
            if self.total_rows_processed > 0 and elapsed_time.total_seconds() > 0:
                script_rate = self.total_rows_processed / (elapsed_time.total_seconds() / 60)
                self.log(f"   🏃 Script Rate: {script_rate:,.0f} rows/minute")
            
            # Estimate completion time if we have data
            if self.chunks_processed > 0 and mysql_rows > 0:
                avg_chunk_time = elapsed_time.total_seconds() / self.chunks_processed
                if mysql_rows < self.total_rows_processed * 2:  # Rough estimate
                    remaining_estimate = "Nearing completion"
                else:
                    remaining_estimate = f"~{avg_chunk_time:.0f}s per chunk average"
                self.log(f"   ⏳ Timing: {remaining_estimate}")
            
            # Data consistency check
            if self.total_rows_processed > mysql_rows:
                self.log(f"   ⚠️  Note: Script count ({self.total_rows_processed:,}) > Watermark ({mysql_rows:,})", "warning")
            
        else:
            self.log("   ❌ Could not retrieve watermark status", "warning")
        
        self.log(f"   ===============================")
    
    def detect_completion(self) -> bool:
        """Detect if backup is complete"""
        watermark = self.get_current_watermark()
        
        if not watermark:
            return False
        
        mysql_status = watermark.get('mysql_status', '')
        mysql_rows = watermark.get('mysql_rows_extracted', 0)
        
        # Check for completion indicators
        # NOTE: We should NOT stop just because last chunk was successful
        # We need to check if there's actually no more data to process
        
        # For backup-only mode, only complete if last sync returned 0 rows
        # This requires checking the actual sync result, not just watermark status
        if self.mode == 'backup-only':
            # Don't auto-complete - let the sync command determine when done
            return False
            
        # For other modes, check Redshift status too
        if mysql_status == 'success' and mysql_rows > 0:
            redshift_status = watermark.get('redshift_status', '')
            if redshift_status == 'success':
                return True
        
        return False
    
    def progressive_backup_phase(self):
        """Execute progressive backup phase (MySQL → S3)"""
        self.log(f"🚀 === STARTING PROGRESSIVE BACKUP PHASE ===")
        self.log(f"   📋 Table: {self.table_name}")
        self.log(f"   📦 Chunk Size: {self.chunk_size:,} rows")
        self.log(f"   🔢 Max Chunks: {self.max_chunks or 'unlimited'}")
        self.log(f"   ⏰ Timeout per chunk: {self.timeout_seconds}s")
        self.log(f"   📊 Monitor interval: every 5 chunks")
        self.log(f"   ===============================================")
        
        consecutive_failures = 0
        max_failures = 3
        last_progress_update = datetime.now()
        
        while not self.interrupted:
            # Check max chunks limit
            if self.max_chunks and self.chunks_processed >= self.max_chunks:
                self.log(f"🏁 Reached maximum chunks limit: {self.max_chunks}", "info")
                break
            
            # Progress update every 5 chunks or every 10 minutes
            time_since_update = datetime.now() - last_progress_update
            if (self.chunks_processed > 0 and self.chunks_processed % 5 == 0) or \
               time_since_update.total_seconds() > 600:  # 10 minutes
                self.monitor_progress()
                last_progress_update = datetime.now()
            
            # Run backup chunk
            success, has_more_data = self.run_sync_chunk('backup-only')
            
            if success:
                consecutive_failures = 0
                
                # Check if there's no more data to process
                if not has_more_data:
                    self.log("🎉 Backup phase completed - no more data to process", "success")
                    break
                    
            else:
                consecutive_failures += 1
                self.log(f"⚠️  Consecutive failures: {consecutive_failures}/{max_failures}", "warning")
                
                if consecutive_failures >= max_failures:
                    self.log(f"🛑 Aborting after {max_failures} consecutive failures", "error")
                    return False
            
            # Progress pause between chunks
            if not self.interrupted:
                pause_time = 60 if success else 300  # 1 min success, 5 min failure
                self.log(f"⏸️  Pausing {pause_time} seconds before next chunk...")
                
                # Show countdown for longer pauses
                if pause_time >= 60:
                    for remaining in range(pause_time, 0, -60):
                        if self.interrupted:
                            break
                        if remaining > 60:
                            self.log(f"   ⏳ {remaining//60} minutes remaining...")
                        time.sleep(min(60, remaining))
                else:
                    time.sleep(pause_time)
        
        # Final summary
        if not self.interrupted:
            self.log(f"✅ Progressive backup phase completed successfully")
            self.log(f"   📊 Total chunks processed: {self.chunks_processed}")
            self.log(f"   📈 Total rows in session: {self.total_rows_processed:,}")
        
        return not self.interrupted
    
    def bulk_redshift_phase(self):
        """Execute bulk Redshift loading phase (S3 → Redshift)"""
        self.log(f"📊 Starting Bulk Redshift Loading Phase")
        
        # For Redshift loading, we typically do it in one operation
        success, _ = self.run_sync_chunk('redshift-only')
        
        if success:
            self.log("Bulk Redshift loading completed successfully", "success")
            return True
        else:
            self.log("Bulk Redshift loading failed", "error")
            return False
    
    def full_progressive_sync(self):
        """Execute complete progressive sync (backup + redshift)"""
        self.log(f"🔄 Starting Full Progressive Sync")
        
        # Phase 1: Progressive backup
        backup_success = self.progressive_backup_phase()
        
        if not backup_success:
            self.log("Progressive backup phase failed", "error")
            return False
        
        if self.interrupted:
            self.log("Sync interrupted during backup phase", "warning")
            return False
        
        # Phase 2: Bulk Redshift load
        self.log("Transitioning to Redshift loading phase...")
        time.sleep(5)  # Brief pause between phases
        
        redshift_success = self.bulk_redshift_phase()
        
        return redshift_success
    
    def execute(self):
        """Main execution method"""
        try:
            self.log(f"🎯 Starting Flexible Progressive Backup")
            self.log(f"   Session ID: {self.session_id}")
            self.log(f"   Table: {self.table_name}")
            self.log(f"   Mode: {self.mode}")
            self.log(f"   Chunk Size: {self.chunk_size:,}")
            self.log(f"   Log File: {self.log_file}")
            
            # Setup initial watermark if specified
            if self.start_timestamp:
                self.setup_initial_watermark()
            
            # Initial watermark check
            self.log("📋 Initial Watermark Status:")
            self.monitor_progress()
            
            # Execute based on mode
            if self.mode == 'backup-only':
                success = self.progressive_backup_phase()
            elif self.mode == 'redshift-only':
                success = self.bulk_redshift_phase()
            elif self.mode == 'full':
                success = self.full_progressive_sync()
            else:
                raise ValueError(f"Unknown mode: {self.mode}")
            
            # Final status
            self.log("📊 Final Status:")
            self.monitor_progress()
            
            elapsed_time = datetime.now() - self.start_time
            self.log(f"⏱️  Total Execution Time: {elapsed_time}")
            self.log(f"📈 Total Chunks Processed: {self.chunks_processed}")
            
            if success:
                self.log("🎉 Progressive backup completed successfully!", "success")
                return True
            else:
                self.log("❌ Progressive backup failed", "error")
                return False
                
        except KeyboardInterrupt:
            self.log("Received interrupt signal, shutting down gracefully...", "warning")
            return False
        except Exception as e:
            self.log(f"Unexpected error: {e}", "error")
            return False
        finally:
            self.log(f"📄 Complete log saved to: {self.log_file}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Flexible Progressive Backup Tool for Massive Tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic progressive backup
  python %(prog)s settlement.massive_table

  # Custom chunk size and backup-only mode
  python %(prog)s settlement.huge_table --chunk-size 10000000 --mode backup-only

  # Resume from specific timestamp
  python %(prog)s settlement.big_table --start-timestamp "2025-01-01 00:00:00"

  # Full pipeline with custom monitoring
  python %(prog)s settlement.enormous_table --mode full --monitor-interval 300 --max-chunks 20

  # Redshift-only loading of accumulated S3 data
  python %(prog)s settlement.massive_table --mode redshift-only
        """
    )
    
    # Required arguments
    parser.add_argument(
        'table_name',
        help='Table name to backup (e.g., settlement.massive_table)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=5000000,
        help='Number of rows per chunk (default: 5,000,000)'
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'backup-only', 'redshift-only'],
        default='full',
        help='Backup mode (default: full)'
    )
    
    parser.add_argument(
        '--start-timestamp',
        help='Starting timestamp for fresh sync (format: "YYYY-MM-DD HH:MM:SS")'
    )
    
    parser.add_argument(
        '--max-chunks',
        type=int,
        help='Maximum number of chunks to process (unlimited if not specified)'
    )
    
    parser.add_argument(
        '--timeout-minutes',
        type=int,
        default=120,
        help='Timeout per chunk in minutes (default: 120)'
    )
    
    parser.add_argument(
        '--monitor-interval',
        type=int,
        default=300,
        help='Progress monitoring interval in seconds (default: 300)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['debug', 'info', 'warning', 'error'],
        default='info',
        help='Logging level (default: info)'
    )
    
    parser.add_argument(
        '--verbose-output',
        action='store_true',
        help='Show full sync command output for each chunk'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be executed without running'
    )
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - Commands that would be executed:")
        print(f"   Table: {args.table_name}")
        print(f"   Chunk Size: {args.chunk_size:,}")
        print(f"   Mode: {args.mode}")
        print(f"   Start Timestamp: {args.start_timestamp or 'Not specified'}")
        print(f"   Max Chunks: {args.max_chunks or 'Unlimited'}")
        print("   Commands:")
        if args.mode in ['full', 'backup-only']:
            print(f"     python -m src.cli.main sync -t {args.table_name} --limit {args.chunk_size} --backup-only")
        if args.mode in ['full', 'redshift-only']:
            print(f"     python -m src.cli.main sync -t {args.table_name} --redshift-only")
        return
    
    try:
        # Create and execute progressive backup manager
        manager = ProgressiveBackupManager(
            table_name=args.table_name,
            chunk_size=args.chunk_size,
            mode=args.mode,
            start_timestamp=args.start_timestamp,
            max_chunks=args.max_chunks,
            timeout_minutes=args.timeout_minutes,
            monitor_interval=args.monitor_interval,
            log_level=args.log_level,
            verbose_output=args.verbose_output
        )
        
        success = manager.execute()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()