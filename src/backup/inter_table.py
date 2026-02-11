"""
Inter-table parallel backup strategy implementation.

This module implements parallel backup processing where multiple tables
are processed simultaneously using thread pools. Best for scenarios with
many tables and sufficient system resources.
"""

from typing import List, Dict, Any, Tuple, Optional
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import threading
from collections import defaultdict

from src.backup.base import BaseBackupStrategy
from src.utils.exceptions import BackupError, DatabaseError, raise_backup_error
from src.utils.logging import get_backup_logger


class InterTableBackupStrategy(BaseBackupStrategy):
    """
    Inter-table parallel backup strategy implementation.
    
    Processes multiple tables simultaneously using thread pools.
    Each table gets its own database connection and processing thread.
    Best for scenarios with many small to medium tables.
    """
    
    def __init__(self, config, pipeline_config=None):
        super().__init__(config, pipeline_config)
        self.strategy_name = "inter-table"
        self.logger.set_context(strategy="inter_table_parallel", gemini_mode=True)
        self._thread_local = threading.local()
        self._results_lock = threading.Lock()
        self._table_results = {}
    
    def execute(self, tables: List[str], chunk_size: Optional[int] = None, max_total_rows: Optional[int] = None, limit: Optional[int] = None, source_connection: Optional[str] = None, initial_lookback_minutes: Optional[int] = None, end_time: Optional[str] = None) -> bool:
        """
        Execute inter-table parallel backup for all specified tables.

        Args:
            tables: List of table names to backup
            chunk_size: Optional row limit per chunk (overrides config)
            max_total_rows: Optional maximum total rows to process
            limit: Deprecated - use chunk_size instead (for backward compatibility)
            source_connection: Optional connection name to use instead of default
            initial_lookback_minutes: Optional minutes to look back (default: None)

        Returns:
            True if all tables backed up successfully, False otherwise
        """
        if not tables:
            self.logger.logger.warning("No tables specified for backup")
            return False

        # Handle backward compatibility with old 'limit' parameter
        effective_chunk_size = chunk_size or limit
        effective_max_total_rows = max_total_rows
        
        if len(tables) == 1:
            self.logger.logger.info("Single table provided, consider using sequential strategy")
        
        max_workers = min(self.config.backup.max_workers, len(tables))
        
        self.logger.backup_started("inter_table_parallel", tables, max_workers=max_workers)
        self.metrics.start_operation()
        
        try:
            # Get backup time window
            if end_time:
                current_timestamp = end_time
                self.logger.logger.info(f"Using provided end time: {current_timestamp}")
            else:
                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.logger.info(f"Using system time as end time: {current_timestamp}")
            
            # Initialize results tracking
            self._table_results = {}
            successful_tables = []
            failed_tables = []
            
            # Execute parallel processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all table processing tasks
                future_to_table = {
                    executor.submit(
                        self._process_table_thread,
                        table_name,
                        current_timestamp,
                        i + 1,
                        len(tables),
                        effective_chunk_size,
                        effective_max_total_rows,
                        source_connection,
                        initial_lookback_minutes
                    ): table_name
                    for i, table_name in enumerate(tables)
                }
                
                # Monitor progress and collect results
                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    
                    try:
                        success = future.result(timeout=self.config.backup.timeout_seconds)
                        
                        with self._results_lock:
                            self._table_results[table_name] = success
                        
                        if success:
                            successful_tables.append(table_name)
                            self.metrics.tables_processed += 1
                            
                            self.logger.logger.info(
                                "Table completed successfully",
                                table_name=table_name,
                                completed_tables=len(successful_tables) + len(failed_tables),
                                total_tables=len(tables)
                            )
                        else:
                            failed_tables.append(table_name)
                            self.metrics.errors += 1
                            
                            self.logger.logger.error(
                                "Table processing failed",
                                table_name=table_name,
                                completed_tables=len(successful_tables) + len(failed_tables),
                                total_tables=len(tables)
                            )
                    
                    except Exception as e:
                        failed_tables.append(table_name)
                        self.metrics.errors += 1
                        self.logger.error_occurred(e, f"parallel_table_processing_{table_name}")
            
            # Update watermarks for successful tables
            watermark_updated = False
            if successful_tables:
                try:
                    all_watermarks_updated = True
                    extraction_time = datetime.now()
                    for table_name in successful_tables:
                        table_metrics = self.metrics.per_table_metrics.get(table_name, {})
                        rows_extracted = table_metrics.get('rows', 0)

                        success = self.update_watermarks(
                            table_name=table_name,
                            extraction_time=extraction_time,
                            max_data_timestamp=current_timestamp,
                            rows_extracted=rows_extracted,
                            status='success'
                        )
                        if not success:
                            all_watermarks_updated = False

                    watermark_updated = all_watermarks_updated
                except Exception as e:
                    self.logger.error_occurred(e, "watermark_update")
                    watermark_updated = False
            
            # Calculate final results
            all_success = len(failed_tables) == 0
            
            self.metrics.end_operation()
            
            # Log final results
            self.logger.backup_completed(
                "inter_table_parallel",
                all_success,
                self.metrics.get_duration(),
                successful_tables=successful_tables,
                failed_tables=failed_tables,
                watermark_updated=watermark_updated,
                parallel_workers=max_workers
            )
            
            # Log detailed summary
            summary = self.get_backup_summary()
            self.logger.logger.info(
                "Inter-table parallel backup summary",
                **summary,
                successful_tables=len(successful_tables),
                failed_tables=len(failed_tables),
                parallel_workers=max_workers,
                parallelization_efficiency=self._calculate_efficiency(tables)
            )
            
            return all_success
            
        except Exception as e:
            self.metrics.end_operation()
            self.metrics.errors += 1
            self.logger.error_occurred(e, "inter_table_parallel_backup")
            self.logger.backup_completed("inter_table_parallel", False, self.metrics.get_duration())
            raise_backup_error("inter_table_parallel_backup", underlying_error=e)
        
        finally:
            self.cleanup_resources()
    
    def _process_table_thread(
        self,
        table_name: str,
        current_timestamp: str,
        table_index: int,
        total_tables: int,
        chunk_size: Optional[int] = None,
        max_total_rows: Optional[int] = None,
        source_connection: Optional[str] = None,
        initial_lookback_minutes: Optional[int] = None
    ) -> bool:
        """
        Process a single table in a separate thread.

        Args:
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            table_index: Index of this table (for progress tracking)
            total_tables: Total number of tables being processed
            chunk_size: Optional chunk size for processing
            max_total_rows: Optional maximum total rows to process
            source_connection: Optional source connection name

        Returns:
            True if table processed successfully
        """
        thread_id = threading.get_ident()
        
        # Set thread-local context
        self.logger.set_context(
            thread_id=thread_id,
            table_name=table_name,
            table_index=table_index,
            total_tables=total_tables
        )
        
        try:
            self.logger.table_started(
                table_name,
                thread_id=thread_id,
                progress=f"{table_index}/{total_tables}"
            )
            
            table_start_time = time.time()
            
            # Create dedicated database session for this thread
            with self.database_session(source_connection) as db_conn:
                success = self._process_single_table_parallel(
                    db_conn, table_name, current_timestamp, thread_id, source_connection, chunk_size, initial_lookback_minutes
                )
                
                table_duration = time.time() - table_start_time
                
                if success:
                    self.logger.table_completed(
                        table_name,
                        self.metrics.per_table_metrics.get(table_name, {}).get('rows', 0),
                        self.metrics.per_table_metrics.get(table_name, {}).get('batches', 0),
                        table_duration,
                        thread_id=thread_id
                    )
                else:
                    self.logger.logger.error(
                        "Table processing failed",
                        table_name=table_name,
                        thread_id=thread_id,
                        duration=table_duration
                    )
                
                return success
        
        except Exception as e:
            self.logger.error_occurred(e, f"parallel_table_thread_{table_name}")
            return False
    
    def _process_single_table_parallel(
        self,
        db_conn,
        table_name: str,
        current_timestamp: str,
        thread_id: int,
        source_connection: Optional[str] = None,
        chunk_size: Optional[int] = None,
        initial_lookback_minutes: Optional[int] = None
    ) -> bool:
        """
        Process a single table with parallel-specific optimizations.

        Args:
            db_conn: Database connection for this thread
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            thread_id: Thread identifier
            source_connection: Optional connection name (for logging and context)
            chunk_size: Optional chunk size for processing

        Returns:
            True if table processed successfully
        """
        cursor = None
        try:
            # Create cursor with buffered=False (Unbuffered) to stream results and avoid OOM
            # Note: safe to use unbuffered now that compress=False (avoids driver bug)
            cursor = db_conn.cursor(dictionary=True, buffered=False)
            
            # Validate table structure
            if not self.validate_table_exists(cursor, table_name):
                self.logger.logger.error(
                    "Table validation failed",
                    table_name=table_name,
                    thread_id=thread_id
                )
                return False
            
            # Get last watermark for this specific table
            last_watermark = self.watermark_manager.get_last_watermark(table_name)

            # P1: Handle initial lookback override for first run
            # Check for empty/reset watermark: None, 0, small unix timestamps, or 1970-01-01 dates
            is_first_run = (
                not last_watermark or
                str(last_watermark).startswith('1970-01-01') or
                (isinstance(last_watermark, (int, float)) and last_watermark < 86400) or  # < 1 day in unix
                (isinstance(last_watermark, str) and last_watermark.isdigit() and int(last_watermark) < 86400)
            )

            if initial_lookback_minutes and is_first_run:
                try:
                    # Get timestamp format from pipeline config
                    timestamp_format = 'datetime'  # Default
                    if hasattr(self, 'pipeline_config') and self.pipeline_config:
                        tables_config = self.pipeline_config.get('tables', {})
                        for tbl_name, tbl_cfg in tables_config.items():
                            if tbl_name in table_name or table_name.endswith(tbl_name):
                                timestamp_format = tbl_cfg.get('timestamp_format', 'datetime')
                                break

                    if timestamp_format == 'unix':
                        # For unix timestamps, calculate lookback as unix timestamp
                        try:
                            # Parse provided timestamp string to unix
                            from datetime import datetime as dt
                            # Handle potential timezone info in string 
                            clean_ts = current_timestamp.replace('T', ' ').split('+')[0]
                            # Try with and without T separator formats
                            try:
                                current_dt = dt.strptime(clean_ts, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                current_dt = dt.strptime(current_timestamp, '%Y-%m-%dT%H:%M:%S')
                                
                            current_unix = int(current_dt.timestamp())
                        except (ValueError, TypeError) as e:
                            # Fallback if parsing fails (shouldn't happen with valid end_time)
                            import time
                            current_unix = int(time.time())
                            self.logger.logger.warning(f"Failed to parse current_timestamp '{current_timestamp}' for unix calculation: {e}, falling back to system time")

                        new_watermark = current_unix - (initial_lookback_minutes * 60)
                    else:
                        # For datetime format
                        try:
                            clean_ts = current_timestamp.replace('T', ' ').split('+')[0]
                            try:
                                current_dt = datetime.strptime(clean_ts, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                current_dt = datetime.strptime(current_timestamp, '%Y-%m-%dT%H:%M:%S')
                        except (ValueError, TypeError) as e:
                            self.logger.logger.warning(f"Failed to parse datetime '{current_timestamp}': {e}, using now()")
                            current_dt = datetime.now()
                            
                        looking_back_dt = current_dt - timedelta(minutes=initial_lookback_minutes)
                        new_watermark = looking_back_dt.strftime('%Y-%m-%d %H:%M:%S')

                    self.logger.logger.info(
                        f"First run detected: Using {initial_lookback_minutes}m lookback",
                        table_name=table_name,
                        original_watermark=last_watermark,
                        new_watermark=new_watermark,
                        timestamp_format=timestamp_format
                    )
                    last_watermark = new_watermark
                except Exception as e:
                    self.logger.logger.warning(f"Failed to calculate initial lookback: {e}")

            # Log backup window with actual values for debugging
            self.logger.logger.info(
                "Processing backup window",
                table_name=table_name,
                thread_id=thread_id,
                last_watermark=last_watermark,
                current_timestamp=current_timestamp
            )
            
            # Get estimated row count
            estimated_rows = self.get_table_row_count(
                cursor, table_name, last_watermark, current_timestamp
            )
            
            if estimated_rows == 0:
                self.logger.logger.info(
                    "No new data to backup",
                    table_name=table_name,
                    thread_id=thread_id
                )
                return True
            
            self.logger.logger.info(
                f"Processing {estimated_rows} estimated rows",
                table_name=table_name,
                thread_id=thread_id,
                estimated_rows=estimated_rows
            )
            
            # Execute incremental query with optional limit
            incremental_query = self.get_incremental_query(
                table_name, last_watermark, current_timestamp, limit=chunk_size
            )
            
            cursor.execute(incremental_query)
            
            # Process data in batches with parallel optimizations
            batch_id = 0
            total_rows_processed = 0
            
            while True:
                batch_start_time = time.time()
                
                # Use configured chunk size (default to 10000 if None)
                # This ensures we respect the batch_size configuration for S3 uploads
                fetch_size = chunk_size if chunk_size else 10000
                
                # Fetch batch
                batch_data = cursor.fetchmany(fetch_size)
                if not batch_data:
                    break
                
                batch_id += 1
                batch_size = len(batch_data)
                total_rows_processed += batch_size
                
                # Process batch with thread-safe operations
                batch_success = self._process_batch_parallel(
                    batch_data, table_name, batch_id, current_timestamp, thread_id
                )
                
                if not batch_success:
                    self.logger.logger.error(
                        "Batch processing failed",
                        table_name=table_name,
                        batch_id=batch_id,
                        thread_id=thread_id
                    )
                    return False
                
                # Log progress (less verbose for parallel processing)
                if batch_id % 10 == 0:  # Log every 10th batch
                    batch_duration = time.time() - batch_start_time
                    progress_pct = (total_rows_processed / max(estimated_rows, total_rows_processed)) * 100
                    
                    self.logger.logger.debug(
                        "Batch progress",
                        table_name=table_name,
                        batch_id=batch_id,
                        total_processed=total_rows_processed,
                        progress_percent=round(progress_pct, 1),
                        thread_id=thread_id
                    )
            
            # Record table metrics (thread-safe)
            table_duration = time.time() - (self.metrics.start_time or time.time())
            
            with self._results_lock:
                self.metrics.add_table_metrics(
                    table_name, total_rows_processed, batch_id, table_duration
                )
            
            self.logger.logger.info(
                "Table processing completed",
                table_name=table_name,
                thread_id=thread_id,
                total_rows=total_rows_processed,
                total_batches=batch_id,
                estimated_rows=estimated_rows
            )
            
            return True
            
        except Exception as e:
            self.logger.error_occurred(e, f"parallel_table_processing_{table_name}")
            return False
        
        finally:
            if cursor:
                try:
                    # Consume any unread results to prevent "Unread result found" error
                    while cursor.nextset():
                        pass
                except:
                    pass
                try:
                    cursor.close()
                except:
                    pass
    
    def _process_batch_parallel(
        self, 
        batch_data: List[Dict], 
        table_name: str, 
        batch_id: int, 
        current_timestamp: str,
        thread_id: int
    ) -> bool:
        """
        Process a batch with parallel-specific optimizations.
        
        Args:
            batch_data: Batch data to process
            table_name: Name of the table
            batch_id: Batch identifier
            current_timestamp: Current backup timestamp
            thread_id: Thread identifier
        
        Returns:
            True if batch processed successfully
        """
        try:
            # Use the base process_batch method with additional thread context
            success = self.process_batch(
                batch_data, table_name, batch_id, current_timestamp
            )
            
            if success:
                self.logger.logger.debug(
                    "Batch completed",
                    table_name=table_name,
                    batch_id=batch_id,
                    batch_size=len(batch_data),
                    thread_id=thread_id
                )
            
            return success
            
        except Exception as e:
            self.logger.error_occurred(e, f"parallel_batch_processing_{table_name}")
            return False
    
    def _calculate_efficiency(self, tables: List[str]) -> Dict[str, float]:
        """
        Calculate parallelization efficiency metrics.
        
        Args:
            tables: List of processed tables
        
        Returns:
            Dictionary with efficiency metrics
        """
        total_duration = self.metrics.get_duration()
        if total_duration == 0:
            return {"efficiency": 0.0, "theoretical_speedup": 1.0}
        
        # Calculate theoretical sequential time
        sequential_time = sum(
            self.metrics.per_table_metrics.get(table, {}).get('duration', 0)
            for table in tables
        )
        
        if sequential_time == 0:
            return {"efficiency": 0.0, "theoretical_speedup": 1.0}
        
        # Calculate efficiency
        theoretical_speedup = sequential_time / total_duration
        max_possible_speedup = min(len(tables), self.config.backup.max_workers)
        efficiency = (theoretical_speedup / max_possible_speedup) * 100
        
        return {
            "efficiency_percent": round(efficiency, 1),
            "theoretical_speedup": round(theoretical_speedup, 2),
            "max_possible_speedup": max_possible_speedup,
            "sequential_time": round(sequential_time, 2),
            "parallel_time": round(total_duration, 2)
        }
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get information about this backup strategy"""
        return {
            "name": "Inter-Table Parallel Backup Strategy",
            "description": "Processes multiple tables simultaneously using thread pools",
            "advantages": [
                "Faster processing for multiple tables",
                "Better resource utilization",
                "Parallel I/O operations",
                "Scalable with number of tables"
            ],
            "disadvantages": [
                "Higher resource usage",
                "More complex error handling",
                "Potential for connection exhaustion",
                "Database load spikes"
            ],
            "best_for": [
                "Many small to medium tables",
                "Systems with good I/O capacity",
                "When speed is prioritized",
                "Sufficient database connections available"
            ],
            "configuration": {
                "max_workers": self.config.backup.max_workers,
                "batch_size": self.config.backup.batch_size,
                "timeout_seconds": self.config.backup.timeout_seconds
            }
        }
    
    def estimate_completion_time(self, tables: List[str]) -> Dict[str, Any]:
        """
        Estimate completion time for inter-table parallel backup.
        
        Args:
            tables: List of tables to backup
        
        Returns:
            Dictionary with time estimates
        """
        estimated_rows_per_table = 10000  # Default estimate
        processing_rate = 5000  # rows per second
        
        # Calculate parallel processing estimates
        max_workers = min(self.config.backup.max_workers, len(tables))
        total_estimated_rows = len(tables) * estimated_rows_per_table
        
        # Estimate parallel speedup (assuming 70% efficiency)
        parallel_efficiency = 0.7
        theoretical_speedup = max_workers * parallel_efficiency
        
        sequential_time = total_estimated_rows / processing_rate
        parallel_time = sequential_time / theoretical_speedup
        
        return {
            "total_tables": len(tables),
            "max_workers": max_workers,
            "estimated_total_rows": total_estimated_rows,
            "sequential_duration_seconds": sequential_time,
            "parallel_duration_seconds": parallel_time,
            "parallel_duration_minutes": round(parallel_time / 60, 1),
            "estimated_speedup": round(theoretical_speedup, 1),
            "parallel_efficiency": parallel_efficiency,
            "strategy": "inter_table_parallel"
        }