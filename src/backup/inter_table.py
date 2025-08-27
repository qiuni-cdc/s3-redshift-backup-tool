"""
Inter-table parallel backup strategy implementation.

This module implements parallel backup processing where multiple tables
are processed simultaneously using thread pools. Best for scenarios with
many tables and sufficient system resources.
"""

from typing import List, Dict, Any, Tuple, Optional
import time
from datetime import datetime
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
    
    def __init__(self, config):
        super().__init__(config)
        self.logger.set_context(strategy="inter_table_parallel", gemini_mode=True)
        self._thread_local = threading.local()
        self._results_lock = threading.Lock()
        self._table_results = {}
    
    def execute(self, tables: List[str], chunk_size: Optional[int] = None, limit: Optional[int] = None) -> bool:
        """
        Execute inter-table parallel backup for all specified tables.
        
        Args:
            tables: List of table names to backup
            chunk_size: Optional row limit per chunk (overrides config)
            limit: Deprecated - use chunk_size instead (for backward compatibility)
        
        Returns:
            True if all tables backed up successfully, False otherwise
        """
        if not tables:
            self.logger.logger.warning("No tables specified for backup")
            return False
        
        # Handle backward compatibility with old 'limit' parameter
        effective_chunk_size = chunk_size or limit
        
        if len(tables) == 1:
            self.logger.logger.info("Single table provided, consider using sequential strategy")
        
        max_workers = min(self.config.backup.max_workers, len(tables))
        
        self.logger.backup_started("inter_table_parallel", tables, max_workers=max_workers)
        self.metrics.start_operation()
        
        try:
            # Get backup time window
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
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
                        max_total_rows
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
                    watermark_updated = self.update_watermarks(
                        current_timestamp, successful_tables, table_specific=False
                    )
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
        max_total_rows: Optional[int] = None
    ) -> bool:
        """
        Process a single table in a separate thread.
        
        Args:
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            table_index: Index of this table (for progress tracking)
            total_tables: Total number of tables being processed
        
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
            with self.database_session() as db_conn:
                success = self._process_single_table_parallel(
                    db_conn, table_name, current_timestamp, thread_id
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
        thread_id: int
    ) -> bool:
        """
        Process a single table with parallel-specific optimizations.
        
        Args:
            db_conn: Database connection for this thread
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            thread_id: Thread identifier
        
        Returns:
            True if table processed successfully
        """
        cursor = None
        try:
            # Create cursor with dictionary output
            cursor = db_conn.cursor(dictionary=True, buffered=False)
            
            # Validate table structure
            if not self.validate_table_exists(cursor, table_name):
                self.logger.logger.error(
                    "Table validation failed",
                    table_name=table_name,
                    thread_id=thread_id
                )
                return False
            
            # Get last watermark
            last_watermark = self.watermark_manager.get_last_watermark()
            
            # Log backup window
            self.logger.logger.debug(
                "Processing backup window",
                table_name=table_name,
                thread_id=thread_id,
                start_time=last_watermark,
                end_time=current_timestamp
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
                
                # Fetch batch
                batch_data = cursor.fetchmany(self.config.backup.batch_size)
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
                cursor.close()
    
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