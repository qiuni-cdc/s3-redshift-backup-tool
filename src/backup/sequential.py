"""
Sequential backup strategy implementation.

This module implements the sequential backup strategy where tables are processed
one by one in sequence using row-based chunking (timestamp + ID pagination).
This is the most reliable strategy for consistent backups with exact row counts.
"""

from typing import List, Dict, Any, Optional
import time
from datetime import datetime

from src.backup.base import BaseBackupStrategy
from src.backup.row_based import RowBasedBackupStrategy
from src.utils.exceptions import BackupError, DatabaseError, raise_backup_error
from src.utils.logging import get_backup_logger


class SequentialBackupStrategy(BaseBackupStrategy):
    """
    Sequential backup strategy implementation using row-based chunking.
    
    Processes tables one by one in sequence, ensuring consistent state
    and reliable error handling. Uses timestamp + ID pagination for
    exact row count control and perfect resume capability.
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.strategy_name = "sequential"
        self.logger.set_context(strategy="sequential", chunking_type="row_based")
    
    def check_memory_usage(self, batch_number: int) -> bool:
        """Delegate to memory manager"""
        return self.memory_manager.check_memory_usage(batch_number)
    
    def force_gc_if_needed(self, batch_number: int):
        """Delegate to memory manager"""
        return self.memory_manager.force_gc_if_needed(batch_number)
    
    def execute(self, tables: List[str], chunk_size: Optional[int] = None, max_total_rows: Optional[int] = None, limit: Optional[int] = None, source_connection: Optional[str] = None) -> bool:
        """
        Execute sequential backup for all specified tables using row-based chunking.
        
        Args:
            tables: List of table names to backup
            chunk_size: Optional row limit per chunk (overrides config)
            max_total_rows: Optional maximum total rows to process  
            limit: Deprecated - use chunk_size instead (for backward compatibility)
            source_connection: Optional connection name to use instead of default
        
        Returns:
            True if all tables backed up successfully, False otherwise
        """
        if not tables:
            self.logger.logger.warning("No tables specified for backup")
            return False
        
        self.logger.backup_started("sequential", tables)
        self.metrics.start_operation()
        
        try:
            # Get backup time window
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Process each table sequentially
            successful_tables = []
            failed_tables = []
            
            with self.database_session(source_connection) as db_conn:
                for i, table_name in enumerate(tables):
                    table_start_time = time.time()
                    
                    self.logger.logger.info(
                        f"Processing table {i+1}/{len(tables)}",
                        table_name=table_name,
                        progress=f"{i+1}/{len(tables)}"
                    )
                    
                    try:
                        # Use row-based chunking strategy (timestamp + ID pagination)
                        self.logger.logger.info(
                            "Using row-based chunking strategy (timestamp + ID pagination)",
                            table_name=table_name,
                            target_rows_per_chunk=self.config.backup.target_rows_per_chunk
                        )
                        # Handle backward compatibility with old 'limit' parameter
                        effective_chunk_size = chunk_size or limit
                        
                        success = self._process_single_table_row_based(
                            db_conn, table_name, current_timestamp, effective_chunk_size, max_total_rows, source_connection
                        )
                        
                        table_duration = time.time() - table_start_time
                        
                        if success:
                            successful_tables.append(table_name)
                            self.metrics.tables_processed += 1
                            
                            self.logger.table_completed(
                                table_name,
                                self.metrics.per_table_metrics.get(table_name, {}).get('rows', 0),
                                self.metrics.per_table_metrics.get(table_name, {}).get('batches', 0),
                                table_duration
                            )
                        else:
                            failed_tables.append(table_name)
                            self.metrics.errors += 1
                            self.logger.logger.error(
                                "Table backup failed",
                                table_name=table_name,
                                duration=table_duration
                            )
                    
                    except Exception as e:
                        failed_tables.append(table_name)
                        self.metrics.errors += 1
                        self.logger.error_occurred(e, f"table_backup_{table_name}")
                        
                        # Continue with next table unless it's a critical error
                        if isinstance(e, DatabaseError) and "connection" in str(e).lower():
                            self.logger.logger.error("Database connection lost, stopping backup")
                            break
            
            # Watermarks are updated per table by row-based strategy
            watermark_updated = len(successful_tables) > 0
            
            # Calculate final results
            all_success = len(failed_tables) == 0
            partial_success = len(successful_tables) > 0
            
            self.metrics.end_operation()
            
            # Log final results
            self.logger.backup_completed(
                "sequential", 
                all_success, 
                self.metrics.get_duration(),
                successful_tables=successful_tables,
                failed_tables=failed_tables,
                watermark_updated=watermark_updated
            )
            
            # Log summary with statistics
            summary = self.get_backup_summary()
            self.logger.logger.info(
                "Sequential backup summary",
                **summary,
                successful_tables=len(successful_tables),
                failed_tables=len(failed_tables),
                chunking_strategy="row_based"
            )
            
            return all_success
            
        except Exception as e:
            self.metrics.end_operation()
            self.metrics.errors += 1
            self.logger.error_occurred(e, "sequential_backup")
            self.logger.backup_completed("sequential", False, self.metrics.get_duration())
            raise_backup_error("sequential_backup", underlying_error=e)
        
        finally:
            self.cleanup_resources()
    
    def _process_single_table_row_based(
        self, 
        db_conn, 
        table_name: str, 
        current_timestamp: str,
        chunk_size: Optional[int] = None,
        max_total_rows: Optional[int] = None,
        source_connection: Optional[str] = None
    ) -> bool:
        """
        Process a single table using row-based chunking (timestamp + ID pagination).
        
        Args:
            db_conn: Database connection
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            chunk_size: Optional row limit per chunk
            max_total_rows: Optional maximum total rows to process
            source_connection: Optional connection name (for logging and context)
            
        Returns:
            True if table processed successfully
        """
        # Delegate to the specialized row-based strategy
        try:
            row_based_strategy = RowBasedBackupStrategy(self.config)
            
            # P1 FIX: Transfer necessary components but create new memory manager instance
            # to avoid shared state pollution between strategies
            row_based_strategy.logger = self.logger
            row_based_strategy.watermark_manager = self.watermark_manager
            
            # P1 FIX: Create isolated memory manager for row-based strategy
            from src.backup.base import MemoryConfig, MemoryManager
            memory_config = MemoryConfig.from_app_config(self.config)
            row_based_strategy.memory_manager = MemoryManager(
                memory_config=memory_config,
                logger=self.logger.logger  # Share logger but isolate memory state
            )
            
            row_based_strategy.s3_manager = self.s3_manager
            row_based_strategy.process_batch = self.process_batch
            # Use row-based strategy's own validate_table_exists (with CDC support)
            # row_based_strategy.validate_table_exists = self.validate_table_exists  # REMOVED
            row_based_strategy.update_watermarks = self.update_watermarks
            row_based_strategy.database_session = self.database_session
            
            # Pass pipeline configuration for CDC integration
            if hasattr(self, 'pipeline_config'):
                row_based_strategy.pipeline_config = self.pipeline_config
            
            # Use the row-based strategy to process this single table
            success = row_based_strategy._process_single_table_row_based(
                db_conn, table_name, current_timestamp, chunk_size, max_total_rows, source_connection
            )
            
            # CRITICAL FIX: Transfer metrics back from row-based strategy to sequential strategy
            if success:
                # Get the metrics from the row-based strategy that did the actual work
                row_based_summary = row_based_strategy.get_backup_summary()
                row_based_table_metrics = row_based_summary.get('per_table_metrics', {}).get(table_name, {})
                
                # Transfer table-specific metrics to sequential strategy
                if row_based_table_metrics:
                    table_rows = row_based_table_metrics.get('rows', 0)
                    table_batches = row_based_table_metrics.get('batches', 0)
                    table_duration = row_based_table_metrics.get('duration', 0.0)
                    table_bytes = row_based_table_metrics.get('bytes', 0)
                    
                    # Add to sequential strategy's metrics
                    self.metrics.add_table_metrics(
                        table_name, table_rows, table_batches, table_duration, table_bytes
                    )
                    
                    self.logger.logger.info(
                        "Metrics transferred from row-based to sequential strategy",
                        table_name=table_name,
                        transferred_rows=table_rows,
                        transferred_batches=table_batches,
                        transferred_bytes=table_bytes
                    )
                else:
                    # Fallback: extract from row-based strategy metrics directly
                    self.metrics.total_rows += row_based_strategy.metrics.total_rows
                    self.metrics.total_batches += row_based_strategy.metrics.total_batches
                    self.metrics.total_bytes += row_based_strategy.metrics.total_bytes
                    
                    self.logger.logger.info(
                        "Metrics transferred (fallback method) from row-based to sequential strategy",
                        table_name=table_name,
                        transferred_rows=row_based_strategy.metrics.total_rows,
                        transferred_batches=row_based_strategy.metrics.total_batches
                    )
            
            return success
            
        except Exception as e:
            self.logger.error_occurred(e, f"row_based_processing_{table_name}")
            return False
    
    def _process_batch_with_retries(
        self, 
        batch_data: List[Dict], 
        table_name: str, 
        batch_id: int, 
        current_timestamp: str
    ) -> bool:
        """
        Process a batch with retry logic.
        
        Args:
            batch_data: Batch data to process
            table_name: Name of the table
            batch_id: Batch identifier
            current_timestamp: Current backup timestamp
        
        Returns:
            True if batch processed successfully
        """
        max_attempts = self.config.backup.retry_attempts
        
        for attempt in range(1, max_attempts + 1):
            try:
                success = self.process_batch(
                    batch_data, table_name, batch_id, current_timestamp
                )
                
                if success:
                    return True
                else:
                    if attempt < max_attempts:
                        self.logger.retry_attempt(
                            f"batch_processing_{table_name}",
                            attempt,
                            max_attempts
                        )
                        time.sleep(2 ** (attempt - 1))  # Exponential backoff
                    
            except Exception as e:
                if attempt < max_attempts:
                    self.logger.retry_attempt(
                        f"batch_processing_{table_name}",
                        attempt,
                        max_attempts,
                        error=e
                    )
                    time.sleep(2 ** (attempt - 1))  # Exponential backoff
                else:
                    self.logger.error_occurred(e, f"batch_retry_exhausted_{table_name}")
                    return False
        
        return False
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get information about this backup strategy"""
        return {
            "name": "Sequential Backup Strategy (Row-Based)",
            "description": "Processes tables one by one using timestamp + ID pagination for exact row counts",
            "advantages": [
                "Maximum data consistency",
                "Predictable resource usage", 
                "Simple error handling",
                "Exact row count control",
                "Perfect resume capability",
                "No dependency on data patterns"
            ],
            "disadvantages": [
                "Slower for multiple tables",
                "No parallelization benefits"
            ],
            "best_for": [
                "Small to medium number of tables",
                "When data consistency is critical",
                "Limited system resources",
                "Large tables requiring exact chunk sizes",
                "Tables with mixed data density patterns"
            ],
            "configuration": {
                "batch_size": self.config.backup.batch_size,
                "retry_attempts": self.config.backup.retry_attempts,
                "timeout_seconds": self.config.backup.timeout_seconds,
                "target_rows_per_chunk": self.config.backup.target_rows_per_chunk,
                "chunking_strategy": "row_based"
            }
        }
    
    def estimate_completion_time(self, tables: List[str]) -> Dict[str, Any]:
        """
        Estimate completion time for sequential backup.
        
        Args:
            tables: List of tables to backup
        
        Returns:
            Dictionary with time estimates
        """
        # Row-based chunking provides more predictable estimates
        estimated_rows_per_table = 10000  # Default estimate
        processing_rate = 5000  # rows per second (from test results)
        
        total_estimated_rows = len(tables) * estimated_rows_per_table
        estimated_seconds = total_estimated_rows / processing_rate
        
        return {
            "total_tables": len(tables),
            "estimated_total_rows": total_estimated_rows,
            "estimated_duration_seconds": estimated_seconds,
            "estimated_duration_minutes": round(estimated_seconds / 60, 1),
            "processing_rate_rows_per_second": processing_rate,
            "strategy": "sequential",
            "chunking_strategy": "row_based",
            "predictability": "high"  # Row-based chunking offers high predictability
        }