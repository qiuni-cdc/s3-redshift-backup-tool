"""
Sequential backup strategy implementation.

This module implements the sequential backup strategy where tables are processed
one by one in sequence. This is the most reliable strategy for consistent backups
but may take longer for large numbers of tables.
"""

from typing import List, Dict, Any, Optional
import time
from datetime import datetime

from src.backup.base import BaseBackupStrategy
from src.utils.exceptions import BackupError, DatabaseError, raise_backup_error
from src.utils.logging import get_backup_logger


class SequentialBackupStrategy(BaseBackupStrategy):
    """
    Sequential backup strategy implementation.
    
    Processes tables one by one in sequence, ensuring consistent state
    and reliable error handling. Best for scenarios where data consistency
    is more important than processing speed.
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.logger.set_context(strategy="sequential", gemini_mode=True)
    
    def execute(self, tables: List[str], limit: Optional[int] = None) -> bool:
        """
        Execute sequential backup for all specified tables.
        
        Args:
            tables: List of table names to backup
        
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
            
            with self.database_session() as db_conn:
                for i, table_name in enumerate(tables):
                    table_start_time = time.time()
                    
                    self.logger.logger.info(
                        f"Processing table {i+1}/{len(tables)}",
                        table_name=table_name,
                        progress=f"{i+1}/{len(tables)}"
                    )
                    
                    try:
                        success = self._process_single_table(
                            db_conn, table_name, current_timestamp, limit
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
            
            # Watermarks are now updated per table, no global update needed
            watermark_updated = len(successful_tables) > 0  # True if any table succeeded
            
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
            
            # Log summary with Gemini statistics
            summary = self.get_backup_summary()
            self.logger.logger.info(
                "Sequential backup summary",
                **summary,
                successful_tables=len(successful_tables),
                failed_tables=len(failed_tables)
            )
            
            # Log Gemini mode statistics
            gemini_stats = summary.get('gemini_stats', {})
            if gemini_stats.get('total_batches_processed', 0) > 0:
                self.logger.logger.info(
                    "Gemini mode statistics",
                    gemini_success_rate=f"{gemini_stats.get('gemini_success_rate', 0)}%",
                    schemas_discovered=gemini_stats.get('schemas_discovered', 0),
                    total_batches=gemini_stats.get('total_batches_processed', 0),
                    strategy="sequential"
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
    
    def _process_single_table(
        self, 
        db_conn, 
        table_name: str, 
        current_timestamp: str,
        limit: Optional[int] = None
    ) -> bool:
        """
        Process a single table for backup.
        
        Args:
            db_conn: Database connection
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
        
        Returns:
            True if table processed successfully
        """
        cursor = None
        try:
            self.logger.table_started(table_name)
            
            # Create cursor with dictionary output
            cursor = db_conn.cursor(dictionary=True, buffered=False)
            
            # Validate table structure
            if not self.validate_table_exists(cursor, table_name):
                self.logger.logger.error(
                    "Table validation failed",
                    table_name=table_name
                )
                return False
            
            # Get last watermark for this specific table
            last_watermark = self.get_table_watermark_timestamp(table_name)
            
            # Log backup window
            self.logger.logger.info(
                "Processing backup window",
                table_name=table_name,
                start_time=last_watermark,
                end_time=current_timestamp
            )
            
            # Get estimated row count for progress tracking
            estimated_rows = self.get_table_row_count(
                cursor, table_name, last_watermark, current_timestamp
            )
            
            if estimated_rows == 0:
                self.logger.logger.info(
                    "No new data to backup",
                    table_name=table_name
                )
                return True
            
            self.logger.logger.info(
                f"Estimated {estimated_rows} rows to process",
                table_name=table_name,
                estimated_rows=estimated_rows
            )
            
            # Execute incremental query with optional limit
            incremental_query = self.get_incremental_query(
                table_name, last_watermark, current_timestamp, limit=limit
            )
            
            cursor.execute(incremental_query)
            
            # Process data in batches
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
                
                # Process batch with retries
                batch_success = self._process_batch_with_retries(
                    batch_data, table_name, batch_id, current_timestamp
                )
                
                if not batch_success:
                    self.logger.logger.error(
                        "Batch processing failed after retries",
                        table_name=table_name,
                        batch_id=batch_id
                    )
                    return False
                
                # Log progress
                batch_duration = time.time() - batch_start_time
                progress_pct = (total_rows_processed / max(estimated_rows, total_rows_processed)) * 100
                
                self.logger.logger.debug(
                    "Batch completed",
                    table_name=table_name,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    total_processed=total_rows_processed,
                    progress_percent=round(progress_pct, 1),
                    batch_duration=round(batch_duration, 2)
                )
            
            # Record table metrics
            table_duration = time.time() - (self.metrics.start_time or time.time())
            self.metrics.add_table_metrics(
                table_name, total_rows_processed, batch_id, table_duration
            )
            
            # Get max data timestamp from processed data for watermark
            max_data_timestamp = None
            if total_rows_processed > 0:
                # Get last batch to find max timestamp
                cursor.execute(f"""
                    SELECT MAX(`update_at`) as max_update_at 
                    FROM {table_name} 
                    WHERE `update_at` > '{last_watermark}' 
                    AND `update_at` <= '{current_timestamp}'
                """)
                result = cursor.fetchone()
                if result and result.get('max_update_at'):
                    max_data_timestamp = result['max_update_at']
            
            # Update S3 watermark for this table
            try:
                watermark_updated = self.update_watermarks(
                    table_name=table_name,
                    extraction_time=datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S'),
                    max_data_timestamp=max_data_timestamp,
                    rows_extracted=total_rows_processed,
                    status='success',
                    s3_file_count=batch_id
                )
                
                if not watermark_updated:
                    self.logger.logger.warning(
                        "Failed to update watermark, but table processing succeeded",
                        table_name=table_name
                    )
                
            except Exception as e:
                self.logger.logger.warning(
                    "Watermark update failed but table processing succeeded",
                    table_name=table_name,
                    error=str(e)
                )
            
            self.logger.logger.info(
                "Table processing completed",
                table_name=table_name,
                total_rows=total_rows_processed,
                total_batches=batch_id,
                estimated_rows=estimated_rows,
                accuracy_percent=round((total_rows_processed / max(estimated_rows, 1)) * 100, 1)
            )
            
            return True
            
        except Exception as e:
            # Update watermark with error status
            try:
                self.update_watermarks(
                    table_name=table_name,
                    extraction_time=datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S'),
                    max_data_timestamp=None,
                    rows_extracted=0,
                    status='failed',
                    s3_file_count=0,
                    error_message=str(e)
                )
            except Exception as watermark_error:
                self.logger.logger.warning(
                    "Failed to update error watermark",
                    table_name=table_name,
                    watermark_error=str(watermark_error)
                )
            
            self.logger.error_occurred(e, f"table_processing_{table_name}")
            return False
        
        finally:
            if cursor:
                cursor.close()
    
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
            "name": "Sequential Backup Strategy",
            "description": "Processes tables one by one in sequence for maximum reliability",
            "advantages": [
                "Maximum data consistency",
                "Predictable resource usage", 
                "Simple error handling",
                "Reliable for all table sizes"
            ],
            "disadvantages": [
                "Slower for multiple tables",
                "No parallelization benefits"
            ],
            "best_for": [
                "Small to medium number of tables",
                "When data consistency is critical",
                "Limited system resources",
                "Simple, reliable operations"
            ],
            "configuration": {
                "batch_size": self.config.backup.batch_size,
                "retry_attempts": self.config.backup.retry_attempts,
                "timeout_seconds": self.config.backup.timeout_seconds
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
        # This is a rough estimate based on configuration
        # In a real implementation, you might query table sizes
        
        estimated_rows_per_table = 10000  # Default estimate
        processing_rate = 5000  # rows per second (from our test results)
        
        total_estimated_rows = len(tables) * estimated_rows_per_table
        estimated_seconds = total_estimated_rows / processing_rate
        
        return {
            "total_tables": len(tables),
            "estimated_total_rows": total_estimated_rows,
            "estimated_duration_seconds": estimated_seconds,
            "estimated_duration_minutes": round(estimated_seconds / 60, 1),
            "processing_rate_rows_per_second": processing_rate,
            "strategy": "sequential"
        }