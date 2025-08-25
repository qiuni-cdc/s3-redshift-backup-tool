"""
Row-based backup strategy implementation.

This module implements a simple, reliable row-based backup strategy where
chunk_size represents the exact number of rows processed per chunk.
Uses timestamp + ID for reliable pagination and user-friendly progress tracking.
"""

from typing import List, Dict, Any, Optional, Tuple
import time
from datetime import datetime

from src.backup.base import BaseBackupStrategy
from src.utils.exceptions import BackupError, DatabaseError, raise_backup_error
from src.utils.logging import get_backup_logger


class RowBasedBackupStrategy(BaseBackupStrategy):
    """
    Simple row-based backup strategy implementation.
    
    Processes tables using exact row counts with timestamp + ID pagination.
    Provides predictable chunk sizes and user-friendly time-based progress.
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.logger.set_context(strategy="row_based", chunking_type="timestamp_id")
    
    def execute(self, tables: List[str], chunk_size: Optional[int] = None, max_total_rows: Optional[int] = None, limit: Optional[int] = None) -> bool:
        """
        Execute row-based backup for all specified tables.
        
        Args:
            tables: List of table names to backup
            chunk_size: Optional row limit per chunk (overrides config)
            max_total_rows: Optional maximum total rows to process across all chunks
            limit: Deprecated - use chunk_size instead (for backward compatibility)
        
        Returns:
            True if all tables backed up successfully, False otherwise
        """
        if not tables:
            self.logger.logger.warning("No tables specified for backup")
            return False
        
        start_time = time.time()
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        successful_tables = []
        failed_tables = []
        
        with self.database_session() as db_conn:
            for i, table_name in enumerate(tables):
                table_start_time = time.time()
                
                self.logger.logger.info(
                    f"Processing table {i+1}/{len(tables)} with row-based chunking",
                    table_name=table_name,
                    progress=f"{i+1}/{len(tables)}"
                )
                
                # Handle backward compatibility with old 'limit' parameter
                effective_chunk_size = chunk_size or limit
                
                try:
                    success = self._process_single_table_row_based(
                        db_conn, table_name, current_timestamp, effective_chunk_size, max_total_rows
                    )
                    
                    if success:
                        successful_tables.append(table_name)
                        table_duration = time.time() - table_start_time
                        self.logger.table_completed(table_name, table_duration)
                    else:
                        failed_tables.append(table_name)
                        self.logger.table_failed(table_name)
                        
                except Exception as e:
                    failed_tables.append(table_name)
                    self.logger.table_failed(table_name, error=e)
                    self.logger.error_occurred(e, f"table_backup_{table_name}")
        
        # Update final metrics and watermarks
        duration = time.time() - start_time
        success = len(failed_tables) == 0
        
        self.logger.logger.info(
            "Row-based backup completed",
            success=success,
            duration_seconds=duration,
            successful_tables=len(successful_tables),
            failed_tables=len(failed_tables),
            watermark_updated=success
        )
        
        return success
    
    def _process_single_table_row_based(
        self, 
        db_conn, 
        table_name: str, 
        current_timestamp: str, 
        chunk_size: Optional[int] = None,
        max_total_rows: Optional[int] = None
    ) -> bool:
        """
        Process a single table using row-based chunking.
        
        Args:
            db_conn: Database connection
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            chunk_size: Optional row limit per chunk (chunk size)
            max_total_rows: Optional maximum total rows to process (total limit)
            
        Returns:
            True if table processed successfully
        """
        cursor = None
        table_start_time = time.time()  # Track table processing start time
        total_rows_processed = 0  # Track rows for error reporting
        try:
            self.logger.table_started(table_name)
            
            # CRITICAL FIX: Reset S3 stats for accurate per-table file counting
            self.s3_manager.reset_stats()
            self.logger.logger.info(
                "Reset S3 upload stats for new table processing",
                table_name=table_name,
                fix_applied="s3_stats_reset_per_table"
            )
            
            # Create cursor with dictionary output
            cursor = db_conn.cursor(dictionary=True, buffered=False)
            
            # Validate table structure
            if not self.validate_table_exists(cursor, table_name):
                self.logger.logger.error(
                    "Table validation failed for row-based backup",
                    table_name=table_name
                )
                return False
            
            # Check for required columns
            if not self._validate_required_columns(cursor, table_name):
                return False
            
            # Get current watermark for resume capability
            watermark = self.watermark_manager.get_table_watermark(table_name)
            
            if not watermark or not watermark.last_mysql_data_timestamp:
                # No watermark - start from beginning
                last_timestamp = '1970-01-01 00:00:00'
                last_id = 0
                self.logger.logger.info(
                    "No watermark found, starting row-based backup from beginning",
                    table_name=table_name,
                    initial_timestamp=last_timestamp
                )
            else:
                # Resume from watermark
                raw_timestamp = watermark.last_mysql_data_timestamp
                last_id = getattr(watermark, 'last_processed_id', 0)
                
                # Convert ISO format to MySQL format if needed
                if isinstance(raw_timestamp, str) and 'T' in raw_timestamp:
                    # Convert '2024-01-01T00:00:00Z' to '2024-01-01 00:00:00'
                    last_timestamp = raw_timestamp.replace('T', ' ').replace('Z', '')
                    if '+' in last_timestamp:
                        last_timestamp = last_timestamp.split('+')[0]  # Remove timezone
                elif raw_timestamp is None:
                    # Handle None timestamp - fallback to epoch start
                    last_timestamp = '1970-01-01 00:00:00'
                    self.logger.logger.warning(
                        "Watermark timestamp is None, using epoch start",
                        table_name=table_name
                    )
                else:
                    last_timestamp = str(raw_timestamp)
                self.logger.logger.info(
                    "Resuming row-based backup from watermark",
                    table_name=table_name,
                    last_timestamp=last_timestamp,
                    last_id=last_id
                )
            
            # Determine chunk size
            effective_chunk_size = chunk_size or self.config.backup.target_rows_per_chunk
            
            # Process table in exact row-based chunks
            chunk_number = 1
            
            self.logger.logger.info(
                "Starting row-based chunking",
                table_name=table_name,
                chunk_size=effective_chunk_size,
                max_total_rows=max_total_rows,
                max_chunks_calculated=max_total_rows // effective_chunk_size if max_total_rows else None,
                resume_from_timestamp=last_timestamp,
                resume_from_id=last_id
            )
            
            while True:
                # Check total row limit before processing next chunk
                if max_total_rows and total_rows_processed >= max_total_rows:
                    self.logger.logger.info(
                        "Reached maximum total rows limit - stopping backup",
                        table_name=table_name,
                        max_total_rows=max_total_rows,
                        total_rows_processed=total_rows_processed,
                        chunks_completed=chunk_number - 1,
                        limit_enforcement="STRICT_LIMIT_REACHED"
                    )
                    break
                
                # Debug: Log limit enforcement status before each chunk
                if max_total_rows:
                    remaining_rows = max_total_rows - total_rows_processed
                    self.logger.logger.info(
                        "Chunk limit check before processing",
                        table_name=table_name,
                        chunk_number=chunk_number,
                        max_total_rows=max_total_rows,
                        total_rows_processed=total_rows_processed,
                        remaining_rows=remaining_rows,
                        will_process="YES" if remaining_rows > 0 else "NO"
                    )
                
                # Adjust chunk size if approaching total limit
                current_chunk_size = effective_chunk_size
                if max_total_rows:
                    remaining_rows = max_total_rows - total_rows_processed
                    current_chunk_size = min(effective_chunk_size, remaining_rows)
                    if current_chunk_size <= 0:
                        self.logger.logger.info(
                            "No remaining rows to process - stopping backup",
                            table_name=table_name,
                            max_total_rows=max_total_rows,
                            total_rows_processed=total_rows_processed,
                            remaining_rows=remaining_rows,
                            limit_enforcement="ZERO_REMAINING_ROWS"
                        )
                        break
                    elif remaining_rows < effective_chunk_size:
                        self.logger.logger.info(
                            "Final partial chunk to respect total row limit",
                            table_name=table_name,
                            remaining_rows=remaining_rows,
                            adjusted_chunk_size=current_chunk_size,
                            limit_enforcement="FINAL_PARTIAL_CHUNK"
                        )
                
                chunk_start_time = time.time()
                
                # Get next chunk of exactly chunk_size rows (or remaining)
                chunk_data, chunk_last_timestamp, chunk_last_id = self._get_next_chunk(
                    cursor, table_name, last_timestamp, last_id, current_chunk_size
                )
                
                if not chunk_data:
                    self.logger.logger.info(
                        "No more data found, row-based backup complete",
                        table_name=table_name,
                        total_chunks=chunk_number - 1,
                        total_rows=total_rows_processed
                    )
                    break
                
                rows_in_chunk = len(chunk_data)
                
                self.logger.logger.info(
                    f"Processing row-based chunk {chunk_number}",
                    table_name=table_name,
                    chunk_size_actual=rows_in_chunk,
                    chunk_size_requested=current_chunk_size,
                    time_range_start=chunk_data[0]['update_at'],
                    time_range_end=chunk_data[-1]['update_at'],
                    id_range_start=chunk_data[0]['ID'],
                    id_range_end=chunk_data[-1]['ID']
                )
                
                # Process chunk in smaller batches for S3 upload
                batch_size = self.config.backup.batch_size
                batch_success_count = 0
                
                for i in range(0, rows_in_chunk, batch_size):
                    batch_data = chunk_data[i:i + batch_size]
                    batch_id = f"chunk_{chunk_number}_batch_{(i // batch_size) + 1}"
                    
                    batch_success = self._process_batch_with_retries(
                        batch_data, table_name, batch_id, current_timestamp
                    )
                    
                    if not batch_success:
                        self.logger.logger.error(
                            f"Failed to process batch in chunk {chunk_number}",
                            table_name=table_name,
                            batch_id=batch_id,
                            batch_size=len(batch_data)
                        )
                        return False
                    
                    batch_success_count += 1
                
                # Update progress
                total_rows_processed += rows_in_chunk
                chunk_duration = time.time() - chunk_start_time
                
                # Update watermark after successful chunk (absolute progress tracking)
                self._update_chunk_watermark_absolute(
                    table_name, chunk_last_timestamp, chunk_last_id, 
                    total_rows_processed  # Use absolute total, not incremental
                )
                
                self.logger.logger.info(
                    f"Completed row-based chunk {chunk_number}",
                    table_name=table_name,
                    rows_in_chunk=rows_in_chunk,
                    total_rows=total_rows_processed,
                    max_total_rows=max_total_rows,
                    remaining_rows=max_total_rows - total_rows_processed if max_total_rows else None,
                    chunk_duration_seconds=round(chunk_duration, 2),
                    batches_processed=batch_success_count,
                    last_timestamp=chunk_last_timestamp,
                    last_id=chunk_last_id
                )
                
                # Update for next iteration
                last_timestamp = chunk_last_timestamp
                last_id = chunk_last_id
                chunk_number += 1
                
                # Memory management
                self.memory_manager.force_gc_if_needed(chunk_number)
                if not self.memory_manager.check_memory_usage(chunk_number):
                    self.logger.logger.warning(
                        "Memory usage high during row-based backup",
                        table_name=table_name,
                        chunk_number=chunk_number
                    )
            
            # Final watermark update with completion status (additive session total)
            self._set_final_watermark_with_session_control(
                table_name=table_name,
                extraction_time=datetime.now(),
                max_data_timestamp=datetime.fromisoformat(last_timestamp),
                last_processed_id=last_id,
                session_rows_processed=total_rows_processed,  # Session total
                status='success'
            )
            
            # CRITICAL FIX: Update metrics object with actual processed data
            table_end_time = time.time()
            table_duration = table_end_time - table_start_time
            total_batches = chunk_number - 1  # chunks completed
            
            # Add table metrics to the metrics object
            self.metrics.add_table_metrics(
                table_name, 
                total_rows_processed, 
                total_batches, 
                table_duration, 
                0  # bytes will be estimated in process_batch
            )
            
            self.logger.logger.info(
                "Row-based table backup completed successfully",
                table_name=table_name,
                total_chunks=total_batches,
                total_rows=total_rows_processed,
                final_timestamp=last_timestamp,
                final_id=last_id,
                metrics_updated=True
            )
            
            return True
            
        except Exception as e:
            self.logger.error_occurred(e, "row_based_backup", table_name=table_name)
            # Update watermark with error status (additive session count)
            try:
                self._set_final_watermark_with_session_control(
                    table_name=table_name,
                    extraction_time=datetime.now(),
                    session_rows_processed=total_rows_processed,  # Session partial progress
                    status='failed',
                    error_message=str(e)
                )
            except Exception as watermark_error:
                self.logger.logger.error(
                    "Failed to update error watermark",
                    table_name=table_name,
                    original_error=str(e),
                    watermark_error=str(watermark_error)
                )
            return False
            
        finally:
            # Ensure cursor is properly closed
            if cursor:
                try:
                    # Consume any remaining results
                    while cursor.nextset():
                        pass
                except:
                    pass
                try:
                    cursor.close()
                except:
                    pass
    
    def _get_next_chunk(
        self, 
        cursor, 
        table_name: str, 
        last_timestamp: str, 
        last_id: int, 
        chunk_size: int
    ) -> Tuple[List[Dict], str, int]:
        """
        Get next chunk using timestamp + ID pagination.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table
            last_timestamp: Last processed timestamp
            last_id: Last processed ID
            chunk_size: Exact number of rows to fetch
            
        Returns:
            Tuple of (chunk_data, last_timestamp, last_id)
        """
        try:
            # Handle None/null parameters with safe defaults
            safe_last_id = last_id if last_id is not None else 0
            safe_table_name = table_name if table_name is not None else "INVALID_TABLE"
            safe_last_timestamp = last_timestamp if last_timestamp is not None else '1970-01-01 00:00:00'
            
            # Validate critical parameters
            if table_name is None:
                raise ValueError(f"table_name cannot be None")
            if last_timestamp is None:
                self.logger.logger.error(
                    "last_timestamp is None in _get_next_chunk",
                    table_name=table_name,
                    last_id=last_id
                )
                raise ValueError(f"last_timestamp cannot be None for table {table_name}")
            
            # Option 2: Timestamp + ID based query for reliable pagination
            query = f"""
            SELECT * FROM {safe_table_name}
            WHERE update_at > '{safe_last_timestamp}' 
               OR (update_at = '{safe_last_timestamp}' AND ID > {safe_last_id})
            ORDER BY update_at, ID 
            LIMIT {chunk_size}
            """
            
            self.logger.logger.info(
                "Executing row-based chunk query",
                table_name=table_name,
                last_timestamp=last_timestamp,
                last_id=last_id,
                safe_last_id=safe_last_id,
                chunk_size=chunk_size,
                query_preview=query.replace('\n', ' ').strip()[:300] + "..."
            )
            
            cursor.execute(query)
            chunk_data = cursor.fetchall()
            
            # Debug: Log query result with first/last row details
            if chunk_data:
                first_row = chunk_data[0]
                last_row = chunk_data[-1]
                self.logger.logger.info(
                    "Query execution completed - DETAILED DEBUG",
                    table_name=table_name,
                    rows_found=len(chunk_data),
                    chunk_size_requested=chunk_size,
                    rows_vs_limit_ratio=f"{len(chunk_data)}/{chunk_size}",
                    first_row_id=first_row.get('ID'),
                    first_row_timestamp=str(first_row.get('update_at')),
                    last_row_id=last_row.get('ID'),
                    last_row_timestamp=str(last_row.get('update_at')),
                    query_executed=query.replace('\n', ' ').strip()
                )
                
                # Critical check: Verify LIMIT was respected
                if len(chunk_data) > chunk_size:
                    self.logger.logger.error(
                        "CRITICAL BUG: Query returned more rows than LIMIT",
                        table_name=table_name,
                        requested_limit=chunk_size,
                        actual_rows=len(chunk_data),
                        excess_rows=len(chunk_data) - chunk_size
                    )
            else:
                self.logger.logger.info(
                    "Query execution completed - no rows found",
                    table_name=table_name,
                    query_executed=query.replace('\n', ' ').strip()
                )
            
            # Consume any remaining results to prevent cursor issues
            try:
                while cursor.nextset():
                    pass
            except:
                pass
            
            if not chunk_data:
                self.logger.logger.warning(
                    "No data found with current query - check timestamp/ID values",
                    table_name=table_name,
                    last_timestamp=last_timestamp,
                    last_id=last_id
                )
                return [], last_timestamp, last_id
            
            # Extract last timestamp and ID for next iteration
            last_row = chunk_data[-1]
            
            # Handle potential None values in row data
            update_at_value = last_row.get('update_at')
            if update_at_value is None:
                self.logger.logger.error(
                    "update_at is None in chunk data",
                    table_name=table_name,
                    row_id=last_row.get('ID'),
                    chunk_size=len(chunk_data)
                )
                chunk_last_timestamp = '1970-01-01 00:00:00'  # Safe fallback
            else:
                chunk_last_timestamp = update_at_value.strftime('%Y-%m-%d %H:%M:%S')
            
            chunk_last_id = last_row.get('ID', 0)
            
            self.logger.logger.debug(
                "Retrieved row-based chunk",
                table_name=table_name,
                rows_retrieved=len(chunk_data),
                chunk_last_timestamp=chunk_last_timestamp,
                chunk_last_id=chunk_last_id
            )
            
            return chunk_data, chunk_last_timestamp, chunk_last_id
            
        except Exception as e:
            self.logger.logger.error(
                "Failed to retrieve chunk",
                table_name=table_name,
                error=str(e),
                last_timestamp=last_timestamp,
                last_id=last_id
            )
            raise DatabaseError(f"Failed to retrieve chunk for {table_name}: {e}")
    
    def _validate_required_columns(self, cursor, table_name: str) -> bool:
        """
        Validate that table has required columns for row-based chunking.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table to validate
        
        Returns:
            True if table has required columns, False otherwise
        """
        try:
            cursor.execute(f"DESCRIBE {table_name}")
            describe_results = cursor.fetchall()
            
            # Handle both dictionary and tuple cursors
            if describe_results and isinstance(describe_results[0], dict):
                columns = [row['Field'] for row in describe_results]
            else:
                columns = [row[0] for row in describe_results]
            
            # Check for required columns
            missing_columns = []
            if 'update_at' not in columns:
                missing_columns.append('update_at')
            if 'ID' not in columns and 'id' not in columns:
                missing_columns.append('ID')
            
            if missing_columns:
                self.logger.logger.error(
                    "Table missing required columns for row-based chunking",
                    table_name=table_name,
                    missing_columns=missing_columns,
                    available_columns=columns[:10]
                )
                return False
            
            self.logger.logger.info(
                "Table validation successful for row-based chunking",
                table_name=table_name,
                column_count=len(columns),
                has_update_at=True,
                has_id=True
            )
            
            return True
            
        except Exception as e:
            self.logger.logger.error(
                "Table validation failed",
                table_name=table_name,
                error=str(e)
            )
            return False
    
    
    def _process_batch_with_retries(
        self, 
        batch_data: List[Dict], 
        table_name: str, 
        batch_id: str, 
        current_timestamp: str,
        max_retries: int = 3
    ) -> bool:
        """
        Process a batch with retry logic.
        
        Args:
            batch_data: List of row dictionaries
            table_name: Name of the table
            batch_id: Batch identifier
            current_timestamp: Current backup timestamp
            max_retries: Maximum retry attempts
        
        Returns:
            True if batch processed successfully
        """
        for attempt in range(max_retries):
            try:
                success = self.process_batch(
                    batch_data, table_name, batch_id, current_timestamp
                )
                
                if success:
                    return True
                else:
                    self.logger.logger.warning(
                        f"Batch processing failed, attempt {attempt + 1}/{max_retries}",
                        table_name=table_name,
                        batch_id=batch_id,
                        batch_size=len(batch_data)
                    )
                    
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        
            except Exception as e:
                self.logger.logger.warning(
                    f"Batch processing exception, attempt {attempt + 1}/{max_retries}",
                    table_name=table_name,
                    batch_id=batch_id,
                    error=str(e)
                )
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error_occurred(e, f"batch_retry_{table_name}")
        
        return False
    
    def _set_final_watermark_absolute(
        self, 
        table_name: str, 
        extraction_time: datetime,
        max_data_timestamp: Optional[datetime] = None,
        last_processed_id: Optional[int] = None,
        total_rows_processed: int = 0,
        status: str = 'success',
        error_message: Optional[str] = None
    ):
        """
        Set final watermark with absolute values (not additive).
        
        Args:
            table_name: Name of the table
            extraction_time: Time of extraction
            max_data_timestamp: Latest data timestamp processed
            last_processed_id: Last processed ID
            total_rows_processed: Absolute total rows processed
            status: Final status ('success' or 'failed')
            error_message: Optional error message for failed status
        """
        try:
            # Get current watermark to preserve S3 file list and other metadata
            current_watermark = self.watermark_manager.get_table_watermark(table_name)
            
            # Build absolute watermark update (not additive)
            watermark_data = {
                'last_mysql_extraction_time': extraction_time.isoformat(),
                'mysql_status': status,
                'backup_strategy': 'row_based'
            }
            
            # Set absolute values
            if max_data_timestamp:
                watermark_data['last_mysql_data_timestamp'] = max_data_timestamp.isoformat()
            if last_processed_id is not None:
                watermark_data['last_processed_id'] = last_processed_id
            
            # CRITICAL: Set absolute row count, not additive
            watermark_data['mysql_rows_extracted'] = total_rows_processed
            
            if error_message:
                watermark_data['last_error'] = error_message
            
            # Preserve existing S3 file list and other metadata
            if current_watermark:
                if hasattr(current_watermark, 'processed_s3_files') and current_watermark.processed_s3_files:
                    watermark_data['processed_s3_files'] = current_watermark.processed_s3_files
                if hasattr(current_watermark, 's3_file_count'):
                    watermark_data['s3_file_count'] = current_watermark.s3_file_count
            
            # Use direct update to S3 to bypass additive logic
            success = self.watermark_manager._update_watermark_direct(
                table_name=table_name,
                watermark_data=watermark_data
            )
            
            self.logger.logger.info(
                "Final watermark set with absolute values",
                table_name=table_name,
                total_rows_final=total_rows_processed,
                status=status,
                additive_bypassed=True
            )
            
            return success
            
        except Exception as e:
            self.logger.logger.error(
                "Failed to set final absolute watermark",
                table_name=table_name,
                error=str(e),
                total_rows=total_rows_processed
            )
            # Fallback to regular update_watermarks if absolute method fails
            return self.update_watermarks(
                table_name=table_name,
                extraction_time=extraction_time,
                max_data_timestamp=max_data_timestamp,
                last_processed_id=last_processed_id,
                rows_extracted=total_rows_processed,
                status=status,
                error_message=error_message
            )
    
    def _update_chunk_watermark_absolute(
        self, 
        table_name: str, 
        last_timestamp: str, 
        last_id: int, 
        total_rows_processed: int
    ):
        """
        Update watermark with resume data only (timestamp + ID).
        
        This method updates ONLY the resume position (timestamp/ID) without 
        touching mysql_rows_extracted to prevent overwriting cumulative totals.
        
        Args:
            table_name: Name of the table
            last_timestamp: Last processed timestamp
            last_id: Last processed ID
            total_rows_processed: Session progress (used only for logging)
        """
        try:
            # Validate parameters before watermark update
            if last_timestamp is None:
                self.logger.logger.error(
                    "last_timestamp is None in watermark update",
                    table_name=table_name,
                    last_id=last_id,
                    total_rows=total_rows_processed
                )
                last_timestamp = '1970-01-01 00:00:00'  # Safe fallback
            
            # Use direct watermark update ONLY for resume data (NOT row counts)
            watermark_data = {
                'last_mysql_data_timestamp': last_timestamp,
                'last_processed_id': last_id,
                # CRITICAL FIX: Don't touch mysql_rows_extracted in chunk updates
                # 'mysql_rows_extracted': total_rows_processed,  # ❌ REMOVED - causes overwrite
                'mysql_status': 'in_progress',
                'backup_strategy': 'row_based',
                'last_mysql_extraction_time': datetime.now().isoformat()
            }
            
            # Use direct update to bypass additive logic
            success = self.watermark_manager._update_watermark_direct(
                table_name=table_name,
                watermark_data=watermark_data
            )
            
            self.logger.logger.debug(
                "Updated chunk watermark with resume data only",
                table_name=table_name,
                last_timestamp=last_timestamp,
                last_id=last_id,
                session_progress_rows=total_rows_processed,
                row_count_preserved=True
            )
            
            return success
            
        except Exception as e:
            self.logger.logger.warning(
                "Failed to update chunk watermark with absolute values",
                table_name=table_name,
                error=str(e),
                total_rows=total_rows_processed
            )
            return False
    
    def _set_final_watermark_with_session_control(
        self, 
        table_name: str, 
        extraction_time: datetime,
        max_data_timestamp: Optional[datetime] = None,
        last_processed_id: Optional[int] = None,
        session_rows_processed: int = 0,
        status: str = 'success',
        error_message: Optional[str] = None
    ):
        """
        CRITICAL FIX: Set final watermark using session-controlled mode.
        
        Uses the new watermark mode system to handle session vs cross-session updates correctly.
        This replaces the old additive logic that caused double-counting bugs.
        
        Args:
            table_name: Name of the table
            extraction_time: Time of extraction
            max_data_timestamp: Latest data timestamp processed
            last_processed_id: Last processed ID
            session_rows_processed: Rows processed in this session only
            status: Final status ('success' or 'failed')
            error_message: Optional error message for failed status
        """
        try:
            # CRITICAL FIX: Generate unique session ID
            import uuid
            session_id = f"row_based_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            # CRITICAL FIX: Get actual S3 file count from S3Manager
            s3_stats = self.s3_manager.get_upload_stats()
            actual_s3_files = s3_stats.get('total_files', 0)
            
            self.logger.logger.info(
                "S3 file count tracking for watermark update",
                table_name=table_name,
                s3_files_created=actual_s3_files,
                session_rows=session_rows_processed,
                fix_applied="s3_count_tracking"
            )
            
            # Use the NEW mode-controlled watermark update with REAL S3 count
            success = self.watermark_manager.update_mysql_watermark(
                table_name=table_name,
                extraction_time=extraction_time,
                max_data_timestamp=max_data_timestamp,
                last_processed_id=last_processed_id,
                rows_extracted=session_rows_processed,
                status=status,
                backup_strategy='row_based',
                s3_file_count=actual_s3_files,  # ✅ FIXED: Real S3 count instead of 0
                error_message=error_message,
                mode='auto',  # Let the system decide absolute vs additive
                session_id=session_id
            )
            
            self.logger.logger.info(
                "Final watermark updated with session control",
                table_name=table_name,
                session_rows=session_rows_processed,
                status=status,
                session_id=session_id,
                mode_system="auto_detection"
            )
            
            return success
            
        except Exception as e:
            self.logger.logger.error(
                "Failed to set final watermark with session control",
                table_name=table_name,
                error=str(e),
                session_rows=session_rows_processed
            )
            # Fallback to old method if new mode fails (with S3 count fix)
            try:
                s3_stats = self.s3_manager.get_upload_stats()
                fallback_s3_files = s3_stats.get('total_files', 0)
            except:
                fallback_s3_files = 0  # Safe fallback if S3Manager fails
                
            return self.update_watermarks(
                table_name=table_name,
                extraction_time=extraction_time,
                max_data_timestamp=max_data_timestamp,
                last_processed_id=last_processed_id,
                rows_extracted=session_rows_processed,
                s3_file_count=fallback_s3_files,  # ✅ FIXED: Include S3 count in fallback
                status=status,
                error_message=error_message
            )