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
from src.backup.cdc_backup_integration import create_cdc_integration


class RowBasedBackupStrategy(BaseBackupStrategy):
    """
    Simple row-based backup strategy implementation.
    
    Processes tables using exact row counts with timestamp + ID pagination.
    Provides predictable chunk sizes and user-friendly time-based progress.
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.logger.set_context(strategy="row_based", chunking_type="timestamp_id")
        self.cdc_integration = None
    
    def execute(self, tables: List[str], chunk_size: Optional[int] = None, max_total_rows: Optional[int] = None, limit: Optional[int] = None, source_connection: Optional[str] = None) -> bool:
        """
        Execute row-based backup for all specified tables.
        
        Args:
            tables: List of table names to backup
            chunk_size: Optional row limit per chunk (overrides config)
            max_total_rows: Optional maximum total rows to process across all chunks
            limit: Deprecated - use chunk_size instead (for backward compatibility)
            source_connection: Optional connection name to use instead of default
        
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
        
        # Initialize CDC integration if not already done
        if not self.cdc_integration:
            pipeline_config = getattr(self, 'pipeline_config', None)
            self.cdc_integration = create_cdc_integration(None, pipeline_config)
        
        with self.database_session(source_connection) as db_conn:
            for i, table_name in enumerate(tables):
                table_start_time = time.time()
                lock_id = None
                
                self.logger.logger.info(
                    f"Processing table {i+1}/{len(tables)} with row-based chunking",
                    table_name=table_name,
                    progress=f"{i+1}/{len(tables)}"
                )
                
                # Handle backward compatibility with old 'limit' parameter
                effective_chunk_size = chunk_size or limit
                
                try:
                    # Acquire lock before processing table to prevent concurrent operations
                    self.logger.logger.debug(f"Acquiring lock for table {table_name}")
                    lock_id = self.watermark_manager.simple_manager.acquire_lock(table_name)
                    self.logger.logger.info(f"Lock acquired for {table_name}: {lock_id}")
                    
                    success = self._process_single_table_row_based(
                        db_conn, table_name, current_timestamp, effective_chunk_size, max_total_rows, source_connection
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
                    
                finally:
                    # Always release lock, even if error occurs
                    if lock_id:
                        try:
                            self.watermark_manager.simple_manager.release_lock(table_name, lock_id)
                            self.logger.logger.debug(f"Released lock for {table_name}: {lock_id}")
                        except Exception as lock_error:
                            self.logger.logger.error(f"Failed to release lock for {table_name}: {lock_error}")
        
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
        max_total_rows: Optional[int] = None,
        source_connection: Optional[str] = None
    ) -> bool:
        """
        Process a single table using row-based chunking.
        
        Args:
            db_conn: Database connection
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
            chunk_size: Optional row limit per chunk (chunk size)
            max_total_rows: Optional maximum total rows to process (total limit)
            source_connection: Optional connection name (for logging and context)
            
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
            
            # Use CDC system for table validation
            table_config = self._get_table_config(table_name)
            table_schema = self._get_table_schema(cursor, table_name)
            
            is_valid, issues = self.cdc_integration.validate_table_for_cdc(
                table_name, table_schema, table_config
            )
            
            if not is_valid:
                self.logger.logger.error(
                    "CDC validation failed for table",
                    table_name=table_name,
                    issues=issues
                )
                return False
            
            # Get current watermark for resume capability
            watermark = self.watermark_manager.get_table_watermark(table_name)
            
            # DEBUG: Log watermark details to identify MAX query bug
            if watermark:
                self.logger.logger.warning(
                    "DEBUG: Watermark contents",
                    table_name=table_name,
                    last_mysql_data_timestamp=watermark.last_mysql_data_timestamp,
                    last_processed_id=getattr(watermark, 'last_processed_id', 'NOT_SET'),
                    backup_strategy=getattr(watermark, 'backup_strategy', 'NOT_SET'),
                    mysql_status=getattr(watermark, 'mysql_status', 'NOT_SET')
                )
            else:
                self.logger.logger.warning(
                    "DEBUG: No watermark found for table",
                    table_name=table_name
                )
            
            if not watermark:
                # No watermark - start from beginning
                last_timestamp = '1970-01-01 00:00:00'
                last_id = 0
                self.logger.logger.info(
                    "No watermark found, starting row-based backup from beginning",
                    table_name=table_name,
                    initial_timestamp=last_timestamp
                )
            elif not watermark.last_mysql_data_timestamp and not getattr(watermark, 'last_processed_id', 0):
                # No timestamp AND no ID - start from beginning
                last_timestamp = '1970-01-01 00:00:00'
                last_id = 0
                self.logger.logger.info(
                    "Empty watermark found, starting row-based backup from beginning",
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
            
            # Get column information from CDC strategy
            table_config = self._get_table_config(table_name)
            if table_config:
                timestamp_column = table_config.get('cdc_timestamp_column', 'updated_at')
                id_column = table_config.get('cdc_id_column', 'id')
            else:
                # Fallback to old detection logic for legacy tables
                timestamp_column = self._get_configured_timestamp_column(table_name) or self._detect_timestamp_column(cursor, table_name)
                id_column = self._get_configured_id_column(table_name) or self._detect_id_column(cursor, table_name)
            
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
                    cursor, table_name, last_timestamp, last_id, current_chunk_size, timestamp_column, id_column
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
                
                # Safe logging for both timestamp and id_only strategies
                log_data = {
                    "table_name": table_name,
                    "chunk_size_actual": rows_in_chunk,
                    "chunk_size_requested": current_chunk_size,
                    "id_range_start": chunk_data[0][id_column],
                    "id_range_end": chunk_data[-1][id_column],
                    "columns_detected": f"{timestamp_column}, {id_column}"
                }
                
                # Only add timestamp ranges if timestamp column exists in data
                if timestamp_column in chunk_data[0]:
                    log_data["time_range_start"] = chunk_data[0][timestamp_column]
                    log_data["time_range_end"] = chunk_data[-1][timestamp_column]
                else:
                    log_data["strategy_note"] = "id_only strategy - no timestamp tracking"
                
                self.logger.logger.info(
                    f"Processing row-based chunk {chunk_number}",
                    **log_data
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
            # Handle both datetime objects and ISO format strings
            if isinstance(last_timestamp, datetime):
                max_data_timestamp = last_timestamp
            else:
                max_data_timestamp = datetime.fromisoformat(last_timestamp)
            
            self._set_final_watermark_with_session_control(
                table_name=table_name,
                extraction_time=datetime.now(),
                max_data_timestamp=max_data_timestamp,
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
    
    def _detect_timestamp_column(self, cursor, table_name: str) -> str:
        """
        Detect the correct timestamp column name for CDC.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table (may be scoped for v1.2.0)
            
        Returns:
            Name of the timestamp column to use
        """
        # Get table columns - extract MySQL table name from potentially scoped name
        mysql_table_name = self._extract_mysql_table_name(table_name)
        cursor.execute(f"DESCRIBE {mysql_table_name}")
        describe_results = cursor.fetchall()
        
        if isinstance(describe_results[0], dict):
            columns = [row['Field'] for row in describe_results]
        else:
            columns = [row[0] for row in describe_results]
        
        # Check in order of preference
        timestamp_column_candidates = ['updated_at', 'update_at', 'last_modified', 'modified_at']
        
        for candidate in timestamp_column_candidates:
            if candidate in columns:
                return candidate
                
        # Fallback to default if none found (should not happen after validation)
        return 'update_at'
    
    def _detect_id_column(self, cursor, table_name: str) -> str:
        """
        Detect the correct ID column name for CDC.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table (may be scoped for v1.2.0)
            
        Returns:
            Name of the ID column to use
        """
        # Get table columns - extract MySQL table name from potentially scoped name
        mysql_table_name = self._extract_mysql_table_name(table_name)
        cursor.execute(f"DESCRIBE {mysql_table_name}")
        describe_results = cursor.fetchall()
        
        if isinstance(describe_results[0], dict):
            columns = [row['Field'] for row in describe_results]
        else:
            columns = [row[0] for row in describe_results]
        
        # Check in order of preference
        id_column_candidates = ['id', 'ID', 'Id', 'pk_id', 'primary_id']
        
        for candidate in id_column_candidates:
            if candidate in columns:
                return candidate
                
        # Fallback to default if none found (should not happen after validation)
        return 'ID'
    
    def _get_configured_timestamp_column(self, table_name: str) -> str:
        """
        Get timestamp column from pipeline configuration if available.
        """
        try:
            # Try to load pipeline configuration
            from src.core.configuration_manager import ConfigurationManager
            config_manager = ConfigurationManager()
            
            # Extract simple table name from full table name (handle scoped names)
            # For scoped names like 'US_DW_RO_SSH:settlement.settle_orders', we want 'settlement.settle_orders'
            unscoped_table_name = self._extract_mysql_table_name(table_name)
            simple_table_name = unscoped_table_name.split('.')[-1] if '.' in unscoped_table_name else unscoped_table_name
            
            # Try both full and simple table names across all pipelines
            # Prioritize specific pipelines over default
            pipeline_priority = ['us_dw_pipeline', 'us_dw_hybrid_v1_2'] + [p for p in config_manager.list_pipelines() if p not in ['us_dw_pipeline', 'us_dw_hybrid_v1_2', 'default']] + ['default']
            
            # FIXED: Try unscoped table name first for scoped lookups
            for name_variant in [unscoped_table_name, table_name, simple_table_name]:
                for pipeline_name in pipeline_priority:
                    try:
                        table_config = config_manager.get_table_config(pipeline_name, name_variant)
                        if table_config and table_config.cdc_timestamp_column:
                            self.logger.logger.info(
                                f"Using configured timestamp column for {table_name}",
                                configured_column=table_config.cdc_timestamp_column,
                                pipeline=pipeline_name,
                                table_variant_matched=name_variant
                            )
                            return table_config.cdc_timestamp_column
                    except Exception:
                        continue  # Try next pipeline
        except Exception as e:
            self.logger.logger.debug(f"Could not load pipeline config for timestamp: {e}")
            
        return None
    
    def _get_configured_id_column(self, table_name: str) -> str:
        """
        Get ID column from pipeline configuration if available.
        """
        try:
            # Try to load pipeline configuration
            from src.core.configuration_manager import ConfigurationManager
            config_manager = ConfigurationManager()
            
            # Extract simple table name from full table name (handle scoped names)
            # For scoped names like 'US_DW_RO_SSH:settlement.settle_orders', we want 'settlement.settle_orders'
            unscoped_table_name = self._extract_mysql_table_name(table_name)
            simple_table_name = unscoped_table_name.split('.')[-1] if '.' in unscoped_table_name else unscoped_table_name
            
            # Try both full and simple table names across all pipelines
            # Prioritize specific pipelines over default
            pipeline_priority = ['us_dw_pipeline', 'us_dw_hybrid_v1_2'] + [p for p in config_manager.list_pipelines() if p not in ['us_dw_pipeline', 'us_dw_hybrid_v1_2', 'default']] + ['default']
            
            # FIXED: Try unscoped table name first for scoped lookups
            for name_variant in [unscoped_table_name, table_name, simple_table_name]:
                for pipeline_name in pipeline_priority:
                    try:
                        table_config = config_manager.get_table_config(pipeline_name, name_variant)
                        if table_config and table_config.cdc_id_column:
                            self.logger.logger.info(
                                f"Using configured ID column for {table_name}",
                                configured_column=table_config.cdc_id_column,
                                pipeline=pipeline_name,
                                table_variant_matched=name_variant
                            )
                            return table_config.cdc_id_column
                    except Exception:
                        continue  # Try next pipeline
        except Exception as e:
            self.logger.logger.debug(f"Could not load pipeline config for ID: {e}")
            
        return None
    
    def _count_actual_s3_files(self, table_name: str) -> int:
        """
        Count actual S3 files for the table using S3Manager's logic.
        This provides accurate count even after restarts/interruptions.
        """
        try:
            # Use existing S3Manager to count files for this table
            if not self.s3_manager or not hasattr(self.s3_manager, 's3_client') or not self.s3_manager.s3_client:
                self.logger.logger.warning(f"S3Manager not available for file counting: {table_name}")
                return 0
            
            # Get table-specific file pattern
            safe_table_name = table_name.replace('.', '_')
            
            # Search for all parquet files for this table
            response = self.s3_manager.s3_client.list_objects_v2(
                Bucket=self.s3_manager.bucket_name,
                Prefix='incremental/',
                MaxKeys=1000
            )
            
            # Filter for this table's files
            total_files = 0
            files = response.get('Contents', [])
            
            for file_obj in files:
                key = file_obj['Key']
                # Check if file belongs to this table and is a parquet file
                if (safe_table_name in key and 
                    key.endswith('.parquet')):
                    total_files += 1
                    self.logger.logger.debug(f"Found S3 file: {key}")
            
            # Handle pagination if there are more files
            while response.get('IsTruncated', False):
                response = self.s3_manager.s3_client.list_objects_v2(
                    Bucket=self.s3_manager.bucket_name,
                    Prefix='incremental/',
                    MaxKeys=1000,
                    ContinuationToken=response['NextContinuationToken']
                )
                
                files = response.get('Contents', [])
                for file_obj in files:
                    key = file_obj['Key']
                    if (safe_table_name in key and 
                        key.endswith('.parquet')):
                        total_files += 1
                        self.logger.logger.debug(f"Found S3 file: {key}")
            
            self.logger.logger.info(
                f"Actual S3 file count for {table_name}: {total_files} files",
                table_name=table_name,
                total_files_found=total_files,
                search_prefix='incremental/'
            )
            
            return total_files
            
        except Exception as e:
            self.logger.logger.error(
                f"Failed to count S3 files for {table_name}: {e}",
                table_name=table_name,
                error=str(e)
            )
            # Fallback to in-memory stats if S3 count fails
            s3_stats = self.s3_manager.get_upload_stats()
            return s3_stats.get('total_files', 0)
    
    def _get_next_chunk(
        self, 
        cursor, 
        table_name: str, 
        last_timestamp: str, 
        last_id: int, 
        chunk_size: int,
        timestamp_column: str,
        id_column: str
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
            # FIXED: Extract actual MySQL table name from potentially scoped name for v1.2.0
            safe_table_name = self._extract_mysql_table_name(table_name) if table_name is not None else "INVALID_TABLE"
            safe_last_timestamp = last_timestamp if last_timestamp is not None else '1970-01-01 00:00:00'
            
            # Validate critical parameters
            if table_name is None:
                raise ValueError(f"table_name cannot be None")
            
            # Initialize CDC integration if not already done
            if not self.cdc_integration:
                try:
                    pipeline_config = getattr(self, 'pipeline_config', None)
                    self.cdc_integration = create_cdc_integration(None, pipeline_config)
                except Exception as e:
                    self.logger.logger.error(f"Failed to initialize CDC integration: {e}")
                    return [], last_timestamp, last_id
            
            # Use CDC integration to build query
            table_config = self._get_table_config(table_name)
            watermark = {
                'last_mysql_data_timestamp': safe_last_timestamp,
                'last_processed_id': safe_last_id
            }
            
            # Extract MySQL table name for query building
            mysql_table_name = self._extract_mysql_table_name(table_name)
            query = self.cdc_integration.build_incremental_query(
                mysql_table_name, watermark, chunk_size, table_config
            )
            
            self.logger.logger.info(
                "Executing CDC-generated chunk query",
                table_name=table_name,
                last_timestamp=last_timestamp,
                last_id=last_id,
                safe_last_id=safe_last_id,
                chunk_size=chunk_size,
                query_preview=query.replace('\n', ' ').strip()[:300] + "..."
            )
            
            cursor.execute(query)
            chunk_data = cursor.fetchall()
            
            # Convert to list of dictionaries if needed
            if chunk_data and not isinstance(chunk_data[0], dict):
                # Handle tuple results from regular cursors
                column_names = [desc[0] for desc in cursor.description]
                chunk_data = [dict(zip(column_names, row)) for row in chunk_data]
            
            if not chunk_data:
                # No more data
                return [], safe_last_timestamp, safe_last_id
            
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
                    first_row_id=first_row.get(id_column),
                    last_row_id=last_row.get(id_column),
                    id_column_used=id_column,
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
            
            # Initialize CDC integration if not already done
            if not self.cdc_integration:
                try:
                    pipeline_config = getattr(self, 'pipeline_config', None)
                    self.cdc_integration = create_cdc_integration(None, pipeline_config)
                except Exception as e:
                    self.logger.logger.error(f"Failed to initialize CDC integration: {e}")
                    return [], safe_last_timestamp, safe_last_id
            
            # Extract watermark using CDC integration
            table_config = self._get_table_config(table_name)
            watermark_data = self.cdc_integration.extract_watermark_from_batch(
                table_name, chunk_data, table_config
            )
            
            # Convert to legacy format for compatibility
            chunk_last_timestamp = watermark_data.last_timestamp or safe_last_timestamp
            chunk_last_id = watermark_data.last_id or safe_last_id
            
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
            table_name: Name of the table to validate (may be scoped for v1.2.0)
        
        Returns:
            True if table has required columns, False otherwise
        """
        try:
            # Extract actual MySQL table name from potentially scoped name
            mysql_table_name = self._extract_mysql_table_name(table_name)
            
            cursor.execute(f"DESCRIBE {mysql_table_name}")
            describe_results = cursor.fetchall()
            
            # Handle both dictionary and tuple cursors
            if describe_results and isinstance(describe_results[0], dict):
                columns = [row['Field'] for row in describe_results]
            else:
                columns = [row[0] for row in describe_results]
            
            # Check for required columns (flexible timestamp and ID column names)
            missing_columns = []
            
            # Check timestamp columns with flexible naming (only required for timestamp-based strategies)
            timestamp_candidates = ['updated_at', 'update_at', 'last_modified', 'modified_at']
            has_timestamp_column = any(col in columns for col in timestamp_candidates)
            
            # Check if this is an id_only table by checking existing watermark strategy
            is_id_only_table = False
            try:
                from src.core.s3_watermark_manager import S3WatermarkManager
                from src.config.settings import AppConfig
                config = AppConfig()
                watermark_manager = S3WatermarkManager(config)
                watermark = watermark_manager.get_table_watermark(table_name)
                if watermark and watermark.backup_strategy == 'id_only':
                    is_id_only_table = True
                    self.logger.logger.info(f"Table {table_name} detected as id_only strategy from watermark")
            except:
                pass
            
            # Only require timestamp column for non-id_only tables
            if not has_timestamp_column and not is_id_only_table:
                missing_columns.append('timestamp column (tried: updated_at, update_at, last_modified, modified_at)')
            
            # Check ID columns with flexible naming  
            id_candidates = ['id', 'ID', 'Id', 'pk_id', 'primary_id']
            has_id_column = any(col in columns for col in id_candidates)
            if not has_id_column:
                missing_columns.append('ID column (tried: id, ID, Id, pk_id)')
            
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
            
            # Use v2.0 direct API for better performance
            try:
                self.watermark_manager.simple_manager.update_mysql_state(
                    table_name=table_name,
                    timestamp=watermark_data.get('last_mysql_data_timestamp'),
                    id=watermark_data.get('last_processed_id'),
                    status=watermark_data.get('mysql_status', 'success'),
                    error=watermark_data.get('last_error'),
                    rows_extracted=total_rows_processed
                )
                success = True
            except Exception as e:
                self.logger.logger.error(f"Failed to update watermark for {table_name}: {e}")
                success = False
            
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
            
            # Use v2.0 direct API for better performance
            try:
                self.watermark_manager.simple_manager.update_mysql_state(
                    table_name=table_name,
                    timestamp=watermark_data.get('last_mysql_data_timestamp'),
                    id=watermark_data.get('last_processed_id'),
                    status=watermark_data.get('mysql_status', 'success'),
                    error=watermark_data.get('last_error'),
                    rows_extracted=total_rows_processed  # Track incremental progress
                )
                success = True
            except Exception as e:
                self.logger.logger.error(f"Failed to update watermark for {table_name}: {e}")
                success = False
            
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
        Set final watermark using absolute row count.
        
        SIMPLIFIED: Always uses the provided row count as absolute value.
        No accumulation, no modes, no complexity.
        
        Args:
            table_name: Name of the table
            extraction_time: Time of extraction
            max_data_timestamp: Latest data timestamp processed
            last_processed_id: Last processed ID
            session_rows_processed: Rows processed in this session (absolute count)
            status: Final status ('success' or 'failed')
            error_message: Optional error message for failed status
        """
        try:
            
            # Get actual S3 file count from S3 directly (not in-memory stats)
            actual_s3_files = self._count_actual_s3_files(table_name)
            
            self.logger.logger.info(
                "S3 file count tracking for watermark update",
                table_name=table_name,
                s3_files_created=actual_s3_files,
                session_rows=session_rows_processed,
                s3_files_tracked=True
            )
            
            # Use simplified watermark update with absolute count
            success = self.watermark_manager.update_mysql_watermark(
                table_name=table_name,
                extraction_time=extraction_time,
                max_data_timestamp=max_data_timestamp,
                last_processed_id=last_processed_id,
                rows_extracted=session_rows_processed,
                status=status,
                backup_strategy='row_based',
                s3_file_count=actual_s3_files,
                error_message=error_message
            )
            
            self.logger.logger.info(
                "Final watermark updated with absolute count",
                table_name=table_name,
                session_rows=session_rows_processed,
                status=status
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
    
    def _get_table_config(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table configuration from pipeline config"""
        if hasattr(self, 'pipeline_config') and self.pipeline_config:
            tables = self.pipeline_config.get('tables', {})
            
            # Try exact match first
            if table_name in tables:
                return tables.get(table_name)
            
            # Extract unscoped table name for scoped lookups (US_DW_UNIDW_SSH:unidw.table -> unidw.table)
            unscoped_name = self._extract_mysql_table_name(table_name)
            if unscoped_name in tables:
                return tables.get(unscoped_name)
        
        return None
    
    def _get_table_schema(self, cursor, table_name: str) -> Dict[str, str]:
        """Get table schema from database"""
        try:
            mysql_table_name = self._extract_mysql_table_name(table_name)
            cursor.execute(f"DESCRIBE {mysql_table_name}")
            describe_results = cursor.fetchall()
            
            # Handle both dictionary and tuple cursors
            if describe_results and isinstance(describe_results[0], dict):
                schema = {row['Field']: row['Type'] for row in describe_results}
            else:
                schema = {row[0]: row[1] for row in describe_results}
            
            return schema
        except Exception as e:
            self.logger.logger.error(f"Failed to get table schema for {table_name}: {e}")
            return {}
    
    def validate_table_exists(self, cursor, table_name: str) -> bool:
        """
        Override BaseBackupStrategy validation to use CDC system
        
        Args:
            cursor: Database cursor
            table_name: Name of the table to validate (may be scoped for v1.2.0)
        
        Returns:
            True if table is valid, False otherwise
        """
        try:
            # Extract actual MySQL table name from potentially scoped name
            mysql_table_name = self._extract_mysql_table_name(table_name)
            
            self.logger.logger.debug("Validating table structure", 
                                   scoped_table_name=table_name,
                                   mysql_table_name=mysql_table_name)
            
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{mysql_table_name.split('.')[-1]}'")
            if not cursor.fetchone():
                self.logger.error_occurred(
                    Exception(f"Table {mysql_table_name} does not exist"), 
                    "table_validation"
                )
                return False
            
            # Initialize CDC integration if not already done
            if not self.cdc_integration:
                try:
                    pipeline_config = getattr(self, 'pipeline_config', None)
                    self.cdc_integration = create_cdc_integration(None, pipeline_config)
                except Exception as e:
                    self.logger.logger.error(f"Failed to initialize CDC integration: {e}")
                    return False
            
            # Use CDC system for column validation instead of hardcoded requirements
            table_config = self._get_table_config(table_name)
            table_schema = self._get_table_schema(cursor, table_name)
            
            is_valid, issues = self.cdc_integration.validate_table_for_cdc(
                table_name, table_schema, table_config
            )
            
            if not is_valid:
                self.logger.logger.error(
                    "CDC validation failed for table",
                    table_name=table_name,
                    issues=issues
                )
                return False
            
            self.logger.logger.info(
                "Table validation successful using CDC system",
                table_name=table_name,
                column_count=len(table_schema)
            )
            
            return True
            
        except Exception as e:
            self.logger.error_occurred(e, "table_validation")
            return False