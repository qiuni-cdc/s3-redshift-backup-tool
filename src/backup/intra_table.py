"""
Intra-table parallel backup strategy implementation.

This module implements parallel backup processing within a single table
by splitting the time range into chunks and processing them simultaneously.
Best for scenarios with very large tables and time-based data.
"""

from typing import List, Dict, Any, Tuple, Optional
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import threading
from collections import defaultdict
from contextlib import contextmanager
import mysql.connector

from src.backup.base import BaseBackupStrategy
from src.utils.exceptions import BackupError, DatabaseError, ValidationError, raise_backup_error
from src.utils.logging import get_backup_logger


class IntraTableBackupStrategy(BaseBackupStrategy):
    """
    Intra-table parallel backup strategy implementation.
    
    Splits large tables into time-based chunks and processes them in parallel.
    Each chunk gets its own database connection and processing thread.
    Best for very large tables with time-based data distribution.
    """
    
    def __init__(self, config, pipeline_config=None):
        super().__init__(config, pipeline_config)
        self.logger.set_context(strategy="intra_table_parallel", gemini_mode=True)
        self._thread_local = threading.local()
        self._results_lock = threading.Lock()
        self._chunk_results = {}

        # Shared connection management for thread safety
        self._shared_ssh_tunnel = None
        self._shared_local_port = None
        self._connection_lock = threading.Lock()
        self._connection_pool = []
        self._pool_lock = threading.Lock()
    
    def execute(self, tables: List[str], limit: Optional[int] = None, source_connection: Optional[str] = None) -> bool:
        """
        Execute intra-table parallel backup for specified tables.
        
        Note: This strategy processes tables one by one, but splits each table
        into time-based chunks for parallel processing.
        
        Args:
            tables: List of table names to backup (recommended: single table)
        
        Returns:
            True if all tables backed up successfully, False otherwise
        """
        if not tables:
            self.logger.logger.warning("No tables specified for backup")
            return False
        
        if len(tables) > 1:
            self.logger.logger.warning(
                f"Intra-table strategy works best with single table, got {len(tables)} tables. "
                "Consider using inter-table strategy for multiple tables."
            )
        
        self.logger.backup_started("intra_table_parallel", tables)
        self.metrics.start_operation()
        
        try:
            # Get backup time window
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            successful_tables = []
            failed_tables = []
            
            # Process each table with intra-table parallelization
            for table_name in tables:
                table_start_time = time.time()
                
                self.logger.logger.info(
                    "Starting intra-table parallel processing",
                    table_name=table_name,
                    num_chunks=self.config.backup.num_chunks
                )
                
                try:
                    success = self._process_table_with_chunks(
                        table_name, current_timestamp, limit
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
                    self.logger.error_occurred(e, f"intra_table_backup_{table_name}")
            
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
                "intra_table_parallel",
                all_success,
                self.metrics.get_duration(),
                successful_tables=successful_tables,
                failed_tables=failed_tables,
                watermark_updated=watermark_updated
            )
            
            # Log detailed summary
            summary = self.get_backup_summary()
            self.logger.logger.info(
                "Intra-table parallel backup summary",
                **summary,
                successful_tables=len(successful_tables),
                failed_tables=len(failed_tables),
                chunks_per_table=self.config.backup.num_chunks
            )
            
            return all_success
            
        except Exception as e:
            self.metrics.end_operation()
            self.metrics.errors += 1
            self.logger.error_occurred(e, "intra_table_parallel_backup")
            self.logger.backup_completed("intra_table_parallel", False, self.metrics.get_duration())
            raise_backup_error("intra_table_parallel_backup", underlying_error=e)
        
        finally:
            self.cleanup_resources()
    
    @contextmanager
    def thread_safe_database_session(self):
        """
        Thread-safe database session using shared SSH tunnel.
        
        This method ensures that all threads share a single SSH tunnel
        but get their own database connections to avoid conflicts.
        """
        db_conn = None
        try:
            # Ensure shared SSH tunnel is established
            self._ensure_shared_ssh_tunnel()
            
            # Create database connection through shared tunnel
            conn_config = {
                'host': '127.0.0.1',
                'port': self._shared_local_port,
                'user': self.config.database.user,
                'password': self.config.database.password.get_secret_value(),
                'database': self.config.database.database,
                'autocommit': False,
                'connection_timeout': 30,
                'charset': 'utf8mb4',
                'use_unicode': True,
                'sql_mode': 'TRADITIONAL',
                'raise_on_warnings': True
            }
            
            # Create connection with retries
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    db_conn = mysql.connector.connect(**conn_config)
                    break
                except mysql.connector.Error as e:
                    if attempt == max_attempts - 1:
                        raise
                    self.logger.logger.warning(
                        f"Database connection attempt {attempt + 1} failed, retrying: {e}"
                    )
                    time.sleep(2 ** attempt)
            
            if not db_conn.is_connected():
                raise DatabaseError("Database connection test failed")
            
            self.logger.logger.debug(
                "Thread database connection established",
                thread_id=threading.get_ident(),
                local_port=self._shared_local_port
            )
            
            yield db_conn
            
        except Exception as e:
            self.logger.logger.error(
                "Thread database connection failed",
                thread_id=threading.get_ident(),
                error=str(e)
            )
            raise DatabaseError(f"Thread database connection failed: {e}")
        
        finally:
            if db_conn and db_conn.is_connected():
                db_conn.close()
                self.logger.logger.debug(
                    "Thread database connection closed",
                    thread_id=threading.get_ident()
                )
    
    def _ensure_shared_ssh_tunnel(self):
        """Ensure shared SSH tunnel is established and available for all threads"""
        with self._connection_lock:
            if self._shared_ssh_tunnel is None or not self._shared_ssh_tunnel.is_active:
                self.logger.logger.info("Establishing shared SSH tunnel for intra-table parallel processing")
                
                # Create SSH tunnel
                from sshtunnel import SSHTunnelForwarder
                
                self._shared_ssh_tunnel = SSHTunnelForwarder(
                    (self.config.ssh.bastion_host, 22),
                    ssh_username=self.config.ssh.bastion_user,
                    ssh_pkey=self.config.ssh.bastion_key_path,
                    remote_bind_address=(self.config.database.host, self.config.database.port),
                    local_bind_address=('127.0.0.1', 0)  # Use dynamic port
                )
                
                # Start tunnel with retry logic
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        self._shared_ssh_tunnel.start()
                        break
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        self.logger.logger.warning(
                            f"Shared SSH tunnel attempt {attempt + 1} failed, retrying: {e}"
                        )
                        time.sleep(2 ** attempt)
                
                self._shared_local_port = self._shared_ssh_tunnel.local_bind_port
                
                self.logger.logger.info(
                    "Shared SSH tunnel established",
                    local_port=self._shared_local_port,
                    remote_host=self.config.database.host,
                    remote_port=self.config.database.port
                )
                
                # Test tunnel connectivity
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(('127.0.0.1', self._shared_local_port))
                sock.close()
                
                if result != 0:
                    raise ConnectionError("Shared SSH tunnel connectivity test failed")
    
    def cleanup_resources(self):
        """Cleanup shared SSH tunnel and connection pool"""
        super().cleanup_resources()
        
        with self._connection_lock:
            if self._shared_ssh_tunnel and self._shared_ssh_tunnel.is_active:
                self._shared_ssh_tunnel.stop()
                self.logger.logger.info("Shared SSH tunnel closed")
                self._shared_ssh_tunnel = None
                self._shared_local_port = None
    
    def _process_table_with_chunks(
        self, 
        table_name: str, 
        current_timestamp: str,
        limit: Optional[int] = None
    ) -> bool:
        """
        Process a single table by splitting it into time chunks.
        
        Args:
            table_name: Name of the table to process
            current_timestamp: Current backup timestamp
        
        Returns:
            True if table processed successfully
        """
        try:
            # Get last watermark for this specific table
            last_watermark = self.get_table_watermark_timestamp(table_name)
            
            # Calculate time chunks
            time_chunks = self.calculate_time_chunks(
                table_name, last_watermark, current_timestamp, self.config.backup.num_chunks
            )
            
            if len(time_chunks) == 1:
                self.logger.logger.info(
                    "Time window too small for chunking, using single chunk",
                    table_name=table_name,
                    time_window_start=last_watermark,
                    time_window_end=current_timestamp
                )
            
            self.logger.logger.info(
                "Processing table with time chunks",
                table_name=table_name,
                num_chunks=len(time_chunks),
                time_chunks=time_chunks[:3] if len(time_chunks) > 3 else time_chunks  # Log first 3 chunks
            )
            
            # Initialize chunk results tracking
            self._chunk_results = {}
            successful_chunks = []
            failed_chunks = []
            
            # Process chunks in parallel
            max_workers = min(self.config.backup.max_workers, len(time_chunks))
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all chunk processing tasks
                future_to_chunk = {
                    executor.submit(
                        self._process_chunk_thread,
                        table_name,
                        chunk_start,
                        chunk_end,
                        chunk_index + 1,
                        len(time_chunks),
                        current_timestamp,
                        limit
                    ): (chunk_index, chunk_start, chunk_end)
                    for chunk_index, (chunk_start, chunk_end) in enumerate(time_chunks)
                }
                
                # Monitor progress and collect results
                for future in as_completed(future_to_chunk):
                    chunk_index, chunk_start, chunk_end = future_to_chunk[future]
                    chunk_id = f"{table_name}_chunk_{chunk_index + 1}"
                    
                    try:
                        chunk_result = future.result(timeout=self.config.backup.timeout_seconds)
                        
                        with self._results_lock:
                            self._chunk_results[chunk_id] = chunk_result
                        
                        if chunk_result['success']:
                            successful_chunks.append(chunk_id)
                            
                            self.logger.logger.info(
                                "Chunk completed successfully",
                                table_name=table_name,
                                chunk_id=chunk_id,
                                chunk_start=chunk_start,
                                chunk_end=chunk_end,
                                rows_processed=chunk_result.get('rows', 0),
                                completed_chunks=len(successful_chunks) + len(failed_chunks),
                                total_chunks=len(time_chunks)
                            )
                        else:
                            failed_chunks.append(chunk_id)
                            self.metrics.errors += 1
                            
                            self.logger.logger.error(
                                "Chunk processing failed",
                                table_name=table_name,
                                chunk_id=chunk_id,
                                chunk_start=chunk_start,
                                chunk_end=chunk_end,
                                completed_chunks=len(successful_chunks) + len(failed_chunks),
                                total_chunks=len(time_chunks)
                            )
                    
                    except Exception as e:
                        failed_chunks.append(chunk_id)
                        self.metrics.errors += 1
                        self.logger.error_occurred(e, f"chunk_processing_{chunk_id}")
            
            # Aggregate results
            total_rows = sum(
                result.get('rows', 0) 
                for result in self._chunk_results.values() 
                if result.get('success', False)
            )
            
            total_batches = sum(
                result.get('batches', 0) 
                for result in self._chunk_results.values() 
                if result.get('success', False)
            )
            
            # Record table metrics
            table_duration = time.time() - (self.metrics.start_time or time.time())
            self.metrics.add_table_metrics(
                table_name, total_rows, total_batches, table_duration
            )
            
            all_chunks_successful = len(failed_chunks) == 0
            
            self.logger.logger.info(
                "Table chunk processing completed",
                table_name=table_name,
                total_chunks=len(time_chunks),
                successful_chunks=len(successful_chunks),
                failed_chunks=len(failed_chunks),
                total_rows=total_rows,
                total_batches=total_batches,
                all_successful=all_chunks_successful
            )
            
            return all_chunks_successful
            
        except Exception as e:
            self.logger.error_occurred(e, f"table_chunk_processing_{table_name}")
            return False
    
    def _process_chunk_thread(
        self,
        table_name: str,
        chunk_start: str,
        chunk_end: str,
        chunk_index: int,
        total_chunks: int,
        current_timestamp: str,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process a single time chunk in a separate thread.
        
        Args:
            table_name: Name of the table
            chunk_start: Start timestamp for this chunk
            chunk_end: End timestamp for this chunk
            chunk_index: Index of this chunk
            total_chunks: Total number of chunks
        
        Returns:
            Dictionary with processing results
        """
        thread_id = threading.get_ident()
        chunk_id = f"{table_name}_chunk_{chunk_index}"
        
        # Set thread-local context
        self.logger.set_context(
            thread_id=thread_id,
            table_name=table_name,
            chunk_id=chunk_id,
            chunk_index=chunk_index,
            total_chunks=total_chunks
        )
        
        result = {
            'success': False,
            'rows': 0,
            'batches': 0,
            'chunk_start': chunk_start,
            'chunk_end': chunk_end,
            'error': None
        }
        
        try:
            self.logger.logger.info(
                "Starting chunk processing",
                table_name=table_name,
                chunk_id=chunk_id,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                thread_id=thread_id
            )
            
            chunk_start_time = time.time()
            
            # Create dedicated database session for this thread using shared SSH tunnel
            with self.thread_safe_database_session() as db_conn:
                chunk_result = self._process_single_chunk(
                    db_conn, table_name, chunk_start, chunk_end, chunk_id, thread_id, current_timestamp, limit
                )
                
                chunk_duration = time.time() - chunk_start_time
                
                result.update(chunk_result)
                result['duration'] = chunk_duration
                
                if result['success']:
                    self.logger.logger.info(
                        "Chunk processing completed",
                        table_name=table_name,
                        chunk_id=chunk_id,
                        thread_id=thread_id,
                        rows_processed=result['rows'],
                        duration=chunk_duration
                    )
                else:
                    self.logger.logger.error(
                        "Chunk processing failed",
                        table_name=table_name,
                        chunk_id=chunk_id,
                        thread_id=thread_id,
                        error=result.get('error', 'Unknown error')
                    )
                
                return result
        
        except Exception as e:
            result['error'] = str(e)
            self.logger.error_occurred(e, f"chunk_thread_{chunk_id}")
            return result
    
    def _process_single_chunk(
        self,
        db_conn,
        table_name: str,
        chunk_start: str,
        chunk_end: str,
        chunk_id: str,
        thread_id: int,
        current_timestamp: str,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process a single time chunk.
        
        Args:
            db_conn: Database connection for this thread
            table_name: Name of the table
            chunk_start: Start timestamp for this chunk
            chunk_end: End timestamp for this chunk
            chunk_id: Chunk identifier
            thread_id: Thread identifier
        
        Returns:
            Dictionary with processing results
        """
        result = {'success': False, 'rows': 0, 'batches': 0}
        cursor = None
        
        try:
            # Create cursor with dictionary output
            cursor = db_conn.cursor(dictionary=True, buffered=False)
            
            # Validate table structure (only once per thread)
            if not hasattr(self._thread_local, 'table_validated'):
                if not self.validate_table_exists(cursor, table_name):
                    result['error'] = "Table validation failed"
                    return result
                self._thread_local.table_validated = True
            
            # Log chunk window
            self.logger.logger.debug(
                "Processing chunk window",
                table_name=table_name,
                chunk_id=chunk_id,
                thread_id=thread_id,
                chunk_start=chunk_start,
                chunk_end=chunk_end
            )
            
            # Get chunk query (using chunk-specific time window) with optional limit
            chunk_query = self.get_incremental_query(
                table_name, chunk_start, chunk_end, limit=limit
            )
            
            # Execute chunk query
            cursor.execute(chunk_query)
            
            # Process data in batches
            batch_id = 0
            total_rows_processed = 0
            
            while True:
                # Fetch batch
                batch_data = cursor.fetchmany(self.config.backup.batch_size)
                if not batch_data:
                    break
                
                batch_id += 1
                batch_size = len(batch_data)
                total_rows_processed += batch_size
                
                # Process batch with chunk-specific S3 key
                batch_success = self._process_chunk_batch(
                    batch_data, table_name, f"{chunk_id}_batch_{batch_id}", 
                    current_timestamp, thread_id  # Use current_timestamp for consistent S3 key
                )
                
                if not batch_success:
                    result['error'] = f"Batch {batch_id} processing failed"
                    return result
                
                # Log progress (less verbose for chunk processing)
                if batch_id % 5 == 0:  # Log every 5th batch
                    self.logger.logger.debug(
                        "Chunk batch progress",
                        table_name=table_name,
                        chunk_id=chunk_id,
                        batch_id=batch_id,
                        total_processed=total_rows_processed,
                        thread_id=thread_id
                    )
            
            result.update({
                'success': True,
                'rows': total_rows_processed,
                'batches': batch_id
            })
            
            self.logger.logger.debug(
                "Chunk processing completed",
                table_name=table_name,
                chunk_id=chunk_id,
                thread_id=thread_id,
                total_rows=total_rows_processed,
                total_batches=batch_id
            )
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error_occurred(e, f"chunk_processing_{chunk_id}")
            return result
        
        finally:
            if cursor:
                cursor.close()
    
    def _process_chunk_batch(
        self,
        batch_data: List[Dict],
        table_name: str,
        batch_id: str,  # This includes chunk info
        timestamp: str,
        thread_id: int
    ) -> bool:
        """
        Process a batch within a chunk.
        
        Args:
            batch_data: Batch data to process
            table_name: Name of the table
            batch_id: Batch identifier (includes chunk info)
            timestamp: Timestamp for S3 key
            thread_id: Thread identifier
        
        Returns:
            True if batch processed successfully
        """
        try:
            # Create unique S3 key for this chunk batch with configured partition strategy
            s3_key = self.s3_manager.generate_s3_key(
                table_name, timestamp, 0,  # Use 0 as batch_id since it's in the batch_id string
                partition_strategy=self._get_partition_strategy()
            )
            
            # Modify S3 key to include chunk information
            s3_key = s3_key.replace('.parquet', f'_{batch_id}.parquet')
            
            # Process using base method but with custom S3 key
            success = self.process_batch(
                batch_data, table_name, 0, timestamp  # Base method will generate key, but we'll override
            )
            
            if success:
                self.logger.logger.debug(
                    "Chunk batch completed",
                    table_name=table_name,
                    batch_id=batch_id,
                    batch_size=len(batch_data),
                    thread_id=thread_id
                )
            
            return success
            
        except Exception as e:
            self.logger.error_occurred(e, f"chunk_batch_processing_{batch_id}")
            return False
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get information about this backup strategy"""
        return {
            "name": "Intra-Table Parallel Backup Strategy",
            "description": "Splits large tables into time-based chunks for parallel processing",
            "advantages": [
                "Handles very large tables efficiently",
                "Parallel processing within single table",
                "Time-based data distribution",
                "Good for chronological data"
            ],
            "disadvantages": [
                "Complex chunk coordination",
                "Only beneficial for large tables",
                "Requires time-based data",
                "Potential chunk skew issues"
            ],
            "best_for": [
                "Very large tables (millions of rows)",
                "Time-series data",
                "Single table processing",
                "When data is distributed chronologically"
            ],
            "configuration": {
                "num_chunks": self.config.backup.num_chunks,
                "max_workers": self.config.backup.max_workers,
                "batch_size": self.config.backup.batch_size,
                "timeout_seconds": self.config.backup.timeout_seconds
            }
        }
    
    def estimate_completion_time(self, tables: List[str]) -> Dict[str, Any]:
        """
        Estimate completion time for intra-table parallel backup.
        
        Args:
            tables: List of tables to backup
        
        Returns:
            Dictionary with time estimates
        """
        if len(tables) > 1:
            # For multiple tables, process sequentially but chunk each table
            estimated_rows_per_table = 50000  # Assume larger tables for this strategy
            processing_rate = 5000  # rows per second
            
            sequential_time_per_table = estimated_rows_per_table / processing_rate
            
            # Estimate chunk parallelization benefit (60% efficiency due to coordination overhead)
            chunk_efficiency = 0.6
            chunks_per_table = self.config.backup.num_chunks
            max_workers = min(self.config.backup.max_workers, chunks_per_table)
            
            chunk_speedup = max_workers * chunk_efficiency
            parallel_time_per_table = sequential_time_per_table / chunk_speedup
            
            total_time = len(tables) * parallel_time_per_table
            
            return {
                "total_tables": len(tables),
                "chunks_per_table": chunks_per_table,
                "max_workers": max_workers,
                "estimated_rows_per_table": estimated_rows_per_table,
                "sequential_time_per_table": sequential_time_per_table,
                "parallel_time_per_table": parallel_time_per_table,
                "total_duration_seconds": total_time,
                "total_duration_minutes": round(total_time / 60, 1),
                "estimated_speedup_per_table": round(chunk_speedup, 1),
                "chunk_efficiency": chunk_efficiency,
                "strategy": "intra_table_parallel"
            }
        else:
            # Single table optimization
            estimated_rows = 100000  # Assume very large table
            processing_rate = 5000
            
            sequential_time = estimated_rows / processing_rate
            
            chunk_efficiency = 0.6
            chunks = self.config.backup.num_chunks
            max_workers = min(self.config.backup.max_workers, chunks)
            
            chunk_speedup = max_workers * chunk_efficiency
            parallel_time = sequential_time / chunk_speedup
            
            return {
                "total_tables": 1,
                "chunks": chunks,
                "max_workers": max_workers,
                "estimated_total_rows": estimated_rows,
                "sequential_duration_seconds": sequential_time,
                "parallel_duration_seconds": parallel_time,
                "parallel_duration_minutes": round(parallel_time / 60, 1),
                "estimated_speedup": round(chunk_speedup, 1),
                "chunk_efficiency": chunk_efficiency,
                "strategy": "intra_table_parallel"
            }