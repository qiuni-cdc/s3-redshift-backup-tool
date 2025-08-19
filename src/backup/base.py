"""
Base backup strategy implementation.

This module provides the abstract base class and common functionality
for all backup strategies in the S3 to Redshift backup system.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Generator
import time
import gc
import psutil
import os
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
from contextlib import contextmanager

from src.config.settings import AppConfig
from src.core.connections import ConnectionManager
from src.core.s3_manager import S3Manager
from src.core.s3_watermark_manager import S3WatermarkManager
from src.config.schemas import get_table_schema, validate_table_data
from src.utils.validation import validate_data, ValidationResult
from src.utils.exceptions import (
    BackupError, 
    DatabaseError, 
    ValidationError,
    raise_backup_error
)
from src.utils.logging import BackupLogger, get_backup_logger


class BackupMetrics:
    """Container for backup operation metrics"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.tables_processed = 0
        self.total_rows = 0
        self.total_batches = 0
        self.total_bytes = 0
        self.errors = 0
        self.warnings = 0
        self.skipped_tables = 0
        self.per_table_metrics = {}
    
    def start_operation(self):
        """Mark the start of backup operation"""
        self.start_time = time.time()
    
    def end_operation(self):
        """Mark the end of backup operation"""
        self.end_time = time.time()
    
    def add_table_metrics(self, table_name: str, rows: int, batches: int, 
                         duration: float, bytes_uploaded: int = 0):
        """Add metrics for a specific table"""
        self.per_table_metrics[table_name] = {
            'rows': rows,
            'batches': batches,
            'duration': duration,
            'bytes': bytes_uploaded,
            'rows_per_second': rows / duration if duration > 0 else 0
        }
        self.total_rows += rows
        self.total_batches += batches
        self.total_bytes += bytes_uploaded
    
    def get_duration(self) -> float:
        """Get total operation duration in seconds"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        duration = self.get_duration()
        return {
            'duration_seconds': duration,
            'tables_processed': self.tables_processed,
            'total_rows': self.total_rows,
            'total_batches': self.total_batches,
            'total_bytes': self.total_bytes,
            'total_mb': round(self.total_bytes / 1024 / 1024, 2),
            'errors': self.errors,
            'warnings': self.warnings,
            'skipped_tables': self.skipped_tables,
            'avg_rows_per_second': self.total_rows / duration if duration > 0 else 0,
            'per_table_metrics': self.per_table_metrics
        }


class MemoryManager:
    """Memory management utilities for backup operations"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.memory_limit_bytes = self.config.backup.memory_limit_mb * 1024 * 1024
        self.gc_threshold = self.config.backup.gc_threshold
        self.check_interval = self.config.backup.memory_check_interval
        self.batch_count = 0
        self.process = psutil.Process(os.getpid())
        
    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics"""
        memory_info = self.process.memory_info()
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': self.process.memory_percent(),
            'available_mb': psutil.virtual_memory().available / 1024 / 1024
        }
    
    def check_memory_usage(self, batch_number: int) -> bool:
        """
        Check if memory usage is within limits.
        
        Returns:
            True if memory usage is acceptable, False if over limit
        """
        if batch_number % self.check_interval != 0:
            return True
            
        memory = self.get_memory_usage()
        memory_limit_mb = self.memory_limit_bytes / 1024 / 1024
        
        if memory['rss_mb'] > memory_limit_mb:
            logger.warning(
                "Memory usage approaching limit",
                current_mb=round(memory['rss_mb'], 2),
                limit_mb=memory_limit_mb,
                percent=round(memory['percent'], 2)
            )
            return False
        
        return True
    
    def force_gc_if_needed(self, batch_number: int):
        """Force garbage collection based on batch count and memory usage"""
        self.batch_count += 1
        
        if self.batch_count >= self.gc_threshold:
            logger.debug("Forcing garbage collection", batches_processed=self.batch_count)
            gc.collect()
            self.batch_count = 0
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get comprehensive memory statistics"""
        memory = self.get_memory_usage()
        return {
            'current_usage_mb': round(memory['rss_mb'], 2),
            'memory_limit_mb': self.memory_limit_bytes / 1024 / 1024,
            'usage_percent': round(memory['percent'], 2),
            'available_mb': round(memory['available_mb'], 2),
            'gc_threshold': self.gc_threshold,
            'batches_since_gc': self.batch_count
        }


class BaseBackupStrategy(ABC):
    """
    Abstract base class for all backup strategies.
    
    Provides common functionality for incremental backup operations including
    connection management, data processing, validation, and error handling.
    """
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection_manager = ConnectionManager(config)
        self.s3_manager = S3Manager(config)
        self.watermark_manager = S3WatermarkManager(config)
        self.logger = get_backup_logger()
        self.metrics = BackupMetrics()
        self.memory_manager = MemoryManager(config)
        
        # Initialize components with S3 client
        self.s3_client = None
        self._initialize_components()
        
        # Initialize flexible schema manager for any table
        from src.core.flexible_schema_manager import FlexibleSchemaManager
        self.flexible_schema_manager = FlexibleSchemaManager(self.connection_manager)
        
        # Track PoC mode usage (renamed from Gemini)
        self._poc_mode_enabled = True
        self._gemini_mode_enabled = True  # For compatibility
        self._gemini_usage_stats = {
            'schemas_discovered': 0,
            'batches_with_gemini': 0,  # Keep for compatibility
            'batches_with_fallback': 0
        }
    
    def _initialize_components(self):
        """Initialize components with shared S3 client"""
        try:
            self.s3_client = self.connection_manager.get_s3_client()
            self.s3_manager.s3_client = self.s3_client
            # S3WatermarkManager has its own S3 client initialization
            
            self.logger.connection_established("S3")
        except Exception as e:
            self.logger.error_occurred(e, "S3 client initialization")
            raise
    
    @abstractmethod
    def execute(self, tables: List[str], limit: Optional[int] = None) -> bool:
        """
        Execute backup strategy for given tables.
        
        Args:
            tables: List of table names to backup
            limit: Optional row limit per query (for testing purposes)
        
        Returns:
            True if backup successful, False otherwise
        """
        pass
    
    def get_incremental_query(
        self, 
        table_name: str, 
        last_watermark: str, 
        current_timestamp: str,
        custom_where: Optional[str] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Generate incremental query for table.
        
        Args:
            table_name: Name of the table
            last_watermark: Last backup timestamp
            current_timestamp: Current backup timestamp
            custom_where: Additional WHERE conditions
            limit: Optional LIMIT for testing large tables
        
        Returns:
            SQL query string for incremental data
        """
        where_conditions = [
            f"`update_at` > '{last_watermark}'",
            f"`update_at` <= '{current_timestamp}'"
        ]
        
        if custom_where:
            where_conditions.append(custom_where)
        
        # Apply limit if specified (for testing purposes)
        if limit is not None:
            self.logger.logger.info(
                "Applying explicit LIMIT to query",
                table_name=table_name,
                limit=limit,
                reason="explicit_limit"
            )
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY `update_at`, `ID`
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        self.logger.logger.debug(
            "Generated incremental query",
            table_name=table_name,
            last_watermark=last_watermark,
            current_timestamp=current_timestamp,
            limit=limit,
            query_preview=query[:200] + "..." if len(query) > 200 else query
        )
        
        return query
    
    def validate_table_exists(self, cursor, table_name: str) -> bool:
        """
        Validate that table exists and has required columns.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table to validate
        
        Returns:
            True if table is valid, False otherwise
        """
        try:
            self.logger.logger.debug("Validating table structure", table_name=table_name)
            
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name.split('.')[-1]}'")
            if not cursor.fetchone():
                self.logger.error_occurred(
                    Exception(f"Table {table_name} does not exist"), 
                    "table_validation"
                )
                return False
            
            # Check table structure
            cursor.execute(f"DESCRIBE {table_name}")
            describe_results = cursor.fetchall()
            # Handle both dictionary and tuple cursors
            if describe_results and isinstance(describe_results[0], dict):
                # Dictionary cursor - get the 'Field' column
                columns = [row['Field'] for row in describe_results]
            else:
                # Tuple cursor - get the first element
                columns = [row[0] for row in describe_results]
            
            # Check for required update_at column
            if 'update_at' not in columns:
                self.logger.error_occurred(
                    Exception(f"Table {table_name} missing update_at column"), 
                    "table_validation"
                )
                return False
            
            # Check for ID column (primary key)
            if 'ID' not in columns and 'id' not in columns:
                self.logger.logger.warning(
                    "Table missing ID column",
                    table_name=table_name,
                    available_columns=columns[:10]  # Log first 10 columns
                )
            
            self.logger.logger.info(
                "Table validation successful",
                table_name=table_name,
                column_count=len(columns)
            )
            
            return True
            
        except Exception as e:
            self.logger.error_occurred(e, f"table_validation_{table_name}")
            return False
    
    def get_table_row_count(
        self, 
        cursor, 
        table_name: str, 
        last_watermark: str, 
        current_timestamp: str
    ) -> int:
        """
        Get estimated row count for incremental backup window.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table
            last_watermark: Last backup timestamp
            current_timestamp: Current backup timestamp
        
        Returns:
            Estimated row count (or -1 for large tables where count is skipped)
        """
        # Skip count for known large tables to avoid timeout
        large_tables = [
            'settlement.settlement_normal_delivery_detail',
            'settlement_normal_delivery_detail'
        ]
        
        table_basename = table_name.split('.')[-1]
        if table_name in large_tables or table_basename in large_tables:
            self.logger.logger.info(
                "Skipping row count for large table",
                table_name=table_name,
                reason="large_table_optimization"
            )
            return -1  # Indicate unknown count
        
        try:
            count_query = f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE `update_at` > '{last_watermark}' 
            AND `update_at` <= '{current_timestamp}'
            """
            
            # Debug log the actual query
            self.logger.logger.debug(
                "Executing count query",
                query=count_query.strip(),
                table_name=table_name
            )
            
            cursor.execute(count_query)
            result = cursor.fetchone()
            
            if not result:
                return 0
                
            # Handle both dictionary and tuple cursor results
            if isinstance(result, dict):
                count = result['COUNT(*)'] if 'COUNT(*)' in result else list(result.values())[0]
            else:
                count = result[0]
            
            self.logger.logger.debug(
                "Retrieved table row count",
                table_name=table_name,
                row_count=count,
                window_start=last_watermark,
                window_end=current_timestamp
            )
            
            return count
            
        except Exception as e:
            self.logger.logger.error(
                "Failed to get row count",
                table_name=table_name,
                error=str(e),
                error_type=type(e).__name__
            )
            # Return -1 to indicate unknown count instead of failing
            return -1
    
    def process_batch(
        self, 
        batch_data: List[Dict], 
        table_name: str, 
        batch_id: int, 
        current_timestamp: str,
        schema: Optional[pa.Schema] = None,
        validate_data_quality: bool = True
    ) -> bool:
        """
        Process a single batch of data.
        
        Args:
            batch_data: List of row dictionaries
            table_name: Name of the table
            batch_id: Batch identifier
            current_timestamp: Current backup timestamp
            schema: Optional schema for validation
            validate_data_quality: Whether to perform data validation
        
        Returns:
            True if batch processed successfully
        """
        if not batch_data:
            return True
        
        batch_start_time = time.time()
        
        try:
            self.logger.logger.debug(
                "Processing batch",
                table_name=table_name,
                batch_id=batch_id,
                batch_size=len(batch_data)
            )
            
            # Convert to DataFrame
            df = pd.DataFrame(batch_data)
            
            # Data validation if enabled
            if validate_data_quality:
                validation_result = self._validate_batch_data(df, table_name, batch_id)
                if not validation_result.is_valid:
                    self.metrics.errors += 1
                    self.logger.error_occurred(
                        ValidationError(f"Batch validation failed: {validation_result.errors}"),
                        f"batch_validation_{table_name}"
                    )
                    return False
                
                self.metrics.warnings += len(validation_result.warnings)
            
            # Flexible Schema Mode: Use dynamic schema discovery for any table
            try:
                # Use flexible schema manager for dynamic discovery
                if not schema:
                    self.logger.logger.debug(
                        "Using flexible schema discovery",
                        table_name=table_name,
                        batch_id=batch_id
                    )
                    # Get schema dynamically for any table
                    flexible_schema, redshift_ddl = self.flexible_schema_manager.get_table_schema(table_name)
                    schema = flexible_schema
                    self._gemini_usage_stats['schemas_discovered'] += 1
                    
                    self.logger.logger.info(
                        "Dynamic schema discovered",
                        table_name=table_name,
                        columns=len(schema),
                        schema_cached=table_name in self.flexible_schema_manager._schema_cache
                    )
                
                # Generate S3 key
                s3_key = self.s3_manager.generate_s3_key(
                    table_name, current_timestamp, batch_id
                )
                
                # Use flexible upload with dynamic schema alignment
                success = self.s3_manager.upload_dataframe(
                    df,
                    s3_key,
                    schema=schema,
                    use_schema_alignment=True,  # Enable flexible alignment
                    compression="snappy"
                )
                
                if success:
                    self._gemini_usage_stats['batches_with_gemini'] += 1
                    self.logger.logger.debug(
                        "Flexible schema batch upload successful",
                        table_name=table_name,
                        batch_id=batch_id,
                        schema_fields=len(schema),
                        s3_key=s3_key
                    )
                
            except Exception as e:
                self.logger.logger.warning(
                    "Flexible schema mode failed, falling back to basic upload",
                    table_name=table_name,
                    batch_id=batch_id,
                    error=str(e)
                )
                
                # Fallback to basic upload without PoC features
                table = pa.Table.from_pandas(df)
                s3_key = self.s3_manager.generate_s3_key(
                    table_name, current_timestamp, batch_id
                )
                success = self.s3_manager.upload_parquet(table, s3_key)
                
                if success:
                    self._gemini_usage_stats['batches_with_fallback'] += 1
            
            if success:
                batch_duration = time.time() - batch_start_time
                
                self.logger.batch_processed(
                    table_name, batch_id, len(batch_data), s3_key
                )
                
                # Update metrics - estimate bytes from DataFrame size
                batch_bytes = len(df) * len(df.columns) * 8  # Rough estimate: 8 bytes per cell
                self.metrics.total_bytes += batch_bytes
                
                return True
            else:
                self.metrics.errors += 1
                self.logger.error_occurred(
                    Exception(f"S3 upload failed for batch {batch_id}"),
                    f"s3_upload_{table_name}"
                )
                return False
                
        except Exception as e:
            self.metrics.errors += 1
            self.logger.error_occurred(e, f"batch_processing_{table_name}")
            raise_backup_error(
                "batch_processing",
                table_name=table_name,
                batch_id=batch_id,
                underlying_error=e
            )
    
    def _validate_batch_data(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        batch_id: int
    ) -> ValidationResult:
        """Validate batch data quality"""
        try:
            # Use comprehensive data validation
            validation_result = validate_data(df, table_name)
            
            # Log validation summary
            if not validation_result.is_valid:
                self.logger.logger.error(
                    "Batch validation failed",
                    table_name=table_name,
                    batch_id=batch_id,
                    errors=validation_result.errors[:3],  # Log first 3 errors
                    error_count=len(validation_result.errors)
                )
            elif validation_result.warnings:
                self.logger.logger.warning(
                    "Batch validation warnings",
                    table_name=table_name,
                    batch_id=batch_id,
                    warnings=validation_result.warnings[:3],  # Log first 3 warnings
                    warning_count=len(validation_result.warnings)
                )
            
            return validation_result
            
        except Exception as e:
            # Create a failed validation result
            result = ValidationResult(
                is_valid=False,
                errors=[f"Validation process failed: {e}"],
                warnings=[],
                row_count=len(df),
                column_count=len(df.columns),
                null_counts={},
                type_issues=[],
                constraint_violations=[]
            )
            return result
    
    @contextmanager
    def database_session(self):
        """Context manager for database operations"""
        try:
            with self.connection_manager.ssh_tunnel() as local_port:
                with self.connection_manager.database_connection(local_port) as db_conn:
                    self.logger.connection_established("Database", host="localhost")
                    yield db_conn
        except Exception as e:
            self.logger.error_occurred(e, "database_session")
            raise
        finally:
            self.logger.connection_closed("Database")
    
    def calculate_time_chunks(
        self, 
        table_name: str, 
        last_watermark: str, 
        current_timestamp: str, 
        num_chunks: int
    ) -> List[Tuple[str, str]]:
        """
        Calculate time-based chunks for parallel processing.
        
        Args:
            table_name: Name of the table
            last_watermark: Start timestamp
            current_timestamp: End timestamp
            num_chunks: Number of chunks to create
        
        Returns:
            List of (start_time, end_time) tuples
        """
        try:
            start_dt = datetime.strptime(last_watermark, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S')
            
            total_duration = end_dt - start_dt
            chunk_duration = total_duration / num_chunks
            
            chunks = []
            current_start = start_dt
            
            for i in range(num_chunks):
                chunk_end = current_start + chunk_duration
                if i == num_chunks - 1:  # Last chunk
                    chunk_end = end_dt  # Ensure we don't miss any data due to rounding
                
                chunks.append((
                    current_start.strftime('%Y-%m-%d %H:%M:%S'),
                    chunk_end.strftime('%Y-%m-%d %H:%M:%S')
                ))
                
                current_start = chunk_end
            
            self.logger.logger.debug(
                "Calculated time chunks",
                table_name=table_name,
                num_chunks=len(chunks),
                total_duration_hours=total_duration.total_seconds() / 3600,
                chunk_duration_hours=chunk_duration.total_seconds() / 3600
            )
            
            return chunks
            
        except Exception as e:
            self.logger.error_occurred(e, f"time_chunk_calculation_{table_name}")
            # Fallback: return single chunk
            return [(last_watermark, current_timestamp)]
    
    def update_watermarks(
        self, 
        table_name: str,
        extraction_time: datetime,
        max_data_timestamp: Optional[datetime] = None,
        rows_extracted: int = 0,
        status: str = 'success',
        s3_file_count: int = 0,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update S3-based watermarks after successful backup.
        
        Args:
            table_name: Name of the table that was backed up
            extraction_time: When the backup process ran
            max_data_timestamp: Maximum update_at from extracted data
            rows_extracted: Number of rows extracted
            status: Status (pending, success, failed)
            s3_file_count: Number of S3 files created
            error_message: Error message if backup failed
        
        Returns:
            True if watermarks updated successfully
        """
        try:
            success = self.watermark_manager.update_mysql_watermark(
                table_name=table_name,
                extraction_time=extraction_time,
                max_data_timestamp=max_data_timestamp,
                rows_extracted=rows_extracted,
                status=status,
                backup_strategy=self.__class__.__name__.replace('BackupStrategy', '').lower(),
                s3_file_count=s3_file_count,
                error_message=error_message
            )
            
            if success:
                timestamp_str = extraction_time.strftime('%Y-%m-%d %H:%M:%S')
                self.logger.watermark_updated(table_name, timestamp_str)
            
            return success
            
        except Exception as e:
            self.logger.error_occurred(e, f"watermark_update_{table_name}")
            return False
    
    def get_table_watermark_timestamp(self, table_name: str) -> str:
        """
        Get incremental start timestamp for a table from S3 watermarks.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Timestamp string for incremental backup start
        """
        try:
            return self.watermark_manager.get_incremental_start_timestamp(table_name)
        except ValueError as e:
            # User needs to set initial watermark
            self.logger.error(f"Watermark not found for {table_name}: {e}")
            raise e
        except Exception as e:
            self.logger.error_occurred(e, f"watermark_retrieval_{table_name}")
            # For other errors, suggest checking connectivity and trying again
            raise RuntimeError(
                f"Failed to retrieve watermark for '{table_name}' due to system error: {e}. "
                f"Please check S3 connectivity and try again, or reset the watermark if corrupted."
            )
    
    def cleanup_resources(self):
        """Clean up resources after backup operation"""
        try:
            if hasattr(self, 'connection_manager'):
                self.connection_manager.close_all_connections()
            
            self.logger.logger.info("Resources cleaned up successfully")
            
        except Exception as e:
            self.logger.logger.warning(f"Error during resource cleanup: {e}")
    
    def get_backup_summary(self) -> Dict[str, Any]:
        """Get comprehensive backup operation summary"""
        summary = self.metrics.get_summary()
        
        # Calculate Gemini usage percentage
        total_batches = self._gemini_usage_stats['batches_with_gemini'] + self._gemini_usage_stats['batches_with_fallback']
        gemini_percentage = (self._gemini_usage_stats['batches_with_gemini'] / max(total_batches, 1)) * 100
        
        summary.update({
            'strategy': self.__class__.__name__,
            'config': {
                'batch_size': self.config.backup.batch_size,
                'max_workers': self.config.backup.max_workers,
                'timeout_seconds': self.config.backup.timeout_seconds
            },
            's3_stats': self.s3_manager.get_upload_stats(),
            'gemini_stats': {
                'mode_enabled': self._gemini_mode_enabled,
                'schemas_discovered': self._gemini_usage_stats['schemas_discovered'],
                'batches_with_gemini': self._gemini_usage_stats['batches_with_gemini'],
                'batches_with_fallback': self._gemini_usage_stats['batches_with_fallback'],
                'gemini_success_rate': round(gemini_percentage, 1),
                'total_batches_processed': total_batches
            }
        })
        
        return summary