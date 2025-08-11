"""
Base backup strategy implementation.

This module provides the abstract base class and common functionality
for all backup strategies in the S3 to Redshift backup system.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Generator
import time
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
from contextlib import contextmanager

from src.config.settings import AppConfig
from src.core.connections import ConnectionManager
from src.core.s3_manager import S3Manager
from src.core.watermark import WatermarkManager
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
        self.watermark_manager = WatermarkManager(config)
        self.logger = get_backup_logger()
        self.metrics = BackupMetrics()
        
        # Initialize components with S3 client
        self.s3_client = None
        self._initialize_components()
        
        # Initialize dynamic schema manager
        from src.config.dynamic_schemas import DynamicSchemaManager
        self.dynamic_schema_manager = DynamicSchemaManager(self.connection_manager)
    
    def _initialize_components(self):
        """Initialize components with shared S3 client"""
        try:
            self.s3_client = self.connection_manager.get_s3_client()
            self.s3_manager.s3_client = self.s3_client
            self.watermark_manager.s3_client = self.s3_client
            
            self.logger.connection_established("S3")
        except Exception as e:
            self.logger.error_occurred(e, "S3 client initialization")
            raise
    
    @abstractmethod
    def execute(self, tables: List[str]) -> bool:
        """
        Execute backup strategy for given tables.
        
        Args:
            tables: List of table names to backup
        
        Returns:
            True if backup successful, False otherwise
        """
        pass
    
    def get_incremental_query(
        self, 
        table_name: str, 
        last_watermark: str, 
        current_timestamp: str,
        custom_where: Optional[str] = None
    ) -> str:
        """
        Generate incremental query for table.
        
        Args:
            table_name: Name of the table
            last_watermark: Last backup timestamp
            current_timestamp: Current backup timestamp
            custom_where: Additional WHERE conditions
        
        Returns:
            SQL query string for incremental data
        """
        where_conditions = [
            f"`update_at` > '{last_watermark}'",
            f"`update_at` <= '{current_timestamp}'"
        ]
        
        if custom_where:
            where_conditions.append(custom_where)
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY `update_at`, `ID`
        """
        
        self.logger.logger.debug(
            "Generated incremental query",
            table_name=table_name,
            last_watermark=last_watermark,
            current_timestamp=current_timestamp,
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
            Estimated row count
        """
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
                query=count_query.strip(),
                error=str(e),
                error_type=type(e).__name__
            )
            # Re-raise the exception to stop processing if query is fundamentally broken
            return 0
    
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
            
            # Schema validation and conversion
            if schema:
                try:
                    table = validate_table_data(df, table_name, strict=False)
                except Exception as e:
                    self.logger.error_occurred(e, f"schema_conversion_{table_name}")
                    # Fallback to automatic schema inference
                    table = pa.Table.from_pandas(df)
            else:
                table = pa.Table.from_pandas(df)
            
            # Generate S3 key
            s3_key = self.s3_manager.generate_s3_key(
                table_name, current_timestamp, batch_id
            )
            
            # Upload to S3
            success = self.s3_manager.upload_parquet(
                table, 
                s3_key,
                metadata={
                    'table_name': table_name,
                    'batch_id': str(batch_id),
                    'backup_timestamp': current_timestamp,
                    'row_count': str(len(batch_data))
                }
            )
            
            if success:
                batch_duration = time.time() - batch_start_time
                
                self.logger.batch_processed(
                    table_name, batch_id, len(batch_data), s3_key
                )
                
                # Update metrics
                batch_bytes = len(table.to_pandas().to_csv().encode('utf-8'))  # Rough estimate
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
        current_timestamp: str, 
        tables: List[str], 
        table_specific: bool = False
    ) -> bool:
        """
        Update watermarks after successful backup.
        
        Args:
            current_timestamp: New watermark timestamp
            tables: List of successfully backed up tables
            table_specific: Whether to use table-specific watermarks
        
        Returns:
            True if watermarks updated successfully
        """
        try:
            if table_specific:
                # Update watermark for each table
                for table_name in tables:
                    success = self.watermark_manager.update_watermark(
                        current_timestamp, 
                        table_name=table_name,
                        metadata={
                            'backup_strategy': self.__class__.__name__,
                            'table_count': len(tables),
                            'backup_duration': self.metrics.get_duration()
                        }
                    )
                    if not success:
                        self.logger.error_occurred(
                            Exception(f"Failed to update watermark for {table_name}"),
                            "watermark_update"
                        )
                        return False
            else:
                # Update global watermark
                success = self.watermark_manager.update_watermark(
                    current_timestamp,
                    metadata={
                        'backup_strategy': self.__class__.__name__,
                        'tables_backed_up': tables,
                        'backup_duration': self.metrics.get_duration(),
                        'total_rows': self.metrics.total_rows
                    }
                )
                if not success:
                    return False
            
            self.logger.watermark_updated("previous", current_timestamp)
            return True
            
        except Exception as e:
            self.logger.error_occurred(e, "watermark_update")
            return False
    
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
        summary.update({
            'strategy': self.__class__.__name__,
            'config': {
                'batch_size': self.config.backup.batch_size,
                'max_workers': self.config.backup.max_workers,
                'timeout_seconds': self.config.backup.timeout_seconds
            },
            's3_stats': self.s3_manager.get_upload_stats()
        })
        
        return summary