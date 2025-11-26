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
from src.core.watermark_adapter import create_watermark_manager
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


class MemoryConfig:
    """Configuration for memory management operations"""
    
    def __init__(self, memory_limit_mb: int = 1024, gc_threshold: int = 50, check_interval: int = 10):
        self.memory_limit_mb = memory_limit_mb
        self.gc_threshold = gc_threshold
        self.check_interval = check_interval
        
    @classmethod
    def from_app_config(cls, app_config: AppConfig) -> 'MemoryConfig':
        """Create MemoryConfig from AppConfig with error handling"""
        try:
            return cls(
                memory_limit_mb=app_config.backup.memory_limit_mb,
                gc_threshold=app_config.backup.gc_threshold,
                check_interval=app_config.backup.memory_check_interval
            )
        except AttributeError as e:
            # P1 FIX: Graceful fallback to defaults if config is incomplete
            return cls()  # Use safe defaults


class MemoryManager:
    """
    P1 FIX: Refactored memory management utilities with proper dependency injection
    and isolation to fix architectural coupling issues.
    """
    
    def __init__(self, memory_config: MemoryConfig = None, logger=None, process_monitor=None):
        """
        Initialize MemoryManager with dependency injection
        
        Args:
            memory_config: Memory configuration object (defaults to safe config)
            logger: Logger instance (optional, creates default if None)  
            process_monitor: Process monitoring interface (defaults to psutil)
        """
        # P1 FIX: Use dependency injection instead of hard-coded config
        self.memory_config = memory_config or MemoryConfig()
        
        # P1 FIX: Configurable memory limits
        self.memory_limit_bytes = self.memory_config.memory_limit_mb * 1024 * 1024
        self.gc_threshold = self.memory_config.gc_threshold
        self.check_interval = self.memory_config.check_interval
        
        # P1 FIX: Instance-specific batch count (no shared state)
        self.batch_count = 0
        self._last_gc_time = time.time()
        
        # P1 FIX: Dependency injection for process monitoring
        self.process_monitor = process_monitor or self._create_default_process_monitor()
        
        # P1 FIX: Lazy logger initialization to avoid circular imports
        self._logger = logger
        
        # Statistics tracking
        self._stats = {
            'memory_checks': 0,
            'gc_operations': 0,
            'limit_warnings': 0,
            'errors': 0
        }
    
    @property
    def logger(self):
        """P1 FIX: Lazy logger initialization to prevent circular import issues"""
        if self._logger is None:
            try:
                from src.utils.logging import get_logger
                self._logger = get_logger(__name__)
            except ImportError:
                # Fallback to basic logging if circular import occurs
                import logging
                self._logger = logging.getLogger(__name__)
        return self._logger
    
    def _create_default_process_monitor(self):
        """P1 FIX: Factory method for process monitor to allow testing/mocking"""
        try:
            return psutil.Process(os.getpid())
        except Exception as e:
            self.logger.warning(f"Failed to create process monitor: {e}")
            return None
        
    def get_memory_usage(self) -> Dict[str, float]:
        """
        P1 FIX: Get current memory usage statistics with error handling
        """
        try:
            # P1 FIX: Check if process monitor is available
            if not self.process_monitor:
                self._stats['errors'] += 1
                self.logger.warning("Process monitor not available, returning default values")
                return {
                    'rss_mb': 0.0,
                    'vms_mb': 0.0,
                    'percent': 0.0,
                    'available_mb': 1024.0  # Default safe value
                }
            
            memory_info = self.process_monitor.memory_info()
            
            # P1 FIX: Separate system memory check to avoid tight coupling
            try:
                available_mb = psutil.virtual_memory().available / 1024 / 1024
            except Exception:
                available_mb = 1024.0  # Safe default
            
            return {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'percent': self.process_monitor.memory_percent(),
                'available_mb': available_mb
            }
            
        except Exception as e:
            # P1 FIX: Comprehensive error handling
            self._stats['errors'] += 1
            self.logger.error(f"Failed to get memory usage: {e}")
            
            # Return safe defaults to prevent backup failures
            return {
                'rss_mb': 0.0,
                'vms_mb': 0.0, 
                'percent': 0.0,
                'available_mb': 1024.0
            }
    
    def check_memory_usage(self, batch_number: int) -> bool:
        """
        P1 FIX: Check if memory usage is within limits with better error handling
        
        Returns:
            True if memory usage is acceptable, False if over limit
        """
        try:
            self._stats['memory_checks'] += 1
            
            # P1 FIX: Configurable check interval
            if batch_number % self.check_interval != 0:
                return True
                
            memory = self.get_memory_usage()
            memory_limit_mb = self.memory_limit_bytes / 1024 / 1024
            
            # P1 FIX: Handle case where memory monitoring failed
            if memory['rss_mb'] == 0.0 and memory['percent'] == 0.0:
                # Memory monitoring failed, assume OK to continue
                self.logger.debug("Memory monitoring unavailable, assuming safe to continue")
                return True
            
            if memory['rss_mb'] > memory_limit_mb:
                self._stats['limit_warnings'] += 1
                self.logger.warning(
                    "Memory usage approaching limit",
                    current_mb=round(memory['rss_mb'], 2),
                    limit_mb=memory_limit_mb,
                    percent=round(memory['percent'], 2),
                    batch_number=batch_number
                )
                return False
            
            return True
            
        except Exception as e:
            # P1 FIX: Don't fail backup on memory check errors
            self._stats['errors'] += 1
            self.logger.error(f"Memory check failed: {e}")
            return True  # Assume safe to continue
    
    def force_gc_if_needed(self, batch_number: int):
        """P1 FIX: Force garbage collection with better timing and error handling"""
        try:
            self.batch_count += 1
            current_time = time.time()
            
            # P1 FIX: Time-based GC to prevent excessive collection
            time_since_last_gc = current_time - self._last_gc_time
            
            if (self.batch_count >= self.gc_threshold and 
                time_since_last_gc >= 30):  # Minimum 30 seconds between GC
                
                self.logger.debug(
                    "Forcing garbage collection", 
                    batches_processed=self.batch_count,
                    time_since_last_gc=round(time_since_last_gc, 1)
                )
                
                gc.collect()
                self.batch_count = 0
                self._last_gc_time = current_time
                self._stats['gc_operations'] += 1
                
        except Exception as e:
            # P1 FIX: Don't fail backup on GC errors
            self._stats['errors'] += 1
            self.logger.error(f"Garbage collection failed: {e}")
    
    def reset_state(self):
        """P1 FIX: Reset memory manager state for isolated usage"""
        self.batch_count = 0
        self._last_gc_time = time.time()
        self._stats = {
            'memory_checks': 0,
            'gc_operations': 0,
            'limit_warnings': 0,
            'errors': 0
        }
        self.logger.debug("Memory manager state reset")
    
    def get_statistics(self) -> Dict[str, Any]:
        """P1 FIX: Get memory manager performance statistics"""
        return {
            **self._stats,
            'current_batch_count': self.batch_count,
            'memory_limit_mb': self.memory_config.memory_limit_mb,
            'gc_threshold': self.gc_threshold,
            'check_interval': self.check_interval,
            'time_since_last_gc': time.time() - self._last_gc_time
        }
    
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
    
    def __init__(self, config: AppConfig, pipeline_config: Optional[Dict[str, Any]] = None):
        self.config = config
        self.pipeline_config = pipeline_config  # Store pipeline configuration
        self.connection_manager = ConnectionManager(config)
        self.s3_manager = S3Manager(config)
        self.watermark_manager = create_watermark_manager(config.to_dict())

        self.logger = get_backup_logger()
        self.metrics = BackupMetrics()
        
        # P1 FIX: Use new MemoryManager with dependency injection
        memory_config = MemoryConfig.from_app_config(config)
        self.memory_manager = MemoryManager(
            memory_config=memory_config,
            logger=self.logger.logger  # Pass logger to prevent circular imports
        )
        
        # Initialize connection registry for multi-schema support
        from src.core.connection_registry import ConnectionRegistry
        try:
            self.connection_registry = ConnectionRegistry()
            self.logger.logger.info("Multi-schema connection registry initialized")
        except Exception as e:
            self.logger.logger.warning(f"Connection registry initialization failed, using v1.0.0 compatibility mode: {e}")
            self.connection_registry = None
        
        # Initialize components with S3 client
        self.s3_client = None
        self._initialize_components()
        
        # Initialize flexible schema manager for any table with connection registry
        from src.core.flexible_schema_manager import FlexibleSchemaManager
        self.flexible_schema_manager = FlexibleSchemaManager(self.connection_manager, connection_registry=self.connection_registry)
        
        # Track PoC mode usage (renamed from Gemini)
        self._poc_mode_enabled = True
        self._gemini_mode_enabled = True  # For compatibility
        self._gemini_usage_stats = {
            'schemas_discovered': 0,
            'batches_with_gemini': 0,  # Keep for compatibility
            'batches_with_fallback': 0
        }
        
        # Track S3 files created during backup for watermark blacklist
        self._created_s3_files = []

    def _get_partition_strategy(self) -> str:
        """
        Get partition strategy from pipeline config or fallback to default.

        Returns:
            Partition strategy: 'datetime', 'table', or 'hybrid'
        """
        if self.pipeline_config:
            # Try to get from pipeline config
            s3_config = self.pipeline_config.get('pipeline', {}).get('s3', {})
            partition_strategy = s3_config.get('partition_strategy')
            if partition_strategy:
                return partition_strategy

        # Fallback to default
        return 'datetime'

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
    def execute(self, tables: List[str], chunk_size: Optional[int] = None, max_total_rows: Optional[int] = None, limit: Optional[int] = None, source_connection: Optional[str] = None) -> bool:
        """
        Execute backup strategy for given tables.
        
        Args:
            tables: List of table names to backup
            chunk_size: Optional row limit per chunk (overrides config)
            max_total_rows: Optional maximum total rows to process
            limit: Deprecated - use chunk_size instead (for backward compatibility)
            source_connection: Optional connection name to use instead of default
        
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
            f"`update_at` > '{last_watermark}'",  # CRITICAL FIX: Use exclusive comparison to prevent duplicate processing
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
    
    def detect_optimal_window_size(
        self, 
        table_name: str, 
        current_watermark: str = None,
        target_rows: int = None
    ) -> int:
        """
        Auto-detect optimal time window size based on data density analysis.
        
        Args:
            table_name: Name of the table to analyze
            current_watermark: Starting point for analysis (optional)
            target_rows: Target number of rows per chunk
            
        Returns:
            Optimal window size in hours
        """
        if target_rows is None:
            target_rows = self.config.backup.target_rows_per_chunk
        
        try:
            self.logger.logger.info(
                "Starting auto-detection of optimal time window",
                table_name=table_name,
                target_rows=target_rows,
                current_watermark=current_watermark
            )
            
            with self.database_session() as db_conn:
                cursor = db_conn.cursor()
                
                # Test different time windows to understand data density
                test_windows = [1, 4, 12, 24, 48, 168]  # 1hr, 4hr, 12hr, 1day, 2day, 1week
                density_data = []
                
                for hours in test_windows:
                    try:
                        # Count rows starting from watermark date, not recent data
                        base_date = current_watermark or 'NOW()'
                        if current_watermark:
                            # Sample from watermark forward to understand actual data density
                            query = f"""
                            SELECT COUNT(*) as row_count 
                            FROM {table_name}
                            WHERE `update_at` > '{current_watermark}'
                            AND `update_at` <= DATE_ADD('{current_watermark}', INTERVAL {hours} HOUR)
                            """
                        else:
                            # Fallback to recent data if no watermark
                            query = f"""
                            SELECT COUNT(*) as row_count 
                            FROM {table_name}
                            WHERE `update_at` >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)
                            AND `update_at` <= NOW()
                            """
                        
                        cursor.execute(query)
                        result = cursor.fetchone()
                        
                        # Ensure all results are consumed to prevent "Unread result found" error
                        try:
                            while cursor.nextset():
                                pass
                        except:
                            pass  # No more result sets
                        
                        # Handle both dictionary and tuple cursors
                        if result:
                            if isinstance(result, dict):
                                row_count = result.get('row_count', result.get('COUNT(*)', 0))
                            else:
                                row_count = result[0]  # Tuple cursor - first element is the count
                        else:
                            row_count = 0
                        
                        if row_count > 0:
                            rows_per_hour = row_count / hours
                            density_data.append({
                                'window_hours': hours,
                                'total_rows': row_count,
                                'rows_per_hour': rows_per_hour,
                                'hours_for_target': target_rows / rows_per_hour if rows_per_hour > 0 else float('inf')
                            })
                            
                            self.logger.logger.debug(
                                f"Density sample: {hours}h window",
                                table_name=table_name,
                                hours=hours,
                                rows=row_count,
                                rows_per_hour=round(rows_per_hour, 2)
                            )
                    
                    except Exception as sample_error:
                        self.logger.logger.warning(
                            f"Failed to sample {hours}h window",
                            table_name=table_name,
                            hours=hours,
                            error=str(sample_error)
                        )
                        continue
                
                if not density_data:
                    self.logger.logger.warning(
                        "No data found in recent time windows, using default",
                        table_name=table_name,
                        default_hours=self.config.backup.default_chunk_time_window_hours
                    )
                    return self.config.backup.default_chunk_time_window_hours
                
                # Analyze patterns and calculate optimal window
                optimal_hours = self._calculate_optimal_window_from_density(
                    density_data, target_rows, table_name
                )
                
                # Apply bounds
                optimal_hours = max(
                    self.config.backup.min_window_hours,
                    min(self.config.backup.max_window_hours, optimal_hours)
                )
                
                self.logger.logger.info(
                    "Auto-detection completed",
                    table_name=table_name,
                    optimal_window_hours=optimal_hours,
                    target_rows=target_rows,
                    samples_analyzed=len(density_data)
                )
                
                return optimal_hours
                
        except Exception as e:
            self.logger.logger.error(
                "Auto-detection failed, using default window size",
                table_name=table_name,
                error=str(e),
                default_hours=self.config.backup.default_chunk_time_window_hours
            )
            return self.config.backup.default_chunk_time_window_hours
    
    def _calculate_optimal_window_from_density(
        self, 
        density_data: List[Dict], 
        target_rows: int, 
        table_name: str
    ) -> int:
        """Calculate optimal window size from density analysis results."""
        
        # Strategy 1: Find the most stable density pattern
        if len(density_data) >= 3:
            # Look for consistent rates across different windows
            rates = [d['rows_per_hour'] for d in density_data]
            
            # Calculate coefficient of variation (stability metric)
            import statistics
            
            # Handle all-zero rates scenario
            if all(rate == 0 for rate in rates):
                self.logger.logger.warning(
                    "All density rates are zero, using default window",
                    table_name=table_name,
                    default_hours=self.config.backup.default_chunk_time_window_hours
                )
                return self.config.backup.default_chunk_time_window_hours
            
            mean_rate = statistics.mean(rates)
            std_rate = statistics.stdev(rates) if len(rates) > 1 else 0
            cv = std_rate / mean_rate if mean_rate > 0 else float('inf')
            
            self.logger.logger.debug(
                "Density pattern analysis",
                table_name=table_name,
                mean_rate=round(mean_rate, 2),
                std_rate=round(std_rate, 2),
                coefficient_variation=round(cv, 3),
                pattern_stability="stable" if cv < 0.5 else "variable"
            )
            
            # Use most stable rate for calculation
            if cv < 0.5:  # Stable pattern
                optimal_hours = target_rows / mean_rate
                pattern_type = "stable_continuous"
            else:
                # Variable pattern - look for batch indicators
                optimal_hours = self._detect_batch_pattern(density_data, target_rows, table_name)
                pattern_type = "batch_detected"
        else:
            # Limited data - use simple calculation
            best_sample = max(density_data, key=lambda x: x['total_rows'])
            optimal_hours = best_sample['hours_for_target']
            pattern_type = "limited_data"
        
        # Round to sensible intervals
        if optimal_hours <= 2:
            final_hours = 1  # Hourly processing
        elif optimal_hours <= 8:
            final_hours = round(optimal_hours)  # Round to hours
        elif optimal_hours <= 48:
            final_hours = round(optimal_hours / 6) * 6  # Round to 6-hour intervals
        else:
            final_hours = round(optimal_hours / 24) * 24  # Round to days
        
        self.logger.logger.info(
            "Optimal window calculation complete",
            table_name=table_name,
            raw_optimal_hours=round(optimal_hours, 2),
            final_window_hours=final_hours,
            pattern_type=pattern_type,
            target_rows=target_rows
        )
        
        return int(final_hours)
    
    def _detect_batch_pattern(self, density_data: List[Dict], target_rows: int, table_name: str) -> float:
        """Detect if data follows batch patterns (daily/weekly ETL)."""
        
        # Look for patterns in the data
        daily_sample = next((d for d in density_data if d['window_hours'] == 24), None)
        weekly_sample = next((d for d in density_data if d['window_hours'] == 168), None)
        
        if weekly_sample and daily_sample:
            # Compare weekly vs daily rates
            weekly_daily_ratio = weekly_sample['rows_per_hour'] / daily_sample['rows_per_hour']
            
            if weekly_daily_ratio > 0.8:  # Similar rates = continuous data
                optimal_hours = target_rows / weekly_sample['rows_per_hour']
                batch_type = "continuous"
            elif weekly_sample['total_rows'] > target_rows * 0.8:  # Weekly batch fits target
                optimal_hours = 168  # Use weekly windows
                batch_type = "weekly_batch"
            elif daily_sample['total_rows'] > target_rows * 0.8:  # Daily batch fits target
                optimal_hours = 24  # Use daily windows
                batch_type = "daily_batch"
            else:
                # Smaller batches - use fraction of day
                optimal_hours = max(1, target_rows / daily_sample['rows_per_hour'])
                batch_type = "sub_daily_batch"
        else:
            # Limited samples - use best available
            best_sample = max(density_data, key=lambda x: x['total_rows'])
            optimal_hours = best_sample['hours_for_target']
            batch_type = "unknown_pattern"
        
        self.logger.logger.debug(
            "Batch pattern detection",
            table_name=table_name,
            batch_type=batch_type,
            optimal_hours=round(optimal_hours, 2)
        )
        
        return optimal_hours
    
    def calculate_next_time_window(
        self, 
        current_watermark: str, 
        window_hours: int = None,
        end_timestamp: Optional[str] = None,
        table_name: str = None
    ) -> Tuple[str, str]:
        """
        Calculate the next time window for progressive backup with auto-detection.
        
        Args:
            current_watermark: Starting timestamp (format: 'YYYY-MM-DD HH:MM:SS')
            window_hours: Size of time window in hours (auto-detected if None)
            end_timestamp: Optional end boundary
            table_name: Table name for auto-detection (required if window_hours is None)
            
        Returns:
            Tuple of (start_time, end_time) for the chunk
        """
        from datetime import datetime, timedelta
        
        # Auto-detect optimal window size if not specified
        if window_hours is None:
            if table_name and self.config.backup.auto_detect_patterns:
                window_hours = self.detect_optimal_window_size(table_name, current_watermark)
                self.logger.logger.info(
                    "Using auto-detected window size",
                    table_name=table_name,
                    window_hours=window_hours,
                    auto_detected=True
                )
            else:
                # Fallback to configuration
                window_hours = self.config.backup.chunk_time_window_hours
                self.logger.logger.info(
                    "Using configured window size",
                    table_name=table_name,
                    window_hours=window_hours,
                    auto_detected=False
                )
        
        try:
            start_dt = datetime.strptime(current_watermark, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Try with microseconds or Z suffix
            try:
                if 'T' in current_watermark:
                    # ISO format with T separator
                    clean_watermark = current_watermark.replace('T', ' ').replace('Z', '').split('.')[0]
                    start_dt = datetime.strptime(clean_watermark, '%Y-%m-%d %H:%M:%S')
                else:
                    # Try truncating microseconds
                    start_dt = datetime.strptime(current_watermark[:19], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                self.logger.logger.error(
                    "Invalid watermark timestamp format",
                    watermark=current_watermark,
                    expected_format="YYYY-MM-DD HH:MM:SS"
                )
                raise
        
        end_dt = start_dt + timedelta(hours=window_hours)
        
        # Don't go beyond the specified end boundary
        if end_timestamp:
            try:
                max_end_dt = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
                if end_dt > max_end_dt:
                    end_dt = max_end_dt
                    self.logger.logger.debug(
                        "Window truncated by end boundary",
                        original_end=end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        truncated_end=max_end_dt.strftime('%Y-%m-%d %H:%M:%S')
                    )
            except ValueError:
                self.logger.logger.warning(
                    "Invalid end timestamp format, ignoring boundary",
                    end_timestamp=end_timestamp
                )
        
        start_time = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        self.logger.logger.debug(
            "Calculated adaptive time window",
            table_name=table_name,
            start_time=start_time,
            end_time=end_time,
            window_hours=window_hours,
            duration_hours=(end_dt - start_dt).total_seconds() / 3600,
            auto_detected=window_hours is None
        )
        
        return (start_time, end_time)

    def get_incremental_query_with_time_window(
        self, 
        table_name: str, 
        start_timestamp: str,
        end_timestamp: str,
        custom_where: Optional[str] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Generate incremental query with specific time window.
        
        Args:
            table_name: Name of the table to query
            start_timestamp: Start of time window (exclusive)
            end_timestamp: End of time window (inclusive)
            custom_where: Additional WHERE conditions
            limit: Maximum number of rows to return
            
        Returns:
            SQL query string for the time window
        """
        where_conditions = [
            f"`update_at` > '{start_timestamp}'",  # Correct: exclusive start comparison
            f"`update_at` <= '{end_timestamp}'"
        ]
        
        if custom_where:
            where_conditions.append(custom_where)
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY `update_at`, `ID`
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        # Calculate window duration for logging
        from datetime import datetime
        try:
            start_dt = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
            window_hours = (end_dt - start_dt).total_seconds() / 3600
        except ValueError:
            window_hours = "unknown"
        
        self.logger.logger.info(
            "Generated time-window query",
            table_name=table_name,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            window_hours=window_hours,
            limit=limit,
            query_preview=query.replace('\n', ' ').strip()[:150] + "..."
        )
        
        return query
    
    def _extract_mysql_table_name(self, table_name: str) -> str:
        """
        Extract actual MySQL table name from potentially scoped table name.
        
        Handles v1.2.0 multi-schema scoped names:
        - 'settlement.settle_orders' → 'settlement.settle_orders' (unscoped)
        - 'US_DW_RO_SSH:settlement.settle_orders' → 'settlement.settle_orders' (connection scoped)
        - 'us_dw_pipeline:settlement.settle_orders' → 'settlement.settle_orders' (pipeline scoped)
        
        Args:
            table_name: Table name (may include scope prefix)
            
        Returns:
            MySQL table name without scope prefix
        """
        if ':' in table_name:
            # Scoped table name - extract actual table name after colon
            _, actual_table = table_name.split(':', 1)
            return actual_table
        else:
            # Unscoped table name - return as-is
            return table_name

    def validate_table_exists(self, cursor, table_name: str) -> bool:
        """
        Validate that table exists and has required columns.
        
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
            
            # Check table structure
            cursor.execute(f"DESCRIBE {mysql_table_name}")
            describe_results = cursor.fetchall()
            # Handle both dictionary and tuple cursors
            if describe_results and isinstance(describe_results[0], dict):
                # Dictionary cursor - get the 'Field' column
                columns = [row['Field'] for row in describe_results]
            else:
                # Tuple cursor - get the first element
                columns = [row[0] for row in describe_results]
            
            # Check for required timestamp column (flexible names)
            timestamp_candidates = ['updated_at', 'update_at', 'last_modified', 'modified_at']
            has_timestamp_column = any(col in columns for col in timestamp_candidates)
            if not has_timestamp_column:
                self.logger.error_occurred(
                    Exception(f"Table {table_name} missing timestamp column (tried: {', '.join(timestamp_candidates)})"), 
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
            
            # Ensure all results are consumed to prevent "Unread result found" error
            try:
                while cursor.nextset():
                    pass
            except:
                pass  # No more result sets
            
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

                # Generate S3 key with configured partition strategy
                s3_key = self.s3_manager.generate_s3_key(
                    table_name, current_timestamp, batch_id,
                    partition_strategy=self._get_partition_strategy()
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
                
                # Fallback to PoC-compatible upload (same method that works for parcel detail)
                s3_key = self.s3_manager.generate_s3_key(
                    table_name, current_timestamp, batch_id,
                    partition_strategy=self._get_partition_strategy()
                )
                # Use the proven PoC upload method instead of complex parquet generation
                success = self.s3_manager.upload_dataframe(
                    df, s3_key, use_schema_alignment=False, compression="snappy"
                )
                
                if success:
                    self._gemini_usage_stats['batches_with_fallback'] += 1
            
            if success:
                batch_duration = time.time() - batch_start_time
                
                # Track S3 file for watermark blacklist  
                s3_uri = f"s3://{self.config.s3.bucket_name}/{s3_key}"
                self._created_s3_files.append(s3_uri)
                
                self.logger.batch_processed(
                    table_name, batch_id, len(batch_data), s3_key
                )
                
                # Update metrics - estimate bytes from DataFrame size
                batch_bytes = len(df) * len(df.columns) * 8  # Rough estimate: 8 bytes per cell
                self.metrics.total_bytes += batch_bytes
                
                # Cleanup DataFrame to prevent memory leaks
                del df
                import gc
                gc.collect()
                
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
    def database_session(self, connection_name: Optional[str] = None):
        """
        Context manager for database operations with multi-schema support.

        Args:
            connection_name: Required connection name from connection registry.

        Raises:
            ValueError: If connection_name is None or connection_registry is not available
        """
        if not connection_name:
            raise ValueError("connection_name is required - v1.0.0 compatibility removed")

        if not self.connection_registry:
            raise ValueError("Connection registry not initialized - cannot perform database operations")

        # Use connection registry for multi-schema support
        try:
            with self.connection_registry.get_mysql_connection(connection_name) as db_conn:
                config = self.connection_registry.get_connection(connection_name)
                self.logger.connection_established("Database", host=config.host, database=config.database)
                yield db_conn
        except Exception as e:
            self.logger.error_occurred(e, f"database_session({connection_name})")
            raise
        finally:
            self.logger.connection_closed(f"Database({connection_name})")
    
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
        last_processed_id: Optional[int] = None,
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
            # Use v2.0 direct API for better performance  
            self.watermark_manager.simple_manager.update_mysql_state(
                table_name=table_name,
                timestamp=max_data_timestamp,
                id=last_processed_id,
                status=status,
                error=error_message,
                rows_extracted=rows_extracted
            )
            success = True
            
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
        """P1 FIX: Clean up resources after backup operation including memory manager state"""
        try:
            if hasattr(self, 'connection_manager'):
                self.connection_manager.close_all_connections()
            
            # P1 FIX: Reset memory manager state to prevent pollution between runs
            if hasattr(self, 'memory_manager'):
                self.memory_manager.reset_state()
                self.logger.logger.debug("Memory manager state reset during cleanup")
            
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