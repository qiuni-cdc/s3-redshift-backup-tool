"""
Structured logging configuration for the S3 to Redshift backup system.

This module provides structured logging capabilities using structlog,
with support for different output formats, log levels, and contextual information.
"""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Dict, Optional
import structlog
from structlog.types import FilteringBoundLogger


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_logs: bool = False,
    include_caller: bool = True
) -> FilteringBoundLogger:
    """
    Setup structured logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        json_logs: Whether to output logs in JSON format
        include_caller: Whether to include caller information in logs
    
    Returns:
        Configured structlog logger instance
    """
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper())
    )
    
    # Setup log file handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        
        # Get root logger and add file handler
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
    
    # Configure processors based on output format
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    # Add caller information if requested
    if include_caller:
        processors.append(structlog.processors.CallsiteParameterAdder())
    
    # Add timestamp
    processors.append(structlog.processors.TimeStamper(fmt="ISO"))
    
    # Choose final renderer based on format preference
    if json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback
            )
        )
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger()


def get_logger(name: str = None) -> FilteringBoundLogger:
    """
    Get a logger instance with optional name.
    
    Args:
        name: Logger name (typically __name__ of calling module)
    
    Returns:
        Configured structlog logger instance
    """
    if name:
        return structlog.get_logger(name)
    return structlog.get_logger()


class BackupLogger:
    """
    Specialized logger for backup operations with contextual information.
    
    This class provides methods for logging backup-specific events
    with consistent structure and context.
    """
    
    def __init__(self, logger: FilteringBoundLogger = None):
        self.logger = logger or get_logger("backup")
        self._context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs):
        """Set persistent context for all log messages"""
        self._context.update(kwargs)
    
    def clear_context(self):
        """Clear all persistent context"""
        self._context.clear()
    
    def with_context(self, **kwargs) -> FilteringBoundLogger:
        """Get logger with additional context for one-time use"""
        full_context = {**self._context, **kwargs}
        return self.logger.bind(**full_context)
    
    def backup_started(self, strategy: str, tables: list, **kwargs):
        """Log backup operation start"""
        self.with_context(
            event="backup_started",
            strategy=strategy,
            table_count=len(tables),
            tables=tables,
            **kwargs
        ).info("Backup operation started")
    
    def backup_completed(self, strategy: str, success: bool, duration: float = None, **kwargs):
        """Log backup operation completion"""
        level = "info" if success else "error"
        getattr(self.with_context(
            event="backup_completed",
            strategy=strategy,
            success=success,
            duration_seconds=duration,
            **kwargs
        ), level)("Backup operation completed")
    
    def table_started(self, table_name: str, **kwargs):
        """Log table processing start"""
        self.with_context(
            event="table_started",
            table_name=table_name,
            **kwargs
        ).info("Table processing started")
    
    def table_completed(self, table_name: str, rows_processed: int, batches: int, 
                       duration: float = None, **kwargs):
        """Log table processing completion"""
        self.with_context(
            event="table_completed",
            table_name=table_name,
            rows_processed=rows_processed,
            batch_count=batches,
            duration_seconds=duration,
            **kwargs
        ).info("Table processing completed")
    
    def table_failed(self, table_name: str, error: Exception = None, **kwargs):
        """Log table processing failure"""
        self.with_context(
            event="table_failed",
            table_name=table_name,
            error_type=type(error).__name__ if error else None,
            error_message=str(error) if error else None,
            **kwargs
        ).error("Table processing failed")
    
    def batch_processed(self, table_name: str, batch_id: int, batch_size: int,
                       s3_key: str = None, **kwargs):
        """Log batch processing"""
        self.with_context(
            event="batch_processed",
            table_name=table_name,
            batch_id=batch_id,
            batch_size=batch_size,
            s3_key=s3_key,
            **kwargs
        ).debug("Batch processed successfully")
    
    def connection_established(self, connection_type: str, host: str = None, **kwargs):
        """Log connection establishment"""
        self.with_context(
            event="connection_established",
            connection_type=connection_type,
            host=host,
            **kwargs
        ).info("Connection established")
    
    def connection_closed(self, connection_type: str, **kwargs):
        """Log connection closure"""
        self.with_context(
            event="connection_closed",
            connection_type=connection_type,
            **kwargs
        ).debug("Connection closed")
    
    def watermark_updated(self, old_watermark: str, new_watermark: str, **kwargs):
        """Log watermark update"""
        self.with_context(
            event="watermark_updated",
            old_watermark=old_watermark,
            new_watermark=new_watermark,
            **kwargs
        ).info("High watermark updated")
    
    def error_occurred(self, error: Exception, operation: str, **kwargs):
        """Log error with context"""
        self.with_context(
            event="error_occurred",
            operation=operation,
            error_type=type(error).__name__,
            error_message=str(error),
            **kwargs
        ).error("Operation failed")
    
    def retry_attempt(self, operation: str, attempt: int, max_attempts: int, 
                     error: Exception = None, **kwargs):
        """Log retry attempt"""
        self.with_context(
            event="retry_attempt",
            operation=operation,
            attempt=attempt,
            max_attempts=max_attempts,
            error_type=type(error).__name__ if error else None,
            **kwargs
        ).warning("Retrying operation")
    
    def performance_metric(self, metric_name: str, value: float, unit: str = None, **kwargs):
        """Log performance metric"""
        self.with_context(
            event="performance_metric",
            metric_name=metric_name,
            value=value,
            unit=unit,
            **kwargs
        ).info("Performance metric recorded")


# Global logger instance for convenience
_default_logger: Optional[BackupLogger] = None


def get_backup_logger() -> BackupLogger:
    """Get the default backup logger instance"""
    global _default_logger
    if _default_logger is None:
        _default_logger = BackupLogger()
    return _default_logger


def configure_logging_from_config(config) -> BackupLogger:
    """
    Configure logging from application configuration.
    
    Args:
        config: AppConfig instance with logging settings
    
    Returns:
        Configured BackupLogger instance
    """
    # Setup base logging
    setup_logging(
        level=config.log_level,
        json_logs=not config.debug,  # Use JSON in production, console in debug
        include_caller=config.debug
    )
    
    # Create and configure backup logger
    backup_logger = BackupLogger()
    backup_logger.set_context(
        application="s3-redshift-backup",
        debug_mode=config.debug
    )
    
    return backup_logger