"""
Custom exception hierarchy for the S3 to Redshift backup system.

This module defines all custom exceptions used throughout the application,
providing a clear hierarchy for error handling and debugging.
"""


class BackupSystemError(Exception):
    """
    Base exception for all backup system errors.
    
    All custom exceptions in the system inherit from this base class
    to allow for consistent error handling and logging.
    """
    
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
    
    def __str__(self):
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConnectionError(BackupSystemError):
    """
    Raised when connection-related operations fail.
    
    This includes SSH tunnel failures, database connection issues,
    and S3 client creation problems.
    """
    pass


class BackupError(BackupSystemError):
    """
    Raised when backup operations fail.
    
    This includes failures in data processing, batch uploads,
    and backup strategy execution.
    """
    pass


class ConfigurationError(BackupSystemError):
    """
    Raised when configuration-related issues occur.
    
    This includes invalid configuration values, missing required settings,
    and configuration validation failures.
    """
    pass


class S3Error(BackupSystemError):
    """
    Raised when S3 operations fail.
    
    This includes upload failures, download failures, bucket access issues,
    and S3 authentication problems.
    """
    
    def __init__(self, message: str, s3_key: str = None, bucket: str = None, **kwargs):
        super().__init__(message, **kwargs)
        self.s3_key = s3_key
        self.bucket = bucket
        
        if s3_key:
            self.details["s3_key"] = s3_key
        if bucket:
            self.details["bucket"] = bucket


class WatermarkError(BackupSystemError):
    """
    Raised when watermark management operations fail.
    
    This includes failures to read or update high watermark files,
    watermark validation issues, and timestamp parsing problems.
    """
    
    def __init__(self, message: str, watermark_value: str = None, **kwargs):
        super().__init__(message, **kwargs)
        self.watermark_value = watermark_value
        
        if watermark_value:
            self.details["watermark_value"] = watermark_value


class DatabaseError(BackupSystemError):
    """
    Raised when database operations fail.
    
    This includes SQL execution failures, table validation issues,
    and data retrieval problems.
    """
    
    def __init__(self, message: str, table_name: str = None, query: str = None, **kwargs):
        super().__init__(message, **kwargs)
        self.table_name = table_name
        self.query = query
        
        if table_name:
            self.details["table_name"] = table_name
        if query:
            # Truncate long queries for logging
            self.details["query"] = query[:200] + "..." if len(query) > 200 else query


class ValidationError(BackupSystemError):
    """
    Raised when data validation fails.
    
    This includes schema validation failures, data type mismatches,
    and data quality issues.
    """
    
    def __init__(self, message: str, field_name: str = None, expected_type: str = None, 
                 actual_value: str = None, **kwargs):
        super().__init__(message, **kwargs)
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_value = actual_value
        
        if field_name:
            self.details["field_name"] = field_name
        if expected_type:
            self.details["expected_type"] = expected_type
        if actual_value:
            self.details["actual_value"] = str(actual_value)


class RetryExhaustedError(BackupSystemError):
    """
    Raised when retry attempts are exhausted.
    
    This is used when operations fail repeatedly despite retry attempts.
    """
    
    def __init__(self, message: str, attempts: int = None, last_error: Exception = None, **kwargs):
        super().__init__(message, **kwargs)
        self.attempts = attempts
        self.last_error = last_error
        
        if attempts:
            self.details["attempts"] = attempts
        if last_error:
            self.details["last_error"] = str(last_error)


class TimeoutError(BackupSystemError):
    """
    Raised when operations exceed their timeout limits.
    
    This includes backup operations that take too long,
    network timeouts, and other time-based failures.
    """
    
    def __init__(self, message: str, timeout_seconds: int = None, **kwargs):
        super().__init__(message, **kwargs)
        self.timeout_seconds = timeout_seconds
        
        if timeout_seconds:
            self.details["timeout_seconds"] = timeout_seconds


# Convenience functions for common error scenarios
def raise_connection_error(component: str, host: str = None, port: int = None, 
                          underlying_error: Exception = None):
    """Raise a standardized connection error"""
    details = {"component": component}
    if host:
        details["host"] = host
    if port:
        details["port"] = port
    if underlying_error:
        details["underlying_error"] = str(underlying_error)
    
    message = f"Failed to connect to {component}"
    if host:
        message += f" at {host}"
    if port:
        message += f":{port}"
    
    raise ConnectionError(message, details)


def raise_backup_error(operation: str, table_name: str = None, batch_id: int = None,
                      underlying_error: Exception = None):
    """Raise a standardized backup error"""
    details = {"operation": operation}
    if table_name:
        details["table_name"] = table_name
    if batch_id:
        details["batch_id"] = batch_id
    if underlying_error:
        details["underlying_error"] = str(underlying_error)
    
    message = f"Backup operation '{operation}' failed"
    if table_name:
        message += f" for table {table_name}"
    if batch_id:
        message += f" (batch {batch_id})"
    
    raise BackupError(message, details)