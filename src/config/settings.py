from pydantic import SecretStr, Field, validator
from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any
import os
from pathlib import Path


class DatabaseConfig(BaseSettings):
    """Database connection configuration"""
    host: str = Field(..., description="Database host")
    port: int = Field(3306, description="Database port")
    user: str = Field(..., description="Database username")
    password: Optional[SecretStr] = Field(None, description="Database password")
    database: str = Field(..., description="Database name")
    
    class Config:
        env_prefix = "DB_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


class SSHConfig(BaseSettings):
    """SSH bastion host configuration"""
    bastion_host: str = Field(..., description="Bastion host address")
    bastion_user: str = Field(..., description="SSH username")
    bastion_key_path: str = Field(..., description="Path to SSH private key")
    local_port: int = Field(0, description="Local port for SSH tunnel (0 for dynamic)")
    
    class Config:
        env_prefix = "SSH_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"
    
    @validator('bastion_key_path')
    def validate_key_path(cls, v):
        """Validate SSH key file exists (skip validation for placeholder paths)"""
        # Skip validation for placeholder/template values
        if v and v.startswith('/path/to/'):
            # Placeholder value from template - will be validated when actually used
            return v

        # Only validate real paths
        if v and not Path(v).exists():
            # Don't raise error - just return the path
            # Validation will happen when connection is actually used
            import logging
            logging.warning(f"SSH key file not found: {v} (will fail if this connection is used)")
        return v


class S3Config(BaseSettings):
    """S3 storage configuration with performance optimizations"""
    bucket_name: str = Field(..., description="S3 bucket name")
    access_key: str = Field(..., description="AWS access key")
    secret_key: SecretStr = Field(..., description="AWS secret key")
    region: str = Field("us-east-1", description="AWS region")
    incremental_path: str = Field("/incremental/", description="S3 path for incremental data")
    high_watermark_key: str = Field(
        "/high_watermark/last_run_timestamp.txt", 
        description="S3 key for high watermark file"
    )
    
    # Performance optimization settings
    multipart_threshold: int = Field(
        104857600,  # 100MB in bytes
        description="File size threshold for multipart upload (bytes)"
    )
    multipart_chunksize: int = Field(
        52428800,   # 50MB in bytes
        description="Size of each multipart upload chunk (bytes)"
    )
    max_concurrency: int = Field(
        10, 
        description="Maximum concurrent S3 upload threads"
    )
    max_bandwidth: Optional[int] = Field(
        None, 
        description="Maximum bandwidth for uploads in bytes/sec (None for unlimited)"
    )
    
    # Connection and retry settings
    max_pool_connections: int = Field(
        20,
        description="Maximum number of connections in the connection pool"
    )
    retry_max_attempts: int = Field(
        3,
        description="Maximum retry attempts for failed S3 operations"
    )
    retry_mode: str = Field(
        "adaptive",
        description="Retry mode: standard, adaptive, or legacy"
    )
    
    class Config:
        env_prefix = "S3_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


class RedshiftSSHConfig(BaseSettings):
    """SSH bastion host configuration for Redshift"""
    host: Optional[str] = Field(None, description="Redshift SSH bastion host address")
    username: Optional[str] = Field(None, description="Redshift SSH username")
    private_key_path: Optional[str] = Field(None, description="Path to Redshift SSH private key")
    local_port: int = Field(0, description="Local port for Redshift SSH tunnel (0 for dynamic)")

    class Config:
        env_prefix = "REDSHIFT_SSH_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    @validator('private_key_path')
    def validate_key_path(cls, v):
        """Validate SSH key file exists (skip validation for placeholder paths)"""
        # Skip validation for placeholder/template values
        if v and v.startswith('/path/to/'):
            # Placeholder value from template - will be validated when actually used
            return v

        # Only validate real paths
        if v and not Path(v).exists():
            # Don't raise error - just return the path
            # Validation will happen when connection is actually used
            import logging
            logging.warning(f"Redshift SSH key file not found: {v} (will fail if this connection is used)")
        return v


class RedshiftConfig(BaseSettings):
    """Redshift connection configuration"""
    host: str = Field(..., description="Redshift host")
    port: int = Field(5439, description="Redshift port")
    user: str = Field(..., description="Redshift username")
    password: SecretStr = Field(..., description="Redshift password")
    database: str = Field(..., description="Redshift database")
    schema: str = Field("public", description="Redshift schema")
    
    class Config:
        env_prefix = "REDSHIFT_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


class BackupConfig(BaseSettings):
    """Backup operation configuration with performance optimizations"""
    batch_size: int = Field(10000, description="Number of rows per batch")
    max_workers: int = Field(4, description="Maximum parallel workers")
    num_chunks: int = Field(4, description="Number of time chunks for intra-table strategy (deprecated)")
    retry_attempts: int = Field(3, description="Number of retry attempts for failed operations")
    timeout_seconds: int = Field(300, description="Timeout for operations in seconds")
    
    # Memory management settings
    memory_limit_mb: int = Field(
        4096, 
        description="Memory limit in MB for backup processes"
    )
    gc_threshold: int = Field(
        1000, 
        description="Number of batches before forcing garbage collection"
    )
    memory_check_interval: int = Field(
        10,
        description="Check memory usage every N batches"
    )
    
    # Row-based chunking settings (exact row counts with timestamp+ID pagination)
    target_rows_per_chunk: int = Field(5000000, description="Exact number of rows per chunk")
    max_rows_per_chunk: int = Field(10000000, description="Maximum rows per chunk (safety limit)")
    
    @validator('target_rows_per_chunk')
    def validate_target_rows(cls, v):
        if v <= 0:
            raise ValueError("target_rows_per_chunk must be positive")
        return v
    
    @validator('max_rows_per_chunk')
    def validate_max_rows(cls, v, values):
        if v <= 0:
            raise ValueError("max_rows_per_chunk must be positive")
        target = values.get('target_rows_per_chunk', 0)
        if target > 0 and v <= target:
            raise ValueError("max_rows_per_chunk must be greater than target_rows_per_chunk")
        return v
    
    # Performance settings
    enable_compression: bool = Field(
        True,
        description="Enable compression for parquet files"
    )
    compression_level: int = Field(
        6,
        description="Compression level (1-9, higher = more compression)"
    )
    
    class Config:
        env_prefix = "BACKUP_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"
    
    @validator('batch_size')
    def validate_batch_size(cls, v):
        """Validate batch size is positive"""
        if v <= 0:
            raise ValueError("Batch size must be positive")
        return v
    
    @validator('max_workers')
    def validate_max_workers(cls, v):
        """Validate max workers is reasonable"""
        if v <= 0 or v > 20:
            raise ValueError("Max workers must be between 1 and 20")
        return v
    


class AppConfig(BaseSettings):
    """Main application configuration"""
    log_level: str = Field("INFO", description="Logging level")
    debug: bool = Field(False, description="Enable debug mode")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra fields from environment
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration"""
        if not hasattr(self, '_database'):
            self._database = DatabaseConfig()
        return self._database
    
    @property
    def ssh(self) -> Optional[SSHConfig]:
        """Get SSH configuration (None if SSH disabled)"""
        if not hasattr(self, '_ssh'):
            self._ssh = SSHConfig()
        return self._ssh
    
    @property
    def s3(self) -> S3Config:
        """Get S3 configuration"""
        if not hasattr(self, '_s3'):
            self._s3 = S3Config()
        return self._s3
    
    @property
    def backup(self) -> BackupConfig:
        """Get backup configuration"""
        if not hasattr(self, '_backup'):
            self._backup = BackupConfig()
        return self._backup
    
    @property
    def redshift(self) -> RedshiftConfig:
        """Get Redshift configuration"""
        if not hasattr(self, '_redshift'):
            self._redshift = RedshiftConfig()
        return self._redshift
    
    @property
    def redshift_ssh(self) -> Optional[RedshiftSSHConfig]:
        """Get Redshift SSH configuration (None if SSH disabled)"""
        if not hasattr(self, '_redshift_ssh'):
            self._redshift_ssh = RedshiftSSHConfig()
        return self._redshift_ssh
    
    @validator('log_level')
    def validate_log_level(cls, v):
        """Validate log level is valid"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()
    
    @classmethod
    def load(cls, config_file: Optional[str] = None) -> "AppConfig":
        """Load configuration from environment variables or file"""
        if config_file and Path(config_file).exists():
            return cls(_env_file=config_file)
        return cls()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert AppConfig to dictionary with all nested configurations.
        
        This method properly serializes all configuration sections including
        nested objects that are accessed via properties. Unlike model_dump(),
        this includes all the actual configuration data needed by components.
        """
        return {
            'log_level': self.log_level,
            'debug': self.debug,
            's3': {
                'bucket_name': self.s3.bucket_name,
                'region': self.s3.region,
                'access_key_id': self.s3.access_key,  # Map access_key to access_key_id
                'secret_access_key': self.s3.secret_key.get_secret_value(),  # Extract secret value
                'watermark_prefix': getattr(self.s3, 'watermark_prefix', 'watermarks/v2/'),
                'multipart_threshold': self.s3.multipart_threshold,
                'multipart_chunksize': self.s3.multipart_chunksize,
                'max_concurrency': self.s3.max_concurrency,
                'max_pool_connections': self.s3.max_pool_connections,
                'retry_max_attempts': self.s3.retry_max_attempts,
                'retry_mode': self.s3.retry_mode,
            },
            'database': {
                'host': self.database.host,
                'port': self.database.port,
                'user': self.database.user,
                'password': self.database.password,
                'database': self.database.database,
            },
            'redshift': {
                'host': self.redshift.host,
                'port': self.redshift.port,
                'user': self.redshift.user,
                'password': self.redshift.password,
                'database': self.redshift.database,
                'schema': self.redshift.schema,
            },
            'backup': {
                'batch_size': self.backup.batch_size,
                'max_workers': self.backup.max_workers,
                'retry_attempts': self.backup.retry_attempts,
                'timeout_seconds': self.backup.timeout_seconds,
                'memory_limit_mb': self.backup.memory_limit_mb,
                'gc_threshold': self.backup.gc_threshold,
                'memory_check_interval': self.backup.memory_check_interval,
                'enable_compression': self.backup.enable_compression,
                'compression_level': self.backup.compression_level,
            },
            'ssh': {
                'bastion_host': self.ssh.bastion_host,
                'bastion_user': self.ssh.bastion_user,
                'bastion_key_path': self.ssh.bastion_key_path,
                'local_port': self.ssh.local_port,
            },
            'redshift_ssh': {
                'bastion_host': self.redshift_ssh.host,
                'bastion_user': self.redshift_ssh.username,
                'bastion_key_path': self.redshift_ssh.private_key_path,
                'local_port': self.redshift_ssh.local_port,
            }
        }
    
    def validate_all(self) -> list:
        """Validate all configuration and return list of errors"""
        errors = []
        
        try:
            # Test if we can create all sub-configs
            DatabaseConfig()
        except Exception as e:
            errors.append(f"Database config error: {e}")
        
        try:
            SSHConfig()
        except Exception as e:
            errors.append(f"SSH config error: {e}")
        
        try:
            S3Config()
        except Exception as e:
            errors.append(f"S3 config error: {e}")
        
        try:
            BackupConfig()
        except Exception as e:
            errors.append(f"Backup config error: {e}")
        
        return errors