from pydantic import SecretStr, Field, validator
from pydantic_settings import BaseSettings
from typing import Optional
import os
from pathlib import Path


class DatabaseConfig(BaseSettings):
    """Database connection configuration"""
    host: str = Field(..., description="Database host")
    port: int = Field(3306, description="Database port")
    user: str = Field(..., description="Database username")
    password: SecretStr = Field(..., description="Database password")
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
        """Validate SSH key file exists"""
        if not Path(v).exists():
            raise ValueError(f"SSH key file not found: {v}")
        return v


class S3Config(BaseSettings):
    """S3 storage configuration"""
    bucket_name: str = Field(..., description="S3 bucket name")
    access_key: str = Field(..., description="AWS access key")
    secret_key: SecretStr = Field(..., description="AWS secret key")
    region: str = Field("us-east-1", description="AWS region")
    incremental_path: str = Field("/incremental/", description="S3 path for incremental data")
    high_watermark_key: str = Field(
        "/high_watermark/last_run_timestamp.txt", 
        description="S3 key for high watermark file"
    )
    
    class Config:
        env_prefix = "S3_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


class BackupConfig(BaseSettings):
    """Backup operation configuration"""
    batch_size: int = Field(10000, description="Number of rows per batch")
    max_workers: int = Field(4, description="Maximum parallel workers")
    num_chunks: int = Field(4, description="Number of time chunks for intra-table strategy")
    retry_attempts: int = Field(3, description="Number of retry attempts for failed operations")
    timeout_seconds: int = Field(300, description="Timeout for operations in seconds")
    
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
    def ssh(self) -> SSHConfig:
        """Get SSH configuration"""
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