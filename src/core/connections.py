"""
Connection management for SSH tunneling, database connections, and S3 clients.

This module provides context managers and connection pooling for all external
services used by the backup system, with proper error handling and cleanup.
"""

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from sshtunnel import SSHTunnelForwarder
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from contextlib import contextmanager
from typing import Generator, Optional, Dict, Any
import time
import socket
from pathlib import Path

from src.config.settings import AppConfig
from src.utils.exceptions import (
    ConnectionError, 
    raise_connection_error,
    ConfigurationError
)
from src.utils.logging import get_logger


logger = get_logger(__name__)


class ConnectionManager:
    """
    Manages all external connections for the backup system.
    
    Provides context managers for SSH tunnels, database connections,
    and S3 clients with proper resource management and error handling.
    """
    
    def __init__(self, config: AppConfig):
        self.config = config
        self._ssh_tunnel: Optional[SSHTunnelForwarder] = None
        self._db_pool: Optional[MySQLConnectionPool] = None
        self._s3_client: Optional[Any] = None
        
        # Validate configuration on initialization
        self._validate_config()
    
    def _validate_config(self):
        """Validate connection configuration"""
        # Check SSH key file exists
        ssh_key_path = Path(self.config.ssh.bastion_key_path)
        if not ssh_key_path.exists():
            raise ConfigurationError(f"SSH key file not found: {ssh_key_path}")
        
        # Validate SSH key permissions (should be 600)
        if ssh_key_path.stat().st_mode & 0o777 != 0o600:
            logger.warning(
                "SSH key file permissions are not 600",
                key_path=str(ssh_key_path),
                current_permissions=oct(ssh_key_path.stat().st_mode & 0o777)
            )
    
    @contextmanager
    def ssh_tunnel(self) -> Generator[int, None, None]:
        """
        Create SSH tunnel and return local port.
        
        Yields:
            Local port number for the established tunnel
            
        Raises:
            ConnectionError: If tunnel establishment fails
        """
        tunnel = None
        try:
            logger.info("Establishing SSH tunnel", bastion_host=self.config.ssh.bastion_host)
            
            # Create SSH tunnel
            tunnel = SSHTunnelForwarder(
                (self.config.ssh.bastion_host, 22),
                ssh_username=self.config.ssh.bastion_user,
                ssh_pkey=self.config.ssh.bastion_key_path,
                remote_bind_address=(self.config.database.host, self.config.database.port),
                local_bind_address=('127.0.0.1', self.config.ssh.local_port)
            )
            
            # Start tunnel with retry logic
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    tunnel.start()
                    break
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(
                        "SSH tunnel attempt failed, retrying",
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        error=str(e)
                    )
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            local_port = tunnel.local_bind_port
            self._ssh_tunnel = tunnel
            
            logger.info(
                "SSH tunnel established",
                local_port=local_port,
                remote_host=self.config.database.host,
                remote_port=self.config.database.port
            )
            
            # Test tunnel connectivity
            if not self._test_tunnel_connectivity(local_port):
                raise ConnectionError("SSH tunnel connectivity test failed")
            
            yield local_port
            
        except Exception as e:
            logger.error("Failed to establish SSH tunnel", error=str(e))
            raise_connection_error(
                "SSH tunnel",
                host=self.config.ssh.bastion_host,
                underlying_error=e
            )
        finally:
            if tunnel and tunnel.is_active:
                tunnel.stop()
                logger.info("SSH tunnel closed")
                self._ssh_tunnel = None
    
    def _test_tunnel_connectivity(self, local_port: int) -> bool:
        """Test if tunnel is working by attempting to connect to local port"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(('127.0.0.1', local_port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    @contextmanager
    def database_connection(self, local_port: int) -> Generator[mysql.connector.MySQLConnection, None, None]:
        """
        Create database connection through SSH tunnel.
        
        Args:
            local_port: Local port from SSH tunnel
            
        Yields:
            MySQL database connection
            
        Raises:
            ConnectionError: If database connection fails
        """
        conn = None
        try:
            logger.info("Establishing database connection", local_port=local_port)
            
            # Connection configuration
            conn_config = {
                'host': '127.0.0.1',
                'port': local_port,
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
            
            # Establish connection with retry
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    conn = mysql.connector.connect(**conn_config)
                    break
                except mysql.connector.Error as e:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(
                        "Database connection attempt failed, retrying",
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        error=str(e)
                    )
                    time.sleep(2 ** attempt)
            
            # Test connection
            if not conn.is_connected():
                raise ConnectionError("Database connection test failed")
            
            # Get server info for logging
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            server_version = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(
                "Database connection established",
                database=self.config.database.database,
                server_version=server_version
            )
            
            yield conn
            
        except mysql.connector.Error as e:
            logger.error("Database connection failed", error=str(e))
            raise_connection_error(
                "Database",
                host="127.0.0.1",
                port=local_port,
                underlying_error=e
            )
        except Exception as e:
            logger.error("Unexpected database connection error", error=str(e))
            raise_connection_error(
                "Database",
                underlying_error=e
            )
        finally:
            if conn and conn.is_connected():
                conn.close()
                logger.info("Database connection closed")
    
    def get_s3_client(self):
        """
        Create or return cached S3 client.
        
        Returns:
            Configured boto3 S3 client
            
        Raises:
            ConnectionError: If S3 client creation fails
        """
        if self._s3_client is not None:
            return self._s3_client
        
        try:
            logger.info("Creating S3 client", region=self.config.s3.region)
            
            # Create S3 client
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config.s3.access_key,
                aws_secret_access_key=self.config.s3.secret_key.get_secret_value(),
                region_name=self.config.s3.region
            )
            
            # Test S3 connectivity
            self._test_s3_connectivity()
            
            logger.info("S3 client created successfully", bucket=self.config.s3.bucket_name)
            return self._s3_client
            
        except (ClientError, BotoCoreError) as e:
            logger.error("S3 client creation failed", error=str(e))
            raise_connection_error(
                "S3",
                underlying_error=e
            )
        except Exception as e:
            logger.error("Unexpected S3 client error", error=str(e))
            raise_connection_error(
                "S3",
                underlying_error=e
            )
    
    def _test_s3_connectivity(self):
        """Test S3 connectivity by checking bucket access"""
        try:
            self._s3_client.head_bucket(Bucket=self.config.s3.bucket_name)
            logger.debug("S3 connectivity test passed")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise ConnectionError(f"S3 bucket not found: {self.config.s3.bucket_name}")
            elif error_code == '403':
                raise ConnectionError(f"Access denied to S3 bucket: {self.config.s3.bucket_name}")
            else:
                raise ConnectionError(f"S3 connectivity test failed: {error_code}")
    
    def create_connection_pool(self, pool_size: int = 5) -> MySQLConnectionPool:
        """
        Create a MySQL connection pool for high-performance scenarios.
        
        Note: This requires direct database access (not through SSH tunnel)
        and should only be used when SSH tunneling is not required.
        
        Args:
            pool_size: Number of connections in the pool
            
        Returns:
            MySQL connection pool
        """
        if self._db_pool is not None:
            return self._db_pool
        
        try:
            logger.info("Creating database connection pool", pool_size=pool_size)
            
            pool_config = {
                'pool_name': 'backup_pool',
                'pool_size': pool_size,
                'pool_reset_session': True,
                'host': self.config.database.host,
                'port': self.config.database.port,
                'user': self.config.database.user,
                'password': self.config.database.password.get_secret_value(),
                'database': self.config.database.database,
                'charset': 'utf8mb4',
                'use_unicode': True
            }
            
            self._db_pool = MySQLConnectionPool(**pool_config)
            
            logger.info("Database connection pool created")
            return self._db_pool
            
        except mysql.connector.Error as e:
            logger.error("Connection pool creation failed", error=str(e))
            raise_connection_error(
                "Database connection pool",
                host=self.config.database.host,
                port=self.config.database.port,
                underlying_error=e
            )
    
    @contextmanager
    def pooled_connection(self) -> Generator[mysql.connector.MySQLConnection, None, None]:
        """
        Get connection from pool (for use without SSH tunnel).
        
        Yields:
            MySQL connection from pool
        """
        if self._db_pool is None:
            raise ConnectionError("Connection pool not initialized")
        
        conn = None
        try:
            conn = self._db_pool.get_connection()
            logger.debug("Retrieved connection from pool")
            yield conn
        finally:
            if conn:
                conn.close()  # Returns connection to pool
                logger.debug("Returned connection to pool")
    
    def close_all_connections(self):
        """Close all cached connections and cleanup resources"""
        if self._ssh_tunnel and self._ssh_tunnel.is_active:
            self._ssh_tunnel.stop()
            self._ssh_tunnel = None
        
        if self._db_pool:
            # Close all connections in pool
            try:
                # Connection pools don't have a direct close method
                # Connections will be closed when pool is garbage collected
                self._db_pool = None
                logger.info("Database connection pool closed")
            except Exception as e:
                logger.warning("Error closing connection pool", error=str(e))
        
        self._s3_client = None
        logger.info("All connections closed")
    
    def health_check(self) -> Dict[str, str]:
        """
        Perform health check on all connection types.
        
        Returns:
            Dictionary with health status for each connection type
        """
        health = {}
        
        # Test S3 connectivity
        try:
            s3_client = self.get_s3_client()
            s3_client.head_bucket(Bucket=self.config.s3.bucket_name)
            health['s3'] = 'OK'
        except Exception as e:
            health['s3'] = f'ERROR: {str(e)}'
        
        # Test SSH connectivity
        try:
            with self.ssh_tunnel() as local_port:
                health['ssh'] = 'OK'
        except Exception as e:
            health['ssh'] = f'ERROR: {str(e)}'
        
        # Test database connectivity (through SSH)
        try:
            with self.ssh_tunnel() as local_port:
                with self.database_connection(local_port) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    health['database'] = 'OK'
        except Exception as e:
            health['database'] = f'ERROR: {str(e)}'
        
        return health