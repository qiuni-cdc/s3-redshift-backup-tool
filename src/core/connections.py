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
from src.core.connection_registry import ConnectionRegistry, ConnectionConfig
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
        
        # Initialize connection registry for multi-connection support
        try:
            self.connection_registry = ConnectionRegistry()
            logger.info("ConnectionRegistry initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize ConnectionRegistry, falling back to basic config: {e}")
            self.connection_registry = None
        
        # Validate configuration on initialization
        self._validate_config()
    
    def get_connection_config(self, connection_name: Optional[str] = None) -> ConnectionConfig:
        """
        Get connection configuration by name from ConnectionRegistry or fallback to basic config.
        
        Args:
            connection_name: Name of the connection (e.g., 'US_DW_UNIDW_SSH')
            
        Returns:
            ConnectionConfig object with connection details
        """
        if self.connection_registry and connection_name:
            try:
                # Try to get connection from registry
                conn_config = self.connection_registry.get_connection(connection_name)
                if conn_config:
                    logger.info(f"Using connection configuration for: {connection_name}")
                    return conn_config
            except Exception as e:
                logger.warning(f"Failed to get connection {connection_name} from registry: {e}")
        
        # Fallback to basic config from AppConfig
        logger.info("Using fallback DatabaseConfig configuration")
        
        # Handle case where password might be None (for connection-specific passwords)
        password = ""
        if hasattr(self.config.database, 'password') and self.config.database.password:
            password = self.config.database.password.get_secret_value()
        else:
            # Try to get password from environment directly
            import os
            password = os.getenv('DB_PASSWORD', '')
            if not password:
                logger.warning("No password found in DatabaseConfig or environment variables")
        
        return ConnectionConfig(
            name="default",
            type="mysql",
            host=self.config.database.host,
            port=self.config.database.port,
            database=self.config.database.database,
            username=self.config.database.user,
            password=password,
            ssh_tunnel={
                'enabled': True,
                'host': self.config.ssh.bastion_host,
                'username': self.config.ssh.bastion_user,
                'private_key_path': self.config.ssh.bastion_key_path,
                'local_port': self.config.ssh.local_port
            }
        )
    
    def _validate_config(self):
        """Validate connection configuration (lazy validation for SSH)"""
        # Skip all SSH validation if SSH is disabled (config.ssh is None)
        if self.config.ssh is None:
            return

        # Skip SSH validation for placeholder/template values
        ssh_key_path_str = self.config.ssh.bastion_key_path
        if ssh_key_path_str.startswith('/path/to/'):
            return

        # Only validate SSH key if it exists (lazy validation)
        ssh_key_path = Path(ssh_key_path_str)
        if not ssh_key_path.exists():
            return

        # Validate SSH key permissions (should be 600)
        if ssh_key_path.stat().st_mode & 0o777 != 0o600:
            logger.warning(
                "SSH key file permissions are not 600",
                key_path=str(ssh_key_path),
                current_permissions=oct(ssh_key_path.stat().st_mode & 0o777)
            )
    
    @contextmanager
    def ssh_tunnel(self, connection_name: Optional[str] = None) -> Generator[int, None, None]:
        """
        Create SSH tunnel and return local port (or None for direct connection).

        Args:
            connection_name: Optional connection name to use specific SSH configuration

        Yields:
            Local port number for the established tunnel, or None for direct connection

        Raises:
            ConnectionError: If tunnel establishment fails
        """
        tunnel = None
        try:
            # Get connection configuration (from registry or fallback)
            conn_config_obj = self.get_connection_config(connection_name)
            ssh_config = conn_config_obj.ssh_tunnel

            # Check if SSH is disabled
            if not ssh_config.get('enabled', True):
                logger.info("SSH tunnel disabled, using direct connection",
                           connection_name=connection_name or "default")
                yield None
                return

            # Check if global SSH config is None (when ssh_tunnel.enabled: false)
            if self.config.ssh is None:
                logger.info("SSH tunnel disabled (global config is None), using direct connection",
                           connection_name=connection_name or "default")
                yield None
                return

            logger.info("Establishing SSH tunnel",
                       bastion_host=ssh_config.get('host', self.config.ssh.bastion_host),
                       connection_name=connection_name or "default")
            
            # Create SSH tunnel
            tunnel = SSHTunnelForwarder(
                (ssh_config.get('host', self.config.ssh.bastion_host), 22),
                ssh_username=ssh_config.get('username', self.config.ssh.bastion_user),
                ssh_pkey=ssh_config.get('private_key_path', self.config.ssh.bastion_key_path),
                remote_bind_address=(conn_config_obj.host, conn_config_obj.port),
                local_bind_address=('127.0.0.1', ssh_config.get('local_port', self.config.ssh.local_port))
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

    @contextmanager
    def database_connection(self, local_port: int, connection_name: Optional[str] = None) -> Generator[mysql.connector.MySQLConnection, None, None]:
        """
        Create database connection through SSH tunnel.
        
        Args:
            local_port: Local port from SSH tunnel
            connection_name: Optional connection name to use specific configuration
            
        Yields:
            MySQL database connection
            
        Raises:
            ConnectionError: If database connection fails
        """
        conn = None
        try:
            # Get connection configuration (from registry or fallback)
            conn_config_obj = self.get_connection_config(connection_name)

            # Determine host and port based on whether SSH tunnel is used
            if local_port is None:
                # Direct connection (no SSH tunnel)
                host = conn_config_obj.host
                port = conn_config_obj.port
                logger.info(
                    "Establishing direct database connection",
                    host=host,
                    port=port,
                    connection_name=connection_name or "default",
                    database=conn_config_obj.database
                )
            else:
                # Connection through SSH tunnel
                host = '127.0.0.1'
                port = local_port
                logger.info(
                    "Establishing database connection through SSH tunnel",
                    local_port=local_port,
                    connection_name=connection_name or "default",
                    database=conn_config_obj.database
                )

            # Connection configuration
            conn_config = {
                'host': host,
                'port': port,
                'user': conn_config_obj.username,
                'password': conn_config_obj.password,
                'database': conn_config_obj.database,
                'autocommit': False,
                'raise_on_warnings': True,
                'compress': False,  # Disable compression (Test C-ext MTU handling)
                'use_pure': False   # Keep C-Extension (Clean config)
            }
            
            # Use safe timeout 
            conn_config['connection_timeout'] = 60
            
            logger.info("Initiating database connection...", config={k: v for k, v in conn_config.items() if k != 'password'})

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
    
    @contextmanager
    def redshift_ssh_tunnel(self) -> Generator[int, None, None]:
        """
        Create SSH tunnel for Redshift connection and return local port.

        Yields:
            Local port number for the established Redshift tunnel

        Raises:
            ConnectionError: If tunnel establishment fails
        """
        # Guard: this method should only be called when Redshift SSH is configured
        if self.config.redshift_ssh is None:
            raise RuntimeError("Redshift SSH is not configured but redshift_ssh_tunnel was called")

        tunnel = None
        try:
            logger.info("Establishing Redshift SSH tunnel", bastion_host=self.config.redshift_ssh.host)

            # Create SSH tunnel for Redshift
            tunnel = SSHTunnelForwarder(
                (self.config.redshift_ssh.host, 22),
                ssh_username=self.config.redshift_ssh.username,
                ssh_pkey=self.config.redshift_ssh.private_key_path,
                remote_bind_address=(self.config.redshift.host, self.config.redshift.port),
                local_bind_address=('127.0.0.1', self.config.redshift_ssh.local_port)
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
                        "Redshift SSH tunnel attempt failed, retrying",
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        error=str(e)
                    )
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            local_port = tunnel.local_bind_port
            self._redshift_ssh_tunnel = tunnel
            
            logger.info(
                "Redshift SSH tunnel established",
                local_port=local_port,
                remote_host=self.config.redshift.host,
                remote_port=self.config.redshift.port
            )
            
            # Test connectivity
            if not self._test_tunnel_connectivity(local_port):
                raise ConnectionError("Redshift SSH tunnel connectivity test failed")
            
            yield local_port
            
        except Exception as e:
            if tunnel:
                try:
                    tunnel.stop()
                except:
                    pass
            error_msg = f"Failed to establish Redshift SSH tunnel: {e}"
            logger.error(error_msg, bastion_host=self.config.redshift_ssh.host)
            raise_connection_error("redshift_ssh_tunnel", error_msg)
        
        finally:
            if tunnel and tunnel.is_active:
                tunnel.stop()
                logger.info("Redshift SSH tunnel closed")
                self._redshift_ssh_tunnel = None
    
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
    def database_session(self, connection_name: Optional[str] = None) -> Generator[mysql.connector.MySQLConnection, None, None]:
        """
        Create database session with SSH tunnel using connection configuration.
        
        This is a convenience method that combines SSH tunnel and database connection
        based on the connection name from connections.yml.
        
        Args:
            connection_name: Name of the connection from connections.yml (e.g., 'US_DW_UNIDW_SSH')
            
        Yields:
            MySQL database connection
            
        Raises:
            ConnectionError: If tunnel or database connection fails
        """
        with self.ssh_tunnel(connection_name) as local_port:
            with self.database_connection(local_port, connection_name) as db_conn:
                yield db_conn
    
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
            logger.info("Creating optimized S3 client", region=self.config.s3.region)
            
            # Import boto3 optimization components
            from botocore.config import Config
            
            # Create optimized boto3 configuration
            boto_config = Config(
                region_name=self.config.s3.region,
                retries={
                    'max_attempts': self.config.s3.retry_max_attempts,
                    'mode': self.config.s3.retry_mode
                },
                max_pool_connections=self.config.s3.max_pool_connections,
                # Enable TCP keepalive for long-running connections
                tcp_keepalive=True,
                # Timeout settings to prevent indefinite hangs
                connect_timeout=60,   # 60 seconds to establish S3 connection
                read_timeout=1800     # 30 minutes to read S3 response (for large files)
            )
            
            # Create optimized S3 client
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config.s3.access_key,
                aws_secret_access_key=self.config.s3.secret_key.get_secret_value(),
                config=boto_config
            )
            
            # Test S3 connectivity
            self._test_s3_connectivity()
            
            logger.info(
                "Optimized S3 client created successfully", 
                bucket=self.config.s3.bucket_name,
                multipart_threshold=f"{self.config.s3.multipart_threshold // 1024 // 1024}MB",
                max_concurrency=self.config.s3.max_concurrency,
                max_pool_connections=self.config.s3.max_pool_connections
            )
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
    
    def get_s3_transfer_config(self):
        """
        Get optimized S3 transfer configuration for large file uploads.
        
        Returns:
            boto3.s3.transfer.TransferConfig: Optimized transfer configuration
        """
        from boto3.s3.transfer import TransferConfig
        
        transfer_config = TransferConfig(
            multipart_threshold=self.config.s3.multipart_threshold,
            multipart_chunksize=self.config.s3.multipart_chunksize,
            max_concurrency=self.config.s3.max_concurrency,
            max_bandwidth=self.config.s3.max_bandwidth,
            use_threads=True
        )
        
        logger.debug(
            "S3 transfer config created",
            multipart_threshold=f"{self.config.s3.multipart_threshold // 1024 // 1024}MB",
            chunk_size=f"{self.config.s3.multipart_chunksize // 1024 // 1024}MB",
            max_concurrency=self.config.s3.max_concurrency
        )
        
        return transfer_config
    
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
        
        # Test SSH connectivity using configured connection
        try:
            # Use the connection config that was populated by create_app_config
            if hasattr(self.config, 'ssh') and self.config.ssh is not None and self.config.ssh.bastion_host:
                with self.ssh_tunnel() as local_port:
                    health['ssh'] = 'OK'
            else:
                health['ssh'] = 'SKIPPED: No SSH tunnel configured'
        except Exception as e:
            health['ssh'] = f'ERROR: {str(e)}'

        # Test database connectivity (through SSH)
        try:
            if hasattr(self.config, 'database') and self.config.database.host:
                with self.ssh_tunnel() as local_port:
                    with self.database_connection(local_port) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        cursor.close()
                        health['database'] = 'OK'
            else:
                health['database'] = 'SKIPPED: No database configured'
        except Exception as e:
            health['database'] = f'ERROR: {str(e)}'
        
        # Test Redshift connectivity (if configured)
        try:
            if hasattr(self.config, 'redshift') and self.config.redshift.host:
                import psycopg2
                
                # Use Redshift SSH tunnel if configured and not None
                if (hasattr(self.config, 'redshift_ssh') and
                    self.config.redshift_ssh is not None and
                    self.config.redshift_ssh.host and
                    self.config.redshift_ssh.host not in ['None', '', 'null']):
                    with self.redshift_ssh_tunnel() as local_port:
                        conn = psycopg2.connect(
                            host='localhost',
                            port=local_port,
                            database=self.config.redshift.database,
                            user=self.config.redshift.user,
                            password=self.config.redshift.password.get_secret_value(),
                            options=f'-c search_path={self.config.redshift.schema}'
                        )
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        cursor.close()
                        conn.close()
                        health['redshift'] = 'OK'
                else:
                    # Direct connection
                    conn = psycopg2.connect(
                        host=self.config.redshift.host,
                        port=self.config.redshift.port,
                        database=self.config.redshift.database,
                        user=self.config.redshift.user,
                        password=self.config.redshift.password.get_secret_value(),
                        options=f'-c search_path={self.config.redshift.schema}'
                    )
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    conn.close()
                    health['redshift'] = 'OK'
            else:
                health['redshift'] = 'SKIPPED: Not configured'
        except Exception as e:
            health['redshift'] = f'ERROR: {str(e)}'
        
        return health