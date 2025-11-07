"""
Connection Registry System for Multi-Schema Support (v1.1.0)

Manages multiple database connections with pooling, validation, and SSH tunnel support.
Provides the foundation for multi-schema data integration while maintaining backward compatibility.
"""

import os
import time
import socket
import logging
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
import yaml
import mysql.connector
import mysql.connector.pooling
import psycopg2
import psycopg2.pool
from sshtunnel import SSHTunnelForwarder

from src.utils.exceptions import ValidationError, ConnectionError
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConnectionConfig:
    """Connection configuration with validation and defaults"""
    name: str
    type: str  # 'mysql' or 'redshift'
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: Optional[str] = None
    connection_pool: Dict[str, Any] = field(default_factory=dict)
    ssh_tunnel: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    
    def __post_init__(self):
        """Post-initialization validation and defaults"""
        # Set default schema for Redshift
        if self.type == 'redshift' and not self.schema:
            self.schema = 'public'
        
        # Set default connection pool settings
        if not self.connection_pool:
            if self.type == 'mysql':
                self.connection_pool = {
                    'min_connections': 2,
                    'max_connections': 10,
                    'retry_attempts': 3
                }
            elif self.type == 'redshift':
                self.connection_pool = {
                    'min_connections': 1,
                    'max_connections': 5,
                    'retry_attempts': 2
                }


class ConnectionRegistry:
    """
    Manages multiple database connections with pooling and validation.
    
    Key Features:
    - Configuration-driven connection management via YAML
    - Connection pooling for MySQL with automatic retry
    - SSH tunnel support for secure Redshift connections
    - Health monitoring and connection validation
    - Environment variable interpolation
    - Backward compatibility with v1.0.0 environment variables
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize connection registry
        
        Args:
            config_path: Path to connections.yml file. If None, auto-detects.
        """
        # Load environment variables from .env file first
        self._load_environment_variables()
        
        self.config_path = self._resolve_config_path(config_path)
        self.connections: Dict[str, ConnectionConfig] = {}
        self.mysql_pools: Dict[str, mysql.connector.pooling.MySQLConnectionPool] = {}
        self.redshift_pools: Dict[str, psycopg2.pool.ThreadedConnectionPool] = {}
        self.ssh_tunnels: Dict[str, SSHTunnelForwarder] = {}
        
        # Connection settings
        self.default_timeout = 30
        self.connection_retry_delay = 5
        self.pool_recycle_time = 3600
        self.health_check_interval = 300
        
        # Load and validate configurations
        self._ensure_config_directory()
        self._load_connections()
        self._validate_configurations()
        
        logger.info(f"Connection registry initialized with {len(self.connections)} connections")
        logger.debug(f"Available connections: {list(self.connections.keys())}")
    
    def _load_environment_variables(self):
        """Load environment variables from .env file"""
        try:
            from dotenv import load_dotenv
        except ImportError:
            # Fallback to manual .env file parsing if dotenv not available
            logger.debug("python-dotenv not available, using manual .env file parsing")
            self._load_env_file_manual()
            return
        
        # Try to find .env file in multiple locations
        env_locations = [
            Path(".env"),
            Path("../.env"),
            Path.cwd() / ".env",
            Path.home() / ".env"
        ]
        
        loaded = False
        for env_path in env_locations:
            if env_path.exists():
                try:
                    load_dotenv(env_path, override=False)  # Don't override existing env vars
                    logger.debug(f"Loaded environment variables from: {env_path}")
                    loaded = True
                    break
                except Exception as e:
                    logger.warning(f"Failed to load environment variables from {env_path}: {e}")
        
        if not loaded:
            logger.debug("No .env file found, using system environment variables only")
    
    def _load_env_file_manual(self):
        """Manually load .env file if dotenv package not available"""
        env_locations = [
            Path(".env"),
            Path("../.env"),
            Path.cwd() / ".env",
            Path.home() / ".env"
        ]
        
        for env_path in env_locations:
            if env_path.exists():
                try:
                    with open(env_path, 'r') as f:
                        for line_num, line in enumerate(f, 1):
                            line = line.strip()
                            # Skip empty lines and comments
                            if not line or line.startswith('#'):
                                continue
                            
                            # Parse KEY=VALUE format
                            if '=' in line:
                                key, value = line.split('=', 1)
                                key = key.strip()
                                value = value.strip()
                                
                                # Remove quotes if present
                                if (value.startswith('"') and value.endswith('"')) or \
                                   (value.startswith("'") and value.endswith("'")):
                                    value = value[1:-1]
                                
                                # Only set if not already in environment (don't override)
                                if key not in os.environ:
                                    os.environ[key] = value
                    
                    logger.debug(f"Manually loaded environment variables from: {env_path}")
                    return
                    
                except Exception as e:
                    logger.warning(f"Failed to manually load environment variables from {env_path}: {e}")
        
        logger.debug("No .env file found, using system environment variables only")
    
    def _resolve_config_path(self, config_path: Optional[str]) -> Path:
        """Resolve configuration file path with fallbacks"""
        if config_path:
            return Path(config_path)
        
        # Try multiple locations
        candidates = [
            Path("config/connections.yml"),
            Path("connections.yml"),
            Path("config/connections.yaml"),
            Path("connections.yaml")
        ]
        
        for candidate in candidates:
            if candidate.exists():
                return candidate
        
        # Return default path (will be created if needed)
        return Path("config/connections.yml")
    
    def _ensure_config_directory(self):
        """Ensure configuration directory exists"""
        config_dir = self.config_path.parent
        config_dir.mkdir(parents=True, exist_ok=True)
        
        # Create default configuration if none exists
        if not self.config_path.exists():
            logger.info("No connection configuration found, creating default configuration")
            self._create_default_config()
    
    def _create_default_config(self):
        """Create default configuration template (v1.2.0 style - no 'default' connections)"""
        default_config = {
            'connections': {
                'sources': {
                    'mysql_source': {
                        'type': 'mysql',
                        'host': '${MYSQL_HOST}',
                        'port': 3306,
                        'database': '${MYSQL_DATABASE}',
                        'username': '${MYSQL_USERNAME}',
                        'password': '${MYSQL_PASSWORD}',
                        'description': 'Example MySQL source connection - rename and configure as needed'
                    }
                },
                'targets': {
                    'redshift_target': {
                        'type': 'redshift',
                        'host': '${REDSHIFT_HOST}',
                        'port': 5439,
                        'database': '${REDSHIFT_DATABASE}',
                        'username': '${REDSHIFT_USERNAME}',
                        'password': '${REDSHIFT_PASSWORD}',
                        'schema': 'public',
                        'ssh_tunnel': {
                            'enabled': True,
                            'host': '${SSH_HOST}',
                            'port': 22,
                            'username': '${SSH_USER}',
                            'private_key_path': '${SSH_KEY_PATH}'
                        },
                        'description': 'Example Redshift target connection - rename and configure as needed'
                    }
                }
            },
            'connection_settings': {
                'default_timeout': 30,
                'connection_retry_delay': 5,
                'pool_recycle_time': 3600,
                'health_check_interval': 300
            }
        }
        
        with open(self.config_path, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, indent=2)
        
        logger.info(f"Created default connection configuration: {self.config_path}")
    
    def _load_connections(self):
        """Load connection configurations from YAML with environment variable interpolation"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Apply global connection settings
            global_settings = config.get('connection_settings', {})
            self.default_timeout = global_settings.get('default_timeout', 30)
            self.connection_retry_delay = global_settings.get('connection_retry_delay', 5)
            self.pool_recycle_time = global_settings.get('pool_recycle_time', 3600)
            self.health_check_interval = global_settings.get('health_check_interval', 300)
            
            connections_config = config.get('connections', {})
            
            # Load source connections (MySQL)
            for name, conn_config in connections_config.get('sources', {}).items():
                processed_config = self._interpolate_environment_variables(conn_config)
                # Remove 'type' from processed_config to avoid conflict with explicit type parameter
                processed_config.pop('type', None)
                self.connections[name] = ConnectionConfig(
                    name=name,
                    type='mysql',
                    **processed_config
                )
                logger.debug(f"Loaded source connection: {name}")

            # Load target connections (Redshift)
            for name, conn_config in connections_config.get('targets', {}).items():
                processed_config = self._interpolate_environment_variables(conn_config)
                # Remove 'type' from processed_config to avoid conflict with explicit type parameter
                processed_config.pop('type', None)
                self.connections[name] = ConnectionConfig(
                    name=name,
                    type='redshift',
                    **processed_config
                )
                logger.debug(f"Loaded target connection: {name}")
            
            if not self.connections:
                raise ValidationError("No connections found in configuration")
            
            logger.info(f"Successfully loaded {len(self.connections)} connection configurations")
            
        except FileNotFoundError:
            raise ValidationError(f"Connection configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValidationError(f"Invalid YAML in connection configuration: {e}")
        except Exception as e:
            logger.error(f"Failed to load connection configurations: {e}")
            raise ValidationError(f"Connection configuration error: {e}")
    
    def _interpolate_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Interpolate environment variables in configuration values"""
        import re
        
        def interpolate_value(value):
            if isinstance(value, str):
                # Replace ${VAR_NAME} with environment variable value
                def replace_var(match):
                    var_name = match.group(1)
                    env_value = os.getenv(var_name)
                    if env_value is None:
                        logger.warning(f"Environment variable {var_name} not found, using placeholder")
                        return f"${{{var_name}}}"  # Keep original if not found
                    return env_value
                
                return re.sub(r'\$\{([A-Z_][A-Z0-9_]*)\}', replace_var, value)
            elif isinstance(value, dict):
                return {k: interpolate_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [interpolate_value(item) for item in value]
            else:
                return value
        
        return interpolate_value(config)
    
    def _validate_configurations(self):
        """Validate all connection configurations"""
        for name, config in self.connections.items():
            try:
                self._validate_single_config(config)
                logger.debug(f"Connection configuration validated: {name}")
            except Exception as e:
                logger.error(f"Invalid configuration for {name}: {e}")
                raise ValidationError(f"Configuration validation failed for {name}: {e}")
    
    def _validate_single_config(self, config: ConnectionConfig):
        """Validate a single connection configuration"""
        # Check for placeholder values (environment variables not resolved)
        placeholder_pattern = r'\$\{[A-Z_][A-Z0-9_]*\}'
        import re
        
        critical_fields = [config.host, config.database, config.username, config.password]
        for field in critical_fields:
            if isinstance(field, str) and re.search(placeholder_pattern, field):
                logger.warning(f"Unresolved environment variable in {config.name}: {field}")
        
        # Required fields validation
        if not config.host or not config.database or not config.username:
            raise ValidationError(f"Missing required connection fields for {config.name}")
        
        # Type-specific validation
        if config.type not in ['mysql', 'redshift']:
            raise ValidationError(f"Unsupported connection type: {config.type}")
        
        # Port validation
        if not isinstance(config.port, int) or not (1 <= config.port <= 65535):
            raise ValidationError(f"Invalid port for {config.name}: {config.port}")
        
        # SSH tunnel validation for both MySQL and Redshift
        if config.ssh_tunnel and config.ssh_tunnel.get('enabled', False):
            required_ssh_fields = ['host', 'username']
            for field in required_ssh_fields:
                if not config.ssh_tunnel.get(field):
                    raise ValidationError(f"Missing SSH tunnel field '{field}' for {config.name}")
    
    @contextmanager
    def get_mysql_connection(self, connection_name: str):
        """
        Get MySQL connection with automatic pooling, retry logic, and proper cleanup
        
        Args:
            connection_name: Name of the connection configuration
            
        Yields:
            MySQL connection from pool
            
        Raises:
            ValidationError: If connection configuration is invalid
            ConnectionError: If connection cannot be established
        """
        if connection_name not in self.connections:
            raise ValidationError(f"Unknown connection: {connection_name}")
        
        config = self.connections[connection_name]
        if config.type != 'mysql':
            raise ValidationError(f"Connection {connection_name} is not a MySQL connection")
        
        # Create pool if it doesn't exist
        if connection_name not in self.mysql_pools:
            self._create_mysql_pool(connection_name, config)
        
        connection = None
        try:
            connection = self.mysql_pools[connection_name].get_connection()
            
            # Test connection with simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            logger.debug(f"Retrieved MySQL connection for {connection_name}")
            yield connection
            
        except mysql.connector.Error as e:
            logger.error(f"Failed to get MySQL connection for {connection_name}: {e}")
            
            # Try to recreate pool on connection failure
            if connection_name in self.mysql_pools:
                del self.mysql_pools[connection_name]
                
                # Also clean up SSH tunnel if it exists
                if connection_name in self.ssh_tunnels:
                    try:
                        self.ssh_tunnels[connection_name].stop()
                        del self.ssh_tunnels[connection_name]
                    except Exception as tunnel_cleanup_error:
                        logger.warning(f"Error cleaning up SSH tunnel during MySQL reconnection: {tunnel_cleanup_error}")
                
                try:
                    self._create_mysql_pool(connection_name, config)
                    connection = self.mysql_pools[connection_name].get_connection()
                    logger.info(f"Successfully recreated MySQL connection pool for {connection_name}")
                    yield connection
                except Exception as retry_e:
                    logger.error(f"Failed to recreate MySQL pool: {retry_e}")
                    raise ConnectionError(f"MySQL connection failed for {connection_name}: {retry_e}")
            else:
                raise ConnectionError(f"MySQL connection failed for {connection_name}: {e}")
        
        finally:
            # Ensure connection is returned to pool
            if connection is not None:
                try:
                    connection.close()  # This returns connection to pool
                    logger.debug(f"Returned MySQL connection to pool for {connection_name}")
                except Exception as e:
                    logger.warning(f"Error returning MySQL connection to pool: {e}")
    
    def get_mysql_connection_direct(self, connection_name: str) -> mysql.connector.MySQLConnection:
        """
        Get MySQL connection directly (for backward compatibility - use context manager version when possible)
        
        WARNING: Caller is responsible for closing the connection to return it to the pool
        """
        if connection_name not in self.connections:
            raise ValidationError(f"Unknown connection: {connection_name}")
        
        config = self.connections[connection_name]
        if config.type != 'mysql':
            raise ValidationError(f"Connection {connection_name} is not a MySQL connection")
        
        # Create pool if it doesn't exist
        if connection_name not in self.mysql_pools:
            self._create_mysql_pool(connection_name, config)
        
        try:
            connection = self.mysql_pools[connection_name].get_connection()
            
            # Test connection with simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            logger.debug(f"Retrieved MySQL connection for {connection_name}")
            return connection
            
        except mysql.connector.Error as e:
            logger.error(f"Failed to get MySQL connection for {connection_name}: {e}")
            
            # Try to recreate pool on connection failure
            if connection_name in self.mysql_pools:
                del self.mysql_pools[connection_name]
                
                # Also clean up SSH tunnel if it exists
                if connection_name in self.ssh_tunnels:
                    try:
                        self.ssh_tunnels[connection_name].stop()
                        del self.ssh_tunnels[connection_name]
                    except Exception as tunnel_cleanup_error:
                        logger.warning(f"Error cleaning up SSH tunnel during MySQL reconnection: {tunnel_cleanup_error}")
                
                try:
                    self._create_mysql_pool(connection_name, config)
                    connection = self.mysql_pools[connection_name].get_connection()
                    logger.info(f"Successfully recreated MySQL connection pool for {connection_name}")
                    return connection
                except Exception as retry_e:
                    logger.error(f"Failed to recreate MySQL pool: {retry_e}")
            
            raise ConnectionError(f"MySQL connection failed for {connection_name}: {e}")
    
    def _create_mysql_pool(self, name: str, config: ConnectionConfig):
        """Create MySQL connection pool with optimized settings and SSH tunnel support"""
        
        pool_config = config.connection_pool
        
        # Set up SSH tunnel if configured for MySQL
        ssh_tunnel = None
        target_host = config.host
        target_port = config.port
        
        if config.ssh_tunnel and config.ssh_tunnel.get('enabled', False):
            try:
                ssh_tunnel = self._create_ssh_tunnel(name, config)
                target_host = 'localhost'
                target_port = ssh_tunnel.local_bind_port
                logger.info(f"SSH tunnel established for MySQL {name} on local port {target_port}")
                
                # Wait a moment for tunnel to stabilize before creating pool
                time.sleep(0.5)
                
            except Exception as ssh_error:
                logger.error(f"SSH tunnel creation failed for MySQL {name}: {ssh_error}")
                raise ConnectionError(f"MySQL SSH tunnel setup failed for {name}: {ssh_error}")
        
        pool_settings = {
            'pool_name': f"{name}_pool",
            'pool_size': pool_config.get('max_connections', 10),
            'pool_reset_session': True,
            'host': target_host,
            'port': target_port,
            'database': config.database,
            'user': config.username,
            'password': config.password,
            'charset': 'utf8mb4',
            'use_unicode': True,
            'autocommit': True,
            'connection_timeout': self.default_timeout,
            'sql_mode': 'TRADITIONAL',
            'raise_on_warnings': True,
        }
        
        # Retry pool creation with exponential backoff
        max_attempts = 3
        retry_delay = 1.0
        
        for attempt in range(max_attempts):
            try:
                logger.debug(f"Creating MySQL connection pool for {name} (attempt {attempt + 1})")
                self.mysql_pools[name] = mysql.connector.pooling.MySQLConnectionPool(**pool_settings)
                
                # Test pool with a simple connection
                test_conn = self.mysql_pools[name].get_connection()
                try:
                    cursor = test_conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    logger.debug(f"MySQL pool test connection successful for {name}")
                finally:
                    test_conn.close()
                
                if ssh_tunnel:
                    logger.info(
                        f"Created MySQL connection pool for {name}: "
                        f"{pool_settings['pool_size']} connections via SSH tunnel localhost:{target_port} -> {config.host}:{config.port}/{config.database}"
                    )
                else:
                    logger.info(
                        f"Created MySQL connection pool for {name}: "
                        f"{pool_settings['pool_size']} connections to {config.host}:{config.port}/{config.database}"
                    )
                return  # Success, exit retry loop
                
            except mysql.connector.Error as e:
                logger.warning(f"MySQL pool creation attempt {attempt + 1} failed for {name}: {e}")
                
                if attempt < max_attempts - 1:
                    logger.debug(f"Retrying MySQL pool creation for {name} in {retry_delay}s")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    # Final attempt failed, clean up SSH tunnel if created
                    if ssh_tunnel and name in self.ssh_tunnels:
                        try:
                            ssh_tunnel.stop()
                            del self.ssh_tunnels[name]
                        except Exception as cleanup_error:
                            logger.warning(f"Error cleaning up SSH tunnel after pool creation failure: {cleanup_error}")
                    
                    logger.error(f"Failed to create MySQL pool for {name} after {max_attempts} attempts: {e}")
                    raise ConnectionError(f"MySQL pool creation failed for {name}: {e}")
            except Exception as e:
                # Non-MySQL errors (e.g., configuration issues)
                if ssh_tunnel and name in self.ssh_tunnels:
                    try:
                        ssh_tunnel.stop()
                        del self.ssh_tunnels[name]
                    except Exception as cleanup_error:
                        logger.warning(f"Error cleaning up SSH tunnel after pool creation failure: {cleanup_error}")
                
                logger.error(f"Failed to create MySQL pool for {name}: {e}")
                raise ConnectionError(f"MySQL pool creation failed for {name}: {e}")
    
    @contextmanager
    def get_redshift_connection(self, connection_name: str):
        """
        Get Redshift connection with SSH tunnel support
        
        Args:
            connection_name: Name of the connection configuration
            
        Yields:
            psycopg2 connection to Redshift
            
        Raises:
            ValidationError: If connection configuration is invalid
            ConnectionError: If connection cannot be established
        """
        if connection_name not in self.connections:
            raise ValidationError(f"Unknown connection: {connection_name}")
        
        config = self.connections[connection_name]
        if config.type != 'redshift':
            raise ValidationError(f"Connection {connection_name} is not a Redshift connection")
        
        ssh_tunnel = None
        local_port = config.port
        connection = None
        
        try:
            # Set up SSH tunnel if configured
            if config.ssh_tunnel and config.ssh_tunnel.get('enabled', False):
                try:
                    ssh_tunnel = self._create_ssh_tunnel(connection_name, config)
                    local_port = ssh_tunnel.local_bind_port
                    logger.debug(f"SSH tunnel established for {connection_name} on local port {local_port}")
                    
                    # Wait a moment for tunnel to stabilize before connecting
                    time.sleep(0.5)
                    
                except Exception as ssh_error:
                    logger.error(f"SSH tunnel creation failed for {connection_name}: {ssh_error}")
                    raise ConnectionError(f"SSH tunnel setup failed for {connection_name}: {ssh_error}")
            
            # Create Redshift connection with retry logic
            connection_params = {
                'host': 'localhost' if ssh_tunnel else config.host,
                'port': local_port,
                'database': config.database,
                'user': config.username,
                'password': config.password,
                'sslmode': 'prefer',
                'connect_timeout': self.default_timeout,
                'options': f'-c search_path={config.schema}' if config.schema else None
            }
            
            # Remove None values from connection params
            connection_params = {k: v for k, v in connection_params.items() if v is not None}
            
            max_attempts = 3
            retry_delay = 1.0
            
            for attempt in range(max_attempts):
                try:
                    logger.debug(f"Connecting to Redshift {connection_name} (attempt {attempt + 1})")
                    connection = psycopg2.connect(**connection_params)
                    connection.set_session(autocommit=True)
                    
                    # Test connection
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                    
                    logger.debug(f"Established Redshift connection for {connection_name}")
                    
                    yield connection
                    return  # Success, exit retry loop
                    
                except psycopg2.Error as db_error:
                    logger.warning(f"Redshift connection attempt {attempt + 1} failed for {connection_name}: {db_error}")
                    
                    if connection:
                        try:
                            connection.close()
                            connection = None
                        except:
                            pass
                    
                    if attempt < max_attempts - 1:
                        logger.debug(f"Retrying Redshift connection for {connection_name} in {retry_delay}s")
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Exponential backoff
                    else:
                        logger.error(f"Redshift database connection failed for {connection_name} after {max_attempts} attempts: {db_error}")
                        raise ConnectionError(f"Redshift database connection failed for {connection_name}: {db_error}")
            
        except Exception as e:
            logger.error(f"Redshift connection failed for {connection_name}: {e}")
            raise ConnectionError(f"Redshift connection failed for {connection_name}: {e}")
        
        finally:
            # Cleanup in reverse order
            if connection:
                try:
                    connection.close()
                    logger.debug(f"Closed Redshift connection for {connection_name}")
                except Exception as e:
                    logger.warning(f"Error closing Redshift connection: {e}")
            
            if ssh_tunnel:
                try:
                    ssh_tunnel.stop()
                    if connection_name in self.ssh_tunnels:
                        del self.ssh_tunnels[connection_name]
                    logger.debug(f"SSH tunnel closed for {connection_name}")
                except Exception as e:
                    logger.warning(f"Error closing SSH tunnel: {e}")
    
    def _create_ssh_tunnel(self, name: str, config: ConnectionConfig) -> SSHTunnelForwarder:
        """Create SSH tunnel with enhanced error handling and proper timing"""
        
        ssh_config = config.ssh_tunnel
        
        # Check if tunnel already exists and is active
        if name in self.ssh_tunnels:
            existing_tunnel = self.ssh_tunnels[name]
            if existing_tunnel.is_active:
                logger.debug(f"Reusing existing SSH tunnel for {name} on port {existing_tunnel.local_bind_port}")
                return existing_tunnel
            else:
                # Clean up inactive tunnel
                try:
                    existing_tunnel.stop()
                    del self.ssh_tunnels[name]
                    logger.debug(f"Cleaned up inactive SSH tunnel for {name}")
                except Exception as cleanup_error:
                    logger.warning(f"Error cleaning up inactive tunnel: {cleanup_error}")
        
        # Determine authentication method
        auth_params = {}
        if ssh_config.get('private_key_path'):
            private_key_path = ssh_config['private_key_path']
            # Validate key file exists
            if not Path(private_key_path).exists():
                raise ConnectionError(f"SSH private key not found: {private_key_path}")
            auth_params['ssh_private_key'] = private_key_path
        elif ssh_config.get('password'):
            auth_params['ssh_password'] = ssh_config['password']
        else:
            # Try to use SSH agent or default key locations
            logger.debug(f"No explicit SSH authentication provided for {name}, using default methods")
        
        # Create a compatible logger for SSHTunnelForwarder
        # The SSHTunnelForwarder expects a standard Python logger, not structlog
        ssh_logger = logging.getLogger(f'sshtunnel_{name}')
        ssh_logger.setLevel(logging.WARNING)  # Reduce verbose SSH output
        
        tunnel_params = {
            'ssh_address_or_host': (ssh_config['host'], ssh_config.get('port', 22)),
            'ssh_username': ssh_config['username'],
            'remote_bind_address': (config.host, config.port),
            'local_bind_address': ('127.0.0.1', 0),  # Auto-assign local port
            'logger': ssh_logger,
            'set_keepalive': 30.0,  # Keep connection alive
            'compression': True,  # Enable SSH compression
            **auth_params
        }
        
        tunnel = None
        try:
            tunnel = SSHTunnelForwarder(**tunnel_params)
            tunnel.daemon_forward_servers = False  # Use non-daemon threads for clean shutdown
            tunnel.daemon_transport = False  # Ensure transport threads also non-daemon
            tunnel.skip_tunnel_checkup = False  # Enable tunnel health checks
            
            logger.debug(f"Starting SSH tunnel for {name} to {ssh_config['host']}...")
            
            # Start tunnel with timeout
            try:
                tunnel.start()
            except Exception as start_error:
                logger.error(f"Failed to start SSH tunnel for {name}: {start_error}")
                if tunnel:
                    try:
                        tunnel.stop()
                    except:
                        pass  # Ignore cleanup errors
                raise ConnectionError(f"SSH tunnel start failed for {name}: {start_error}")
            
            # Wait for tunnel to become fully active with retries
            max_wait_attempts = 10
            wait_interval = 0.5
            
            for attempt in range(max_wait_attempts):
                if tunnel.is_active:
                    # Additional verification: try to connect to local port
                    try:
                        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        test_socket.settimeout(2)
                        result = test_socket.connect_ex(('127.0.0.1', tunnel.local_bind_port))
                        test_socket.close()
                        
                        if result == 0:
                            logger.debug(f"SSH tunnel for {name} verified active on port {tunnel.local_bind_port}")
                            break
                        else:
                            logger.debug(f"SSH tunnel port check failed for {name}, attempt {attempt + 1}")
                    except Exception as port_check_error:
                        logger.debug(f"Port verification failed for {name}: {port_check_error}")
                
                if attempt < max_wait_attempts - 1:
                    time.sleep(wait_interval)
                    wait_interval = min(wait_interval * 1.2, 2.0)  # Exponential backoff
            else:
                # Tunnel failed to become properly active
                logger.error(f"SSH tunnel for {name} failed to become fully active after {max_wait_attempts} attempts")
                try:
                    tunnel.stop()
                except:
                    pass
                raise ConnectionError(f"SSH tunnel for {name} failed to become fully active")
            
            # Store tunnel reference for reuse and cleanup
            self.ssh_tunnels[name] = tunnel
            
            logger.info(
                f"SSH tunnel established for {name}: "
                f"localhost:{tunnel.local_bind_port} -> {config.host}:{config.port} "
                f"(via {ssh_config['host']})"
            )
            
            return tunnel
            
        except ConnectionError:
            # Re-raise connection errors without modification
            raise
        except Exception as e:
            logger.error(f"Failed to create SSH tunnel for {name}: {e}")
            if tunnel:
                try:
                    tunnel.stop()
                except:
                    pass
            raise ConnectionError(f"SSH tunnel creation failed for {name}: {e}")
    
    def get_connection(self, connection_name: str) -> ConnectionConfig:
        """Get connection configuration by name"""
        if connection_name not in self.connections:
            available = list(self.connections.keys())
            raise ValidationError(f"Unknown connection: {connection_name}. Available: {available}")
        
        return self.connections[connection_name]
    
    def get_connection_info(self, connection_name: str) -> Dict[str, Any]:
        """Get connection information without sensitive data"""
        if connection_name not in self.connections:
            raise ValidationError(f"Unknown connection: {connection_name}")
        
        config = self.connections[connection_name]
        
        return {
            'name': config.name,
            'type': config.type,
            'host': config.host,
            'port': config.port,
            'database': config.database,
            'username': config.username,  # Include username for reference
            'schema': config.schema,
            'description': config.description,
            'has_ssh_tunnel': bool(config.ssh_tunnel and config.ssh_tunnel.get('enabled', False)),
            'pool_settings': {
                k: v for k, v in config.connection_pool.items() 
                if k not in ['password']  # Exclude sensitive data
            },
            'status': 'active' if connection_name in self.mysql_pools or connection_name in self.ssh_tunnels else 'configured'
        }
    
    def list_connections(self) -> Dict[str, Dict[str, Any]]:
        """List all available connections with their information"""
        return {
            name: self.get_connection_info(name)
            for name in sorted(self.connections.keys())
        }
    
    def test_connection(self, connection_name: str) -> Dict[str, Any]:
        """
        Test a specific connection and return detailed results
        
        Returns:
            Dict with test results including success status, timing, and error details
        """
        import time
        
        test_result = {
            'connection': connection_name,
            'success': False,
            'duration_seconds': 0,
            'error': None,
            'details': {}
        }
        
        if connection_name not in self.connections:
            test_result['error'] = f"Unknown connection: {connection_name}"
            return test_result
        
        config = self.connections[connection_name]
        start_time = time.time()
        
        try:
            if config.type == 'mysql':
                with self.get_mysql_connection(connection_name) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT VERSION(), CONNECTION_ID(), DATABASE()")
                    version, connection_id, database = cursor.fetchone()
                    test_result['details'] = {
                        'mysql_version': version,
                        'connection_id': connection_id,
                        'current_database': database
                    }
                    cursor.close()
            
            elif config.type == 'redshift':
                with self.get_redshift_connection(connection_name) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT version(), pg_backend_pid(), current_database(), current_schema()")
                        version, backend_pid, database, schema = cursor.fetchone()
                        test_result['details'] = {
                            'redshift_version': version,
                            'backend_pid': backend_pid,
                            'current_database': database,
                            'current_schema': schema
                        }
            
            test_result['success'] = True
            test_result['duration_seconds'] = round(time.time() - start_time, 2)
            
            logger.info(f"Connection test successful: {connection_name} ({test_result['duration_seconds']}s)")
            
        except ConnectionError as e:
            test_result['error'] = str(e)
            test_result['duration_seconds'] = round(time.time() - start_time, 2)
            logger.error(f"Connection test failed for {connection_name}: {e}")
        except mysql.connector.Error as e:
            test_result['error'] = f"MySQL error: {str(e)}"
            test_result['duration_seconds'] = round(time.time() - start_time, 2)
            logger.error(f"MySQL connection test failed for {connection_name}: {e}")
        except psycopg2.Error as e:
            test_result['error'] = f"PostgreSQL error: {str(e)}"
            test_result['duration_seconds'] = round(time.time() - start_time, 2)
            logger.error(f"PostgreSQL connection test failed for {connection_name}: {e}")
        except Exception as e:
            test_result['error'] = f"Unexpected error: {str(e)}"
            test_result['duration_seconds'] = round(time.time() - start_time, 2)
            logger.error(f"Connection test failed for {connection_name}: {e}")
        
        return test_result
    
    def test_all_connections(self) -> Dict[str, Dict[str, Any]]:
        """Test all configured connections"""
        results = {}
        for connection_name in self.connections.keys():
            results[connection_name] = self.test_connection(connection_name)
        
        # Summary statistics
        total_connections = len(results)
        successful_connections = sum(1 for r in results.values() if r['success'])
        
        logger.info(f"Connection tests completed: {successful_connections}/{total_connections} successful")
        
        return {
            'results': results,
            'summary': {
                'total_connections': total_connections,
                'successful_connections': successful_connections,
                'success_rate': round(successful_connections / total_connections * 100, 1) if total_connections > 0 else 0
            }
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status of connection registry"""
        return {
            'total_connections': len(self.connections),
            'mysql_pools_active': len(self.mysql_pools),
            'ssh_tunnels_active': len(self.ssh_tunnels),
            'connection_types': {
                'mysql': len([c for c in self.connections.values() if c.type == 'mysql']),
                'redshift': len([c for c in self.connections.values() if c.type == 'redshift'])
            },
            'configuration_file': str(self.config_path),
            'configuration_exists': self.config_path.exists(),
            'settings': {
                'default_timeout': self.default_timeout,
                'connection_retry_delay': self.connection_retry_delay,
                'pool_recycle_time': self.pool_recycle_time,
                'health_check_interval': self.health_check_interval
            }
        }
    
    def close_all_connections(self):
        """Close all connections, pools, and tunnels"""
        closed_count = 0

        # Close SSH tunnels gracefully
        for name, tunnel in list(self.ssh_tunnels.items()):
            try:
                if tunnel and tunnel.is_active:
                    logger.debug(f"Stopping SSH tunnel: {name}")
                    tunnel.stop(force=False)  # Graceful stop first
                    closed_count += 1
                    logger.debug(f"Closed SSH tunnel: {name}")
            except Exception as e:
                # Log but don't fail - we're shutting down anyway
                logger.debug(f"Error closing SSH tunnel {name}: {e}")
        self.ssh_tunnels.clear()
        
        # MySQL pools cleanup (connections auto-close when pool is deleted)
        mysql_pools_count = len(self.mysql_pools)
        self.mysql_pools.clear()
        closed_count += mysql_pools_count
        
        logger.info(f"Connection registry shutdown complete: {closed_count} connections/tunnels closed")
    
    def reload_configuration(self):
        """Reload connection configuration from file"""
        # Close existing connections
        self.close_all_connections()
        
        # Clear existing configuration
        self.connections.clear()
        
        # Reload from file
        self._load_connections()
        self._validate_configurations()
        
        logger.info(f"Configuration reloaded: {len(self.connections)} connections available")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup"""
        self.close_all_connections()

    def __del__(self):
        """Destructor to ensure SSH tunnels are closed on garbage collection"""
        try:
            # Only log if there are actually tunnels to close
            if hasattr(self, 'ssh_tunnels') and self.ssh_tunnels:
                logger.debug("ConnectionRegistry destructor called, closing tunnels")
                self.close_all_connections()
        except Exception:
            # Suppress errors during destruction to avoid issues at interpreter shutdown
            pass