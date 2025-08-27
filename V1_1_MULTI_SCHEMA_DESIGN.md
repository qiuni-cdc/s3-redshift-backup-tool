# Version 1.1.0 - Multi-Schema Foundation Design

**Release Target:** Q4 2025  
**Base Version:** v1.0.0 Production System  
**Strategic Goal:** Transform single-connection backup tool into multi-schema data integration platform

---

## 🎯 **Executive Summary**

Version 1.1.0 represents the first major evolution of our production-proven backup system. Building on the solid foundation of v1.0.0 (65M+ row processing capability), this version introduces **multi-schema support** while maintaining 100% backward compatibility with existing workflows.

**Key Innovation:** Configuration-driven connection management that allows the system to serve multiple databases and projects simultaneously without disrupting current operations.

---

## 📋 **Design Principles**

### **1. Backward Compatibility First**
- All v1.0.0 CLI commands continue working unchanged
- Existing workflows use "default" connection configuration
- Zero migration required for current production users

### **2. Progressive Enhancement**
- Add new capabilities without modifying core processing logic
- Configuration layer sits above existing reliable code
- Gradual adoption path for new features

### **3. Configuration Over Code**
- Business logic moves from hardcoded values to YAML configurations
- Connection details externalized from application code
- Table-specific behavior defined declaratively

### **4. Production Reliability**
- Preserve battle-tested error handling and recovery
- Maintain existing performance characteristics
- Extend proven watermark and S3 management systems

---

## 🏗️ **Architecture Overview**

### **Current v1.0.0 Architecture**
```
MySQL (hardcoded) → S3 → Redshift (hardcoded)
     ↓                        ↓
Single Connection      Single Target Schema
```

### **New v1.1.0 Architecture**
```
Multiple MySQL Sources → Connection Registry → S3 Data Lake → Multiple Redshift Targets
         ↓                        ↓                              ↓
Configuration-Driven    Pooled Connections      Schema-Isolated Storage
```

### **Core Components**

1. **ConnectionRegistry**: Manages multiple database connections with pooling
2. **ConfigurationManager**: Loads and validates YAML pipeline definitions  
3. **EnhancedCLI**: Supports both legacy and new multi-schema syntax
4. **SchemaIsolationManager**: Ensures data separation in S3 and Redshift
5. **MigrationTools**: Assists transition from v1.0.0 to v1.1.0

---

## 📂 **Configuration Architecture**

### **Configuration Hierarchy**
```
config/
├── connections.yml          # Database connection definitions
├── pipelines/              # Pipeline-specific configurations
│   ├── default.yml         # v1.0.0 compatibility pipeline
│   ├── sales_pipeline.yml  # Multi-schema pipeline example
│   └── analytics_pipeline.yml
├── tables/                 # Table-specific overrides
│   ├── customers.yml
│   └── orders.yml
└── environments/           # Environment-specific settings
    ├── development.yml
    ├── staging.yml
    └── production.yml
```

### **Connection Configuration**
```yaml
# config/connections.yml
connections:
  sources:
    # Default connection (v1.0.0 compatibility)
    default:
      type: "mysql"
      host: "${MYSQL_HOST}"
      port: 3306
      database: "${MYSQL_DATABASE}"
      username: "${MYSQL_USERNAME}" 
      password: "${MYSQL_PASSWORD}"
      description: "Legacy v1.0.0 connection"
    
    # New multi-schema connections
    sales_mysql:
      type: "mysql"
      host: "sales-db.company.com"
      port: 3306
      database: "sales_production"
      username: "${SALES_DB_USER}"
      password: "${SALES_DB_PASS}"
      connection_pool:
        min_connections: 2
        max_connections: 10
        retry_attempts: 3
      description: "Sales department MySQL database"
    
    inventory_mysql:
      type: "mysql" 
      host: "inventory-cluster.company.com"
      port: 3306
      database: "inventory_system"
      username: "${INVENTORY_DB_USER}"
      password: "${INVENTORY_DB_PASS}"
      connection_pool:
        min_connections: 1
        max_connections: 5
        retry_attempts: 2
      description: "Inventory management system"
  
  targets:
    # Default target (v1.0.0 compatibility)
    default:
      type: "redshift"
      host: "${REDSHIFT_HOST}"
      port: 5439
      database: "${REDSHIFT_DATABASE}"
      username: "${REDSHIFT_USERNAME}"
      password: "${REDSHIFT_PASSWORD}"
      schema: "public"
      ssh_tunnel:
        enabled: true
        host: "${SSH_HOST}"
        port: 22
        username: "${SSH_USER}"
        private_key_path: "${SSH_KEY_PATH}"
      description: "Legacy v1.0.0 Redshift target"
    
    # New multi-schema targets
    reporting_redshift:
      type: "redshift"
      host: "reporting-cluster.redshift.amazonaws.com"
      port: 5439
      database: "analytics"
      username: "${REPORTING_DB_USER}"
      password: "${REPORTING_DB_PASS}"
      schema: "reporting"
      ssh_tunnel:
        enabled: true
        host: "${BASTION_HOST}"
        port: 22
        username: "${BASTION_USER}"
        private_key_path: "${BASTION_KEY}"
      description: "Reporting and analytics Redshift cluster"
    
    staging_redshift:
      type: "redshift"
      host: "staging-cluster.redshift.amazonaws.com"
      port: 5439
      database: "staging"
      username: "${STAGING_DB_USER}"
      password: "${STAGING_DB_PASS}"
      schema: "raw_data"
      description: "Staging environment for development"

# Global connection settings
connection_settings:
  default_timeout: 30
  connection_retry_delay: 5
  pool_recycle_time: 3600
  health_check_interval: 300
```

### **Pipeline Configuration**
```yaml
# config/pipelines/sales_pipeline.yml
pipeline:
  name: "sales_to_reporting"
  version: "1.1.0"
  description: "Sales data integration to reporting warehouse"
  
  # Connection mapping
  source: "sales_mysql"
  target: "reporting_redshift"
  
  # Processing configuration
  processing:
    strategy: "sequential"  # or "parallel"
    batch_size: 10000
    max_parallel_tables: 3
    
  # S3 configuration
  s3:
    isolation_prefix: "sales_pipeline"  # Isolate from other pipelines
    partition_strategy: "hybrid"
    compression: "snappy"
  
  # Default table settings (can be overridden per table)
  default_table_config:
    backup_strategy: "sequential"
    watermark_strategy: "automatic"
    data_validation: true

# Table definitions
tables:
  customers:
    full_name: "customers"  # Table name in source database
    target_name: "dim_customers"  # Optional target table name
    cdc_strategy: "hybrid"  # Future: v1.2.0 feature
    cdc_timestamp_column: "updated_at"
    cdc_id_column: "customer_id"
    table_type: "dimension"  # Future: SCD support
    
    # v1.1.0 specific settings
    processing:
      batch_size: 5000
      priority: "high"
    
    validation:
      required_columns: ["customer_id", "customer_name"]
      max_null_percentage: 5.0
  
  orders:
    full_name: "orders"
    cdc_strategy: "hybrid"
    cdc_timestamp_column: "created_at"
    cdc_id_column: "order_id"
    table_type: "fact"
    
    processing:
      batch_size: 15000
      priority: "medium"
      
    validation:
      required_columns: ["order_id", "customer_id", "order_date"]
      max_null_percentage: 1.0

  order_items:
    full_name: "order_items"
    cdc_strategy: "hybrid"
    cdc_timestamp_column: "created_at"
    cdc_id_column: "item_id"
    table_type: "fact"
    
    # Dependencies (future feature)
    depends_on: ["orders"]  # Process after orders table
```

### **Default Pipeline (v1.0.0 Compatibility)**
```yaml
# config/pipelines/default.yml
pipeline:
  name: "default_legacy"
  version: "1.0.0"
  description: "Legacy v1.0.0 compatibility pipeline"
  
  source: "default"
  target: "default"
  
  # Use existing v1.0.0 behavior
  processing:
    strategy: "sequential"
    
  s3:
    isolation_prefix: ""  # No isolation (v1.0.0 behavior)
    partition_strategy: "datetime"
    
  # Minimal configuration for backward compatibility
  default_table_config:
    backup_strategy: "sequential"
    watermark_strategy: "automatic"
```

---

## 🔧 **Core Component Design**

### **1. Connection Registry System**

```python
# src/core/connection_registry.py

from typing import Dict, Any, Optional
from contextlib import contextmanager
import mysql.connector.pooling
import psycopg2.pool
from dataclasses import dataclass
import yaml

@dataclass
class ConnectionConfig:
    """Connection configuration with validation"""
    name: str
    type: str  # 'mysql' or 'redshift'
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: Optional[str] = None
    connection_pool: Optional[Dict[str, Any]] = None
    ssh_tunnel: Optional[Dict[str, Any]] = None
    description: Optional[str] = None

class ConnectionRegistry:
    """
    Manages multiple database connections with pooling and validation.
    
    Key Features:
    - Connection pooling for performance
    - SSH tunnel management for secure connections
    - Health monitoring and automatic recovery
    - Configuration-driven setup
    """
    
    def __init__(self, config_path: str = "config/connections.yml"):
        self.config_path = config_path
        self.connections: Dict[str, ConnectionConfig] = {}
        self.mysql_pools: Dict[str, mysql.connector.pooling.MySQLConnectionPool] = {}
        self.redshift_pools: Dict[str, psycopg2.pool.ThreadedConnectionPool] = {}
        self.ssh_tunnels: Dict[str, Any] = {}  # SSH tunnel instances
        
        # Load and validate configurations
        self._load_connections()
        self._validate_configurations()
        
        logger.info(f"Connection registry initialized with {len(self.connections)} connections")
    
    def _load_connections(self):
        """Load connection configurations from YAML"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Load source connections
            for name, conn_config in config.get('connections', {}).get('sources', {}).items():
                self.connections[name] = ConnectionConfig(
                    name=name,
                    type='mysql',
                    **conn_config
                )
            
            # Load target connections
            for name, conn_config in config.get('connections', {}).get('targets', {}).items():
                self.connections[name] = ConnectionConfig(
                    name=name,
                    type='redshift',
                    **conn_config
                )
                
            logger.info(f"Loaded {len(self.connections)} connection configurations")
            
        except Exception as e:
            logger.error(f"Failed to load connection configurations: {e}")
            raise ValidationError(f"Connection configuration error: {e}")
    
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
        # Required fields validation
        required_fields = ['host', 'port', 'database', 'username', 'password']
        for field in required_fields:
            if not getattr(config, field):
                raise ValidationError(f"Missing required field: {field}")
        
        # Type-specific validation
        if config.type == 'mysql':
            if config.port not in range(1, 65536):
                raise ValidationError(f"Invalid MySQL port: {config.port}")
        elif config.type == 'redshift':
            if config.port not in range(1, 65536):
                raise ValidationError(f"Invalid Redshift port: {config.port}")
            if not config.schema:
                config.schema = 'public'  # Default schema
        else:
            raise ValidationError(f"Unsupported connection type: {config.type}")
    
    def get_mysql_connection(self, connection_name: str = "default") -> mysql.connector.MySQLConnection:
        """
        Get MySQL connection with automatic pooling and retry logic
        
        Args:
            connection_name: Name of the connection configuration
            
        Returns:
            MySQL connection from pool
            
        Raises:
            ConnectionError: If connection cannot be established
            ValidationError: If connection configuration is invalid
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
            logger.debug(f"Retrieved MySQL connection for {connection_name}")
            return connection
        except mysql.connector.Error as e:
            logger.error(f"Failed to get MySQL connection for {connection_name}: {e}")
            raise BackupConnectionError(f"MySQL connection failed: {e}")
    
    def _create_mysql_pool(self, name: str, config: ConnectionConfig):
        """Create MySQL connection pool"""
        pool_config = config.connection_pool or {}
        
        pool_settings = {
            'pool_name': f"{name}_pool",
            'pool_size': pool_config.get('max_connections', 5),
            'pool_reset_session': True,
            'host': config.host,
            'port': config.port,
            'database': config.database,
            'user': config.username,
            'password': config.password,
            'charset': 'utf8mb4',
            'use_unicode': True,
            'autocommit': True,
            'connection_timeout': 30,
        }
        
        try:
            self.mysql_pools[name] = mysql.connector.pooling.MySQLConnectionPool(**pool_settings)
            logger.info(f"Created MySQL connection pool for {name} with {pool_settings['pool_size']} connections")
        except mysql.connector.Error as e:
            logger.error(f"Failed to create MySQL pool for {name}: {e}")
            raise BackupConnectionError(f"MySQL pool creation failed: {e}")
    
    @contextmanager
    def get_redshift_connection(self, connection_name: str = "default"):
        """
        Get Redshift connection with SSH tunnel support
        
        Args:
            connection_name: Name of the connection configuration
            
        Yields:
            Redshift connection (psycopg2)
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        if connection_name not in self.connections:
            raise ValidationError(f"Unknown connection: {connection_name}")
        
        config = self.connections[connection_name]
        if config.type != 'redshift':
            raise ValidationError(f"Connection {connection_name} is not a Redshift connection")
        
        ssh_tunnel = None
        local_port = config.port
        
        try:
            # Set up SSH tunnel if configured
            if config.ssh_tunnel and config.ssh_tunnel.get('enabled', False):
                ssh_tunnel = self._create_ssh_tunnel(connection_name, config)
                local_port = ssh_tunnel.local_bind_port
                logger.debug(f"SSH tunnel established for {connection_name} on port {local_port}")
            
            # Create Redshift connection
            connection_params = {
                'host': 'localhost' if ssh_tunnel else config.host,
                'port': local_port,
                'database': config.database,
                'user': config.username,
                'password': config.password,
                'sslmode': 'prefer',
                'connect_timeout': 30
            }
            
            connection = psycopg2.connect(**connection_params)
            logger.debug(f"Established Redshift connection for {connection_name}")
            
            yield connection
            
        except Exception as e:
            logger.error(f"Redshift connection failed for {connection_name}: {e}")
            raise BackupConnectionError(f"Redshift connection failed: {e}")
        
        finally:
            # Cleanup
            if 'connection' in locals():
                connection.close()
            if ssh_tunnel:
                ssh_tunnel.stop()
                logger.debug(f"SSH tunnel closed for {connection_name}")
    
    def _create_ssh_tunnel(self, name: str, config: ConnectionConfig):
        """Create SSH tunnel for Redshift connection"""
        from sshtunnel import SSHTunnelForwarder
        
        ssh_config = config.ssh_tunnel
        
        tunnel = SSHTunnelForwarder(
            (ssh_config['host'], ssh_config.get('port', 22)),
            ssh_username=ssh_config['username'],
            ssh_private_key=ssh_config.get('private_key_path'),
            remote_bind_address=(config.host, config.port),
            local_bind_address=('127.0.0.1', 0),  # Auto-assign local port
            logger=logger
        )
        
        tunnel.start()
        self.ssh_tunnels[name] = tunnel
        
        return tunnel
    
    def get_connection_info(self, connection_name: str) -> Dict[str, Any]:
        """Get connection information (without sensitive data)"""
        if connection_name not in self.connections:
            raise ValidationError(f"Unknown connection: {connection_name}")
        
        config = self.connections[connection_name]
        
        return {
            'name': config.name,
            'type': config.type,
            'host': config.host,
            'port': config.port,
            'database': config.database,
            'schema': config.schema,
            'description': config.description,
            'has_ssh_tunnel': bool(config.ssh_tunnel and config.ssh_tunnel.get('enabled', False)),
            'pool_settings': config.connection_pool
        }
    
    def list_connections(self) -> Dict[str, Dict[str, Any]]:
        """List all available connections"""
        return {
            name: self.get_connection_info(name)
            for name in self.connections.keys()
        }
    
    def test_connection(self, connection_name: str) -> bool:
        """Test a specific connection"""
        try:
            config = self.connections[connection_name]
            
            if config.type == 'mysql':
                conn = self.get_mysql_connection(connection_name)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                conn.close()
            
            elif config.type == 'redshift':
                with self.get_redshift_connection(connection_name) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
            
            logger.info(f"Connection test successful: {connection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed for {connection_name}: {e}")
            return False
    
    def close_all_connections(self):
        """Close all connections and pools"""
        # Close SSH tunnels
        for tunnel in self.ssh_tunnels.values():
            tunnel.stop()
        self.ssh_tunnels.clear()
        
        # Close MySQL pools
        for pool in self.mysql_pools.values():
            # MySQL connector pools don't have explicit close method
            # Connections will be closed when pool is garbage collected
            pass
        self.mysql_pools.clear()
        
        # Redshift connections are context-managed
        
        logger.info("All connections closed")
```

### **2. Configuration Manager**

```python
# src/core/configuration_manager.py

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from pathlib import Path
import yaml
from src.utils.exceptions import ValidationError
from src.utils.logging import get_logger

logger = get_logger(__name__)

@dataclass
class TableConfig:
    """Table-specific configuration"""
    full_name: str
    target_name: Optional[str] = None
    cdc_strategy: str = "hybrid"
    cdc_timestamp_column: str = "updated_at"
    cdc_id_column: str = "id"
    table_type: str = "fact"  # 'fact' or 'dimension'
    processing: Dict[str, Any] = field(default_factory=dict)
    validation: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)

@dataclass  
class PipelineConfig:
    """Pipeline configuration"""
    name: str
    version: str
    description: str
    source: str
    target: str
    processing: Dict[str, Any]
    s3: Dict[str, Any]
    default_table_config: Dict[str, Any]
    tables: Dict[str, TableConfig]

class ConfigurationManager:
    """
    Manages pipeline and table configurations with validation and inheritance
    
    Features:
    - YAML configuration loading and validation
    - Environment-specific overrides
    - Configuration inheritance and defaults
    - Hot-reload capability for development
    """
    
    def __init__(self, config_root: str = "config"):
        self.config_root = Path(config_root)
        self.pipelines: Dict[str, PipelineConfig] = {}
        self.environments: Dict[str, Dict[str, Any]] = {}
        self.current_environment = "production"
        
        # Load configurations
        self._load_environments()
        self._load_pipelines()
        
        logger.info(f"Configuration manager initialized with {len(self.pipelines)} pipelines")
    
    def _load_environments(self):
        """Load environment-specific configurations"""
        env_dir = self.config_root / "environments"
        if not env_dir.exists():
            logger.warning("No environments directory found, using defaults")
            return
        
        for env_file in env_dir.glob("*.yml"):
            env_name = env_file.stem
            try:
                with open(env_file, 'r') as f:
                    self.environments[env_name] = yaml.safe_load(f)
                logger.debug(f"Loaded environment configuration: {env_name}")
            except Exception as e:
                logger.error(f"Failed to load environment {env_name}: {e}")
    
    def _load_pipelines(self):
        """Load all pipeline configurations"""
        pipeline_dir = self.config_root / "pipelines"
        if not pipeline_dir.exists():
            raise ValidationError("Pipelines directory not found")
        
        for pipeline_file in pipeline_dir.glob("*.yml"):
            pipeline_name = pipeline_file.stem
            try:
                self._load_single_pipeline(pipeline_file, pipeline_name)
                logger.debug(f"Loaded pipeline configuration: {pipeline_name}")
            except Exception as e:
                logger.error(f"Failed to load pipeline {pipeline_name}: {e}")
                raise ValidationError(f"Pipeline configuration error: {e}")
    
    def _load_single_pipeline(self, pipeline_file: Path, pipeline_name: str):
        """Load and validate a single pipeline configuration"""
        with open(pipeline_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Apply environment overrides
        config_data = self._apply_environment_overrides(config_data)
        
        # Validate pipeline structure
        pipeline_section = config_data.get('pipeline', {})
        tables_section = config_data.get('tables', {})
        
        # Create table configurations
        table_configs = {}
        for table_name, table_data in tables_section.items():
            # Merge with default table config
            default_config = pipeline_section.get('default_table_config', {})
            merged_config = {**default_config, **table_data}
            
            table_configs[table_name] = TableConfig(
                full_name=merged_config.get('full_name', table_name),
                target_name=merged_config.get('target_name'),
                cdc_strategy=merged_config.get('cdc_strategy', 'hybrid'),
                cdc_timestamp_column=merged_config.get('cdc_timestamp_column', 'updated_at'),
                cdc_id_column=merged_config.get('cdc_id_column', 'id'),
                table_type=merged_config.get('table_type', 'fact'),
                processing=merged_config.get('processing', {}),
                validation=merged_config.get('validation', {}),
                depends_on=merged_config.get('depends_on', [])
            )
        
        # Create pipeline configuration
        self.pipelines[pipeline_name] = PipelineConfig(
            name=pipeline_section.get('name', pipeline_name),
            version=pipeline_section.get('version', '1.1.0'),
            description=pipeline_section.get('description', ''),
            source=pipeline_section.get('source', 'default'),
            target=pipeline_section.get('target', 'default'),
            processing=pipeline_section.get('processing', {}),
            s3=pipeline_section.get('s3', {}),
            default_table_config=pipeline_section.get('default_table_config', {}),
            tables=table_configs
        )
    
    def _apply_environment_overrides(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment-specific overrides"""
        if self.current_environment not in self.environments:
            return config_data
        
        env_overrides = self.environments[self.current_environment]
        
        # Simple deep merge for now (can be enhanced with more sophisticated merging)
        def merge_dicts(base: Dict, override: Dict) -> Dict:
            result = base.copy()
            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value)
                else:
                    result[key] = value
            return result
        
        return merge_dicts(config_data, env_overrides)
    
    def get_pipeline_config(self, pipeline_name: str) -> PipelineConfig:
        """Get pipeline configuration by name"""
        if pipeline_name not in self.pipelines:
            raise ValidationError(f"Unknown pipeline: {pipeline_name}")
        return self.pipelines[pipeline_name]
    
    def get_table_config(self, pipeline_name: str, table_name: str) -> TableConfig:
        """Get table configuration within a pipeline"""
        pipeline_config = self.get_pipeline_config(pipeline_name)
        if table_name not in pipeline_config.tables:
            raise ValidationError(f"Table {table_name} not found in pipeline {pipeline_name}")
        return pipeline_config.tables[table_name]
    
    def list_pipelines(self) -> List[str]:
        """List all available pipelines"""
        return list(self.pipelines.keys())
    
    def list_tables(self, pipeline_name: str) -> List[str]:
        """List all tables in a pipeline"""
        pipeline_config = self.get_pipeline_config(pipeline_name)
        return list(pipeline_config.tables.keys())
    
    def validate_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """Validate pipeline configuration and return validation report"""
        pipeline_config = self.get_pipeline_config(pipeline_name)
        validation_report = {
            'pipeline': pipeline_name,
            'valid': True,
            'errors': [],
            'warnings': [],
            'tables_validated': 0
        }
        
        try:
            # Validate pipeline-level configuration
            if not pipeline_config.source:
                validation_report['errors'].append("Missing source connection")
            if not pipeline_config.target:
                validation_report['errors'].append("Missing target connection")
            
            # Validate table configurations
            for table_name, table_config in pipeline_config.tables.items():
                try:
                    self._validate_table_config(table_config)
                    validation_report['tables_validated'] += 1
                except Exception as e:
                    validation_report['errors'].append(f"Table {table_name}: {e}")
            
            # Check for dependency cycles
            dependency_errors = self._check_dependency_cycles(pipeline_config)
            validation_report['errors'].extend(dependency_errors)
            
            validation_report['valid'] = len(validation_report['errors']) == 0
            
        except Exception as e:
            validation_report['valid'] = False
            validation_report['errors'].append(f"Pipeline validation failed: {e}")
        
        return validation_report
    
    def _validate_table_config(self, table_config: TableConfig):
        """Validate a single table configuration"""
        if not table_config.full_name:
            raise ValidationError("Missing table full_name")
        
        if table_config.cdc_strategy not in ['hybrid', 'timestamp_only', 'id_only', 'full_sync']:
            raise ValidationError(f"Invalid CDC strategy: {table_config.cdc_strategy}")
        
        if table_config.table_type not in ['fact', 'dimension']:
            raise ValidationError(f"Invalid table type: {table_config.table_type}")
        
        # Validate CDC column requirements
        if table_config.cdc_strategy in ['hybrid', 'timestamp_only']:
            if not table_config.cdc_timestamp_column:
                raise ValidationError("CDC timestamp column required for strategy")
        
        if table_config.cdc_strategy in ['hybrid', 'id_only']:
            if not table_config.cdc_id_column:
                raise ValidationError("CDC ID column required for strategy")
    
    def _check_dependency_cycles(self, pipeline_config: PipelineConfig) -> List[str]:
        """Check for circular dependencies in table processing"""
        errors = []
        
        # Build dependency graph
        graph = {}
        for table_name, table_config in pipeline_config.tables.items():
            graph[table_name] = table_config.depends_on
        
        # Check for cycles using DFS
        def has_cycle(node: str, visited: set, rec_stack: set) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in graph:
                    continue  # Skip non-existent dependencies
                if neighbor not in visited:
                    if has_cycle(neighbor, visited, rec_stack):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        visited = set()
        for table in graph:
            if table not in visited:
                if has_cycle(table, visited, set()):
                    errors.append(f"Circular dependency detected involving table: {table}")
        
        return errors
    
    def set_environment(self, environment: str):
        """Set current environment and reload configurations"""
        if environment not in self.environments:
            logger.warning(f"Unknown environment: {environment}, using defaults")
        
        self.current_environment = environment
        self._load_pipelines()  # Reload with new environment
        logger.info(f"Environment changed to: {environment}")
```

### **3. Enhanced CLI Design**

```python
# Enhanced src/cli/main.py sections

import click
from typing import Optional, List
from src.core.connection_registry import ConnectionRegistry
from src.core.configuration_manager import ConfigurationManager

# Initialize new components
connection_registry = ConnectionRegistry()
config_manager = ConfigurationManager()

@cli.group(invoke_without_command=True)
@click.pass_context
def sync(ctx):
    """Sync data between source and target with multi-schema support"""
    if ctx.invoked_subcommand is None:
        # Handle legacy v1.0.0 syntax without subcommands
        # This preserves backward compatibility
        pass

# New v1.1.0 multi-schema syntax
@sync.command(name="pipeline")
@click.option('--pipeline', '-p', required=True, help='Pipeline configuration name')
@click.option('--table', '-t', multiple=True, required=True, help='Table names to sync')
@click.option('--backup-only', is_flag=True, help='Only backup to S3 (MySQL → S3)')
@click.option('--redshift-only', is_flag=True, help='Only load to Redshift (S3 → Redshift)')
@click.option('--limit', type=int, help='Limit rows per query (for testing)')
@click.option('--dry-run', is_flag=True, help='Preview operations without execution')
@click.pass_context
def sync_pipeline(ctx, pipeline: str, table: List[str], backup_only: bool, redshift_only: bool, 
                 limit: Optional[int], dry_run: bool):
    """Sync tables using pipeline configuration (v1.1.0 syntax)"""
    
    try:
        # Load pipeline configuration
        pipeline_config = config_manager.get_pipeline_config(pipeline)
        
        # Validate pipeline
        validation_report = config_manager.validate_pipeline(pipeline)
        if not validation_report['valid']:
            click.echo(f"❌ Pipeline validation failed:")
            for error in validation_report['errors']:
                click.echo(f"  - {error}")
            raise click.Abort()
        
        # Get connections
        source_connection = pipeline_config.source
        target_connection = pipeline_config.target
        
        click.echo(f"🚀 Starting pipeline: {pipeline_config.name}")
        click.echo(f"📊 Source: {source_connection} → Target: {target_connection}")
        click.echo(f"📋 Tables: {', '.join(table)}")
        
        if dry_run:
            click.echo("🔍 DRY RUN - No actual data movement will occur")
        
        # Process each table
        for table_name in table:
            if table_name not in pipeline_config.tables:
                click.echo(f"⚠️  Warning: Table {table_name} not found in pipeline configuration")
                continue
            
            table_config = pipeline_config.tables[table_name]
            
            if dry_run:
                _preview_table_sync(pipeline_config, table_config, backup_only, redshift_only)
            else:
                _execute_table_sync(
                    pipeline_config, table_config, source_connection, target_connection,
                    backup_only, redshift_only, limit
                )
        
        click.echo("✅ Pipeline execution completed")
        
    except Exception as e:
        logger.error(f"Pipeline sync failed: {e}")
        click.echo(f"❌ Pipeline sync failed: {e}")
        raise click.Abort()

# Alternative v1.1.0 syntax for ad-hoc connections
@sync.command(name="connections") 
@click.option('--source', '-s', required=True, help='Source connection name')
@click.option('--target', '-r', required=True, help='Target connection name')
@click.option('--table', '-t', multiple=True, required=True, help='Table names to sync')
@click.option('--backup-only', is_flag=True, help='Only backup to S3')
@click.option('--redshift-only', is_flag=True, help='Only load to Redshift')
@click.option('--limit', type=int, help='Limit rows per query')
@click.pass_context
def sync_connections(ctx, source: str, target: str, table: List[str], backup_only: bool, 
                    redshift_only: bool, limit: Optional[int]):
    """Sync tables using explicit connections (v1.1.0 syntax)"""
    
    try:
        # Validate connections exist
        source_info = connection_registry.get_connection_info(source)
        target_info = connection_registry.get_connection_info(target)
        
        click.echo(f"🚀 Starting connection-based sync")
        click.echo(f"📊 Source: {source} ({source_info['database']}) → Target: {target} ({target_info['database']})")
        click.echo(f"📋 Tables: {', '.join(table)}")
        
        # Create ad-hoc pipeline configuration
        ad_hoc_pipeline = _create_adhoc_pipeline_config(source, target, list(table))
        
        # Process tables using default settings
        for table_name in table:
            default_table_config = TableConfig(
                full_name=table_name,
                cdc_strategy="hybrid",  # Default to most robust strategy
                cdc_timestamp_column="updated_at",
                cdc_id_column="id"
            )
            
            _execute_table_sync(
                ad_hoc_pipeline, default_table_config, source, target,
                backup_only, redshift_only, limit
            )
        
        click.echo("✅ Connection-based sync completed")
        
    except Exception as e:
        logger.error(f"Connection-based sync failed: {e}")
        click.echo(f"❌ Connection-based sync failed: {e}")
        raise click.Abort()

# Maintain v1.0.0 backward compatibility
@sync.command(name="legacy", hidden=True)  # Hidden but functional
@click.option('--table', '-t', multiple=True, required=True, help='Table names (v1.0.0 syntax)')
@click.option('--backup-only', is_flag=True, help='Only backup to S3')
@click.option('--redshift-only', is_flag=True, help='Only load to Redshift')
@click.option('--limit', type=int, help='Limit rows per query')
@click.pass_context
def sync_legacy(ctx, table: List[str], backup_only: bool, redshift_only: bool, limit: Optional[int]):
    """Legacy v1.0.0 sync command (backward compatibility)"""
    
    # Use default pipeline for v1.0.0 compatibility
    try:
        default_pipeline = config_manager.get_pipeline_config("default")
        
        click.echo("🔄 Using v1.0.0 compatibility mode")
        click.echo(f"📋 Tables: {', '.join(table)}")
        
        for table_name in table:
            # Create basic table config for v1.0.0 behavior
            legacy_table_config = TableConfig(
                full_name=table_name,
                cdc_strategy="timestamp_only",  # v1.0.0 behavior
                cdc_timestamp_column="updated_at"
            )
            
            _execute_table_sync(
                default_pipeline, legacy_table_config, "default", "default",
                backup_only, redshift_only, limit
            )
        
        click.echo("✅ Legacy sync completed")
        
    except Exception as e:
        logger.error(f"Legacy sync failed: {e}")
        click.echo(f"❌ Legacy sync failed: {e}")
        raise click.Abort()

# Detect and redirect v1.0.0 syntax automatically
@cli.command()
@click.option('--table', '-t', multiple=True, help='Table names')
@click.option('--backup-only', is_flag=True, help='Only backup to S3')
@click.option('--redshift-only', is_flag=True, help='Only load to Redshift')
@click.option('--limit', type=int, help='Limit rows per query')
@click.pass_context
def sync_auto_detect(ctx, table: List[str], backup_only: bool, redshift_only: bool, limit: Optional[int]):
    """Auto-detect sync syntax for backward compatibility"""
    
    if table:
        # This looks like v1.0.0 syntax, redirect to legacy handler
        click.echo("🔍 Detected v1.0.0 syntax, using compatibility mode...")
        ctx.invoke(sync_legacy, table=table, backup_only=backup_only, 
                  redshift_only=redshift_only, limit=limit)
    else:
        # Show help for new syntax
        click.echo("📚 v1.1.0 Multi-Schema Sync Options:")
        click.echo("")
        click.echo("Pipeline-based sync (recommended):")
        click.echo("  python -m src.cli.main sync pipeline --pipeline sales_pipeline --table customers")
        click.echo("")
        click.echo("Connection-based sync:")
        click.echo("  python -m src.cli.main sync connections --source sales_mysql --target reporting_redshift --table orders")
        click.echo("")
        click.echo("Legacy v1.0.0 syntax (still supported):")
        click.echo("  python -m src.cli.main sync legacy --table settlement.table_name")

# Configuration management commands
@cli.group()
def config():
    """Configuration management commands"""
    pass

@config.command()
def list_pipelines():
    """List available pipelines"""
    pipelines = config_manager.list_pipelines()
    click.echo("📋 Available Pipelines:")
    for pipeline in pipelines:
        pipeline_config = config_manager.get_pipeline_config(pipeline)
        click.echo(f"  • {pipeline}: {pipeline_config.description}")
        click.echo(f"    {pipeline_config.source} → {pipeline_config.target}")

@config.command()
@click.argument('pipeline')
def show_pipeline(pipeline: str):
    """Show pipeline configuration details"""
    try:
        pipeline_config = config_manager.get_pipeline_config(pipeline)
        
        click.echo(f"📊 Pipeline: {pipeline_config.name}")
        click.echo(f"Description: {pipeline_config.description}")
        click.echo(f"Version: {pipeline_config.version}")
        click.echo(f"Source: {pipeline_config.source}")
        click.echo(f"Target: {pipeline_config.target}")
        click.echo("")
        click.echo("Tables:")
        for table_name, table_config in pipeline_config.tables.items():
            click.echo(f"  • {table_name} ({table_config.table_type})")
            click.echo(f"    CDC: {table_config.cdc_strategy}")
            if table_config.depends_on:
                click.echo(f"    Depends on: {', '.join(table_config.depends_on)}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}")

@config.command()
@click.argument('pipeline')
def validate_pipeline(pipeline: str):
    """Validate pipeline configuration"""
    validation_report = config_manager.validate_pipeline(pipeline)
    
    if validation_report['valid']:
        click.echo(f"✅ Pipeline {pipeline} is valid")
        click.echo(f"📊 Tables validated: {validation_report['tables_validated']}")
    else:
        click.echo(f"❌ Pipeline {pipeline} has validation errors:")
        for error in validation_report['errors']:
            click.echo(f"  - {error}")
        
        if validation_report['warnings']:
            click.echo("⚠️  Warnings:")
            for warning in validation_report['warnings']:
                click.echo(f"  - {warning}")

# Connection management commands
@cli.group()
def connections():
    """Connection management commands"""
    pass

@connections.command()
def list():
    """List available connections"""
    all_connections = connection_registry.list_connections()
    
    click.echo("📊 Available Connections:")
    
    # Group by type
    sources = {k: v for k, v in all_connections.items() if v['type'] == 'mysql'}
    targets = {k: v for k, v in all_connections.items() if v['type'] == 'redshift'}
    
    if sources:
        click.echo("\n🗄️  Sources (MySQL):")
        for name, info in sources.items():
            click.echo(f"  • {name}: {info['host']}:{info['port']}/{info['database']}")
            if info['description']:
                click.echo(f"    {info['description']}")
    
    if targets:
        click.echo("\n🎯 Targets (Redshift):")
        for name, info in targets.items():
            tunnel_status = "🔒 SSH" if info['has_ssh_tunnel'] else "🌐 Direct"
            click.echo(f"  • {name}: {info['host']}:{info['port']}/{info['database']} ({tunnel_status})")
            if info['description']:
                click.echo(f"    {info['description']}")

@connections.command()
@click.argument('connection_name')
def test(connection_name: str):
    """Test a specific connection"""
    click.echo(f"🔍 Testing connection: {connection_name}")
    
    try:
        success = connection_registry.test_connection(connection_name)
        if success:
            click.echo(f"✅ Connection {connection_name} is working")
        else:
            click.echo(f"❌ Connection {connection_name} failed")
    except Exception as e:
        click.echo(f"❌ Connection test failed: {e}")

@connections.command()
@click.argument('connection_name')
def info(connection_name: str):
    """Show detailed connection information"""
    try:
        info = connection_registry.get_connection_info(connection_name)
        
        click.echo(f"📊 Connection: {info['name']}")
        click.echo(f"Type: {info['type'].upper()}")
        click.echo(f"Host: {info['host']}:{info['port']}")
        click.echo(f"Database: {info['database']}")
        if info['schema']:
            click.echo(f"Schema: {info['schema']}")
        if info['description']:
            click.echo(f"Description: {info['description']}")
        
        if info['has_ssh_tunnel']:
            click.echo("🔒 SSH Tunnel: Enabled")
        
        if info['pool_settings']:
            click.echo("🏊 Connection Pool:")
            for key, value in info['pool_settings'].items():
                click.echo(f"  {key}: {value}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}")

# Helper functions
def _preview_table_sync(pipeline_config, table_config, backup_only: bool, redshift_only: bool):
    """Preview table sync operations without execution"""
    click.echo(f"🔍 Preview: {table_config.full_name}")
    click.echo(f"  Strategy: {table_config.cdc_strategy}")
    click.echo(f"  Type: {table_config.table_type}")
    
    if not redshift_only:
        click.echo(f"  📤 MySQL → S3: {pipeline_config.source}")
    
    if not backup_only:
        click.echo(f"  📥 S3 → Redshift: {pipeline_config.target}")
    
    if table_config.depends_on:
        click.echo(f"  📋 Dependencies: {', '.join(table_config.depends_on)}")

def _execute_table_sync(pipeline_config, table_config, source_connection: str, target_connection: str,
                       backup_only: bool, redshift_only: bool, limit: Optional[int]):
    """Execute actual table sync with new configuration support"""
    
    # This is where we integrate with existing backup strategies
    # but pass the new configuration parameters
    
    # Create backup strategy with multi-schema support
    if pipeline_config.processing.get('strategy') == 'parallel':
        # Use inter-table strategy with configuration
        strategy = InterTableBackupStrategy(
            config=ctx.obj['config'],
            connection_registry=connection_registry,  # New parameter
            pipeline_config=pipeline_config,  # New parameter
        )
    else:
        # Use sequential strategy with configuration
        strategy = RowBasedBackupStrategy(
            config=ctx.obj['config'], 
            connection_registry=connection_registry,  # New parameter
            pipeline_config=pipeline_config,  # New parameter
        )
    
    # Execute with table-specific configuration
    strategy.execute_table(
        table_config=table_config,
        source_connection=source_connection,
        target_connection=target_connection,
        backup_only=backup_only,
        redshift_only=redshift_only,
        limit=limit
    )

def _create_adhoc_pipeline_config(source: str, target: str, tables: List[str]):
    """Create ad-hoc pipeline configuration for connection-based sync"""
    from src.core.configuration_manager import PipelineConfig, TableConfig
    
    # Create basic table configs
    table_configs = {}
    for table_name in tables:
        table_configs[table_name] = TableConfig(
            full_name=table_name,
            cdc_strategy="hybrid",
            cdc_timestamp_column="updated_at",
            cdc_id_column="id"
        )
    
    return PipelineConfig(
        name=f"adhoc_{source}_to_{target}",
        version="1.1.0",
        description=f"Ad-hoc pipeline from {source} to {target}",
        source=source,
        target=target,
        processing={'strategy': 'sequential'},
        s3={'isolation_prefix': f"adhoc_{source}_{target}", 'partition_strategy': 'hybrid'},
        default_table_config={},
        tables=table_configs
    )
```

---

## 🔄 **Backward Compatibility Strategy**

### **Automatic Detection and Redirection**

The system will automatically detect v1.0.0 syntax and redirect to compatibility mode:

```bash
# v1.0.0 syntax (still works unchanged)
python -m src.cli.main sync -t settlement.table_name
# → Automatically uses "default" pipeline configuration

# New v1.1.0 syntax options
python -m src.cli.main sync pipeline -p sales_pipeline -t customers
python -m src.cli.main sync connections -s sales_mysql -r reporting_redshift -t orders
```

### **Configuration Migration**

Existing hardcoded settings automatically mapped to default configuration:

```yaml
# config/pipelines/default.yml (automatically created)
pipeline:
  name: "default_legacy"
  version: "1.0.0"
  source: "default"  # Maps to existing .env settings
  target: "default"  # Maps to existing .env settings
  
  processing:
    strategy: "sequential"  # Preserve v1.0.0 behavior
    
  s3:
    isolation_prefix: ""  # No isolation for backward compatibility
    partition_strategy: "datetime"  # v1.0.0 partitioning
```

### **Environment Variable Mapping**

```yaml
# config/connections.yml
connections:
  sources:
    default:  # v1.0.0 compatibility connection
      type: "mysql"
      host: "${MYSQL_HOST}"          # Existing variable
      database: "${MYSQL_DATABASE}"   # Existing variable
      username: "${MYSQL_USERNAME}"   # Existing variable
      password: "${MYSQL_PASSWORD}"   # Existing variable

  targets:
    default:  # v1.0.0 compatibility connection  
      type: "redshift"
      host: "${REDSHIFT_HOST}"        # Existing variable
      database: "${REDSHIFT_DATABASE}" # Existing variable
      username: "${REDSHIFT_USERNAME}" # Existing variable
      password: "${REDSHIFT_PASSWORD}" # Existing variable
      ssh_tunnel:
        enabled: true
        host: "${SSH_HOST}"           # Existing variable
        username: "${SSH_USER}"      # Existing variable
        private_key_path: "${SSH_KEY_PATH}" # Existing variable
```

---

## 🧪 **Migration and Testing Strategy**

### **Phase 1: Safe Deployment (Week 1-2)**

1. **Deploy Configuration Layer**: Add new components without changing existing code
2. **Default Configuration**: Auto-generate default pipeline from existing settings
3. **Compatibility Testing**: Verify all v1.0.0 commands work unchanged
4. **Production Validation**: Test with actual production workloads

### **Phase 2: Multi-Schema Introduction (Week 3-4)**

1. **Create New Pipelines**: Add sales_pipeline and analytics_pipeline configurations
2. **Connection Testing**: Validate new database connections
3. **Isolated Testing**: Test new syntax with development data
4. **Documentation**: Update user guides with new capabilities

### **Phase 3: Production Rollout (Week 5-6)**

1. **Gradual Adoption**: Migrate select tables to new pipelines
2. **Parallel Operation**: Run both v1.0.0 and v1.1.0 approaches simultaneously
3. **Performance Validation**: Ensure no degradation in processing speed
4. **User Training**: Educate users on new capabilities

### **Comprehensive Test Plan**

```bash
# Test v1.0.0 compatibility
python -m src.cli.main sync -t settlement.table_name
python -m src.cli.main watermark get -t settlement.table_name
python -m src.cli.main s3clean list -t settlement.table_name

# Test v1.1.0 pipeline syntax
python -m src.cli.main sync pipeline -p sales_pipeline -t customers
python -m src.cli.main config validate-pipeline sales_pipeline

# Test v1.1.0 connection syntax  
python -m src.cli.main sync connections -s sales_mysql -r reporting_redshift -t orders
python -m src.cli.main connections test sales_mysql

# Test configuration management
python -m src.cli.main config list-pipelines
python -m src.cli.main connections list
```

### **Success Criteria**

- ✅ **100% v1.0.0 Compatibility**: All existing commands work unchanged
- ✅ **5+ Database Support**: Successfully manage multiple connections
- ✅ **Configuration Validation**: All YAML configurations validate correctly
- ✅ **Performance Maintenance**: No degradation in processing speed
- ✅ **Production Stability**: Zero issues with existing production workflows

---

## 📊 **Implementation Timeline**

### **Month 1: Foundation (Weeks 1-4)**

- **Week 1**: ConnectionRegistry implementation and testing
- **Week 2**: ConfigurationManager development and validation
- **Week 3**: Enhanced CLI design and backward compatibility
- **Week 4**: Integration testing and bug fixes

### **Month 2: Deployment (Weeks 5-8)**

- **Week 5**: Configuration migration tools and documentation
- **Week 6**: Production deployment with v1.0.0 compatibility validation
- **Week 7**: Multi-schema pipeline creation and testing
- **Week 8**: User training and documentation completion

### **Success Metrics**

**Technical Achievements:**
- Support 5+ simultaneous database connections
- Process multiple pipelines without interference
- Maintain 100% backward compatibility
- Zero production disruption during transition

**Business Benefits:**
- Enable multi-project data integration
- Reduce configuration complexity through YAML
- Provide foundation for v1.2.0 CDC enhancements
- Demonstrate enterprise scalability

---

## 🚀 **Future Integration Points**

### **v1.2.0 CDC Engine Integration**
```yaml
# Future configuration enhancement
tables:
  customers:
    cdc_strategy: "hybrid"  # Already supported structure
    cdc_timestamp_column: "updated_at"
    cdc_id_column: "customer_id"
    # v1.2.0 will add:
    # data_quality_checks: ["null_check", "duplicate_check"]
    # custom_sql: "SELECT * FROM customers WHERE active = 1"
```

### **v1.3.0 SCD Support Integration**  
```yaml
# Future dimensional table configuration
tables:
  dim_customers:
    table_type: "dimension"  # Already supported
    # v1.3.0 will add:
    # scd_type: "type_2" 
    # business_key: ["customer_id"]
    # scd_columns: ["name", "address", "tier"]
```

### **v2.0.0 Enterprise Features**
- Pipeline orchestration using dependency graphs
- Enterprise monitoring integration
- Advanced data lineage tracking
- Multi-tenant isolation and security

---

## 📚 **Documentation and Training**

### **User Migration Guide**
- Step-by-step transition from v1.0.0 to v1.1.0
- Configuration examples for common scenarios
- Troubleshooting guide for migration issues

### **Administrator Guide** 
- Connection management best practices
- Pipeline design patterns
- Security configuration recommendations
- Performance tuning guidelines

### **Developer Guide**
- Extension points for custom functionality
- Configuration schema reference
- API documentation for new components
- Testing framework for custom configurations

---

## 🎯 **Conclusion**

Version 1.1.0 transforms our production-proven backup system into a flexible, multi-schema data integration platform while preserving the reliability that made v1.0.0 successful. This design provides:

**Immediate Benefits:**
- Multi-database support without disrupting existing workflows
- Configuration-driven approach reducing hardcoded dependencies
- Foundation for advanced features in subsequent versions

**Strategic Value:**
- Positions system as enterprise data integration platform
- Enables organization-wide adoption across multiple projects
- Provides scalable architecture for future enhancements

**Risk Mitigation:**
- 100% backward compatibility ensures zero disruption
- Gradual adoption path allows careful transition
- Comprehensive testing validates production readiness

**Next Steps:**
1. Stakeholder review and approval
2. Development team resource allocation  
3. Implementation sprint planning
4. Production deployment scheduling

This design successfully bridges the gap between our specialized v1.0.0 backup tool and the comprehensive enterprise platform envisioned for v2.0.0, while maintaining the operational excellence that makes our system production-ready.