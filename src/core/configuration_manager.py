"""
Configuration Management System for Multi-Schema Support (v1.1.0)

Manages pipeline and table configurations with validation, inheritance, and environment support.
Provides declarative configuration for complex data integration workflows.
"""

import os
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
import yaml
import re

from src.utils.exceptions import ValidationError
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class TableConfig:
    """Table-specific configuration with validation and defaults"""
    full_name: str
    target_name: Optional[str] = None
    cdc_strategy: str = "hybrid"
    cdc_timestamp_column: str = "updated_at" 
    cdc_id_column: str = "id"
    table_type: str = "fact"  # 'fact' or 'dimension'
    full_sync_mode: str = "append"  # 'replace', 'paginate', 'append' (for full_sync strategy)
    processing: Dict[str, Any] = field(default_factory=dict)
    validation: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    additional_where: Optional[str] = None  # NEW: Additional WHERE clause for index optimization
    cdc_ordering: Optional[List[str]] = None  # NEW: Custom ordering columns for index optimization
    
    def __post_init__(self):
        """Post-initialization validation and defaults"""
        # Set target_name default - strip scope and schema prefix for Redshift compatibility
        if not self.target_name:
            # Strip scope prefix (e.g., "US_DW_UNIDW_SSH:unidw.dw_parcel_pricing_temp" -> "unidw.dw_parcel_pricing_temp")
            table_name = self.full_name
            if ':' in table_name:
                _, table_name = table_name.split(':', 1)

            # Strip schema prefix (e.g., "unidw.dw_parcel_pricing_temp" -> "dw_parcel_pricing_temp")
            if '.' in table_name:
                _, table_name = table_name.rsplit('.', 1)

            self.target_name = table_name
        
        # Validate CDC strategy
        valid_strategies = ['hybrid', 'timestamp_only', 'id_only', 'full_sync', 'custom_sql']
        if self.cdc_strategy not in valid_strategies:
            raise ValidationError(f"Invalid CDC strategy: {self.cdc_strategy}")
        
        # Validate table type
        valid_types = ['fact', 'dimension']
        if self.table_type not in valid_types:
            raise ValidationError(f"Invalid table type: {self.table_type}")
        
        # Set processing defaults
        if not self.processing:
            self.processing = {
                'batch_size': 10000,
                'priority': 'medium',
                'timeout_minutes': 60
            }
        
        # Set validation defaults
        if not self.validation:
            self.validation = {
                'required_columns': [],
                'max_null_percentage': 10.0,
                'enable_data_quality_checks': True
            }


@dataclass
class PipelineConfig:
    """Pipeline configuration with comprehensive settings"""
    name: str
    version: str
    description: str
    source: str
    target: str
    processing: Dict[str, Any]
    s3: Dict[str, Any]
    default_table_config: Dict[str, Any]
    tables: Dict[str, TableConfig]
    s3_config: Optional[str] = None  # Reference to named S3 config in connections.yml
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-initialization setup and validation"""
        # Set metadata defaults
        if not self.metadata:
            self.metadata = {
                'created_at': datetime.now().isoformat(),
                'created_by': 'configuration_manager',
                'tags': []
            }
        
        # Set processing defaults
        default_processing = {
            'strategy': 'sequential',
            'batch_size': 10000,
            'max_parallel_tables': 3,
            'timeout_minutes': 120
        }
        self.processing = {**default_processing, **self.processing}
        
        # Set S3 defaults
        default_s3 = {
            'isolation_prefix': self.name.replace(' ', '_').lower(),
            'partition_strategy': 'hybrid',
            'compression': 'snappy'
        }
        self.s3 = {**default_s3, **self.s3}


class ConfigurationManager:
    """
    Manages pipeline and table configurations with advanced features
    
    Features:
    - YAML configuration loading with validation
    - Environment-specific overrides and variable interpolation
    - Configuration inheritance and templating
    - Hot-reload capability for development
    - Dependency validation and resolution
    - Configuration versioning and migration
    """
    
    def __init__(self, config_root: str = "config"):
        """
        Initialize configuration manager

        Args:
            config_root: Root directory for configuration files
        """
        self.config_root = Path(config_root)
        self.pipelines: Dict[str, PipelineConfig] = {}
        self.environments: Dict[str, Dict[str, Any]] = {}
        self.templates: Dict[str, Dict[str, Any]] = {}
        self.current_environment = self._detect_environment()

        # Configuration cache and metadata
        self._config_cache: Dict[str, Any] = {}
        self._last_reload = None
        self._file_timestamps: Dict[Path, float] = {}

        # NEW: Load connections.yml for system-wide settings
        self.connections_config: Dict[str, Any] = {}

        # CRITICAL: Load .env file BEFORE interpolating variables in YAML files
        self._load_dotenv()

        # Initialize configuration structure
        self._ensure_config_structure()
        self._load_connections()  # NEW: Load connections first
        self._load_environments()
        self._load_templates()
        self._load_pipelines()

        logger.info(
            f"Configuration manager initialized: "
            f"{len(self.pipelines)} pipelines, "
            f"{len(self.environments)} environments, "
            f"current environment: {self.current_environment}"
        )
    
    def _load_dotenv(self):
        """Load .env file to populate environment variables for YAML interpolation"""
        try:
            from dotenv import load_dotenv

            # Try to find .env file in project root (parent of config_root)
            env_file = Path.cwd() / '.env'

            if env_file.exists():
                load_dotenv(env_file, override=False)  # Don't override existing env vars
                logger.debug(f"Loaded .env file from: {env_file}")
            else:
                logger.warning(f"No .env file found at: {env_file}")

        except ImportError:
            logger.warning("python-dotenv not installed, skipping .env loading")
        except Exception as e:
            logger.warning(f"Failed to load .env file: {e}")

    def _detect_environment(self) -> str:
        """Detect current environment from various sources"""
        # Check environment variable
        env = os.getenv('BACKUP_ENVIRONMENT', '').lower()
        if env in ['development', 'dev', 'staging', 'stage', 'production', 'prod']:
            return 'development' if env in ['dev'] else 'staging' if env in ['stage'] else 'production' if env in ['prod'] else env

        # Check for common development indicators
        if os.getenv('DEBUG') == '1' or os.getenv('DEV') == '1':
            return 'development'

        # Default to production for safety
        return 'production'
    
    def _ensure_config_structure(self):
        """Ensure configuration directory structure exists"""
        directories = [
            self.config_root,
            self.config_root / "pipelines",
            self.config_root / "environments",
            self.config_root / "templates",
            self.config_root / "tables"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Create default files if they don't exist
        self._create_default_files()
    
    def _create_default_files(self):
        """Create default configuration files for v1.0.0 compatibility"""
        
        # Default pipeline for v1.0.0 compatibility
        default_pipeline_path = self.config_root / "pipelines" / "default.yml"
        if not default_pipeline_path.exists():
            default_pipeline = {
                'pipeline': {
                    'name': 'default_legacy',
                    'version': '1.0.0',
                    'description': 'Default pipeline for v1.0.0 compatibility',
                    'source': 'default',
                    'target': 'default',
                    'processing': {
                        'strategy': 'sequential',
                        'batch_size': 10000
                    },
                    's3': {
                        'isolation_prefix': '',  # No isolation for backward compatibility
                        'partition_strategy': 'datetime',
                        'compression': 'snappy'
                    },
                    'default_table_config': {
                        'backup_strategy': 'sequential',
                        'watermark_strategy': 'automatic'
                    }
                },
                'tables': {
                    # Tables will be dynamically added based on CLI usage
                }
            }
            
            with open(default_pipeline_path, 'w') as f:
                yaml.dump(default_pipeline, f, default_flow_style=False, indent=2)
            
            logger.info("Created default pipeline configuration for v1.0.0 compatibility")
        
        # Production environment defaults
        prod_env_path = self.config_root / "environments" / "production.yml"
        if not prod_env_path.exists():
            prod_env = {
                'pipeline': {
                    'processing': {
                        'max_parallel_tables': 5,
                        'timeout_minutes': 240
                    },
                    'default_table_config': {
                        'validation': {
                            'enable_data_quality_checks': True,
                            'max_null_percentage': 5.0
                        }
                    }
                }
            }
            
            with open(prod_env_path, 'w') as f:
                yaml.dump(prod_env, f, default_flow_style=False, indent=2)
            
            logger.info("Created production environment configuration")
        
        # Development environment with relaxed settings
        dev_env_path = self.config_root / "environments" / "development.yml"
        if not dev_env_path.exists():
            dev_env = {
                'pipeline': {
                    'processing': {
                        'max_parallel_tables': 2,
                        'timeout_minutes': 30
                    },
                    'default_table_config': {
                        'validation': {
                            'enable_data_quality_checks': False,
                            'max_null_percentage': 20.0
                        },
                        'processing': {
                            'batch_size': 1000  # Smaller batches for development
                        }
                    }
                }
            }
            
            with open(dev_env_path, 'w') as f:
                yaml.dump(dev_env, f, default_flow_style=False, indent=2)
            
            logger.info("Created development environment configuration")

    def _load_connections(self):
        """Load connections.yml with environment variable substitution"""
        connections_file = self.config_root / "connections.yml"

        if not connections_file.exists():
            logger.warning("connections.yml not found, some features may be limited")
            return

        try:
            with open(connections_file, 'r') as f:
                config_data = yaml.safe_load(f) or {}

            # Interpolate environment variables
            self.connections_config = self._interpolate_environment_variables(config_data)
            self._file_timestamps[connections_file] = connections_file.stat().st_mtime

            logger.info(
                f"Loaded connections configuration: "
                f"{len(self.connections_config.get('connections', {}).get('sources', {}))} sources, "
                f"{len(self.connections_config.get('connections', {}).get('targets', {}))} targets"
            )

        except Exception as e:
            logger.error(f"Failed to load connections.yml: {e}")
            raise ValidationError(f"Failed to load connections configuration: {e}")

    def _load_environments(self):
        """Load environment-specific configurations"""
        env_dir = self.config_root / "environments"
        if not env_dir.exists():
            logger.warning("No environments directory found")
            return
        
        for env_file in env_dir.glob("*.yml"):
            env_name = env_file.stem
            try:
                with open(env_file, 'r') as f:
                    env_config = yaml.safe_load(f) or {}
                
                # Interpolate environment variables
                self.environments[env_name] = self._interpolate_environment_variables(env_config)
                self._file_timestamps[env_file] = env_file.stat().st_mtime
                
                logger.debug(f"Loaded environment configuration: {env_name}")
                
            except Exception as e:
                logger.error(f"Failed to load environment {env_name}: {e}")
    
    def _load_templates(self):
        """Load configuration templates for reuse"""
        template_dir = self.config_root / "templates"
        if not template_dir.exists():
            return
        
        for template_file in template_dir.glob("*.yml"):
            template_name = template_file.stem
            try:
                with open(template_file, 'r') as f:
                    template_config = yaml.safe_load(f) or {}
                
                self.templates[template_name] = template_config
                self._file_timestamps[template_file] = template_file.stat().st_mtime
                
                logger.debug(f"Loaded configuration template: {template_name}")
                
            except Exception as e:
                logger.error(f"Failed to load template {template_name}: {e}")
    
    def _load_pipelines(self):
        """Load all pipeline configurations with full processing"""
        pipeline_dir = self.config_root / "pipelines"
        if not pipeline_dir.exists():
            raise ValidationError("Pipelines directory not found")
        
        self.pipelines.clear()
        
        for pipeline_file in pipeline_dir.glob("*.yml"):
            pipeline_name = pipeline_file.stem
            try:
                self._load_single_pipeline(pipeline_file, pipeline_name)
                self._file_timestamps[pipeline_file] = pipeline_file.stat().st_mtime
                logger.debug(f"Loaded pipeline configuration: {pipeline_name}")
                
            except Exception as e:
                logger.error(f"Failed to load pipeline {pipeline_name}: {e}")
                # Continue loading other pipelines rather than failing completely
        
        if not self.pipelines:
            logger.warning("No valid pipeline configurations found")
        else:
            logger.info(f"Successfully loaded {len(self.pipelines)} pipeline configurations")
    
    def _load_single_pipeline(self, pipeline_file: Path, pipeline_name: str):
        """Load and process a single pipeline configuration"""
        with open(pipeline_file, 'r') as f:
            config_data = yaml.safe_load(f) or {}
        
        # Apply template inheritance
        config_data = self._apply_template_inheritance(config_data)
        
        # Apply environment overrides
        config_data = self._apply_environment_overrides(config_data)
        
        # Interpolate environment variables
        config_data = self._interpolate_environment_variables(config_data)
        
        # Validate structure
        if 'pipeline' not in config_data:
            raise ValidationError(f"Missing 'pipeline' section in {pipeline_name}")
        
        pipeline_section = config_data['pipeline']
        tables_section = config_data.get('tables', {})
        
        # Process table configurations
        table_configs = self._process_table_configurations(
            tables_section, 
            pipeline_section.get('default_table_config', {})
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
            tables=table_configs,
            s3_config=pipeline_section.get('s3_config'),  # Read S3 config from pipeline YAML
            metadata=pipeline_section.get('metadata', {})
        )
    
    def _apply_template_inheritance(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply template inheritance with cycle detection"""
        if 'extends' not in config_data:
            return config_data
        
        extends = config_data.pop('extends')
        if isinstance(extends, str):
            extends = [extends]
        
        # Security: Check for circular dependencies
        self._validate_template_inheritance_chain(extends, set())
        
        # Apply templates in order with depth limit
        result = {}
        for template_name in extends:
            if template_name in self.templates:
                # Recursively process template inheritance with cycle detection
                template_config = self.templates[template_name].copy()
                processed_template = self._apply_template_inheritance_safe(template_config, {template_name}, 0)
                result = self._deep_merge_dicts(result, processed_template)
            else:
                logger.warning(f"Template not found: {template_name}")
        
        # Apply the specific configuration on top
        result = self._deep_merge_dicts(result, config_data)
        
        return result
    
    def _apply_template_inheritance_safe(self, config_data: Dict[str, Any], visited: Set[str], depth: int) -> Dict[str, Any]:
        """Apply template inheritance with cycle and depth protection"""
        MAX_INHERITANCE_DEPTH = 10  # Prevent deep inheritance chains
        
        if depth > MAX_INHERITANCE_DEPTH:
            raise ValidationError(f"Template inheritance depth exceeded {MAX_INHERITANCE_DEPTH} levels")
        
        if 'extends' not in config_data:
            return config_data
        
        extends = config_data.pop('extends')
        if isinstance(extends, str):
            extends = [extends]
        
        result = {}
        for template_name in extends:
            # Cycle detection
            if template_name in visited:
                raise ValidationError(f"Circular template inheritance detected: {' -> '.join(visited)} -> {template_name}")
            
            if template_name in self.templates:
                new_visited = visited.copy()
                new_visited.add(template_name)
                
                template_config = self.templates[template_name].copy()
                processed_template = self._apply_template_inheritance_safe(template_config, new_visited, depth + 1)
                result = self._deep_merge_dicts(result, processed_template)
            else:
                logger.warning(f"Template not found: {template_name}")
        
        # Apply the specific configuration on top
        result = self._deep_merge_dicts(result, config_data)
        return result
    
    def _validate_template_inheritance_chain(self, extends: List[str], visited: Set[str]):
        """Validate template inheritance chain for cycles"""
        for template_name in extends:
            if template_name in visited:
                chain = ' -> '.join(visited) + f' -> {template_name}'
                raise ValidationError(f"Circular template inheritance detected: {chain}")
            
            if template_name in self.templates:
                template_config = self.templates[template_name]
                if 'extends' in template_config:
                    template_extends = template_config['extends']
                    if isinstance(template_extends, str):
                        template_extends = [template_extends]
                    
                    new_visited = visited.copy()
                    new_visited.add(template_name)
                    self._validate_template_inheritance_chain(template_extends, new_visited)
    
    def _apply_environment_overrides(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment-specific overrides"""
        if self.current_environment not in self.environments:
            return config_data
        
        env_overrides = self.environments[self.current_environment]
        return self._deep_merge_dicts(config_data, env_overrides)
    
    def _deep_merge_dicts(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge dictionaries with override priority"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge_dicts(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _interpolate_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively interpolate environment variables in configuration with security controls"""
        
        # Security: Define allowlist of safe environment variables
        ALLOWED_ENV_VARS = {
            # Database connections
            'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_DATABASE', 'MYSQL_USERNAME', 'MYSQL_PASSWORD',
            'REDSHIFT_HOST', 'REDSHIFT_PORT', 'REDSHIFT_DATABASE', 'REDSHIFT_USERNAME', 'REDSHIFT_PASSWORD',
            # SSH tunnel settings
            'SSH_HOST', 'SSH_PORT', 'SSH_USER', 'SSH_USERNAME', 'SSH_KEY_PATH', 'SSH_PASSWORD',
            # S3 configuration
            'S3_BUCKET', 'S3_PREFIX', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION',
            # General settings
            'BACKUP_ENVIRONMENT', 'DEBUG', 'DEV', 'ENVIRONMENT',
        }

        def is_safe_env_var(var_name: str) -> bool:
            """Check if environment variable is safe to interpolate"""
            # Direct allowlist match
            if var_name in ALLOWED_ENV_VARS:
                return True

            # Check safe prefixes (infrastructure and connection variables)
            safe_prefixes = [
                'BACKUP_', 'CONFIG_', 'DB_', 'DB2_', 'CONN_',
                'SSH_', 'REDSHIFT_', 'MYSQL_', 'AWS_', 'S3_'
            ]
            for prefix in safe_prefixes:
                if var_name.startswith(prefix):
                    return True

            return False
        
        def interpolate_value(value):
            if isinstance(value, str):
                # Replace ${VAR_NAME} patterns with security validation
                def replace_var(match):
                    var_name = match.group(1) or match.group(2)

                    # Security check: only allow safe environment variables
                    if not is_safe_env_var(var_name):
                        logger.warning(f"Environment variable '{var_name}' not in allowlist, skipping interpolation")
                        return match.group(0)  # Keep original placeholder

                    env_value = os.getenv(var_name)
                    if env_value is None:
                        logger.debug(f"Environment variable {var_name} not found, keeping placeholder")
                        return match.group(0)  # Keep original if not found

                    # Additional security: prevent command injection patterns
                    if self._contains_suspicious_patterns(env_value):
                        logger.error(f"Environment variable '{var_name}' contains suspicious patterns, blocking interpolation")
                        raise ValidationError(f"Security violation: Environment variable '{var_name}' contains potentially dangerous content")

                    return env_value
                
                # Handle both ${VAR} and $VAR patterns with strict validation
                value = re.sub(r'\$\{([A-Z_][A-Z0-9_]*)\}|\$([A-Z_][A-Z0-9_]*)', replace_var, value)
                
            elif isinstance(value, dict):
                return {k: interpolate_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [interpolate_value(item) for item in value]
            
            return value
        
        return interpolate_value(config)
    
    def _contains_suspicious_patterns(self, value: str) -> bool:
        """Check for suspicious patterns that might indicate injection attacks"""
        if not isinstance(value, str):
            return False
        
        suspicious_patterns = [
            # Command execution patterns
            r'`[^`]*`',           # Backticks
            r'\$\([^)]*\)',       # Command substitution
            r';\s*[a-zA-Z]',      # Command chaining
            r'\|\s*[a-zA-Z]',     # Pipe to command
            r'&&\s*[a-zA-Z]',     # AND command chaining
            r'\|\|\s*[a-zA-Z]',   # OR command chaining
            # File system access
            r'\.\./',             # Directory traversal
            r'/etc/',             # System directories
            r'/proc/',            # Process directory
            r'rm\s+-',            # Dangerous rm commands
            # Network patterns
            r'curl\s+',           # Network requests
            r'wget\s+',           # Downloads
            r'nc\s+',             # Netcat
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        
        return False
    
    def _process_table_configurations(self, tables_section: Dict[str, Any], default_config: Dict[str, Any]) -> Dict[str, TableConfig]:
        """Process table configurations with inheritance and validation"""
        table_configs = {}
        
        for table_name, table_data in tables_section.items():
            # Deep merge with default configuration
            merged_config = self._deep_merge_dicts(default_config, table_data or {})
            
            # Create table configuration with validation
            try:
                table_configs[table_name] = TableConfig(
                    full_name=merged_config.get('full_name', table_name),
                    target_name=merged_config.get('target_name'),
                    cdc_strategy=merged_config.get('cdc_strategy', 'hybrid'),
                    cdc_timestamp_column=merged_config.get('cdc_timestamp_column', 'updated_at'),
                    cdc_id_column=merged_config.get('cdc_id_column', 'id'),
                    table_type=merged_config.get('table_type', 'fact'),
                    full_sync_mode=merged_config.get('full_sync_mode', 'append'),
                    processing=merged_config.get('processing', {}),
                    validation=merged_config.get('validation', {}),
                    depends_on=merged_config.get('depends_on', []),
                    additional_where=merged_config.get('additional_where'),  # NEW: Index optimization
                    cdc_ordering=merged_config.get('cdc_ordering')  # NEW: Custom ordering
                )
            except Exception as e:
                logger.error(f"Failed to create table configuration for {table_name}: {e}")
                raise ValidationError(f"Table configuration error for {table_name}: {e}")
        
        return table_configs
    
    def get_pipeline_config(self, pipeline_name: str) -> PipelineConfig:
        """Get pipeline configuration by name with auto-reload check"""
        # Check if we need to reload
        self._check_auto_reload()
        
        if pipeline_name not in self.pipelines:
            # Try dynamic table registration for v1.0.0 compatibility
            if pipeline_name == "default":
                return self._get_or_create_default_pipeline()
            
            available_pipelines = list(self.pipelines.keys())
            raise ValidationError(
                f"Unknown pipeline: {pipeline_name}. "
                f"Available pipelines: {', '.join(available_pipelines)}"
            )
        
        return self.pipelines[pipeline_name]
    
    def _get_or_create_default_pipeline(self) -> PipelineConfig:
        """Get or create default pipeline for v1.0.0 compatibility"""
        if "default" in self.pipelines:
            return self.pipelines["default"]
        
        # Create minimal default pipeline
        default_tables = {}  # Will be populated dynamically
        
        self.pipelines["default"] = PipelineConfig(
            name="default_legacy",
            version="1.0.0",
            description="Auto-generated default pipeline for v1.0.0 compatibility",
            source="default",
            target="default",
            processing={'strategy': 'sequential'},
            s3={'isolation_prefix': '', 'partition_strategy': 'datetime'},
            default_table_config={},
            tables=default_tables
        )
        
        logger.info("Created default pipeline for v1.0.0 compatibility")
        return self.pipelines["default"]
    
    def register_table_dynamically(self, pipeline_name: str, table_name: str, table_config: Optional[Dict[str, Any]] = None):
        """Dynamically register a table in a pipeline (for v1.0.0 compatibility)"""
        if pipeline_name not in self.pipelines:
            if pipeline_name == "default":
                self._get_or_create_default_pipeline()
            else:
                raise ValidationError(f"Cannot register table in unknown pipeline: {pipeline_name}")
        
        pipeline = self.pipelines[pipeline_name]
        
        # Create table config with defaults
        config = table_config or {}
        table_obj = TableConfig(
            full_name=config.get('full_name', table_name),
            target_name=config.get('target_name'),
            cdc_strategy=config.get('cdc_strategy', 'timestamp_only'),  # v1.0.0 default
            cdc_timestamp_column=config.get('cdc_timestamp_column', 'updated_at'),
            cdc_id_column=config.get('cdc_id_column', 'id'),
            table_type=config.get('table_type', 'fact'),
            full_sync_mode=config.get('full_sync_mode', 'append'),
            processing=config.get('processing', {}),
            validation=config.get('validation', {}),
            depends_on=config.get('depends_on', []),
            additional_where=config.get('additional_where'),  # NEW: Index optimization
            cdc_ordering=config.get('cdc_ordering')  # NEW: Custom ordering
        )
        
        pipeline.tables[table_name] = table_obj
        logger.debug(f"Dynamically registered table {table_name} in pipeline {pipeline_name}")
    
    def get_table_config(self, pipeline_name: str, table_name: str) -> TableConfig:
        """Get table configuration within a pipeline"""
        pipeline_config = self.get_pipeline_config(pipeline_name)
        
        if table_name not in pipeline_config.tables:
            # For v1.0.0 compatibility, try dynamic registration
            if pipeline_name == "default":
                self.register_table_dynamically(pipeline_name, table_name)
                return pipeline_config.tables[table_name]
            
            available_tables = list(pipeline_config.tables.keys())
            raise ValidationError(
                f"Table {table_name} not found in pipeline {pipeline_name}. "
                f"Available tables: {', '.join(available_tables)}"
            )
        
        return pipeline_config.tables[table_name]
    
    def list_pipelines(self) -> List[str]:
        """List all available pipelines"""
        self._check_auto_reload()
        return sorted(self.pipelines.keys())
    
    def list_tables(self, pipeline_name: str) -> List[str]:
        """List all tables in a pipeline"""
        pipeline_config = self.get_pipeline_config(pipeline_name)
        return sorted(pipeline_config.tables.keys())
    
    def validate_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """Validate pipeline configuration and return comprehensive report"""
        validation_report = {
            'pipeline': pipeline_name,
            'valid': True,
            'errors': [],
            'warnings': [],
            'tables_validated': 0,
            'validation_details': {
                'pipeline_structure': {'valid': True, 'issues': []},
                'table_configurations': {'valid': True, 'issues': []},
                'dependencies': {'valid': True, 'issues': []},
                'resource_requirements': {'valid': True, 'issues': []}
            }
        }
        
        try:
            pipeline_config = self.get_pipeline_config(pipeline_name)
            
            # Validate pipeline-level configuration
            self._validate_pipeline_structure(pipeline_config, validation_report)
            
            # Validate table configurations
            self._validate_table_configurations(pipeline_config, validation_report)
            
            # Check dependency relationships
            self._validate_dependencies(pipeline_config, validation_report)
            
            # Validate resource requirements
            self._validate_resource_requirements(pipeline_config, validation_report)
            
            validation_report['valid'] = len(validation_report['errors']) == 0
            
        except Exception as e:
            validation_report['valid'] = False
            validation_report['errors'].append(f"Pipeline validation failed: {e}")
            validation_report['validation_details']['pipeline_structure']['valid'] = False
            validation_report['validation_details']['pipeline_structure']['issues'].append(str(e))
        
        return validation_report
    
    def _validate_pipeline_structure(self, pipeline_config: PipelineConfig, report: Dict[str, Any]):
        """Validate pipeline-level structure and settings"""
        details = report['validation_details']['pipeline_structure']
        
        # Check required fields
        if not pipeline_config.source:
            details['issues'].append("Missing source connection")
            report['errors'].append("Missing source connection")
            details['valid'] = False
        
        if not pipeline_config.target:
            details['issues'].append("Missing target connection")
            report['errors'].append("Missing target connection") 
            details['valid'] = False
        
        # Validate processing strategy
        valid_strategies = ['sequential', 'parallel']
        strategy = pipeline_config.processing.get('strategy', 'sequential')
        if strategy not in valid_strategies:
            details['issues'].append(f"Invalid processing strategy: {strategy}")
            report['errors'].append(f"Invalid processing strategy: {strategy}")
            details['valid'] = False
        
        # Check S3 configuration
        s3_config = pipeline_config.s3
        if not s3_config.get('partition_strategy'):
            details['issues'].append("Missing S3 partition strategy")
            report['warnings'].append("Missing S3 partition strategy")
        
        # Validate compression setting
        valid_compression = ['snappy', 'gzip', 'lz4', 'brotli', 'uncompressed']
        compression = s3_config.get('compression', 'snappy')
        if compression not in valid_compression:
            details['issues'].append(f"Invalid compression: {compression}")
            report['warnings'].append(f"Invalid compression: {compression}")
    
    def _validate_table_configurations(self, pipeline_config: PipelineConfig, report: Dict[str, Any]):
        """Validate all table configurations in the pipeline"""
        details = report['validation_details']['table_configurations']
        
        if not pipeline_config.tables:
            details['issues'].append("No tables configured in pipeline")
            report['warnings'].append("No tables configured in pipeline")
            return
        
        for table_name, table_config in pipeline_config.tables.items():
            try:
                self._validate_single_table_config(table_config, pipeline_config)
                report['tables_validated'] += 1
            except Exception as e:
                details['issues'].append(f"Table {table_name}: {e}")
                report['errors'].append(f"Table {table_name}: {e}")
                details['valid'] = False

    def _validate_single_table_config(self, table_config: TableConfig, pipeline_config: 'PipelineConfig'):
        """Validate a single table configuration"""
        # Check required fields
        if not table_config.full_name:
            raise ValidationError("Missing table full_name")
        
        # Validate CDC strategy requirements
        if table_config.cdc_strategy in ['hybrid', 'timestamp_only']:
            if not table_config.cdc_timestamp_column:
                raise ValidationError(f"CDC timestamp column required for {table_config.cdc_strategy} strategy")
        
        if table_config.cdc_strategy in ['hybrid', 'id_only']:
            if not table_config.cdc_id_column:
                raise ValidationError(f"CDC ID column required for {table_config.cdc_strategy} strategy")
        
        # Validate processing settings using proper hierarchy
        from src.utils.validation import resolve_batch_size

        # Convert table_config to dictionary format for utility function
        table_config_dict = {
            'processing': table_config.processing
        }

        # Convert pipeline_config to dictionary format
        pipeline_config_dict = {
            'processing': getattr(pipeline_config, 'processing', {}) or {}
        }

        batch_size = resolve_batch_size(
            table_config=table_config_dict,
            pipeline_config=pipeline_config_dict
        )

        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValidationError(f"Invalid batch_size: {batch_size}")
        
        # Validate validation settings
        max_null_pct = table_config.validation.get('max_null_percentage', 10.0)
        if not isinstance(max_null_pct, (int, float)) or not (0 <= max_null_pct <= 100):
            raise ValidationError(f"Invalid max_null_percentage: {max_null_pct}")
    
    def _validate_dependencies(self, pipeline_config: PipelineConfig, report: Dict[str, Any]):
        """Validate table dependencies and check for cycles"""
        details = report['validation_details']['dependencies']
        
        # Build dependency graph
        graph = {}
        for table_name, table_config in pipeline_config.tables.items():
            graph[table_name] = table_config.depends_on
        
        # Check for missing dependencies
        all_tables = set(graph.keys())
        for table_name, dependencies in graph.items():
            for dep in dependencies:
                if dep not in all_tables:
                    details['issues'].append(f"Table {table_name} depends on non-existent table: {dep}")
                    report['errors'].append(f"Table {table_name} depends on non-existent table: {dep}")
                    details['valid'] = False
        
        # Check for circular dependencies
        cycles = self._detect_dependency_cycles(graph)
        if cycles:
            for cycle in cycles:
                cycle_str = " -> ".join(cycle)
                details['issues'].append(f"Circular dependency detected: {cycle_str}")
                report['errors'].append(f"Circular dependency detected: {cycle_str}")
                details['valid'] = False
    
    def _detect_dependency_cycles(self, graph: Dict[str, List[str]]) -> List[List[str]]:
        """Detect circular dependencies using DFS"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {node: WHITE for node in graph}
        cycles = []
        
        def dfs(node, path):
            if color[node] == GRAY:
                # Found a cycle
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
                return
            
            if color[node] == BLACK:
                return
            
            color[node] = GRAY
            path.append(node)
            
            for neighbor in graph.get(node, []):
                if neighbor in graph:  # Only follow existing nodes
                    dfs(neighbor, path)
            
            path.pop()
            color[node] = BLACK
        
        for node in graph:
            if color[node] == WHITE:
                dfs(node, [])
        
        return cycles
    
    def _validate_resource_requirements(self, pipeline_config: PipelineConfig, report: Dict[str, Any]):
        """Validate resource requirements and constraints"""
        details = report['validation_details']['resource_requirements']
        
        # Check parallel processing limits
        if pipeline_config.processing.get('strategy') == 'parallel':
            max_parallel = pipeline_config.processing.get('max_parallel_tables', 3)
            table_count = len(pipeline_config.tables)
            
            if max_parallel > table_count:
                details['issues'].append(f"max_parallel_tables ({max_parallel}) exceeds table count ({table_count})")
                report['warnings'].append(f"max_parallel_tables ({max_parallel}) exceeds table count ({table_count})")
            
            # Check for resource conflicts
            high_priority_tables = [
                name for name, config in pipeline_config.tables.items()
                if config.processing.get('priority') == 'high'
            ]
            
            if len(high_priority_tables) > max_parallel:
                details['issues'].append(f"Too many high-priority tables ({len(high_priority_tables)}) for parallel capacity ({max_parallel})")
                report['warnings'].append(f"Consider increasing max_parallel_tables or reducing high-priority tables")
        
        # Validate timeout settings
        pipeline_timeout = pipeline_config.processing.get('timeout_minutes', 120)
        for table_name, table_config in pipeline_config.tables.items():
            table_timeout = table_config.processing.get('timeout_minutes', 60)
            if table_timeout > pipeline_timeout:
                details['issues'].append(f"Table {table_name} timeout ({table_timeout}m) exceeds pipeline timeout ({pipeline_timeout}m)")
                report['warnings'].append(f"Table {table_name} timeout exceeds pipeline timeout")
    
    def _check_auto_reload(self):
        """Check if configuration files have changed and reload if needed"""
        if not hasattr(self, '_auto_reload_enabled') or not self._auto_reload_enabled:
            return
        
        reload_needed = False
        
        # Check all tracked files for changes
        for file_path, last_mtime in self._file_timestamps.items():
            if file_path.exists():
                current_mtime = file_path.stat().st_mtime
                if current_mtime > last_mtime:
                    reload_needed = True
                    break
        
        if reload_needed:
            logger.info("Configuration files changed, reloading...")
            self.reload_configuration()
    
    def set_environment(self, environment: str):
        """Set current environment and reload configurations"""
        if environment != self.current_environment:
            self.current_environment = environment
            logger.info(f"Environment changed to: {environment}")
            self.reload_configuration()
    
    def get_environment(self) -> str:
        """Get current environment"""
        return self.current_environment
    
    def list_environments(self) -> List[str]:
        """List available environments"""
        return sorted(self.environments.keys())
    
    def enable_auto_reload(self, enabled: bool = True):
        """Enable or disable automatic configuration reloading"""
        self._auto_reload_enabled = enabled
        logger.info(f"Auto-reload {'enabled' if enabled else 'disabled'}")
    
    def reload_configuration(self):
        """Reload all configuration files"""
        logger.info("Reloading all configuration files...")
        
        # Clear existing data
        self.pipelines.clear()
        self.environments.clear()
        self.templates.clear()
        self.connections_config.clear()
        self._config_cache.clear()
        self._file_timestamps.clear()

        # Reload everything
        self._load_connections()
        self._load_environments()
        self._load_templates()
        self._load_pipelines()
        
        self._last_reload = datetime.now()
        logger.info(f"Configuration reload completed: {len(self.pipelines)} pipelines loaded")
    
    def get_configuration_status(self) -> Dict[str, Any]:
        """Get comprehensive configuration status"""
        return {
            'pipelines': {
                'count': len(self.pipelines),
                'names': list(self.pipelines.keys())
            },
            'environments': {
                'count': len(self.environments),
                'current': self.current_environment,
                'available': list(self.environments.keys())
            },
            'templates': {
                'count': len(self.templates),
                'available': list(self.templates.keys())
            },
            'configuration': {
                'root_directory': str(self.config_root),
                'auto_reload_enabled': getattr(self, '_auto_reload_enabled', False),
                'last_reload': self._last_reload.isoformat() if self._last_reload else None,
                'tracked_files': len(self._file_timestamps)
            },
            'validation_summary': self._get_validation_summary()
        }
    
    def _get_validation_summary(self) -> Dict[str, Any]:
        """Get validation summary for all pipelines"""
        summary = {
            'total_pipelines': len(self.pipelines),
            'valid_pipelines': 0,
            'invalid_pipelines': 0,
            'total_tables': 0,
            'validation_errors': []
        }
        
        for pipeline_name in self.pipelines.keys():
            try:
                validation_result = self.validate_pipeline(pipeline_name)
                if validation_result['valid']:
                    summary['valid_pipelines'] += 1
                else:
                    summary['invalid_pipelines'] += 1
                    summary['validation_errors'].extend([
                        f"{pipeline_name}: {error}" for error in validation_result['errors']
                    ])
                
                summary['total_tables'] += validation_result['tables_validated']
                
            except Exception as e:
                summary['invalid_pipelines'] += 1
                summary['validation_errors'].append(f"{pipeline_name}: Validation failed - {e}")
        
        return summary

    def get_s3_config(self, s3_config_name: str) -> Dict[str, Any]:
        """
        Get S3 configuration from connections.yml

        Args:
            s3_config_name: Name of S3 config to retrieve (required)

        Returns:
            S3 configuration dictionary

        Raises:
            ValidationError: If s3_config_name not found
        """
        s3_configs = self.connections_config.get('s3_configs', {})
        if s3_config_name not in s3_configs:
            available = ', '.join(s3_configs.keys()) if s3_configs else 'none'
            raise ValidationError(
                f"S3 config '{s3_config_name}' not found in connections.yml. "
                f"Available: {available}"
            )

        config = s3_configs[s3_config_name]
        logger.info(f"Using S3 config '{s3_config_name}': {config.get('bucket_name', 'unknown-bucket')}")
        return config

    def list_s3_configs(self) -> Dict[str, str]:
        """
        List all available S3 configurations

        Returns:
            Dictionary mapping S3 config names to their descriptions
        """
        s3_configs = self.connections_config.get('s3_configs', {})
        return {
            name: config.get('description', 'No description')
            for name, config in s3_configs.items()
        }

    def get_backup_config(self) -> Dict[str, Any]:
        """Get backup configuration from connections.yml"""
        return self.connections_config.get('backup', {})

    def get_connection(self, connection_name: str) -> Dict[str, Any]:
        """Get specific connection configuration"""
        sources = self.connections_config.get('connections', {}).get('sources', {})
        targets = self.connections_config.get('connections', {}).get('targets', {})

        if connection_name in sources:
            return sources[connection_name]
        elif connection_name in targets:
            return targets[connection_name]
        else:
            raise ValidationError(f"Connection '{connection_name}' not found in connections.yml")

    def create_app_config(self, source_connection: str, target_connection: str, s3_config_name: str) -> 'AppConfig':
        """
        Create AppConfig from connections.yml (v1.2.0 multi-pipeline system).

        All parameters are REQUIRED - no v1.0.0 compatibility/defaults.

        Args:
            source_connection: Required source connection name
            target_connection: Required target connection name
            s3_config_name: Required S3 config name

        Returns:
            AppConfig instance populated from connections.yml

        Raises:
            ValueError: If any parameter is None or if connection/config not found
        """
        from src.config.settings import AppConfig
        import os

        # Validate required parameters
        if not source_connection:
            raise ValueError("source_connection is required - v1.0.0 compatibility removed")
        if not target_connection:
            raise ValueError("target_connection is required - v1.0.0 compatibility removed")
        if not s3_config_name:
            raise ValueError("s3_config_name is required - v1.0.0 compatibility removed")

        # Get connections
        sources = self.connections_config.get('connections', {}).get('sources', {})
        targets = self.connections_config.get('connections', {}).get('targets', {})
        s3_configs = self.connections_config.get('s3_configs', {})

        if not sources:
            raise ValidationError("No source connections defined in connections.yml")
        if not targets:
            raise ValidationError("No target connections defined in connections.yml")
        if not s3_configs:
            raise ValidationError("No S3 configurations defined in connections.yml")

        # Validate specified connections exist
        if source_connection not in sources:
            raise ValidationError(f"Source connection '{source_connection}' not found in connections.yml")
        if target_connection not in targets:
            raise ValidationError(f"Target connection '{target_connection}' not found in connections.yml")
        if s3_config_name not in s3_configs:
            raise ValidationError(f"S3 config '{s3_config_name}' not found in connections.yml")

        source_config = sources[source_connection]
        target_config = targets[target_connection]
        s3_config = self.get_s3_config(s3_config_name)
        backup_config = self.connections_config.get('backup', {})

        logger.debug(f"Creating AppConfig from source={source_connection}, target={target_connection}, s3_config={s3_config_name}")

        # Import config classes
        from src.config.settings import DatabaseConfig, SSHConfig, S3Config, RedshiftConfig, RedshiftSSHConfig, BackupConfig
        from pydantic import SecretStr

        # Build AppConfig directly from YAML data (no env var bridge)
        app_config = AppConfig()

        # Database config (source)
        app_config._database = DatabaseConfig(
            host=source_config.get('host'),
            port=source_config.get('port', 3306),
            user=source_config.get('username'),
            password=SecretStr(source_config.get('password')) if source_config.get('password') else None,
            database=source_config.get('database')
        )

        # SSH config (source) - only if enabled
        source_ssh = source_config.get('ssh_tunnel', {})
        if source_ssh.get('enabled', False):
            app_config._ssh = SSHConfig(
                bastion_host=source_ssh.get('host'),
                bastion_user=source_ssh.get('username'),
                bastion_key_path=source_ssh.get('private_key_path'),
                local_port=source_ssh.get('local_port', 0)
            )
        else:
            # No SSH tunnel - set to None or create placeholder that won't be used
            app_config._ssh = None

        # Redshift config (target)
        app_config._redshift = RedshiftConfig(
            host=target_config.get('host'),
            port=target_config.get('port', 5439),
            user=target_config.get('username'),
            password=SecretStr(target_config.get('password')) if target_config.get('password') else None,
            database=target_config.get('database'),
            schema=target_config.get('schema', 'public')
        )

        # Redshift SSH config (target) - only if enabled
        target_ssh = target_config.get('ssh_tunnel', {})
        if target_ssh.get('enabled', False):
            app_config._redshift_ssh = RedshiftSSHConfig(
                host=target_ssh.get('host'),
                username=target_ssh.get('username'),
                private_key_path=target_ssh.get('private_key_path'),
                local_port=target_ssh.get('local_port', 0)
            )
        else:
            # No SSH tunnel - set to None
            app_config._redshift_ssh = None

        # S3 config
        app_config._s3 = S3Config(
            bucket_name=s3_config.get('bucket_name'),
            access_key=s3_config.get('access_key_id'),
            secret_key=SecretStr(s3_config.get('secret_access_key')) if s3_config.get('secret_access_key') else None,
            region=s3_config.get('region', 'us-west-2'),
            incremental_path=s3_config.get('incremental_path', 'incremental/'),
            high_watermark_key=s3_config.get('high_watermark_key', 'high_watermark'),
            multipart_threshold=s3_config.get('performance', {}).get('multipart_threshold', 8388608),
            multipart_chunksize=s3_config.get('performance', {}).get('multipart_chunksize', 8388608),
            max_concurrency=s3_config.get('performance', {}).get('max_concurrency', 10),
            max_pool_connections=s3_config.get('performance', {}).get('max_pool_connections', 10),
            retry_max_attempts=s3_config.get('performance', {}).get('retry_max_attempts', 3),
            retry_mode=s3_config.get('performance', {}).get('retry_mode', 'standard')
        )

        # Backup config
        app_config._backup = BackupConfig(
            batch_size=backup_config.get('batch_size', 10000),
            max_workers=backup_config.get('max_workers', 4),
            retry_attempts=backup_config.get('retry_attempts', 3),
            timeout_seconds=backup_config.get('timeout_seconds', 300),
            memory_limit_mb=backup_config.get('memory_limit_mb', 512),
            gc_threshold=backup_config.get('gc_threshold', 100),
            memory_check_interval=backup_config.get('memory_check_interval', 10),
            enable_compression=backup_config.get('enable_compression', True),
            compression_level=backup_config.get('compression_level', 6),
            target_rows_per_chunk=backup_config.get('target_rows_per_chunk', 50000),
            max_rows_per_chunk=backup_config.get('max_rows_per_chunk', 100000)
        )

        logger.info(
            f"Created AppConfig from connections.yml: "
            f"source={source_name}, target={target_name}"
        )

        return app_config

    def export_configuration(self, output_path: Optional[str] = None) -> Dict[str, Any]:
        """Export current configuration for backup or analysis"""
        export_data = {
            'metadata': {
                'export_timestamp': datetime.now().isoformat(),
                'environment': self.current_environment,
                'version': '1.1.0'
            },
            'pipelines': {},
            'environments': self.environments.copy(),
            'templates': self.templates.copy()
        }
        
        # Export pipeline configurations (serialize TableConfig objects)
        for name, pipeline in self.pipelines.items():
            export_data['pipelines'][name] = {
                'name': pipeline.name,
                'version': pipeline.version,
                'description': pipeline.description,
                'source': pipeline.source,
                'target': pipeline.target,
                'processing': pipeline.processing,
                's3': pipeline.s3,
                'default_table_config': pipeline.default_table_config,
                'metadata': pipeline.metadata,
                'tables': {
                    table_name: {
                        'full_name': table_config.full_name,
                        'target_name': table_config.target_name,
                        'cdc_strategy': table_config.cdc_strategy,
                        'cdc_timestamp_column': table_config.cdc_timestamp_column,
                        'cdc_id_column': table_config.cdc_id_column,
                        'table_type': table_config.table_type,
                        'processing': table_config.processing,
                        'validation': table_config.validation,
                        'depends_on': table_config.depends_on
                    }
                    for table_name, table_config in pipeline.tables.items()
                }
            }
        
        # Save to file if path provided
        if output_path:
            output_file = Path(output_path)
            with open(output_file, 'w') as f:
                yaml.dump(export_data, f, default_flow_style=False, indent=2)
            logger.info(f"Configuration exported to: {output_file}")
        
        return export_data