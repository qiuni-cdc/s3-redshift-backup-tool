"""
Dynamic Schema Management System - Gemini Implementation

Implements the Gemini solution for dynamic schema discovery from MySQL databases,
automatic PyArrow schema generation, and Redshift table creation.
"""

import pyarrow as pa
import mysql.connector
import psycopg2
from typing import Dict, Tuple, List, Optional, Any
from pathlib import Path
import json

from src.utils.exceptions import ValidationError, ConnectionError as BackupConnectionError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class DynamicSchemaManager:
    """
    Manages schemas by dynamically discovering them from the source database.
    
    This implements the Gemini approach:
    1. Discover MySQL table schema using INFORMATION_SCHEMA
    2. Translate types: MySQL → PyArrow → Redshift
    3. Cache schemas to avoid repeated database queries
    4. Generate Redshift DDL with performance keys
    """
    
    def __init__(self, connection_manager):
        """
        Initialize with connection manager for secure database access
        
        Args:
            connection_manager: ConnectionManager instance for database access
        """
        self.connection_manager = connection_manager
        # Get config from connection manager for Redshift operations
        self.config = connection_manager.config if hasattr(connection_manager, 'config') else None
        
        # Schema cache: table_name -> (PyArrow schema, Redshift DDL)
        self._schema_cache: Dict[str, Tuple[pa.Schema, str]] = {}
        
        # Performance keys configuration
        self._performance_keys_config = self._load_performance_keys_config()
        
        logger.info("Dynamic Schema Manager initialized with Gemini approach")
    
    def _load_performance_keys_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Load optional performance keys configuration for Redshift tables
        
        Returns:
            Dict with table-specific DISTKEY and SORTKEY configurations
        """
        config_file = Path('redshift_keys.json')
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                logger.info(f"Loaded performance keys config: {len(config)} tables")
                return config
            except Exception as e:
                logger.warning(f"Failed to load performance keys config: {e}")
        
        # Default empty config - use AUTO keys
        logger.debug("No performance keys config found, using AUTO keys")
        return {}
    
    def get_schemas(self, table_name: str) -> Tuple[pa.Schema, str]:
        """
        Get PyArrow schema and Redshift DDL for a table
        
        Args:
            table_name: Full table name (e.g., 'settlement.settlement_normal_delivery_detail')
        
        Returns:
            Tuple of (PyArrow schema, Redshift CREATE TABLE DDL)
        
        Raises:
            ValidationError: If table not found or schema discovery fails
        """
        # Check cache first
        if table_name not in self._schema_cache:
            logger.info(f"Schema not cached, discovering from MySQL: {table_name}")
            self._discover_and_cache_schema(table_name)
        else:
            logger.debug(f"Using cached schema for {table_name}")
        
        return self._schema_cache[table_name]
    
    def _discover_and_cache_schema(self, table_name: str):
        """
        Connect to MySQL, discover table schema, translate it, and cache results
        
        Args:
            table_name: Full table name to discover
        
        Raises:
            ValidationError: If table not found
            ConnectionError: If database connection fails
        """
        logger.info(f"Discovering schema for {table_name}")
        
        # Parse table name
        if '.' not in table_name:
            raise ValidationError(f"Table name must include database: {table_name}")
        
        db_name, tbl_name = table_name.split('.', 1)
        
        # Enhanced INFORMATION_SCHEMA query with COLUMN_TYPE for decimal precision
        # SECURITY FIX: Use parameterized query to prevent SQL injection
        discovery_query = """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
               NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION;
        """
        
        # Validate table name components against injection patterns
        self._validate_table_name_security(db_name, tbl_name)
        
        try:
            # Use existing connection manager for secure access
            with self.connection_manager.ssh_tunnel() as local_port:
                with self.connection_manager.database_connection(local_port) as conn:
                    cursor = conn.cursor(dictionary=True)
                    # SECURITY FIX: Use parameterized query with proper escaping
                    cursor.execute(discovery_query, (db_name, tbl_name))
                    columns_info = cursor.fetchall()
                    cursor.close()
            
            if not columns_info:
                raise ValidationError(f"Table '{table_name}' not found in source database.")
            
            logger.info(f"Discovered {len(columns_info)} columns for {table_name}")
            
            # Translate to PyArrow schema and Redshift DDL
            pyarrow_schema, redshift_ddl = self._translate_schema(table_name, columns_info)
            
            # Cache the results
            self._schema_cache[table_name] = (pyarrow_schema, redshift_ddl)
            
            logger.info(f"Schema cached successfully for {table_name}")
            
        except mysql.connector.Error as e:
            logger.error(f"MySQL error during schema discovery for {table_name}: {e}")
            raise BackupConnectionError(f"Failed to discover schema for {table_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during schema discovery for {table_name}: {e}")
            raise ValidationError(f"Schema discovery failed for {table_name}: {e}")
    
    def _translate_schema(self, table_name: str, columns_info: List[Dict]) -> Tuple[pa.Schema, str]:
        """
        Translate MySQL column info to PyArrow schema and Redshift DDL
        
        Args:
            table_name: Full table name
            columns_info: List of column dictionaries from INFORMATION_SCHEMA
        
        Returns:
            Tuple of (PyArrow schema, Redshift DDL)
        """
        # Build PyArrow fields
        fields = []
        redshift_columns = []
        
        for col in columns_info:
            col_name = col['COLUMN_NAME']
            mysql_type = col['DATA_TYPE']
            column_type = col.get('COLUMN_TYPE', mysql_type)  # Full type like decimal(10,3)
            is_nullable = col['IS_NULLABLE'] == 'YES'
            precision = col.get('NUMERIC_PRECISION')
            scale = col.get('NUMERIC_SCALE')
            
            # Enhanced type translation with full column information
            pa_type, redshift_type = self._translate_type_enhanced(mysql_type, column_type, precision, scale)
            
            # Create PyArrow field
            fields.append(pa.field(col_name, pa_type, nullable=is_nullable))
            
            # Create Redshift column definition
            redshift_columns.append(f'"{col_name}" {redshift_type}')
            
            logger.debug(f"Translated {col_name}: {column_type} → {pa_type} → {redshift_type}")
        
        # Create PyArrow schema
        pyarrow_schema = pa.schema(fields)
        
        # Generate Redshift DDL
        redshift_ddl = self._generate_redshift_ddl(table_name, redshift_columns)
        
        logger.info(f"Schema translation complete: {len(fields)} fields")
        return pyarrow_schema, redshift_ddl
    
    def _translate_type_enhanced(self, mysql_type: str, column_type: str, precision: int, scale: int) -> Tuple[pa.DataType, str]:
        """
        Enhanced type translation using full MySQL column information
        
        Args:
            mysql_type: Basic MySQL type (e.g., 'decimal', 'varchar')
            column_type: Full column type (e.g., 'decimal(10,3)', 'varchar(32)')
            precision: Numeric precision (for decimal types)
            scale: Numeric scale (for decimal types)
        
        Returns:
            Tuple of (PyArrow type, Redshift type)
        """
        mysql_type_lower = mysql_type.lower()
        column_type_lower = column_type.lower()
        
        # Integer types
        if 'int' in mysql_type_lower:
            return (pa.int64(), 'BIGINT')
        
        # Timestamp and datetime
        if 'datetime' in mysql_type_lower or 'timestamp' in mysql_type_lower:
            return (pa.timestamp('us'), 'TIMESTAMP')
        
        # Decimal types with precise precision/scale
        if mysql_type_lower in ['decimal', 'numeric']:
            if precision is not None and scale is not None:
                # Use exact precision from MySQL
                return (pa.decimal128(precision, scale), f'DECIMAL({precision},{scale})')
            else:
                # Parse from column_type if precision/scale not available
                import re
                decimal_match = re.search(r'decimal\((\d+),(\d+)\)', column_type_lower)
                if decimal_match:
                    p = int(decimal_match.group(1))
                    s = int(decimal_match.group(2))
                    return (pa.decimal128(p, s), f'DECIMAL({p},{s})')
                else:
                    # Default decimal
                    return (pa.decimal128(18, 2), 'DECIMAL(18,2)')
        
        # Float and double types
        if any(t in mysql_type_lower for t in ['float', 'double']):
            return (pa.float64(), 'DOUBLE PRECISION')
        
        # Date types
        if 'date' in mysql_type_lower and 'datetime' not in mysql_type_lower:
            return (pa.date32(), 'DATE')
        
        # Boolean types
        if mysql_type_lower in ['tinyint(1)', 'boolean', 'bool']:
            return (pa.bool_(), 'BOOLEAN')
        
        # Enhanced string type mapping with precise sizing from column_type
        if any(t in mysql_type_lower for t in ['varchar', 'char', 'text']):
            # Extract size from column_type like varchar(32), char(8), etc.
            import re
            size_match = re.search(r'\((\d+)\)', column_type_lower)
            if size_match:
                size = int(size_match.group(1))
                # Use exact size from MySQL
                return (pa.string(), f'VARCHAR({size})')
            else:
                # TEXT types without explicit size
                if 'text' in mysql_type_lower:
                    return (pa.string(), 'VARCHAR(65535)')
                else:
                    return (pa.string(), 'VARCHAR(255)')  # Default for unspecified VARCHAR
        
        # Default to string for unknown types
        return (pa.string(), 'VARCHAR(255)')
    
    def _generate_redshift_ddl(self, table_name: str, columns: List[str]) -> str:
        """
        Generate Redshift CREATE TABLE DDL statement
        
        Args:
            table_name: Full table name (e.g., 'settlement.table_name')
            columns: List of Redshift column definitions
        
        Returns:
            Complete CREATE TABLE DDL statement
        """
        # Extract table name for Redshift (schema.table → public.table)
        db_name, tbl_name = table_name.split('.', 1)
        redshift_table_name = f"public.{tbl_name}"
        
        # Default performance settings
        dist_style = "DISTSTYLE AUTO"
        sort_key = "SORTKEY AUTO"
        
        # Check for custom performance configuration
        key_config = self._performance_keys_config.get(table_name)
        if key_config:
            if key_config.get("distkey"):
                dist_style = f'DISTKEY({key_config["distkey"]})'
            if key_config.get("sortkey"):
                if isinstance(key_config["sortkey"], list):
                    sort_key_cols = ', '.join(key_config["sortkey"])
                else:
                    sort_key_cols = key_config["sortkey"]
                sort_key = f"SORTKEY({sort_key_cols})"
        
        # Generate complete DDL
        ddl = f"""
CREATE TABLE IF NOT EXISTS {redshift_table_name} (
    {','.join(columns)}
)
{dist_style}
{sort_key};
        """.strip()
        
        logger.debug(f"Generated Redshift DDL for {table_name}")
        return ddl
    
    def _validate_table_name_security(self, db_name: str, tbl_name: str):
        """
        Validate table name components for security issues
        
        Args:
            db_name: Database name component
            tbl_name: Table name component
            
        Raises:
            ValidationError: If table name contains suspicious patterns
        """
        import re
        
        # Check for basic SQL injection patterns
        dangerous_patterns = [
            r'[;\'"\\]',  # SQL terminators and quotes
            r'--',        # SQL comments
            r'/\*',       # SQL block comments
            r'\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE)\b',  # SQL keywords
            r'\b(UNION|SELECT|FROM|WHERE)\b',  # More SQL keywords
            r'[\x00-\x1F\x7F]',  # Control characters
        ]
        
        for component, name in [(db_name, "database"), (tbl_name, "table")]:
            # Basic length check
            if len(component) > 64:  # MySQL identifier limit
                raise ValidationError(f"Invalid {name} name: too long (max 64 characters)")
            
            # Check for dangerous patterns
            for pattern in dangerous_patterns:
                if re.search(pattern, component, re.IGNORECASE):
                    logger.error(f"Security validation failed: suspicious pattern in {name} name")
                    raise ValidationError(f"Invalid {name} name: contains suspicious characters")
        
        # Additional validation: must be valid MySQL identifiers
        identifier_pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
        if not re.match(identifier_pattern, db_name):
            raise ValidationError(f"Invalid database name format: {db_name}")
        if not re.match(identifier_pattern, tbl_name):
            raise ValidationError(f"Invalid table name format: {tbl_name}")
        
        logger.debug(f"Table name security validation passed: {db_name}.{tbl_name}")

    def clear_cache(self):
        """Clear the schema cache"""
        cache_size = len(self._schema_cache)
        self._schema_cache.clear()
        logger.info(f"Cleared schema cache ({cache_size} entries)")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about cached schemas"""
        return {
            'cached_tables': list(self._schema_cache.keys()),
            'cache_size': len(self._schema_cache),
            'performance_config_tables': list(self._performance_keys_config.keys())
        }