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
from datetime import datetime

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
        
        # P1 FIX: Enhanced schema cache with TTL support
        # Schema cache: table_name -> {'schemas': (PyArrow, DDL), 'cached_at': datetime, 'access_count': int}
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
        
        # Cache configuration
        self._cache_ttl_hours = 24  # Default 24 hour TTL
        self._max_cache_size = 100  # Maximum cached schemas
        self._cache_stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expired': 0
        }
        
        # Performance keys configuration
        self._performance_keys_config = self._load_performance_keys_config()
        
        logger.info("Dynamic Schema Manager initialized with Gemini approach and TTL cache")
    
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
        Get PyArrow schema and Redshift DDL for a table with TTL cache support
        
        Args:
            table_name: Full table name (e.g., 'settlement.settlement_normal_delivery_detail')
        
        Returns:
            Tuple of (PyArrow schema, Redshift CREATE TABLE DDL)
        
        Raises:
            ValidationError: If table not found or schema discovery fails
        """
        # P1 FIX: Check cache with TTL validation
        if table_name in self._schema_cache:
            cache_entry = self._schema_cache[table_name]
            
            # Check if cache entry is still valid (not expired)
            if self._is_cache_entry_valid(cache_entry):
                # Update access statistics
                cache_entry['access_count'] += 1
                cache_entry['last_accessed'] = datetime.now()
                self._cache_stats['hits'] += 1
                
                logger.debug(f"Using cached schema for {table_name} (age: {self._get_cache_age_hours(cache_entry):.1f}h)")
                return cache_entry['schemas']
            else:
                # Cache expired, remove it
                logger.info(f"Cache expired for {table_name}, removing and re-discovering")
                del self._schema_cache[table_name]
                self._cache_stats['expired'] += 1
        
        # Cache miss or expired - discover schema
        logger.info(f"Schema not cached or expired, discovering from MySQL: {table_name}")
        self._cache_stats['misses'] += 1
        self._discover_and_cache_schema(table_name)
        
        return self._schema_cache[table_name]['schemas']
    
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
            
            # P1 FIX: Cache with TTL metadata
            cache_entry = {
                'schemas': (pyarrow_schema, redshift_ddl),
                'cached_at': datetime.now(),
                'last_accessed': datetime.now(),
                'access_count': 1,
                'table_columns': len(columns_info)
            }
            
            # Enforce cache size limit with LRU eviction
            self._enforce_cache_size_limit()
            
            self._schema_cache[table_name] = cache_entry
            
            logger.info(f"Schema cached successfully for {table_name} with TTL support")
            
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
    
    def _is_cache_entry_valid(self, cache_entry: Dict[str, Any]) -> bool:
        """
        Check if a cache entry is still valid (not expired)
        
        Args:
            cache_entry: Cache entry with cached_at timestamp
            
        Returns:
            True if cache entry is valid, False if expired
        """
        if 'cached_at' not in cache_entry:
            return False
        
        cached_at = cache_entry['cached_at']
        age_hours = (datetime.now() - cached_at).total_seconds() / 3600
        
        return age_hours < self._cache_ttl_hours
    
    def _get_cache_age_hours(self, cache_entry: Dict[str, Any]) -> float:
        """Get the age of a cache entry in hours"""
        if 'cached_at' not in cache_entry:
            return float('inf')
        
        cached_at = cache_entry['cached_at']
        return (datetime.now() - cached_at).total_seconds() / 3600
    
    def _enforce_cache_size_limit(self):
        """
        Enforce maximum cache size by evicting least recently used entries
        """
        if len(self._schema_cache) < self._max_cache_size:
            return
        
        # Find least recently used entries
        entries_by_access = [
            (table_name, entry['last_accessed'], entry['access_count'])
            for table_name, entry in self._schema_cache.items()
        ]
        
        # Sort by last access time (oldest first), then by access count (least used first)
        entries_by_access.sort(key=lambda x: (x[1], x[2]))
        
        # Evict oldest entries until we're under the limit
        entries_to_evict = len(self._schema_cache) - self._max_cache_size + 10  # Evict 10 extra for buffer
        
        for i in range(min(entries_to_evict, len(entries_by_access))):
            table_name = entries_by_access[i][0]
            if table_name in self._schema_cache:
                del self._schema_cache[table_name]
                self._cache_stats['evictions'] += 1
                logger.debug(f"Evicted schema cache entry for {table_name} (LRU policy)")
    
    def _cleanup_expired_cache_entries(self):
        """
        Proactively clean up expired cache entries
        """
        expired_tables = []
        
        for table_name, cache_entry in self._schema_cache.items():
            if not self._is_cache_entry_valid(cache_entry):
                expired_tables.append(table_name)
        
        for table_name in expired_tables:
            del self._schema_cache[table_name]
            self._cache_stats['expired'] += 1
            logger.debug(f"Removed expired schema cache entry for {table_name}")
        
        if expired_tables:
            logger.info(f"Cleaned up {len(expired_tables)} expired schema cache entries")

    def clear_cache(self):
        """Clear the schema cache and reset statistics"""
        cache_size = len(self._schema_cache)
        self._schema_cache.clear()
        
        # Reset cache statistics
        self._cache_stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expired': 0
        }
        
        logger.info(f"Cleared schema cache ({cache_size} entries) and reset statistics")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get comprehensive information about cached schemas and performance"""
        # Calculate cache statistics
        total_requests = self._cache_stats['hits'] + self._cache_stats['misses']
        hit_rate = (self._cache_stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        # Get cache entry details
        cache_details = {}
        current_time = datetime.now()
        
        for table_name, cache_entry in self._schema_cache.items():
            age_hours = self._get_cache_age_hours(cache_entry)
            cache_details[table_name] = {
                'age_hours': round(age_hours, 2),
                'access_count': cache_entry.get('access_count', 0),
                'table_columns': cache_entry.get('table_columns', 0),
                'is_valid': self._is_cache_entry_valid(cache_entry),
                'last_accessed': cache_entry.get('last_accessed', cache_entry.get('cached_at')).isoformat()
            }
        
        return {
            'cached_tables': list(self._schema_cache.keys()),
            'cache_size': len(self._schema_cache),
            'max_cache_size': self._max_cache_size,
            'cache_ttl_hours': self._cache_ttl_hours,
            'performance_config_tables': list(self._performance_keys_config.keys()),
            'statistics': {
                **self._cache_stats,
                'hit_rate_percent': round(hit_rate, 2),
                'total_requests': total_requests
            },
            'cache_details': cache_details
        }
    
    def set_cache_ttl(self, ttl_hours: int):
        """
        Set cache TTL and cleanup expired entries
        
        Args:
            ttl_hours: Time-to-live in hours for cache entries
        """
        old_ttl = self._cache_ttl_hours
        self._cache_ttl_hours = max(1, ttl_hours)  # Minimum 1 hour
        
        # Cleanup entries that are now expired with new TTL
        if self._cache_ttl_hours < old_ttl:
            self._cleanup_expired_cache_entries()
        
        logger.info(f"Updated cache TTL from {old_ttl}h to {self._cache_ttl_hours}h")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_requests = self._cache_stats['hits'] + self._cache_stats['misses']
        hit_rate = (self._cache_stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self._cache_stats,
            'hit_rate_percent': round(hit_rate, 2),
            'total_requests': total_requests,
            'cache_efficiency': 'excellent' if hit_rate > 80 else 'good' if hit_rate > 60 else 'needs_improvement'
        }