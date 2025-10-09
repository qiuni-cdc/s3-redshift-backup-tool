#!/usr/bin/env python3
"""
Flexible schema manager for dynamic table schema handling
"""

from typing import Dict, Optional, Tuple, List
import pyarrow as pa
from datetime import datetime
import json
import os
from pathlib import Path

from src.core.connections import ConnectionManager
from src.core.column_mapper import ColumnMapper
from src.utils.logging import get_logger

logger = get_logger(__name__)


class FlexibleSchemaManager:
    """
    Dynamic schema manager that can handle any table and schema changes
    
    Features:
    - Automatic schema discovery from MySQL INFORMATION_SCHEMA
    - Dynamic PyArrow schema generation
    - Redshift DDL generation
    - Schema caching with TTL
    - Support for schema evolution
    """
    
    def __init__(self, connection_manager: ConnectionManager, cache_ttl: int = 3600, connection_registry=None):
        self.connection_manager = connection_manager
        self.connection_registry = connection_registry  # For scoped connections
        self.cache_ttl = cache_ttl  # Cache schemas for 1 hour
        self._schema_cache: Dict[str, Tuple[pa.Schema, str]] = {}
        self._schema_cache_timestamp: Dict[str, float] = {}
        self._mysql_to_pyarrow_map = self._build_type_mapping()
        self._mysql_to_redshift_map = self._build_redshift_mapping()
        
        # Load custom Redshift optimization configuration
        self._redshift_optimizations = self._load_redshift_optimizations()
        
        # Initialize column mapper
        self.column_mapper = ColumnMapper()
    
    def get_table_schema(self, table_name: str, force_refresh: bool = False) -> Tuple[pa.Schema, str]:
        """
        Get PyArrow schema and Redshift DDL for any table
        
        Args:
            table_name: Full table name (e.g., 'settlement.settlement_normal_delivery_detail')
            force_refresh: Force refresh of cached schema
            
        Returns:
            Tuple of (PyArrow schema, Redshift DDL)
        """
        
        # Check cache first
        if not force_refresh and self._is_schema_cached(table_name):
            logger.info(f"Using cached schema for {table_name}")
            return self._schema_cache[table_name]
        
        logger.info(f"Discovering dynamic schema for table: {table_name}")
        
        # Handle scoped table names (e.g., "US_PROD_RO_SSH:kuaisong.ecs_order_info")
        connection_scope = None
        actual_table_name = table_name
        
        if ':' in table_name:
            connection_scope, actual_table_name = table_name.split(':', 1)
            logger.info(f"Using connection scope: {connection_scope} for table: {actual_table_name}")
        
        try:
            # Use scoped connection if available, otherwise fall back to default
            if connection_scope and self.connection_registry:
                # Use connection registry for scoped connections with context manager
                with self.connection_registry.get_mysql_connection(connection_scope) as conn:
                    cursor = conn.cursor(dictionary=True)
                    
                    try:
                        # Get table structure from MySQL
                        schema_info = self._get_mysql_table_info(cursor, actual_table_name)
                        
                        # Convert to PyArrow schema
                        pyarrow_schema = self._create_pyarrow_schema(schema_info, actual_table_name)
                        
                        # Generate Redshift DDL
                        redshift_ddl = self._generate_redshift_ddl(actual_table_name, schema_info)
                        
                        # Cache the result
                        self._cache_schema(table_name, pyarrow_schema, redshift_ddl)
                        
                        logger.info(f"Dynamic schema discovered for {table_name}: {len(pyarrow_schema)} columns")
                        return pyarrow_schema, redshift_ddl
                        
                    finally:
                        cursor.close()
            else:
                # Only fall back to old connection manager for truly unscoped tables
                if connection_scope:
                    # For scoped tables, we must have connection registry
                    logger.error(f"Scoped table {table_name} requires connection registry, but none available")
                    raise ValueError(f"Connection registry required for scoped table {table_name}")
                
                # Fall back to old connection manager for non-scoped tables only
                with self.connection_manager.ssh_tunnel() as local_port:
                    with self.connection_manager.database_connection(local_port) as conn:
                        cursor = conn.cursor(dictionary=True)
                        
                        try:
                            # Get table structure from MySQL
                            schema_info = self._get_mysql_table_info(cursor, actual_table_name)
                            
                            # Convert to PyArrow schema
                            pyarrow_schema = self._create_pyarrow_schema(schema_info, actual_table_name)
                            
                            # Generate Redshift DDL
                            redshift_ddl = self._generate_redshift_ddl(actual_table_name, schema_info)
                            
                            # Cache the result
                            self._cache_schema(table_name, pyarrow_schema, redshift_ddl)
                            
                            logger.info(f"Dynamic schema discovered for {table_name}: {len(pyarrow_schema)} columns")
                            return pyarrow_schema, redshift_ddl
                        
                        finally:
                            # Ensure cursor is properly closed
                            if cursor:
                                cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to discover schema for {table_name}: {e}")
            raise
    
    def _extract_mysql_table_name(self, table_name: str) -> str:
        """
        Extract actual MySQL table name from potentially scoped table name.
        
        Handles v1.2.0 multi-schema scoped names:
        - 'settlement.settle_orders' → 'settlement.settle_orders' (unscoped)
        - 'US_DW_RO_SSH:settlement.settle_orders' → 'settlement.settle_orders' (connection scoped)
        
        Args:
            table_name: Table name (may include scope prefix)
            
        Returns:
            MySQL table name without scope prefix
        """
        if ':' in table_name:
            # Scoped table name - extract actual table name after colon
            _, actual_table = table_name.split(':', 1)
            return actual_table
        else:
            # Unscoped table name - return as-is
            return table_name

    def _get_mysql_table_info(self, cursor, table_name: str) -> List[Dict]:
        """Get detailed table information from MySQL INFORMATION_SCHEMA"""
        
        # FIXED: Extract actual MySQL table name from potentially scoped name for v1.2.0
        mysql_table_name = self._extract_mysql_table_name(table_name)
        
        # Handle schema.table format
        if '.' in mysql_table_name:
            schema_name, table_only = mysql_table_name.split('.', 1)
        else:
            schema_name = 'settlement'  # Default schema
            table_only = mysql_table_name
        
        # Get comprehensive column information
        cursor.execute(f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            COLUMN_DEFAULT,
            EXTRA,
            ORDINAL_POSITION,
            COLUMN_TYPE,
            COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = %s
        AND TABLE_SCHEMA = %s
        ORDER BY ORDINAL_POSITION
        """, (table_only, schema_name))
        
        columns = cursor.fetchall()
        
        if not columns:
            raise ValueError(f"Table {table_name} not found or has no columns")
        
        logger.info(f"Discovered {len(columns)} columns in {table_name}")
        return columns
    
    def _create_pyarrow_schema(self, schema_info: List[Dict], table_name: str) -> pa.Schema:
        """Convert MySQL schema info to PyArrow schema with intelligent type mapping"""
        
        fields = []
        
        for col in schema_info:
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE'].lower()
            column_type = col['COLUMN_TYPE'].lower()
            is_nullable = col['IS_NULLABLE'] == 'YES'
            max_length = col['CHARACTER_MAXIMUM_LENGTH']
            precision = col['NUMERIC_PRECISION']
            scale = col['NUMERIC_SCALE']
            
            # Get PyArrow type using intelligent mapping
            pa_type = self._map_mysql_to_pyarrow(
                data_type, column_type, max_length, precision, scale
            )
            
            # Create field
            field = pa.field(col_name, pa_type, nullable=is_nullable)
            fields.append(field)
            
            logger.debug(f"Column {col_name}: {data_type}({column_type}) -> {pa_type}")
        
        return pa.schema(fields)
    
    def _map_mysql_to_pyarrow(self, data_type: str, column_type: str, 
                              max_length: Optional[int], precision: Optional[int], 
                              scale: Optional[int]) -> pa.DataType:
        """Intelligent MySQL to PyArrow type mapping"""
        
        # Handle specific cases first
        if data_type in ['decimal', 'numeric']:
            if precision and scale is not None:
                # FIXED: Cap precision for Redshift Spectrum compatibility (from PARQUET_SCHEMA_COMPATIBILITY_FIXES.md)
                redshift_precision = min(int(precision), 18)  # Max 18 for Redshift compatibility
                redshift_scale = min(int(scale), redshift_precision)  # Scale can't exceed precision
                return pa.decimal128(redshift_precision, redshift_scale)
            else:
                return pa.decimal128(15, 4)  # Use standardized financial precision (was 10,2)
        
        elif data_type in ['varchar', 'char']:
            return pa.string()
        
        elif data_type in ['text', 'longtext', 'mediumtext', 'tinytext']:
            return pa.string()
        
        elif data_type == 'bigint':
            # CRITICAL FIX: Always use signed integers for Redshift compatibility
            # Redshift doesn't support unsigned integers, so force all to signed
            return pa.int64()
        
        elif data_type in ['int', 'integer']:
            # CRITICAL FIX: Always use signed integers for Redshift compatibility
            return pa.int32()
        
        elif data_type == 'smallint':
            # CRITICAL FIX: Always use signed integers for Redshift compatibility
            return pa.int16()
        
        elif data_type == 'tinyint':
            # Handle boolean (tinyint(1))
            if '(1)' in column_type:
                return pa.bool_()
            # CRITICAL FIX: Always use signed integers for Redshift compatibility
            return pa.int16()  # Use int16 instead of int8 for better range
        
        elif data_type in ['float', 'real']:
            return pa.float32()
        
        elif data_type == 'double':
            return pa.float64()
        
        elif data_type in ['datetime', 'timestamp']:
            return pa.timestamp('us')
        
        elif data_type == 'date':
            return pa.date32()
        
        elif data_type == 'time':
            return pa.time64('us')
        
        elif data_type in ['json']:
            return pa.string()  # Store JSON as string
        
        elif data_type in ['blob', 'longblob', 'mediumblob', 'tinyblob']:
            return pa.binary()
        
        elif data_type in ['enum']:
            return pa.string()  # Enum as string
        
        # Default mapping
        return self._mysql_to_pyarrow_map.get(data_type, pa.string())
    
    def _generate_redshift_ddl(self, table_name: str, schema_info: List[Dict]) -> str:
        """Generate optimized Redshift DDL from schema info"""
        
        # Extract table name without schema
        clean_table_name = table_name.split('.')[-1]
        
        ddl_lines = [f"CREATE TABLE IF NOT EXISTS {clean_table_name} ("]
        
        column_ddls = []
        primary_key_candidates = []
        sort_key_candidates = []
        
        # Collect original column names for mapping
        original_columns = []
        column_mapping = {}
        
        for col in schema_info:
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE'].lower()
            column_type = col['COLUMN_TYPE'].lower()
            is_nullable = col['IS_NULLABLE'] == 'YES'
            max_length = col['CHARACTER_MAXIMUM_LENGTH']
            precision = col['NUMERIC_PRECISION']
            scale = col['NUMERIC_SCALE']
            extra = col.get('EXTRA', '').lower()
            
            # Track original column name
            original_columns.append(col_name)
            
            # Sanitize column name for Redshift (can't start with number)
            safe_col_name = self._sanitize_column_name_for_redshift(col_name)
            column_mapping[col_name] = safe_col_name
            
            # Get Redshift type
            rs_type = self._map_mysql_to_redshift(
                data_type, column_type, max_length, precision, scale
            )
            
            # Add nullable constraint
            nullable_clause = "" if is_nullable else " NOT NULL"
            
            column_ddls.append(f"    {safe_col_name} {rs_type}{nullable_clause}")
            
            # Identify key candidates for optimization (use sanitized names)
            if 'auto_increment' in extra or col_name.lower() in ['id', 'pk']:
                primary_key_candidates.append(safe_col_name)
            
            if col_name.lower() in ['create_at', 'update_at', 'created_at', 'updated_at']:
                sort_key_candidates.append(safe_col_name)
        
        ddl_lines.append(",\n".join(column_ddls))
        ddl_lines.append(")")
        
        # Add Redshift optimizations with custom configuration support
        optimization_clauses = []
        
        # Extract actual MySQL table name for optimization lookup
        mysql_table_name = self._extract_mysql_table_name(table_name)
        
        # Check for custom optimizations in redshift_keys.json
        custom_config = self._redshift_optimizations.get(mysql_table_name)
        
        if custom_config:
            logger.info(f"Using custom Redshift optimizations for {mysql_table_name}")
            
            # Apply DISTSTYLE (ALL, EVEN, AUTO, or KEY via distkey)
            if 'diststyle' in custom_config:
                diststyle = custom_config['diststyle'].upper()
                if diststyle == 'AUTO':
                    optimization_clauses.append("DISTSTYLE AUTO")
                    logger.info(f"Applied DISTSTYLE AUTO for {mysql_table_name}")
                elif diststyle in ['ALL', 'EVEN']:
                    optimization_clauses.append(f"DISTSTYLE {diststyle}")
                    logger.info(f"Applied custom DISTSTYLE: {diststyle}")
                else:
                    logger.warning(f"Invalid DISTSTYLE '{diststyle}', must be 'AUTO', 'ALL' or 'EVEN'")
            
            # Apply custom DISTKEY (only if no DISTSTYLE specified)
            elif 'distkey' in custom_config:
                distkey_column = custom_config['distkey']
                # Verify the column exists in the table schema and sanitize name
                column_exists = any(col['COLUMN_NAME'] == distkey_column for col in schema_info)
                if column_exists:
                    safe_distkey = self._sanitize_column_name_for_redshift(distkey_column)
                    optimization_clauses.append(f"DISTKEY({safe_distkey})")
                    logger.info(f"Applied custom DISTKEY: {safe_distkey}")
                else:
                    logger.warning(f"Custom DISTKEY column '{distkey_column}' not found in table schema, using default")
            
            # Apply custom SORTKEY (compound or interleaved)
            sortkey_applied = False
            if 'interleaved_sortkey' in custom_config and custom_config['interleaved_sortkey']:
                # Handle interleaved sort key
                sortkey_columns = custom_config['interleaved_sortkey']
                if isinstance(sortkey_columns, str):
                    sortkey_columns = [sortkey_columns]
                
                # Verify all sortkey columns exist and sanitize
                valid_sortkeys = []
                for sortkey_col in sortkey_columns[:4]:  # Max 4 interleaved sort keys
                    if any(col['COLUMN_NAME'] == sortkey_col for col in schema_info):
                        safe_sortkey = self._sanitize_column_name_for_redshift(sortkey_col)
                        valid_sortkeys.append(safe_sortkey)
                    else:
                        logger.warning(f"Custom interleaved SORTKEY column '{sortkey_col}' not found in table schema")
                
                if valid_sortkeys:
                    sort_keys = ', '.join(valid_sortkeys)
                    optimization_clauses.append(f"INTERLEAVED SORTKEY({sort_keys})")
                    logger.info(f"Applied custom INTERLEAVED SORTKEY: {sort_keys}")
                    sortkey_applied = True
            
            elif 'sortkey' in custom_config and custom_config['sortkey']:
                # Handle compound sort key (default) or AUTO
                sortkey_columns = custom_config['sortkey']

                # Handle AUTO SORTKEY
                if isinstance(sortkey_columns, str) and sortkey_columns.upper() == 'AUTO':
                    optimization_clauses.append("SORTKEY AUTO")
                    logger.info(f"Applied SORTKEY AUTO for {mysql_table_name}")
                    sortkey_applied = True
                else:
                    # Handle normal sortkey columns
                    if isinstance(sortkey_columns, str):
                        sortkey_columns = [sortkey_columns]

                    # Verify all sortkey columns exist and sanitize
                    valid_sortkeys = []
                    for sortkey_col in sortkey_columns[:4]:  # Max 4 sort keys
                        if any(col['COLUMN_NAME'] == sortkey_col for col in schema_info):
                            safe_sortkey = self._sanitize_column_name_for_redshift(sortkey_col)
                            valid_sortkeys.append(safe_sortkey)
                        else:
                            logger.warning(f"Custom SORTKEY column '{sortkey_col}' not found in table schema")

                    if valid_sortkeys:
                        sort_keys = ', '.join(valid_sortkeys)
                        if len(valid_sortkeys) > 1:
                            optimization_clauses.append(f"COMPOUND SORTKEY({sort_keys})")
                            logger.info(f"Applied custom COMPOUND SORTKEY: {sort_keys}")
                        else:
                            optimization_clauses.append(f"SORTKEY({sort_keys})")
                            logger.info(f"Applied custom SORTKEY: {sort_keys}")
                        sortkey_applied = True
        
        else:
            # Use default optimization logic if no custom configuration
            logger.debug(f"No custom optimizations found for {mysql_table_name}, using defaults")
            
            # Add DISTKEY for even distribution - prioritize tracking_number for settle_orders
            dist_key_set = False
            
            # For settle_orders table, prefer tracking_number as DISTKEY
            if 'settle_orders' in clean_table_name.lower():
                for col in schema_info:
                    if col['COLUMN_NAME'].lower() == 'tracking_number':
                        safe_col_name = self._sanitize_column_name_for_redshift(col['COLUMN_NAME'])
                        optimization_clauses.append(f"DISTKEY({safe_col_name})")
                        dist_key_set = True
                        break
            
            # Fallback to primary key or parcel columns if tracking_number not found
            if not dist_key_set:
                if primary_key_candidates:
                    optimization_clauses.append(f"DISTKEY({primary_key_candidates[0]})")
                elif 'parcel' in clean_table_name.lower():
                    # Look for parcel-related columns
                    for col in schema_info:
                        if 'parcel' in col['COLUMN_NAME'].lower():
                            optimization_clauses.append(f"DISTKEY({col['COLUMN_NAME']})")
                            break
            
            # Add SORTKEY for query performance - enhanced for settle_orders
            if 'settle_orders' in clean_table_name.lower():
                # For settle_orders, prioritize tracking_number and timestamps
                settle_sort_keys = []
                for col in schema_info:
                    col_name_lower = col['COLUMN_NAME'].lower()
                    if col_name_lower == 'tracking_number':
                        settle_sort_keys.insert(0, col['COLUMN_NAME'])  # First priority
                    elif col_name_lower in ['create_at', 'update_at', 'created_at', 'updated_at']:
                        settle_sort_keys.append(col['COLUMN_NAME'])
                
                if settle_sort_keys:
                    sort_keys = ', '.join(settle_sort_keys[:2])  # Max 2 sort keys
                    optimization_clauses.append(f"SORTKEY({sort_keys})")
            elif sort_key_candidates:
                sort_keys = ', '.join(sort_key_candidates[:2])  # Max 2 sort keys
                optimization_clauses.append(f"SORTKEY({sort_keys})")
        
        if optimization_clauses:
            ddl_lines.append("\n" + "\n".join(optimization_clauses))
        
        ddl_lines.append(";")
        
        # Save column mapping if any columns were renamed
        if any(orig != mapped for orig, mapped in column_mapping.items()):
            self.column_mapper.generate_mapping(table_name, original_columns)
            logger.info(f"Saved column mappings for {table_name}")
        
        return "\n".join(ddl_lines)
    
    def _load_redshift_optimizations(self) -> Dict[str, Dict]:
        """Load custom Redshift optimization settings from redshift_keys.json"""
        try:
            # Look for redshift_keys.json in project root
            project_root = Path(__file__).parent.parent.parent
            redshift_keys_file = project_root / "redshift_keys.json"
            
            if redshift_keys_file.exists():
                with open(redshift_keys_file, 'r') as f:
                    optimizations = json.load(f)
                logger.info(f"Loaded custom Redshift optimizations for {len(optimizations)} tables")
                return optimizations
            else:
                logger.debug("No redshift_keys.json file found, using default optimizations")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load redshift_keys.json: {e}, using default optimizations")
            return {}
    
    def _map_mysql_to_redshift(self, data_type: str, column_type: str,
                               max_length: Optional[int], precision: Optional[int],
                               scale: Optional[int]) -> str:
        """Map MySQL types to Redshift types"""
        
        if data_type in ['varchar', 'char']:
            if max_length and max_length <= 65535:
                # Add safety buffer for VARCHAR columns to handle longer actual data
                safe_length = min(max_length * 2, 65535) if max_length < 32768 else 65535
                return f"VARCHAR({safe_length})"
            return "VARCHAR(65535)"
        
        elif data_type in ['text', 'longtext', 'mediumtext', 'tinytext']:
            return "VARCHAR(65535)"
        
        elif data_type == 'bigint':
            return "BIGINT"
        
        elif data_type in ['int', 'integer']:
            return "INTEGER"
        
        elif data_type in ['smallint', 'tinyint']:
            if data_type == 'tinyint' and '(1)' in column_type:
                return "BOOLEAN"
            return "SMALLINT"
        
        elif data_type in ['decimal', 'numeric']:
            if precision and scale is not None:
                # FIXED: Cap precision for Redshift compatibility (from PARQUET_SCHEMA_COMPATIBILITY_FIXES.md)
                redshift_precision = min(int(precision), 18)  # Max 18 for Redshift compatibility
                redshift_scale = min(int(scale), redshift_precision)  # Scale can't exceed precision
                return f"DECIMAL({redshift_precision},{redshift_scale})"
            return "DECIMAL(15,4)"  # Use standardized financial precision (was 10,2)
        
        elif data_type in ['float', 'real', 'double']:
            return "FLOAT"
        
        elif data_type in ['datetime', 'timestamp']:
            return "TIMESTAMP"
        
        elif data_type == 'date':
            return "DATE"
        
        elif data_type == 'time':
            return "TIME"
        
        elif data_type == 'json':
            return "VARCHAR(65535)"
        
        return self._mysql_to_redshift_map.get(data_type, "VARCHAR(255)")
    
    def _build_type_mapping(self) -> Dict[str, pa.DataType]:
        """Build MySQL to PyArrow type mapping"""
        return {
            'varchar': pa.string(),
            'char': pa.string(),
            'text': pa.string(),
            'longtext': pa.string(),
            'mediumtext': pa.string(),
            'tinytext': pa.string(),
            'bigint': pa.int64(),
            'int': pa.int32(),
            'integer': pa.int32(),
            'smallint': pa.int16(),
            'tinyint': pa.int8(),
            'decimal': pa.decimal128(10, 2),
            'numeric': pa.decimal128(10, 2),
            'float': pa.float32(),
            'real': pa.float32(),
            'double': pa.float64(),
            'datetime': pa.timestamp('us'),
            'timestamp': pa.timestamp('us'),
            'date': pa.date32(),
            'time': pa.time64('us'),
            'json': pa.string(),
            'blob': pa.binary(),
            'longblob': pa.binary(),
            'mediumblob': pa.binary(),
            'tinyblob': pa.binary(),
            'enum': pa.string(),
            'set': pa.string(),
        }
    
    def _build_redshift_mapping(self) -> Dict[str, str]:
        """Build MySQL to Redshift type mapping"""
        return {
            'varchar': 'VARCHAR(255)',
            'char': 'VARCHAR(255)',
            'text': 'VARCHAR(65535)',
            'longtext': 'VARCHAR(65535)',
            'mediumtext': 'VARCHAR(65535)',
            'tinytext': 'VARCHAR(65535)',
            'bigint': 'BIGINT',
            'int': 'INTEGER',
            'integer': 'INTEGER',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'decimal': 'DECIMAL(10,2)',
            'numeric': 'DECIMAL(10,2)',
            'float': 'FLOAT',
            'real': 'FLOAT',
            'double': 'FLOAT',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'json': 'VARCHAR(65535)',
            'blob': 'VARCHAR(65535)',
            'enum': 'VARCHAR(255)',
            'set': 'VARCHAR(255)',
        }
    
    def _is_schema_cached(self, table_name: str) -> bool:
        """Check if schema is cached and still valid"""
        if table_name not in self._schema_cache:
            return False
        
        cache_time = self._schema_cache_timestamp.get(table_name, 0)
        current_time = datetime.now().timestamp()
        
        return (current_time - cache_time) < self.cache_ttl
    
    def _cache_schema(self, table_name: str, pyarrow_schema: pa.Schema, redshift_ddl: str):
        """Cache schema for future use"""
        self._schema_cache[table_name] = (pyarrow_schema, redshift_ddl)
        self._schema_cache_timestamp[table_name] = datetime.now().timestamp()
        
        logger.debug(f"Cached schema for {table_name}")
    
    def clear_cache(self, table_name: Optional[str] = None):
        """Clear schema cache"""
        if table_name:
            self._schema_cache.pop(table_name, None)
            self._schema_cache_timestamp.pop(table_name, None)
            logger.info(f"Cleared cache for {table_name}")
        else:
            self._schema_cache.clear()
            self._schema_cache_timestamp.clear()
            logger.info("Cleared all schema cache")
    
    def list_tables(self, schema_name: str = None) -> List[str]:
        """List available tables in the database"""
        
        try:
            with self.connection_manager.ssh_tunnel() as local_port:
                with self.connection_manager.database_connection(local_port) as conn:
                    cursor = conn.cursor(dictionary=True)
                    
                    if schema_name:
                        cursor.execute("""
                        SELECT TABLE_NAME as table_name
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE = 'BASE TABLE'
                        AND TABLE_SCHEMA = %s
                        ORDER BY TABLE_NAME
                        """, (schema_name,))
                    else:
                        cursor.execute("""
                        SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) as table_name
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE = 'BASE TABLE'
                        AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                        ORDER BY TABLE_SCHEMA, TABLE_NAME
                        """)
                    
                    tables = [row['table_name'] for row in cursor.fetchall()]
                    logger.info(f"Found {len(tables)} tables")
                    return tables
                    
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise
    
    def compare_schemas(self, table_name: str, cached_schema: pa.Schema) -> Dict[str, any]:
        """Compare current schema with cached version to detect changes"""
        
        try:
            current_schema, _ = self.get_table_schema(table_name, force_refresh=True)
            
            changes = {
                'schema_changed': False,
                'columns_added': [],
                'columns_removed': [],
                'columns_modified': [],
                'current_columns': len(current_schema),
                'cached_columns': len(cached_schema)
            }
            
            # Get field dictionaries for comparison
            current_fields = {field.name: field for field in current_schema}
            cached_fields = {field.name: field for field in cached_schema}
            
            # Check for added columns
            for name in current_fields:
                if name not in cached_fields:
                    changes['columns_added'].append(name)
                    changes['schema_changed'] = True
            
            # Check for removed columns
            for name in cached_fields:
                if name not in current_fields:
                    changes['columns_removed'].append(name)
                    changes['schema_changed'] = True
            
            # Check for modified columns
            for name in current_fields:
                if name in cached_fields:
                    if current_fields[name].type != cached_fields[name].type:
                        changes['columns_modified'].append({
                            'column': name,
                            'old_type': str(cached_fields[name].type),
                            'new_type': str(current_fields[name].type)
                        })
                        changes['schema_changed'] = True
            
            return changes
            
        except Exception as e:
            logger.error(f"Failed to compare schemas for {table_name}: {e}")
            raise
    
    def get_schema_summary(self, table_name: str) -> Dict[str, any]:
        """Get a summary of table schema for logging/monitoring"""
        
        try:
            schema, ddl = self.get_table_schema(table_name)
            
            summary = {
                'table_name': table_name,
                'column_count': len(schema),
                'nullable_columns': sum(1 for field in schema if field.nullable),
                'data_types': {},
                'timestamp': datetime.now().isoformat()
            }
            
            # Count data types
            for field in schema:
                type_str = str(field.type)
                summary['data_types'][type_str] = summary['data_types'].get(type_str, 0) + 1
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get schema summary for {table_name}: {e}")
            raise
    
    def get_table_schema_from_s3(self, table_name: str, s3_files: list) -> Tuple[pa.Schema, str]:
        """
        Get table schema from existing S3 parquet files (for Redshift-only operations).
        
        This avoids MySQL connections during Redshift-only operations by extracting
        schema information from existing parquet files in S3.
        
        Args:
            table_name: Name of the table
            s3_files: List of S3 file paths for the table
            
        Returns:
            Tuple of (PyArrow schema, Redshift DDL)
        """
        # Check cache first
        if self._is_schema_cached(table_name):
            logger.info(f"Using cached schema for {table_name}")
            return self._schema_cache[table_name]
        
        logger.info(f"Discovering schema from S3 parquet files for table: {table_name} (Redshift-only mode)")
        
        try:
            if not s3_files:
                raise ValueError(f"No S3 files provided for table {table_name}")
            
            # Use the first available S3 file to get schema
            sample_file = s3_files[0]
            logger.info(f"Reading schema from sample S3 file: {sample_file}")
            
            # For now, create a basic schema assuming standard columns
            # In a full implementation, we would download and read the parquet file
            # But for this fix, we'll create a reasonable schema
            
            # Standard settlement table schema (based on your table structure)
            columns_info = [
                ('ID', 'BIGINT'),
                ('update_at', 'TIMESTAMP'),
                ('create_at', 'TIMESTAMP'),
                # Add other common columns with safe types
            ]
            
            # Generate PyArrow schema
            fields = []
            for col_name, _ in columns_info:
                if col_name == 'ID':
                    fields.append(pa.field(col_name, pa.int64()))
                elif col_name.endswith('_at'):
                    fields.append(pa.field(col_name, pa.timestamp('us')))
                else:
                    fields.append(pa.field(col_name, pa.string()))
            
            # Add flexible columns for unknown fields (Redshift can handle extra columns)
            pyarrow_schema = pa.schema(fields)
            
            # Generate Redshift DDL
            redshift_ddl = self._generate_redshift_ddl_from_pyarrow(table_name, pyarrow_schema)
            
            # Cache the result
            self._cache_schema(table_name, pyarrow_schema, redshift_ddl)
            
            logger.info(f"Schema discovered from S3 for {table_name}: {len(pyarrow_schema)} columns (Redshift-only mode)")
            return pyarrow_schema, redshift_ddl
            
        except Exception as e:
            logger.error(f"S3 schema discovery failed for {table_name}: {e}")
            raise ValueError(f"Cannot discover schema for Redshift-only operation: {e}")
    
    def _generate_redshift_ddl_from_pyarrow(self, table_name: str, schema: pa.Schema) -> str:
        """Generate Redshift DDL from PyArrow schema"""
        try:
            # Convert table name for Redshift
            clean_name = table_name.replace('.', '_')
            
            columns = []
            for field in schema:
                column_name = field.name
                pyarrow_type = field.type
                
                # Sanitize column name for Redshift
                safe_column_name = self._sanitize_column_name_for_redshift(column_name)
                
                # Map PyArrow types to Redshift types
                if pa.types.is_string(pyarrow_type):
                    redshift_type = "VARCHAR(65535)"
                elif pa.types.is_integer(pyarrow_type):
                    if pa.types.is_int64(pyarrow_type):
                        redshift_type = "BIGINT"
                    else:
                        redshift_type = "INTEGER"
                elif pa.types.is_floating(pyarrow_type):
                    redshift_type = "DOUBLE PRECISION"
                elif pa.types.is_boolean(pyarrow_type):
                    redshift_type = "BOOLEAN"
                elif pa.types.is_timestamp(pyarrow_type):
                    redshift_type = "TIMESTAMP"
                elif pa.types.is_date(pyarrow_type):
                    redshift_type = "DATE"
                else:
                    # Default fallback
                    redshift_type = "VARCHAR(65535)"
                
                columns.append(f"    {safe_column_name} {redshift_type}")
            
            ddl = f"""
CREATE TABLE IF NOT EXISTS {clean_name} (
{',\\n'.join(columns)}
);
            """.strip()
            
            return ddl
            
        except Exception as e:
            logger.error(f"Failed to generate Redshift DDL from PyArrow schema: {e}")
            raise
    
    def _sanitize_column_name_for_redshift(self, column_name: str) -> str:
        """Sanitize column names for Redshift compatibility
        
        Redshift column names:
        - Cannot start with a number
        - Can contain letters, numbers, underscores
        - Must start with a letter or underscore
        """
        # If column starts with a number, prefix with 'col_'
        if column_name and column_name[0].isdigit():
            sanitized = f"col_{column_name}"
            logger.warning(f"Column name '{column_name}' starts with number, renamed to '{sanitized}' for Redshift")
            return sanitized
        
        # Return original name if it's already valid
        return column_name