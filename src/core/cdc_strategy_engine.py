"""
CDC Strategy Engine for v1.2.0 - Flexible Change Data Capture

This module implements the core CDC Strategy Engine that enables flexible 
change data capture beyond the hardcoded 'updated_at' approach used in v1.0.0/v1.1.0.

Key Features:
- Multiple CDC strategies: timestamp_only, hybrid, id_only, full_sync, custom_sql
- Dynamic query building based on table configuration
- Flexible watermark extraction and management
- 100% backward compatibility with v1.0.0/v1.1.0 behavior
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging
import re
from enum import Enum

logger = logging.getLogger(__name__)


# Security validation functions
def _validate_sql_identifier(identifier: str) -> bool:
    """
    Validate SQL identifier (table/column name) is safe
    
    Only allows alphanumeric characters, underscores, and dots (for schema.table)
    """
    if not identifier or not isinstance(identifier, str):
        return False
    return re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', identifier) is not None


def _sanitize_sql_value(value: Any) -> str:
    """
    Sanitize SQL value to prevent injection
    
    Basic escaping - for production should use proper parameterized queries
    """
    if value is None:
        return 'NULL'
    
    if isinstance(value, (int, float)):
        return str(value)
    
    # String values - escape single quotes
    str_value = str(value)
    return "'" + str_value.replace("'", "''") + "'"


def _validate_custom_sql_security(custom_query: str) -> bool:
    """
    Validate custom SQL query for basic security
    
    This is a basic security check. For production, use a proper SQL parser.
    """
    if not custom_query or not isinstance(custom_query, str):
        return False
    
    query_lower = custom_query.lower().strip()
    
    # Must be a SELECT statement
    if not query_lower.startswith('select'):
        return False
    
    # Block dangerous SQL keywords
    dangerous_keywords = [
        'drop', 'delete', 'insert', 'update', 'alter', 'create',
        'truncate', 'grant', 'revoke', 'exec', 'execute',
        'xp_', 'sp_', 'union', '--', '/*', '*/', 'script',
        'javascript', 'vbscript', 'onload', 'onerror'
    ]
    
    for keyword in dangerous_keywords:
        if keyword in query_lower:
            return False
    
    # Block multiple statements (basic check)
    if ';' in query_lower and not query_lower.endswith(';'):
        return False
    
    # Must contain expected template variables
    required_templates = ['{table_name}', '{limit}']
    for template in required_templates:
        if template not in custom_query:
            return False
    
    return True


class CDCSecurityError(Exception):
    """Raised when CDC operation fails security validation"""
    pass


class CDCStrategyType(Enum):
    """Supported CDC strategy types"""
    TIMESTAMP_ONLY = "timestamp_only"      # v1.0.0 compatibility
    HYBRID = "hybrid"                      # timestamp + ID (most robust)
    ID_ONLY = "id_only"                    # append-only tables
    FULL_SYNC = "full_sync"                # complete refresh
    CUSTOM_SQL = "custom_sql"              # user-defined queries
    JOIN_DRIVEN = "join_driven"            # driven by another table's CDC column


@dataclass
class CDCConfig:
    """CDC configuration for a table"""
    strategy: CDCStrategyType
    timestamp_column: Optional[str] = None
    id_column: Optional[str] = None
    ordering_columns: Optional[List[str]] = None
    custom_query: Optional[str] = None
    batch_size: int = 50000
    timestamp_format: Optional[str] = None  # "datetime", "unix", "auto"
    additional_where: Optional[str] = None  # Additional WHERE clause snippet
    # join_driven strategy fields
    join_table: Optional[str] = None   # Reference table to join (e.g. "kuaisong.uni_tracking_info")
    join_column: Optional[str] = None  # CDC column from the join table (e.g. "update_time")
    join_key: Optional[str] = None     # Join key column in both tables (default: "order_id")
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Validate CDC configuration"""
        if self.strategy == CDCStrategyType.TIMESTAMP_ONLY and not self.timestamp_column:
            raise ValueError("timestamp_only strategy requires timestamp_column")
        if self.strategy == CDCStrategyType.HYBRID and not (self.timestamp_column and self.id_column):
            raise ValueError("hybrid strategy requires both timestamp_column and id_column")
        if self.strategy == CDCStrategyType.ID_ONLY and not self.id_column:
            raise ValueError("id_only strategy requires id_column")
        if self.strategy == CDCStrategyType.CUSTOM_SQL and not self.custom_query:
            raise ValueError("custom_sql strategy requires custom_query")
        if self.strategy == CDCStrategyType.JOIN_DRIVEN and not (self.join_table and self.join_column):
            raise ValueError("join_driven strategy requires join_table and join_column")


@dataclass 
class WatermarkData:
    """Watermark data extracted from CDC processing"""
    last_timestamp: Optional[datetime] = None
    last_id: Optional[int] = None
    row_count: int = 0
    strategy_used: Optional[str] = None
    additional_data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.additional_data is None:
            self.additional_data = {}


class CDCStrategy(ABC):
    """Abstract base class for CDC strategies"""
    
    def __init__(self, config: CDCConfig):
        self.config = config
        self.strategy_name = config.strategy.value
        
    @abstractmethod
    def build_query(self, table_name: str, watermark: Dict[str, Any], limit: int, table_schema: Optional[Dict[str, str]] = None, end_time: Optional[str] = None) -> str:
        """Build SQL query for incremental data extraction"""
        pass
    
    @abstractmethod
    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """Extract watermark information from processed rows"""
        pass
    
    @abstractmethod
    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        """Validate that table schema supports this CDC strategy"""
        pass
    
    def get_ordering_clause(self) -> str:
        """Get ORDER BY clause for consistent ordering"""
        if self.config.ordering_columns:
            return f"ORDER BY {', '.join(self.config.ordering_columns)}"
        return ""
    
    def format_timestamp(self, timestamp: datetime) -> str:
        """Format timestamp for SQL queries"""
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')


class TimestampOnlyCDCStrategy(CDCStrategy):
    """
    Timestamp-only CDC strategy (v1.0.0/v1.1.0 compatibility)
    
    Uses a single timestamp column for change detection.
    Maintains compatibility with existing 'updated_at' based systems.
    """
    
    def build_query(self, table_name: str, watermark: Dict[str, Any], limit: int, table_schema: Optional[Dict[str, str]] = None, end_time: Optional[str] = None) -> str:
        """Build timestamp-based incremental query"""
        # SECURITY: Validate table name and column names
        if not _validate_sql_identifier(table_name):
            raise CDCSecurityError(f"Invalid table name: {table_name}")

        timestamp_col = self.config.timestamp_column
        if not _validate_sql_identifier(timestamp_col):
            raise CDCSecurityError(f"Invalid timestamp column: {timestamp_col}")

        # Extract watermark data with manual ID override support
        # Check for manual ID override (consistent with other CDC strategies)
        metadata = watermark.get('metadata', {})
        cdc_config = metadata.get('cdc_config', {})

        # For timestamp-only strategy, manual ID can provide starting point
        manual_id_override = None
        if cdc_config.get('manual_id_set') and 'id_start_override' in cdc_config:
            manual_id_override = cdc_config['id_start_override']
            logger.info(f"Manual ID override detected for timestamp-only strategy: {manual_id_override}")

        last_timestamp = watermark.get('last_mysql_data_timestamp')

        # Determine is_unix_timestamp upfront — needed for both lower and upper bound
        col_type = ''
        if self.config.timestamp_format == 'unix':
            is_unix_timestamp = True
            logger.info(f"Using UNIX timestamp (configured): {timestamp_col}")
        elif self.config.timestamp_format == 'datetime':
            is_unix_timestamp = False
            logger.info(f"Using datetime format (configured): {timestamp_col}")
        else:  # 'auto' or None
            if table_schema and timestamp_col in table_schema:
                col_type = table_schema[timestamp_col].lower()
            is_unix_timestamp = any(t in col_type for t in ['int', 'bigint', 'tinyint', 'smallint', 'mediumint'])
            if is_unix_timestamp:
                logger.info(f"Using UNIX timestamp conversion for {timestamp_col} (type: {col_type})")
            else:
                logger.info(f"Using standard datetime for {timestamp_col} (type: {col_type})")

        if manual_id_override is not None:
            # Manual ID override takes precedence - use ID-based filtering
            # SECURITY: Validate ID override
            if not isinstance(manual_id_override, (int, float)) or manual_id_override < 0:
                raise CDCSecurityError(f"Invalid manual ID override: {manual_id_override}")
            safe_id_override = _sanitize_sql_value(manual_id_override)
            where_clause = f"WHERE id > {safe_id_override}"
            logger.info(f"Using manual ID override for timestamp-only strategy: WHERE id > {safe_id_override}")
        elif last_timestamp:
            # For unix integer columns, convert datetime strings to unix epoch so MySQL
            # doesn't silently truncate '2026-02-27 04:15:17' → 2026 via implicit cast.
            if is_unix_timestamp:
                ts_str = str(last_timestamp).strip()
                if any(c in ts_str for c in ['-', 'T', '/', ' ']):
                    # Datetime string — convert to unix epoch (mirrors upper-bound logic)
                    try:
                        from datetime import datetime as _dt
                        clean_ts = ts_str.replace('T', ' ').split('.')[0].split('+')[0].rstrip('Z').strip()
                        lower_unix = int(_dt.strptime(clean_ts, '%Y-%m-%d %H:%M:%S').timestamp())
                        base_where = f"{timestamp_col} > {lower_unix}"
                        logger.info(f"Converted lower bound datetime string to unix: {ts_str} → {lower_unix}")
                    except Exception as e:
                        logger.warning(f"Failed to convert lower bound to unix: {e}. Using sanitized string.")
                        base_where = f"{timestamp_col} > {_sanitize_sql_value(last_timestamp)}"
                else:
                    # Already numeric (unix integer or its string representation)
                    base_where = f"{timestamp_col} > {_sanitize_sql_value(last_timestamp)}"
            else:
                # SECURITY: Sanitize timestamp value
                base_where = f"{timestamp_col} > {_sanitize_sql_value(last_timestamp)}"

            # Add additional WHERE clause if specified (for index optimization)
            if self.config.additional_where:
                where_clause = f"WHERE {self.config.additional_where} AND {base_where}"
                logger.info(f"Using additional WHERE clause: {self.config.additional_where}")
            else:
                where_clause = f"WHERE {base_where}"
                logger.info(f"No additional WHERE clause configured for this table")
        else:
            # First run - no watermark
            where_clause = ""

        # Apply end_time upper bound (enforces 5-min buffer; also bounds first-run queries)
        if end_time:
            try:
                clean_end = end_time.replace('T', ' ').split('.')[0].split('+')[0]
                if is_unix_timestamp:
                    from datetime import datetime as _dt
                    end_unix = int(_dt.strptime(clean_end, '%Y-%m-%d %H:%M:%S').timestamp())
                    upper_cond = f"{timestamp_col} < {end_unix}"
                else:
                    safe_end = _sanitize_sql_value(clean_end)
                    upper_cond = f"{timestamp_col} < {safe_end}"

                if where_clause:
                    where_clause = where_clause + f" AND {upper_cond}"
                else:
                    where_clause = f"WHERE {upper_cond}"
                logger.info(f"Applied end_time upper bound: {upper_cond}")
            except Exception as e:
                logger.warning(f"Failed to apply end_time upper bound: {e}. Query will run without upper bound.")

        # Build ordering - validate ordering columns
        if self.config.ordering_columns:
            # SECURITY: Validate all ordering columns
            safe_ordering_cols = []
            for col in self.config.ordering_columns:
                if not _validate_sql_identifier(col):
                    raise CDCSecurityError(f"Invalid ordering column: {col}")
                safe_ordering_cols.append(col)
            order_clause = f"ORDER BY {', '.join(safe_ordering_cols)}"
        else:
            order_clause = f"ORDER BY {timestamp_col}"

        # SECURITY: Validate limit is positive integer
        if not isinstance(limit, int) or limit <= 0:
            raise CDCSecurityError(f"Invalid limit value: {limit}")

        query = f"""
        SELECT * FROM {table_name}
        {where_clause}
        {order_clause}
        LIMIT {limit}
        """.strip()

        # Enhanced logging: Output complete query for debugging
        logger.info(f"TimestampOnly CDC query built", extra={
            "table": table_name,
            "timestamp_column": timestamp_col,
            "last_timestamp": last_timestamp,
            "end_time": end_time,
            "limit": limit,
            "is_unix_timestamp": is_unix_timestamp,
            "where_clause": where_clause,
            "complete_query": query  # Full query for debugging
        })

        # Also log to console for immediate visibility
        print(f"[CDC DEBUG] Executing query for {table_name}:")
        print(f"[CDC DEBUG] Query: {query}")
        print(f"[CDC DEBUG] Watermark: last_timestamp={last_timestamp}, end_time={end_time}, is_unix={is_unix_timestamp}")

        return query
    
    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """Extract watermark data from timestamp-only processing"""
        if not rows:
            return WatermarkData(strategy_used=self.strategy_name)
        
        # Get last row's timestamp
        last_row = rows[-1]
        timestamp_col = self.config.timestamp_column
        raw_timestamp = last_row.get(timestamp_col)
        
        # Convert UNIX timestamp to datetime string based on configuration
        processed_timestamp = raw_timestamp
        should_convert_unix = False
        
        # Determine if we should convert UNIX timestamp based on configuration
        if self.config.timestamp_format == 'unix':
            should_convert_unix = True
        elif self.config.timestamp_format == 'datetime':
            should_convert_unix = False
        elif self.config.timestamp_format == 'auto' or self.config.timestamp_format is None:
            # Auto-detect: if it's a numeric value, likely UNIX timestamp
            should_convert_unix = isinstance(raw_timestamp, (int, float))
        
        if should_convert_unix and raw_timestamp is not None and isinstance(raw_timestamp, (int, float)):
            # Subtract 1s before storing: next cycle queries WHERE ts > (T-1) which is
            # equivalent to >= T, catching rows committed at the exact watermark second
            # after our query completed (boundary-second rows are never permanently missed).
            adjusted_ts = int(raw_timestamp) - 1
            try:
                from datetime import datetime
                dt = datetime.fromtimestamp(adjusted_ts)
                processed_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                logger.debug(f"Converted UNIX timestamp {raw_timestamp} (adj -1s -> {adjusted_ts}) to {processed_timestamp} (format: {self.config.timestamp_format})")
            except (ValueError, OSError) as e:
                logger.warning(f"Failed to convert UNIX timestamp {raw_timestamp}: {e}")
                # Keep raw value if conversion fails
                processed_timestamp = raw_timestamp
        
        return WatermarkData(
            last_timestamp=processed_timestamp,
            row_count=len(rows),
            strategy_used=self.strategy_name,
            additional_data={'timestamp_column': timestamp_col}
        )
    
    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        """Validate table has required timestamp column"""
        timestamp_col = self.config.timestamp_column
        
        if timestamp_col not in table_schema:
            logger.error(f"Table missing required timestamp column: {timestamp_col}")
            return False
        
        # Check if column type is timestamp-like
        col_type = table_schema[timestamp_col].lower()
        if not any(t in col_type for t in ['timestamp', 'datetime', 'date']):
            logger.warning(f"Timestamp column {timestamp_col} has non-timestamp type: {col_type}")
            # Don't fail - allow flexibility
        
        return True


class HybridCDCStrategy(CDCStrategy):
    """
    Hybrid CDC strategy (timestamp + ID)
    
    Most robust approach using both timestamp and ID columns for total ordering.
    Handles edge cases like:
    - Multiple rows with same timestamp
    - Clock adjustments and timezone issues
    - High-frequency updates
    """
    
    def build_query(self, table_name: str, watermark: Dict[str, Any], limit: int, table_schema: Optional[Dict[str, str]] = None, end_time: Optional[str] = None) -> str:
        """Build hybrid timestamp+ID incremental query"""
        # SECURITY: Validate table name and column names
        if not _validate_sql_identifier(table_name):
            raise CDCSecurityError(f"Invalid table name: {table_name}")

        timestamp_col = self.config.timestamp_column
        if not _validate_sql_identifier(timestamp_col):
            raise CDCSecurityError(f"Invalid timestamp column: {timestamp_col}")

        id_col = self.config.id_column
        if not _validate_sql_identifier(id_col):
            raise CDCSecurityError(f"Invalid ID column: {id_col}")

        # Extract watermark data with manual ID override support
        last_timestamp = watermark.get('last_mysql_data_timestamp')

        # Check for manual ID override (same logic as IdOnlyCDCStrategy)
        metadata = watermark.get('metadata', {})
        cdc_config = metadata.get('cdc_config', {})

        # Priority: manual ID override > last_processed_id > default 0
        if cdc_config.get('manual_id_set') and 'id_start_override' in cdc_config:
            last_id = cdc_config['id_start_override']
            logger.info(f"Using manual ID override for {table_name}: {last_id}")
        else:
            last_id = watermark.get('last_processed_id', 0)

        # SECURITY: Validate limit is positive integer
        if not isinstance(limit, int) or limit <= 0:
            raise CDCSecurityError(f"Invalid limit value: {limit}")

        # Determine is_unix_timestamp for end_time upper bound formatting
        col_type = ''
        if self.config.timestamp_format == 'unix':
            is_unix_timestamp = True
        elif self.config.timestamp_format == 'datetime':
            is_unix_timestamp = False
        else:  # 'auto' or None
            if table_schema and timestamp_col in table_schema:
                col_type = table_schema[timestamp_col].lower()
            is_unix_timestamp = any(t in col_type for t in ['int', 'bigint', 'tinyint', 'smallint', 'mediumint'])

        # Check for manual ID override (takes precedence for hybrid strategy)
        if cdc_config.get('manual_id_set') and 'id_start_override' in cdc_config:
            # Manual ID override - use ID-only filtering for hybrid strategy
            safe_id_override = _sanitize_sql_value(last_id)  # last_id is already set to id_start_override above
            where_clause = f"WHERE {id_col} > {safe_id_override}"
            logger.info(f"Using manual ID override for hybrid strategy: WHERE {id_col} > {safe_id_override}")
        elif last_timestamp:
            # For unix integer columns, convert datetime strings to unix epoch.
            if is_unix_timestamp:
                ts_str = str(last_timestamp).strip()
                if any(c in ts_str for c in ['-', 'T', '/', ' ']):
                    try:
                        from datetime import datetime as _dt
                        clean_ts = ts_str.replace('T', ' ').split('.')[0].split('+')[0].rstrip('Z').strip()
                        lower_unix = int(_dt.strptime(clean_ts, '%Y-%m-%d %H:%M:%S').timestamp())
                        safe_timestamp = str(lower_unix)
                        logger.info(f"Converted lower bound datetime string to unix: {ts_str} → {lower_unix}")
                    except Exception as e:
                        logger.warning(f"Failed to convert lower bound to unix: {e}. Using sanitized string.")
                        safe_timestamp = _sanitize_sql_value(last_timestamp)
                else:
                    safe_timestamp = _sanitize_sql_value(last_timestamp)
            else:
                safe_timestamp = _sanitize_sql_value(last_timestamp)
            safe_last_id = _sanitize_sql_value(last_id)
            # Hybrid condition: timestamp > last OR (timestamp = last AND id > last_id)
            where_clause = f"""WHERE {timestamp_col} > {safe_timestamp}
                OR ({timestamp_col} = {safe_timestamp} AND {id_col} > {safe_last_id})"""
        else:
            # First run - no watermark
            where_clause = ""

        # Apply end_time upper bound (enforces 5-min buffer; also bounds first-run queries)
        if end_time:
            try:
                clean_end = end_time.replace('T', ' ').split('.')[0].split('+')[0]
                if is_unix_timestamp:
                    from datetime import datetime as _dt
                    end_unix = int(_dt.strptime(clean_end, '%Y-%m-%d %H:%M:%S').timestamp())
                    upper_cond = f"{timestamp_col} < {end_unix}"
                else:
                    safe_end = _sanitize_sql_value(clean_end)
                    upper_cond = f"{timestamp_col} < {safe_end}"

                if where_clause:
                    where_clause = where_clause + f" AND {upper_cond}"
                else:
                    where_clause = f"WHERE {upper_cond}"
                logger.info(f"Applied end_time upper bound: {upper_cond}")
            except Exception as e:
                logger.warning(f"Failed to apply end_time upper bound: {e}. Query will run without upper bound.")

        # Hybrid ordering: timestamp, then ID
        order_clause = f"ORDER BY {timestamp_col}, {id_col}"

        query = f"""
        SELECT * FROM {table_name}
        {where_clause}
        {order_clause}
        LIMIT {limit}
        """.strip()

        logger.info(f"Hybrid CDC query built", extra={
            "table": table_name,
            "timestamp_column": timestamp_col,
            "id_column": id_col,
            "last_timestamp": last_timestamp,
            "end_time": end_time,
            "last_id": last_id,
            "limit": limit
        })

        return query
    
    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """Extract watermark data from hybrid processing"""
        if not rows:
            return WatermarkData(strategy_used=self.strategy_name)

        # Get last row's timestamp and ID
        last_row = rows[-1]
        timestamp_col = self.config.timestamp_column
        id_col = self.config.id_column

        last_timestamp = last_row.get(timestamp_col)
        last_id = last_row.get(id_col)

        # Subtract 1s so next cycle reads >= T instead of > T,
        # catching rows committed at the watermark second after our query ran.
        if last_timestamp is not None:
            if isinstance(last_timestamp, (int, float)):
                last_timestamp = int(last_timestamp) - 1
            elif isinstance(last_timestamp, str):
                try:
                    from datetime import datetime as _dt, timedelta
                    clean = last_timestamp.replace('T', ' ').split('.')[0].split('+')[0].rstrip('Z').strip()
                    last_timestamp = (_dt.strptime(clean, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass  # Keep original if parse fails

        return WatermarkData(
            last_timestamp=last_timestamp,
            last_id=last_id,
            row_count=len(rows),
            strategy_used=self.strategy_name,
            additional_data={
                'timestamp_column': timestamp_col,
                'id_column': id_col
            }
        )
    
    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        """Validate table has required timestamp and ID columns"""
        timestamp_col = self.config.timestamp_column
        id_col = self.config.id_column
        
        # Check timestamp column
        if timestamp_col not in table_schema:
            logger.error(f"Table missing required timestamp column: {timestamp_col}")
            return False
        
        # Check ID column  
        if id_col not in table_schema:
            logger.error(f"Table missing required ID column: {id_col}")
            return False
        
        # Validate column types
        timestamp_type = table_schema[timestamp_col].lower()
        id_type = table_schema[id_col].lower()
        
        if not any(t in timestamp_type for t in ['timestamp', 'datetime', 'date']):
            logger.warning(f"Timestamp column {timestamp_col} has non-timestamp type: {timestamp_type}")
        
        if not any(t in id_type for t in ['int', 'bigint', 'serial']):
            logger.warning(f"ID column {id_col} has non-integer type: {id_type}")
        
        return True


class IdOnlyCDCStrategy(CDCStrategy):
    """
    ID-only CDC strategy for append-only tables
    
    Uses auto-increment ID column for change detection.
    Suitable for:
    - Log tables
    - Event streams  
    - Append-only transaction records
    """
    
    def build_query(self, table_name: str, watermark: Dict[str, Any], limit: int, table_schema: Optional[Dict[str, str]] = None, end_time: Optional[str] = None) -> str:
        """Build ID-based incremental query"""
        # SECURITY: Validate table name and column names
        if not _validate_sql_identifier(table_name):
            raise CDCSecurityError(f"Invalid table name: {table_name}")
        
        id_col = self.config.id_column
        if not _validate_sql_identifier(id_col):
            raise CDCSecurityError(f"Invalid ID column: {id_col}")
        
        # Extract watermark data - check for manual ID overrides first
        metadata = watermark.get('metadata', {})
        cdc_config = metadata.get('cdc_config', {})
        
        # Priority: manual ID override > last_processed_id > default 0
        if cdc_config.get('manual_id_set') and 'id_start_override' in cdc_config:
            last_id = cdc_config['id_start_override']
            logger.info(f"Using manual ID override for {table_name}: {last_id}")
        else:
            last_id = watermark.get('last_processed_id', 0)
        
        # SECURITY: Validate limit is positive integer
        if not isinstance(limit, int) or limit <= 0:
            raise CDCSecurityError(f"Invalid limit value: {limit}")
        
        # SECURITY: Validate and sanitize last_id
        if not isinstance(last_id, (int, float)) or last_id < 0:
            raise CDCSecurityError(f"Invalid last_id value: {last_id}")
        
        safe_last_id = _sanitize_sql_value(last_id)
        where_clause = f"WHERE {id_col} > {safe_last_id}"
        order_clause = f"ORDER BY {id_col}"
        
        query = f"""
        SELECT * FROM {table_name}
        {where_clause}
        {order_clause}
        LIMIT {limit}
        """.strip()
        
        logger.info(f"IdOnly CDC query built", extra={
            "table": table_name,
            "id_column": id_col,
            "last_id": last_id,
            "limit": limit
        })
        
        return query
    
    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """Extract watermark data from ID-only processing"""
        if not rows:
            return WatermarkData(strategy_used=self.strategy_name)
        
        # Get last row's ID
        last_row = rows[-1]
        id_col = self.config.id_column
        last_id = last_row.get(id_col)
        
        return WatermarkData(
            last_id=last_id,
            row_count=len(rows),
            strategy_used=self.strategy_name,
            additional_data={'id_column': id_col}
        )
    
    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        """Validate table has required ID column"""
        id_col = self.config.id_column
        
        if id_col not in table_schema:
            logger.error(f"Table missing required ID column: {id_col}")
            return False
        
        # Check if column type is integer-like
        col_type = table_schema[id_col].lower()
        if not any(t in col_type for t in ['int', 'bigint', 'serial']):
            logger.warning(f"ID column {id_col} has non-integer type: {col_type}")
        
        return True


class FullSyncStrategy(CDCStrategy):
    """
    Enhanced full sync strategy with replace/paginate/append modes
    
    Supports three modes:
    - replace: TRUNCATE table before loading (true dimension refresh)
    - paginate: OFFSET-based pagination for large tables without CDC columns
    - append: Current behavior (backward compatibility)
    
    Suitable for:
    - Small dimension tables (replace mode)
    - Large tables without CDC columns (paginate mode)  
    - Legacy compatibility (append mode)
    """
    
    def __init__(self, config: CDCConfig):
        super().__init__(config)
        # Extract full_sync_mode from config metadata
        metadata = getattr(config, 'metadata', {}) or {}
        self.sync_mode = metadata.get('full_sync_mode', 'append')  # Default: backward compatible
        
        # Validate sync mode
        valid_modes = ['replace', 'paginate', 'append']
        if self.sync_mode not in valid_modes:
            raise ValueError(f"Invalid full_sync_mode '{self.sync_mode}'. Must be one of: {valid_modes}")
    
    def build_query(self, table_name: str, watermark: Dict[str, Any], limit: int, table_schema: Optional[Dict[str, str]] = None, end_time: Optional[str] = None) -> str:
        """Build full sync query based on mode"""
        # SECURITY: Validate table name
        if not _validate_sql_identifier(table_name):
            raise CDCSecurityError(f"Invalid table name: {table_name}")
        
        # SECURITY: Validate limit is positive integer
        if not isinstance(limit, int) or limit <= 0:
            raise CDCSecurityError(f"Invalid limit value: {limit}")
        
        # FULL_SYNC REPLACE MODE: Always read complete table data
        # Replace mode should ignore existing watermarks and perform full extraction
        if self.sync_mode == 'replace':
            logger.info(f"FullSync replace mode: reading complete table", extra={
                "table": table_name,
                "mode": "replace",
                "strategy": "full_table_scan"
            })
        
        if self.sync_mode == 'paginate':
            # NEW: OFFSET-based pagination for large tables
            metadata = watermark.get('metadata', {})
            last_offset = metadata.get('last_offset', 0)
            
            # SECURITY: Validate offset is non-negative integer
            if not isinstance(last_offset, (int, float)) or last_offset < 0:
                last_offset = 0
            
            safe_offset = int(last_offset)
            
            query = f"""
            SELECT * FROM {table_name}
            ORDER BY id
            LIMIT {limit} OFFSET {safe_offset}
            """.strip()
            
            logger.info(f"FullSync paginate query built", extra={
                "table": table_name,
                "limit": limit,
                "offset": safe_offset,
                "mode": "paginate"
            })
        elif self.sync_mode == 'replace':
            # REPLACE MODE: Full table sync for dimension refresh
            # 
            # DESIGN DECISION: Use a very high LIMIT instead of unlimited query
            # WHY: The row-based chunking system expects queries with LIMIT clauses
            #      and validates that results don't exceed the requested limit.
            #      For full_sync replace mode, we need ALL rows but within chunking framework.
            # 
            # SOLUTION: Set LIMIT to 999,999,999 (999M rows) which accommodates any 
            #           reasonable dimension table while satisfying chunking expectations.
            #           This allows the complete table to be processed in one chunk.
            #
            # BEHAVIOR: Row-based strategy will:
            #   1. Execute this query and get all table rows (e.g., 112K for ecs_staff)  
            #   2. Process them in S3 upload batches (e.g., 50K rows per batch)
            #   3. Complete in one chunking iteration instead of endless loop
            #   4. Redshift loader will truncate target table before loading (replace mode)
            large_limit = 999_999_999  # 999M rows - supports any dimension table size
            
            query = f"""
            SELECT * FROM {table_name}
            ORDER BY {self.config.id_column}
            LIMIT {large_limit}
            """.strip()
            
            logger.info(f"FullSync replace query built", extra={
                "table": table_name,
                "mode": "replace", 
                "limit": large_limit,
                "purpose": "complete_table_refresh",
                "note": "Uses high LIMIT to work with row-based chunking while reading full table"
            })
        else:
            # APPEND mode - use limit for backward compatibility
            query = f"""
            SELECT * FROM {table_name}
            LIMIT {limit}
            """.strip()
            
            logger.info(f"FullSync {self.sync_mode} query built", extra={
                "table": table_name,
                "limit": limit,
                "mode": self.sync_mode
            })
        
        return query
    
    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """Extract watermark data from full sync based on mode"""
        additional_data = {'full_sync': True, 'sync_mode': self.sync_mode}
        
        if self.sync_mode == 'replace':
            # REPLACE MODE: Mark as completed after processing
            # This prevents infinite loops by setting a completion timestamp
            completion_timestamp = datetime.now()
            
            logger.info(f"FullSync replace mode completed", extra={
                "rows_processed": len(rows),
                "completion_timestamp": completion_timestamp.isoformat(),
                "mode": "replace"
            })
            
            return WatermarkData(
                last_timestamp=completion_timestamp.isoformat(),  # Convert to ISO string for JSON serialization
                last_id=None,  # No specific ID tracking for full sync
                row_count=len(rows),
                strategy_used=self.strategy_name,
                additional_data=additional_data
            )
            
        elif self.sync_mode == 'paginate':
            # Track pagination offset for next iteration
            current_offset = getattr(self, '_current_offset', 0) + len(rows)
            additional_data['last_offset'] = current_offset
            
            logger.info(f"FullSync paginate watermark extracted", extra={
                "rows_processed": len(rows),
                "new_offset": current_offset,
                "mode": "paginate"
            })
            
            return WatermarkData(
                row_count=len(rows),
                strategy_used=self.strategy_name,
                additional_data=additional_data
            )
        
        else:  # append mode - backward compatible
            return WatermarkData(
                row_count=len(rows),
                strategy_used=self.strategy_name,
                additional_data=additional_data
            )
    
    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        """Validate table schema based on full sync mode"""
        if self.sync_mode == 'paginate':
            # Paginate mode requires 'id' column for consistent ordering
            if 'id' not in table_schema:
                logger.error(f"Paginate mode requires 'id' column for consistent ordering")
                return False
            
            # Check if ID column type is suitable for ordering
            id_type = table_schema['id'].lower()
            if not any(t in id_type for t in ['int', 'bigint', 'serial']):
                logger.warning(f"ID column has non-integer type: {id_type} - ordering may be inconsistent")
        
        return True
    
    def requires_truncate_before_load(self) -> bool:
        """Whether this strategy requires table truncation before loading"""
        return self.sync_mode == 'replace'
    
    def get_sync_mode(self) -> str:
        """Get the current sync mode for this strategy"""
        return self.sync_mode
    
    def set_current_offset(self, offset: int) -> None:
        """Set current offset for pagination tracking"""
        self._current_offset = offset


class CustomSQLStrategy(CDCStrategy):
    """
    Custom SQL strategy for user-defined incremental logic
    
    Allows complete customization of incremental query logic.
    Supports template variables:
    - {table_name}: Table name
    - {last_timestamp}: Last watermark timestamp
    - {last_id}: Last watermark ID
    - {limit}: Row limit
    """
    
    def build_query(self, table_name: str, watermark: Dict[str, Any], limit: int, table_schema: Optional[Dict[str, str]] = None, end_time: Optional[str] = None) -> str:
        """Build custom user-defined query"""
        # SECURITY: Validate all inputs
        if not _validate_sql_identifier(table_name):
            raise CDCSecurityError(f"Invalid table name: {table_name}")
        
        if not isinstance(limit, int) or limit <= 0:
            raise CDCSecurityError(f"Invalid limit value: {limit}")
        
        custom_query = self.config.custom_query
        if not custom_query:
            raise CDCSecurityError("Custom query is required for CustomSQL strategy")
        
        # SECURITY: Validate custom SQL for basic security
        if not _validate_custom_sql_security(custom_query):
            raise CDCSecurityError(f"Custom SQL failed security validation: {custom_query[:100]}...")
        
        # Extract watermark data for template substitution
        last_timestamp = watermark.get('last_mysql_data_timestamp', '')
        last_id = watermark.get('last_processed_id', 0)
        
        # SECURITY: Sanitize all template values
        safe_table_name = table_name  # Already validated above
        safe_last_timestamp = _sanitize_sql_value(last_timestamp) if last_timestamp else 'NULL'
        safe_last_id = _sanitize_sql_value(last_id)
        safe_limit = str(limit)  # Already validated as positive integer
        
        # Template variable substitution with safe values
        try:
            query = custom_query.format(
                table_name=safe_table_name,
                last_timestamp=safe_last_timestamp,
                last_id=safe_last_id,
                limit=safe_limit
            )
        except KeyError as e:
            raise CDCSecurityError(f"Invalid template variable in custom query: {e}")
        except Exception as e:
            raise CDCSecurityError(f"Failed to format custom query: {e}")
        
        # SECURITY: Final validation of generated query
        if len(query) > 10000:  # Prevent excessively long queries
            raise CDCSecurityError("Generated query is too long (>10000 chars)")
        
        logger.info(f"CustomSQL query built", extra={
            "table": table_name,
            "custom_query_length": len(custom_query),
            "generated_query_length": len(query),
            "limit": limit
        })
        
        return query
    
    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """Extract watermark data from custom SQL processing"""
        if not rows:
            return WatermarkData(strategy_used=self.strategy_name)
        
        # Custom SQL strategy - try to extract common watermark fields
        last_row = rows[-1]
        
        # Try common timestamp column names
        last_timestamp = None
        for col in ['updated_at', 'created_at', 'timestamp', 'last_modified']:
            if col in last_row:
                last_timestamp = last_row[col]
                break
        
        # Try common ID column names
        last_id = None
        for col in ['id', 'ID', 'record_id', 'pk_id']:
            if col in last_row:
                last_id = last_row[col]
                break
        
        return WatermarkData(
            last_timestamp=last_timestamp,
            last_id=last_id,
            row_count=len(rows),
            strategy_used=self.strategy_name,
            additional_data={'custom_sql': True}
        )
    
    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        """Custom SQL validation is user responsibility"""
        logger.info("CustomSQL strategy - schema validation skipped (user responsibility)")
        return True


class JoinDrivenCDCStrategy(CDCStrategy):
    """
    Join-driven CDC strategy for tables that lack their own advancing CDC column.

    Use case — ecs_order_info:
      add_time is set once at order creation and never changes.  A time-based
      watermark on add_time only captures NEW orders, leaving a cold-start gap
      for all orders created before the pipeline started.

    Solution: drive ECS extraction via uni_tracking_info.update_time (which
    advances continuously for every active order).  Any order that is actively
    tracked will cycle through a UTI batch, and its ECS row will be extracted
    in that same cycle.  No cold-start gap, no one-time backfill needed.

    Query pattern:
        SELECT e.*, u.{join_column} AS __cdc_join_ts
        FROM {table} e
        INNER JOIN {join_table} u ON u.{join_key} = e.{join_key}
        WHERE u.{join_column} > {watermark}
          AND u.{join_column} < {end_time}
        ORDER BY u.{join_column}, e.{join_key}
        LIMIT {limit}

    Watermark: tracks join_column (e.g. update_time from uni_tracking_info).
    The __cdc_join_ts internal column is stripped from every row IN-PLACE inside
    extract_watermark_data(), which is called before S3 upload — no extra column
    lands in the raw Redshift table.

    First run (no watermark): extracts the oldest 100K active orders ordered by
    update_time, then paginates forward on subsequent cycles until all active
    orders are covered.  After that, steady-state is ~100K rows/cycle.
    """

    _INTERNAL_COL = '__cdc_join_ts'

    def build_query(
        self,
        table_name: str,
        watermark: Dict[str, Any],
        limit: int,
        table_schema: Optional[Dict[str, str]] = None,
        end_time: Optional[str] = None,
    ) -> str:
        join_table  = self.config.join_table
        join_column = self.config.join_column
        join_key    = self.config.join_key or 'order_id'

        # Security: validate every identifier that goes into the SQL
        for name in [table_name, join_table, join_column, join_key]:
            if not _validate_sql_identifier(name):
                raise CDCSecurityError(f"Invalid SQL identifier: {name!r}")
        if not isinstance(limit, int) or limit <= 0:
            raise CDCSecurityError(f"Invalid limit: {limit}")

        last_timestamp = watermark.get('last_mysql_data_timestamp')

        # ── Lower bound (watermark) ───────────────────────────────────────────
        where_parts = []
        if last_timestamp:
            ts_str = str(last_timestamp).strip()
            if any(c in ts_str for c in ['-', 'T', '/', ' ']):
                # Datetime string — convert to unix epoch
                try:
                    from datetime import datetime as _dt
                    clean = ts_str.replace('T', ' ').split('.')[0].split('+')[0].rstrip('Z').strip()
                    lower_unix = int(_dt.strptime(clean, '%Y-%m-%d %H:%M:%S').timestamp())
                    where_parts.append(f"u.{join_column} > {lower_unix}")
                    logger.info(f"JoinDriven lower bound: {ts_str!r} → unix {lower_unix}")
                except Exception as e:
                    logger.warning(f"Failed to parse lower bound {ts_str!r}: {e}. Using sanitised string.")
                    where_parts.append(f"u.{join_column} > {_sanitize_sql_value(last_timestamp)}")
            else:
                # Already numeric
                where_parts.append(f"u.{join_column} > {_sanitize_sql_value(last_timestamp)}")

        # ── Upper bound (end_time / 5-min buffer) ────────────────────────────
        if end_time:
            try:
                from datetime import datetime as _dt
                clean_end = end_time.replace('T', ' ').split('.')[0].split('+')[0]
                end_unix  = int(_dt.strptime(clean_end, '%Y-%m-%d %H:%M:%S').timestamp())
                where_parts.append(f"u.{join_column} < {end_unix}")
                logger.info(f"JoinDriven upper bound: {end_time!r} → unix {end_unix}")
            except Exception as e:
                logger.warning(f"Failed to apply end_time upper bound: {e}. Query will run without it.")

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        query = (
            f"SELECT e.*, u.{join_column} AS {self._INTERNAL_COL}\n"
            f"FROM {table_name} e\n"
            f"INNER JOIN {join_table} u ON u.{join_key} = e.{join_key}\n"
            f"{where_clause}\n"
            f"ORDER BY u.{join_column}, e.{join_key}\n"
            f"LIMIT {limit}"
        ).strip()

        logger.info(
            f"JoinDriven CDC query built",
            extra={
                "table": table_name,
                "join_table": join_table,
                "join_column": join_column,
                "last_timestamp": last_timestamp,
                "end_time": end_time,
                "limit": limit,
                "complete_query": query,
            },
        )
        print(f"[CDC DEBUG] JoinDriven query for {table_name}:\n{query}")
        return query

    def extract_watermark_data(self, rows: List[Dict[str, Any]]) -> WatermarkData:
        """
        Extract watermark from the __cdc_join_ts internal column and strip it
        from every row IN-PLACE.

        This is called inside _get_next_chunk() with the same list reference
        that is later passed to the S3 uploader.  Mutating here means the
        internal column never reaches the raw Redshift table.
        """
        if not rows:
            return WatermarkData(strategy_used=self.strategy_name)

        raw_ts = rows[-1].get(self._INTERNAL_COL)

        # Strip the internal column from all rows before they reach S3
        for row in rows:
            row.pop(self._INTERNAL_COL, None)

        # Subtract 1s then convert unix int → datetime string for watermark storage.
        # Next cycle queries WHERE join_col > (T-1) which is equivalent to >= T,
        # catching rows committed at the watermark second after our query ran.
        processed_ts = raw_ts
        if raw_ts is not None and isinstance(raw_ts, (int, float)):
            try:
                from datetime import datetime
                adjusted_ts = int(raw_ts) - 1
                processed_ts = datetime.fromtimestamp(adjusted_ts).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass  # Keep raw value if conversion fails

        return WatermarkData(
            last_timestamp=processed_ts,
            row_count=len(rows),
            strategy_used=self.strategy_name,
            additional_data={'join_column': self.config.join_column},
        )

    def validate_table_schema(self, table_schema: Dict[str, str]) -> bool:
        join_key = self.config.join_key or 'order_id'
        if join_key not in table_schema:
            logger.error(f"Table missing join key column: {join_key!r}")
            return False
        return True


class CDCStrategyFactory:
    """Factory for creating CDC strategy instances"""

    _strategies = {
        CDCStrategyType.TIMESTAMP_ONLY: TimestampOnlyCDCStrategy,
        CDCStrategyType.HYBRID: HybridCDCStrategy,
        CDCStrategyType.ID_ONLY: IdOnlyCDCStrategy,
        CDCStrategyType.FULL_SYNC: FullSyncStrategy,
        CDCStrategyType.CUSTOM_SQL: CustomSQLStrategy,
        CDCStrategyType.JOIN_DRIVEN: JoinDrivenCDCStrategy,
    }
    
    @classmethod
    def create_strategy(cls, config: CDCConfig) -> CDCStrategy:
        """Create CDC strategy instance from configuration"""
        # SECURITY: Validate configuration before creating strategy
        cls._validate_config_security(config)
        
        strategy_class = cls._strategies.get(config.strategy)
        
        if not strategy_class:
            raise ValueError(f"Unsupported CDC strategy: {config.strategy}")
        
        return strategy_class(config)
    
    @classmethod
    def _validate_config_security(cls, config: CDCConfig) -> None:
        """Validate CDC configuration for security issues"""
        # Validate column names
        if config.timestamp_column and not _validate_sql_identifier(config.timestamp_column):
            raise CDCSecurityError(f"Invalid timestamp column name: {config.timestamp_column}")
        
        if config.id_column and not _validate_sql_identifier(config.id_column):
            raise CDCSecurityError(f"Invalid ID column name: {config.id_column}")
        
        # Validate ordering columns
        if config.ordering_columns:
            for col in config.ordering_columns:
                if not _validate_sql_identifier(col):
                    raise CDCSecurityError(f"Invalid ordering column name: {col}")
        
        # Validate custom query for CustomSQL strategy
        if config.strategy == CDCStrategyType.CUSTOM_SQL:
            if not config.custom_query:
                raise CDCSecurityError("Custom query is required for CustomSQL strategy")
            if not _validate_custom_sql_security(config.custom_query):
                raise CDCSecurityError(f"Custom SQL failed security validation: {config.custom_query[:100]}...")
        
        # Validate batch size
        if config.batch_size and (not isinstance(config.batch_size, int) or config.batch_size <= 0):
            raise CDCSecurityError(f"Invalid batch size: {config.batch_size}")
        
        logger.info("CDC configuration passed security validation", extra={
            "strategy": config.strategy.value,
            "timestamp_column": config.timestamp_column,
            "id_column": config.id_column,
            "has_custom_query": bool(config.custom_query)
        })
    
    @classmethod
    def get_supported_strategies(cls) -> List[str]:
        """Get list of supported CDC strategy names"""
        return [strategy.value for strategy in CDCStrategyType]


# Utility functions for v1.0.0/v1.1.0 compatibility
def create_legacy_cdc_config(timestamp_column: str = "updated_at") -> CDCConfig:
    """Create CDC config for v1.0.0/v1.1.0 compatibility"""
    return CDCConfig(
        strategy=CDCStrategyType.TIMESTAMP_ONLY,
        timestamp_column=timestamp_column
    )


def migrate_legacy_watermark(legacy_watermark: Dict[str, Any]) -> WatermarkData:
    """Convert legacy watermark format to new WatermarkData format"""
    return WatermarkData(
        last_timestamp=legacy_watermark.get('last_mysql_data_timestamp'),
        last_id=legacy_watermark.get('last_processed_id'),
        row_count=legacy_watermark.get('mysql_row_count', 0),
        strategy_used='timestamp_only',  # Legacy is always timestamp-only
        additional_data={'legacy_migration': True}
    )