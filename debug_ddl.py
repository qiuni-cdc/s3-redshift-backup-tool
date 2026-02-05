import sys
import os
import logging

# Ensure src is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from src.core.connection_registry import ConnectionRegistry
from src.core.connections import ConnectionManager
from src.core.flexible_schema_manager import FlexibleSchemaManager
from src.config.settings import AppConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_ddl(table_name):
    print(f"\nDebugging DDL generation for: {table_name}")
    
    try:
        # Initialize
        config = AppConfig()
        registry = ConnectionRegistry()
        conn_manager = ConnectionManager(config)
        
        schema_manager = FlexibleSchemaManager(conn_manager, connection_registry=registry)
        
        # Get schema
        print("Fetching schema from MySQL...")
        pyarrow_schema, ddl = schema_manager.get_table_schema(table_name, force_refresh=True)
        
        print("\n" + "="*80)
        print("PYARROW SCHEMA FIELDS:")
        print("="*80)
        for field in pyarrow_schema:
            print(f"{field.name}: {field.type}")
            
        print("\n" + "="*80)
        print("GENERATED REDSHIFT DDL:")
        print("="*80)
        print(ddl)
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
    else:
        # Default to the table from pipeline config
        table_name = "US_PROD_RO_SSH:kuaisong.ecs_order_info"
        print(f"No table specified, defaulting to: {table_name}")
        print("Usage: python debug_ddl.py <connection:schema.table>")
        
    debug_ddl(table_name)
