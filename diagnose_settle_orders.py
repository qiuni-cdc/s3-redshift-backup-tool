#!/usr/bin/env python3
"""
Diagnose settle_orders schema mismatch between MySQL and Redshift
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from src.core.connections import ConnectionManager
from src.core.connection_registry import ConnectionRegistry
from src.core.flexible_schema_manager import FlexibleSchemaManager
from src.config.settings import AppConfig

load_dotenv()

def main():
    print("=" * 80)
    print("SETTLE_ORDERS SCHEMA DIAGNOSTIC")
    print("=" * 80)

    # Initialize components
    print("\nLoading configuration...")
    config = AppConfig()

    print("Initializing connection manager...")
    conn_manager = ConnectionManager(config)

    print("Initializing connection registry...")
    # ConnectionRegistry auto-detects connections.yml if no path provided
    conn_registry = ConnectionRegistry()

    print("Initializing schema manager...")
    schema_manager = FlexibleSchemaManager(conn_manager, connection_registry=conn_registry)

    table_name = "US_DW_RO_SSH:settlement.settle_orders"

    print(f"\n1. Discovering schema for {table_name}...")
    try:
        pyarrow_schema, redshift_ddl = schema_manager.get_table_schema(table_name, force_refresh=True)

        print(f"\n2. PyArrow Schema ({len(pyarrow_schema)} columns):")
        print("-" * 80)
        for field in pyarrow_schema:
            print(f"  {field.name:40s} {field.type} (nullable={field.nullable})")

        print(f"\n3. Redshift DDL:")
        print("-" * 80)
        print(redshift_ddl)

        print(f"\n4. Column name mapping:")
        print("-" * 80)
        mappings = schema_manager.column_mapper.load_mapping(table_name)
        if mappings:
            for orig, mapped in mappings.items():
                print(f"  {orig:40s} → {mapped}")
        else:
            print("  No column mappings found (all columns compatible)")

        print("\n" + "=" * 80)
        print("✅ Schema discovery successful")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
