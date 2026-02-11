import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from src.core.configuration_manager import ConfigurationManager
from src.core.gemini_redshift_loader import GeminiRedshiftLoader
from src.core.connection_registry import ConnectionRegistry

def debug_loader_logic():
    print("🚀 Starting Debug for GeminiRedshiftLoader")
    
    try:
        # 1. Load Config
        print("Loading config_manager...")
        config_manager = ConfigurationManager("config")
        print("Loaded config_manager")

        pipeline_name = "order_tracking_hybrid_dbt"
        print(f"Getting pipeline config for '{pipeline_name}'...")
        pipeline_config = config_manager.get_pipeline_config(pipeline_name)
        
        if not pipeline_config:
            print(f"❌ Pipeline {pipeline_name} not found")
            return

        print(f"✅ Loaded pipeline: {pipeline_name}")
        
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return

    # 2. Setup Loader
    try:
        connection_registry = ConnectionRegistry()
        # Create AppConfig properly using the config manager helper
        print("Creating AppConfig...")
        app_config = config_manager.create_app_config(
            source_connection=pipeline_config.source,
            target_connection=pipeline_config.target,
            s3_config_name=pipeline_config.s3_config
        )
        
        print("Initializing GeminiRedshiftLoader...")
        loader = GeminiRedshiftLoader(app_config, connection_registry=connection_registry)
        print("✅ GeminiRedshiftLoader initialized")
        
    except Exception as e:
        print(f"❌ Failed to initialize loader: {e}")
        import traceback
        traceback.print_exc()
        return

    # 3. Test Table Logic
    table_name = "kuaisong.ecs_order_info"
    if table_name not in pipeline_config.tables:
        print(f"❌ Table {table_name} not found in pipeline config")
        return

    # Retrieve table config object
    table_config = pipeline_config.tables[table_name]
    print(f"\n📋 Testing table: {table_name}")
    print(f"   Config Target Name: {getattr(table_config, 'target_name', 'MISSING')}")
    print(f"   Config Target Schema: {getattr(table_config, 'target_schema', 'MISSING')}")

    # Test _get_redshift_table_name
    print("   Testing _get_redshift_table_name...")
    try:
        resolved_name = loader._get_redshift_table_name(table_name, table_config)
        print(f"   Resolved Target Name: {resolved_name}")
        
        if resolved_name != "ecs_order_info_raw":
            print(f"   ❌ ERROR: Target name mismatch! Expected 'ecs_order_info_raw', got '{resolved_name}'")
        else:
            print("   ✅ Target name correct")
    except Exception as e:
        print(f"   ❌ _get_redshift_table_name failed: {e}")

    # Test DDL Fix Logic
    print("\n🔧 Testing DDL Fix Logic:")
    mock_ddls = [
        f"CREATE TABLE {table_name} (id INT, data VARCHAR(100))",
        f"CREATE TABLE `kuaisong`.`ecs_order_info` (id INT)",
        f"CREATE TABLE IF NOT EXISTS ecs_order_info (id INT)",
        f"CREATE TABLE `ecs_order_info` (id INT)"
    ]
    
    for ddl in mock_ddls:
        try:
            fixed_ddl = loader._fix_ddl_table_name(ddl, resolved_name, table_name)
            expected = f"CREATE TABLE {resolved_name}"
            # Check for various formats of CREATE TABLE
            if expected in fixed_ddl or f"CREATE TABLE `{resolved_name}`" in fixed_ddl:
                 print(f"   ✅ Fixed: {fixed_ddl}")
            else:
                 print(f"   ❌ FAILED Fix: {ddl} -> {fixed_ddl}")
        except Exception as e:
            print(f"   ❌ DDL fix failed for '{ddl}': {e}")

if __name__ == "__main__":
    debug_loader_logic()
