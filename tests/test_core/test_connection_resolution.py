#!/usr/bin/env python3
"""
Test script to debug connection password resolution
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded environment variables from {env_file}")
    else:
        print(f"❌ .env file not found at {env_file}")
except ImportError:
    print("❌ python-dotenv not installed, trying manual .env loading")
    # Manual .env loading as fallback
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print(f"✅ Manually loaded environment variables from {env_file}")
    else:
        print(f"❌ .env file not found at {env_file}")
print()

def test_environment_variables():
    """Test if environment variables are loaded correctly"""
    print("=== Environment Variables Test ===")
    
    password_vars = [
        'DB_PASSWORD',
        'DB_US_DW_PASSWORD', 
        'DB_US_DW_RO_PASSWORD',
        'DB_CA_DW_RO_PASSWORD',
        'DB_US_PROD_RO_PASSWORD',
        'DB_US_QA_PASSWORD',
        'DB2_US_RO_PASSWORD'
    ]
    
    for var in password_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {'*' * len(value)} (length: {len(value)})")
        else:
            print(f"❌ {var}: NOT SET")
    print()

def test_connection_registry():
    """Test ConnectionRegistry loading"""
    print("=== ConnectionRegistry Test ===")
    
    try:
        from src.core.connection_registry import ConnectionRegistry
        
        registry = ConnectionRegistry()
        print(f"✅ ConnectionRegistry initialized with {len(registry.connections)} connections")
        
        # Test specific connection
        connection_name = "US_DW_UNIDW_SSH"
        conn_config = registry.get_connection(connection_name)
        
        if conn_config:
            print(f"✅ Found connection: {connection_name}")
            print(f"   Host: {conn_config.host}")
            print(f"   Database: {conn_config.database}")
            print(f"   Username: {conn_config.username}")
            print(f"   Password: {'*' * len(conn_config.password) if conn_config.password else 'EMPTY'}")
            print(f"   SSH enabled: {conn_config.ssh_tunnel.get('enabled', False)}")
        else:
            print(f"❌ Connection {connection_name} not found")
            print(f"Available connections: {list(registry.connections.keys())}")
            
    except Exception as e:
        print(f"❌ ConnectionRegistry failed: {e}")
        import traceback
        traceback.print_exc()
    print()

def test_connection_manager():
    """Test ConnectionManager integration"""
    print("=== ConnectionManager Test ===")
    
    try:
        from src.config.settings import AppConfig
        from src.core.connections import ConnectionManager
        
        config = AppConfig.load()
        connection_manager = ConnectionManager(config)
        
        # Test getting connection config
        connection_name = "US_DW_UNIDW_SSH"
        conn_config = connection_manager.get_connection_config(connection_name)
        
        print(f"✅ ConnectionManager loaded")
        print(f"   Connection: {connection_name}")
        print(f"   Host: {conn_config.host}")
        print(f"   Database: {conn_config.database}")
        print(f"   Username: {conn_config.username}")
        print(f"   Password: {'*' * len(conn_config.password) if conn_config.password else 'EMPTY'}")
        
    except Exception as e:
        print(f"❌ ConnectionManager failed: {e}")
        import traceback
        traceback.print_exc()
    print()

def main():
    """Run all tests"""
    print("Testing Connection Password Resolution\n")
    
    test_environment_variables()
    test_connection_registry()
    test_connection_manager()

if __name__ == "__main__":
    main()