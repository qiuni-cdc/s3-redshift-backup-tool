#!/usr/bin/env python3
"""
Create Redshift table that exactly matches the parquet schema
"""

import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2
import os

def read_env_file():
    """Read configuration from .env file"""
    config = {}
    try:
        with open('.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key] = value
    except Exception as e:
        print(f"⚠️  Could not read .env file: {e}")
    return config

def parquet_to_redshift_type(dtype):
    """Convert pandas dtype to Redshift type"""
    dtype_str = str(dtype)
    
    if 'int64' in dtype_str:
        return 'BIGINT'
    elif 'int32' in dtype_str:
        return 'INTEGER'
    elif 'float64' in dtype_str:
        return 'DOUBLE PRECISION'
    elif 'float32' in dtype_str:
        return 'REAL'
    elif 'datetime64' in dtype_str:
        return 'TIMESTAMP'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        return 'VARCHAR(MAX)'  # Default for object/string types

def main():
    print("🏗️  Creating Redshift Table from Parquet Schema")
    print("=" * 55)
    
    # Read a sample parquet file
    sample_file = '/tmp/sample.parquet'
    if os.path.exists(sample_file):
        print("✅ Using existing sample parquet file")
    else:
        print("❌ Sample parquet file not found. Please run:")
        print("   aws s3 cp s3://redshift-dw-qa-uniuni-com/incremental/year=2025/month=08/day=07/hour=09/settlement_settlement_normal_delivery_detail_20250807_092022_batch_0001.parquet /tmp/sample.parquet")
        return
    
    # Analyze parquet schema
    df = pd.read_parquet(sample_file)
    print(f"📊 Parquet file: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    # Generate CREATE TABLE statement
    table_name = "public.settlement_normal_delivery_detail_test"
    
    create_sql = f"DROP TABLE IF EXISTS {table_name};\n\n"
    create_sql += f"CREATE TABLE {table_name} (\n"
    
    columns = []
    for col in df.columns:
        dtype = df[col].dtype
        redshift_type = parquet_to_redshift_type(dtype)
        
        # Clean column name for Redshift (replace spaces, special chars)
        clean_col = col.replace(' ', '_').replace('-', '_')
        
        columns.append(f"    {clean_col} {redshift_type}")
        
        print(f"  {col:<35} {str(dtype):<20} -> {redshift_type}")
    
    create_sql += ",\n".join(columns)
    create_sql += "\n);"
    
    print("\n" + "="*60)
    print("📝 Generated CREATE TABLE Statement:")
    print("="*60)
    print(create_sql)
    
    # Automatically proceed with table creation
    print("\n✅ Proceeding with table creation...")
    
    # Connect to Redshift and create table
    env_config = read_env_file()
    password = env_config.get('REDSHIFT_PASSWORD')
    
    # Setup SSH tunnel
    tunnel = SSHTunnelForwarder(
        ('35.82.216.244', 22),
        ssh_username='chenqi',
        ssh_pkey='/home/qi_chen/test_env/chenqi.pem',
        remote_bind_address=('redshift-dw.qa.uniuni.com', 5439),
        local_bind_address=('localhost', 0)
    )
    
    tunnel.start()
    local_port = tunnel.local_bind_port
    print(f"✅ SSH tunnel: localhost:{local_port}")
    
    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host='localhost',
            port=local_port,
            database='dev',
            user='chenqi',
            password=password
        )
        
        print("✅ Connected to Redshift")
        
        cursor = conn.cursor()
        
        # Execute CREATE TABLE
        print("🏗️  Creating table...")
        cursor.execute(create_sql)
        conn.commit()
        print("✅ Table created successfully!")
        
        # Test COPY with a single file first
        print("\n📥 Testing COPY with single file...")
        copy_sql = f"""
        COPY {table_name}
        FROM 's3://redshift-dw-qa-uniuni-com/incremental/year=2025/month=08/day=07/hour=09/settlement_settlement_normal_delivery_detail_20250807_092022_batch_0001.parquet'
        ACCESS_KEY_ID 'YOUR_AWS_ACCESS_KEY_ID'
        SECRET_ACCESS_KEY 'YOUR_AWS_SECRET_ACCESS_KEY'
        FORMAT AS PARQUET;
        """
        
        cursor.execute(copy_sql)
        conn.commit()
        print("✅ Single file COPY successful!")
        
        # Check row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        print(f"📊 Loaded {count:,} rows from single file")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        tunnel.stop()
        print("🔗 SSH tunnel closed")

if __name__ == "__main__":
    main()