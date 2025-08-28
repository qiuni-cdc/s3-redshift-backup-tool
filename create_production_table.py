#!/usr/bin/env python3
"""
Create production table with proper schema, table name, and column definitions
Based on MySQL table structure with Redshift optimizations
"""

from sshtunnel import SSHTunnelForwarder
import psycopg2

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

def get_redshift_table_ddl(schema_name):
    """Generate Redshift table DDL based on MySQL structure"""
    return f"""
    DROP TABLE IF EXISTS {schema_name}.settlement_normal_delivery_detail CASCADE;
    
    CREATE TABLE {schema_name}.settlement_normal_delivery_detail (
        -- Primary key and core identifiers
        ID BIGINT NOT NULL,
        billing_num VARCHAR(255) NOT NULL,
        partner_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        carrier_id INTEGER,
        
        -- Invoice and order information  
        invoice_number VARCHAR(255),
        invoice_date TIMESTAMP,
        ant_parcel_no VARCHAR(32),
        uni_no VARCHAR(32), 
        uni_sn VARCHAR(32),
        third_party_tno VARCHAR(255),
        
        -- Shipping and tracking details
        parcel_scan_time TIMESTAMP,
        batch_number VARCHAR(255),
        reference_number VARCHAR(255),
        warehouse_id SMALLINT,
        airport_code VARCHAR(8),
        consignee_address VARCHAR(255),
        zone VARCHAR(32),
        zipcode VARCHAR(60),
        driver_id INTEGER,
        
        -- Weight and pricing (proper DECIMAL types)
        actual_weight DECIMAL(10,3),
        dimensional_weight DECIMAL(10,3), 
        weight_uom VARCHAR(8),
        net_price DECIMAL(10,2),
        signature_fee DECIMAL(10,2),
        tax DECIMAL(10,2),
        fuel_surcharge DECIMAL(10,2),
        shipping_fee DECIMAL(10,2),
        charge_currency VARCHAR(8),
        
        -- Location and status
        province VARCHAR(128),
        latest_status VARCHAR(45),
        last_status_update_at TIMESTAMP,
        is_valid SMALLINT DEFAULT 1,
        create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        -- Additional business fields
        price_card_name VARCHAR(255),
        in_warehouse_date TIMESTAMP,
        inject_warehouse VARCHAR(8),
        gst DECIMAL(10,2),
        qst DECIMAL(10,2),
        consignee_name VARCHAR(255),
        order_create_time TIMESTAMP,
        
        -- Dimensions
        dimension_uom VARCHAR(255),
        length DECIMAL(10,3),
        width DECIMAL(10,3), 
        height DECIMAL(10,3),
        
        -- Extended fields
        org_zipcode VARCHAR(255),
        payment_code VARCHAR(255),
        invoice_to VARCHAR(64),
        remote_surcharge_fees DECIMAL(10,2),
        gv_order_receive_time TIMESTAMP
    )
    -- OPTIMAL PERFORMANCE SETTINGS FOR YOUR SPECIFIC USE CASE
    DISTKEY(ant_parcel_no)           -- Perfect: Even distribution + join optimization  
    SORTKEY(create_at, billing_num); -- Time-based filtering first, then partner filtering
    """

def test_database_and_schema_combinations():
    """Test different database and schema combinations"""
    env_config = read_env_file()
    password = env_config.get('REDSHIFT_PASSWORD')
    
    # Test combinations
    combinations = [
        ('dw', 'unidw_ods'),
        ('dw', 'public'), 
        ('dev', 'unidw_ods'),
        ('dev', 'public')
    ]
    
    tunnel = SSHTunnelForwarder(
        ('your.redshift.bastion.host', 22),
        ssh_username='chenqi',
        ssh_pkey='/path/to/your/ssh/key.pem',
        remote_bind_address=('your.redshift.cluster.com', 5439),
        local_bind_address=('localhost', 0)
    )
    
    tunnel.start()
    local_port = tunnel.local_bind_port
    print(f"✅ SSH tunnel: localhost:{local_port}")
    
    working_combo = None
    
    try:
        for database, schema in combinations:
            print(f"\n🧪 Testing {database}.{schema}...")
            
            try:
                conn = psycopg2.connect(
                    host='localhost',
                    port=local_port,
                    database=database,
                    user='chenqi',
                    password=password
                )
                
                cursor = conn.cursor()
                print(f"   ✅ Connected to {database}")
                
                # Create schema if needed
                if schema != 'public':
                    try:
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                        conn.commit()
                        print(f"   ✅ Schema {schema} ready")
                    except Exception as e:
                        print(f"   ❌ Schema creation failed: {e}")
                        conn.rollback()
                        conn.close()
                        continue
                
                # Test table creation
                try:
                    cursor.execute(f"CREATE TABLE {schema}.test_table_permissions (id INTEGER);")
                    cursor.execute(f"DROP TABLE {schema}.test_table_permissions;")
                    conn.commit()
                    print(f"   ✅ Table creation works in {schema}")
                    
                    working_combo = (database, schema)
                    conn.close()
                    break
                    
                except Exception as e:
                    print(f"   ❌ Table creation failed: {e}")
                    conn.rollback()
                
                conn.close()
                
            except Exception as e:
                print(f"   ❌ Connection to {database} failed: {e}")
                continue
                
    finally:
        tunnel.stop()
    
    return working_combo

def create_production_table(database, schema):
    """Create the production table with correct specifications"""
    env_config = read_env_file()
    password = env_config.get('REDSHIFT_PASSWORD')
    
    tunnel = SSHTunnelForwarder(
        ('your.redshift.bastion.host', 22),
        ssh_username='chenqi',
        ssh_pkey='/path/to/your/ssh/key.pem',
        remote_bind_address=('your.redshift.cluster.com', 5439),
        local_bind_address=('localhost', 0)
    )
    
    tunnel.start()
    local_port = tunnel.local_bind_port
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=local_port,
            database=database,
            user='chenqi',
            password=password
        )
        
        print(f"✅ Connected to {database}")
        cursor = conn.cursor()
        
        # Create schema if needed
        if schema != 'public':
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
        
        # Create production table
        ddl = get_redshift_table_ddl(schema)
        cursor.execute(ddl)
        conn.commit()
        
        print(f"✅ Production table created: {database}.{schema}.settlement_normal_delivery_detail")
        
        # Verify table structure
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' AND table_name = 'settlement_normal_delivery_detail'
            ORDER BY ordinal_position
            LIMIT 10;
        """)
        
        columns = cursor.fetchall()
        print(f"\n📋 Table structure (first 10 columns):")
        for col_name, data_type, nullable in columns:
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            print(f"   {col_name:<25} {data_type:<20} {null_info}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        return False
    finally:
        tunnel.stop()

def main():
    print("🏗️  Creating Production Table with Correct Specifications")
    print("=" * 60)
    print("📋 Requirements:")
    print("   • Database: dw (preferred) or dev (fallback)")
    print("   • Schema: unidw_ods (preferred) or public (fallback)")
    print("   • Table: settlement_normal_delivery_detail")
    print("   • Optimizations: DISTKEY(ant_parcel_no), SORTKEY(create_at, billing_num)")
    print()
    
    # Step 1: Find working database/schema combination
    print("🔍 Step 1: Finding working database/schema combination...")
    working_combo = test_database_and_schema_combinations()
    
    if not working_combo:
        print("❌ No working database/schema combination found!")
        return
    
    database, schema = working_combo
    print(f"\n✅ Selected: {database}.{schema}")
    
    # Step 2: Create production table
    print(f"\n🏗️  Step 2: Creating production table...")
    success = create_production_table(database, schema)
    
    if success:
        print(f"\n🎉 SUCCESS!")
        print(f"✅ Production table ready: {database}.{schema}.settlement_normal_delivery_detail")
        print(f"✅ Optimized for time-based queries and parcel tracking")
        print(f"✅ Proper data types matching MySQL source")
        
        print(f"\n📝 Next steps:")
        print(f"   1. Update COPY scripts to use: {database}.{schema}.settlement_normal_delivery_detail")
        print(f"   2. Migrate existing data if needed")
        print(f"   3. Set up regular data loading process")
    else:
        print(f"\n❌ Table creation failed")

if __name__ == "__main__":
    main()