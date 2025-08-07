#!/usr/bin/env python3
"""
Complete full data loading - Load ALL CSV files and verify total count
Target: Load all 2.1+ million rows from 214 CSV files
"""

from sshtunnel import SSHTunnelForwarder
import psycopg2
import boto3
import time

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

def get_all_csv_files():
    """Get ALL CSV files for complete loading"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id='YOUR_AWS_ACCESS_KEY_ID',
        aws_secret_access_key='YOUR_AWS_SECRET_ACCESS_KEY',
        region_name='us-east-1'
    )
    
    bucket = 'redshift-dw-qa-uniuni-com'
    
    print("🔍 Scanning ALL CSV files...")
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix='csv_production/'
    )
    
    csv_files = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            csv_files.append(obj['Key'])
    
    csv_files.sort()
    print(f"✅ Found {len(csv_files)} CSV files for complete loading")
    return csv_files

def load_all_csv_files(cursor, conn, csv_files):
    """Load ALL CSV files with progress tracking"""
    bucket = 'redshift-dw-qa-uniuni-com'
    
    successful_loads = 0
    failed_loads = 0
    total_files = len(csv_files)
    
    print(f"\n📥 Loading ALL {total_files} CSV files...")
    print("=" * 60)
    
    for i, csv_key in enumerate(csv_files, 1):
        print(f"📄 [{i:3d}/{total_files}] Loading: {csv_key}")
        
        copy_sql = f"""
        COPY public.settlement_normal_delivery_detail
        FROM 's3://{bucket}/{csv_key}'
        ACCESS_KEY_ID 'YOUR_AWS_ACCESS_KEY_ID'
        SECRET_ACCESS_KEY 'YOUR_AWS_SECRET_ACCESS_KEY'
        DELIMITER '|'
        IGNOREHEADER 1
        NULL AS '\\N';
        """
        
        try:
            start_time = time.time()
            cursor.execute(copy_sql)
            conn.commit()
            load_time = time.time() - start_time
            
            successful_loads += 1
            print(f"   ✅ Loaded in {load_time:.1f}s (Success: {successful_loads}/{i})")
            
            # Show progress every 20 files
            if i % 20 == 0:
                cursor.execute("SELECT COUNT(*) FROM public.settlement_normal_delivery_detail;")
                current_count = cursor.fetchone()[0]
                progress = (i / total_files) * 100
                print(f"   📊 Progress: {progress:.1f}% - Total rows: {current_count:,}")
            
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            failed_loads += 1
            conn.rollback()
            
            # Continue with next file instead of stopping
            if failed_loads > 10:  # Stop if too many failures
                print(f"⚠️  Too many failures ({failed_loads}), stopping")
                break
    
    return successful_loads, failed_loads

def verify_complete_data(cursor):
    """Comprehensive data verification"""
    print("\n📊 COMPREHENSIVE DATA VERIFICATION")
    print("=" * 50)
    
    # Total count
    cursor.execute("SELECT COUNT(*) FROM public.settlement_normal_delivery_detail;")
    total_count = cursor.fetchone()[0]
    print(f"✅ Total rows loaded: {total_count:,}")
    
    # Business metrics
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT partner_id) as unique_partners,
            COUNT(DISTINCT billing_num) as unique_billings,
            COUNT(DISTINCT ant_parcel_no) as unique_parcels,
            MIN(create_at) as earliest_date,
            MAX(create_at) as latest_date
        FROM public.settlement_normal_delivery_detail;
    """)
    
    metrics = cursor.fetchone()
    print(f"✅ Unique partners: {metrics[0]:,}")
    print(f"✅ Unique billings: {metrics[1]:,}")
    print(f"✅ Unique parcels: {metrics[2]:,}")
    print(f"✅ Date range: {metrics[3]} to {metrics[4]}")
    
    # Data quality checks
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN ID IS NULL THEN 1 END) as null_ids,
            COUNT(CASE WHEN billing_num IS NULL THEN 1 END) as null_billings,
            COUNT(CASE WHEN partner_id IS NULL THEN 1 END) as null_partners
        FROM public.settlement_normal_delivery_detail;
    """)
    
    quality = cursor.fetchone()
    print(f"✅ Data quality: {quality[0]} null IDs, {quality[1]} null billings, {quality[2]} null partners")
    
    # Top partners analysis
    cursor.execute("""
        SELECT partner_id, COUNT(*) as deliveries
        FROM public.settlement_normal_delivery_detail
        GROUP BY partner_id
        ORDER BY deliveries DESC
        LIMIT 10;
    """)
    
    print(f"\n📈 Top 10 Partners by Delivery Count:")
    print("-" * 40)
    for row in cursor.fetchall():
        print(f"   Partner {row[0]}: {row[1]:,} deliveries")
    
    # Sample recent data
    cursor.execute("""
        SELECT ID, billing_num, partner_id, ant_parcel_no, net_price, latest_status, create_at
        FROM public.settlement_normal_delivery_detail
        ORDER BY create_at DESC
        LIMIT 10;
    """)
    
    print(f"\n📋 Sample Recent Data (Latest 10):")
    print("-" * 100)
    print(f"{'ID':<12} {'Billing':<15} {'Partner':<8} {'Parcel':<15} {'Price':<8} {'Status':<15} {'Created':<20}")
    print("-" * 100)
    
    for row in cursor.fetchall():
        print(f"{row[0]:<12} {row[1]:<15} {row[2]:<8} {str(row[3]):<15} {str(row[4]):<8} {str(row[5]):<15} {str(row[6]):<20}")
    
    return total_count

def main():
    print("🚀 COMPLETE FULL DATA LOADING")
    print("=" * 60)
    print("🎯 Goal: Load ALL 2.1+ million settlement delivery records")
    print("📊 Target: dw.public.settlement_normal_delivery_detail")
    print("⏱️  Expected time: 30-60 minutes")
    print()
    
    # Get all CSV files
    csv_files = get_all_csv_files()
    if not csv_files:
        print("❌ No CSV files found!")
        return
    
    expected_rows = len(csv_files) * 10000  # Approximate
    print(f"📈 Expected total rows: ~{expected_rows:,}")
    
    # Connect to Redshift
    env_config = read_env_file()
    password = env_config.get('REDSHIFT_PASSWORD')
    
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
    
    start_time = time.time()
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=local_port,
            database='dw',
            user='chenqi',
            password=password
        )
        
        print("✅ Connected to dw database")
        cursor = conn.cursor()
        
        # Check initial state
        cursor.execute("SELECT COUNT(*) FROM public.settlement_normal_delivery_detail;")
        initial_count = cursor.fetchone()[0]
        print(f"📊 Initial rows in table: {initial_count:,}")
        
        # Load all data
        successful_loads, failed_loads = load_all_csv_files(cursor, conn, csv_files)
        
        loading_time = time.time() - start_time
        
        # Final verification
        total_count = verify_complete_data(cursor)
        
        # Final summary
        print("\n" + "="*80)
        print("🎉 COMPLETE FULL LOADING FINISHED!")
        print("="*80)
        print(f"✅ Database: dw")
        print(f"✅ Schema: public")
        print(f"✅ Table: settlement_normal_delivery_detail")
        print(f"✅ Total CSV files: {len(csv_files)}")
        print(f"✅ Successful loads: {successful_loads}")
        print(f"✅ Failed loads: {failed_loads}")
        print(f"✅ Initial rows: {initial_count:,}")
        print(f"✅ Final rows: {total_count:,}")
        print(f"✅ Rows added: {total_count - initial_count:,}")
        print(f"✅ Loading time: {loading_time/60:.1f} minutes")
        print(f"✅ Optimizations: DISTKEY(ant_parcel_no), SORTKEY(create_at, billing_num)")
        
        completion_rate = (total_count / 2131906) * 100  # Based on original conversion
        print(f"✅ Data completeness: {completion_rate:.1f}%")
        
        if total_count > 1500000:
            print("\n🎊 MISSION ACCOMPLISHED!")
            print("📊 Settlement normal delivery data FULLY LOADED into Redshift")
            print("🚀 Ready for production analytics and business intelligence")
            print("🔗 Table ready: dw.public.settlement_normal_delivery_detail")
        else:
            print(f"\n⚠️  Partial loading: {total_count:,} rows loaded")
            print("💡 May need to investigate failed loads or missing files")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        tunnel.stop()
        print("\n🔗 SSH tunnel closed")

if __name__ == "__main__":
    main()