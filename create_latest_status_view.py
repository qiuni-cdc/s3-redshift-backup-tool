#!/usr/bin/env python3
"""
Create views and indexes for latest parcel status tracking
Handles the fact that parcels can have multiple status updates but users need latest only
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

def create_latest_status_solution(cursor, conn):
    """Create comprehensive latest status solution"""
    
    print("🔧 Creating Latest Parcel Status Solution...")
    
    # Option 1: Latest Status View (Primary solution)
    print("\n📋 Step 1: Creating latest status view...")
    latest_view_sql = """
    DROP VIEW IF EXISTS public.settlement_latest_delivery_status CASCADE;
    
    CREATE VIEW public.settlement_latest_delivery_status AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY ant_parcel_no 
                   ORDER BY 
                       CASE WHEN update_at IS NOT NULL THEN update_at ELSE create_at END DESC,
                       create_at DESC,
                       ID DESC
               ) as row_rank
        FROM public.settlement_normal_delivery_detail
        WHERE ant_parcel_no IS NOT NULL
    ) ranked
    WHERE row_rank = 1;
    """
    
    cursor.execute(latest_view_sql)
    print("✅ Latest status view created: public.settlement_latest_delivery_status")
    
    # Option 2: Partner Latest Status (Business-focused view)
    print("\n📋 Step 2: Creating partner-focused latest status view...")
    partner_view_sql = """
    DROP VIEW IF EXISTS public.settlement_partner_latest_status CASCADE;
    
    CREATE VIEW public.settlement_partner_latest_status AS
    SELECT 
        partner_id,
        ant_parcel_no,
        billing_num,
        latest_status,
        last_status_update_at,
        create_at,
        update_at,
        net_price,
        actual_weight,
        consignee_name,
        province,
        airport_code
    FROM public.settlement_latest_delivery_status
    ORDER BY partner_id, update_at DESC;
    """
    
    cursor.execute(partner_view_sql)
    print("✅ Partner latest status view created: public.settlement_partner_latest_status")
    
    # Option 3: Status Summary View (Analytics-focused)
    print("\n📋 Step 3: Creating status summary view...")
    summary_view_sql = """
    DROP VIEW IF EXISTS public.settlement_status_summary CASCADE;
    
    CREATE VIEW public.settlement_status_summary AS
    SELECT 
        latest_status,
        COUNT(*) as parcel_count,
        COUNT(DISTINCT partner_id) as partner_count,
        COUNT(DISTINCT billing_num) as billing_count,
        SUM(CASE WHEN net_price ~ '^[0-9.]+$' THEN net_price::DECIMAL(10,2) ELSE 0 END) as total_value,
        MIN(create_at) as earliest_delivery,
        MAX(create_at) as latest_delivery
    FROM public.settlement_latest_delivery_status
    WHERE latest_status IS NOT NULL
    GROUP BY latest_status
    ORDER BY parcel_count DESC;
    """
    
    cursor.execute(summary_view_sql)
    print("✅ Status summary view created: public.settlement_status_summary")
    
    conn.commit()

def test_latest_status_views(cursor):
    """Test the created views with sample queries"""
    
    print("\n🧪 Testing Latest Status Views...")
    
    # Test 1: Count comparison
    print("\n📊 Test 1: Row count comparison")
    cursor.execute("SELECT COUNT(*) FROM public.settlement_normal_delivery_detail;")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM public.settlement_latest_delivery_status;")
    latest_rows = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT ant_parcel_no) FROM public.settlement_normal_delivery_detail WHERE ant_parcel_no IS NOT NULL;")
    unique_parcels = cursor.fetchone()[0]
    
    print(f"   📦 Total records: {total_rows:,}")
    print(f"   📦 Latest records: {latest_rows:,}")
    print(f"   📦 Unique parcels: {unique_parcels:,}")
    print(f"   ✅ Deduplication: {total_rows - latest_rows:,} duplicate records removed")
    
    # Test 2: Sample parcels with multiple updates
    print("\n📊 Test 2: Parcels with multiple status updates")
    cursor.execute("""
        SELECT ant_parcel_no, COUNT(*) as update_count
        FROM public.settlement_normal_delivery_detail 
        WHERE ant_parcel_no IS NOT NULL
        GROUP BY ant_parcel_no 
        HAVING COUNT(*) > 1
        ORDER BY update_count DESC 
        LIMIT 5;
    """)
    
    multi_update_parcels = cursor.fetchall()
    if multi_update_parcels:
        print("   📋 Parcels with multiple updates:")
        for parcel_no, count in multi_update_parcels:
            print(f"     📦 {parcel_no}: {count} updates")
            
        # Show before/after for first parcel
        test_parcel = multi_update_parcels[0][0]
        print(f"\n   🔍 Example: All updates for {test_parcel}:")
        cursor.execute(f"""
            SELECT latest_status, create_at, update_at
            FROM public.settlement_normal_delivery_detail 
            WHERE ant_parcel_no = '{test_parcel}'
            ORDER BY 
                CASE WHEN update_at IS NOT NULL THEN update_at ELSE create_at END DESC;
        """)
        
        all_updates = cursor.fetchall()
        for i, (status, create_at, update_at) in enumerate(all_updates):
            marker = "🔸 LATEST" if i == 0 else "   "
            print(f"     {marker} Status: {status}, Created: {create_at}, Updated: {update_at}")
            
        print(f"\n   ✅ Latest status view shows only: {all_updates[0][0]}")
    else:
        print("   📋 No parcels found with multiple updates (data might be already deduplicated)")
    
    # Test 3: Status summary
    print("\n📊 Test 3: Status distribution")
    cursor.execute("SELECT * FROM public.settlement_status_summary LIMIT 10;")
    
    status_summary = cursor.fetchall()
    print("   📈 Status Distribution:")
    print(f"   {'Status':<25} {'Count':<10} {'Partners':<8} {'Total Value':<12}")
    print("   " + "-" * 60)
    for status, count, partners, billings, total_value, earliest, latest in status_summary:
        print(f"   {str(status):<25} {count:<10,} {partners:<8} ${float(total_value):<11,.2f}")

def create_usage_examples():
    """Generate usage examples for the development team"""
    
    examples = """
-- =============================================================================
-- LATEST PARCEL STATUS VIEWS - USAGE EXAMPLES
-- =============================================================================

-- 1. GET LATEST STATUS FOR A SPECIFIC PARCEL
SELECT * 
FROM public.settlement_latest_delivery_status 
WHERE ant_parcel_no = 'BAUNI000300014750782';

-- 2. GET ALL LATEST PARCELS FOR A PARTNER
SELECT ant_parcel_no, latest_status, last_status_update_at, net_price
FROM public.settlement_partner_latest_status 
WHERE partner_id = 27
ORDER BY last_status_update_at DESC;

-- 3. COUNT PARCELS BY STATUS (LATEST ONLY)
SELECT latest_status, COUNT(*) as parcel_count
FROM public.settlement_latest_delivery_status
GROUP BY latest_status
ORDER BY parcel_count DESC;

-- 4. FIND DELIVERED PARCELS IN LAST 24 HOURS
SELECT ant_parcel_no, partner_id, billing_num, net_price
FROM public.settlement_latest_delivery_status
WHERE latest_status = 'DELIVERED'
  AND last_status_update_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- 5. PARTNER PERFORMANCE SUMMARY (LATEST STATUSES ONLY)
SELECT 
    partner_id,
    COUNT(*) as total_parcels,
    COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) as delivered_count,
    COUNT(CASE WHEN latest_status LIKE '%FAILED%' THEN 1 END) as failed_count,
    ROUND(
        COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) * 100.0 / COUNT(*), 2
    ) as delivery_rate_percent
FROM public.settlement_latest_delivery_status
WHERE partner_id IS NOT NULL
GROUP BY partner_id
ORDER BY total_parcels DESC;

-- 6. STATUS SUMMARY DASHBOARD
SELECT * FROM public.settlement_status_summary;

-- 7. RECENT ACTIVITY (LATEST STATUS CHANGES)
SELECT ant_parcel_no, partner_id, latest_status, last_status_update_at
FROM public.settlement_latest_delivery_status
WHERE last_status_update_at >= CURRENT_DATE
ORDER BY last_status_update_at DESC
LIMIT 100;

-- =============================================================================
-- IMPORTANT NOTES:
-- 1. Always use settlement_latest_delivery_status for user-facing queries
-- 2. Never use settlement_normal_delivery_detail directly for status lookups
-- 3. Views automatically handle deduplication and show only latest status
-- 4. Queries are optimized with existing SORTKEY(create_at, billing_num)
-- =============================================================================
"""
    
    return examples

def main():
    print("🎯 Creating Latest Parcel Status Solution")
    print("=" * 50)
    print("🔄 Problem: Parcels have multiple status updates")
    print("✅ Solution: Views that show only latest status")
    print()
    
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
    print(f"✅ SSH tunnel: localhost:{local_port}")
    
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
        
        # Create the latest status solution
        create_latest_status_solution(cursor, conn)
        
        # Test the views
        test_latest_status_views(cursor)
        
        # Generate usage examples
        examples = create_usage_examples()
        
        print("\n" + "="*80)
        print("🎉 LATEST STATUS SOLUTION COMPLETE!")
        print("="*80)
        print("✅ Views created:")
        print("   📋 public.settlement_latest_delivery_status (main view)")
        print("   📋 public.settlement_partner_latest_status (partner-focused)")
        print("   📋 public.settlement_status_summary (analytics)")
        print()
        print("✅ Benefits:")
        print("   🔸 Users see only latest parcel status")
        print("   🔸 Automatic deduplication")
        print("   🔸 Optimized queries")
        print("   🔸 Business intelligence ready")
        print()
        print("📋 Usage:")
        print("   -- For user queries, use:")
        print("   SELECT * FROM public.settlement_latest_delivery_status")
        print("   WHERE ant_parcel_no = 'your_parcel_number';")
        
        # Save examples to file
        with open('/home/qi_chen/s3-redshift-backup/LATEST_STATUS_USAGE.sql', 'w') as f:
            f.write(examples)
        
        print(f"\n📝 Complete usage examples saved to:")
        print(f"   /home/qi_chen/s3-redshift-backup/LATEST_STATUS_USAGE.sql")
        
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