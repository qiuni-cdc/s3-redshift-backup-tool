#!/usr/bin/env python3
"""
Quick check of current loading status
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

def main():
    print("⚡ Quick Status Check")
    print("=" * 25)
    
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
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=local_port,
            database='dw',
            user='chenqi',
            password=password
        )
        
        cursor = conn.cursor()
        
        # Current count
        cursor.execute("SELECT COUNT(*) FROM public.settlement_normal_delivery_detail;")
        count = cursor.fetchone()[0]
        
        print(f"📊 Current rows: {count:,}")
        progress = (count / 2131906) * 100
        print(f"📈 Progress: {progress:.1f}%")
        
        if count > 0:
            cursor.execute("""
                SELECT COUNT(DISTINCT partner_id), MIN(create_at), MAX(create_at)
                FROM public.settlement_normal_delivery_detail;
            """)
            stats = cursor.fetchone()
            print(f"🤝 Partners: {stats[0]}")
            print(f"📅 Date range: {stats[1]} to {stats[2]}")
        
        print(f"\n🎯 Target: 2,131,906 rows")
        print(f"📋 Table: dw.public.settlement_normal_delivery_detail")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        tunnel.stop()

if __name__ == "__main__":
    main()