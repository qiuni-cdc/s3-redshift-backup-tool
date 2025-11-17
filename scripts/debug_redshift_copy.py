#!/usr/bin/env python3
"""
Debug Redshift COPY Issues - No System Table Access Required

Usage:
    python scripts/debug_redshift_copy.py --config config/connections.yml --env us_dw
    python scripts/debug_redshift_copy.py --test-s3 s3://bucket/path/file.parquet
"""

import argparse
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import AppConfig
from src.core.connections import ConnectionManager
from src.utils.redshift_diagnostics import RedshiftDiagnostics
from src.utils.logging import get_logger

logger = get_logger(__name__)


def load_config(config_path: str, env: str) -> AppConfig:
    """Load configuration from file"""
    try:
        config = AppConfig.from_yaml(config_path, env)
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)


def print_section(title: str):
    """Print a formatted section header"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def run_diagnostics(config: AppConfig, test_s3_uri: str = None):
    """Run comprehensive diagnostics"""

    print_section("🔍 Redshift Connection Diagnostics")

    connection_manager = ConnectionManager(config)

    try:
        # Test connection
        print("\n📡 Testing Redshift connection...")

        if hasattr(config, 'redshift_ssh') and config.redshift_ssh.bastion_host:
            print(f"   Using SSH tunnel via {config.redshift_ssh.bastion_host}")

            with connection_manager.redshift_ssh_tunnel() as local_port:
                import psycopg2

                conn = psycopg2.connect(
                    host='localhost',
                    port=local_port,
                    database=config.redshift.database,
                    user=config.redshift.user,
                    password=config.redshift.password.get_secret_value(),
                    connect_timeout=30
                )

                diagnostics = RedshiftDiagnostics(conn)
                run_diagnostic_tests(diagnostics, config, test_s3_uri)

                conn.close()
        else:
            import psycopg2

            print(f"   Using direct connection to {config.redshift.host}")

            conn = psycopg2.connect(
                host=config.redshift.host,
                port=config.redshift.port,
                database=config.redshift.database,
                user=config.redshift.user,
                password=config.redshift.password.get_secret_value(),
                connect_timeout=30
            )

            diagnostics = RedshiftDiagnostics(conn)
            run_diagnostic_tests(diagnostics, config, test_s3_uri)

            conn.close()

        print_section("✅ Diagnostics Complete")

    except Exception as e:
        print(f"\n❌ Diagnostic failed: {e}")
        logger.error(f"Diagnostic error: {e}", exc_info=True)
        sys.exit(1)


def run_diagnostic_tests(diagnostics: RedshiftDiagnostics, config: AppConfig, test_s3_uri: str = None):
    """Run all diagnostic tests"""

    # Test 1: Basic connectivity
    print("\n1️⃣  Basic Connectivity Test")
    print("-" * 60)

    result = diagnostics.test_basic_connectivity()

    if result['connected']:
        print(f"   ✅ Connected successfully")
        print(f"   📊 Database: {result['database']}")
        print(f"   👤 User: {result['user']}")
        print(f"   🆔 Backend PID: {result['pid']}")
        print(f"   🕐 Server Time: {result['server_time']}")
        print(f"   📦 Version: {result['version'][:80]}...")
    else:
        print(f"   ❌ Connection failed: {result.get('error')}")
        return

    # Test 2: Network latency
    print("\n2️⃣  Network Latency Test")
    print("-" * 60)

    latency_result = diagnostics.test_network_latency()

    if latency_result['success']:
        print(f"   ✅ Network latency test passed")
        print(f"   📊 Measurements: {latency_result['measurements_ms']} ms")
        print(f"   📈 Average: {latency_result['avg_latency_ms']} ms")
        print(f"   📊 Min: {latency_result['min_latency_ms']} ms")
        print(f"   📊 Max: {latency_result['max_latency_ms']} ms")

        # Warn if latency is high
        if latency_result['avg_latency_ms'] > 100:
            print(f"   ⚠️  WARNING: High latency detected (>{latency_result['avg_latency_ms']}ms)")
            print(f"      This may cause COPY operations to be slow or timeout")
    else:
        print(f"   ❌ Latency test failed: {latency_result.get('error')}")

    # Test 3: Connection info
    print("\n3️⃣  Connection Details")
    print("-" * 60)

    conn_info = diagnostics.get_connection_info()

    if conn_info['success']:
        info = conn_info['connection_info']
        print(f"   📡 Host: {info['host']}:{info['port']}")
        print(f"   🗄️  Database: {info['database']}")
        print(f"   👤 User: {info['user']}")
        print(f"   🆔 Backend PID: {info['backend_pid']}")
        print(f"   🔢 Server Version: {info['server_version']}")
        print(f"   📝 Encoding: {info['encoding']}")

        # Check transaction status
        status_map = {
            0: 'IDLE',
            1: 'ACTIVE',
            2: 'IN_TRANSACTION',
            3: 'IN_ERROR',
            4: 'UNKNOWN'
        }
        tx_status = status_map.get(info['transaction_status'], 'UNKNOWN')
        print(f"   🔄 Transaction Status: {tx_status}")

    # Test 4: S3 COPY test (if URI provided)
    if test_s3_uri:
        print("\n4️⃣  S3 COPY Test")
        print("-" * 60)
        print(f"   📁 Testing S3 URI: {test_s3_uri}")
        print(f"   ⏱️  Timeout: 60 seconds")

        copy_result = diagnostics.test_s3_copy_simple(
            s3_uri=test_s3_uri,
            aws_access_key=config.s3.access_key,
            aws_secret_key=config.s3.secret_key.get_secret_value(),
            timeout_seconds=60
        )

        if copy_result['success']:
            print(f"   ✅ COPY test PASSED")
            print(f"   📊 Rows loaded: {copy_result['rows_loaded']}")
            print(f"   ⏱️  Time elapsed: {copy_result['elapsed_seconds']}s")
        elif copy_result.get('timed_out'):
            print(f"   ❌ COPY test TIMED OUT after {copy_result['elapsed_seconds']}s")
            print(f"   🔍 Root cause: Network issue or stuck COPY operation")
            print(f"   💡 Recommendation:")
            print(f"      - Check SSH tunnel stability")
            print(f"      - Verify S3 file is accessible")
            print(f"      - Check Redshift cluster is not paused")
        else:
            print(f"   ❌ COPY test FAILED")
            print(f"   ⚠️  Error: {copy_result.get('error')}")
            print(f"   ⏱️  Time elapsed: {copy_result['elapsed_seconds']}s")

    # Test 5: Check existing table data (example)
    print("\n5️⃣  Table Verification")
    print("-" * 60)

    # Check a sample table if schema is configured
    if hasattr(config.redshift, 'schema'):
        schema = config.redshift.schema
        print(f"   📋 Schema: {schema}")

        # List tables
        try:
            with diagnostics.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = %s
                    ORDER BY tablename
                    LIMIT 10
                """, (schema,))

                tables = cursor.fetchall()

                if tables:
                    print(f"   ✅ Found {len(tables)} tables in schema '{schema}':")
                    for table in tables[:5]:
                        print(f"      • {table[0]}")

                    if len(tables) > 5:
                        print(f"      ... and {len(tables) - 5} more")
                else:
                    print(f"   ⚠️  No tables found in schema '{schema}'")

        except Exception as e:
            print(f"   ❌ Failed to list tables: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Debug Redshift COPY issues without system table access'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='config/connections.yml',
        help='Path to connections config file'
    )

    parser.add_argument(
        '--env',
        type=str,
        default='us_dw',
        help='Environment name (e.g., us_dw, us_prod)'
    )

    parser.add_argument(
        '--test-s3',
        type=str,
        help='S3 URI to test COPY operation (e.g., s3://bucket/path/file.parquet)'
    )

    args = parser.parse_args()

    # Load configuration
    print(f"📁 Loading config from: {args.config}")
    print(f"🌍 Environment: {args.env}")

    config = load_config(args.config, args.env)

    # Run diagnostics
    run_diagnostics(config, test_s3_uri=args.test_s3)


if __name__ == '__main__':
    main()
