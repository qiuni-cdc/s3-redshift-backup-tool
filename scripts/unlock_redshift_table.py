#!/usr/bin/env python3
"""
Redshift Lock Management Tool

Usage:
    # Check locks
    python scripts/unlock_redshift_table.py --env redshift_default --check

    # Kill specific PID
    python scripts/unlock_redshift_table.py --env redshift_default --kill 1073815845

    # Auto-kill long-running locks (>5 minutes)
    python scripts/unlock_redshift_table.py --env redshift_default --auto-kill --timeout 300
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import AppConfig
from src.core.connections import ConnectionManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


def print_section(title: str):
    """Print a formatted section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def check_locks(conn):
    """Check current locks in Redshift"""
    print_section("🔍 Current Locks")

    try:
        with conn.cursor() as cursor:
            # Get locks with table names
            cursor.execute("""
                SELECT
                    l.database,
                    COALESCE(c.relname, 'N/A') as table_name,
                    COALESCE(n.nspname, 'N/A') as schema_name,
                    l.pid,
                    l.granted
                FROM pg_locks l
                LEFT JOIN pg_class c ON l.relation = c.oid
                LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
                ORDER BY l.pid, l.granted
            """)

            locks = cursor.fetchall()

            if not locks:
                print("   ✅ No locks found")
                return []

            print(f"\n   Found {len(locks)} locks:\n")
            print(f"   {'Database':<12} {'Schema':<12} {'Table':<25} {'PID':<12} {'Granted':<8}")
            print(f"   {'-'*12} {'-'*12} {'-'*25} {'-'*12} {'-'*8}")

            lock_info = []
            for lock in locks:
                database, table_name, schema_name, pid, granted = lock
                print(f"   {str(database):<12} {schema_name:<12} {table_name:<25} {str(pid):<12} {str(granted):<8}")
                lock_info.append({
                    'database': database,
                    'table': table_name,
                    'schema': schema_name,
                    'pid': pid,
                    'granted': granted
                })

            # Check current session PID
            cursor.execute("SELECT pg_backend_pid()")
            my_pid = cursor.fetchone()[0]
            print(f"\n   Your current session PID: {my_pid}")

            return lock_info

    except Exception as e:
        logger.error(f"Failed to check locks: {e}")
        return []


def get_lock_details(conn, pid):
    """Get detailed information about a specific lock"""
    print_section(f"🔎 Lock Details for PID {pid}")

    try:
        with conn.cursor() as cursor:
            # Try to get query information (may not work without permissions)
            try:
                cursor.execute("""
                    SELECT
                        query,
                        SUBSTRING(querytxt, 1, 150) as query_text,
                        starttime,
                        endtime,
                        DATEDIFF(second, starttime, COALESCE(endtime, GETDATE())) as duration_seconds,
                        CASE WHEN endtime IS NULL THEN 'RUNNING' ELSE 'COMPLETED' END as status
                    FROM stl_query
                    WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
                    ORDER BY starttime DESC
                    LIMIT 10
                """)

                queries = cursor.fetchall()

                if queries:
                    print("\n   Recent queries from your user:\n")
                    for q in queries[:5]:
                        query_id, query_text, start, end, duration, status = q
                        print(f"   Query {query_id}: {status}")
                        print(f"   Duration: {duration}s")
                        print(f"   Text: {query_text}")
                        print(f"   Started: {start}")
                        print()
                else:
                    print("   ℹ️  No recent queries found (may lack permissions)")

            except Exception as e:
                print(f"   ⚠️  Cannot access query history: {e}")
                print(f"   This is normal if you don't have access to stl_query")

    except Exception as e:
        logger.error(f"Failed to get lock details: {e}")


def cancel_backend(conn, pid, terminate=False):
    """Cancel or terminate a backend process"""
    action = "terminate" if terminate else "cancel"
    print_section(f"🛑 Attempting to {action} PID {pid}")

    try:
        with conn.cursor() as cursor:
            # Check if it's the current session
            cursor.execute("SELECT pg_backend_pid()")
            my_pid = cursor.fetchone()[0]

            if pid == my_pid:
                print(f"   ⚠️  WARNING: This is your current session!")
                print(f"   💡 RECOMMENDATION: Instead of terminating your own session,")
                print(f"      you should run ROLLBACK or COMMIT to release locks.")
                print(f"")
                print(f"   In your SQL client, run:")
                print(f"      ROLLBACK;  -- to cancel changes and release locks")
                print(f"      -- OR")
                print(f"      COMMIT;    -- to save changes and release locks")
                print(f"")
                confirm = input(f"   Do you still want to {action} your own session? (yes/no): ")
                if confirm.lower() != 'yes':
                    print("   ❌ Operation cancelled")
                    print("   ✅ Please run ROLLBACK or COMMIT in your SQL session instead")
                    return False

            # Execute cancel or terminate
            func = "pg_terminate_backend" if terminate else "pg_cancel_backend"
            cursor.execute(f"SELECT {func}(%s)", (pid,))
            result = cursor.fetchone()[0]

            if result:
                print(f"   ✅ Successfully {action}ed PID {pid}")
                return True
            else:
                print(f"   ❌ Failed to {action} PID {pid} (process may not exist)")
                return False

    except Exception as e:
        logger.error(f"Failed to {action} backend: {e}")
        return False


def auto_kill_long_locks(conn, timeout_seconds=300):
    """Automatically kill locks that have been held for too long"""
    print_section(f"🤖 Auto-killing locks older than {timeout_seconds}s")

    try:
        with conn.cursor() as cursor:
            # Get current session PID
            cursor.execute("SELECT pg_backend_pid()")
            my_pid = cursor.fetchone()[0]

            # Try to find long-running queries
            try:
                cursor.execute("""
                    SELECT
                        query,
                        DATEDIFF(second, starttime, GETDATE()) as duration_seconds
                    FROM stl_query
                    WHERE endtime IS NULL
                      AND starttime < CURRENT_TIMESTAMP - INTERVAL '%s seconds'
                    ORDER BY starttime
                """ % timeout_seconds)

                long_queries = cursor.fetchall()

                if not long_queries:
                    print(f"   ✅ No queries running longer than {timeout_seconds}s")
                    return

                print(f"   Found {len(long_queries)} long-running queries")

                # Note: Without stv_recents access, we can't map query to PID
                # This is a limitation
                print(f"   ⚠️  Cannot auto-kill without system table access")
                print(f"   Please manually identify and kill PIDs using --kill <pid>")

            except Exception as e:
                print(f"   ⚠️  Cannot access query information: {e}")
                print(f"   Auto-kill requires stl_query access")

    except Exception as e:
        logger.error(f"Auto-kill failed: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Redshift Lock Management Tool'
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
        required=True,
        help='Environment name (e.g., redshift_default)'
    )

    parser.add_argument(
        '--check',
        action='store_true',
        help='Check current locks'
    )

    parser.add_argument(
        '--kill',
        type=int,
        metavar='PID',
        help='Kill a specific PID'
    )

    parser.add_argument(
        '--cancel',
        type=int,
        metavar='PID',
        help='Cancel queries from a specific PID (safer than kill)'
    )

    parser.add_argument(
        '--auto-kill',
        action='store_true',
        help='Automatically kill long-running locks'
    )

    parser.add_argument(
        '--timeout',
        type=int,
        default=300,
        help='Timeout in seconds for auto-kill (default: 300)'
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = AppConfig.from_yaml(args.config, args.env)
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        sys.exit(1)

    # Connect to Redshift
    connection_manager = ConnectionManager(config)

    try:
        if hasattr(config, 'redshift_ssh') and config.redshift_ssh.bastion_host:
            print(f"📡 Connecting via SSH tunnel to {config.redshift_ssh.bastion_host}...")
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

                execute_operations(conn, args)
                conn.close()
        else:
            import psycopg2
            print(f"📡 Connecting directly to {config.redshift.host}...")
            conn = psycopg2.connect(
                host=config.redshift.host,
                port=config.redshift.port,
                database=config.redshift.database,
                user=config.redshift.user,
                password=config.redshift.password.get_secret_value(),
                connect_timeout=30
            )

            execute_operations(conn, args)
            conn.close()

    except Exception as e:
        print(f"❌ Failed to connect to Redshift: {e}")
        sys.exit(1)


def execute_operations(conn, args):
    """Execute the requested operations"""

    # Default to --check if no operation specified
    if not any([args.check, args.kill, args.cancel, args.auto_kill]):
        args.check = True

    if args.check:
        locks = check_locks(conn)

        # If locks found, show details for each PID
        if locks:
            unique_pids = set(lock['pid'] for lock in locks)
            for pid in unique_pids:
                get_lock_details(conn, pid)

    if args.cancel:
        cancel_backend(conn, args.cancel, terminate=False)

    if args.kill:
        cancel_backend(conn, args.kill, terminate=True)

    if args.auto_kill:
        auto_kill_long_locks(conn, args.timeout)

    print_section("✅ Operation Complete")


if __name__ == '__main__':
    main()
