"""
Redshift Diagnostics - No system table permissions required

This module provides diagnostic tools that work with regular user permissions.
"""

import time
import signal
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Dict, Any
import psycopg2

from src.utils.logging import get_logger

logger = get_logger(__name__)


class TimeoutException(Exception):
    """Exception raised when operation times out"""
    pass


def timeout_handler(signum, frame):
    """Signal handler for timeout"""
    raise TimeoutException("Operation timed out")


@contextmanager
def query_timeout(seconds: int):
    """
    Context manager for query timeout using signals.

    Usage:
        with query_timeout(300):  # 5 minutes
            cursor.execute("COPY ...")
    """
    # Set the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        # Cancel the alarm and restore old handler
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


class RedshiftDiagnostics:
    """Diagnostic tools for Redshift without system table access"""

    def __init__(self, connection):
        """
        Initialize diagnostics with a database connection.

        Args:
            connection: psycopg2 connection object
        """
        self.conn = connection
        self.logger = logger

    def test_basic_connectivity(self) -> Dict[str, Any]:
        """
        Test basic database connectivity and get session info.

        Returns:
            Dict with connection status and info
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        current_database() as database,
                        current_user as user,
                        pg_backend_pid() as pid,
                        version() as version,
                        now() as server_time
                """)
                result = cursor.fetchone()

                return {
                    'connected': True,
                    'database': result[0],
                    'user': result[1],
                    'pid': result[2],
                    'version': result[3],
                    'server_time': result[4]
                }
        except Exception as e:
            self.logger.error(f"Connectivity test failed: {e}")
            return {
                'connected': False,
                'error': str(e)
            }

    def test_s3_copy_simple(self, s3_uri: str, aws_access_key: str,
                           aws_secret_key: str, timeout_seconds: int = 60) -> Dict[str, Any]:
        """
        Test a simple COPY operation with timeout to detect stuck operations.

        Args:
            s3_uri: S3 URI of file to test
            aws_access_key: AWS access key
            aws_secret_key: AWS secret key
            timeout_seconds: Timeout in seconds (default 60)

        Returns:
            Dict with test results
        """
        start_time = time.time()

        # Create temporary test table
        test_table = f"_copy_test_{int(time.time())}"

        try:
            with self.conn.cursor() as cursor:
                # Create minimal test table
                self.logger.info(f"Creating test table: {test_table}")
                cursor.execute(f"""
                    CREATE TEMP TABLE {test_table} (
                        id INTEGER,
                        data VARCHAR(100)
                    )
                """)

                # Try COPY with timeout
                copy_cmd = f"""
                    COPY {test_table}
                    FROM '{s3_uri}'
                    ACCESS_KEY_ID '{aws_access_key}'
                    SECRET_ACCESS_KEY '{aws_secret_key}'
                    FORMAT AS PARQUET
                    MAXERROR 1
                """

                self.logger.info(f"Testing COPY from {s3_uri} with {timeout_seconds}s timeout")

                try:
                    with query_timeout(timeout_seconds):
                        cursor.execute(copy_cmd)

                        # Get row count
                        cursor.execute("SELECT pg_last_copy_count()")
                        rows = cursor.fetchone()[0]

                        elapsed = time.time() - start_time

                        return {
                            'success': True,
                            'rows_loaded': rows,
                            'elapsed_seconds': round(elapsed, 2),
                            'timed_out': False
                        }

                except TimeoutException:
                    self.logger.error(f"COPY operation timed out after {timeout_seconds}s")

                    # Try to cancel the query
                    try:
                        self.conn.cancel()
                    except:
                        pass

                    return {
                        'success': False,
                        'error': f'COPY timed out after {timeout_seconds}s',
                        'timed_out': True,
                        'elapsed_seconds': timeout_seconds
                    }

        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"COPY test failed: {e}")

            return {
                'success': False,
                'error': str(e),
                'elapsed_seconds': round(elapsed, 2),
                'timed_out': False
            }
        finally:
            # Cleanup test table
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
            except:
                pass

    def check_table_data(self, schema: str, table_name: str) -> Dict[str, Any]:
        """
        Check table data and metadata.

        Args:
            schema: Schema name
            table_name: Table name

        Returns:
            Dict with table info
        """
        try:
            with self.conn.cursor() as cursor:
                # Check if table exists
                cursor.execute("""
                    SELECT
                        schemaname,
                        tablename,
                        tableowner
                    FROM pg_tables
                    WHERE schemaname = %s
                      AND tablename = %s
                """, (schema, table_name))

                table_info = cursor.fetchone()

                if not table_info:
                    return {
                        'exists': False,
                        'schema': schema,
                        'table': table_name
                    }

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                row_count = cursor.fetchone()[0]

                # Get sample data
                cursor.execute(f"SELECT * FROM {schema}.{table_name} LIMIT 5")
                sample_rows = cursor.fetchall()

                return {
                    'exists': True,
                    'schema': table_info[0],
                    'table': table_info[1],
                    'owner': table_info[2],
                    'row_count': row_count,
                    'sample_rows': len(sample_rows)
                }

        except Exception as e:
            self.logger.error(f"Failed to check table {schema}.{table_name}: {e}")
            return {
                'exists': False,
                'error': str(e)
            }

    def test_network_latency(self) -> Dict[str, Any]:
        """
        Test network latency to Redshift.

        Returns:
            Dict with latency measurements
        """
        measurements = []

        try:
            for i in range(5):
                start = time.time()

                with self.conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

                elapsed = (time.time() - start) * 1000  # Convert to ms
                measurements.append(elapsed)

                time.sleep(0.1)  # Small delay between tests

            return {
                'success': True,
                'measurements_ms': [round(m, 2) for m in measurements],
                'avg_latency_ms': round(sum(measurements) / len(measurements), 2),
                'max_latency_ms': round(max(measurements), 2),
                'min_latency_ms': round(min(measurements), 2)
            }

        except Exception as e:
            self.logger.error(f"Network latency test failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get detailed connection information.

        Returns:
            Dict with connection details
        """
        try:
            info = {
                'host': self.conn.get_dsn_parameters().get('host'),
                'port': self.conn.get_dsn_parameters().get('port'),
                'database': self.conn.get_dsn_parameters().get('dbname'),
                'user': self.conn.get_dsn_parameters().get('user'),
                'server_version': self.conn.server_version,
                'protocol_version': self.conn.protocol_version,
                'backend_pid': self.conn.get_backend_pid(),
                'transaction_status': self.conn.get_transaction_status(),
                'encoding': self.conn.encoding
            }

            return {
                'success': True,
                'connection_info': info
            }

        except Exception as e:
            self.logger.error(f"Failed to get connection info: {e}")
            return {
                'success': False,
                'error': str(e)
            }
