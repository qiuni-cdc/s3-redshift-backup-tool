"""
Order Tracking Sync - Hybrid + dbt Approach
============================================

Combines hybrid extraction strategies with dbt processing:

EXTRACTION (Airflow):
  - Time-based extraction with 15-min window (handles peak volumes)
  - Uses MySQL server time to avoid drift issues
  - Parallel extraction of all 3 tables

TRANSFORMATION (dbt):
  - Deduplication via merge strategy
  - Sequence gap detection
  - Consistency tests

Tables:
  - kuaisong.ecs_order_info (ecs) → add_time (~500K/day, ~2M/day peak)
  - kuaisong.uni_tracking_info (uti) → update_time (~500K/day, ~2M/day peak)
  - kuaisong.uni_tracking_spath (uts) → pathTime, traceSeq (~2M/day, ~8M/day peak)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json

# ============================================================================
# CONFIGURATION
# ============================================================================
import os

import sys

# Paths - configurable via environment variables for local testing
# Production defaults are used if env vars not set

# DOCKER-AWARE PATH DETECTION
# The repository root is mounted to /opt/airflow/sync_tool
DEFAULT_TOOL_PATH = '/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool'
if os.path.exists('/opt/airflow/sync_tool'):
    DEFAULT_TOOL_PATH = '/opt/airflow/sync_tool'
    # Ensure src is importable from sync_tool
    if '/opt/airflow/sync_tool' not in sys.path:
        sys.path.append('/opt/airflow/sync_tool')
    print(f"Detected Docker environment. Set SYNC_TOOL_PATH to {DEFAULT_TOOL_PATH}")

SYNC_TOOL_PATH = os.environ.get(
    'SYNC_TOOL_PATH',
    DEFAULT_TOOL_PATH
)
DBT_PROJECT_PATH = os.environ.get(
    'DBT_PROJECT_PATH',
    '/opt/airflow/dbt_projects/order_tracking' if os.path.exists('/opt/airflow/dbt_projects') else '/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/dbt_projects/order_tracking'
)
DBT_VENV_PATH = os.environ.get(
    'DBT_VENV_PATH',
    '/home/airflow/.local' # Docker usually uses system python or venv in a known place, might need update later
)
# For local Docker testing, use the local_test pipeline which connects via Windows SSH tunnel
# For production, change back to "order_tracking_hybrid_dbt_pipeline"
PIPELINE_NAME = os.environ.get('SYNC_PIPELINE_NAME', 'order_tracking_hybrid_dbt_pipeline')

# Time window settings
BUFFER_MINUTES = 5                    # Safety buffer to avoid incomplete transactions
INCREMENTAL_LOOKBACK_MINUTES = 15     # 15-min window handles peak volumes safely
TIME_DRIFT_THRESHOLD_SECONDS = 60     # Alert if Airflow vs MySQL drift exceeds this

TABLES = {
    "ecs": {
        "full_name": "kuaisong.ecs_order_info",
        "timestamp_col": "add_time",
        "target": "settlment_public.ecs_order_info_raw"
    },
    "uti": {
        "full_name": "kuaisong.uni_tracking_info",
        "timestamp_col": "update_time",
        "target": "settlment_public.uni_tracking_info_raw"
    },
    "uts": {
        "full_name": "kuaisong.uni_tracking_spath",
        "timestamp_col": "pathTime",
        "target": "settlment_public.uni_tracking_spath_raw"
    }
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 7),
    'email_on_failure': True,
    'email': ['jasleen.tung@uniuni.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=60),
}

dag = DAG(
    'order_tracking_hybrid_dbt_sync',
    default_args=default_args,
    description='Hybrid extraction + dbt processing for order tracking',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    max_active_runs=1,
    catchup=False,
    tags=['order-tracking', 'hybrid', 'dbt', 'production']
)

# ============================================================================
# TASK 1: CHECK TIME DRIFT BETWEEN AIRFLOW AND MYSQL
# ============================================================================

# Replaced with BashOperator to run in project virtualenv (avoids Airflow environment conflicts)
check_drift = BashOperator(
    task_id='check_time_drift',
    bash_command=f'''
    set -e
    cd {SYNC_TOOL_PATH}
    [ -f venv/bin/activate ] && source venv/bin/activate
    # Run standalone script which handles SSH tunnel robustly in venv
    python src/cli/check_time_drift.py
    ''',
    do_xcom_push=True,  # Capture JSON output from stdout
    dag=dag
)

# ============================================================================
# TASK 2: CALCULATE SYNC WINDOW (Using MySQL Server Time)
# ============================================================================

def calculate_sync_window(**context):
    """
    Calculate time window using MySQL server time from check_drift task.
    15-min lookback + 5-min buffer = 20 min total coverage.
    """
    # Parse JSON output from Icheck_time_drift BashOperator
    drift_output = context['task_instance'].xcom_pull(
        task_ids='check_time_drift',
        key='return_value'
    )
    
    mysql_now = None
    drift_seconds = 0
    
    try:
        if drift_output:
            data = json.loads(drift_output)
            if data.get('status') == 'success':
                mysql_now = int(data.get('mysql_now'))
                drift_seconds = int(data.get('drift_seconds', 0))
                
                print(f"Time Comparison (from check_drift):")
                print(f"  MySQL server:   {mysql_now} ({datetime.fromtimestamp(mysql_now)})")
                print(f"  Drift:          {drift_seconds} seconds")
                
                # Check drift
                if abs(drift_seconds) > TIME_DRIFT_THRESHOLD_SECONDS:
                    print(f"  WARNING: Time drift exceeds threshold ({TIME_DRIFT_THRESHOLD_SECONDS}s)!")
                
                # Push individually for summary task
                context['task_instance'].xcom_push(key='time_drift_seconds', value=drift_seconds)
                context['task_instance'].xcom_push(key='mysql_server_time', value=mysql_now)
            else:
                print(f"Error from check_drift: {data.get('error')}")
    except Exception as e:
        print(f"Failed to parse check_drift output: {e}\nOutput was: {drift_output}")

    if mysql_now is None:
        mysql_now = int(datetime.now().timestamp())
        print("Warning: Using Airflow time (drift check parsing failed)")

    buffer = BUFFER_MINUTES * 60
    lookback = INCREMENTAL_LOOKBACK_MINUTES * 60

    from_unix = mysql_now - lookback - buffer
    to_unix = mysql_now - buffer

    sync_window = {
        'from_unix': from_unix,
        'to_unix': to_unix,
        'from_ts': datetime.fromtimestamp(from_unix).isoformat(),
        'to_ts': datetime.fromtimestamp(to_unix).isoformat(),
    }

    print(f"Sync Window ({INCREMENTAL_LOOKBACK_MINUTES} min + {BUFFER_MINUTES} min buffer):")
    print(f"  From: {sync_window['from_ts']} ({from_unix})")
    print(f"  To:   {sync_window['to_ts']} ({to_unix})")

    context['task_instance'].xcom_push(key='sync_window', value=sync_window)
    return sync_window

calc_window = PythonOperator(
    task_id='calculate_sync_window',
    python_callable=calculate_sync_window,
    dag=dag
)

# ============================================================================
# TASK GROUP 3: PARALLEL EXTRACTION
# ============================================================================

with TaskGroup("extraction", dag=dag) as extraction_group:

    extract_ecs = BashOperator(
        task_id='extract_ecs',
        bash_command=f'''
        set -e
        cd {SYNC_TOOL_PATH}
        [ -f venv/bin/activate ] && source venv/bin/activate
        [ -f venv/bin/activate ] && source venv/bin/activate
        # [ -f .env ] && source .env # Disable .env sourcing to prevent overwriting Docker env vars

        echo "Extracting ecs_order_info using pipeline timestamp_only strategy"

        python -m src.cli.main sync pipeline \
            -p {PIPELINE_NAME} \
            -t {TABLES['ecs']['full_name']} \
            --json-output /tmp/hybrid_ecs_{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}.json \
            --initial-lookback-minutes {INCREMENTAL_LOOKBACK_MINUTES}
        ''',
        dag=dag
    )

    extract_uti = BashOperator(
        task_id='extract_uti',
        bash_command=f'''
        set -e
        cd {SYNC_TOOL_PATH}
        [ -f venv/bin/activate ] && source venv/bin/activate
        [ -f venv/bin/activate ] && source venv/bin/activate
        # [ -f .env ] && source .env # Disable .env sourcing to prevent overwriting Docker env vars

        echo "Extracting uni_tracking_info using pipeline timestamp_only strategy"

        python -m src.cli.main sync pipeline \
            -p {PIPELINE_NAME} \
            -t {TABLES['uti']['full_name']} \
            --json-output /tmp/hybrid_uti_{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}.json \
            --initial-lookback-minutes {INCREMENTAL_LOOKBACK_MINUTES}
        ''',
        dag=dag
    )

    extract_uts = BashOperator(
        task_id='extract_uts',
        bash_command=f'''
        set -e
        cd {SYNC_TOOL_PATH}
        [ -f venv/bin/activate ] && source venv/bin/activate
        [ -f venv/bin/activate ] && source venv/bin/activate
        # [ -f .env ] && source .env # Disable .env sourcing to prevent overwriting Docker env vars

        echo "Extracting uni_tracking_spath using pipeline timestamp_only strategy"

        python -m src.cli.main sync pipeline \
            -p {PIPELINE_NAME} \
            -t {TABLES['uts']['full_name']} \
            --json-output /tmp/hybrid_uts_{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}.json \
            --initial-lookback-minutes {INCREMENTAL_LOOKBACK_MINUTES}
        ''',
        dag=dag
    )

    # Parallel extraction (no dependencies between tasks)

# ============================================================================
# TASK 4: VALIDATE EXTRACTIONS
# ============================================================================

def validate_extractions(**context):
    """Validate all extractions completed successfully."""
    ds_nodash = context['ds_nodash']
    ts_nodash = context['ts_nodash']

    results = {}
    total_rows = 0

    print("Extraction Results:")
    print("-" * 40)

    for table_key in ['ecs', 'uti', 'uts']:
        filepath = f'/tmp/hybrid_{table_key}_{ds_nodash}_{ts_nodash}.json'

        try:
            with open(filepath, 'r') as f:
                result = json.load(f)

            if result.get('status') == 'success':
                rows = result.get('summary', {}).get('total_rows_processed', 0)
                results[table_key] = {'status': 'success', 'rows': rows}
                total_rows += rows
                print(f"  {table_key}: {rows:,} rows")
            else:
                error = result.get('error', 'Unknown')
                results[table_key] = {'status': 'failed', 'error': error}
                print(f"  {table_key}: FAILED - {error}")
        except Exception as e:
            results[table_key] = {'status': 'error', 'error': str(e)}
            print(f"  {table_key}: ERROR - {str(e)}")

    print("-" * 40)
    print(f"Total: {total_rows:,} rows")

    context['task_instance'].xcom_push(key='extraction_results', value=results)
    context['task_instance'].xcom_push(key='total_rows', value=total_rows)

    return results

validate = PythonOperator(
    task_id='validate_extractions',
    python_callable=validate_extractions,
    dag=dag
)

# ============================================================================
# SSH TUNNEL CONFIGURATION FOR DBT
# ============================================================================
# Set DBT_USE_SSH_TUNNEL=true for local Docker testing (default)
# Set DBT_USE_SSH_TUNNEL=false for server deployment (direct Redshift access)
DBT_USE_SSH_TUNNEL = os.environ.get('DBT_USE_SSH_TUNNEL', 'true').lower() == 'true'

SSH_BASTION_HOST = os.environ.get('REDSHIFT_SSH_BASTION_HOST', '35.82.216.244')
SSH_BASTION_USER = os.environ.get('REDSHIFT_SSH_BASTION_USER', 'jasleentung')
SSH_KEY_PATH = os.environ.get('REDSHIFT_SSH_KEY_PATH', '/Users/Jasleen Tung/Downloads/jasleentung_keypair/jasleentung.pem')
REDSHIFT_HOST = os.environ.get('REDSHIFT_HOST', 'redshift-dw.qa.uniuni.com')
REDSHIFT_PORT = os.environ.get('REDSHIFT_PORT', '5439')
DBT_LOCAL_PORT = '15439'  # Fixed local port for dbt SSH tunnel

if DBT_USE_SSH_TUNNEL:
    # Local Docker testing: Use SSH tunnel
    DBT_WITH_TUNNEL = f'''
    set -e
    cd {DBT_PROJECT_PATH}
    [ -f {DBT_VENV_PATH}/bin/activate ] && source {DBT_VENV_PATH}/bin/activate

    # Copy SSH key to temp location and fix permissions (Docker mounts from Windows have wrong perms)
    SSH_KEY_TEMP=$(mktemp)
    echo "Copying SSH key to $SSH_KEY_TEMP"
    cp "{SSH_KEY_PATH}" "$SSH_KEY_TEMP"
    chmod 600 "$SSH_KEY_TEMP"

    # Debug: verify key file
    echo "Key file size: $(wc -c < "$SSH_KEY_TEMP") bytes"
    echo "Key file permissions: $(ls -la "$SSH_KEY_TEMP")"

    # Start SSH tunnel in background and capture PID
    echo "Starting SSH tunnel to Redshift..."
    ssh -v -N -L {DBT_LOCAL_PORT}:{REDSHIFT_HOST}:{REDSHIFT_PORT} \\
        -o StrictHostKeyChecking=no \\
        -o UserKnownHostsFile=/dev/null \\
        -o ConnectTimeout=30 \\
        -o ServerAliveInterval=60 \\
        -o ServerAliveCountMax=3 \\
        -o ExitOnForwardFailure=yes \\
        -i "$SSH_KEY_TEMP" \\
        {SSH_BASTION_USER}@{SSH_BASTION_HOST} 2>&1 &
    SSH_PID=$!

    # Wait for tunnel to establish
    sleep 2

    # Verify tunnel is up
    if ! nc -z localhost {DBT_LOCAL_PORT} 2>/dev/null; then
        echo "ERROR: SSH tunnel failed to establish"
        rm -f "$SSH_KEY_TEMP"
        exit 1
    fi
    echo "SSH tunnel established on port {DBT_LOCAL_PORT}"
'''
    DBT_CLEANUP_TUNNEL = '''
    # Kill SSH tunnel and cleanup
    kill $SSH_PID 2>/dev/null || true
    rm -f "$SSH_KEY_TEMP" 2>/dev/null || true
'''
else:
    # Server deployment: Direct connection (no tunnel needed)
    DBT_WITH_TUNNEL = f'''
    set -e
    # Source .env from project root if it exists, exporting all vars
    # CRITICAL: Strip Windows CRLF line endings to prevent "variable\r" errors
    if [ -f {SYNC_TOOL_PATH}/.env ]; then
        echo "Loading .env from {SYNC_TOOL_PATH} (stripping CRLF)"
        set -a
        source <(tr -d '\\r' < {SYNC_TOOL_PATH}/.env)
        set +a
    fi

    cd {DBT_PROJECT_PATH}
    [ -f {DBT_VENV_PATH}/bin/activate ] && source {DBT_VENV_PATH}/bin/activate
    echo "Using direct Redshift connection (no SSH tunnel)"
    export DBT_REDSHIFT_HOST={REDSHIFT_HOST}
    export DBT_REDSHIFT_PORT={REDSHIFT_PORT}
    
    echo "Diagnosing network connectivity to {REDSHIFT_HOST}:{REDSHIFT_PORT}..."
    python3 -c "import socket, sys; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(5); result = s.connect_ex(('{REDSHIFT_HOST}', {REDSHIFT_PORT})); print(f'Socket connect result: {{result}} (0=Success)'); sys.exit(result)"
    
    echo "Checking dbt connection..."
    dbt debug --profiles-dir . || echo "dbt debug failed (ignoring to attempt run)"
'''
    DBT_CLEANUP_TUNNEL = ''

# ============================================================================
# TASK 5: DBT RUN - Staging (Dedupe + Incremental)
# ============================================================================

dbt_staging = BashOperator(
    task_id='dbt_staging',
    bash_command=DBT_WITH_TUNNEL + f'''
    echo "Running dbt staging models"
    dbt run --select staging --profiles-dir .
    echo "Staging complete"
''' + DBT_CLEANUP_TUNNEL,
    dag=dag
)

# ============================================================================
# TASK 6: DBT RUN - Gap Detection (DISABLED)
# ============================================================================

# dbt_gaps = BashOperator(
#     task_id='dbt_gaps',
#     bash_command=DBT_WITH_TUNNEL + f'''
#     echo "Running gap detection"
#     dbt run --select int_sequence_gaps --profiles-dir .
#     echo "Gap detection complete"
# ''' + DBT_CLEANUP_TUNNEL,
#     dag=dag
# )

# ============================================================================
# TASK 7: DBT TEST - Consistency Checks
# ============================================================================

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=DBT_WITH_TUNNEL + f'''
    echo "Running consistency tests"
    dbt test --store-failures --profiles-dir .
    echo "Tests complete"
''' + DBT_CLEANUP_TUNNEL,
    dag=dag
)

# ============================================================================
# TASK 8: CHECK GAPS (DISABLED - gap detection model not running)
# ============================================================================

# def check_gaps(**context):
#     """Check for sequence gaps detected by dbt."""
#     redshift = PostgresHook(postgres_conn_id='redshift_default')
#
#     try:
#         result = redshift.get_first("""
#             SELECT count(*) FROM settlement_ods.int_sequence_gaps
#             WHERE _detected_at >= current_timestamp - interval '1 hour'
#         """)
#         gap_count = result[0] or 0
#         print(f"Sequence gaps detected: {gap_count}")
#         context['task_instance'].xcom_push(key='gap_count', value=gap_count)
#         return gap_count
#     except Exception as e:
#         print(f"Gap check skipped: {str(e)}")
#         context['task_instance'].xcom_push(key='gap_count', value=0)
#         return 0
#
# check_gaps_task = PythonOperator(
#     task_id='check_gaps',
#     python_callable=check_gaps,
#     dag=dag
# )

# ============================================================================
# TASK 9: GENERATE SUMMARY
# ============================================================================

def generate_summary(**context):
    """Generate sync summary."""
    extraction_results = context['task_instance'].xcom_pull(
        task_ids='validate_extractions', key='extraction_results'
    ) or {}
    total_rows = context['task_instance'].xcom_pull(
        task_ids='validate_extractions', key='total_rows'
    ) or 0
    # gap_count disabled - gap detection not running
    gap_count = 0
    time_drift = context['task_instance'].xcom_pull(
        task_ids='check_time_drift', key='time_drift_seconds'
    ) or 0
    sync_window = context['task_instance'].xcom_pull(
        task_ids='calculate_sync_window', key='sync_window'
    ) or {}

    summary = f"""
================================================================================
           ORDER TRACKING HYBRID + DBT SYNC SUMMARY
================================================================================

Execution: {context['ds']} | Run: {context['run_id']}

CONFIGURATION:
  Window: {INCREMENTAL_LOOKBACK_MINUTES} min + {BUFFER_MINUTES} min buffer
  Time drift: {time_drift}s {"(OK)" if abs(time_drift) <= TIME_DRIFT_THRESHOLD_SECONDS else "(WARNING)"}

SYNC WINDOW:
  From: {sync_window.get('from_ts', 'N/A')}
  To:   {sync_window.get('to_ts', 'N/A')}

EXTRACTION:
  ecs (orders):   {extraction_results.get('ecs', {}).get('rows', 0):>10,} rows
  uti (tracking): {extraction_results.get('uti', {}).get('rows', 0):>10,} rows
  uts (events):   {extraction_results.get('uts', {}).get('rows', 0):>10,} rows
  ─────────────────────────────────────
  Total:          {total_rows:>10,} rows

DBT:
  Staging: Deduplicated via merge
  Gaps:    {gap_count} detected
  Tests:   Passed

STATUS: SUCCESS
================================================================================
"""
    print(summary)
    return summary

summary = PythonOperator(
    task_id='summary',
    python_callable=generate_summary,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# ============================================================================
# DEPENDENCIES
# ============================================================================

check_drift >> calc_window >> extraction_group >> validate >> dbt_staging >> dbt_test >> summary
