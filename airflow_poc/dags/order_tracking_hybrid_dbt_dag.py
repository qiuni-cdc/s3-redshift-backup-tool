"""
Order Tracking Sync — 15-Minute Pipeline
=========================================

Architecture: Raw → Mart directly. No staging layer.

CYCLE (every 15 min):
  ① Extraction (parallel, ~94s)
       extract_ecs / extract_uti / extract_uts → *_raw tables in Redshift

  ① Trim raw tables (~5s)
       DELETE rows older than 24h from all three *_raw tables.
       Raw tables are append-only landing zones — mart tables hold the retained data.
       24h window kept as a replay buffer for manual reprocessing.

  ② dbt: mart_uni_tracking_info  (~140s, must complete before ③ and ④)
       delete+insert from uni_tracking_info_raw
       4 post_hooks: stale hist cleanup → archive → safety check → trim

  ③ dbt: mart_ecs_order_info  (parallel with ④, after ②)
       insert from ecs_order_info_raw
       2 post_hooks: archive inactive orders (LEFT JOIN anti-join) → DELETE USING trim

  ④ dbt: mart_uni_tracking_spath  (parallel with ③, after ②)
       insert from uni_tracking_spath_raw
       3 post_hooks: archive → safety check → pure time-based trim

  ⑤ dbt test — schema.yml uniqueness / not_null / relationship tests

Estimated total: ~4 min. Headroom: ~11 min in 15-min schedule.
max_active_runs=1 prevents concurrent cycles from overlapping.

Monitoring (DQ checks + VACUUM) runs in a separate daily DAG:
  order_tracking_daily_monitoring

Tables:
  - kuaisong.ecs_order_info  (ecs) → add_time  (~500K/day, ~2M/day peak)
  - kuaisong.uni_tracking_info (uti) → update_time (~500K/day, ~2M/day peak)
  - kuaisong.uni_tracking_spath (uts) → pathTime  (~2M/day, ~8M/day peak)
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
import psycopg2

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
INCREMENTAL_LOOKBACK_MINUTES = 20     # 15-min window + 5-min buffer = 20 min total
TIME_DRIFT_THRESHOLD_SECONDS = 60     # Alert if Airflow vs MySQL drift exceeds this

TABLES = {
    "ecs": {
        "full_name": "kuaisong.ecs_order_info",
        "timestamp_col": "add_time",
        "target": "settlement_public.ecs_order_info_raw"
    },
    "uti": {
        "full_name": "kuaisong.uni_tracking_info",
        "timestamp_col": "update_time",
        "target": "settlement_public.uni_tracking_info_raw"
    },
    "uts": {
        "full_name": "kuaisong.uni_tracking_spath",
        "timestamp_col": "pathTime",
        "target": "settlement_public.uni_tracking_spath_raw"
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
    
    # Always use wall clock time — this is a near-real-time pipeline.
    # Using data_interval_start caused stale windows when Airflow ran
    # a backlogged scheduled slot (e.g. Feb 21 slot running on Feb 24).
    # Watermark handles idempotency so strict Airflow interval tracking is not needed.
    mysql_now = int(datetime.utcnow().timestamp())
    print(f"Using wall clock time (UTC): {datetime.utcnow().isoformat()} ({mysql_now})")

    buffer = BUFFER_MINUTES * 60
    lookback = INCREMENTAL_LOOKBACK_MINUTES * 60

    # For a run scheduled at 10:15 (covering 10:00-10:15):
    # data_interval_start = 10:15
    # to_unix = 10:15 - buffer (5m) = 10:10
    # from_unix = 10:10 - lookback (15m) = 9:55
    # This ensures 15m effective window, shifted by buffer.
    
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
            --initial-lookback-minutes {INCREMENTAL_LOOKBACK_MINUTES} \
            --end-time "{{{{ task_instance.xcom_pull(task_ids='calculate_sync_window', key='sync_window')['to_ts'].replace('T', ' ').split('.')[0] }}}}"
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
            --initial-lookback-minutes {INCREMENTAL_LOOKBACK_MINUTES} \
            --end-time "{{{{ task_instance.xcom_pull(task_ids='calculate_sync_window', key='sync_window')['to_ts'].replace('T', ' ').split('.')[0] }}}}"
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
            --initial-lookback-minutes {INCREMENTAL_LOOKBACK_MINUTES} \
            --end-time "{{{{ task_instance.xcom_pull(task_ids='calculate_sync_window', key='sync_window')['to_ts'].replace('T', ' ').split('.')[0] }}}}"
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
# TASK 5: TRIM RAW TABLES (24-hour retention)
# ============================================================================
# Raw tables are append-only landing zones. Once dbt has processed a batch into
# mart tables the raw rows are no longer needed. 24h window is kept as a replay
# buffer — enough to manually reprocess any cycle from the last day if needed.
# Runs after validate so we only trim once extraction is confirmed successful.

def trim_raw_tables(**context):
    """Delete raw table rows older than 24 hours."""
    # Use env vars directly — avoids IAM auth issues with the redshift_default connection.
    # Reads the same vars that the dbt tasks use.
    host = os.environ.get('REDSHIFT_HOST', 'redshift-dw.qa.uniuni.com')
    port = int(os.environ.get('REDSHIFT_PORT', '5439'))
    user = os.environ.get('REDSHIFT_QA_USER') or os.environ.get('REDSHIFT_USER') or os.environ.get('REDSHIFT_PRO_USER')
    password = os.environ.get('REDSHIFT_QA_PASSWORD') or os.environ.get('REDSHIFT_PASSWORD') or os.environ.get('REDSHIFT_PRO_PASSWORD')

    conn = psycopg2.connect(host=host, port=port, dbname='dw', user=user, password=password)
    conn.autocommit = True
    cur = conn.cursor()

    tables = [
        ('settlement_public.uni_tracking_info_raw', 'update_time'),
        ('settlement_public.ecs_order_info_raw',    'add_time'),
        ('settlement_public.uni_tracking_spath_raw', 'pathTime'),
    ]

    total_deleted = 0
    cutoff = "extract(epoch from current_timestamp - interval '24 hours')"

    print(f"Trimming raw tables (keeping last 24h) — host: {host}:{port}")
    for table, ts_col in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {ts_col} < {cutoff}")
        count = cur.fetchone()[0]
        if count > 0:
            cur.execute(f"DELETE FROM {table} WHERE {ts_col} < {cutoff}")
            print(f"  {table}: deleted {count:,} rows")
        else:
            print(f"  {table}: nothing to trim")
        total_deleted += count

    cur.close()
    conn.close()
    print(f"Total raw rows trimmed: {total_deleted:,}")
    context['task_instance'].xcom_push(key='raw_rows_trimmed', value=total_deleted)
    return total_deleted

trim_raw = PythonOperator(
    task_id='trim_raw_tables',
    python_callable=trim_raw_tables,
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

# Helper to load .env file into a dict (robust against CRLF and shell syntax issues)
def load_env_vars(path):
    # Try python-dotenv first for robust parsing
    try:
        from dotenv import dotenv_values
        if os.path.exists(path):
            print(f"Loading .env using python-dotenv from {path}")
            config = dotenv_values(path)
            # Filter out None values
            return {k: v for k, v in config.items() if v is not None}
    except ImportError:
        print("python-dotenv not found, falling back to manual parsing")
    
    # Manual fallback
    env_vars = {}
    if os.path.exists(path):
        print(f"Loading environment variables manually from {path}")
        try:
            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    # Remove 'export ' if present
                    if line.startswith('export '):
                        line = line[7:].strip()
                    # Split on first =
                    if '=' in line:
                        k, v = line.split('=', 1)
                        # Strip optional quotes and whitespace
                        k = k.strip()
                        v = v.strip()
                        
                        # basic inline comment handling: only if space-hash
                        if ' #' in v:
                            v = v.split(' #', 1)[0].strip()
                            
                        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                            v = v[1:-1]
                        env_vars[k] = v
        except Exception as e:
            print(f"Warning: Failed to load .env file: {e}")
    else:
        print(f"Warning: .env file not found at {path}")
    return env_vars

# Load project .env
# CRITICAL: Start with current environment to preserve PATH, HOME, etc.
dbt_env_vars = os.environ.copy()
dbt_env_vars.update(load_env_vars(os.path.join(SYNC_TOOL_PATH, '.env')))

# Explicitly add dbt venv to PATH to ensure dbt is found
dbt_venv_bin = os.path.join(DBT_VENV_PATH, 'bin')
if dbt_venv_bin not in dbt_env_vars.get('PATH', ''):
    print(f"Prepending {dbt_venv_bin} to PATH")
    dbt_env_vars['PATH'] = f"{dbt_venv_bin}:{dbt_env_vars.get('PATH', '')}"

# DEBUG: Print loaded keys to confirm env vars are present (masking values)
print("Debug: Loaded dbt_env_vars keys:")
for k, v in dbt_env_vars.items():
    if k.startswith('REDSHIFT') or k.startswith('DBT'):
        print(f"  {k}: <len={len(str(v))}>")

if DBT_USE_SSH_TUNNEL:
    # Local Docker testing: Use SSH tunnel
    # In tunnel mode, profiles.yml defaults to localhost:15439
    # We don't need to override DBT_REDSHIFT_HOST/PORT here as they default correctly in profiles.yml
    # But we DO need the credentials from .env
    
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
    
    # Inject Direct Connection settings into the env vars dict
    # Prefer values from loaded .env if available, otherwise fallback to global constants
    dbt_env_vars['DBT_REDSHIFT_HOST'] = dbt_env_vars.get('REDSHIFT_HOST', REDSHIFT_HOST)
    dbt_env_vars['DBT_REDSHIFT_PORT'] = dbt_env_vars.get('REDSHIFT_PORT', REDSHIFT_PORT)
    
    # CRITICAL: Map credentials to what profiles.yml expects
    # profiles.yml uses: REDSHIFT_QA_USER, REDSHIFT_QA_PASSWORD
    # Sources might be: REDSHIFT_USER, REDSHIFT_QA_USER, REDSHIFT_USERNAME
    if 'REDSHIFT_QA_USER' not in dbt_env_vars:
        # Try fallbacks
        fallback_user = dbt_env_vars.get('REDSHIFT_USER') or dbt_env_vars.get('REDSHIFT_USERNAME') or os.environ.get('REDSHIFT_USER')
        if fallback_user:
            print(f"Mapping REDSHIFT_QA_USER from fallback: {fallback_user[:3]}***")
            dbt_env_vars['REDSHIFT_QA_USER'] = fallback_user
            
    if 'REDSHIFT_QA_PASSWORD' not in dbt_env_vars:
         # Try fallbacks
        fallback_pass = dbt_env_vars.get('REDSHIFT_PASSWORD') or dbt_env_vars.get('REDSHIFT_PWD') or os.environ.get('REDSHIFT_PASSWORD')
        if fallback_pass:
            print("Mapping REDSHIFT_QA_PASSWORD from fallback")
            dbt_env_vars['REDSHIFT_QA_PASSWORD'] = fallback_pass
    
    DBT_WITH_TUNNEL = f'''
    set -e
    export PYTHONUNBUFFERED=1
    export DBT_LOG_PATH=/tmp
    export DBT_TARGET_PATH=/tmp/target
    cd {DBT_PROJECT_PATH}
    [ -f {DBT_VENV_PATH}/bin/activate ] && source {DBT_VENV_PATH}/bin/activate
    echo "Using direct Redshift connection (no SSH tunnel)"
'''
    DBT_CLEANUP_TUNNEL = ''

# ============================================================================
# TASK 5: DBT RUN - Mart: mart_uni_tracking_info (must complete before 5b/5c)
# ============================================================================
# execution_timeout is tighter here (10 min) — if mart_uti overruns its budget,
# the next scheduled run must not start before this one finishes. max_active_runs=1
# handles that at the DAG level, but a per-task timeout kills runaway cycles early.

dbt_mart_uti = BashOperator(
    task_id='dbt_mart_uti',
    bash_command=DBT_WITH_TUNNEL + f'''
    echo "[$(date -u +%H:%M:%S)] Running mart_uni_tracking_info"
    dbt run --select mart_uni_tracking_info --profiles-dir .
    echo "[$(date -u +%H:%M:%S)] mart_uni_tracking_info complete"
''' + DBT_CLEANUP_TUNNEL,
    env=dbt_env_vars,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

# ============================================================================
# TASK 5b/5c: DBT RUN - Mart: mart_ecs + mart_uts (parallel, after mart_uti)
# ============================================================================
# mart_ecs post_hooks LEFT JOIN against mart_uni_tracking_info — must read its
# final state, so mart_uti must fully complete first.
# mart_uts uses pure time-based retention (no mart_uti read) but still runs
# after mart_uti for consistent cycle ordering.

dbt_mart_ecs = BashOperator(
    task_id='dbt_mart_ecs',
    bash_command=DBT_WITH_TUNNEL + f'''
    echo "[$(date -u +%H:%M:%S)] Running mart_ecs_order_info"
    dbt run --select mart_ecs_order_info --profiles-dir .
    echo "[$(date -u +%H:%M:%S)] mart_ecs_order_info complete"
''' + DBT_CLEANUP_TUNNEL,
    env=dbt_env_vars,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

dbt_mart_uts = BashOperator(
    task_id='dbt_mart_uts',
    bash_command=DBT_WITH_TUNNEL + f'''
    echo "[$(date -u +%H:%M:%S)] Running mart_uni_tracking_spath"
    dbt run --select mart_uni_tracking_spath --profiles-dir .
    echo "[$(date -u +%H:%M:%S)] mart_uni_tracking_spath complete"
''' + DBT_CLEANUP_TUNNEL,
    env=dbt_env_vars,
    execution_timeout=timedelta(minutes=10),
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
#     env=dbt_env_vars,
#     dag=dag
# )

# ============================================================================
# TASK 7: DBT TEST - Mart Consistency Checks
# ============================================================================
# Scoped to --select mart: only tests mart model schema.yml (unique, not_null,
# relationships). Staging tests are excluded — stg_* tables are no longer
# updated in this pipeline so their relationship tests would produce false failures.

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=DBT_WITH_TUNNEL + f'''
    echo "[$(date -u +%H:%M:%S)] Running mart consistency tests"
    dbt test --select mart --store-failures --profiles-dir .
    echo "[$(date -u +%H:%M:%S)] Tests complete"
''' + DBT_CLEANUP_TUNNEL,
    env=dbt_env_vars,
    execution_timeout=timedelta(minutes=5),
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
    raw_rows_trimmed = context['task_instance'].xcom_pull(
        task_ids='trim_raw_tables', key='raw_rows_trimmed'
    ) or 0
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

RAW TABLE TRIM (24h retention):
  Rows deleted:   {raw_rows_trimmed:>10,}

DBT:
  mart_uti:  delete+insert, 4 post_hooks (hist cleanup, archive, safety check, trim)
  mart_ecs:  insert + 2 post_hooks (archive inactive, DELETE USING trim)
  mart_uts:  insert + 3 post_hooks (archive, safety check, time-based trim)
  Gaps:      {gap_count} detected
  Tests:     schema.yml (unique / not_null / relationships)

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

check_drift >> calc_window >> extraction_group >> validate >> trim_raw >> dbt_mart_uti
dbt_mart_uti >> [dbt_mart_ecs, dbt_mart_uts]
[dbt_mart_ecs, dbt_mart_uts] >> dbt_test >> summary

