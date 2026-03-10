"""
PROD → DW MySQL Hourly Full Sync (Phase 1: Small/Medium Tables)
================================================================
Replicates 7 reference/operational tables from US PROD MySQL to
US DW MySQL (uniods schema). Full sync (TRUNCATE + INSERT) every hour.

Phase 2 (large tables — order_details, uni_prealert_order,
uni_tracking_addon_spath) will use incremental CDC and is not
included here.

Tables synced:
  1. kuaisong.uni_pattern_config       (~57 rows)
  2. kuaisong.uni_warehouses           (~76 rows)
  3. kuaisong.uni_customer             (~3K rows)
  4. kuaisong.uni_zipcodes             (~10K rows)
  5. kuaisong.uni_mawb_box             (~31K rows)
  6. kuaisong.ecs_staff                (~158K rows)
  7. kuaisong.uni_prealert_info        (~284K rows)

Connection: mysql-connector-python with env vars from .env
  Source (PROD): DB_PROD_HOST / DB_PROD_PORT / DB_US_PROD_RO_PASSWORD
  Target (DW):   DB_DW_WRITE_HOST / DB_DW_WRITE_PORT / DB_DW_WRITE_PASSWORD

SSH Tunnels (QA only):
  Set USE_SSH_TUNNEL=true in .env to enable.
  Tunnels via SSH_BASTION_HOST using SSH_BASTION_USER + SSH_BASTION_KEY_PATH.
  On PROD server, set USE_SSH_TUNNEL=false (direct access).
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
import time
import socket
import smtplib
from email.mime.text import MIMEText
import mysql.connector

# ============================================================================
# CONFIGURATION
# ============================================================================

TABLES = [
    {"source_schema": "kuaisong",        "source_table": "uni_pattern_config",   "target_table": "uni_pattern_config",   "batch_size": 5000},
    {"source_schema": "kuaisong",        "source_table": "uni_warehouses",       "target_table": "uni_warehouses",       "batch_size": 5000},
    {"source_schema": "kuaisong",        "source_table": "uni_customer",         "target_table": "uni_customer",         "batch_size": 5000},
    {"source_schema": "kuaisong",        "source_table": "uni_zipcodes",         "target_table": "uni_zipcodes",         "batch_size": 5000},
    {"source_schema": "kuaisong",        "source_table": "uni_mawb_box",         "target_table": "uni_mawb_box",         "batch_size": 5000},
    {"source_schema": "kuaisong",        "source_table": "ecs_staff",            "target_table": "ecs_staff",            "batch_size": 10000},
    {"source_schema": "kuaisong",        "source_table": "uni_prealert_info",    "target_table": "uni_prealert_info",    "batch_size": 10000},
]

TARGET_SCHEMA = "uniods"

SYNC_TOOL_PATH = "/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"

# Email alert config — set these in .env
ALERT_RECIPIENTS = os.environ.get("SYNC_ALERT_EMAILS", "").split(",")
ALERT_SMTP_HOST = os.environ.get("ALERT_SMTP_HOST", "smtp.office365.com")
ALERT_SMTP_PORT = int(os.environ.get("ALERT_SMTP_PORT", "587"))
ALERT_SMTP_USER = os.environ.get("ALERT_SMTP_USER", "")
ALERT_SMTP_PASSWORD = os.environ.get("ALERT_SMTP_PASSWORD", "")

# SSH tunnel subprocess handles (module-level so they persist across tasks)
_ssh_tunnels = {}


def _load_env():
    """Load environment variables from .env file."""
    env_vars = {}
    env_file = os.path.join(SYNC_TOOL_PATH, ".env")
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key] = value.strip('"').strip("'")
    # os.environ takes precedence (docker-compose env vars)
    for key in ["DB_USER", "DB_US_PROD_RO_PASSWORD",
                "DB_PROD_HOST", "DB_PROD_PORT",
                "DB_DW_WRITE_HOST", "DB_DW_WRITE_PORT", "DB_DW_WRITE_PASSWORD",
                "USE_SSH_TUNNEL", "SSH_BASTION_HOST", "SSH_BASTION_USER",
                "SSH_BASTION_KEY_PATH"]:
        if key in os.environ:
            env_vars[key] = os.environ[key]
    return env_vars


def _find_free_port():
    """Find a free local port for SSH tunnel."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_ssh_tunnel(env, name, remote_host, remote_port):
    """Start an SSH tunnel via bastion and return the local port.
    Uses native subprocess ssh (matches existing connection_registry pattern).
    """
    if name in _ssh_tunnels and _ssh_tunnels[name].poll() is None:
        # Tunnel already running
        return _ssh_tunnels[name]._local_port

    local_port = _find_free_port()
    bastion_host = env.get("SSH_BASTION_HOST", "35.83.114.196")
    bastion_user = env.get("SSH_BASTION_USER", "jasleentung")
    key_path = env.get("SSH_BASTION_KEY_PATH", "")

    cmd = [
        "ssh", "-N", "-L",
        f"{local_port}:{remote_host}:{remote_port}",
        f"{bastion_user}@{bastion_host}",
        "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "ConnectTimeout=15",
    ]

    print(f"  Starting SSH tunnel [{name}]: localhost:{local_port} -> {remote_host}:{remote_port} via {bastion_host}")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    proc._local_port = local_port

    # Wait for tunnel to be ready
    for attempt in range(15):
        time.sleep(1)
        if proc.poll() is not None:
            stderr = proc.stderr.read().decode()
            raise RuntimeError(f"SSH tunnel [{name}] failed to start: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=2):
                print(f"  SSH tunnel [{name}] ready on port {local_port}")
                _ssh_tunnels[name] = proc
                return local_port
        except (ConnectionRefusedError, OSError):
            continue

    proc.kill()
    raise RuntimeError(f"SSH tunnel [{name}] timed out after 15s")


def _stop_all_tunnels():
    """Stop all SSH tunnel subprocesses."""
    for name, proc in _ssh_tunnels.items():
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
            print(f"  SSH tunnel [{name}] stopped")
    _ssh_tunnels.clear()


def _get_connections(env):
    """Return (source_conn, target_conn) with optional SSH tunnels.
    On QA: tunnels through bastion to reach PROD and DW.
    On PROD server: direct connections (no tunnels).
    """
    use_tunnel = env.get("USE_SSH_TUNNEL", "false").lower() == "true"

    prod_host = env.get("DB_PROD_HOST", "us-west-2.ro.db.uniuni.com.internal")
    prod_port = int(env.get("DB_PROD_PORT", "3306"))
    dw_host = env.get("DB_DW_WRITE_HOST", "")
    dw_port = int(env.get("DB_DW_WRITE_PORT", "3306"))

    if use_tunnel:
        print("  SSH tunnel mode enabled")
        prod_local_port = _start_ssh_tunnel(env, "prod", prod_host, prod_port)
        dw_local_port = _start_ssh_tunnel(env, "dw", dw_host, dw_port)
        src_host, src_port = "127.0.0.1", prod_local_port
        tgt_host, tgt_port = "127.0.0.1", dw_local_port
    else:
        src_host, src_port = prod_host, prod_port
        tgt_host, tgt_port = dw_host, dw_port

    # MTU/SSL fix: compress + ssl_disabled + use_pure prevents
    # "Lost connection" errors through SSH tunnels
    conn_base = dict(
        compress=True,
        ssl_disabled=True,
        use_pure=True,
        charset="utf8mb4",
        connection_timeout=30,
    )

    src_conn = mysql.connector.connect(
        host=src_host, port=src_port,
        user=env.get("DB_USER", "jasleentung"),
        password=env.get("DB_US_PROD_RO_PASSWORD", ""),
        **conn_base,
    )

    tgt_conn = mysql.connector.connect(
        host=tgt_host, port=tgt_port,
        user=env.get("DB_USER", "jasleentung"),
        password=env.get("DB_DW_WRITE_PASSWORD", ""),
        autocommit=False,
        **conn_base,
    )

    return src_conn, tgt_conn


# ============================================================================
# EMAIL ALERT
# ============================================================================

def _send_alert(subject, body):
    """Send email alert. Fails silently if SMTP not configured."""
    recipients = [r.strip() for r in ALERT_RECIPIENTS if r.strip()]
    if not recipients or not ALERT_SMTP_USER:
        print(f"  [Alert not sent — SMTP not configured] {subject}")
        return

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = ALERT_SMTP_USER
    msg["To"] = ", ".join(recipients)

    try:
        with smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=15) as server:
            server.starttls()
            server.login(ALERT_SMTP_USER, ALERT_SMTP_PASSWORD)
            server.sendmail(ALERT_SMTP_USER, recipients, msg.as_string())
        print(f"  Alert email sent: {subject}")
    except Exception as e:
        print(f"  WARNING: Failed to send alert email: {e}")


# ============================================================================
# SCHEMA VALIDATION
# ============================================================================

def _get_column_defs(conn, schema, table):
    """Get ordered list of (col_name, col_type) from INFORMATION_SCHEMA."""
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    cols = [(row[0], row[1]) for row in cur.fetchall()]
    cur.close()
    return cols


def _validate_schema(src_conn, tgt_conn, table_config):
    """Compare source vs target schema.
    If mismatch: send alert email and continue with existing target schema.
    If target missing: skip (table must be created manually).
    Returns: 'match', 'mismatch', or 'missing'.
    """
    src_schema = table_config["source_schema"]
    src_table = table_config["source_table"]
    tgt_table = table_config["target_table"]
    full_source = f"{src_schema}.{src_table}"
    full_target = f"{TARGET_SCHEMA}.{tgt_table}"

    print(f"  Schema validation: {full_source} vs {full_target}")

    src_cols = _get_column_defs(src_conn, src_schema, src_table)
    tgt_cols = _get_column_defs(tgt_conn, TARGET_SCHEMA, tgt_table)

    if not src_cols:
        raise RuntimeError(f"Source table {full_source} not found or has no columns")

    # Target table doesn't exist
    if not tgt_cols:
        print(f"  WARNING: Target table {full_target} does not exist — skipping sync")
        _send_alert(
            f"[PROD->DW Sync] Target table missing: {full_target}",
            f"The target table {full_target} does not exist in DW.\n"
            f"Source table {full_source} has {len(src_cols)} columns.\n\n"
            f"Action required: Create the table manually in {TARGET_SCHEMA} schema."
        )
        return "missing"

    # Compare column names and types
    mismatches = []
    src_names = [c[0] for c in src_cols]
    tgt_names = [c[0] for c in tgt_cols]

    src_set = set(src_names)
    tgt_set = set(tgt_names)
    missing_in_target = src_set - tgt_set
    extra_in_target = tgt_set - src_set

    if missing_in_target:
        mismatches.append(f"Columns in source but not target: {missing_in_target}")
    if extra_in_target:
        mismatches.append(f"Columns in target but not source: {extra_in_target}")

    # Check column types for shared columns
    src_dict = dict(src_cols)
    tgt_dict = dict(tgt_cols)
    for col_name in src_set & tgt_set:
        if src_dict[col_name] != tgt_dict[col_name]:
            mismatches.append(
                f"Column `{col_name}`: source={src_dict[col_name]}, target={tgt_dict[col_name]}"
            )

    # Check column order
    if not mismatches and src_names != tgt_names:
        mismatches.append("Column order differs")

    if mismatches:
        detail = "\n".join(f"  - {m}" for m in mismatches)
        print(f"  Schema MISMATCH detected — alerting and continuing with existing target:")
        for m in mismatches:
            print(f"    {m}")

        _send_alert(
            f"[PROD->DW Sync] Schema mismatch: {full_source} vs {full_target}",
            f"Schema mismatch detected during hourly sync.\n\n"
            f"Source: {full_source} ({len(src_cols)} columns)\n"
            f"Target: {full_target} ({len(tgt_cols)} columns)\n\n"
            f"Differences:\n{detail}\n\n"
            f"The sync will continue using the existing target schema.\n"
            f"Only columns present in BOTH source and target will be synced.\n\n"
            f"Action required: Review and update the target table DDL if needed."
        )
        return "mismatch"

    print(f"  Schema OK ({len(src_cols)} columns match)")
    return "match"


# ============================================================================
# SYNC FUNCTION
# ============================================================================

def sync_table(table_config, **context):
    """Validate schema + full sync: TRUNCATE target + batch INSERT from source."""
    src_schema = table_config["source_schema"]
    src_table = table_config["source_table"]
    tgt_table = table_config["target_table"]
    batch_size = table_config["batch_size"]

    full_source = f"{src_schema}.{src_table}"
    full_target = f"{TARGET_SCHEMA}.{tgt_table}"

    print(f"\n{'='*70}")
    print(f"SYNC: {full_source} -> {full_target}")
    print(f"{'='*70}")

    env = _load_env()
    src_conn = None
    tgt_conn = None

    try:
        # --- Connect (with SSH tunnels if USE_SSH_TUNNEL=true) ---
        print("Connecting to source (PROD) and target (DW) ...")
        src_conn, tgt_conn = _get_connections(env)

        # --- Schema validation (alert on mismatch, continue with existing) ---
        schema_result = _validate_schema(src_conn, tgt_conn, table_config)

        # Skip sync if target table doesn't exist
        if schema_result == "missing":
            context["task_instance"].xcom_push(
                key=f"sync_{src_table}",
                value={
                    "source": full_source,
                    "target": full_target,
                    "schema": schema_result,
                    "status": "skipped",
                    "error": "Target table does not exist",
                },
            )
            return

        # Determine which columns to sync
        if schema_result == "mismatch":
            # Only sync columns that exist in BOTH source and target
            src_cols = _get_column_defs(src_conn, src_schema, src_table)
            tgt_cols = _get_column_defs(tgt_conn, TARGET_SCHEMA, tgt_table)
            src_names = set(c[0] for c in src_cols)
            tgt_names = set(c[0] for c in tgt_cols)
            shared_cols = [c[0] for c in src_cols if c[0] in tgt_names]
            print(f"  Syncing {len(shared_cols)} shared columns (of {len(src_cols)} source, {len(tgt_cols)} target)")
        else:
            shared_cols = None  # Use all columns (SELECT *)

        # --- Read source row count ---
        src_cur = src_conn.cursor()
        src_cur.execute(f"SELECT COUNT(*) FROM {full_source}")
        source_count = src_cur.fetchone()[0]
        print(f"Source rows: {source_count:,}")
        src_cur.close()

        # --- Truncate target ---
        print(f"Truncating {full_target} ...")
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute(f"TRUNCATE TABLE {full_target}")
        tgt_conn.commit()
        tgt_cur.close()

        # --- Read source data with server-side cursor ---
        if shared_cols:
            select_cols = ", ".join(f"`{c}`" for c in shared_cols)
            select_sql = f"SELECT {select_cols} FROM {full_source}"
        else:
            select_sql = f"SELECT * FROM {full_source}"

        print(f"Reading from {full_source} ...")
        src_cur = src_conn.cursor(dictionary=True)
        src_cur.execute(select_sql)

        # Build INSERT statement from first row
        columns = None
        total_inserted = 0
        batch = []

        for row in src_cur:
            if columns is None:
                columns = list(row.keys())
                placeholders = ", ".join(["%s"] * len(columns))
                col_names = ", ".join(f"`{c}`" for c in columns)
                insert_sql = f"INSERT INTO {full_target} ({col_names}) VALUES ({placeholders})"

            batch.append(tuple(row.values()))

            if len(batch) >= batch_size:
                tgt_cur = tgt_conn.cursor()
                tgt_cur.executemany(insert_sql, batch)
                tgt_conn.commit()
                total_inserted += len(batch)
                tgt_cur.close()
                batch = []

                if total_inserted % 100000 == 0:
                    print(f"  ... {total_inserted:,} rows inserted")

        # Insert remaining rows
        if batch:
            tgt_cur = tgt_conn.cursor()
            tgt_cur.executemany(insert_sql, batch)
            tgt_conn.commit()
            total_inserted += len(batch)
            tgt_cur.close()

        src_cur.close()

        # --- Verify target count ---
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute(f"SELECT COUNT(*) FROM {full_target}")
        target_count = tgt_cur.fetchone()[0]
        tgt_cur.close()

        match = "MATCH" if source_count == target_count else "MISMATCH"
        print(f"Target rows: {target_count:,} ({match})")
        print(f"Sync complete: {total_inserted:,} rows inserted")

        # Push metrics to XCom
        context["task_instance"].xcom_push(
            key=f"sync_{src_table}",
            value={
                "source": full_source,
                "target": full_target,
                "source_count": source_count,
                "target_count": target_count,
                "inserted": total_inserted,
                "schema": schema_result,
                "status": "success",
            },
        )

        if source_count != target_count:
            print(f"WARNING: Row count mismatch! Source={source_count:,}, Target={target_count:,}")

    except Exception as e:
        print(f"ERROR syncing {full_source}: {e}")
        import traceback
        traceback.print_exc()

        context["task_instance"].xcom_push(
            key=f"sync_{src_table}",
            value={
                "source": full_source,
                "target": full_target,
                "status": "failed",
                "error": str(e),
            },
        )
        raise

    finally:
        if src_conn:
            src_conn.close()
        if tgt_conn:
            tgt_conn.close()

    print(f"{'='*70}\n")


# ============================================================================
# SUMMARY FUNCTION
# ============================================================================

def print_summary(**context):
    """Print summary of all table syncs."""
    print("\n" + "=" * 70)
    print("PROD -> DW MYSQL SYNC - HOURLY SUMMARY")
    print("=" * 70)
    print(f"Execution: {context['ds']} {context.get('ts', '')}")
    print("-" * 70)

    total_rows = 0
    success = 0
    failed = 0
    skipped = 0

    for table in TABLES:
        src_table = table["source_table"]
        result = context["task_instance"].xcom_pull(
            task_ids=f"sync_{src_table}",
            key=f"sync_{src_table}",
        )
        if result and result.get("status") == "success":
            src_cnt = result["source_count"]
            tgt_cnt = result["target_count"]
            match = "OK" if src_cnt == tgt_cnt else "MISMATCH"
            schema_tag = ""
            if result.get("schema") == "mismatch":
                schema_tag = " [schema mismatch — shared cols only]"
            print(f"  {result['source']:<45} -> {tgt_cnt:>10,} rows [{match}]{schema_tag}")
            total_rows += tgt_cnt
            success += 1
        elif result and result.get("status") == "skipped":
            print(f"  {result['source']:<45} -> SKIPPED: {result.get('error', 'target missing')}")
            skipped += 1
        elif result:
            print(f"  {result['source']:<45} -> FAILED: {result.get('error', 'unknown')}")
            failed += 1
        else:
            print(f"  {table['source_schema']}.{src_table:<40} -> NO RESULT")
            failed += 1

    print("-" * 70)
    print(f"Tables: {success} success, {failed} failed, {skipped} skipped")
    print(f"Total rows synced: {total_rows:,}")
    print("=" * 70 + "\n")

    context["task_instance"].xcom_push(
        key="summary",
        value={
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "total_rows": total_rows,
        },
    )

    # Clean up SSH tunnels if any
    _stop_all_tunnels()


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 10),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=50),
}

dag = DAG(
    "prod_to_dw_mysql_sync_hourly",
    default_args=default_args,
    description="Hourly full sync of reference tables from PROD MySQL to DW MySQL (uniods)",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["mysql-sync", "prod-to-dw", "full-sync", "hourly"],
)

# ============================================================================
# TASK GENERATION — sequential chain
# ============================================================================

sync_tasks = []

for table in TABLES:
    def _make_callable(tbl):
        def _run(**ctx):
            return sync_table(tbl, **ctx)
        return _run

    task = PythonOperator(
        task_id=f"sync_{table['source_table']}",
        python_callable=_make_callable(table),
        dag=dag,
    )
    sync_tasks.append(task)

# Chain tasks sequentially
for i in range(1, len(sync_tasks)):
    sync_tasks[i - 1] >> sync_tasks[i]

# Summary runs after all syncs
summary_task = PythonOperator(
    task_id="print_summary",
    python_callable=print_summary,
    trigger_rule="all_done",
    dag=dag,
)

sync_tasks[-1] >> summary_task
