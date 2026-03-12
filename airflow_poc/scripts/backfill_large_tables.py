"""
One-time backfill script for large PROD → DW tables.
=====================================================
Streams rows from PROD MySQL to DW MySQL in chunks, updating the
watermark (uniods.sync_watermarks) after each chunk. Resumable — if
interrupted, re-run the same command and it picks up from the last
watermark.

Usage:
  python3 backfill_large_tables.py <table_name>

Examples:
  python3 backfill_large_tables.py uni_tracking_addon_spath
  python3 backfill_large_tables.py uni_prealert_order
  python3 backfill_large_tables.py order_details

Environment:
  Reads .env from SYNC_TOOL_PATH (or env vars from docker-compose).
  Set USE_SSH_TUNNEL=true for QA (tunnels through bastion).
  Set USE_SSH_TUNNEL=false for PROD (direct connections).
"""
import sys
import os
import time
import subprocess
import socket
import mysql.connector

# ============================================================================
# CONFIGURATION
# ============================================================================

TABLES = {
    "uni_tracking_addon_spath": {"source_schema": "kuaisong", "source_table": "uni_tracking_addon_spath", "target_table": "uni_tracking_addon_spath"},
    "uni_prealert_order":      {"source_schema": "kuaisong", "source_table": "uni_prealert_order",      "target_table": "uni_prealert_order"},
    "order_details":           {"source_schema": "kuaisong", "source_table": "order_details",           "target_table": "order_details"},
}

CHUNK_SIZE = 50000  # rows per chunk
TARGET_SCHEMA = "uniods"
WATERMARK_TABLE = "uniods.sync_watermarks"
SYNC_TOOL_PATH = os.environ.get(
    "SYNC_TOOL_PATH", "/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
)

# SSH tunnel handles
_ssh_tunnels = {}


# ============================================================================
# HELPERS (same logic as DAG)
# ============================================================================

def _load_env():
    env_vars = {}
    env_file = os.path.join(SYNC_TOOL_PATH, ".env")
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key] = value.strip('"').strip("'")
    for key in ["DB_USER", "DB_US_PROD_RO_PASSWORD",
                "DB_PROD_HOST", "DB_PROD_PORT",
                "DB_DW_WRITE_HOST", "DB_DW_WRITE_PORT", "DB_DW_WRITE_PASSWORD",
                "USE_SSH_TUNNEL", "SSH_BASTION_HOST", "SSH_BASTION_USER",
                "SSH_BASTION_KEY_PATH"]:
        if key in os.environ:
            env_vars[key] = os.environ[key]
    return env_vars


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_ssh_tunnel(env, name, remote_host, remote_port):
    if name in _ssh_tunnels and _ssh_tunnels[name].poll() is None:
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

    print(f"  Starting SSH tunnel [{name}]: localhost:{local_port} -> {remote_host}:{remote_port}")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    proc._local_port = local_port

    for _ in range(15):
        time.sleep(1)
        if proc.poll() is not None:
            stderr = proc.stderr.read().decode()
            raise RuntimeError(f"SSH tunnel [{name}] failed: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=2):
                print(f"  SSH tunnel [{name}] ready on port {local_port}")
                _ssh_tunnels[name] = proc
                return local_port
        except (ConnectionRefusedError, OSError):
            continue

    proc.kill()
    raise RuntimeError(f"SSH tunnel [{name}] timed out")


def _get_connections(env):
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

    conn_base = dict(
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


def _get_column_defs(conn, schema, table):
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    cols = [(row[0], row[1], row[2] if len(row) > 2 else "") for row in cur.fetchall()]
    cur.close()
    return cols


# ============================================================================
# BACKFILL
# ============================================================================

def backfill(table_name):
    if table_name not in TABLES:
        print(f"ERROR: Unknown table '{table_name}'. Valid tables: {list(TABLES.keys())}")
        sys.exit(1)

    config = TABLES[table_name]
    src_schema = config["source_schema"]
    src_table = config["source_table"]
    tgt_table = config["target_table"]
    full_source = f"{src_schema}.{src_table}"
    full_target = f"{TARGET_SCHEMA}.{tgt_table}"

    print(f"\n{'='*70}")
    print(f"BACKFILL: {full_source} -> {full_target}")
    print(f"Chunk size: {CHUNK_SIZE:,}")
    print(f"{'='*70}")

    env = _load_env()
    src_conn, tgt_conn = _get_connections(env)

    try:
        # Set longer lock wait timeout and commit any implicit transactions
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute("SET innodb_lock_wait_timeout = 300")
        tgt_cur.close()
        tgt_conn.commit()

        # --- Determine columns (exclude generated) ---
        tgt_cols = _get_column_defs(tgt_conn, TARGET_SCHEMA, tgt_table)
        generated_cols = {c[0] for c in tgt_cols
                         if "GENERATED" in (c[2] or "").upper()
                         or "VIRTUAL" in (c[2] or "").upper()}
        if generated_cols:
            print(f"Excluding generated columns: {generated_cols}")

        # Get all source column names for SELECT *
        src_cur = src_conn.cursor(buffered=True)
        src_cur.execute(f"SELECT * FROM {full_source} LIMIT 0")
        all_col_names = [desc[0] for desc in src_cur.description]
        src_cur.close()

        # Determine insert columns
        tgt_col_names = {c[0] for c in tgt_cols} - generated_cols
        shared_cols = [c for c in all_col_names if c in tgt_col_names]
        if not shared_cols:
            print("ERROR: No shared columns between source and target")
            sys.exit(1)

        col_indices = [all_col_names.index(c) for c in shared_cols]
        col_names_str = ", ".join(f"`{c}`" for c in shared_cols)
        row_placeholder = "(" + ", ".join(["%s"] * len(shared_cols)) + ")"
        SUB_BATCH = 2000  # rows per multi-row INSERT statement
        print(f"Syncing {len(shared_cols)} columns (sub-batch {SUB_BATCH:,})")

        # --- Read current watermark ---
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute(
            f"SELECT last_id FROM {WATERMARK_TABLE} WHERE table_name = %s",
            (tgt_table,),
        )
        row = tgt_cur.fetchone()
        last_id = row[0] if row else 0
        tgt_cur.close()
        print(f"Starting watermark: {last_id:,}")

        # --- Get source max id ---
        src_cur = src_conn.cursor()
        src_cur.execute(f"SELECT MAX(id) FROM {full_source}")
        max_id = src_cur.fetchone()[0] or 0
        src_cur.close()
        print(f"Source MAX(id): {max_id:,}")

        if max_id <= last_id:
            print("Already caught up — nothing to backfill")
            return

        remaining = max_id - last_id
        print(f"Rows to backfill: ~{remaining:,} (id {last_id+1:,} to {max_id:,})")
        print("-" * 70)

        # Clear any pending read transactions before INSERT loop
        tgt_conn.commit()

        # --- Chunked backfill loop ---
        total_inserted = 0
        start_time = time.time()
        chunk_num = 0

        while last_id < max_id:
            chunk_num += 1
            chunk_start = time.time()

            # Fetch chunk
            src_cur = src_conn.cursor()
            src_cur.execute(
                f"SELECT * FROM {full_source} WHERE id > %s ORDER BY id LIMIT %s",
                (last_id, CHUNK_SIZE),
            )
            rows = src_cur.fetchall()
            src_cur.close()

            if not rows:
                print("No more rows — done")
                break

            # Build batch with column filtering
            batch = [tuple(row[i] for i in col_indices) for row in rows]
            chunk_last_id = rows[-1][all_col_names.index("id")]

            # Insert with retry for transient errors (lock timeout, deadlock)
            for attempt in range(5):
                try:
                    tgt_cur = tgt_conn.cursor()

                    # Multi-row INSERT IGNORE in sub-batches
                    for i in range(0, len(batch), SUB_BATCH):
                        sub = batch[i:i + SUB_BATCH]
                        values_str = ", ".join([row_placeholder] * len(sub))
                        stmt = f"INSERT IGNORE INTO {full_target} ({col_names_str}) VALUES {values_str}"
                        flat_params = [v for row in sub for v in row]
                        tgt_cur.execute(stmt, flat_params)

                    # Update watermark
                    tgt_cur.execute(
                        f"INSERT INTO {WATERMARK_TABLE} (table_name, last_id, last_batch_rows, last_sync_at) "
                        f"VALUES (%s, %s, %s, NOW()) "
                        f"ON DUPLICATE KEY UPDATE last_id=VALUES(last_id), last_batch_rows=VALUES(last_batch_rows), last_sync_at=NOW()",
                        (tgt_table, chunk_last_id, len(batch)),
                    )

                    tgt_conn.commit()
                    tgt_cur.close()
                    break
                except mysql.connector.errors.DatabaseError as e:
                    tgt_conn.rollback()
                    if attempt < 4 and ("1205" in str(e) or "1213" in str(e)):
                        wait = (attempt + 1) * 10
                        print(f"    Retry {attempt+1}/5 after lock error, waiting {wait}s...")
                        time.sleep(wait)
                    else:
                        raise

            last_id = chunk_last_id
            total_inserted += len(batch)
            elapsed = time.time() - start_time
            chunk_elapsed = time.time() - chunk_start
            pct = ((last_id - (max_id - remaining)) / remaining * 100) if remaining > 0 else 100
            rate = total_inserted / elapsed if elapsed > 0 else 0

            print(
                f"  Chunk {chunk_num}: +{len(batch):,} rows | "
                f"id={last_id:,} | "
                f"{pct:.1f}% | "
                f"{total_inserted:,} total | "
                f"{rate:,.0f} rows/s | "
                f"chunk {chunk_elapsed:.1f}s"
            )

        # --- Done ---
        elapsed = time.time() - start_time
        print("-" * 70)
        print(f"BACKFILL COMPLETE: {total_inserted:,} rows in {elapsed:.0f}s ({elapsed/60:.1f} min)")
        print(f"Final watermark: {last_id:,}")

    finally:
        src_conn.close()
        tgt_conn.close()
        for name, proc in _ssh_tunnels.items():
            if proc.poll() is None:
                proc.terminate()


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <table_name>")
        print(f"  Tables: {list(TABLES.keys())}")
        sys.exit(1)

    backfill(sys.argv[1])
