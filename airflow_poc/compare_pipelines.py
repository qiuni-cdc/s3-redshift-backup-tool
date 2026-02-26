#!/usr/bin/env python3
"""
Cross-pipeline data quality comparison: MySQL uniods (Kettle) vs Redshift mart.

Compares data for a given UTC day between the old pipeline (MySQL uniods) and
the new pipeline (Redshift mart layer), reporting missing rows, extra rows, and
column-level value mismatches per table.

Tables compared:
  ecs  │ kuaisong.ecs_order_info         ↔  settlement_ods.mart_ecs_order_info
  uti  │ kuaisong.uni_tracking_info      ↔  settlement_ods.mart_uni_tracking_info
  uts  │ kuaisong.uni_tracking_spath     ↔  settlement_ods.mart_uni_tracking_spath

Usage:
    # Compare full mart date range (recommended — auto-detects window from each mart table)
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel --table ecs
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel --output mismatches.csv

    # Compare a single day (legacy / spot-check)
    python airflow_poc/compare_pipelines.py --date 2026-02-01 --env qa --no-rs-tunnel

Notes:
  - Requires SSH tunnel access to the MySQL bastion (35.83.114.196).
  - Run with --no-rs-tunnel when executing from inside the VPC (e.g. QA server).
  - Full-range mode processes data in --chunk-days chunks (default 30) to cap memory.
    uts at full 6-month range is ~1.17B rows total; chunking keeps each batch ~200M rows.
  - Value comparison uses string coercion. Numeric float/decimal columns may show
    false positives if precision differs between MySQL and Redshift (e.g. 1.0 vs 1).
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import mysql.connector
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Table definitions
# ─────────────────────────────────────────────────────────────────────────────

TABLE_CONFIG = {
    "ecs": {
        "mysql_table":    "ecs_order_info",
        "redshift_table": "mart_ecs_order_info",
        "pk":             ["order_id"],
        "date_col":       "add_time",         # unix timestamp
        "dw_exclude":     {"id"},             # auto-increment PK in PROD only
        "stg_exclude":    set(),
    },
    "uti": {
        "mysql_table":    "uni_tracking_info",
        "redshift_table": "mart_uni_tracking_info",
        "pk":             ["order_id"],
        "date_col":       "update_time",
        "dw_exclude":     {"id"},
        "stg_exclude":    set(),
    },
    "uts": {
        "mysql_table":    "uni_tracking_spath",
        "redshift_table": "mart_uni_tracking_spath",
        "pk":             ["order_id", "traceSeq", "pathTime"],
        "date_col":       "pathTime",
        "dw_exclude":     {"id"},
        "stg_exclude":    set(),
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Connection config  (mirrors config/connections.yml)
# ─────────────────────────────────────────────────────────────────────────────

MYSQL_CONFIG = {
    "host":        "us-west-2.ro.db.uniuni.com.internal",
    "port":        3306,
    "database":    "kuaisong",
    "ssh_bastion": "35.83.114.196",
}

# Evaluated at call time so that load_dotenv() has already run.
REDSHIFT_ENVS = {
    "qa": {
        "host":        "redshift-dw.qa.uniuni.com",
        "port":        5439,
        "database":    "dw",
        "user_env":    "REDSHIFT_QA_USER",
        "pass_env":    "REDSHIFT_QA_PASSWORD",
        "schema":      "settlement_ods",
        "ssh_bastion": "35.82.216.244",
    },
    "prod": {
        "host":        "redshift-dw.uniuni.com",
        "port":        5439,
        "database":    "dw",
        "user_env":    "REDSHIFT_PRO_USER",
        "pass_env":    "REDSHIFT_PRO_PASSWORD",
        "schema":      "settlement_ods",
        "ssh_bastion": "35.83.114.196",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Tunnel + connection helpers
# ─────────────────────────────────────────────────────────────────────────────

def open_ssh_tunnel(bastion_host: str, remote_host: str, remote_port: int) -> SSHTunnelForwarder:
    tunnel = SSHTunnelForwarder(
        bastion_host,
        ssh_username=os.getenv("SSH_BASTION_USER"),
        ssh_pkey=os.getenv("SSH_BASTION_KEY_PATH"),
        remote_bind_address=(remote_host, remote_port),
    )
    tunnel.start()
    print(f"  Tunnel open: localhost:{tunnel.local_bind_port} → {remote_host}:{remote_port}")
    return tunnel


def open_redshift_tunnel(env_cfg: dict) -> SSHTunnelForwarder:
    tunnel = SSHTunnelForwarder(
        env_cfg["ssh_bastion"],
        ssh_username=os.getenv("REDSHIFT_SSH_BASTION_USER"),
        ssh_pkey=os.getenv("REDSHIFT_SSH_BASTION_KEY_PATH"),
        remote_bind_address=(env_cfg["host"], env_cfg["port"]),
    )
    tunnel.start()
    print(f"  Tunnel open: localhost:{tunnel.local_bind_port} → {env_cfg['host']}:{env_cfg['port']}")
    return tunnel


def connect_mysql(tunnel: SSHTunnelForwarder):
    return mysql.connector.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=MYSQL_CONFIG["database"],
        connection_timeout=60,
    )


def connect_redshift(host: str, port: int, env_cfg: dict):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=env_cfg["database"],
        user=os.getenv(env_cfg["user_env"]),
        password=os.getenv(env_cfg["pass_env"]),
        connect_timeout=30,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Column detection
# ─────────────────────────────────────────────────────────────────────────────

def get_mysql_columns(conn, table: str) -> list:
    cur = conn.cursor()
    cur.execute(f"DESCRIBE `{table}`")
    cols = [row[0] for row in cur.fetchall()]
    cur.close()
    return cols


def get_redshift_columns(conn, schema: str, table: str) -> list:
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    )
    cols = [row[0] for row in cur.fetchall()]
    cur.close()
    return cols


def resolve_common_columns(mysql_cols: list, rs_cols: list,
                            dw_exclude: set, stg_exclude: set) -> list:
    """Columns present in both sides after applying per-side exclusions.
    Preserves MySQL ordering; matching is case-insensitive."""
    rs_available = {c.lower() for c in rs_cols} - {c.lower() for c in stg_exclude}
    return [c for c in mysql_cols if c not in dw_exclude and c.lower() in rs_available]


def get_mart_date_range(conn, schema: str, table: str, date_col: str) -> tuple:
    """Return (min_unix, max_unix) of date_col in the mart table."""
    dc = date_col.lower()
    cur = conn.cursor()
    cur.execute(f'SELECT MIN("{dc}"), MAX("{dc}") FROM {schema}."{table}"')
    row = cur.fetchone()
    cur.close()
    if not row or row[0] is None:
        raise ValueError(f"Mart table {schema}.{table} is empty — nothing to compare.")
    return int(row[0]), int(row[1])


# ─────────────────────────────────────────────────────────────────────────────
# Data fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_mysql(conn, table: str, date_col: str,
                from_unix: int, to_unix: int, cols: list) -> pd.DataFrame:
    col_list = ", ".join(f"`{c}`" for c in cols)
    query = (
        f"SELECT {col_list} FROM `{table}` "
        f"WHERE `{date_col}` >= {from_unix} AND `{date_col}` <= {to_unix}"
    )
    chunks = []
    cur = conn.cursor(dictionary=True)
    cur.execute(query)
    while True:
        rows = cur.fetchmany(100_000)
        if not rows:
            break
        chunks.append(pd.DataFrame(rows))
    cur.close()
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=cols)


def fetch_redshift(conn, schema: str, table: str, date_col: str,
                   from_unix: int, to_unix: int, cols: list) -> pd.DataFrame:
    # Redshift stores identifiers in lowercase
    col_list = ", ".join(f'"{c.lower()}"' for c in cols)
    dc = date_col.lower()
    query = (
        f'SELECT {col_list} FROM {schema}."{table}" '
        f'WHERE "{dc}" >= {from_unix} AND "{dc}" <= {to_unix}'
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=[c.lower() for c in cols])


# ─────────────────────────────────────────────────────────────────────────────
# Comparison
# ─────────────────────────────────────────────────────────────────────────────

def compare_dataframes(mysql_df: pd.DataFrame, rs_df: pd.DataFrame,
                       pk: list, cols: list) -> dict:
    pk_lower = [c.lower() for c in pk]

    mysql_df = mysql_df.copy()
    rs_df    = rs_df.copy()
    mysql_df.columns = [c.lower() for c in mysql_df.columns]
    rs_df.columns    = [c.lower() for c in rs_df.columns]

    # Key sets (tuples to support composite PKs)
    def key_set(df):
        return set(map(tuple, df[pk_lower].astype(str).values.tolist()))

    mysql_keys    = key_set(mysql_df)
    rs_keys       = key_set(rs_df)
    missing_in_rs = mysql_keys - rs_keys   # in MySQL, absent in Redshift
    extra_in_rs   = rs_keys - mysql_keys   # in Redshift, absent in MySQL

    # Column-level value comparison via inner merge (common keys only)
    compare_cols           = [c.lower() for c in cols if c.lower() not in pk_lower]
    total_value_mismatches = 0
    sample_mismatches      = []

    if compare_cols and not mysql_df.empty and not rs_df.empty:
        merged = mysql_df.merge(rs_df, on=pk_lower, suffixes=("_m", "_r"), how="inner")
        if not merged.empty:
            any_diff = pd.Series(False, index=merged.index)
            for c in compare_cols:
                cm, cr = f"{c}_m", f"{c}_r"
                if cm in merged.columns and cr in merged.columns:
                    any_diff |= merged[cm].astype(str) != merged[cr].astype(str)

            total_value_mismatches = int(any_diff.sum())

            for _, row in merged[any_diff].head(5).iterrows():
                diff_cols = [
                    c for c in compare_cols
                    if f"{c}_m" in merged.columns
                    and str(row[f"{c}_m"]) != str(row[f"{c}_r"])
                ]
                pk_val = tuple(str(row[k]) for k in pk_lower)
                sample_mismatches.append({"pk": pk_val, "diff_cols": diff_cols})

    return {
        "mysql_rows":        len(mysql_df),
        "rs_rows":           len(rs_df),
        "common_cols":       len(cols),
        "missing_in_rs":     len(missing_in_rs),
        "extra_in_rs":       len(extra_in_rs),
        "value_mismatches":  total_value_mismatches,
        "sample_missing":    list(missing_in_rs)[:5],
        "sample_extra":      list(extra_in_rs)[:5],
        "sample_mismatches": sample_mismatches,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def print_report(table_key: str, result: dict):
    passed = (
        result["missing_in_rs"] == 0
        and result["extra_in_rs"] == 0
        and result["value_mismatches"] == 0
    )
    status = "PASS" if passed else "FAIL"

    print(f"\n{'─'*50}")
    print(f"Table: {table_key}")
    print(f"{'─'*50}")
    print(f"  MySQL rows:           {result['mysql_rows']:>10,}")
    print(f"  Redshift rows:        {result['rs_rows']:>10,}")
    print(f"  Common columns:       {result['common_cols']:>10}")
    print(f"\n  Status: {status}")
    print(f"  Missing in Redshift:  {result['missing_in_rs']:>8,}")
    print(f"  Extra in Redshift:    {result['extra_in_rs']:>8,}")
    print(f"  Value mismatches:     {result['value_mismatches']:>8,}")

    if result["sample_missing"]:
        print(f"\n  Sample missing keys:  {result['sample_missing']}")
    if result["sample_extra"]:
        print(f"  Sample extra keys:    {result['sample_extra']}")
    if result["sample_mismatches"]:
        print("\n  Sample mismatches (first 5):")
        for mm in result["sample_mismatches"]:
            print(f"    PK={mm['pk']}  diff cols: {mm['diff_cols']}")


def export_csv(results: dict, path: str):
    rows = []
    for tbl, res in results.items():
        for mm in res["sample_mismatches"]:
            rows.append({
                "table":     tbl,
                "issue":     "value_mismatch",
                "pk":        mm["pk"],
                "diff_cols": ",".join(mm["diff_cols"]),
            })
        for k in res["sample_missing"]:
            rows.append({"table": tbl, "issue": "missing_in_redshift", "pk": k, "diff_cols": ""})
        for k in res["sample_extra"]:
            rows.append({"table": tbl, "issue": "extra_in_redshift", "pk": k, "diff_cols": ""})
    if rows:
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"\nExported to: {path}")
    else:
        print("\nNo mismatches — nothing to export.")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def compare_one_table(mysql_conn, rs_conn, rs_cfg: dict, tbl_key: str,
                      from_unix: int, to_unix: int, cols: list) -> dict:
    """Fetch and compare one [from_unix, to_unix] slice of a table."""
    cfg = TABLE_CONFIG[tbl_key]
    mysql_df = fetch_mysql(
        mysql_conn, cfg["mysql_table"], cfg["date_col"],
        from_unix, to_unix, cols,
    )
    rs_df = fetch_redshift(
        rs_conn, rs_cfg["schema"], cfg["redshift_table"], cfg["date_col"],
        from_unix, to_unix, cols,
    )
    print(f"    MySQL={len(mysql_df):,}  Redshift={len(rs_df):,}", end="  ")
    result = compare_dataframes(mysql_df, rs_df, cfg["pk"], cols)
    del mysql_df, rs_df
    return result


def merge_results(agg: dict, chunk: dict) -> dict:
    """Accumulate chunk result into aggregate."""
    agg["mysql_rows"]       += chunk["mysql_rows"]
    agg["rs_rows"]          += chunk["rs_rows"]
    agg["missing_in_rs"]    += chunk["missing_in_rs"]
    agg["extra_in_rs"]      += chunk["extra_in_rs"]
    agg["value_mismatches"] += chunk["value_mismatches"]
    # Keep first non-empty samples across chunks
    if not agg["sample_missing"] and chunk["sample_missing"]:
        agg["sample_missing"] = chunk["sample_missing"][:5]
    if not agg["sample_extra"] and chunk["sample_extra"]:
        agg["sample_extra"] = chunk["sample_extra"][:5]
    if not agg["sample_mismatches"] and chunk["sample_mismatches"]:
        agg["sample_mismatches"] = chunk["sample_mismatches"][:5]
    return agg


def main():
    parser = argparse.ArgumentParser(
        description="Cross-pipeline comparison: MySQL uniods vs Redshift mart"
    )
    parser.add_argument("--date",  default=None,
                        help="Single UTC date to compare (YYYY-MM-DD). "
                             "Omit to use the full mart date range.")
    parser.add_argument("--table", choices=["ecs", "uti", "uts"],
                        help="Compare one table only (default: all three)")
    parser.add_argument("--output", metavar="FILE",
                        help="Export mismatch sample to CSV")
    parser.add_argument("--env",   choices=["qa", "prod"], default="prod",
                        help="Redshift environment (default: prod)")
    parser.add_argument("--no-rs-tunnel", action="store_true",
                        help="Skip Redshift SSH tunnel (use when running inside VPC)")
    parser.add_argument("--chunk-days", type=int, default=30,
                        help="Chunk size in days for full-range mode (default: 30)")
    args = parser.parse_args()

    tables = [args.table] if args.table else ["ecs", "uti", "uts"]
    rs_cfg = REDSHIFT_ENVS[args.env]

    single_day = args.date is not None
    if single_day:
        day       = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        from_unix = int(day.timestamp())
        to_unix   = from_unix + 86399
        print(f"Mode:       single day {args.date}  ({from_unix} → {to_unix} UTC)")
    else:
        print(f"Mode:       full mart range  (chunk={args.chunk_days} days)")

    print(f"Tables:     {tables}")
    print(f"Redshift:   {args.env}  schema={rs_cfg['schema']}")

    # ── Open tunnels ──────────────────────────────────────────────────────────
    print("\nOpening SSH tunnel to MySQL (uniods)...")
    mysql_tunnel = open_ssh_tunnel(
        MYSQL_CONFIG["ssh_bastion"],
        MYSQL_CONFIG["host"],
        MYSQL_CONFIG["port"],
    )

    rs_tunnel = None
    if not args.no_rs_tunnel:
        print("Opening SSH tunnel to Redshift...")
        rs_tunnel = open_redshift_tunnel(rs_cfg)
        rs_host, rs_port = "127.0.0.1", rs_tunnel.local_bind_port
    else:
        rs_host, rs_port = rs_cfg["host"], rs_cfg["port"]

    mysql_conn = rs_conn = None
    all_results = {}

    try:
        print("Connecting to MySQL...")
        mysql_conn = connect_mysql(mysql_tunnel)

        print("Connecting to Redshift...")
        rs_conn = connect_redshift(rs_host, rs_port, rs_cfg)

        for tbl_key in tables:
            cfg = TABLE_CONFIG[tbl_key]
            print(f"\n{'='*50}")
            print(f"Table: {tbl_key}  ({cfg['mysql_table']} vs {cfg['redshift_table']})")

            mysql_cols = get_mysql_columns(mysql_conn, cfg["mysql_table"])
            rs_cols    = get_redshift_columns(rs_conn, rs_cfg["schema"], cfg["redshift_table"])
            cols       = resolve_common_columns(
                mysql_cols, rs_cols, cfg["dw_exclude"], cfg["stg_exclude"]
            )
            print(f"  Columns: mysql={len(mysql_cols)}  rs={len(rs_cols)}  common={len(cols)}")

            if single_day:
                # ── Single-day mode ──────────────────────────────────────────
                print(f"  Fetching [{cfg['date_col']} {from_unix}..{to_unix}]...")
                result = compare_one_table(
                    mysql_conn, rs_conn, rs_cfg, tbl_key,
                    from_unix, to_unix, cols,
                )
                print()
            else:
                # ── Full-range mode: auto-detect mart window, chunk ──────────
                min_t, max_t = get_mart_date_range(
                    rs_conn, rs_cfg["schema"], cfg["redshift_table"], cfg["date_col"]
                )
                min_dt = datetime.fromtimestamp(min_t, tz=timezone.utc).strftime("%Y-%m-%d")
                max_dt = datetime.fromtimestamp(max_t, tz=timezone.utc).strftime("%Y-%m-%d")
                print(f"  Mart range: {min_dt} → {max_dt}  ({min_t} → {max_t})")

                agg = {
                    "mysql_rows": 0, "rs_rows": 0, "common_cols": len(cols),
                    "missing_in_rs": 0, "extra_in_rs": 0, "value_mismatches": 0,
                    "sample_missing": [], "sample_extra": [], "sample_mismatches": [],
                }

                chunk_secs  = args.chunk_days * 86400
                chunk_start = min_t
                chunk_num   = 0
                while chunk_start <= max_t:
                    chunk_end = min(chunk_start + chunk_secs - 1, max_t)
                    chunk_num += 1
                    cs = datetime.fromtimestamp(chunk_start, tz=timezone.utc).strftime("%Y-%m-%d")
                    ce = datetime.fromtimestamp(chunk_end,   tz=timezone.utc).strftime("%Y-%m-%d")
                    print(f"  Chunk {chunk_num}: {cs} → {ce}", end="  ")
                    chunk_result = compare_one_table(
                        mysql_conn, rs_conn, rs_cfg, tbl_key,
                        chunk_start, chunk_end, cols,
                    )
                    miss = chunk_result["missing_in_rs"]
                    extra = chunk_result["extra_in_rs"]
                    vm   = chunk_result["value_mismatches"]
                    flag = " ✗" if (miss or extra or vm) else " ✓"
                    print(f"missing={miss:,}  extra={extra:,}  value_mm={vm:,}{flag}")
                    merge_results(agg, chunk_result)
                    chunk_start = chunk_end + 1

                result = agg

            all_results[tbl_key] = result
            print_report(tbl_key, result)

        if args.output:
            export_csv(all_results, args.output)

    finally:
        if mysql_conn:
            mysql_conn.close()
        if rs_conn:
            rs_conn.close()
        mysql_tunnel.stop()
        if rs_tunnel:
            rs_tunnel.stop()
        print("\nConnections closed.")


if __name__ == "__main__":
    main()
