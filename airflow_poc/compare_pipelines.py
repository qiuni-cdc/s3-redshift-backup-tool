#!/usr/bin/env python3
"""
Cross-pipeline data quality comparison: MySQL source (kuaisong) vs Redshift mart.

Compares data for a given UTC day between the MySQL source and the new Redshift
mart layer, reporting missing rows, extra rows, and column-level value mismatches.
Also runs Redshift self-consistency checks (duplicate PKs, NULL order_ids, cross-mart
integrity, open exceptions) and produces an overall PASS/FAIL validation summary.

Tables compared:
  ecs  │ kuaisong.ecs_order_info         ↔  settlement_ods.mart_ecs_order_info
  uti  │ kuaisong.uni_tracking_info      ↔  settlement_ods.mart_uni_tracking_info
  uts  │ kuaisong.uni_tracking_spath     ↔  settlement_ods.mart_uni_tracking_spath

Usage:
    # Quick spot-check: last 30 minutes of data (fast, good for smoke-testing)
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel --last-minutes 30

    # Full validation: all 3 tables + self-consistency checks
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel --pipeline-start 2026-02-01

    # One table only
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel --table uti

    # Export mismatch sample to CSV
    python airflow_poc/compare_pipelines.py --env qa --no-rs-tunnel --output mismatches.csv

    # Single day spot-check (no self-consistency checks)
    python airflow_poc/compare_pipelines.py --date 2026-02-01 --env qa --no-rs-tunnel

Notes:
  - Requires SSH tunnel access to the MySQL bastion (35.83.114.196).
  - Run with --no-rs-tunnel when executing from inside the VPC (e.g. QA server).
  - Full-range mode processes data in --chunk-days chunks (default 30) to cap memory.
    uts at full 6-month range is ~1.17B rows total; chunking keeps each batch ~200M rows.
  - Value comparison uses string coercion. Numeric float/decimal columns may show
    false positives if precision differs between MySQL and Redshift (e.g. 1.0 vs 1).

Known gaps (annotated, not counted as bugs):
  - ECS cold-start gap: orders created before --pipeline-start are never extracted
    into mart_ecs (add_time < watermark). Counted separately as INFO, not a bug.
  - UTI extra_in_rs: orders whose update_time advanced in MySQL after our last
    extraction cycle appear as "extra in RS" within the comparison window. Timing
    artifact — resolves on next extraction. Not a bug.
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
    # extra_in_rs for uti is a timing artifact, not a real bug — exclude from PASS/FAIL
    extra_is_timing = table_key == "uti" and result["extra_in_rs"] > 0
    passed = (
        result["missing_in_rs"] == 0
        and result["value_mismatches"] == 0
        and (extra_is_timing or result["extra_in_rs"] == 0)
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
    print(f"  Extra in Redshift:    {result['extra_in_rs']:>8,}", end="")
    if extra_is_timing:
        print("  ← timing artifact (see note below)")
    else:
        print()
    print(f"  Value mismatches:     {result['value_mismatches']:>8,}")

    if result["sample_missing"]:
        print(f"\n  Sample missing keys:  {result['sample_missing']}")
    if result["sample_extra"]:
        print(f"  Sample extra keys:    {result['sample_extra']}")
    if result["sample_mismatches"]:
        print("\n  Sample mismatches (first 5):")
        for mm in result["sample_mismatches"]:
            print(f"    PK={mm['pk']}  diff cols: {mm['diff_cols']}")

    if extra_is_timing:
        print(
            f"\n  Note (extra_in_rs for uti): {result['extra_in_rs']:,} orders exist in our mart\n"
            f"  with update_time inside the comparison window, but MySQL has since moved\n"
            f"  them to a newer update_time (outside the window). This is a 15-min lag\n"
            f"  between extraction cycles — not a data loss or duplication bug. These\n"
            f"  rows will be refreshed on the next extraction cycle."
        )


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
# Self-consistency checks (Redshift-side)
# ─────────────────────────────────────────────────────────────────────────────

def run_self_checks(rs_conn, rs_cfg, pipeline_start_ts=None):
    """
    Run Redshift self-consistency checks.
    Returns a list of {check, result, detail} dicts.
    result is one of: PASS, FAIL, WARN, INFO.

    Checks:
      1. No duplicate PKs in each mart table
      2. No NULL order_id in each mart table
      3. ECS cold-start gap — orders in mart_uti with no mart_ecs row (INFO, expected)
      4. mart_uts orphans — distinct orders in mart_uts with no mart_uti row (WARN if > 0)
      5. Open exceptions by type
    """
    schema = rs_cfg["schema"]
    checks = []
    cur = rs_conn.cursor()

    # ── 1. Duplicate PKs ──────────────────────────────────────────────────────
    # Redshift stores all identifiers in lowercase — traceSeq → traceseq, pathTime → pathtime
    dup_checks = [
        ("mart_uni_tracking_info", "order_id"),
        ("mart_ecs_order_info",    "order_id"),
        ("mart_uni_tracking_spath", "order_id, traceseq, pathtime"),
    ]
    for tbl, pk_expr in dup_checks:
        cur.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_expr}
                FROM {schema}.{tbl}
                GROUP BY {pk_expr}
                HAVING COUNT(*) > 1
            ) d
        """)
        n = cur.fetchone()[0]
        checks.append({
            "check":  f"No duplicate PKs — {tbl}",
            "result": "PASS" if n == 0 else "FAIL",
            "detail": "OK" if n == 0 else f"{n:,} duplicate PK groups found — investigate immediately",
        })

    # ── 2. NULL order_id ─────────────────────────────────────────────────────
    for tbl in ("mart_uni_tracking_info", "mart_ecs_order_info", "mart_uni_tracking_spath"):
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{tbl} WHERE order_id IS NULL")
        n = cur.fetchone()[0]
        checks.append({
            "check":  f"No NULL order_id — {tbl}",
            "result": "PASS" if n == 0 else "FAIL",
            "detail": "OK" if n == 0 else f"{n:,} rows with NULL order_id",
        })

    # ── 3. ECS cold-start gap ─────────────────────────────────────────────────
    # Orders in mart_uti with no mart_ecs row.
    # Root cause: ECS CDC uses add_time (immutable). Orders created before the pipeline
    # watermark are never re-extracted. This is a known bootstrap gap, not a pipeline bug.
    # Fix: one-time historical backfill of ecs_order_info.
    cur.execute(f"""
        SELECT COUNT(*)
        FROM {schema}.mart_uni_tracking_info uti
        LEFT JOIN {schema}.mart_ecs_order_info ecs ON ecs.order_id = uti.order_id
        WHERE ecs.order_id IS NULL
    """)
    ecs_gap = cur.fetchone()[0]

    cur.execute(f"SELECT MIN(add_time), MAX(add_time) FROM {schema}.mart_ecs_order_info")
    ecs_range = cur.fetchone()
    ecs_min_dt = (
        datetime.fromtimestamp(ecs_range[0], tz=timezone.utc).strftime("%Y-%m-%d")
        if ecs_range and ecs_range[0] else "N/A"
    )

    pipeline_note = ""
    if pipeline_start_ts:
        ps_dt = datetime.fromtimestamp(pipeline_start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        pipeline_note = f" Pipeline started {ps_dt}; mart_ecs covers from {ecs_min_dt}."
    else:
        pipeline_note = f" mart_ecs covers add_time from {ecs_min_dt}."

    checks.append({
        "check":  "ECS cold-start gap (mart_uti orders missing from mart_ecs)",
        "result": "INFO",
        "detail": (
            f"{ecs_gap:,} active orders have no ECS row.{pipeline_note}"
            f" Expected for orders created before pipeline start. Fix: one-time ECS backfill."
        ),
    })

    # ── 4. mart_uts orphans ───────────────────────────────────────────────────
    # Distinct orders in mart_uts with no corresponding mart_uti row.
    # With DISTKEY(order_id) co-location this JOIN is local — no cross-node shuffle.
    # Should be zero: every spath event belongs to an order that is (or was) in mart_uti.
    cur.execute(f"""
        SELECT COUNT(DISTINCT uts.order_id)
        FROM {schema}.mart_uni_tracking_spath uts
        LEFT JOIN {schema}.mart_uni_tracking_info uti ON uti.order_id = uts.order_id
        WHERE uti.order_id IS NULL
    """)
    uts_orphans = cur.fetchone()[0]
    checks.append({
        "check":  "mart_uts orphan orders (spath events with no mart_uti row)",
        "result": "PASS" if uts_orphans == 0 else "WARN",
        "detail": (
            "OK" if uts_orphans == 0
            else f"{uts_orphans:,} distinct orders in mart_uts have no mart_uti row — "
                 f"possible extraction lag between uti and uts cycles"
        ),
    })

    # ── 5. Open exceptions by type ────────────────────────────────────────────
    cur.execute(f"""
        SELECT exception_type, COUNT(*) AS n
        FROM {schema}.order_tracking_exceptions
        WHERE resolved_at IS NULL
        GROUP BY exception_type
        ORDER BY n DESC
    """)
    exc_rows = cur.fetchall()
    if exc_rows:
        for exc_type, n in exc_rows:
            checks.append({
                "check":  f"Open exception — {exc_type}",
                "result": "WARN",
                "detail": f"{n:,} open (unresolved)",
            })
    else:
        checks.append({
            "check":  "Open exceptions",
            "result": "PASS",
            "detail": "None — all clear",
        })

    cur.close()
    return checks


def print_self_checks(checks):
    """Print self-consistency check results."""
    icon = {"PASS": "✓", "FAIL": "✗", "WARN": "⚠", "INFO": "i"}
    print(f"\n{'─'*50}")
    print("SELF-CONSISTENCY CHECKS")
    print(f"{'─'*50}")
    for c in checks:
        sym = icon.get(c["result"], "?")
        print(f"  [{c['result']:4s}] {sym}  {c['check']}")
        if c["detail"] != "OK":
            # Indent wrapped detail lines
            for line in c["detail"].split(". "):
                line = line.strip()
                if line:
                    print(f"          {line}.")


def print_validation_summary(all_results, self_checks):
    """
    Print overall PASS/FAIL validation summary.

    Separates:
      - Real bugs: missing rows, value mismatches, FAIL self-checks — need action
      - Timing artifacts: UTI extra_in_rs — expected 15-min lag, not a bug
      - Known gaps: INFO/WARN from self-checks (cold-start gap, open exceptions)
    """
    real_bugs  = []
    timing     = []
    known_gaps = []

    for tbl, res in all_results.items():
        if res["missing_in_rs"] > 0:
            real_bugs.append(
                f"  {tbl}: {res['missing_in_rs']:,} rows in MySQL missing from Redshift"
            )
        if res["value_mismatches"] > 0:
            real_bugs.append(
                f"  {tbl}: {res['value_mismatches']:,} column-level value mismatches"
            )
        if res["extra_in_rs"] > 0:
            if tbl == "uti":
                timing.append(
                    f"  uti: {res['extra_in_rs']:,} rows 'extra in RS' — orders whose "
                    f"update_time advanced in MySQL after last extraction (resolves next cycle)"
                )
            else:
                # For uts/ecs, extra_in_rs means we have rows MySQL doesn't — investigate
                real_bugs.append(
                    f"  {tbl}: {res['extra_in_rs']:,} rows in Redshift not found in MySQL "
                    f"(within comparison window) — investigate"
                )

    for c in self_checks:
        if c["result"] == "FAIL":
            real_bugs.append(f"  self-check FAIL: {c['check']} — {c['detail']}")
        elif c["result"] == "WARN":
            known_gaps.append(f"  {c['check']}: {c['detail']}")
        elif c["result"] == "INFO":
            known_gaps.append(f"  [INFO] {c['check']}: {c['detail']}")

    overall = "FAIL" if real_bugs else "PASS"

    print(f"\n{'═'*50}")
    print("VALIDATION SUMMARY")
    print(f"{'═'*50}")
    print(f"  Overall: {overall}")

    if real_bugs:
        print(f"\n  ACTION REQUIRED ({len(real_bugs)} issue(s)):")
        for b in real_bugs:
            print(b)
    else:
        print("\n  No real bugs detected.")

    if timing:
        print(f"\n  Timing artifacts (not bugs, {len(timing)}):")
        for t in timing:
            print(t)

    if known_gaps:
        print(f"\n  Known / expected gaps ({len(known_gaps)}):")
        for g in known_gaps:
            print(g)


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
    parser.add_argument("--last-minutes", type=int, default=None, metavar="N",
                        help="Compare only the last N minutes of data in each mart table "
                             "(uses mart MAX timestamp as anchor). Useful for quick spot-checks. "
                             "Mutually exclusive with --date.")
    parser.add_argument("--pipeline-start", metavar="YYYY-MM-DD", default=None,
                        help="Pipeline go-live date. Used to annotate the ECS cold-start "
                             "gap (orders created before this date are expected to be "
                             "absent from mart_ecs). Omit if unknown.")
    args = parser.parse_args()

    if args.date and args.last_minutes:
        print("Error: --date and --last-minutes are mutually exclusive.")
        sys.exit(1)

    pipeline_start_ts = None
    if args.pipeline_start:
        try:
            pipeline_start_ts = int(
                datetime.strptime(args.pipeline_start, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        except ValueError:
            print(f"Invalid --pipeline-start date: {args.pipeline_start!r}. Expected YYYY-MM-DD.")
            sys.exit(1)

    tables = [args.table] if args.table else ["ecs", "uti", "uts"]
    rs_cfg = REDSHIFT_ENVS[args.env]

    single_day   = args.date is not None
    last_minutes = args.last_minutes  # int or None
    if single_day:
        day       = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        from_unix = int(day.timestamp())
        to_unix   = from_unix + 86399
        print(f"Mode:       single day {args.date}  ({from_unix} → {to_unix} UTC)")
    elif last_minutes:
        print(f"Mode:       last {last_minutes} minutes  (per-table, anchored at mart MAX)")
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
            elif last_minutes:
                # ── Last-N-minutes mode: anchor at mart MAX, go back N mins ──
                _, max_t = get_mart_date_range(
                    rs_conn, rs_cfg["schema"], cfg["redshift_table"], cfg["date_col"]
                )
                lm_from = max_t - last_minutes * 60
                lm_to   = max_t
                lm_from_dt = datetime.fromtimestamp(lm_from, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                lm_to_dt   = datetime.fromtimestamp(lm_to,   tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                print(f"  Window: {lm_from_dt} → {lm_to_dt} UTC  ({lm_from} → {lm_to})")
                result = compare_one_table(
                    mysql_conn, rs_conn, rs_cfg, tbl_key,
                    lm_from, lm_to, cols,
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
                    miss  = chunk_result["missing_in_rs"]
                    extra = chunk_result["extra_in_rs"]
                    vm    = chunk_result["value_mismatches"]
                    # UTI extra_in_rs is a timing artifact — don't flag as ✗
                    real_issue = miss > 0 or vm > 0 or (extra > 0 and tbl_key != "uti")
                    flag = " ✗" if real_issue else " ✓"
                    extra_note = " (timing)" if extra > 0 and tbl_key == "uti" else ""
                    print(f"missing={miss:,}  extra={extra:,}{extra_note}  value_mm={vm:,}{flag}")
                    merge_results(agg, chunk_result)
                    chunk_start = chunk_end + 1

                result = agg

            all_results[tbl_key] = result
            print_report(tbl_key, result)

        if args.output:
            export_csv(all_results, args.output)

        # ── Self-consistency checks (skipped for single-day spot-checks only) ──
        self_checks = []
        if not single_day:
            print(f"\n{'='*50}")
            print("Running self-consistency checks on Redshift...")
            self_checks = run_self_checks(rs_conn, rs_cfg, pipeline_start_ts)
            print_self_checks(self_checks)

        # ── Validation summary ───────────────────────────────────────────────
        if all_results:
            print_validation_summary(all_results, self_checks)

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
