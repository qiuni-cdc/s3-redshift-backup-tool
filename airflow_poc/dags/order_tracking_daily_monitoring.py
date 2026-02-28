"""
Order Tracking Daily Monitoring
=================================

Runs every day at 2am UTC (off-peak). Data quality checks only.
VACUUM is handled by a separate DAG: order_tracking_vacuum.

DATA QUALITY CHECKS (§12 of order_tracking_final_design.md)
   Four checks that catch extraction gaps and mart integrity issues.

   Test 0 — Extraction count comparison (MySQL vs Redshift raw)
     Counts rows in a closed 2-hour window in both MySQL source and Redshift raw.
     Any gap > 0 = rows dropped during extraction or Redshift COPY.
     Raises directly (does not write to exceptions — this is an infrastructure alert).

   Test 1 — UTI/UTS time alignment
     update_time in mart_uti should equal MAX(pathTime) in mart_uts.
     Mismatch = backdated update_time, missed extraction, or uti/uts sync drift.

   Test 2 — Orders missing from spath everywhere
     Order in mart_uti with no rows in mart_uts AND no rows in any hist_uts table.
     Genuine data integrity issue — spath events should always exist for any order.

   Test 3 — Reactivated orders missing from mart_ecs
     Order in mart_uti with no matching row in mart_ecs.
     Happens when an order reactivates after its ecs row was archived to hist_ecs.
     Fix: restore from hist_ecs manually (see order_tracking_final_design.md §5).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import os
import psycopg2

log = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

MART_SCHEMA = 'settlement_ods'

# Tables monitored
MART_UTI = f'{MART_SCHEMA}.mart_uni_tracking_info'
MART_ECS = f'{MART_SCHEMA}.mart_ecs_order_info'
MART_UTS = f'{MART_SCHEMA}.mart_uni_tracking_spath'
EXCEPTIONS = f'{MART_SCHEMA}.order_tracking_exceptions'

# DQ checks only inspect orders active within this lookback window.
# Stale orders can't develop *new* issues today, and pre-existing issues are
# already in the exceptions table from a prior run.
DQ_LOOKBACK_HOURS = 24
DQ_LOOKBACK_SECS  = DQ_LOOKBACK_HOURS * 3600

# Extraction count check (Test 0)
# MySQL source host — direct internal connection (no SSH tunnel; server-side only)
MYSQL_SOURCE_HOST = os.environ.get('MYSQL_SOURCE_HOST', 'us-west-2.ro.db.uniuni.com.internal')
MYSQL_SOURCE_PORT = int(os.environ.get('MYSQL_SOURCE_PORT', '3306'))

# Buffer matches calc_window (5 min); 2h window stays safely inside raw table's 24h retention
EXTRACTION_BUFFER_SECS = 5 * 60
EXTRACTION_WINDOW_SECS = 2 * 3600

# (mysql_table, mysql_ts_col, raw_table, raw_ts_col) — all timestamps are unix epoch integers
EXTRACTION_CHECK_TABLES = [
    ('ecs', 'kuaisong.ecs_order_info',     'add_time',    'settlement_public.ecs_order_info_raw',      'add_time'),
    ('uti', 'kuaisong.uni_tracking_info',  'update_time', 'settlement_public.uni_tracking_info_raw',   'update_time'),
    ('uts', 'kuaisong.uni_tracking_spath', 'pathTime',    'settlement_public.uni_tracking_spath_raw',  'pathTime'),
]

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 25),
    'email_on_failure': True,
    'email': ['jasleen.tung@uniuni.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'order_tracking_daily_monitoring',
    default_args=default_args,
    description='Daily DQ checks for order tracking mart',
    schedule_interval='0 3 * * *',  # 3am UTC daily, 1h after vacuum
    max_active_runs=1,
    catchup=False,
    tags=['order-tracking', 'monitoring', 'dq']
)

# ============================================================================
# HELPER: get Redshift connection (normal mode for DQ, autocommit for VACUUM)
# ============================================================================

def _get_hist_uts_tables(cur):
    """
    Return all hist_uni_tracking_spath_* tables that exist in Redshift.
    Queried dynamically so Test 2 stays correct after each 6-month period rotation
    without any code change.
    """
    cur.execute(
        "SELECT schemaname || '.' || tablename FROM pg_tables "
        "WHERE schemaname = %s AND tablename LIKE 'hist_uni_tracking_spath_%%' "
        "ORDER BY tablename",
        (MART_SCHEMA,),
    )
    return [row[0] for row in cur.fetchall()]


def get_conn(autocommit=False):
    host     = os.environ.get('REDSHIFT_HOST', 'redshift-dw.qa.uniuni.com')
    port     = int(os.environ.get('REDSHIFT_PORT', '5439'))
    user     = os.environ.get('REDSHIFT_QA_USER') or os.environ.get('REDSHIFT_USER') or os.environ.get('REDSHIFT_PRO_USER')
    password = os.environ.get('REDSHIFT_QA_PASSWORD') or os.environ.get('REDSHIFT_PASSWORD') or os.environ.get('REDSHIFT_PRO_PASSWORD')
    conn = psycopg2.connect(host=host, port=port, dbname='dw', user=user, password=password)
    conn.autocommit = autocommit
    return conn

def get_mysql_conn():
    """
    Direct MySQL connection using env vars. No SSH tunnel — runs server-side only
    where the internal host is reachable. Uses pymysql (ships with airflow-providers-mysql).
    """
    import pymysql
    return pymysql.connect(
        host=MYSQL_SOURCE_HOST,
        port=MYSQL_SOURCE_PORT,
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_US_PROD_RO_PASSWORD') or os.environ.get('DB_PASSWORD'),
        database='kuaisong',
        connect_timeout=30,
        cursorclass=pymysql.cursors.DictCursor,
    )

# ============================================================================
# TASK GROUP 1: DATA QUALITY CHECKS
# ============================================================================

def dq_extraction_count_check(**context):
    """
    Test 0 — Extraction count comparison (MySQL vs Redshift raw).

    Queries both MySQL source and Redshift *_raw tables for the same closed 2-hour
    window ending at now-5min (the same 5-min buffer used by calc_window).

    Any table where MySQL count > Redshift raw count signals rows were dropped during
    extraction or Redshift COPY. Raises directly — this is an infrastructure alert,
    not a data exception. Does NOT write to order_tracking_exceptions.

    Window: [now - 2h - 5min,  now - 5min)
    Alarm condition: mysql_count > raw_count  (gap > 0)

    Notes:
    - All three tables use unix epoch integers for their timestamp columns.
    - uni_tracking_info rows can appear more than once in raw (same order updated in
      multiple cycles); raw_count >= mysql_count is normal for uti — alarm only triggers
      if raw_count < mysql_count (missed extractions).
    - ecs and uts rows are immutable after creation so exact match is expected.
    """
    import time as _time

    to_unix   = int(_time.time()) - EXTRACTION_BUFFER_SECS
    from_unix = to_unix - EXTRACTION_WINDOW_SECS

    log.info(
        "Test 0: extraction count window [%d, %d)  UTC %s → %s",
        from_unix, to_unix,
        datetime.utcfromtimestamp(from_unix).strftime('%Y-%m-%d %H:%M:%S'),
        datetime.utcfromtimestamp(to_unix).strftime('%Y-%m-%d %H:%M:%S'),
    )

    gaps   = []
    mysql_conn = None
    rs_conn    = None

    try:
        mysql_conn = get_mysql_conn()
        rs_conn    = get_conn()
        mysql_cur  = mysql_conn.cursor()
        rs_cur     = rs_conn.cursor()

        for name, mysql_table, mysql_ts, raw_table, raw_ts in EXTRACTION_CHECK_TABLES:
            # MySQL — parameterised query (safe)
            mysql_cur.execute(
                f"SELECT COUNT(*) AS cnt FROM {mysql_table} "
                f"WHERE {mysql_ts} >= %s AND {mysql_ts} < %s",
                (from_unix, to_unix),
            )
            row = mysql_cur.fetchone()
            mysql_cnt = row['cnt'] if isinstance(row, dict) else row[0]

            # Redshift raw — integer literals are safe (no user input)
            rs_cur.execute(
                f"SELECT COUNT(*) FROM {raw_table} "
                f"WHERE {raw_ts} >= {from_unix} AND {raw_ts} < {to_unix}"
            )
            rs_cnt = rs_cur.fetchone()[0]

            gap    = mysql_cnt - rs_cnt
            status = "OK" if gap <= 0 else f"GAP={gap:,}"
            log.info("  [%s] MySQL=%s  Redshift_raw=%s  %s", name, f"{mysql_cnt:,}", f"{rs_cnt:,}", status)

            if gap > 0:
                gaps.append(f"{name}: MySQL={mysql_cnt:,}, Redshift_raw={rs_cnt:,}, missing={gap:,}")

    finally:
        for c in [mysql_conn, rs_conn]:
            try:
                if c:
                    c.close()
            except Exception:
                pass

    if gaps:
        raise ValueError(
            f"Test 0 FAILED — extraction gaps in window [{from_unix}, {to_unix}): "
            + "; ".join(gaps)
        )

    log.info("Test 0 PASSED — all tables match (MySQL source vs Redshift raw)")


def dq_uti_uts_alignment(**context):
    """
    Test 1 — update_time vs MAX(pathTime) alignment.

    Every order in mart_uti must have at least one spath event in mart_uts.
    update_time should equal MAX(pathTime) for that order.
    Mismatch indicates: backdated update_time, missed extraction, or uti/uts drift.

    Only inspects orders active within the last DQ_LOOKBACK_HOURS (48h).
    Older orders can't have new mismatches; pre-existing ones are already in exceptions.

    Writes unresolved mismatches to order_tracking_exceptions as 'UTI_UTS_TIME_MISMATCH'.
    Expected result: 0 rows inserted.
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = f"""
        INSERT INTO {EXCEPTIONS} (order_id, exception_type, detected_at, notes)
        SELECT
            uti.order_id,
            'UTI_UTS_TIME_MISMATCH',
            CURRENT_TIMESTAMP,
            'update_time=' || uti.update_time::varchar
                || ' max_pathTime=' || MAX(uts.pathTime)::varchar
                || ' diff=' || (uti.update_time - MAX(uts.pathTime))::varchar || 's'
        FROM {MART_UTI} uti
        JOIN {MART_UTS} uts ON uti.order_id = uts.order_id
        WHERE uti.update_time > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint - {DQ_LOOKBACK_SECS}
        GROUP BY uti.order_id, uti.update_time
        HAVING uti.update_time <> MAX(uts.pathTime)
          AND NOT EXISTS (
              SELECT 1 FROM {EXCEPTIONS} ex
              WHERE ex.order_id = uti.order_id
                AND ex.exception_type = 'UTI_UTS_TIME_MISMATCH'
                AND ex.resolved_at IS NULL
          )
    """
    cur.execute(sql)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    log.info(f"Test 1 (UTI/UTS alignment): {inserted} new exception(s) logged")
    context['task_instance'].xcom_push(key='dq_test1_count', value=inserted)
    return inserted


def dq_missing_spath(**context):
    """
    Test 2 — Orders in mart_uti with no spath events anywhere.

    Checks mart_uts first. If not there, checks each hist_uts table.
    Missing from all = genuine data integrity issue.

    Only inspects orders active within the last DQ_LOOKBACK_HOURS (48h).
    Newly appeared orders that have no spath events are the real concern here.

    Writes to order_tracking_exceptions as 'ORDER_MISSING_SPATH'.
    Expected result: 0 rows inserted.

    NOTE: This check covers mart_uts + all configured hist_uts tables.
    Add new hist tables to HIST_UTS_TABLES in this file when created (1 Jan / 1 Jul).
    """
    conn = get_conn()
    cur = conn.cursor()

    hist_uts_tables = _get_hist_uts_tables(cur)
    log.info("Test 2: checking %d hist_uts table(s): %s", len(hist_uts_tables), hist_uts_tables)

    hist_not_exists = '\n'.join([
        f"AND NOT EXISTS (SELECT 1 FROM {tbl} h WHERE h.order_id = uti.order_id)"
        for tbl in hist_uts_tables
    ])

    sql = f"""
        INSERT INTO {EXCEPTIONS} (order_id, exception_type, detected_at, notes)
        SELECT
            uti.order_id,
            'ORDER_MISSING_SPATH',
            CURRENT_TIMESTAMP,
            'No spath events in mart_uts or any hist_uts table'
        FROM {MART_UTI} uti
        WHERE uti.update_time > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint - {DQ_LOOKBACK_SECS}
          AND NOT EXISTS (
            SELECT 1 FROM {MART_UTS} uts WHERE uts.order_id = uti.order_id
          )
        {hist_not_exists}
          AND NOT EXISTS (
              SELECT 1 FROM {EXCEPTIONS} ex
              WHERE ex.order_id = uti.order_id
                AND ex.exception_type = 'ORDER_MISSING_SPATH'
                AND ex.resolved_at IS NULL
          )
    """
    cur.execute(sql)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    log.info(f"Test 2 (missing spath): {inserted} new exception(s) logged")
    context['task_instance'].xcom_push(key='dq_test2_count', value=inserted)
    return inserted


def dq_reactivated_missing_ecs(**context):
    """
    Test 3 — Orders in mart_uti with no matching row in mart_ecs.

    Happens when an order reactivates after being dormant >6 months.
    mart_ecs trimmed the ecs row when the order went dormant. The ecs extraction
    watermark (add_time-based) won't re-extract an old creation row.

    Only inspects orders active within the last DQ_LOOKBACK_HOURS (48h).
    A reactivated order must have a recent update_time to appear here.

    Manual fix required: restore ecs row from hist_ecs, delete stale hist entry.
    See: order_tracking_final_design.md §5 (mart_ecs reactivation).

    Writes to order_tracking_exceptions as 'REACTIVATED_ORDER_MISSING_ECS'.
    Expected result: 0 rows inserted (rare edge case).
    """
    conn = get_conn()
    cur = conn.cursor()

    sql = f"""
        INSERT INTO {EXCEPTIONS} (order_id, exception_type, detected_at, notes)
        SELECT
            uti.order_id,
            'REACTIVATED_ORDER_MISSING_ECS',
            CURRENT_TIMESTAMP,
            'Order in mart_uti but not in mart_ecs — restore from hist_ecs manually'
        FROM {MART_UTI} uti
        LEFT JOIN {MART_ECS} ecs ON uti.order_id = ecs.order_id
        WHERE uti.update_time > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint - {DQ_LOOKBACK_SECS}
          AND ecs.order_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM {EXCEPTIONS} ex
              WHERE ex.order_id = uti.order_id
                AND ex.exception_type = 'REACTIVATED_ORDER_MISSING_ECS'
                AND ex.resolved_at IS NULL
          )
    """
    cur.execute(sql)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    log.info(f"Test 3 (reactivated missing ecs): {inserted} new exception(s) logged")
    context['task_instance'].xcom_push(key='dq_test3_count', value=inserted)
    return inserted


def check_unresolved_exceptions(**context):
    """
    Alert check — fail this task if any unresolved exceptions exist.

    Pulls counts from all 3 DQ tests (XCom) and queries the exceptions table
    for the total unresolved count. Logs a summary. Raises if count > 0.

    Downstream alerting: task failure triggers email (email_on_failure=True).
    For Slack/PagerDuty, add an on_failure_callback here.
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT exception_type, COUNT(*) AS cnt
        FROM {EXCEPTIONS}
        WHERE resolved_at IS NULL
        GROUP BY exception_type
        ORDER BY cnt DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Pull per-test new counts from XCom
    ti = context['task_instance']
    new_test1 = ti.xcom_pull(task_ids='dq_checks.dq_uti_uts_alignment',    key='dq_test1_count') or 0
    new_test2 = ti.xcom_pull(task_ids='dq_checks.dq_missing_spath',         key='dq_test2_count') or 0
    new_test3 = ti.xcom_pull(task_ids='dq_checks.dq_reactivated_missing_ecs', key='dq_test3_count') or 0

    total_unresolved = sum(cnt for _, cnt in rows)

    log.info("=" * 60)
    log.info("EXCEPTIONS TABLE SUMMARY (unresolved)")
    log.info("=" * 60)
    if rows:
        for exc_type, cnt in rows:
            log.info(f"  {exc_type}: {cnt}")
    else:
        log.info("  (none)")
    log.info("-" * 60)
    log.info(f"  New today — Test 1: {new_test1}, Test 2: {new_test2}, Test 3: {new_test3}")
    log.info(f"  Total unresolved: {total_unresolved}")
    log.info("=" * 60)

    if total_unresolved > 0:
        raise ValueError(
            f"{total_unresolved} unresolved exception(s) in order_tracking_exceptions. "
            f"Details: {dict(rows)}. "
            f"See order_tracking_final_design.md §10 for resolution steps."
        )


# ============================================================================
# DEFINE TASKS
# ============================================================================

with TaskGroup("dq_checks", dag=dag) as dq_group:

    task_dq_test0 = PythonOperator(
        task_id='dq_extraction_count_check',
        python_callable=dq_extraction_count_check,
        dag=dag
    )

    task_dq_test1 = PythonOperator(
        task_id='dq_uti_uts_alignment',
        python_callable=dq_uti_uts_alignment,
        dag=dag
    )

    task_dq_test2 = PythonOperator(
        task_id='dq_missing_spath',
        python_callable=dq_missing_spath,
        dag=dag
    )

    task_dq_test3 = PythonOperator(
        task_id='dq_reactivated_missing_ecs',
        python_callable=dq_reactivated_missing_ecs,
        dag=dag
    )

task_check_exceptions = PythonOperator(
    task_id='check_unresolved_exceptions',
    python_callable=check_unresolved_exceptions,
    trigger_rule=TriggerRule.ALL_DONE,  # run even if a DQ task fails
    dag=dag
)

# ============================================================================
# DEPENDENCIES
# ============================================================================

# DQ checks run in parallel, then exceptions check consolidates results
dq_group >> task_check_exceptions
