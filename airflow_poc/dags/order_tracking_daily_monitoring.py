"""
Order Tracking Daily Monitoring
=================================

Runs every day at 2am UTC (off-peak). Covers two concerns:

1. DATA QUALITY CHECKS (§12 of order_tracking_final_design.md)
   Three SQL checks that write unresolved issues to order_tracking_exceptions.
   The exceptions table is the single place to monitor — any unresolved row = alert.

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

2. VACUUM (§16 of order_tracking_final_design.md)
   Reclaims ghost rows from high-frequency DELETEs in the 15-min cycle.
   Without VACUUM, ghost rows bloat table size and degrade zone map effectiveness.

   mart_uti:  VACUUM DELETE ONLY — daily (96 DELETE cycles/day)
   mart_uts:  VACUUM DELETE ONLY — daily (96 DELETE cycles/day)
   mart_ecs:  VACUUM DELETE ONLY — weekly (Sundays only; fewer deletes than uti/uts)
   mart_ecs:  VACUUM SORT ONLY   — conditional (only if svv_table_info.unsorted > 15%)

NOTE: VACUUM statements require autocommit mode (cannot run inside a transaction).
      PostgresHook.get_conn() is used with conn.autocommit = True for all VACUUM tasks.

Redshift connection: uses Airflow connection 'redshift_default'.
Configure in Airflow UI → Admin → Connections.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

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

# hist_uts tables — extend this list when new 6-month splits are created (1 Jan / 1 Jul)
HIST_UTS_TABLES = [
    f'{MART_SCHEMA}.hist_uni_tracking_spath_2025_h2',
    f'{MART_SCHEMA}.hist_uni_tracking_spath_2026_h1',
    # f'{MART_SCHEMA}.hist_uni_tracking_spath_2026_h2',  # add on 1 Jul 2026
]

REDSHIFT_CONN_ID = 'redshift_default'
VACUUM_SORT_THRESHOLD_PCT = 15  # trigger VACUUM SORT ONLY when unsorted % exceeds this

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
    description='Daily DQ checks (exceptions table) + VACUUM for order tracking mart',
    schedule_interval='0 2 * * *',  # 2am UTC daily, off-peak
    max_active_runs=1,
    catchup=False,
    tags=['order-tracking', 'monitoring', 'dq', 'vacuum']
)

# ============================================================================
# HELPER: get Redshift connection (normal mode for DQ, autocommit for VACUUM)
# ============================================================================

def get_conn(autocommit=False):
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn

# ============================================================================
# TASK GROUP 1: DATA QUALITY CHECKS
# ============================================================================

def dq_uti_uts_alignment(**context):
    """
    Test 1 — update_time vs MAX(pathTime) alignment.

    Every order in mart_uti must have at least one spath event in mart_uts.
    update_time should equal MAX(pathTime) for that order.
    Mismatch indicates: backdated update_time, missed extraction, or uti/uts drift.

    Writes unresolved mismatches to order_tracking_exceptions as 'UTI_UTS_TIME_MISMATCH'.
    Expected result: 0 rows inserted.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Find mismatches and insert new unresolved ones into exceptions
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

    Writes to order_tracking_exceptions as 'ORDER_MISSING_SPATH'.
    Expected result: 0 rows inserted.

    NOTE: This check covers mart_uts + all configured hist_uts tables.
    Add new hist tables to HIST_UTS_TABLES in this file when created (1 Jan / 1 Jul).
    """
    conn = get_conn()
    cur = conn.cursor()

    # Build the hist_uts NOT EXISTS clauses dynamically
    hist_not_exists = '\n'.join([
        f"AND NOT EXISTS (SELECT 1 FROM {tbl} h WHERE h.order_id = uti.order_id)"
        for tbl in HIST_UTS_TABLES
    ])

    sql = f"""
        INSERT INTO {EXCEPTIONS} (order_id, exception_type, detected_at, notes)
        SELECT
            uti.order_id,
            'ORDER_MISSING_SPATH',
            CURRENT_TIMESTAMP,
            'No spath events in mart_uts or any hist_uts table'
        FROM {MART_UTI} uti
        WHERE NOT EXISTS (
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
        WHERE ecs.order_id IS NULL
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
# TASK GROUP 2: VACUUM
# ============================================================================

def vacuum_mart_uti(**context):
    """
    VACUUM DELETE ONLY mart_uni_tracking_info — runs every day.

    mart_uti receives 96 DELETE cycles/day (one per 15-min extraction).
    Each cycle deletes and re-inserts the current batch, creating ghost rows.
    Without daily VACUUM, ghost rows bloat the table and degrade zone map pruning.
    """
    conn = get_conn(autocommit=True)  # VACUUM requires autocommit
    cur = conn.cursor()
    log.info(f"Starting VACUUM DELETE ONLY {MART_UTI}")
    cur.execute(f"VACUUM DELETE ONLY {MART_UTI}")
    log.info(f"VACUUM DELETE ONLY {MART_UTI} complete")
    cur.close()
    conn.close()


def vacuum_mart_uts(**context):
    """
    VACUUM DELETE ONLY mart_uni_tracking_spath — runs every day.

    mart_uts receives 96 time-based trim DELETEs/day (one per 15-min cycle).
    Daily VACUUM reclaims ghost rows and keeps zone map pruning effective.
    """
    conn = get_conn(autocommit=True)
    cur = conn.cursor()
    log.info(f"Starting VACUUM DELETE ONLY {MART_UTS}")
    cur.execute(f"VACUUM DELETE ONLY {MART_UTS}")
    log.info(f"VACUUM DELETE ONLY {MART_UTS} complete")
    cur.close()
    conn.close()


def vacuum_mart_ecs_delete(**context):
    """
    VACUUM DELETE ONLY mart_ecs_order_info — runs weekly (Sundays only).

    mart_ecs has fewer DELETE operations than mart_uti/uts (only when orders age out).
    Weekly VACUUM is sufficient. Skipped on non-Sunday runs to save cluster resources.
    """
    execution_date = context['execution_date']
    if execution_date.weekday() != 6:  # 6 = Sunday
        log.info(f"Skipping VACUUM mart_ecs — not Sunday (weekday={execution_date.weekday()})")
        return

    conn = get_conn(autocommit=True)
    cur = conn.cursor()
    log.info(f"Starting VACUUM DELETE ONLY {MART_ECS} (Sunday run)")
    cur.execute(f"VACUUM DELETE ONLY {MART_ECS}")
    log.info(f"VACUUM DELETE ONLY {MART_ECS} complete")
    cur.close()
    conn.close()


def vacuum_mart_ecs_sort(**context):
    """
    Conditional VACUUM SORT ONLY mart_ecs_order_info.

    mart_ecs has SORTKEY(partner_id, add_time, order_id). New orders arrive in
    add_time order but partner_id is random — unsorted region grows over time.
    Zone maps on partner_id (the primary filter) degrade as unsorted % rises.

    Only runs VACUUM SORT when svv_table_info.unsorted > VACUUM_SORT_THRESHOLD_PCT (15%).
    Expected cadence: ~every 65–130 days. Running weekly wastes cluster resources.
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT unsorted, tbl_rows, stats_off
        FROM svv_table_info
        WHERE "table" = 'mart_ecs_order_info'
          AND schema = '{MART_SCHEMA}'
    """)
    row = cur.fetchone()
    cur.close()

    if not row:
        log.warning("mart_ecs_order_info not found in svv_table_info — skipping VACUUM SORT check")
        conn.close()
        return

    unsorted_pct, tbl_rows, stats_off = row
    unsorted_pct = unsorted_pct or 0
    log.info(
        f"mart_ecs health — rows: {tbl_rows:,}, unsorted: {unsorted_pct:.1f}%, stats_off: {stats_off}"
    )

    if unsorted_pct <= VACUUM_SORT_THRESHOLD_PCT:
        log.info(
            f"Skipping VACUUM SORT — unsorted {unsorted_pct:.1f}% is below threshold "
            f"({VACUUM_SORT_THRESHOLD_PCT}%)"
        )
        conn.close()
        return

    log.info(
        f"Triggering VACUUM SORT ONLY {MART_ECS} "
        f"(unsorted {unsorted_pct:.1f}% > threshold {VACUUM_SORT_THRESHOLD_PCT}%)"
    )
    conn.close()

    # Re-open with autocommit for VACUUM
    conn = get_conn(autocommit=True)
    cur = conn.cursor()
    cur.execute(f"VACUUM SORT ONLY {MART_ECS}")
    log.info(f"VACUUM SORT ONLY {MART_ECS} complete")
    cur.close()
    conn.close()


# ============================================================================
# DEFINE TASKS
# ============================================================================

with TaskGroup("dq_checks", dag=dag) as dq_group:

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

with TaskGroup("vacuum", dag=dag) as vacuum_group:

    task_vacuum_uti = PythonOperator(
        task_id='vacuum_mart_uti',
        python_callable=vacuum_mart_uti,
        dag=dag
    )

    task_vacuum_uts = PythonOperator(
        task_id='vacuum_mart_uts',
        python_callable=vacuum_mart_uts,
        dag=dag
    )

    task_vacuum_ecs_delete = PythonOperator(
        task_id='vacuum_mart_ecs_delete',
        python_callable=vacuum_mart_ecs_delete,
        dag=dag
    )

    task_vacuum_ecs_sort = PythonOperator(
        task_id='vacuum_mart_ecs_sort',
        python_callable=vacuum_mart_ecs_sort,
        dag=dag
    )

    # mart_ecs sort VACUUM runs after delete VACUUM (more efficient order)
    task_vacuum_ecs_delete >> task_vacuum_ecs_sort

# ============================================================================
# DEPENDENCIES
# ============================================================================

# DQ checks run in parallel, then exceptions check consolidates results
dq_group >> task_check_exceptions

# VACUUM runs in parallel with DQ checks — both start at DAG trigger, independent
# (vacuum_uti and vacuum_uts are fully independent; vacuum_ecs has internal ordering)
