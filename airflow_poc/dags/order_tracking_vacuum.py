"""
Order Tracking Vacuum
======================

Runs every day at 3am UTC (1 hour after DQ monitoring, off-peak).
Reclaims ghost rows from high-frequency DELETEs in the 15-min cycle.

VACUUM (§16 of order_tracking_final_design.md)
   Without VACUUM, ghost rows bloat table size and degrade zone map effectiveness.

   mart_uti:  VACUUM DELETE ONLY — daily (96 DELETE cycles/day)
   mart_uts:  VACUUM DELETE ONLY — daily (96 DELETE cycles/day)
   mart_ecs:  VACUUM DELETE ONLY — daily
   mart_ecs:  VACUUM SORT ONLY   — conditional (only if svv_table_info.unsorted > 15%)

NOTE: VACUUM statements require autocommit mode (cannot run inside a transaction).
      psycopg2 is used directly with conn.autocommit = True for all VACUUM tasks.
      PostgresHook is avoided — the redshift_default connection has IAM auth enabled
      which triggers a boto3 RDS token fetch and fails with NoRegionError.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
import os
import psycopg2

log = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

MART_SCHEMA = 'settlement_ods'

MART_UTI = f'{MART_SCHEMA}.mart_uni_tracking_info'
MART_ECS = f'{MART_SCHEMA}.mart_ecs_order_info'
MART_UTS = f'{MART_SCHEMA}.mart_uni_tracking_spath'

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
    'execution_timeout': timedelta(hours=2),  # VACUUM on large tables can take time
}

dag = DAG(
    'order_tracking_vacuum',
    default_args=default_args,
    description='Daily VACUUM for order tracking mart tables',
    schedule_interval='0 2 * * *',  # 2am UTC daily, runs before monitoring
    max_active_runs=1,
    catchup=False,
    tags=['order-tracking', 'vacuum']
)

# ============================================================================
# HELPER
# ============================================================================

def get_conn(autocommit=False):
    host     = os.environ.get('REDSHIFT_HOST', 'redshift-dw.qa.uniuni.com')
    port     = int(os.environ.get('REDSHIFT_PORT', '5439'))
    user     = os.environ.get('REDSHIFT_QA_USER') or os.environ.get('REDSHIFT_USER') or os.environ.get('REDSHIFT_PRO_USER')
    password = os.environ.get('REDSHIFT_QA_PASSWORD') or os.environ.get('REDSHIFT_PASSWORD') or os.environ.get('REDSHIFT_PRO_PASSWORD')
    conn = psycopg2.connect(host=host, port=port, dbname='dw', user=user, password=password)
    conn.autocommit = autocommit
    return conn

# ============================================================================
# VACUUM FUNCTIONS
# ============================================================================

def vacuum_mart_uti(**context):
    """
    VACUUM DELETE ONLY mart_uni_tracking_info — runs every day.

    mart_uti receives 96 DELETE cycles/day (one per 15-min extraction).
    Each cycle deletes and re-inserts the current batch, creating ghost rows.
    Without daily VACUUM, ghost rows bloat the table and degrade zone map pruning.
    """
    conn = get_conn(autocommit=True)
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
    VACUUM DELETE ONLY mart_ecs_order_info — runs every day.
    """
    conn = get_conn(autocommit=True)
    cur = conn.cursor()
    log.info(f"Starting VACUUM DELETE ONLY {MART_ECS}")
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

# vacuum_uti and vacuum_uts run in parallel; vacuum_ecs has its own internal ordering
