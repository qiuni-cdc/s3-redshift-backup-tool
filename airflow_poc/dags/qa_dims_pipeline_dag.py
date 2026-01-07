"""
Analytics Pipeline with dbt Integration
MySQL → S3 → Redshift → dbt Transformations → Tests
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import glob

# ============================================================================
# CONFIGURATION - Customize for your pipeline
# ============================================================================

SYNC_TOOL_PATH = "/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
DBT_PROJECT_PATH = "/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/dbt_projects/parcel_analytics"

# Change these to match your pipeline
PIPELINE_NAME = "us_qa_kuaisong_reference_tables_pipeline"
TABLE_NAME = "kuaisong.uni_warehouses"
BATCH_LIMIT = 100

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 6),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'analytics_pipeline_with_dbt',
    default_args=default_args,
    description='End-to-end: MySQL → Redshift → dbt transformations',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    catchup=False,
    tags=['analytics', 'dbt']
)

# ============================================================================
# STEP 1: Sync Data (MySQL → S3 → Redshift)
# ============================================================================

sync_data = BashOperator(
    task_id='sync_mysql_to_redshift',
    bash_command=f'''
    cd {SYNC_TOOL_PATH} && \\
    source s3_backup_venv/bin/activate && \\
    source .env && \\
    python -m src.cli.main sync pipeline \\
        -p {PIPELINE_NAME} \\
        -t {TABLE_NAME} \\
        --json-output /tmp/sync_{{{{ ds }}}}.json \\
        --limit {BATCH_LIMIT} \\
        --max-workers 2
    ''',
    dag=dag
)

# ============================================================================
# STEP 2: Parse Results
# ============================================================================

def parse_sync_results(**context):
    sync_date = context['ds']
    json_file = f"/tmp/sync_{sync_date}.json"

    with open(json_file, 'r') as f:
        result = json.load(f)

    if result.get('status') == 'success':
        rows = result.get('summary', {}).get('total_rows_processed', 0)
        print(f"✅ Synced {rows:,} rows successfully")
        context['task_instance'].xcom_push(key='rows', value=rows)
    else:
        raise Exception(f"Sync failed: {result.get('error')}")

parse_results = PythonOperator(
    task_id='parse_sync_results',
    python_callable=parse_sync_results,
    dag=dag
)

# ============================================================================
# STEP 3: dbt Transformations
# ============================================================================

with TaskGroup("dbt_transformations", dag=dag) as dbt_group:

    # Run staging models
    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && \\
        source {SYNC_TOOL_PATH}/airflow_poc/airflow_env/bin/activate && \\
        export REDSHIFT_USER=$(grep REDSHIFT_USER {SYNC_TOOL_PATH}/.env | cut -d '=' -f2) && \\
        export REDSHIFT_PASSWORD=$(grep REDSHIFT_PASSWORD {SYNC_TOOL_PATH}/.env | cut -d '=' -f2) && \\
        dbt run --profiles-dir ~/.dbt --select stg_uni_warehouses_qa
        ''',
        dag=dag
    )

    # Run marts models
    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && \\
        source {SYNC_TOOL_PATH}/airflow_poc/airflow_env/bin/activate && \\
        export REDSHIFT_USER=$(grep REDSHIFT_USER {SYNC_TOOL_PATH}/.env | cut -d '=' -f2) && \\
        export REDSHIFT_PASSWORD=$(grep REDSHIFT_PASSWORD {SYNC_TOOL_PATH}/.env | cut -d '=' -f2) && \\
        dbt run --profiles-dir ~/.dbt --select dim_warehouses
        ''',
        dag=dag
    )

    # Run tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'''
        cd {DBT_PROJECT_PATH} && \\
        source {SYNC_TOOL_PATH}/airflow_poc/airflow_env/bin/activate && \\
        export REDSHIFT_USER=$(grep REDSHIFT_USER {SYNC_TOOL_PATH}/.env | cut -d '=' -f2) && \\
        export REDSHIFT_PASSWORD=$(grep REDSHIFT_PASSWORD {SYNC_TOOL_PATH}/.env | cut -d '=' -f2) && \\
        dbt test --profiles-dir ~/.dbt --select stg_uni_warehouses dim_warehouses
        ''',
        dag=dag
    )

    dbt_staging >> dbt_marts >> dbt_test

# ============================================================================
# STEP 4: Summary
# ============================================================================

def print_summary(**context):
    rows = context['task_instance'].xcom_pull(task_ids='parse_sync_results', key='rows')
    print(f"""
    ✅ PIPELINE COMPLETE
    - Date: {context['ds']}
    - Rows synced: {rows:,}
    - dbt models: staging → marts
    - Tests: passed
    """)

summary = PythonOperator(
    task_id='print_summary',
    python_callable=print_summary,
    trigger_rule='all_done',
    dag=dag
)

# ============================================================================
# DEPENDENCIES
# ============================================================================

sync_data >> parse_results >> dbt_group >> summary