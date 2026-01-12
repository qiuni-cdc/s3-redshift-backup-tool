"""
US QA Kuaisong Reference Tables - Daily Full Sync
Scheduled: 11 PM Vancouver Time (7 AM UTC)

Tables:
1. uni_warehouses (~1K rows)
2. uni_customer (~10K rows)
3. uni_prealert_info (~50K rows)
4. ecs_staff (~500 rows)
5. uni_mawb_box (~10K rows)

Pipeline: us_qa_kuaisong_reference_tables_pipeline
Strategy: Full sync (TRUNCATE + RELOAD)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

# ============================================================================
# CONFIGURATION
# ============================================================================

SYNC_TOOL_PATH = "/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
PIPELINE_NAME = "us_qa_kuaisong_reference_tables_pipeline"

# All 5 tables from the pipeline configuration
TABLES = [
    {
        "name": "kuaisong.uni_warehouses",
        "target": "uni_warehouses_ref",
        "short_name": "warehouses",
        "limit": 50000
    },
    {
        "name": "kuaisong.uni_customer",
        "target": "uni_customer_ref",
        "short_name": "customers",
        "limit": 50000
    },
    {
        "name": "kuaisong.uni_prealert_info",
        "target": "uni_prealert_info_ref",
        "short_name": "prealert",
        "limit": 50000
    },
    {
        "name": "kuaisong.ecs_staff",
        "target": "ecs_staff_ref",
        "short_name": "staff",
        "limit": 50000
    },
    {
        "name": "kuaisong.uni_mawb_box",
        "target": "uni_mawb_box_ref",
        "short_name": "mawb_box",
        "limit": 50000
    }
]

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 11),  # Today
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=45),
}

dag = DAG(
    'us_qa_dim_tables_11pm',
    default_args=default_args,
    description='Daily full sync of US QA Kuaisong reference tables at 11 PM Vancouver',
    schedule_interval='0 7 * * *',  # 7 AM UTC = 11 PM PST (Vancouver)
    max_active_runs=1,
    catchup=False,
    tags=['reference-tables', 'us-qa', 'full-sync']
)

# ============================================================================
# TASK GENERATION - One task per table
# ============================================================================

sync_tasks = []
parse_tasks = []

for idx, table in enumerate(TABLES):
    table_name = table['name']
    short_name = table['short_name']
    target_name = table['target']
    limit = table['limit']

    # Sync task for each table
    sync_task = BashOperator(
        task_id=f'sync_{short_name}',
        bash_command=f'''
        cd {SYNC_TOOL_PATH} && \\
        source s3_backup_venv/bin/activate && \\
        source .env && \\
        python -m src.cli.main sync pipeline \\
            -p {PIPELINE_NAME} \\
            -t {table_name} \\
            --json-output /tmp/sync_{short_name}_{{{{ ds }}}}.json \\
            --limit {limit} \\
            --max-workers 2
        ''',
        dag=dag
    )

    # Parse result task for each table
    def create_parse_function(table_config):
        """Closure to capture table config for each task"""
        def parse_sync_results(**context):
            sync_date = context['ds']
            short = table_config['short_name']
            json_file = f"/tmp/sync_{short}_{sync_date}.json"

            try:
                with open(json_file, 'r') as f:
                    result = json.load(f)

                if result.get('status') == 'success':
                    rows = result.get('summary', {}).get('total_rows_processed', 0)
                    print(f"✅ {table_config['name']} → {table_config['target']}: {rows:,} rows synced")
                    context['task_instance'].xcom_push(
                        key=f'rows_{short}',
                        value={'rows': rows, 'table': table_config['name'], 'target': table_config['target']}
                    )
                else:
                    error_msg = result.get('error', 'Unknown error')
                    raise Exception(f"Sync failed for {table_config['name']}: {error_msg}")
            except FileNotFoundError:
                raise Exception(f"JSON output file not found: {json_file}")
            except json.JSONDecodeError as e:
                raise Exception(f"Invalid JSON in output file: {e}")

        return parse_sync_results

    parse_task = PythonOperator(
        task_id=f'parse_{short_name}_results',
        python_callable=create_parse_function(table),
        dag=dag
    )

    # Set dependency: sync → parse for each table
    sync_task >> parse_task

    sync_tasks.append(sync_task)
    parse_tasks.append(parse_task)

# ============================================================================
# SUMMARY TASK - Runs after all tables complete
# ============================================================================

def print_pipeline_summary(**context):
    """Print comprehensive summary of all table syncs"""
    print("\n" + "="*80)
    print("US QA KUAISONG REFERENCE TABLES - FULL SYNC SUMMARY")
    print("="*80)
    print(f"Execution Date: {context['ds']}")
    print(f"Pipeline: {PIPELINE_NAME}")
    print("-"*80)

    total_rows = 0
    success_count = 0

    for table in TABLES:
        short_name = table['short_name']
        try:
            result = context['task_instance'].xcom_pull(
                task_ids=f'parse_{short_name}_results',
                key=f'rows_{short_name}'
            )
            if result:
                rows = result['rows']
                source = result['table']
                target = result['target']
                total_rows += rows
                success_count += 1
                print(f"✅ {source:35s} → {target:30s} {rows:>10,} rows")
            else:
                print(f"⚠️  {table['name']:35s} → No data returned")
        except Exception as e:
            print(f"❌ {table['name']:35s} → ERROR: {str(e)}")

    print("-"*80)
    print(f"Total Tables Processed: {success_count}/{len(TABLES)}")
    print(f"Total Rows Synced:      {total_rows:,}")
    print("="*80 + "\n")

    # Push summary to XCom for monitoring
    context['task_instance'].xcom_push(key='summary', value={
        'total_tables': len(TABLES),
        'successful_tables': success_count,
        'total_rows': total_rows,
        'execution_date': context['ds']
    })

summary = PythonOperator(
    task_id='print_pipeline_summary',
    python_callable=print_pipeline_summary,
    trigger_rule='all_done',  # Run even if some tables fail
    dag=dag
)

# ============================================================================
# DEPENDENCIES
# ============================================================================

# All parse tasks must complete before summary
parse_tasks >> summary
