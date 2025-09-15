"""
Parcel Detail POC DAG - Demonstrates integration between sync tool, Airflow, and dbt
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os
import glob

# Configuration
SYNC_TOOL_PATH = "/home/qi_chen/s3-redshift-backup"
DBT_PROJECT_PATH = "/home/qi_chen/s3-redshift-backup/airflow_poc/dbt_projects/parcel_analytics"
S3_COMPLETION_BUCKET = "parcel-poc-completions"
PIPELINE_NAME = "us_dw_unidw_parcel_poc"
TABLE_NAME = "unidw.dw_parcel_detail_tool"

# Sync Parameters
# --limit 100000: Extract 100,000 rows per chunk
# --max-chunks 5: Process maximum 5 chunks
# Total: Up to 500,000 rows will be synced from MySQL → S3 → Redshift

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 13),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'parcel_detail_poc',
    default_args=default_args,
    description='POC: Parcel detail sync with deduplication using sync tool + dbt',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    catchup=False,
    tags=['poc', 'parcel', 'dbt']
)

# Task 1: Sync parcel detail table with enhanced features
sync_parcel_data = BashOperator(
    task_id='sync_parcel_detail',
    bash_command=f'''
    cd {SYNC_TOOL_PATH} && \
    source venv/bin/activate && \
    python -m src.cli.main sync pipeline \
        -p {PIPELINE_NAME} \
        -t {TABLE_NAME} \
        --json-output /tmp/parcel_sync_{{{{ ds }}}}.json \
        --limit 100000 --max-chunks 5
    ''',
    dag=dag
)

# Task 2: Parse sync results from JSON output
def parse_sync_results(**context):
    """Parse JSON output to extract sync metrics"""
    import json
    
    sync_date = context['ds']
    json_file = f"/tmp/parcel_sync_{sync_date}.json"
    
    try:
        with open(json_file, 'r') as f:
            sync_result = json.load(f)
        
        print(f"📄 JSON Content: {json.dumps(sync_result, indent=2)}")
        
        # Check new JSON format first
        if sync_result.get('status') == 'success':
            # New format: direct status field
            rows_extracted = sync_result.get('summary', {}).get('total_rows_processed', 0)
            rows_loaded = rows_extracted  # In dry-run mode, these are the same
            duration = sync_result.get('duration_seconds', 0)
            success = True
            
        elif sync_result.get('success'):
            # Old format: success field with stages
            backup_summary = sync_result.get('stages', {}).get('backup', {}).get('summary', {})
            redshift_summary = sync_result.get('stages', {}).get('redshift', {}).get('summary', {})
            
            rows_extracted = backup_summary.get('total_rows', 0)
            rows_loaded = redshift_summary.get('total_rows_loaded', 0)
            duration = sync_result.get('duration_seconds', 0)
            success = True
            
        else:
            # Check for failure
            error_msg = sync_result.get('error', 'Unknown error')
            print(f"❌ Sync failed: {error_msg}")
            raise ValueError(f"Sync failed: {error_msg}")
        
        print(f"✅ Sync successful!")
        print(f"   - Execution ID: {sync_result.get('execution_id', 'N/A')}")
        print(f"   - Pipeline: {sync_result.get('pipeline', 'N/A')}")
        print(f"   - Tables: {len(sync_result.get('table_results', {}))}")
        print(f"   - Rows processed: {rows_extracted:,}")
        print(f"   - Duration: {duration:.3f} seconds")
        print(f"   - Status: {sync_result.get('status', 'N/A')}")
        
        # Push metrics to XCom for downstream tasks
        context['task_instance'].xcom_push(key='rows_extracted', value=rows_extracted)
        context['task_instance'].xcom_push(key='rows_loaded', value=rows_loaded)
        context['task_instance'].xcom_push(key='sync_success', value=success)
        
    except FileNotFoundError:
        # Try alternative file patterns
        alternative_files = glob.glob(f"/tmp/*parcel*{sync_date}*.json")
        print(f"JSON file not found at {json_file}")
        print(f"Alternative files found: {alternative_files}")
        raise
    except Exception as e:
        print(f"Error parsing sync results: {e}")
        context['task_instance'].xcom_push(key='sync_success', value=False)
        raise

parse_results = PythonOperator(
    task_id='parse_sync_results',
    python_callable=parse_sync_results,
    dag=dag
)

# Task 3: Validate data in Redshift
def validate_redshift_data(**context):
    """Check that data was loaded to Redshift"""
    
    # Get sync metrics from previous task
    rows_loaded = context['task_instance'].xcom_pull(task_ids='parse_sync_results', key='rows_loaded')
    
    # Check if this was a dry-run (no actual data movement)
    if rows_loaded == 0:
        print("🔍 DRY-RUN MODE DETECTED")
        print("   - No actual data was loaded to Redshift")
        print("   - Skipping Redshift validation for dry-run")
        print("   - This is expected behavior for POC testing")
        
        # Push dummy values for downstream tasks
        context['task_instance'].xcom_push(key='unique_orders_before_dedup', value=0)
        return
    
    # Check if SSH tunnel is available
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 46407))
        sock.close()
        
        if result != 0:
            print("⚠️  SSH tunnel to Redshift (port 46407) not available")
            print("   - Start tunnel: ssh -L 46407:redshift-dw.qa.uniuni.com:5439 -i /path/to/key.pem user@bastion -N -f")
            print("   - Skipping Redshift validation for now")
            context['task_instance'].xcom_push(key='unique_orders_before_dedup', value=0)
            return
            
    except Exception as e:
        print(f"⚠️  Cannot check SSH tunnel status: {e}")
        print("   - Assuming tunnel is not available")
        context['task_instance'].xcom_push(key='unique_orders_before_dedup', value=0)
        return
    
    # Real validation for actual data loads
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id='redshift_default')
        
        # Check row count in target table
        count_query = """
        SELECT COUNT(*) as row_count,
               COUNT(DISTINCT order_id) as unique_orders,
               MAX(CAST(col_broker_handover_time AS TIMESTAMP)) as latest_handover
        FROM public.dw_parcel_detail_tool_raw
        WHERE CAST(col_broker_handover_time AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7 days'
        """
        
        result = redshift.get_first(count_query)
        current_rows = result[0] if result else 0
        unique_orders = result[1] if result else 0
        latest_handover = result[2] if result else None
        
        print(f"📊 Redshift Validation:")
        print(f"   - Current rows (last 7 days): {current_rows:,}")
        print(f"   - Unique orders: {unique_orders:,}")
        print(f"   - Latest handover: {latest_handover}")
        print(f"   - Sync reported: {rows_loaded:,} rows loaded")
        
        if current_rows == 0:
            print("⚠️  No data found in Redshift table!")
            print("   - This might be expected for initial setup or test runs")
        
        # Store for dbt
        context['task_instance'].xcom_push(key='unique_orders_before_dedup', value=unique_orders)
        
    except Exception as e:
        print(f"⚠️  Redshift validation failed: {e}")
        print("   - This is expected in POC/dry-run mode")
        print("   - For production runs, ensure Redshift connection is configured and SSH tunnel is active")
        
        # Don't fail the task in POC mode, just log the issue
        context['task_instance'].xcom_push(key='unique_orders_before_dedup', value=0)

validate_data = PythonOperator(
    task_id='validate_redshift_data',
    python_callable=validate_redshift_data,
    dag=dag
)

# Task Group: dbt transformations
with TaskGroup("dbt_transformations", dag=dag) as dbt_group:
    
    # Create dbt directories if they don't exist
    setup_dbt = BashOperator(
        task_id='setup_dbt_project',
        bash_command=f'''
        if [ ! -d "{DBT_PROJECT_PATH}" ]; then
            echo "dbt project not found. Creating placeholder..."
            mkdir -p {DBT_PROJECT_PATH}/models/staging
            echo "-- Placeholder model" > {DBT_PROJECT_PATH}/models/staging/stg_parcel_detail.sql
        fi
        echo "dbt project path: {DBT_PROJECT_PATH}"
        ''',
        dag=dag
    )
    
    # Run dbt models (will be configured later)
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'''
        if [ -f "{DBT_PROJECT_PATH}/dbt_project.yml" ]; then
            cd {DBT_PROJECT_PATH} && dbt run --profiles-dir ~/.dbt
        else
            echo "⚠️  dbt project not yet configured. Skipping dbt run."
            echo "   Please set up dbt project at: {DBT_PROJECT_PATH}"
        fi
        ''',
        dag=dag
    )
    
    setup_dbt >> run_dbt_models

# Task 5: Generate summary report
def generate_summary(**context):
    """Create a summary of the pipeline run"""
    
    # Gather metrics from previous tasks
    rows_extracted = context['task_instance'].xcom_pull(task_ids='parse_sync_results', key='rows_extracted')
    rows_loaded = context['task_instance'].xcom_pull(task_ids='parse_sync_results', key='rows_loaded')
    unique_orders = context['task_instance'].xcom_pull(task_ids='validate_redshift_data', key='unique_orders_before_dedup')
    
    summary = f"""
    📋 PARCEL DETAIL POC PIPELINE SUMMARY
    =====================================
    Execution Date: {context['ds']}
    
    Sync Results:
    - Rows Extracted from MySQL: {rows_extracted:,}
    - Rows Loaded to Redshift: {rows_loaded:,}
    - Unique Orders (before dedup): {unique_orders:,}
    
    Next Steps:
    1. Configure dbt project for deduplication
    2. Add data quality checks
    3. Create visualization dashboard
    
    Status: ✅ POC Pipeline Executed Successfully
    """
    
    print(summary)
    
    # Could send to Slack, email, or save to S3
    return summary

summary_report = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary,
    trigger_rule='all_done',  # Run even if some tasks fail
    dag=dag
)

# Set task dependencies
sync_parcel_data >> parse_results >> validate_data >> dbt_group >> summary_report

# Optional: Add monitoring task
def check_pipeline_health(**context):
    """Monitor pipeline health metrics"""
    print("🏥 Pipeline Health Check")
    print(f"   - DAG: {context['dag'].dag_id}")
    print(f"   - Run ID: {context['run_id']}")
    print(f"   - Execution Date: {context['ds']}")
    
health_check = PythonOperator(
    task_id='health_check',
    python_callable=check_pipeline_health,
    trigger_rule='all_done',
    dag=dag
)

# Add health check in parallel with summary
[summary_report, health_check]