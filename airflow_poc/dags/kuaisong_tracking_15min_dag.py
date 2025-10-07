"""
Kuaisong Tracking Tables 15-Minute Sync DAG
Syncs 7 high-volume tracking tables every 15 minutes without historical data
Includes ID-only CDC tables: order_details (71M+ rows), uni_prealert_order (13.4M+ rows), and uni_sorting_parcels (139M+ rows)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import os
import subprocess

# Configuration
SYNC_TOOL_PATH = "/home/qi_chen/s3-redshift-backup"
PIPELINE_NAME = "us_prod_kuaisong_tracking_pipeline"
S3_COMPLETION_BUCKET = Variable.get("s3_completion_bucket", default_var="s3-sync-completions")

def get_smart_timeout(table_name):
    """Simple flag-based timeout: backlog mode = 15min, normal = 8min"""
    try:
        backlog_mode = Variable.get("kuaisong_backlog_mode", default_var="false")
        if backlog_mode.lower() == "true":
            print(f"🔧 {table_name}: BACKLOG MODE - Using 15 minute timeout")
            return timedelta(minutes=15)
        else:
            print(f"⚡ {table_name}: NORMAL MODE - Using 8 minute timeout")
            return timedelta(minutes=8)
    except Exception as e:
        print(f"⚠️ {table_name}: Timeout check failed ({e}), using safe default")
        return timedelta(minutes=10)

# Table configurations with expected volumes and optimized batch sizes
TABLES = [
    {"name": "kuaisong.uni_tracking_info", "expected_volume": 500000, "batch_limit": 100000},
    {"name": "kuaisong.uni_tracking_addon_spath", "expected_volume": 500000, "batch_limit": 20000},
    {"name": "kuaisong.uni_tracking_spath", "expected_volume": 2000000, "batch_limit": 500000},  # Highest volume = largest batch
    {"name": "kuaisong.ecs_order_info", "expected_volume": 500000, "batch_limit": 100000},
    {"name": "kuaisong.order_details", "expected_volume": 1000000, "batch_limit": 100000},  # Largest table (71M+ rows) - ID-only CDC
    {"name": "kuaisong.uni_prealert_order", "expected_volume": 200000, "batch_limit": 50000},  # 13.4M rows - ID-only CDC
    {"name": "kuaisong.uni_sorting_parcels", "expected_volume": 1500000, "batch_limit": 500000}  # 139M+ rows - ID-only CDC for sorting operations
]

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 17),
    'email_on_failure': False,  # Disabled - SMTP not configured
    'email': ['qi.chen@uniuni.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=60),  # 60 min timeout for large dataset processing
}

dag = DAG(
    'kuaisong_tracking_15min_sync',
    default_args=default_args,
    description='Sync 7 kuaisong tracking tables every 15 minutes - includes order_details, uni_prealert_order, and uni_sorting_parcels (all ID-only CDC)',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    max_active_runs=1,  # Only one instance at a time
    catchup=False,      # Don't run historical
    tags=['production', 'tracking', '15min', 'kuaisong', 'order_details', 'sorting']
)

def get_time_window(**context):
    """Calculate 15-minute time window for current execution"""
    execution_date = context['execution_date']
    end_time = execution_date
    start_time = execution_date - timedelta(minutes=15)
    
    # Store in XCom for downstream tasks
    context['task_instance'].xcom_push(key='start_time', value=start_time.isoformat())
    context['task_instance'].xcom_push(key='end_time', value=end_time.isoformat())
    
    print(f"⏰ Time Window: {start_time} to {end_time}")
    return {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat()
    }

# Task 1: Calculate time window
calculate_window = PythonOperator(
    task_id='calculate_time_window',
    python_callable=get_time_window,
    dag=dag
)

# Task Group: Staggered sync to protect production database
# Run tables sequentially to minimize concurrent load
with TaskGroup("sync_tables", dag=dag) as sync_group:
    
    previous_task = None
    for i, table_info in enumerate(TABLES):
        table_name = table_info["name"]
        clean_name = table_name.replace(".", "_")
        batch_limit = table_info["batch_limit"]
        expected_volume = table_info["expected_volume"]
        
        # Individual sync task for each table
        # The pipeline is configured with "timestamp_only" CDC strategy
        # with production-safe settings (volume-appropriate batch sizes)
        sync_task = BashOperator(
            task_id=f'sync_{clean_name}',
            bash_command=f'''
            cd {SYNC_TOOL_PATH} && \
            source venv/bin/activate && \
            
            # Set production safety environment variables
            export MYSQL_QUERY_TIMEOUT=60
            export MYSQL_READ_TIMEOUT=45
            export MYSQL_MAX_EXECUTION_TIME=60000  # 60 seconds in milliseconds
            
            # Run sync with volume-appropriate batch size ({batch_limit} rows for {expected_volume:,} daily volume)
            python -m src.cli.main sync pipeline \
                -p {PIPELINE_NAME} \
                -t {table_name} \
                --json-output /tmp/sync_{clean_name}_{{{{ ds }}}}_{{{{ ts_nodash }}}}.json \
                --limit {batch_limit} \
                --max-workers 1
            ''',
            dag=dag,
            execution_timeout=timedelta(minutes=60),  # 60-minute timeout for large dataset processing
        )
        
        # Chain tasks sequentially to avoid concurrent production queries
        if previous_task:
            previous_task >> sync_task
        previous_task = sync_task

# Task 3: Parse and aggregate results
def aggregate_results(**context):
    """Aggregate results from all table syncs"""
    import glob
    
    total_rows = 0
    total_files = 0
    failed_tables = []
    successful_tables = []
    execution_summaries = []
    
    for table_info in TABLES:
        table_name = table_info["name"]
        clean_name = table_name.replace(".", "_")
        expected_volume = table_info["expected_volume"]
        batch_limit = table_info["batch_limit"]
        
        # Find JSON output file
        pattern = f"/tmp/sync_{clean_name}_{context['ds']}*.json"
        json_files = glob.glob(pattern)
        
        if json_files:
            try:
                with open(json_files[0], 'r') as f:
                    result = json.load(f)
                
                if result.get('status') == 'success':
                    rows = result.get('summary', {}).get('total_rows_processed', 0)
                    files = result.get('summary', {}).get('total_files_created', 0)
                    
                    total_rows += rows
                    total_files += files
                    successful_tables.append(table_name)
                    
                    # Check if volume is reasonable
                    daily_expected = expected_volume
                    hourly_expected = daily_expected / 24
                    fifteen_min_expected = hourly_expected / 4
                    
                    if rows < fifteen_min_expected * 0.1:  # Less than 10% of expected
                        print(f"⚠️ WARNING: {table_name} synced only {rows} rows, expected ~{int(fifteen_min_expected)}")
                    
                    execution_summaries.append({
                        'table': table_name,
                        'status': 'success',
                        'rows': rows,
                        'files': files,
                        'expected_15min': int(fifteen_min_expected),
                        'batch_limit': batch_limit
                    })
                else:
                    failed_tables.append(table_name)
                    execution_summaries.append({
                        'table': table_name,
                        'status': 'failed',
                        'error': result.get('error', 'Unknown error')
                    })
                    
            except Exception as e:
                print(f"Error reading results for {table_name}: {e}")
                failed_tables.append(table_name)
                execution_summaries.append({
                    'table': table_name,
                    'status': 'error',
                    'error': str(e)
                })
        else:
            print(f"No results file found for {table_name}")
            failed_tables.append(table_name)
            execution_summaries.append({
                'table': table_name,
                'status': 'missing',
                'error': 'No output file found'
            })
    
    # Generate summary
    start_time = context['task_instance'].xcom_pull(task_ids='calculate_time_window', key='start_time')
    end_time = context['task_instance'].xcom_pull(task_ids='calculate_time_window', key='end_time')
    
    summary = {
        'execution_date': context['ds'],
        'time_window': f"{start_time} to {end_time}",
        'total_tables': len(TABLES),
        'successful_tables': len(successful_tables),
        'failed_tables': len(failed_tables),
        'total_rows_synced': total_rows,
        'total_files_created': total_files,
        'table_summaries': execution_summaries
    }
    
    print("\n📊 15-MINUTE SYNC SUMMARY")
    print("=" * 50)
    print(f"Time Window: {summary['time_window']}")
    print(f"Success: {summary['successful_tables']}/{summary['total_tables']} tables")
    print(f"Total Rows: {summary['total_rows_synced']:,}")
    print(f"Total Files: {summary['total_files_created']}")
    
    if failed_tables:
        print(f"\n❌ Failed Tables: {', '.join(failed_tables)}")
        
    print("\n📈 Table Details:")
    for table_summary in execution_summaries:
        if table_summary['status'] == 'success':
            batch_limit = table_summary.get('batch_limit', 'N/A')
            print(f"  ✅ {table_summary['table']}: {table_summary['rows']:,} rows (batch: {batch_limit}, expected ~{table_summary['expected_15min']:,})")
        else:
            print(f"  ❌ {table_summary['table']}: {table_summary.get('error', 'Failed')}")
    
    # Store summary for monitoring
    context['task_instance'].xcom_push(key='sync_summary', value=summary)
    
    # Fail the task if any table failed
    if failed_tables:
        raise Exception(f"Sync failed for {len(failed_tables)} tables: {', '.join(failed_tables)}")
    
    return summary

aggregate_results_task = PythonOperator(
    task_id='aggregate_results',
    python_callable=aggregate_results,
    trigger_rule='all_done',  # Run even if some syncs fail
    dag=dag
)

# Task 4: Data validation
def validate_data_freshness(**context):
    """Validate that we have fresh data in Redshift"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import socket
    
    # Check if SSH tunnel is available
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 46407))
        sock.close()
        
        if result != 0:
            print("⚠️ SSH tunnel not available, skipping Redshift validation")
            return
    except:
        print("⚠️ Cannot check SSH tunnel, skipping validation")
        return
    
    try:
        redshift = PostgresHook(postgres_conn_id='redshift_default')
        
        validation_results = []
        for table_info in TABLES:
            table_name = table_info["name"].split('.')[1]  # Remove schema
            
            # Check latest timestamp in each table (order_details uses ID-based CDC, no timestamp)
            if table_name == 'uni_tracking_info':
                query = f"SELECT MAX(FROM_UNIXTIME(update_time)) as latest FROM public.{table_name}"
            elif table_name == 'uni_tracking_addon_spath':
                query = f"SELECT MAX(create_at) as latest FROM public.{table_name}"
            elif table_name == 'uni_tracking_spath':
                query = f"SELECT MAX(FROM_UNIXTIME(pathTime)) as latest FROM public.{table_name}"
            elif table_name == 'ecs_order_info':
                query = f"SELECT MAX(FROM_UNIXTIME(add_time)) as latest FROM public.{table_name}"
            elif table_name == 'order_details':
                # ID-only table - check max ID and row count instead of timestamp
                query = f"SELECT MAX(id) as max_id, COUNT(*) as row_count FROM public.{table_name}"
            elif table_name == 'uni_sorting_parcels':
                # ID-only table - check max ID, row count, and latest sort time
                query = f"SELECT MAX(id) as max_id, COUNT(*) as row_count, MAX(FROM_UNIXTIME(sort_time)) as latest_sort FROM public.{table_name}"
            
            try:
                result = redshift.get_first(query)
                
                if table_name in ['order_details', 'uni_sorting_parcels']:
                    # Handle ID-only table validation
                    max_id = result[0] if result else None
                    row_count = result[1] if result and len(result) > 1 else None
                    
                    if max_id and row_count:
                        validation_info = {
                            'table': table_name,
                            'max_id': max_id,
                            'row_count': row_count,
                            'is_fresh': True,  # ID-based tables are always considered fresh if they have data
                            'validation_type': 'id_based'
                        }
                        
                        # For uni_sorting_parcels, also capture latest sort time
                        if table_name == 'uni_sorting_parcels' and len(result) > 2:
                            latest_sort = result[2]
                            if latest_sort:
                                validation_info['latest_sort_time'] = latest_sort.isoformat()
                                print(f"📊 {table_name}: Max ID {max_id:,}, Total rows {row_count:,}, Latest sort: {latest_sort}")
                            else:
                                print(f"📊 {table_name}: Max ID {max_id:,}, Total rows {row_count:,}")
                        else:
                            print(f"📊 {table_name}: Max ID {max_id:,}, Total rows {row_count:,}")
                        
                        validation_results.append(validation_info)
                    else:
                        validation_results.append({
                            'table': table_name,
                            'max_id': None,
                            'row_count': None,
                            'is_fresh': False,
                            'error': 'No data found',
                            'validation_type': 'id_based'
                        })
                else:
                    # Handle timestamp-based table validation
                    latest_timestamp = result[0] if result else None
                    
                    if latest_timestamp:
                        # Check if data is fresh (within last 30 minutes)
                        time_diff = datetime.now() - latest_timestamp
                        is_fresh = time_diff.total_seconds() < 1800  # 30 minutes
                        
                        validation_results.append({
                            'table': table_name,
                            'latest_data': latest_timestamp.isoformat(),
                            'is_fresh': is_fresh,
                            'lag_minutes': int(time_diff.total_seconds() / 60),
                            'validation_type': 'timestamp_based'
                        })
                        
                        print(f"📊 {table_name}: Latest data from {latest_timestamp} (lag: {int(time_diff.total_seconds() / 60)} min)")
                    else:
                        validation_results.append({
                            'table': table_name,
                            'latest_data': None,
                            'is_fresh': False,
                            'error': 'No data found',
                            'validation_type': 'timestamp_based'
                        })
                    
            except Exception as e:
                print(f"Error validating {table_name}: {e}")
                validation_results.append({
                    'table': table_name,
                    'error': str(e)
                })
        
        # Store validation results
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        
        # Alert if data is stale
        stale_tables = [r['table'] for r in validation_results if not r.get('is_fresh', False)]
        if stale_tables:
            print(f"\n⚠️ ALERT: Stale data detected in: {', '.join(stale_tables)}")
            
    except Exception as e:
        print(f"Redshift validation failed: {e}")

validate_task = PythonOperator(
    task_id='validate_data_freshness',
    python_callable=validate_data_freshness,
    trigger_rule='all_done',
    dag=dag
)

# Task 5: Send alerts if needed
def check_and_alert(**context):
    """Check results and send alerts for failures or anomalies"""
    sync_summary = context['task_instance'].xcom_pull(task_ids='aggregate_results', key='sync_summary')
    validation_results = context['task_instance'].xcom_pull(task_ids='validate_data_freshness', key='validation_results')
    
    alerts = []
    
    # Check for sync failures
    if sync_summary and sync_summary['failed_tables'] > 0:
        alerts.append(f"❌ Sync failed for {sync_summary['failed_tables']} tables")
    
    # Check for low row counts
    if sync_summary:
        for table_summary in sync_summary.get('table_summaries', []):
            if table_summary['status'] == 'success':
                actual = table_summary['rows']
                expected = table_summary['expected_15min']
                if actual < expected * 0.1:  # Less than 10% of expected
                    alerts.append(f"⚠️ Low row count for {table_summary['table']}: {actual} (expected ~{expected})")
    
    # Check for stale data
    if validation_results:
        stale_tables = [r['table'] for r in validation_results if not r.get('is_fresh', False)]
        if stale_tables:
            alerts.append(f"📊 Stale data in: {', '.join(stale_tables)}")
    
    if alerts:
        print("\n🚨 ALERTS SUMMARY:")
        for alert in alerts:
            print(f"  {alert}")
        
        # In production, send to Slack/PagerDuty/email here
        # slack_webhook.send(alerts)
        
    else:
        print("\n✅ All systems operational - no alerts")

alert_task = PythonOperator(
    task_id='check_and_alert',
    python_callable=check_and_alert,
    trigger_rule='all_done',
    dag=dag
)

# Set task dependencies
calculate_window >> sync_group >> aggregate_results_task >> validate_task >> alert_task
