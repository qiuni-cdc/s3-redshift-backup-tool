 """
  US QA Kuaisong Reference Tables - Daily Sync
  Scheduled: 11 PM Vancouver Time (7 AM UTC)
  Tables: uni_warehouses, uni_customer
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

  # Reference tables to sync
  TABLES = [
      {
          "name": "kuaisong.uni_warehouses",
          "short_name": "warehouses",
          "limit": 1000
      },
      {
          "name": "kuaisong.uni_customer",
          "short_name": "customers",
          "limit": 10000
      }
  ]

  # ============================================================================
  # DAG DEFINITION
  # ============================================================================

  default_args = {
      'owner': 'data-team',
      'depends_on_past': False,
      'start_date': datetime(2025, 1, 7),  # Today
      'email_on_failure': False,
      'retries': 2,
      'retry_delay': timedelta(minutes=5),
      'execution_timeout': timedelta(minutes=30),
  }

  dag = DAG(
      'us_qa_reference_tables_11pm_vancouver',
      default_args=default_args,
      description='Daily sync at 11 PM Vancouver time',
      schedule_interval='0 7 * * *',  # 7 AM UTC = 11 PM Vancouver PST
      max_active_runs=1,
      catchup=False,
      tags=['us-qa', 'reference-tables', '11pm-vancouver']
  )

  # ============================================================================
  # TASKS: Sync each table
  # ============================================================================

  sync_tasks = []

  for table in TABLES:
      table_name = table["name"]
      short_name = table["short_name"]
      limit = table["limit"]

      sync_task = BashOperator(
          task_id=f'sync_{short_name}',
          bash_command=f'''
          cd {SYNC_TOOL_PATH} && \\
          source venv/bin/activate && \\
          source .env && \\

          echo "🔄 [{short_name}] Starting sync..." && \\
          echo "   Pipeline: {PIPELINE_NAME}" && \\
          echo "   Table: {table_name}" && \\
          echo "   Limit: {limit} rows" && \\

          python -m src.cli.main sync pipeline \\
              -p {PIPELINE_NAME} \\
              -t {table_name} \\
              --json-output /tmp/sync_{short_name}_{{{{ ds }}}}.json \\
              --limit {limit} \\
              --max-workers 2 && \\

          echo "✅ [{short_name}] Sync complete"
          ''',
          dag=dag
      )

      sync_tasks.append(sync_task)

  # ============================================================================
  # TASK: Validate and Summarize
  # ============================================================================

  def print_summary(**context):
      """Validate results and print summary"""

      results = []
      total_rows = 0
      failed = []

      for table in TABLES:
          short_name = table["short_name"]
          json_file = f"/tmp/sync_{short_name}_{context['ds']}.json"

          try:
              with open(json_file, 'r') as f:
                  result = json.load(f)

              if result.get('status') == 'success':
                  rows = result.get('summary', {}).get('total_rows_processed', 0)
                  duration = result.get('duration_seconds', 0)
                  results.append(f"✅ {table['name']}: {rows:,} rows ({duration:.1f}s)")
                  total_rows += rows
              else:
                  error = result.get('error', 'Unknown error')
                  results.append(f"❌ {table['name']}: FAILED - {error}")
                  failed.append(table['name'])
          except Exception as e:
              results.append(f"❌ {table['name']}: Error - {str(e)}")
              failed.append(table['name'])

      summary = f"""
  ╔════════════════════════════════════════════════════╗
  ║   US QA REFERENCE TABLES - 11 PM VANCOUVER SYNC    ║
  ╚════════════════════════════════════════════════════╝

  📅 Date: {context['ds']}
  🕚 Schedule: 11 PM Vancouver (7 AM UTC)
  📦 Pipeline: {PIPELINE_NAME}

  📊 Results:
  {chr(10).join(['   ' + r for r in results])}

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total Rows: {total_rows:,}

  {'✅ Status: SUCCESS' if not failed else f'❌ Status: FAILED - {len(failed)} table(s)'}

  📍 Target: Redshift public schema
     • public.uni_warehouses
     • public.uni_customer
      """

      print(summary)

      if failed:
          raise Exception(f"Sync failed for: {', '.join(failed)}")

      return summary

  summary_task = PythonOperator(
      task_id='print_summary',
      python_callable=print_summary,
      trigger_rule='all_done',
      dag=dag
  )

  # ============================================================================
  # DEPENDENCIES
  # ============================================================================

  sync_tasks >> summary_task