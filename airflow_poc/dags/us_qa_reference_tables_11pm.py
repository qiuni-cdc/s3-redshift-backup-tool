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
import os
import sys

# Add sync tool to path for imports
sys.path.insert(0, '/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool')

import mysql.connector
import psycopg2

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
# SCHEMA VALIDATION FUNCTION
# ============================================================================

def validate_and_fix_schema(table_config, **context):
    """
    Compare MySQL and Redshift schemas. Drop Redshift table if mismatch detected.
    This prevents VARCHAR length errors and other schema drift issues.
    """
    source_table = table_config['name']  # e.g., kuaisong.uni_customer
    target_table = table_config['target']  # e.g., uni_customer_ref
    schema_name = 'settlement_public'

    print(f"\n{'='*80}")
    print(f"🔍 SCHEMA VALIDATION: {source_table} → {target_table}")
    print(f"{'='*80}\n")

    # Load environment variables
    env_file = f"{SYNC_TOOL_PATH}/.env"
    env_vars = {}
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key] = value.strip('"').strip("'")

    mysql_schema = {}
    redshift_schema = {}
    schema_mismatch = False

    try:
        # ===== STEP 1: Get MySQL Schema =====
        print("📊 Fetching MySQL schema...")
        mysql_conn = mysql.connector.connect(
            host='us-west-2.ro.db.qa.uniuni.com.internal',
            port=3306,
            database='kuaisong',
            user=env_vars.get('DB_USER'),
            password=env_vars.get('DB_US_QA_PASSWORD')
        )

        mysql_cursor = mysql_conn.cursor(dictionary=True)
        schema, table = source_table.split('.')

        mysql_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   CHARACTER_SET_NAME, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)

        # Character set to bytes per character mapping
        charset_bytes = {
            'latin1': 1, 'ascii': 1, 'utf8': 3, 'utf8mb4': 4,
            'ucs2': 2, 'utf16': 4, 'utf16le': 4, 'utf32': 4, 'binary': 1
        }

        for row in mysql_cursor.fetchall():
            col_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']
            max_length = row['CHARACTER_MAXIMUM_LENGTH']
            charset = row.get('CHARACTER_SET_NAME')

            # Calculate byte requirements for VARCHAR columns
            bytes_needed = max_length
            if data_type in ['varchar', 'char'] and max_length and charset:
                bytes_per_char = charset_bytes.get(charset.lower(), 4)
                bytes_needed = max_length * bytes_per_char

            mysql_schema[col_name] = {
                'type': data_type,
                'length': max_length,
                'charset': charset,
                'bytes_needed': bytes_needed,
                'precision': row['NUMERIC_PRECISION'],
                'scale': row['NUMERIC_SCALE']
            }

        mysql_cursor.close()
        mysql_conn.close()

        print(f"✅ MySQL: Found {len(mysql_schema)} columns")

        # ===== STEP 2: Get Redshift Schema =====
        print("📊 Fetching Redshift schema...")

        # Direct connection to Redshift (no SSH tunnel)
        redshift_host = 'redshift-dw.qa.uniuni.com'
        redshift_port = 5439

        try:
            redshift_conn = psycopg2.connect(
                host=redshift_host,
                port=redshift_port,
                dbname='dw',
                user=env_vars.get('REDSHIFT_USER'),
                password=env_vars.get('REDSHIFT_PASSWORD')
            )

            redshift_cursor = redshift_conn.cursor()

            # Check if table exists
            redshift_cursor.execute(f"""
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{target_table}'
                ORDER BY ordinal_position
            """)

            rows = redshift_cursor.fetchall()

            if not rows:
                print(f"⚠️  Redshift: Table {schema_name}.{target_table} does not exist")
                print("✅ No action needed - table will be created on first sync")
                redshift_cursor.close()
                redshift_conn.close()
                return

            for row in rows:
                col_name = row[0]
                data_type = row[1]
                max_length = row[2]

                redshift_schema[col_name] = {
                    'type': data_type,
                    'length': max_length,
                    'precision': row[3],
                    'scale': row[4]
                }

            print(f"✅ Redshift: Found {len(redshift_schema)} columns")

            # ===== STEP 3: Compare Schemas =====
            print("\n🔍 Comparing schemas...")

            mismatches = []

            for col_name, mysql_col in mysql_schema.items():
                if col_name not in redshift_schema:
                    mismatches.append(f"  ❌ Column '{col_name}' missing in Redshift")
                    continue

                redshift_col = redshift_schema[col_name]

                # Compare VARCHAR lengths (considering character set encoding)
                if mysql_col['type'] in ['varchar', 'char']:
                    mysql_bytes_needed = mysql_col.get('bytes_needed', mysql_col['length'])
                    mysql_char_len = mysql_col['length']
                    mysql_charset = mysql_col.get('charset', 'unknown')
                    redshift_len = redshift_col['length']

                    # Redshift VARCHAR is byte-based, MySQL is character-based
                    # Redshift should have enough bytes to store MySQL data with charset encoding
                    if redshift_len and mysql_bytes_needed and redshift_len < mysql_bytes_needed:
                        mismatches.append(
                            f"  ❌ Column '{col_name}': "
                            f"Redshift VARCHAR({redshift_len} bytes) < "
                            f"MySQL VARCHAR({mysql_char_len} chars, charset={mysql_charset}) "
                            f"which needs {mysql_bytes_needed} bytes"
                        )
                        schema_mismatch = True

            # Check for extra columns in Redshift
            for col_name in redshift_schema:
                if col_name not in mysql_schema:
                    mismatches.append(f"  ⚠️  Column '{col_name}' exists in Redshift but not in MySQL")

            if mismatches:
                print(f"\n⚠️  Found {len(mismatches)} schema differences:")
                for mismatch in mismatches:
                    print(mismatch)
            else:
                print("✅ Schemas match perfectly!")

            # ===== STEP 4: Drop Table if Mismatch =====
            if schema_mismatch:
                print(f"\n🔧 FIXING: Dropping table {schema_name}.{target_table} to allow recreation with correct schema")

                redshift_cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{target_table} CASCADE")
                redshift_conn.commit()

                print(f"✅ Table dropped successfully - will be recreated on next sync with correct schema")
            else:
                print(f"\n✅ Schema validation passed - no changes needed")

            redshift_cursor.close()
            redshift_conn.close()

        except psycopg2.OperationalError as e:
            print(f"⚠️  Cannot connect to Redshift: {e}")
            print("⚠️  Check network connectivity to redshift-dw.qa.uniuni.com:5439")
            print("✅ Continuing anyway - sync will handle table creation")
            return

    except Exception as e:
        print(f"❌ Schema validation error: {e}")
        print("⚠️  Continuing anyway - sync will attempt to proceed")
        import traceback
        traceback.print_exc()

    print(f"\n{'='*80}\n")

# ============================================================================
# TASK GENERATION - One task per table
# ============================================================================

validate_tasks = []
sync_tasks = []
parse_tasks = []

for idx, table in enumerate(TABLES):
    table_name = table['name']
    short_name = table['short_name']
    target_name = table['target']
    limit = table['limit']

    # Validation task - runs before sync
    def create_validate_function(table_config):
        """Closure to capture table config"""
        def validate_wrapper(**context):
            return validate_and_fix_schema(table_config, **context)
        return validate_wrapper

    validate_task = PythonOperator(
        task_id=f'validate_schema_{short_name}',
        python_callable=create_validate_function(table),
        dag=dag
    )

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

    # Set dependency: validate → sync → parse for each table
    validate_task >> sync_task >> parse_task

    validate_tasks.append(validate_task)
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
