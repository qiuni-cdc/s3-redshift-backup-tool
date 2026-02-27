# Order Tracking Pipeline — Code Walkthrough v2

**Updated**: 2026-02-27
**Purpose**: Engineers' reference — walks through the actual code, file by file, task by task,
with specific function names, SQL patterns, config values, and file paths.
For the logical reasoning behind each design decision, see `logic_walkthrough_v2.md`.

---

## Table of Contents

1. [Repository Layout](#1-repository-layout)
2. [Pipeline Configuration — order_tracking_hybrid_dbt_pipeline.yml](#2-pipeline-configuration)
3. [Main DAG — order_tracking_hybrid_dbt_dag.py](#3-main-dag)
   - [3a. Constants and Path Detection](#3a-constants-and-path-detection)
   - [3b. Task: check_time_drift](#3b-task-check_time_drift)
   - [3c. Task: calculate_sync_window](#3c-task-calculate_sync_window)
   - [3d. TaskGroup: extraction (parallel)](#3d-taskgroup-extraction-parallel)
   - [3e. Task: validate_extractions](#3e-task-validate_extractions)
   - [3f. Task: trim_raw_tables](#3f-task-trim_raw_tables)
   - [3g. dbt Tasks: mart_uti, mart_ecs, mart_uts](#3g-dbt-tasks-mart_uti-mart_ecs-mart_uts)
   - [3h. Task: dbt_test](#3h-task-dbt_test)
   - [3i. Task: summary](#3i-task-summary)
   - [3j. Task dependencies](#3j-task-dependencies)
4. [dbt Models — Mart Layer](#4-dbt-models--mart-layer)
   - [4a. mart_uni_tracking_info.sql](#4a-mart_uni_tracking_infosql)
   - [4b. mart_ecs_order_info.sql](#4b-mart_ecs_order_infosql)
   - [4c. mart_uni_tracking_spath.sql](#4c-mart_uni_tracking_spathsql)
5. [dbt Models — Staging Layer](#5-dbt-models--staging-layer)
   - [5a. stg_uni_tracking_info.sql](#5a-stg_uni_tracking_infosql)
   - [5b. stg_ecs_order_info.sql](#5b-stg_ecs_order_infosql)
   - [5c. stg_uni_tracking_spath.sql](#5c-stg_uni_tracking_spathsql)
6. [Watermarks — Seeding and Structure](#6-watermarks--seeding-and-structure)
7. [Monitoring DAG — order_tracking_daily_monitoring.py](#7-monitoring-dag)
8. [Vacuum DAG — order_tracking_vacuum.py](#8-vacuum-dag)
9. [SSH Tunnel Handling for dbt](#9-ssh-tunnel-handling-for-dbt)
10. [Environment Variables Reference](#10-environment-variables-reference)
11. [Operational Tasks — Periodic Maintenance](#11-operational-tasks--periodic-maintenance)

---

## 1. Repository Layout

Relevant files for this pipeline:

```
s3-redshift-backup-tool/
├── config/pipelines/
│   └── order_tracking_hybrid_dbt_pipeline.yml     # CDC config for all 3 tables
├── seed_watermarks_to_now.py                       # First-run watermark seeding
├── src/cli/
│   ├── main.py                                     # Entry point: sync pipeline
│   └── check_time_drift.py                         # MySQL vs Airflow clock check
├── airflow_poc/
│   ├── dags/
│   │   ├── order_tracking_hybrid_dbt_dag.py        # Main 15-min pipeline DAG
│   │   ├── order_tracking_daily_monitoring.py      # DQ checks (3am daily)
│   │   └── order_tracking_vacuum.py                # VACUUM (2am daily)
│   ├── dbt_projects/order_tracking/
│   │   ├── dbt_project.yml                         # dbt project + var(mart_schema)
│   │   ├── profiles.yml                            # Redshift connection profiles
│   │   └── models/
│   │       ├── mart/
│   │       │   ├── mart_uni_tracking_info.sql
│   │       │   ├── mart_ecs_order_info.sql
│   │       │   └── mart_uni_tracking_spath.sql
│   │       └── staging/
│   │           ├── stg_uni_tracking_info.sql
│   │           ├── stg_ecs_order_info.sql
│   │           └── stg_uni_tracking_spath.sql
│   └── docs/
│       ├── logic_walkthrough_v2.md                 # Logic and reasoning
│       └── code_walkthrough_v2.md                  # This file
```

---

## 2. Pipeline Configuration

**File**: `config/pipelines/order_tracking_hybrid_dbt_pipeline.yml`

```yaml
pipeline:
  name: "order_tracking_hybrid_dbt"
  source: "US_PROD_RO_SSH"
  target: "redshift_default_direct"
  s3_config: "s3_qa"
  sync_frequency: "*/15 * * * *"

  processing:
    strategy: "parallel"
    batch_size: 100000
    max_parallel_tables: 3
    query_timeout_seconds: 300

  s3:
    isolation_prefix: "order_tracking_hybrid_dbt/"
    partition_strategy: "datetime"
    compression: "snappy"

  window_settings:
    lookback_minutes: 15
    buffer_minutes: 5
```

Three tables are configured, each with `cdc_strategy: "timestamp_only"`:

| Table                      | Timestamp col | Primary key              | Target raw table              | batch_size |
|----------------------------|---------------|--------------------------|-------------------------------|------------|
| `kuaisong.ecs_order_info`  | `add_time`    | `order_id`               | `settlement_public.ecs_order_info_raw` | 100,000 |
| `kuaisong.uni_tracking_info` | `update_time` | `order_id`             | `settlement_public.uni_tracking_info_raw` | 100,000 |
| `kuaisong.uni_tracking_spath` | `pathTime`  | `[order_id, traceSeq]`   | `settlement_public.uni_tracking_spath_raw` | 200,000 |

Notes:
- `cdc_id_column` is set for each table to ensure stable sort order in batch queries and
  prevent duplicates at chunk boundaries. For uts, this is `id` (not the composite PK).
- `target_name` field maps MySQL table names to Redshift raw table names with the `_raw` suffix.
- `target_schema: "settlement_public"` puts raw tables in a separate schema from mart tables
  (`settlement_ods`).
- uts gets `batch_size: 200,000` and `memory_limit_mb: 8192` for the higher-volume table.

---

## 3. Main DAG

**File**: `airflow_poc/dags/order_tracking_hybrid_dbt_dag.py`

DAG ID: `order_tracking_hybrid_dbt_sync`
Schedule: `*/15 * * * *`
`max_active_runs=1`, `catchup=False`
Default retries: 2, retry_delay: 3 minutes, execution_timeout: 60 minutes.
Email on failure: `jasleen.tung@uniuni.com`

---

### 3a. Constants and Path Detection

```python
DEFAULT_TOOL_PATH = '/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool'
if os.path.exists('/opt/airflow/sync_tool'):
    DEFAULT_TOOL_PATH = '/opt/airflow/sync_tool'

SYNC_TOOL_PATH   = os.environ.get('SYNC_TOOL_PATH', DEFAULT_TOOL_PATH)
DBT_PROJECT_PATH = os.environ.get('DBT_PROJECT_PATH', ...)
DBT_VENV_PATH    = os.environ.get('DBT_VENV_PATH', '/home/airflow/.local')
PIPELINE_NAME    = os.environ.get('SYNC_PIPELINE_NAME', 'order_tracking_hybrid_dbt_pipeline')

BUFFER_MINUTES              = 5
INCREMENTAL_LOOKBACK_MINUTES = 20   # 15-min window + 5-min buffer
TIME_DRIFT_THRESHOLD_SECONDS = 60
```

Docker environment detection: if `/opt/airflow/sync_tool` exists, the tool path switches to
the Docker mount. This supports both local Docker testing and Ubuntu server deployment.

`TABLES` dict maps three short keys (`ecs`, `uti`, `uts`) to their full MySQL name, timestamp
column, and Redshift target table. This dict is used by the extraction BashOperator tasks.

---

### 3b. Task: check_time_drift

**Type**: `BashOperator`
**Task ID**: `check_time_drift`

```bash
cd {SYNC_TOOL_PATH}
[ -f venv/bin/activate ] && source venv/bin/activate
python src/cli/check_time_drift.py
```

Runs `src/cli/check_time_drift.py`, which:
1. Connects to MySQL via `ConnectionManager`
2. Executes `SELECT UNIX_TIMESTAMP()`
3. Compares to `datetime.now().timestamp()` (Airflow wall clock)
4. Prints JSON to stdout: `{"mysql_now": ..., "airflow_now": ..., "drift_seconds": ..., "status": "success"}`

The JSON is captured as XCom via `do_xcom_push=True`. Downstream task `calculate_sync_window`
pulls `key='return_value'` from this task. If the script fails, it prints JSON with
`"status": "error"` and exits with code 1.

---

### 3c. Task: calculate_sync_window

**Type**: `PythonOperator`
**Task ID**: `calculate_sync_window`
**Function**: `calculate_sync_window(**context)`

```python
mysql_now = int(datetime.utcnow().timestamp())   # wall clock, not data_interval_start
to_unix   = mysql_now - (BUFFER_MINUTES * 60)   # 5-min buffer applied
sync_window = {
    'to_unix': to_unix,
    'to_ts':   datetime.fromtimestamp(to_unix).isoformat()
}
context['task_instance'].xcom_push(key='sync_window', value=sync_window)
```

Key design choice: uses `datetime.utcnow()` (wall clock), not Airflow's `data_interval_start`.
The reason is documented in the code: using `data_interval_start` caused stale windows when
Airflow ran backlogged slots (a Feb 21 slot running Feb 24 would produce `to_ts = Feb 21`).
Since the watermark handles idempotency, wall clock is always correct.

`to_ts` is the only value pushed downstream. The lower bound (watermark) is determined
inside the extraction CLI, not by this task.

---

### 3d. TaskGroup: extraction (parallel)

**Type**: `TaskGroup` containing 3 `BashOperator` tasks
**Task IDs**: `extraction.extract_ecs`, `extraction.extract_uti`, `extraction.extract_uts`

All three tasks share the same pattern:

```bash
cd {SYNC_TOOL_PATH}
[ -f venv/bin/activate ] && source venv/bin/activate

python -m src.cli.main sync pipeline \
    -p {PIPELINE_NAME} \
    -t {table_full_name} \
    --json-output /tmp/hybrid_{key}_{ds_nodash}_{ts_nodash}.json \
    --initial-lookback-minutes 20 \
    --end-time "{{ task_instance.xcom_pull(task_ids='calculate_sync_window', key='sync_window')['to_ts'].replace('T', ' ').split('.')[0] }}"
```

The `--end-time` argument extracts `to_ts` from XCom and reformats it:
- `to_ts` is an ISO string like `2026-02-27T04:15:17.123456`
- `.replace('T', ' ')` → `2026-02-27 04:15:17.123456`
- `.split('.')[0]` → `2026-02-27 04:15:17`
- Final value passed to MySQL: `'2026-02-27 04:15:17'`

The extraction CLI (`src.cli.main sync pipeline`) reads the watermark from S3, converts the
ISO timestamp string watermark to unix epoch for the `WHERE ts > watermark_unix` lower bound,
and applies `WHERE ts < end_time` as the upper bound. The resulting rows are written to the
corresponding `*_raw` table in Redshift.

The `--json-output` flag writes a JSON result file to `/tmp/` with:
```json
{
  "status": "success",
  "summary": {"total_rows_processed": 12345}
}
```

The three extraction tasks have no dependencies between them — they run in parallel.

---

### 3e. Task: validate_extractions

**Type**: `PythonOperator`
**Task ID**: `validate_extractions`
**Function**: `validate_extractions(**context)`

```python
for table_key in ['ecs', 'uti', 'uts']:
    filepath = f'/tmp/hybrid_{table_key}_{ds_nodash}_{ts_nodash}.json'
    with open(filepath) as f:
        result = json.load(f)
    if result.get('status') == 'success':
        rows = result['summary']['total_rows_processed']
        # 0 rows → warning logged, NOT a failure
    else:
        failed.append(f"{table_key}: {result.get('error')}")

if failed:
    raise ValueError(f"Extraction failed for {len(failed)} table(s) — dbt blocked: ...")
```

Reads the three JSON files from `/tmp/`. Any `status != 'success'` or unreadable file is
collected into a `failed` list. If `failed` is non-empty, `raise ValueError(...)` marks
the task as failed and Airflow skips all downstream tasks (validate → trim_raw → dbt_mart_uti
→ dbt_mart_ecs → dbt_mart_uts → dbt_test).

Results and total row count are pushed to XCom for the `summary` task.

---

### 3f. Task: trim_raw_tables

**Type**: `PythonOperator`
**Task ID**: `trim_raw_tables`
**Function**: `trim_raw_tables(**context)`

Connects to Redshift using environment variables directly (not `PostgresHook`) to avoid
IAM auth issues:

```python
host     = os.environ.get('REDSHIFT_HOST', 'redshift-dw.qa.uniuni.com')
port     = int(os.environ.get('REDSHIFT_PORT', '5439'))
user     = os.environ.get('REDSHIFT_QA_USER') or os.environ.get('REDSHIFT_USER') or ...
password = os.environ.get('REDSHIFT_QA_PASSWORD') or os.environ.get('REDSHIFT_PASSWORD') or ...
conn = psycopg2.connect(host=host, port=port, dbname='dw', user=user, password=password)
conn.autocommit = True
```

Tables trimmed (24-hour retention):

```python
tables = [
    ('settlement_public.uni_tracking_info_raw', 'update_time'),
    ('settlement_public.uni_tracking_spath_raw', 'pathTime'),
]
cutoff = "extract(epoch from current_timestamp - interval '24 hours')"
```

For each table, executes:
```sql
SELECT COUNT(*) FROM {table} WHERE {ts_col} < {cutoff}
DELETE FROM {table} WHERE {ts_col} < {cutoff}
```

`settlement_public.ecs_order_info_raw` is explicitly skipped. The reason is logged:
`add_time` is the order creation time, not the extraction time — trimming by it would delete
valid raw rows for orders created months ago but only recently extracted.

Total deleted rows pushed to XCom as `key='raw_rows_trimmed'`.

---

### 3g. dbt Tasks: mart_uti, mart_ecs, mart_uts

**Type**: `BashOperator` (3 tasks)
**Task IDs**: `dbt_mart_uti`, `dbt_mart_ecs`, `dbt_mart_uts`
**Execution timeout**: 10 minutes each

Each task runs one dbt model:

```bash
dbt run --select mart_uni_tracking_info --profiles-dir .
dbt run --select mart_ecs_order_info    --profiles-dir .
dbt run --select mart_uni_tracking_spath --profiles-dir .
```

All three share the same `bash_command` structure: `DBT_WITH_TUNNEL + dbt run ... + DBT_CLEANUP_TUNNEL`.
The tunnel handling is described in §9.

The `env=dbt_env_vars` parameter passes all environment variables (loaded from `.env` at
DAG parse time via `load_env_vars()`) to the BashOperator subprocess.

---

### 3h. Task: dbt_test

**Type**: `BashOperator`
**Task ID**: `dbt_test`
**Execution timeout**: 5 minutes

```bash
dbt test --select mart --store-failures --profiles-dir .
```

Scoped to `--select mart`: runs only tests defined in the mart models' `schema.yml`.
Staging tests are excluded — staging models are not updated in this pipeline and their
relationship tests would fail against stale data.

`--store-failures` writes failed test rows to Redshift for debugging.

---

### 3i. Task: summary

**Type**: `PythonOperator`
**Task ID**: `summary`
**TriggerRule**: `TriggerRule.ALL_DONE`

Pulls XCom from `validate_extractions` (extraction counts) and `trim_raw_tables` (rows trimmed)
and prints a formatted summary block. `ALL_DONE` ensures the summary runs regardless of
whether upstream tasks succeeded or failed — useful for diagnosing partial failures.

---

### 3j. Task Dependencies

```python
check_drift >> calc_window >> extraction_group >> validate >> trim_raw >> dbt_mart_uti
dbt_mart_uti >> [dbt_mart_ecs, dbt_mart_uts]
[dbt_mart_ecs, dbt_mart_uts] >> dbt_test >> summary
```

The DAG is strictly linear up to `dbt_mart_uti`. After mart_uti completes, mart_ecs and mart_uts
fan out in parallel. Both must complete before `dbt_test` runs. `summary` always runs last.

---

## 4. dbt Models — Mart Layer

All mart models use:
- `materialized='incremental'`
- `incremental_strategy='delete+insert'`
- `dist='order_id'`

The `source_cutoff` Jinja pattern is shared by all three models. It uses `run_query()` inside
an `is_incremental()` guard, which means it only executes on incremental runs (not the first
full-load run). The result is an integer unix timestamp.

### 4a. mart_uni_tracking_info.sql

**File**: `models/mart/mart_uni_tracking_info.sql`

**Source cutoff** (mart-relative, 30 minutes):
```sql
select coalesce(max(update_time), 0) - 1800 from {{ this }}
```
Queries the mart table itself. `source_cutoff` = current mart max - 30 minutes.

**Config**:
```python
config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    dist='order_id',
    sort=['update_time', 'order_id'],
    post_hook=[_ph1, _ph2a, _ph3, _ph4]
)
```

No `incremental_predicates`. The DELETE is pure `WHERE order_id IN (batch)` — no time
window, always finds the old row regardless of its age.

**Main query**:
```sql
with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    where update_time > {{ source_cutoff }}   -- 30-min scan
),
ranked as (
    select *,
        row_number() over (partition by order_id order by update_time desc) as _rn
    from filtered
)
select * exclude(_rn)
from ranked
where _rn = 1
order by update_time, order_id
```

**Post-hooks** (4 steps defined as Jinja string variables `_ph1`, `_ph2a`, `_ph3`, `_ph4`):

`_ph1` — Stale hist cleanup for reactivated orders:
```sql
DELETE FROM {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2
WHERE order_id IN (
    SELECT order_id FROM {{ this }}
    WHERE update_time >= (SELECT COALESCE(MAX(update_time), 0) - 900 FROM {{ this }})
)
```
The 900-second (15-minute) window identifies orders that were just inserted this cycle.
**Update this table name on 1 Jan and 1 Jul** to point to the current hist_uti table.

`_ph2a` — Archive aged-out rows to hist_2025_h2:
```sql
INSERT INTO {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2
SELECT * FROM {{ this }}
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }})
  AND update_time >= extract(epoch from '2025-07-01'::timestamp)
  AND update_time < extract(epoch from '2026-01-01'::timestamp)
  AND order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2)
```
Period routing: `update_time` must fall within the 2025_h2 range (Jul 2025 – Jan 2026).
`NOT IN` guard makes it idempotent on retry. Add a `_ph2b` block when data starts aging
into the 2026_h1 period.

`_ph3` — Safety check (archive routing gap):
```sql
INSERT INTO {{ var('mart_schema') }}.order_tracking_exceptions
    (order_id, exception_type, detected_at, notes)
SELECT DISTINCT m.order_id, 'ARCHIVE_ROUTING_GAP', CURRENT_TIMESTAMP, '...'
FROM {{ this }} m
WHERE m.update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }})
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_info_2025_h2 WHERE order_id = m.order_id)
  AND NOT EXISTS (SELECT 1 FROM order_tracking_exceptions WHERE order_id = m.order_id
                    AND exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL)
```

`_ph4` — Trim aged-out rows:
```sql
DELETE FROM {{ this }}
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }})
  AND order_id NOT IN (
      SELECT order_id FROM {{ var('mart_schema') }}.order_tracking_exceptions
      WHERE exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL
  )
```

---

### 4b. mart_ecs_order_info.sql

**File**: `models/mart/mart_ecs_order_info.sql`

**Source cutoff** (raw-relative, 2 hours):
```sql
select coalesce(max(add_time), 0) - 7200 from settlement_public.ecs_order_info_raw
```
Queries the raw table, not the mart. `source_cutoff` = raw max - 2 hours.

**Config**:
```python
config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    dist='order_id',
    sort=['partner_id', 'add_time', 'order_id'],
    incremental_predicates=[
        this ~ ".add_time > (SELECT COALESCE(MAX(add_time), 0) - 7200 FROM " ~ this ~ ")"
    ],
    post_hook=[_ph1a, _ph2]
)
```

`incremental_predicates` restricts the DELETE to the last 2 hours of mart_ecs. Since
`add_time` never changes, old orders never appear in new batches — restricting DELETE to
2 hours is safe and allows zone map pruning on `SORTKEY(add_time)`.

**Main query**:
```sql
with filtered as (
    select *
    from settlement_public.ecs_order_info_raw
    where add_time > {{ source_cutoff }}   -- 2h scan from raw's latest
),
ranked as (
    select *,
        row_number() over (partition by order_id order by add_time desc) as _rn
    from filtered
)
select * exclude(_rn) from ranked where _rn = 1
order by add_time, order_id
```

**Post-hooks** (2 steps):

`_ph1a` — Archive inactive orders (LEFT JOIN anti-join):
```sql
INSERT INTO {{ var('mart_schema') }}.hist_ecs_order_info_2025_h2
SELECT ecs.*
FROM {{ this }} ecs
LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id
WHERE uti.order_id IS NULL                  -- absent from mart_uti = inactive
  AND ecs.add_time >= extract(epoch from '2025-07-01'::timestamp)
  AND ecs.add_time < extract(epoch from '2026-01-01'::timestamp)
  AND ecs.order_id NOT IN (
      SELECT order_id FROM {{ var('mart_schema') }}.hist_ecs_order_info_2025_h2
  )
```

`ref('mart_uni_tracking_info')` is critical: it creates a dbt compile-time dependency that
forces mart_ecs to run after mart_uti. This is how the DAG ordering is enforced at the
dbt level in addition to the Airflow-level task dependency.

`_ph2` — Trim inactive orders (DELETE USING anti-join, NULL-safe):
```sql
DELETE FROM {{ this }}
USING (
    SELECT ecs.order_id
    FROM {{ this }} ecs
    LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id
    WHERE uti.order_id IS NULL
) to_trim
WHERE {{ this }}.order_id = to_trim.order_id
```

`DELETE USING` with LEFT JOIN is NULL-safe. `NOT IN` would return NULL (not FALSE) if mart_uti
contains any NULL `order_id`, causing all deletions to be silently skipped. At 90M+ rows,
the risk is real.

---

### 4c. mart_uni_tracking_spath.sql

**File**: `models/mart/mart_uni_tracking_spath.sql`

**Source cutoff** (mart-relative, 30 minutes):
```sql
select coalesce(max(pathTime), 0) - 1800 from {{ this }}
```

**Config**:
```python
config(
    materialized='incremental',
    unique_key=['order_id', 'traceSeq', 'pathTime'],
    incremental_strategy='delete+insert',
    dist='order_id',
    sort=['pathTime', 'order_id'],
    incremental_predicates=[
        this ~ ".pathTime > (SELECT COALESCE(MAX(pathTime), 0) - 7200 FROM " ~ this ~ ")"
    ],
    post_hook=[_ph1a, _ph2, _ph3]
)
```

`incremental_predicates` restricts DELETE to 2 hours on `pathTime`. New spath events always
have recent pathTime — old events (different pathTime) are never in a new batch. Zone maps on
`SORTKEY(pathTime)` make this DELETE efficient.

Note on incremental_predicates: the predicate uses a SQL subquery (not a Jinja variable) because
`config()` is evaluated at parse time (`execute=False`), so `run_query()` never fires there. The
subquery is evaluated by Redshift at runtime.

**Main query**:
```sql
with filtered as (
    select *
    from settlement_public.uni_tracking_spath_raw
    where pathTime > {{ source_cutoff }}    -- 30-min scan
),
ranked as (
    select *,
        row_number() over (
            partition by order_id, traceSeq
            order by pathTime desc
        ) as _rn
    from filtered
)
select * exclude(_rn) from ranked where _rn = 1
```

**Post-hooks** (3 steps):

`_ph1a` — Archive old spath events to hist_uts_2025_h2:
```sql
INSERT INTO {{ var('mart_schema') }}.hist_uni_tracking_spath_2025_h2
SELECT * FROM {{ this }}
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }})
  AND pathTime >= extract(epoch from '2025-07-01'::timestamp)
  AND pathTime < extract(epoch from '2026-01-01'::timestamp)
  AND order_id NOT IN (
      SELECT order_id FROM {{ var('mart_schema') }}.hist_uni_tracking_spath_2025_h2
      WHERE pathTime >= extract(epoch from '2025-07-01'::timestamp)
  )
```
The NOT IN guard for uts is scoped by pathTime range (more specific than order_id alone,
since one order has many spath events across multiple periods).

`_ph2` — Safety check (ARCHIVE_ROUTING_GAP_UTS):
```sql
INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at, notes)
SELECT DISTINCT m.order_id, 'ARCHIVE_ROUTING_GAP_UTS', ...
FROM {{ this }} m
WHERE m.pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }})
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_spath_2025_h2
                  WHERE order_id = m.order_id AND pathTime = m.pathTime)
  AND NOT EXISTS (SELECT 1 FROM order_tracking_exceptions
                  WHERE order_id = m.order_id AND exception_type = 'ARCHIVE_ROUTING_GAP_UTS'
                    AND resolved_at IS NULL)
```

`_ph3` — Pure time-based trim:
```sql
DELETE FROM {{ this }}
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }})
  AND order_id NOT IN (
      SELECT order_id FROM order_tracking_exceptions
      WHERE exception_type = 'ARCHIVE_ROUTING_GAP_UTS' AND resolved_at IS NULL
  )
```

---

## 5. dbt Models — Staging Layer

The staging models are NOT run by the main DAG. They exist for reference, alternative query
patterns, and testing. They read directly from the same `*_raw` tables.

### 5a. stg_uni_tracking_info.sql

**Source cutoff**: `MAX(update_time) - 1800` from `{{ this }}` (mart-relative, 30 min)

**Config**: `delete+insert`, `unique_key='order_id'`, `dist='order_id'`, `sort=['update_time', 'order_id']`

No `incremental_predicates`. The DELETE is `WHERE order_id IN (batch)` — no time window.
This was a deliberate fix: the earlier design used a 20-day window on the DELETE, which caused
stale duplicate rows for orders with lifecycle > 20 days. See `stg_uti_dedup_final.md`.

**Post-hook** (retention trim to 20 days):
```sql
DELETE FROM {{ this }}
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM {{ this }})
```
`1728000 seconds = 20 days`. This keeps staging compact at ~10–14M rows.

**Deduplication**: `row_number() OVER (PARTITION BY order_id ORDER BY update_time DESC, id DESC)`.
`id DESC` as tie-breaker provides a deterministic secondary sort for rows with equal `update_time`.

### 5b. stg_ecs_order_info.sql

**Source cutoff**: `MAX(add_time) - 1800` from `{{ this }}` (mart-relative, 30 min)

**Config**: `delete+insert`, `unique_key='order_id'`, `incremental_predicates` on add_time (2-hour
DELETE window), `dist='order_id'`, `sort=['add_time', 'order_id']`

No post-hook retention trim. `add_time` is static and ecs never grows unboundedly in staging.

**Deduplication**: `row_number() OVER (PARTITION BY order_id ORDER BY add_time DESC)`.

### 5c. stg_uni_tracking_spath.sql

**Source cutoff**: `MAX(pathTime) - 1800` from `{{ this }}` (mart-relative, 30 min)

**Config**: `delete+insert`, `unique_key=['order_id', 'traceSeq', 'pathTime']`, `incremental_predicates`
on pathTime (2-hour DELETE window), `dist='order_id'`, `sort='pathTime'`

**Deduplication**: `row_number() OVER (PARTITION BY order_id, traceSeq ORDER BY pathTime DESC)`.

---

## 6. Watermarks — Seeding and Structure

**File**: `seed_watermarks_to_now.py`

Watermarks are stored in S3 at:
```
s3://redshift-dw-qa-uniuni-com/watermarks/v2/{table_key}.json
```

Where `table_key` is built by `clean_table_name()`:
```python
def clean_table_name(table_name, target_connection):
    base   = table_name.replace(":", "_").replace(".", "_").lower()
    target = target_connection.replace(":", "_").replace(".", "_").lower()
    return f"{base}_{target}"
# e.g.: "kuaisong.ecs_order_info" + "redshift_default_direct"
#     → "kuaisong_ecs_order_info_redshift_default_direct"
```

A v2.0 watermark JSON structure:
```json
{
  "version": "2.0",
  "table_name": "kuaisong.ecs_order_info",
  "cdc_strategy": "hybrid",
  "mysql_state": {
    "last_timestamp": "2026-02-27T04:15:17+00:00",
    "last_id": null,
    "status": "success",
    "total_rows": 0,
    "last_session_rows": 0
  },
  "redshift_state": { ... },
  "metadata": {
    "manual_override": true,
    "manual_set_reason": "seeded_to_now_by_seed_watermarks_to_now.py"
  }
}
```

The only field used by the extraction framework as the lower bound is:
```
mysql_state.last_timestamp  (ISO 8601 UTC string)
```

The extraction framework converts this ISO string to a unix epoch integer for the SQL query:
```
'2026-02-27T04:15:17+00:00' → 1772165717
```

**First-run seeding procedure**:
```bash
# Dry-run first
python seed_watermarks_to_now.py

# Apply to S3
python seed_watermarks_to_now.py --apply
```

Requires `S3_ACCESS_KEY`, `S3_SECRET_KEY` in the environment or `.env` file.
Run ONCE before activating the DAG for the first time, or after any extended downtime
where you want to skip historical backfill and start fresh from "now".

---

## 7. Monitoring DAG

**File**: `airflow_poc/dags/order_tracking_daily_monitoring.py`

DAG ID: `order_tracking_daily_monitoring`
Schedule: `0 3 * * *` (3am UTC daily, 1 hour after vacuum)
`max_active_runs=1`, `catchup=False`
Default retries: 1, retry_delay: 5 minutes, execution_timeout: 30 minutes.

**Key constants**:
```python
MART_SCHEMA      = 'settlement_ods'
DQ_LOOKBACK_HOURS = 24
DQ_LOOKBACK_SECS  = 86400
EXTRACTION_BUFFER_SECS = 5 * 60   # 5 min (matches calc_window)
EXTRACTION_WINDOW_SECS = 2 * 3600  # 2h window

HIST_UTS_TABLES = [
    'settlement_ods.hist_uni_tracking_spath_2025_h2',
    'settlement_ods.hist_uni_tracking_spath_2026_h1',
    # Add 2026_h2 on 1 Jul 2026
]
```

**Redshift connection** (`get_conn(autocommit=False)`):
Uses same env var chain as `trim_raw_tables`: `REDSHIFT_QA_USER` → `REDSHIFT_USER` → `REDSHIFT_PRO_USER`.

**MySQL connection** (`get_mysql_conn()`):
Direct internal connection using `pymysql`. No SSH tunnel — runs server-side where
`MYSQL_SOURCE_HOST` (`us-west-2.ro.db.uniuni.com.internal`) is directly reachable.
Credentials: `DB_USER`, `DB_US_PROD_RO_PASSWORD`.

### Test 0: dq_extraction_count_check

```python
to_unix   = int(time.time()) - EXTRACTION_BUFFER_SECS   # now - 5min
from_unix = to_unix - EXTRACTION_WINDOW_SECS            # now - 2h5min

# MySQL
mysql_cur.execute(
    f"SELECT COUNT(*) AS cnt FROM {mysql_table} WHERE {ts_col} >= %s AND {ts_col} < %s",
    (from_unix, to_unix)
)

# Redshift raw
rs_cur.execute(
    f"SELECT COUNT(*) FROM {raw_table} WHERE {raw_ts} >= {from_unix} AND {raw_ts} < {to_unix}"
)

if mysql_cnt > rs_cnt:
    gaps.append(f"{name}: MySQL={mysql_cnt}, Redshift_raw={rs_cnt}, missing={mysql_cnt - rs_cnt}")

if gaps:
    raise ValueError(...)   # Does NOT write to exceptions — this is an infra alert
```

Raises immediately on failure. Does NOT write to `order_tracking_exceptions`.

### Test 1: dq_uti_uts_alignment

```sql
INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at, notes)
SELECT uti.order_id, 'UTI_UTS_TIME_MISMATCH', CURRENT_TIMESTAMP,
       'update_time=' || uti.update_time::varchar
         || ' max_pathTime=' || MAX(uts.pathTime)::varchar
         || ' diff=' || (uti.update_time - MAX(uts.pathTime))::varchar || 's'
FROM mart_uni_tracking_info uti
JOIN mart_uni_tracking_spath uts ON uti.order_id = uts.order_id
WHERE uti.update_time > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint - 86400
GROUP BY uti.order_id, uti.update_time
HAVING uti.update_time <> MAX(uts.pathTime)
  AND NOT EXISTS (
      SELECT 1 FROM order_tracking_exceptions ex
      WHERE ex.order_id = uti.order_id
        AND ex.exception_type = 'UTI_UTS_TIME_MISMATCH'
        AND ex.resolved_at IS NULL
  )
```

Writes new mismatches to exceptions. `NOT EXISTS` guard prevents duplicate entries.
`rowcount` returned as XCom `key='dq_test1_count'`.

### Test 2: dq_missing_spath

Checks mart_uts + all tables in `HIST_UTS_TABLES`:

```sql
INSERT INTO order_tracking_exceptions (order_id, exception_type, ...)
SELECT uti.order_id, 'ORDER_MISSING_SPATH', ...
FROM mart_uni_tracking_info uti
WHERE uti.update_time > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint - 86400
  AND NOT EXISTS (SELECT 1 FROM mart_uni_tracking_spath uts WHERE uts.order_id = uti.order_id)
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_spath_2025_h2 h WHERE h.order_id = uti.order_id)
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_spath_2026_h1 h WHERE h.order_id = uti.order_id)
  AND NOT EXISTS (SELECT 1 FROM order_tracking_exceptions ex WHERE ...)
```

The `hist_not_exists` clauses are generated dynamically from `HIST_UTS_TABLES`:
```python
hist_not_exists = '\n'.join([
    f"AND NOT EXISTS (SELECT 1 FROM {tbl} h WHERE h.order_id = uti.order_id)"
    for tbl in HIST_UTS_TABLES
])
```

### Test 3: dq_reactivated_missing_ecs

```sql
INSERT INTO order_tracking_exceptions (order_id, exception_type, ...)
SELECT uti.order_id, 'REACTIVATED_ORDER_MISSING_ECS', ...
FROM mart_uni_tracking_info uti
LEFT JOIN mart_ecs_order_info ecs ON uti.order_id = ecs.order_id
WHERE uti.update_time > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint - 86400
  AND ecs.order_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM order_tracking_exceptions ex WHERE ...)
```

### check_unresolved_exceptions (consolidation)

TriggerRule: `ALL_DONE` — runs even if a DQ task fails.

```sql
SELECT exception_type, COUNT(*) AS cnt
FROM order_tracking_exceptions
WHERE resolved_at IS NULL
GROUP BY exception_type
ORDER BY cnt DESC
```

Raises if `total_unresolved > 0`, triggering the email alert.

### Task dependencies

```python
dq_group >> task_check_exceptions
```

All 4 DQ tasks (`dq_extraction_count_check`, `dq_uti_uts_alignment`, `dq_missing_spath`,
`dq_reactivated_missing_ecs`) run in parallel within `TaskGroup("dq_checks")`. The
`check_unresolved_exceptions` task waits for all of them before running.

---

## 8. Vacuum DAG

**File**: `airflow_poc/dags/order_tracking_vacuum.py`

DAG ID: `order_tracking_vacuum`
Schedule: `0 2 * * *` (2am UTC daily, 1 hour before monitoring)
`max_active_runs=1`, `catchup=False`
execution_timeout: 2 hours (VACUUM on large tables can take time).

All VACUUM functions follow the same pattern:

```python
def vacuum_mart_uti(**context):
    conn = get_conn(autocommit=True)
    cur = conn.cursor()
    cur.execute(f"VACUUM DELETE ONLY {MART_UTI}")
    cur.close()
    conn.close()
```

`conn.autocommit = True` is required — Redshift VACUUM cannot run inside a transaction block.

Four functions:
- `vacuum_mart_uti`: `VACUUM DELETE ONLY settlement_ods.mart_uni_tracking_info`
- `vacuum_mart_uts`: `VACUUM DELETE ONLY settlement_ods.mart_uni_tracking_spath`
- `vacuum_mart_ecs_delete`: `VACUUM DELETE ONLY settlement_ods.mart_ecs_order_info`
- `vacuum_mart_ecs_sort`: `VACUUM SORT ONLY settlement_ods.mart_ecs_order_info`

Task dependencies within the `vacuum` TaskGroup:
```python
task_vacuum_ecs_delete >> task_vacuum_ecs_sort
```
`ecs_delete` runs before `ecs_sort` — more efficient order for Redshift's internal processing.

`vacuum_mart_uti`, `vacuum_mart_uts`, and `vacuum_mart_ecs_delete/sort` run independently
(uti and uts run in parallel; ecs has its own sequential ordering).

Why not use `PostgresHook`: Airflow's `PostgresHook` has IAM auth enabled on the
`redshift_default` connection, which triggers a boto3 RDS token fetch that fails with
`NoRegionError` in this environment. Raw `psycopg2` with env var credentials is used instead.

---

## 9. SSH Tunnel Handling for dbt

The DAG supports two modes, controlled by `DBT_USE_SSH_TUNNEL` environment variable (default: `'true'`).

**Tunnel mode** (`DBT_USE_SSH_TUNNEL=true`) — for local Docker testing:

```bash
# Copy SSH key and fix permissions (Docker Windows mounts have wrong perms)
SSH_KEY_TEMP=$(mktemp)
cp "{SSH_KEY_PATH}" "$SSH_KEY_TEMP"
chmod 600 "$SSH_KEY_TEMP"

# Start tunnel in background
ssh -N -L 15439:{REDSHIFT_HOST}:5439 \
    -o StrictHostKeyChecking=no \
    -o ServerAliveInterval=60 \
    -o ExitOnForwardFailure=yes \
    -i "$SSH_KEY_TEMP" \
    {SSH_BASTION_USER}@{SSH_BASTION_HOST} 2>&1 &
SSH_PID=$!
sleep 2

# Verify tunnel
nc -z localhost 15439 || exit 1
```

After the dbt command, the cleanup block kills the tunnel and removes the temp key file.

**Direct mode** (`DBT_USE_SSH_TUNNEL=false`) — for server deployment:

Sets `DBT_REDSHIFT_HOST` and `DBT_REDSHIFT_PORT` in the environment and skips all tunnel
setup. `dbt` connects directly to the Redshift endpoint.

The `load_env_vars(path)` function loads the `.env` file using `python-dotenv` if available,
falling back to manual line-by-line parsing that handles CRLF, quotes, and inline comments.
The loaded values are merged into `os.environ.copy()` (preserving `PATH`, `HOME`, etc.) to
produce `dbt_env_vars`, which is passed as `env=dbt_env_vars` to all three dbt BashOperators.

SSH configuration constants (overridable via env vars):
```python
SSH_BASTION_HOST = os.environ.get('REDSHIFT_SSH_BASTION_HOST', '35.82.216.244')
SSH_BASTION_USER = os.environ.get('REDSHIFT_SSH_BASTION_USER', 'jasleentung')
SSH_KEY_PATH     = os.environ.get('REDSHIFT_SSH_KEY_PATH', '/Users/Jasleen Tung/Downloads/...')
DBT_LOCAL_PORT   = '15439'  # Fixed local port for dbt SSH tunnel
```

---

## 10. Environment Variables Reference

| Variable | Used by | Description |
|----------|---------|-------------|
| `SYNC_TOOL_PATH` | DAG | Override path to repo root |
| `DBT_PROJECT_PATH` | DAG | Override path to dbt project |
| `DBT_VENV_PATH` | DAG | Path to virtualenv with dbt installed |
| `SYNC_PIPELINE_NAME` | DAG | Pipeline YAML name (default: `order_tracking_hybrid_dbt_pipeline`) |
| `DBT_USE_SSH_TUNNEL` | DAG | `'true'` for tunnel mode, `'false'` for direct mode |
| `REDSHIFT_HOST` | trim_raw, monitoring, vacuum | Redshift cluster endpoint |
| `REDSHIFT_PORT` | trim_raw, monitoring, vacuum | Redshift port (default: 5439) |
| `REDSHIFT_QA_USER` | trim_raw, monitoring, vacuum | Redshift username |
| `REDSHIFT_QA_PASSWORD` | trim_raw, monitoring, vacuum | Redshift password |
| `REDSHIFT_USER` | fallback | Alternative username env var |
| `REDSHIFT_PASSWORD` | fallback | Alternative password env var |
| `REDSHIFT_SSH_BASTION_HOST` | dbt tunnel | SSH bastion IP |
| `REDSHIFT_SSH_BASTION_USER` | dbt tunnel | SSH bastion username |
| `REDSHIFT_SSH_KEY_PATH` | dbt tunnel | Path to SSH private key file |
| `MYSQL_SOURCE_HOST` | monitoring | MySQL internal hostname |
| `MYSQL_SOURCE_PORT` | monitoring | MySQL port (default: 3306) |
| `DB_USER` | monitoring | MySQL username |
| `DB_US_PROD_RO_PASSWORD` | monitoring | MySQL password |
| `S3_ACCESS_KEY` | seed_watermarks | AWS access key for watermark writes |
| `S3_SECRET_KEY` | seed_watermarks | AWS secret key for watermark writes |
| `S3_REGION` | seed_watermarks | AWS region (default: us-west-2) |

---

## 11. Operational Tasks — Periodic Maintenance

### Every 6 months (1 January and 1 July)

**Create new hist tables** (DDL, run manually):
```sql
CREATE TABLE settlement_ods.hist_uni_tracking_info_2026_h1 (LIKE settlement_ods.mart_uni_tracking_info);
CREATE TABLE settlement_ods.hist_ecs_order_info_2026_h1    (LIKE settlement_ods.mart_ecs_order_info);
CREATE TABLE settlement_ods.hist_uni_tracking_spath_2026_h1 (LIKE settlement_ods.mart_uni_tracking_spath);
```

**Update dbt post-hooks** in the three mart SQL files:
- `mart_uni_tracking_info.sql`: update `_ph1` (stale hist cleanup table name), add `_ph2b` for
  the new period.
- `mart_ecs_order_info.sql`: add `_ph1b` for the new ecs hist period.
- `mart_uni_tracking_spath.sql`: add `_ph1b` for the new uts hist period.

**Update monitoring DAG**:
In `order_tracking_daily_monitoring.py`, add the new hist_uts table to `HIST_UTS_TABLES`:
```python
HIST_UTS_TABLES = [
    'settlement_ods.hist_uni_tracking_spath_2025_h2',
    'settlement_ods.hist_uni_tracking_spath_2026_h1',
    'settlement_ods.hist_uni_tracking_spath_2026_h2',  # add on 1 Jul 2026
]
```

### On first deployment or after extended downtime

Seed watermarks to avoid cold-start crawl:
```bash
cd /path/to/s3-redshift-backup-tool
python seed_watermarks_to_now.py        # dry-run first
python seed_watermarks_to_now.py --apply
```

### Manual exception resolution

When `check_unresolved_exceptions` alerts:

**REACTIVATED_ORDER_MISSING_ECS** (most common):
```sql
-- 1. Find order in hist_ecs
SELECT * FROM settlement_ods.hist_ecs_order_info_2025_h2 WHERE order_id = 'X';

-- 2. Restore to mart_ecs
INSERT INTO settlement_ods.mart_ecs_order_info
SELECT * FROM settlement_ods.hist_ecs_order_info_2025_h2 WHERE order_id = 'X';

-- 3. Delete stale hist entry
DELETE FROM settlement_ods.hist_ecs_order_info_2025_h2 WHERE order_id = 'X';

-- 4. Mark resolved
UPDATE settlement_ods.order_tracking_exceptions
SET resolved_at = CURRENT_TIMESTAMP, resolution_notes = 'Restored from hist_ecs'
WHERE order_id = 'X' AND exception_type = 'REACTIVATED_ORDER_MISSING_ECS' AND resolved_at IS NULL;
```

**ARCHIVE_ROUTING_GAP** or **ARCHIVE_ROUTING_GAP_UTS**:
Means a row's timestamp falls outside all defined hist period windows. Add the missing period
INSERT block to the relevant mart model's post-hooks, then mark the exception resolved.
```sql
UPDATE settlement_ods.order_tracking_exceptions
SET resolved_at = CURRENT_TIMESTAMP, resolution_notes = 'Added missing period, re-archived'
WHERE order_id = 'X' AND exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL;
```

**UTI_UTS_TIME_MISMATCH**:
Investigate the specific order_id. Check extraction logs for the cycle when the mismatch
appeared. If the mismatch was caused by a transient extraction gap that self-healed, mark
resolved. If it persists, investigate whether uti or uts has missing data.

---

*For the logical reasoning behind each design decision, see `logic_walkthrough_v2.md`.*
