# Order Tracking Pipeline — Expert Reference

This file is Claude's working knowledge base for the order_tracking Airflow + dbt pipeline.
Read this at the start of any session involving this pipeline before giving advice.

---

## 1. What the Pipeline Does

MySQL source tables (3) → extraction to Redshift raw tables → dbt mart models (3) → hist archive tables (6+ growing).

**Source → Raw (15-min Airflow DAG, extraction layer):**
- `settlement_public.uni_tracking_info_raw` — one row per order, latest tracking state. **Append-only landing zone — no retention trim. Must add raw table cleanup task.**
- `settlement_public.ecs_order_info_raw` — one row per order, order creation metadata. Same note.
- `settlement_public.uni_tracking_spath_raw` — many rows per order, every spath scan event. Same note.

**Raw → Mart (same 15-min DAG, dbt layer):**
- `settlement_ods.mart_uni_tracking_info` (mart_uti) — active orders, 6-month rolling window on `update_time`
- `settlement_ods.mart_ecs_order_info` (mart_ecs) — active orders, order_id-driven retention (no time cutoff)
- `settlement_ods.mart_uni_tracking_spath` (mart_uts) — active spath events, 6-month rolling window on `pathTime`

**Archive (written by mart post_hooks):**
- `settlement_ods.hist_uni_tracking_info_YYYY_HX`
- `settlement_ods.hist_ecs_order_info_YYYY_HX`
- `settlement_ods.hist_uni_tracking_spath_YYYY_HX`

**Exceptions (written by mart post_hooks + daily monitoring):**
- `settlement_ods.order_tracking_exceptions`

---

## 2. DAG Architecture

### 15-min DAG: `order_tracking_hybrid_dbt`
File: `airflow_poc/dags/order_tracking_hybrid_dbt_dag.py`

```
check_drift >> calc_window >> extraction_group >> validate >> dbt_mart_uti
                                                              dbt_mart_uti >> [dbt_mart_ecs, dbt_mart_uts]
                                                              [dbt_mart_ecs, dbt_mart_uts] >> dbt_test >> summary
```

Key settings:
- `schedule_interval='*/15 * * * *'`
- `max_active_runs=1` — CRITICAL: prevents two cycles from running concurrently. Without this, a slow cycle overlaps the next and mart reads mid-write watermarks.
- `catchup=False`
- `execution_timeout=timedelta(minutes=60)` in `default_args` (per-task overrides exist)
- dbt tasks each have `execution_timeout=timedelta(minutes=10)` for mart, `timedelta(minutes=5)` for test

**Why no `dbt_staging` task:** Raw tables are already well-structured. The mart models read directly from `*_raw` tables. Running staging would add 3–4 min of redundant processing per 15-min cycle. Staging models still exist in the project for historical reasons but are not run in the DAG.

**`dbt_test` scope:** `dbt test --select mart --store-failures` — scoped to mart only. Running `dbt test` without `--select mart` would execute staging relationship tests that would fail for new orders not yet in the staging layer.

### Daily monitoring DAG: `order_tracking_daily_monitoring`
File: `airflow_poc/dags/order_tracking_daily_monitoring.py`

- Schedule: `0 2 * * *` (2am UTC)
- `max_active_runs=1`, `catchup=False`
- Uses `PostgresHook(postgres_conn_id='redshift_default')` — must be configured in Airflow UI
- VACUUM connections require `conn.autocommit = True` — VACUUM cannot run inside a transaction

DQ tests (parallel):
1. `dq_uti_uts_alignment` — orders in mart_uts with no matching mart_uti row (last 24h)
2. `dq_missing_spath` — orders in mart_uti with no spath events in mart_uts OR any hist_uts table
3. `dq_reactivated_missing_ecs` — recently reactivated orders missing from mart_ecs

VACUUM tasks (parallel with DQ):
- `vacuum_mart_uti` — daily VACUUM DELETE ONLY
- `vacuum_mart_uts` — daily VACUUM DELETE ONLY
- `vacuum_mart_ecs_delete` — Sundays only (`logical_date.weekday() != 6` → skip)
- `vacuum_mart_ecs_sort` — conditional: only runs if `svv_table_info.unsorted > 15%` (expected every 65–130 days)

---

## 3. dbt Model Design Decisions

### mart_uni_tracking_info (mart_uti)

- **Strategy:** `delete+insert`, `unique_key='order_id'`
- **No `incremental_predicates`:** DELETE is `WHERE order_id IN (batch)`. Time-bounded predicates would miss old rows for reactivated long-lifecycle orders. Without predicates, DELETE is always correct regardless of order age.
- **Source cutoff:** `max(update_time) - 1800` (30 min). Matches 15-min extraction window + retry buffer.
- **Tie-break:** `order by update_time desc, id desc` in ranked CTE — `id` breaks ties when two source rows share the same `update_time`.
- **Retention:** 6-month rolling window = 15,552,000 seconds on `update_time`.
- **DISTKEY:** `order_id` — 3-way JOIN co-location with mart_ecs and mart_uts is always local.
- **SORTKEY:** `(update_time, order_id)` — zone maps skip all blocks before the query date window.

Post-hook order (critical — never reorder):
1. Clean stale hist_uti entry for reactivated orders (DELETE from latest hist table) — must run BEFORE archive
2a. Archive aged-out rows to `hist_*_2025_h2`
2b. Archive to `hist_*_2026_h1` (add when data starts aging into that window ~Jul 2026)
3. Safety check → write ARCHIVE_ROUTING_GAP to exceptions for any aged-out row not in any hist table
4. Trim — excludes rows with open ARCHIVE_ROUTING_GAP exceptions

### mart_ecs_order_info (mart_ecs)

- **Strategy:** `delete+insert`, `unique_key='order_id'`
- **`incremental_predicates` on `add_time` (2-hour window):** `add_time` is set at order creation and never changes. New batches only contain NEW orders (add_time always within the last extraction window). The 2-hour window handles extraction retries safely — old orders never appear in new batches.
- **Why order_id-driven retention (NOT time-based):** mart_ecs has ONE row per order. Time-based trim on `add_time` would drop long-lifecycle orders (e.g. created Jan 2024, still active Aug 2025) when their add_time ages past 6 months, breaking the 3-way JOIN. Order_id-driven: only trim when mart_uti has already trimmed the order.
- **Post-hook uses `{{ ref('mart_uni_tracking_info') }}`:** Creates the dbt compile-time dependency ensuring mart_ecs runs after mart_uti in the dependency graph.
- **`DELETE USING LEFT JOIN` anti-join (NOT `NOT IN`):** mart_ecs at 90M+ rows may have NULL order_ids. `WHERE order_id NOT IN (subquery)` returns zero rows if subquery has any NULL — silent data retention failure. LEFT JOIN anti-join is NULL-safe.
- **SORTKEY:** `(partner_id, add_time, order_id)` — `partner_id` is the primary query filter, cuts to ~0.3% of rows.

Post-hook order:
1a. Archive inactive orders (absent from mart_uti via LEFT JOIN anti-join) to `hist_ecs_*_2025_h2`
1b. Archive to `hist_ecs_*_2026_h1` (add when needed)
2. Trim — `DELETE USING` anti-join (NULL-safe)

### mart_uni_tracking_spath (mart_uts)

- **Strategy:** `delete+insert`, `unique_key=['order_id', 'traceSeq', 'pathTime']`
- **`incremental_predicates` on `pathTime` (2-hour window):** New spath events always have a recent pathTime. The 2-hour window handles retries without touching historical events (different pathTime = never matched for deletion).
- **Why pure time-based retention (NOT order_id-driven):** mart_uts holds MANY rows per order (every scan event over 6 months). Order_id-driven would keep ALL spath events for every active order indefinitely — unbounded growth at 195M rows/month. Pure 6-month pathTime cap keeps mart_uts at ~1.17B rows permanently. Old events for active orders live in hist_uts.
- **NOT IN guard in archive includes pathTime range filter:** `WHERE pathTime >= extract(epoch from '2025-07-01'::timestamp)` — filters by (order_id, pathTime range) not just order_id, since many spath events share order_id.
- **SORTKEY:** `(pathTime, order_id)` — zone maps skip all blocks before the query date.

Post-hook order:
1a. Archive spath events older than 6 months to `hist_uts_*_2025_h2`
1b. Archive to `hist_uts_*_2026_h1` (add when needed)
2. Safety check → write ARCHIVE_ROUTING_GAP_UTS to exceptions (checks order_id + pathTime pair, not just order_id)
3. Trim — pure time-based, excludes ARCHIVE_ROUTING_GAP_UTS exceptions

---

## 4. Retention & Archiving

### 6-month constant
```
15,552,000 seconds = 180 days
```
Used in all three mart models as: `max(ts_col) - 15552000`.

### hist table naming convention
```
hist_{source_table}_{YYYY}_{h1|h2}
  h1 = Jan 1 – Jul 1
  h2 = Jul 1 – Jan 1
```
Example: `hist_uni_tracking_info_2025_h2` = Jul 2025 – Jan 2026.

### Period rotation (1 Jan and 1 Jul every year)

When data starts aging into a new period (e.g. around Jul 2026 for 2026_h2), do ALL of:
1. Create the 3 new hist tables in Redshift (CTAS from existing stg_* or existing hist table, `WHERE 1=0`)
2. Add new period entry in `dbt_project.yml` `hist_periods` var (uncomment the template entry)
3. Add STEP 2b INSERT block to `mart_uni_tracking_info.sql` post_hooks
4. Update STEP 1 in mart_uti to DELETE from the NEW latest hist_uti table (the one just created)
5. Add STEP 1b INSERT block to `mart_ecs_order_info.sql` post_hooks
6. Add STEP 1b INSERT block to `mart_uni_tracking_spath.sql` post_hooks
7. Add new NOT EXISTS check in the STEP 3 safety checks (mart_uti and mart_uts)
8. Update `HIST_UTS_TABLES` list in `order_tracking_daily_monitoring.py`

**Current state (Feb 2026):**
- Active period: `2025_h2` (Jul 2025 – Jan 2026) — data from Jul–Dec 2025 archives here
- Next period: `2026_h1` (Jan 2026 – Jul 2026) — add ~Jul 2026 when data starts aging in

### NOT IN NULL trap
Never use `WHERE order_id NOT IN (SELECT order_id FROM large_table)` in Redshift at scale.
If the subquery returns ANY NULL, the outer `NOT IN` returns zero rows (SQL NULL semantics).
Use `DELETE USING LEFT JOIN ... WHERE uti.order_id IS NULL` instead (mart_ecs trim pattern).

### Archive idempotency (NOT EXISTS guard)
All archive INSERTs use `AND order_id NOT IN (SELECT order_id FROM hist_table)`.
This makes each archive INSERT safe to retry after a partial failure. The guard ensures the same row is never double-written to hist.

---

## 5. Redshift Specifics

### VACUUM requirements
- VACUUM **cannot run inside a Redshift transaction**
- Always set `conn.autocommit = True` before executing any VACUUM statement
- Forgetting this causes: `ERROR: VACUUM is not allowed in a transaction block`

### psycopg2 single-statement constraint
- `psycopg2.execute()` can only run ONE SQL statement per call
- dbt post_hooks are executed one string at a time via psycopg2 — this is why each post_hook step is a separate string in the list
- The `archive_to_hist` macro uses `{% do run_query() %}` in a loop — it's only for `dbt run-operation` ad-hoc use, NOT for inclusion in post_hooks

### `dbt ref()` in post_hooks
`{{ ref('mart_uni_tracking_info') }}` in mart_ecs post_hooks serves two purposes:
1. At compile time: creates the dbt DAG dependency (mart_ecs depends on mart_uti)
2. At runtime: resolves to the full schema-qualified table name

### DISTKEY co-location
All three mart tables use `DISTKEY(order_id)`. The 3-way JOIN:
```sql
mart_uti JOIN mart_ecs ON order_id JOIN mart_uts ON order_id
```
is always local — rows with the same order_id land on the same Redshift node across all three tables.

### Zone map skipping
- mart_uti SORTKEY `(update_time, order_id)`: queries with `WHERE update_time >= X` skip all blocks before X
- mart_uts SORTKEY `(pathTime, order_id)`: queries with `WHERE pathTime >= X` skip all blocks before X
- mart_ecs SORTKEY `(partner_id, add_time, order_id)`: `WHERE partner_id = X` cuts to ~0.3% of the table

### Steady-state table sizes (approximate)
- mart_uti: ~30M rows (one per active order in rolling 6-month window)
- mart_ecs: matches mart_uti row count (one per active order)
- mart_uts: ~1.17B rows (195M events/month × 6 months)

---

## 6. Exceptions Table

`settlement_ods.order_tracking_exceptions`

| Column | Description |
|--------|-------------|
| `order_id` | The affected order |
| `exception_type` | See types below |
| `detected_at` | Timestamp when exception was first written |
| `resolved_at` | NULL = open/active; populated when manually resolved |
| `notes` | Human-readable description |

**Exception types:**

| Type | Source | Meaning | Action |
|------|--------|---------|--------|
| `ARCHIVE_ROUTING_GAP` | mart_uti post_hook STEP 3 | order's `update_time` aged out but didn't match any hist period. Row is protected from trim. | Add missing period INSERT to mart_uti post_hooks, then set `resolved_at` |
| `ARCHIVE_ROUTING_GAP_UTS` | mart_uts post_hook STEP 2 | spath event's `pathTime` aged out but didn't match any hist period. Row protected. | Same: add missing period to mart_uts, resolve |
| (future DQ types) | daily monitoring DAG | Data quality issues detected by DQ tests | Investigate and resolve |

**Key rule:** Rows with open exceptions (`resolved_at IS NULL`) are NEVER trimmed from mart tables. This prevents silent data loss when a config error (missing period) is discovered.

---

## 7. File Map

```
airflow_poc/
  dags/
    order_tracking_hybrid_dbt_dag.py      # 15-min extraction + mart DAG
    order_tracking_daily_monitoring.py    # daily DQ + VACUUM DAG
  dbt_projects/order_tracking/
    dbt_project.yml                       # vars: mart_schema, hist_periods
    profiles.yml                          # QA: settlement_ods / SSH tunnel
    models/
      staging/
        stg_uni_tracking_info.sql         # dedup fix applied (id desc tie-break)
        stg_ecs_order_info.sql
        stg_uni_tracking_spath.sql
      mart/
        mart_uni_tracking_info.sql        # 4-step post_hooks, no incremental_predicates
        mart_ecs_order_info.sql           # 2-step post_hooks, LEFT JOIN anti-join trim
        mart_uni_tracking_spath.sql       # 3-step post_hooks, pure time-based retention
        schema.yml                        # DQ tests: unique, not_null, relationship(warn)
    macros/
      archive_to_hist.sql                 # reference macro for dbt run-operation ONLY
```

---

## 8. Known Gotchas & Decisions Made

| Gotcha | Decision |
|--------|----------|
| mart_uti needs no `incremental_predicates` | DELETE by `order_id IN (batch)` is always correct; time predicates miss reactivated orders |
| mart_ecs trim uses DELETE USING, not NOT IN | NOT IN NULL trap at 90M+ rows; LEFT JOIN anti-join is NULL-safe |
| hist tables must exist before first dbt run | Pre-flight: CTAS from stg_* tables `WHERE 1=0` |
| dbt test must be scoped `--select mart` | Staging relationship tests fail after removing dbt_staging task from DAG |
| VACUUM needs `autocommit=True` | Cannot run VACUUM in a transaction |
| psycopg2 runs one statement per execute() | Each post_hook step is a separate list entry |
| mart_ecs cannot use time-based retention | Long-lifecycle orders (created 18 months ago, still active) would be dropped |
| mart_uts cannot use order_id-driven retention | Would keep ALL spath events for every active order → unbounded growth |
| `max_active_runs=1` on 15-min DAG | Prevents concurrent cycles from reading mid-write watermarks |
| 2026_h1 hist tables not needed yet (Feb 2026) | Removed STEP 2b/1b blocks; uncomment `2026_h1` in `dbt_project.yml` ~Jul 2026 |
| Step 1 in mart_uti must target LATEST hist table | Reactivated orders' stale entry is always in the most recent hist table (orders never dormant >~1 year) |

---

## 9. QA Testing Checklist

Pre-flight DDL (run once before first `dbt run`):
```sql
-- 1. Exceptions table
CREATE TABLE IF NOT EXISTS settlement_ods.order_tracking_exceptions (
    order_id VARCHAR(64),
    exception_type VARCHAR(64),
    detected_at TIMESTAMP,
    resolved_at TIMESTAMP,
    notes VARCHAR(512)
) DISTKEY(order_id) SORTKEY(exception_type, detected_at);

-- 2. 2025_h2 hist tables (CTAS from stg_* — same schema, zero rows)
CREATE TABLE IF NOT EXISTS settlement_ods.hist_uni_tracking_info_2025_h2
    DISTKEY(order_id) SORTKEY(update_time, order_id)
    AS SELECT * FROM settlement_ods.stg_uni_tracking_info WHERE 1=0;

CREATE TABLE IF NOT EXISTS settlement_ods.hist_ecs_order_info_2025_h2
    DISTKEY(order_id) SORTKEY(partner_id, add_time, order_id)
    AS SELECT * FROM settlement_ods.stg_ecs_order_info WHERE 1=0;

CREATE TABLE IF NOT EXISTS settlement_ods.hist_uni_tracking_spath_2025_h2
    DISTKEY(order_id) SORTKEY(pathTime, order_id)
    AS SELECT * FROM settlement_ods.stg_uni_tracking_spath WHERE 1=0;
```

Verification after first `dbt run --select mart`:
```sql
-- Row counts
SELECT COUNT(*) FROM settlement_ods.mart_uni_tracking_info;
SELECT COUNT(*) FROM settlement_ods.mart_ecs_order_info;
SELECT COUNT(*) FROM settlement_ods.mart_uni_tracking_spath;

-- Any exceptions?
SELECT * FROM settlement_ods.order_tracking_exceptions LIMIT 20;

-- hist tables populated? (expect rows if source data is >6 months old)
SELECT COUNT(*) FROM settlement_ods.hist_uni_tracking_info_2025_h2;

-- 3-way JOIN intact?
SELECT COUNT(*)
FROM settlement_ods.mart_uni_tracking_info uti
LEFT JOIN settlement_ods.mart_ecs_order_info ecs ON uti.order_id = ecs.order_id
WHERE ecs.order_id IS NULL;  -- expect 0
```
