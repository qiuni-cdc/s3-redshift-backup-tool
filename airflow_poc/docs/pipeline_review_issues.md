# Pipeline Review — Issues & Fixes

Cross-check of pipeline correctness, best practices, and architecture gaps.
Date reviewed: 2026-02-27

---

## Issue 1 — Tie-break missing in `mart_uni_tracking_info` · **Medium**

**File:** `dbt_projects/order_tracking/models/mart/mart_uni_tracking_info.sql`

**Problem:**
The ranked CTE uses `ORDER BY update_time DESC` with no secondary sort column.
When two source rows share identical `order_id + update_time`, Redshift can return either row — result is non-deterministic and can differ between runs.
The staging model (`stg_uni_tracking_info.sql`) already has this fix; the mart model does not.

**Fix:**
```sql
-- Before
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY update_time DESC)

-- After
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY update_time DESC, id DESC)
```

**Status:** ✅ Fixed — `mart_uni_tracking_info.sql`

---

## Issue 2 — `ecs_order_info_raw` is never trimmed · **Medium**

**File:** `dags/order_tracking_hybrid_dbt_dag.py` → `trim_raw_tables()`

**Problem:**
`uni_tracking_info_raw` and `uni_tracking_spath_raw` are trimmed to 24 hours after each cycle.
`ecs_order_info_raw` has no trim — it accumulates every extraction batch indefinitely.
In steady state (~500K rows/day), this grows to tens of millions of rows over months for no purpose after mart_ecs has processed each batch.

**Fix:**
Add ecs raw to the trim loop with a 48h window (double the standard 24h for extra safety margin):
```python
# In trim_raw_tables(), add to tables list:
('settlement_public.ecs_order_info_raw', 'add_time'),
```
And change the cutoff from 24h to 48h for ecs only, or use a shared 48h cutoff for all three.

**Status:** ✅ Fixed — `order_tracking_hybrid_dbt_dag.py`

---

## Issue 3 — `HIST_UTS_TABLES` hardcoded in monitoring DAG · **Low / Operational risk**

**File:** `dags/order_tracking_daily_monitoring.py`

**Problem:**
Test 2 (ORDER_MISSING_SPATH) iterates over a hardcoded list of hist_uts tables to confirm spath events exist for every active order. A new period is added every 6 months (1 Jan, 1 Jul). If the list is not updated after period rotation, orders archived to the new table will be false-positived as missing — generating noise alerts every day until fixed.

**Current code:**
```python
HIST_UTS_TABLES = [
    f'{MART_SCHEMA}.hist_uni_tracking_spath_2025_h2',
    f'{MART_SCHEMA}.hist_uni_tracking_spath_2026_h1',
]
```

**Fix:**
Query `pg_tables` at runtime so the list is always in sync with what exists in Redshift:
```python
def _get_hist_uts_tables(cur):
    cur.execute("""
        SELECT schemaname || '.' || tablename
        FROM pg_tables
        WHERE schemaname = %s AND tablename LIKE 'hist_uni_tracking_spath_%%'
        ORDER BY tablename
    """, (MART_SCHEMA,))
    return [row[0] for row in cur.fetchall()]
```

**Status:** ✅ Fixed — `order_tracking_daily_monitoring.py`

---

## Issue 4 — VACUUM SORT runs unconditionally · **Low / Known constraint**

**File:** `dags/order_tracking_vacuum.py` → `vacuum_mart_ecs_sort()`

**Problem:**
`VACUUM SORT ONLY` runs every day regardless of how unsorted the table is.
At ~200K new orders/day with random `partner_id`, the 15% unsorted threshold is reached roughly every 65–130 days, not daily. Running VACUUM SORT on an already-sorted table wastes ~10–15 min of cluster time per day.

**Root cause of workaround:**
`sett_ddl_owner` lacks SELECT on `svv_table_info` (requires superuser). The conditional check was removed to avoid a permission error.

**Fix (requires DBA action):**
Ask DBA to grant `SELECT ON svv_table_info TO sett_ddl_owner`, then re-enable the conditional:
```python
cur.execute("""
    SELECT unsorted FROM svv_table_info
    WHERE "table" = 'mart_ecs_order_info'
""")
unsorted_pct = cur.fetchone()[0] or 0
if unsorted_pct > 15:
    cur.execute(f"VACUUM SORT ONLY {MART_ECS}")
    log.info(f"VACUUM SORT triggered at {unsorted_pct:.1f}% unsorted")
else:
    log.info(f"Skipped VACUUM SORT — unsorted at {unsorted_pct:.1f}%")
```

**Status:** ☐ Open — blocked on DBA permission grant

---

## Issue 5 — ECS post-creation field changes not captured · **Low / Known gap**

**File:** `config/pipelines/order_tracking_hybrid_dbt_pipeline.yml`, `src/core/cdc_strategy_engine.py`

**Problem:**
`ecs_order_info` has no `updated_at` column. The CDC strategy watches only `add_time` (order creation timestamp, immutable). Any ECS field that changes after order creation (partner re-assignment, store correction, etc.) is not detected and not re-extracted.

**Impact:**
Low — `partner_id` (the primary query filter) is set at creation and rarely changes.

**Fix (if ever needed):**
Option A — Full daily ECS refresh for orders active in `mart_uti` (heavy, safe).
Option B — Request a MySQL schema change to add `updated_at` to `ecs_order_info` and use it as the CDC timestamp.

**Monitoring:**
Run `compare_pipelines.py --table ecs` monthly to check mismatch rate between legacy and new pipeline. Escalate if rate is high.

**Status:** ☐ Open — monitor only, no code change planned

---

## Summary

| # | Issue | Severity | Fix effort | Status |
|---|---|---|---|---|
| 1 | Tie-break missing in mart_uti ranked CTE | Medium | 1 line in dbt SQL | ✅ Fixed |
| 2 | ecs_order_info_raw never trimmed | Medium | 1 line in trim_raw_tables | ✅ Fixed |
| 3 | HIST_UTS_TABLES hardcoded in monitoring | Low | Replace hardcode with pg_tables query | ✅ Fixed |
| 4 | VACUUM SORT runs unconditionally | Low | DBA permission grant required | ☐ Blocked |
| 5 | ECS field change gap | Low | Monitor only for now | ☐ Watch |
