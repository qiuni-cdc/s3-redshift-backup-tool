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

**Blocker:** Fix was applied with `id desc` but `uni_tracking_info_raw` has no column named `id` → caused `dbt_mart_uti` to fail in production. Reverted. Need to run `SELECT column_name FROM information_schema.columns WHERE table_name = 'uni_tracking_info_raw'` to find the actual unique row ID column name before re-applying.

**Status:** ⚠️ Reverted — column name needs verification before re-applying

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

---

## Issue 6 — mart_uts archive NOT IN guard too broad · **High**

**File:** `dbt_projects/order_tracking/models/mart/mart_uni_tracking_spath.sql`, post_hook step 1a

**Problem:**
mart_uts has MANY rows per order. Spath events age out incrementally — a few more events cross the 6-month line with each 15-min cycle. On the first cycle an order's events start aging out, some events get archived and the `order_id` is now in hist. On the next cycle, the `NOT IN (SELECT order_id ...)` guard blocks ALL remaining events for that order from ever being archived. They get flagged as `ARCHIVE_ROUTING_GAP_UTS` by the safety check and accumulate in mart_uts permanently (excluded from trim, never archived). The table grows without bound.

The comment in the file even says *"NOT IN guard filters by (order_id, pathTime range) — more specific than order_id alone"* but the actual SQL only uses `order_id`. Comment and code disagree.

**Current code:**
```sql
AND order_id NOT IN (
    SELECT order_id FROM settlement_ods.hist_uni_tracking_spath_2025_h2
    WHERE pathTime >= extract(epoch from '2025-07-01'::timestamp)
)
```

**Fix:** Use `NOT EXISTS` on `(order_id, pathTime)` pair and alias the outer query:
```sql
-- outer SELECT * becomes SELECT m.*  FROM {{ this }} m
AND NOT EXISTS (
    SELECT 1 FROM settlement_ods.hist_uni_tracking_spath_2025_h2 h
    WHERE h.order_id = m.order_id AND h.pathTime = m.pathTime
)
```

**Status:** ☐ Open

---

## Issue 7 — mart_ecs missing safety check before trim · **High**

**File:** `dbt_projects/order_tracking/models/mart/mart_ecs_order_info.sql`

**Problem:**
mart_uti (4 steps) and mart_uts (3 steps) both have a safety check step that catches unarchived rows before trim. mart_ecs goes directly archive → trim (2 steps) with no guard.

If an inactive order's `add_time` falls outside all defined hist periods (e.g., an order created in Jan 2026+ with no 2026_h1 period defined), step 1a skips it and step 2 silently deletes it. No exception is logged, no alert fires.

**Fix:**
Add a step 1b safety check between archive and trim:
```sql
-- Step 1b: catch inactive orders whose add_time doesn't match any hist_ecs period
INSERT INTO settlement_ods.order_tracking_exceptions (order_id, exception_type, detected_at, notes)
SELECT DISTINCT ecs.order_id, 'ARCHIVE_ROUTING_GAP_ECS', CURRENT_TIMESTAMP,
    'Inactive ECS order (add_time outside all defined hist_ecs periods) — excluded from trim'
FROM {{ this }} ecs
LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id
WHERE uti.order_id IS NULL
  AND ecs.add_time NOT BETWEEN extract(epoch from '2025-07-01'::timestamp)
                             AND extract(epoch from '2026-01-01'::timestamp)
  -- add NOT BETWEEN blocks for each defined period as they are added
  AND NOT EXISTS (
      SELECT 1 FROM settlement_ods.order_tracking_exceptions
      WHERE order_id = ecs.order_id
        AND exception_type = 'ARCHIVE_ROUTING_GAP_ECS'
        AND resolved_at IS NULL
  )
```
Then update step 2 (trim) to exclude `ARCHIVE_ROUTING_GAP_ECS` exceptions.

**Status:** ☐ Open

---

## Issue 8 — 2026_h1 hist period not defined · **High (time-sensitive)**

**File:** `dbt_projects/order_tracking/dbt_project.yml`, all 3 mart model post_hooks

**Problem:**
`dbt_project.yml` only has `2025_h2` defined. The comment says "add 2026_h1 on 1 Jan 2026" — that date is already past (today is Feb 2026).

Archiving won't actually trigger until ~Aug 2026 (6 months after the Feb 2026 pipeline start). When it does, rows with `update_time / pathTime / add_time` in `[2026-01-01, 2026-07-01)` will have no archive block defined, the safety checks will fire `ARCHIVE_ROUTING_GAP` exceptions, and marts will grow beyond the 6-month target.

**What needs to happen before Aug 2026:**
1. Create hist tables in Redshift: `hist_uni_tracking_info_2026_h1`, `hist_ecs_order_info_2026_h1`, `hist_uni_tracking_spath_2026_h1`
2. Add `2026_h1` entry to `dbt_project.yml` `hist_periods`
3. Add step 2b archive blocks to `mart_uni_tracking_info.sql` and `mart_uni_tracking_spath.sql`
4. Add step 1b archive block to `mart_ecs_order_info.sql`
5. Add `NOT EXISTS` checks for `hist_*_2026_h1` to safety check steps in all 3 mart models
6. Update mart_uti step 1 table name to `hist_uni_tracking_info_2026_h1` (reactivation cleanup)

**Status:** ☐ Open — not urgent today, deadline ~Aug 2026

---

## Issue 9 — `NOT IN` with potential NULLs in trim steps · **Medium**

**Files:**
- `mart_uni_tracking_info.sql` post_hook step 4
- `mart_uni_tracking_spath.sql` post_hook step 3

**Problem:**
Both trim steps use `NOT IN (SELECT order_id FROM exceptions WHERE ...)`. If `order_tracking_exceptions` ever contains a row with `NULL` order_id, `NOT IN` returns `UNKNOWN` for every candidate row — the entire trim silently deletes nothing. The mart table grows without bound and no error is raised.

**Fix:** Add `AND order_id IS NOT NULL` to both subqueries:
```sql
-- mart_uti step 4 (and mart_uts step 3 — same pattern):
AND order_id NOT IN (
    SELECT order_id FROM settlement_ods.order_tracking_exceptions
    WHERE exception_type = 'ARCHIVE_ROUTING_GAP'
      AND resolved_at IS NULL
      AND order_id IS NOT NULL   -- ← add this
)
```

**Status:** ☐ Open

---

## Issue 10 — stg_ecs_order_info.sql comment says "merge" · **Cosmetic**

**File:** `dbt_projects/order_tracking/models/staging/stg_ecs_order_info.sql`, line 40

**Problem:** Comment says `Strategy: merge` but the model uses `incremental_strategy='delete+insert'`.

**Fix:** Update comment to `Strategy: delete+insert`.

**Status:** ☐ Open

---

## Issue 11 — Monitoring DAG docstring says 2am, schedule is 3am · **Cosmetic**

**File:** `dags/order_tracking_daily_monitoring.py`, line 5

**Problem:** Docstring says `Runs every day at 2am UTC` but actual schedule is `0 3 * * *` (3am UTC).

**Fix:** Update docstring to 3am.

**Status:** ☐ Open

---

## Issue 12 — mart_ecs incremental_predicates defeats DISTKEY · **High / Performance**

**File:** `dbt_projects/order_tracking/models/mart/mart_ecs_order_info.sql`

**Problem:**
`mart_ecs` had `incremental_predicates` filtering `add_time > MAX - 7200` on the mart.
The intent (per docs) was zone map pruning on `add_time` to restrict the DELETE scope.
This does NOT work because `partner_id` is the leading sort key — zone maps on `add_time`
require it to be the leading column. With `partner_id` leading, the filter touches
~1 block per partner (300+ partners) instead of the last few blocks globally.

Result: every 15-min cycle the dbt DELETE was scanning almost the entire mart_ecs table.
Measured: 3m19s on 612K rows — vs mart_uts (3.8M rows) in 22s.

**Fix:**
Remove `incremental_predicates` entirely. Without it, dbt generates:
`DELETE FROM mart_ecs WHERE order_id IN (batch)` — uses DISTKEY(order_id) co-location,
near-zero cost since ECS is write-once and new order_ids aren't in the mart yet.

`partner_id` sort key and 2h raw-relative source cutoff are both correct — unchanged.

**Status:** ✅ Fixed — `mart_ecs_order_info.sql`

---

## Summary

| # | Issue | Severity | Status |
|---|---|---|---|
| 1 | Tie-break missing in mart_uti ranked CTE | Medium | ⚠️ Reverted — needs column verification |
| 2 | ecs_order_info_raw never trimmed | Medium | ✅ Fixed |
| 3 | HIST_UTS_TABLES hardcoded in monitoring | Low | ✅ Fixed |
| 4 | VACUUM SORT runs unconditionally | Low | ☐ Blocked (DBA) |
| 5 | ECS field change gap | Low | ☐ Watch |
| 6 | mart_uts archive NOT IN guard too broad | High | ☐ Open |
| 7 | mart_ecs missing safety check before trim | High | ☐ Open |
| 8 | 2026_h1 hist period not defined | High (Aug 2026 deadline) | ☐ Open |
| 9 | NOT IN with NULL risk in trim steps | Medium | ☐ Open |
| 10 | stg_ecs comment says "merge" | Cosmetic | ☐ Open |
| 11 | Monitoring docstring says 2am | Cosmetic | ☐ Open |
| 12 | mart_ecs incremental_predicates defeats DISTKEY | High / Performance | ✅ Fixed |
