# Order Tracking — Monitoring & Vacuum

Two independent Airflow DAGs run nightly to maintain data quality and table health.

---

## DAG 1 — `order_tracking_daily_monitoring`

**Schedule:** `0 3 * * *` — 3am UTC daily (1 hour after vacuum)
**File:** `airflow_poc/dags/order_tracking_daily_monitoring.py`
**On failure:** Email alert to `jasleen.tung@uniuni.com`

### What it checks

Runs 4 data quality tests against the last **24 hours** of orders
(`update_time > now - 24h`). Results are written to
`settlement_ods.order_tracking_exceptions`.

---

#### Test 0 — Extraction count check (MySQL vs Redshift raw)

| | |
|---|---|
| **What** | Row count in MySQL source vs Redshift `*_raw` tables for the same closed 2-hour window |
| **Window** | `[now - 2h 5min, now - 5min)` — closed window safely inside raw retention |
| **Tables** | `kuaisong.ecs_order_info` vs `settlement_public.ecs_order_info_raw` |
| | `kuaisong.uni_tracking_info` vs `settlement_public.uni_tracking_info_raw` |
| | `kuaisong.uni_tracking_spath` vs `settlement_public.uni_tracking_spath_raw` |
| **Alarm** | MySQL count > Redshift raw count (rows dropped during extraction or Redshift COPY) |
| **Action** | Raises immediately — does **not** write to exceptions table. Infrastructure alert: check extraction logs and Redshift COPY errors. |
| **Note** | uti raw count ≥ MySQL count is normal (same order can appear in multiple 15-min cycles). Alarm only fires if raw count < MySQL count. |

---

#### Test 1 — UTI / UTS time alignment

| | |
|---|---|
| **What** | `update_time` in `mart_uni_tracking_info` should equal `MAX(pathTime)` in `mart_uni_tracking_spath` for the same order |
| **Scope** | Orders with `update_time > now - 24h` |
| **Alarm** | `update_time ≠ MAX(pathTime)` |
| **Causes** | Backdated `update_time`, missed uti or uts extraction cycle, uti/uts sync drift |
| **Exception type** | `UTI_UTS_TIME_MISMATCH` |
| **Action** | Investigate extraction logs for the affected order's time window. Check if a backfill is needed. |

---

#### Test 2 — Orders missing from spath

| | |
|---|---|
| **What** | Orders in `mart_uni_tracking_info` with no rows in `mart_uni_tracking_spath` **and** no rows in any `hist_uts` table |
| **Scope** | Orders with `update_time > now - 24h` |
| **Alarm** | Order has no tracking path events anywhere |
| **Causes** | Genuine data gap — spath events should exist for every order |
| **Exception type** | `ORDER_MISSING_SPATH` |
| **Action** | Verify in MySQL source (`kuaisong.uni_tracking_spath`). If data exists in source but not in Redshift, a targeted backfill is required. |
| **Maintenance** | When a new `hist_uts` table is created (1 Jan / 1 Jul each year), add it to `HIST_UTS_TABLES` in the DAG file. |

---

#### Test 3 — Reactivated orders missing from ecs

| | |
|---|---|
| **What** | Orders in `mart_uni_tracking_info` with no matching row in `mart_ecs_order_info` |
| **Scope** | Orders with `update_time > now - 24h` |
| **Alarm** | Active order has no creation record in mart_ecs |
| **Causes** | Order was dormant for >6 months → mart_ecs archived its row to `hist_ecs_order_info_2025_h2` → order reactivated → mart_ecs never re-extracts old `add_time` rows |
| **Exception type** | `REACTIVATED_ORDER_MISSING_ECS` |
| **Action** | Manual fix: `INSERT INTO mart_ecs SELECT * FROM hist_ecs_order_info_2025_h2 WHERE order_id = <id>` then `DELETE FROM hist_ecs WHERE order_id = <id>`. See `order_tracking_final_design.md §5`. |
| **Expected frequency** | Rare edge case |

---

#### Final step — `check_unresolved_exceptions`

Runs after all 4 tests with `trigger_rule=ALL_DONE` (runs even if a test fails).
Queries `settlement_ods.order_tracking_exceptions` for all unresolved rows, logs a
summary, and raises if `total_unresolved > 0`. This is what triggers the failure email.

**Querying exceptions manually:**
```sql
-- All unresolved exceptions
SELECT exception_type, COUNT(*) AS cnt
FROM settlement_ods.order_tracking_exceptions
WHERE resolved_at IS NULL
GROUP BY exception_type
ORDER BY cnt DESC;

-- Detail for a specific exception type
SELECT order_id, detected_at, notes
FROM settlement_ods.order_tracking_exceptions
WHERE exception_type = 'UTI_UTS_TIME_MISMATCH'
  AND resolved_at IS NULL
ORDER BY detected_at DESC;

-- Resolve an exception after manual fix
UPDATE settlement_ods.order_tracking_exceptions
SET resolved_at = CURRENT_TIMESTAMP, resolved_by = 'your-name', resolution_notes = 'reason'
WHERE order_id = <id> AND exception_type = '<type>' AND resolved_at IS NULL;
```

---

## DAG 2 — `order_tracking_vacuum`

**Schedule:** `0 2 * * *` — 2am UTC daily (runs before monitoring)
**File:** `airflow_poc/dags/order_tracking_vacuum.py`
**On failure:** Email alert to `jasleen.tung@uniuni.com`

### Why VACUUM is needed

The 15-min extraction cycle runs `DELETE+INSERT` on mart tables ~96 times per day.
Each DELETE leaves ghost rows on disk. Without VACUUM, ghost rows accumulate,
bloat table size, and degrade Redshift zone map pruning (slowing all queries).

### What it runs

| Task | Table | Operations | Why |
|---|---|---|---|
| `vacuum_mart_uti` | `mart_uni_tracking_info` | VACUUM DELETE ONLY → ANALYZE | 96 DELETE cycles/day; ANALYZE refreshes planner stats after high-churn deletes |
| `vacuum_mart_uts` | `mart_uni_tracking_spath` | VACUUM DELETE ONLY → ANALYZE | 96 DELETE cycles/day; ANALYZE refreshes planner stats after high-churn deletes |
| `vacuum_mart_ecs_delete` | `mart_ecs_order_info` | VACUUM DELETE ONLY | Post-trim DELETEs; no ANALYZE here — sort task runs it after |
| `vacuum_mart_ecs_sort` | `mart_ecs_order_info` | VACUUM SORT ONLY → ANALYZE | Unconditional — Redshift skips fast if nothing to sort; ANALYZE reflects re-sorted layout |

### VACUUM SORT for mart_ecs

`mart_ecs` has `SORTKEY(partner_id, add_time, order_id)`. New orders arrive roughly
in `add_time` order but `partner_id` is random — the unsorted region grows over time.
Zone maps on `partner_id` (the primary query filter) degrade as unsorted % rises.

`VACUUM SORT ONLY` runs unconditionally every day. Redshift detects quickly when
nothing needs sorting and returns immediately, so there is no wasted effort on
already-sorted tables.

---

## Task execution order

```
2:00am UTC  order_tracking_vacuum
            └── vacuum (parallel)
                ├── vacuum_mart_uti
                ├── vacuum_mart_uts
                └── vacuum_mart_ecs_delete → vacuum_mart_ecs_sort

3:00am UTC  order_tracking_daily_monitoring
            ├── dq_checks (parallel)
            │   ├── dq_extraction_count_check   (Test 0)
            │   ├── dq_uti_uts_alignment         (Test 1)
            │   ├── dq_missing_spath             (Test 2)
            │   └── dq_reactivated_missing_ecs   (Test 3)
            └── check_unresolved_exceptions      (ALL_DONE)
```

---

## Configuration reference

| Constant | Value | Location |
|---|---|---|
| `DQ_LOOKBACK_HOURS` | 24h | `order_tracking_daily_monitoring.py` |
| `EXTRACTION_WINDOW_SECS` | 2h | `order_tracking_daily_monitoring.py` |
| `EXTRACTION_BUFFER_SECS` | 5min | `order_tracking_daily_monitoring.py` |
| Vacuum schedule | `0 2 * * *` | `order_tracking_vacuum.py` |
| Monitoring schedule | `0 3 * * *` | `order_tracking_daily_monitoring.py` |
