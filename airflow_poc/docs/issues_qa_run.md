# Issues with QA Run

## Overview

This document captures all problems observed running `order_tracking_hybrid_dbt_dag` on the QA cluster. Problems span three layers: DAG overhead, extraction (MySQL → S3 → Redshift raw), and dbt staging (raw → staging).

**Environment**: QA Redshift cluster (likely `dc2.large`, limited I/O)
**Data volume**: 24-day historic load in raw + staging tables
**DAG schedule**: every 15 minutes, `max_active_runs=1`

---

## Layer 1: DAG Overhead

### Issue 1.1 — `pip install` on every 15-min cycle (FIXED 2026-02-21)
- **Problem**: `pip install dbt-core dbt-redshift` ran inside the dbt bash command on every DAG run
- **Impact**: 1–3 min added per cycle even when packages already installed
- **Fix**: Removed from runtime bash command. Belongs in Dockerfile/deployment.

### Issue 1.2 — `dbt debug` + socket check on every run (FIXED 2026-02-21)
- **Problem**: Diagnostic block left in from initial debugging:
  - `python3 -c "import socket..."` → ~10s network probe
  - `dbt debug --profiles-dir . --debug` → ~30–60s full diagnostics
  - `cat /tmp/dbt_debug.log` → log dump
- **Impact**: 40–70s overhead per cycle
- **Fix**: Removed from production DAG path.

### Issue 1.3 — Duplicate `source venv/bin/activate` in extraction tasks (FIXED 2026-02-21)
- **Problem**: All 3 extraction bash commands had the venv activation line duplicated
- **Impact**: Cosmetic / minor, harmless
- **Fix**: Removed duplicate lines.

### Issue 1.4 — Schema name typo (FIXED 2026-02-21)
- **Problem**: `raw_schema: "settlment_public"` in `pipeline.yml` (missing 'e') and `"settlment_public.*_raw"` in DAG `TABLES` dict
- **Impact**: None — overridden by table-level `target_schema: "settlement_public"` and `TABLES` dict not used in bash commands
- **Fix**: Corrected to `settlement_public` in both files.

---

## Layer 2: Extraction (MySQL → S3 → Redshift Raw)

### Timing observed (`scheduled__2026-02-21T01:15:00+00:00` run)

| Task | Rows | MySQL read | S3 upload | Redshift COPY | Total |
|---|---|---|---|---|---|
| `extract_ecs` | 30,053 | ~42s | ~1s | **155.6s** (193 rows/sec) | 199.8s |
| `extract_uti` | 100,000 | ~65s | ~2s | **133.9s** (747 rows/sec) | 201.5s |
| `extract_uts` | 200,000 | ~30s | ~2s | **8.2s** (24,487 rows/sec) | 41.7s |

### Issue 2.1 — Redshift COPY into raw tables is slow (OPEN)
- **Problem**: COPY of wide tables into Redshift raw is the bottleneck — not MySQL read speed, not S3 upload
  - `ecs_order_info_raw` (166 cols): 155.6s for 30K rows → 193 rows/sec
  - `uni_tracking_info_raw`: 133.9s for 100K rows → 747 rows/sec
  - `uni_tracking_spath_raw` (few cols): 8.2s for 200K rows → 24,487 rows/sec ✅
- **Root cause**: Redshift columnar storage writes 166 separate column files per data block even for small row counts. Wide tables have extreme per-write overhead on the QA cluster.
- **Status**: 🔲 Open — pending prod cluster test

### Issue 2.2 — `extract_uts` failed on attempt 1, succeeded on attempt 2
- **Observed**: `attempt=1.log` shows Duration 15.6s → `UP_FOR_RETRY`. `attempt=2.log` succeeded.
- **Root cause**: Not fully investigated. Likely a transient connection issue (MySQL SSH tunnel or S3).
- **Status**: 🔲 Open — monitor for recurrence

---

## Layer 3: dbt Staging (Raw → Staging)

### Timing observed (server run, 24-day data, QA cluster)

| Model | Time | Notes |
|---|---|---|
| `stg_ecs_order_info` | ~467–514s | Bottleneck |
| `stg_uni_tracking_info` | ~210–258s | Slow |
| `stg_uni_tracking_spath` | ~20–33s | ✅ Fast |
| **Total** | **~12 min** | Barely fits in 15-min schedule |

### Issue 3.1 — Source scan: 7-day lookback (FIXED earlier)
- **Problem**: dbt was scanning 14M raw rows per run
- **Fix**: Reduced to 30-min source cutoff (`max(timestamp) - 1800`) in all 3 models

### Issue 3.2 — `incremental_predicates` always `> 0` due to parse-time evaluation (FIXED 2026-02-21)
- **Problem**: `config(incremental_predicates=...)` is evaluated at parse time (`execute=False`), so `run_query()` inside it never fires → `delete_cutoff` Jinja variable stays `0` → DELETE scans entire staging table
- **Confirmed**: Compiled SQL showed `add_time > 0` instead of actual cutoff
- **Fix**: Replaced Jinja variable with SQL subquery inside `incremental_predicates` → Redshift evaluates the subquery at runtime
- **Limitation**: Zone maps require literal constants, not SQL subqueries → no zone map pruning benefit. But the DELETE logic is now correct.

### Issue 3.3 — `stg_ecs` / `stg_uti` DELETE scans full staging table — O(table_size) (FIXED 2026-02-21)
- **Problem**: Single `unique_key='order_id'` → dbt generates `DELETE WHERE order_id IN (subquery)` → full staging table scan on every run
- **Scale risk**: stg_ecs=17M rows now, projected 260M at 1 year → pipeline completely breaks
- **Fix**: Switched both models to `incremental_strategy='merge'`
  - MERGE uses DISTKEY (`order_id`) co-location → O(batch_size), not O(table_size)
  - stg_ecs: `add_time` static → UPDATE is a no-op for existing rows
  - stg_uti: `update_time` changes → MERGE UPDATE correctly keeps latest state

### Issue 3.4 — Wide table columnar write overhead is the actual bottleneck (OPEN)
- **Problem**: `stg_ecs_order_info` has 167 columns. Redshift writes 167 separate column files even for a small batch of rows.
- **Diagnostic breakdown**:

  | Step | Time | Notes |
  |---|---|---|
  | SELECT COUNT from raw (read only) | 7.2s | Fast — raw scan is not the problem |
  | CREATE TEMP TABLE (5,398 rows) | **109s** | 167 column files written to columnar store |
  | MERGE scan (17M staging rows) | **~390s** | 167 columns × 17M rows |
  | **Total** | **~514s** | |

- **What was ruled out**:
  - ❌ Source scan (raw read) — fast at 7.2s
  - ❌ DELETE strategy — switching between IN / USING JOIN / MERGE showed no improvement
  - ❌ `incremental_predicates` bug — fixed, but zone maps need literal constants
  - ✅ **Column width is the confirmed bottleneck**
- **Status**: 🔲 Open — pending prod cluster test. Prod cluster has more I/O capacity; may resolve without further changes.

### Issue 3.5 — `threads: 1` limits parallelism (CLOSED - Won't Fix)
- **Problem**: `profiles.yml` dev target has `threads: 1` → all 3 staging models run sequentially
- **Tried**: `threads: 3` — caused bottleneck (likely Redshift WLM queue contention or lock waits from 3 concurrent delete+insert operations)
- **Decision**: Keep `threads: 1`. Sequential execution is more predictable; parallelism gain offset by Redshift queuing.

### Issue 3.6 — Double SSH tunnel setup/teardown per cycle (OPEN)
- **Problem**: `dbt_staging` and `dbt_test` are separate Airflow tasks, each independently setting up and tearing down an SSH tunnel (tunnel mode only)
- **Impact**: ~30–60s extra overhead per cycle in tunnel mode
- **Potential fix**: Merge into a single task, or reduce `dbt_test` to run hourly instead of every 15 min
- **Status**: 🔲 Open (low priority)

---

## Root Cause Summary

| Layer | Bottleneck | Status |
|---|---|---|
| DAG | pip install + dbt debug overhead (~3 min) | ✅ Fixed |
| Extraction | Redshift COPY into 166-col raw table (~156s for ecs) | 🔲 Pending prod test |
| dbt staging | Redshift columnar write of 167-col staging table (~514s for ecs) | 🔲 Pending prod test |

**Core problem**: The QA cluster (`dc2.large`) has limited I/O for wide-table columnar writes. Both the extraction COPY and the dbt staging MERGE hit the same hardware constraint. Prod cluster test will determine whether this resolves on better hardware or requires column narrowing.

---

## Open Action Items

1. **Prod cluster test** — run `dbt run --select staging` on prod Redshift. If stg_ecs drops from ~514s to <120s, hardware was the problem and no further changes needed.
2. **Narrow columns (if prod test still slow)** — identify which of the 167 ecs/uti columns are used downstream. Replace `SELECT *` with explicit column list → biggest possible win.
3. **Monitor `extract_uts` retry** — watch for recurrence of attempt=1 failure pattern.
4. **Weekly VACUUM** — schedule `VACUUM DELETE ONLY` on stg_ecs and stg_uti to clean dead rows from merge operations.
5. **Consider merging `dbt_staging` + `dbt_test`** — eliminates double tunnel overhead (low priority).
