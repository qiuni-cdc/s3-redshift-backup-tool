# dbt Staging Refactor — Tracking Document

## Context

The `order_tracking_hybrid_dbt_dag` runs every 15 minutes. The `dbt_staging` task was taking **~12 minutes** with the old 7-day lookback, making the pipeline barely fit within its own schedule interval.

The staging layer runs 3 incremental models:
- `stg_ecs_order_info` — unique key: `order_id`, sort: `add_time`
- `stg_uni_tracking_info` — unique key: `order_id`, sort: `update_time`
- `stg_uni_tracking_spath` — unique key: `(order_id, traceSeq, pathTime)`, sort: `pathTime`

---

## Issues Identified

### 1. Source scan: 7-day lookback (FIXED earlier)
- **Problem**: dbt was scanning 14M raw rows per run
- **Fix**: Reduced to 30-min source cutoff (`max(timestamp) - 1800`) in all 3 models
- **Status**: ✅ Done

### 2. DELETE bottleneck on `stg_uni_tracking_spath` (FIXED earlier)
- **Problem**: No `incremental_predicates` → Redshift scans the entire staging table on every DELETE
- **Fix**: Added 2-hour delete window (`incremental_predicates` on `pathTime`)
- **Safe because**: `pathTime` is in the `unique_key` and is the SORTKEY — zone map pruning skips 99%+ of blocks
- **Status**: ✅ Done

### 3. `pip install` running on every 15-min DAG cycle (FIXED 2026-02-21)
- **Problem**: `pip install dbt-core dbt-redshift` ran inside `DBT_WITH_TUNNEL` (direct connection path) on every single run. Adds 1–3 min overhead even when packages are already installed.
- **Fix**: Removed from DAG bash command. Belongs in Dockerfile/deployment, not runtime.
- **Status**: ✅ Done

### 4. `dbt debug` + socket connectivity check on every run (FIXED 2026-02-21)
- **Problem**: Diagnostic block added during initial debugging was never removed:
  - `python3 -c "import socket..."` — network probe (~10s)
  - `dbt debug --profiles-dir . --debug` — full dbt diagnostics (~30–60s)
  - `cat /tmp/dbt_debug.log` — log dump
- **Fix**: Removed from production DAG path.
- **Status**: ✅ Done

### 5. `threads: 1` in dev profile — models run sequentially (CLOSED - Won't Fix)
- **Problem**: `profiles.yml` dev target has `threads: 1`. All 3 staging models run one after another.
- **Tried**: `threads: 3` — caused a bottleneck (likely Redshift WLM queue or lock waits from 3 concurrent `delete+insert` operations hitting Redshift simultaneously)
- **Decision**: Keep `threads: 1`. Sequential execution is more predictable and avoids Redshift concurrency issues. The gain from parallelism is offset by Redshift queuing.
- **Status**: ✅ Closed — keeping threads: 1

### 6. Double SSH tunnel setup/teardown per cycle (OPEN)
- **Problem**: `dbt_staging` and `dbt_test` are separate Airflow tasks, each independently setting up and tearing down an SSH tunnel (tunnel mode only).
- **Potential fix**: Merge into a single task, or reduce `dbt_test` run frequency (e.g. hourly instead of every 15 min)
- **Status**: 🔲 Open

---

## Current Model Design (Reference)

| Model | Source scan | Delete window | Rationale |
|---|---|---|---|
| `stg_ecs_order_info` | 30 min | 2 hours | `add_time` fixed at creation. Zone map pruning via SORTKEY. |
| `stg_uni_tracking_info` | 30 min | None | Small table (1 row/order). DELETE by DISTKEY (`order_id`) is fast. No time restriction needed. |
| `stg_uni_tracking_spath` | 30 min | 2 hours | Largest table. `pathTime` is both SORTKEY and part of unique_key → safe for zone map pruning. |

---

## Change Log

| Date | Change | File | Result |
|---|---|---|---|
| Pre-2026-02-21 | Reduced source scan from 7 days → 30 min | `stg_*.sql` | Major reduction in raw rows scanned |
| Pre-2026-02-21 | Added `incremental_predicates` (2h) on `stg_uni_tracking_spath` | `stg_uni_tracking_spath.sql` | DELETE uses SORTKEY zone maps |
| 2026-02-21 | Removed `pip install`, `dbt debug`, socket check from DAG direct-connection path | `order_tracking_hybrid_dbt_dag.py` | Removes ~2–4 min of overhead per run |

---

## Next Steps

1. **Run dbt staging standalone** — `dbt run --select staging --profiles-dir .` directly on server. Confirm models are fast with the optimizations already in place (pip install + dbt debug removed).
2. **Full pipeline test** — Trigger DAG via Airflow UI with logical date = unix `1769300700` (gives sync window starting at `1769299200`, right after the loaded historic data). Measure end-to-end runtime.
3. **Consider merging `dbt_staging` + `dbt_test`** into one task to eliminate double tunnel overhead (open, lower priority).
