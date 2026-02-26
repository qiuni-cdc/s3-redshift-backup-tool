# Order Tracking Pipeline — Implementation Task Flow

**Created**: 2026-02-25
**Companion docs**: `order_tracking_final_design.md`, `order_tracking_master_design.md`

---

## Part A — Implementation Phases (What to Build)

```
PHASE 0 — PRE-REQUISITES  (before any models run)
┌──────────────────────────────────────────────────────────────┐
│  Create DDL (tables must exist before post_hooks fire)       │
│                                                              │
│  ┌────────────────────────┐  ┌────────────────────────┐      │
│  │  hist_uti_2025_h2      │  │  hist_ecs_2025_h2      │      │
│  │  hist_uti_2026_h1      │  │  hist_ecs_2026_h1      │      │
│  └────────────────────────┘  └────────────────────────┘      │
│  ┌────────────────────────┐  ┌────────────────────────┐      │
│  │  hist_uts_2025_h2      │  │  order_tracking_       │      │
│  │  hist_uts_2026_h1      │  │  exceptions            │      │
│  └────────────────────────┘  └────────────────────────┘      │
│                                                              │
│  DDL requirements for all mart + hist tables:                │
│  - order_id BIGINT NOT NULL DISTKEY  (enforces NULL safety)  │
│  - UNIQUE (order_id, exception_type) on exceptions table     │
│    (informational in Redshift; enforce via NOT EXISTS in SQL)│
└─────────────────────────────────────┬────────────────────────┘
                                      │
                                      ▼
PHASE 1 — dbt MACRO + MART MODELS
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  macros/archive_to_hist.sql  ← CREATE FIRST          │    │
│  │  Jinja macro that generates all period-specific      │    │
│  │  INSERT statements + safety check from               │    │
│  │  dbt_project.yml vars.                               │    │
│  │  Adding a new 6-month period = one line in yml only. │    │
│  └──────────────────────────────────────────────────────┘    │
│                              │                               │
│                              ▼                               │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  mart_uni_tracking_info.sql                          │    │
│  │  - ranked CTE: dedup within batch                    │    │
│  │    ORDER BY update_time DESC, id DESC  ← tie-break   │    │
│  │  - delete+insert, NO incremental_predicates          │    │
│  │  - POST_HOOK 1: DELETE stale hist_uti entry          │    │
│  │                 for reactivated orders               │    │
│  │  - POST_HOOK 2: {{ archive_to_hist(...) }} macro     │    │
│  │                 NOT EXISTS guard on each INSERT       │    │
│  │  - POST_HOOK 3: safety check → log gaps to           │    │
│  │                 order_tracking_exceptions            │    │
│  │  - POST_HOOK 4: DELETE aged rows, exclude exceptions │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌───────────────────────────┐  ┌───────────────────────┐    │
│  │  mart_ecs_order_info.sql  │  │  mart_uni_tracking    │    │
│  │  - append-only INSERT     │  │  _spath.sql           │    │
│  │  - POST_HOOK 1: archive   │  │  - append-only INSERT │    │
│  │    LEFT JOIN anti-join    │  │  - POST_HOOK 1:       │    │
│  │    (NULL-safe, not NOT IN)│  │    {{ archive_to_hist │    │
│  │    → hist_ecs_YYYY_HX     │  │    (...) }} macro     │    │
│  │  - POST_HOOK 2: DELETE    │  │  - POST_HOOK 2:       │    │
│  │    USING anti-join        │  │    safety check →     │    │
│  │    (NULL-safe trim)       │  │    exceptions         │    │
│  └───────────────────────────┘  │  - POST_HOOK 3:       │    │
│                                 │    pure time-based    │    │
│                                 │    trim, excl. gaps   │    │
│                                 └───────────────────────┘    │
└─────────────────────────────────────┬────────────────────────┘
                                      │
                                      ▼
PHASE 2 — DAG UPDATE
┌──────────────────────────────────────────────────────────────┐
│  order_tracking_hybrid_dbt_dag.py                            │
│                                                              │
│  Remove:                                                     │
│  - stg_uni_tracking_info task                                │
│  - stg_ecs_order_info task                                   │
│  - stg_uni_tracking_spath task                               │
│  - spath_gate_task / uti_cleanup_task (if present)           │
│                                                              │
│  Add / update:                                               │
│  - max_active_runs=1  ← REQUIRED, prevents cycle overlap    │
│  - catchup=False      ← do not replay missed cycles         │
│  - execution_timeout per task (e.g. 10 min)                 │
│    → kills runaway tasks before next cycle starts           │
│  - dbt run mart_uti  ← must complete before ecs and uts     │
│  - dbt run mart_ecs  ← depends on mart_uti                  │
│  - dbt run mart_uts  ← depends on mart_uti                  │
│  - mart_ecs ║ mart_uts run in parallel                       │
└─────────────────────────────────────┬────────────────────────┘
                                      │
                                      ▼
PHASE 3 — QA VALIDATION  (run on QA environment)
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌───────────────────────────┐  ┌───────────────────────┐    │
│  │  seed_mart_from_staging   │  │  perf_test            │    │
│  │  INSERT mart_uti FROM stg │  │  Measure DELETE time  │    │
│  │  INSERT mart_ecs FROM stg │  │  at current row count │    │
│  │  INSERT mart_uts FROM stg │  │  → project to 90M     │    │
│  └──────────────┬────────────┘  └──────────┬────────────┘    │
│                 │                           │                 │
│                 ▼                           ▼                 │
│  ┌───────────────────────────┐  ┌───────────────────────┐    │
│  │  run_dbt_incremental_QA   │  │  go_nogo_option_B     │    │
│  │  - verify 0 duplicates    │  │  IF DELETE >5min at   │    │
│  │  - verify archive fires   │  │  90M rows:            │    │
│  │  - verify trim fires      │  │  → fallback to        │    │
│  │  - verify hist row count  │  │    Option A (stg_uti  │    │
│  └───────────────────────────┘  │    as 20-day buffer)  │    │
│                                 └───────────────────────┘    │
└─────────────────────────────────────┬────────────────────────┘
                                      │
                                      ▼
PHASE 4 — DATA QUALITY TASKS  (add to Airflow as daily tasks)
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌────────────────────────┐  ┌───────────────────────────┐   │
│  │  test1_daily_task      │  │  test2_daily_task         │   │
│  │  update_time vs        │  │  orders in mart_uti with  │   │
│  │  MAX(pathTime)         │  │  no spath in mart_uts     │   │
│  │  alignment check       │  │  OR hist_uts anywhere     │   │
│  └────────────────────────┘  └───────────────────────────┘   │
│  ┌────────────────────────┐  ┌───────────────────────────┐   │
│  │  test3_daily_task      │  │  compare_pipelines        │   │
│  │  reactivated orders    │  │  --table ecs  (Gap 1)     │   │
│  │  missing ecs row       │  │  --table uts  (Gap 2)     │   │
│  └────────────────────────┘  └───────────────────────────┘   │
│                                                              │
│  All 4 tasks → write results → order_tracking_exceptions    │
│  Alert task → fire if unresolved exceptions > 0             │
└─────────────────────────────────────┬────────────────────────┘
                                      │
                                      ▼
PHASE 5 — PRODUCTION MIGRATION
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  snapshot_staging                                            │
│  CREATE TABLE stg_uni_tracking_info_backup AS               │
│  SELECT * FROM stg_uni_tracking_info;  (+ ecs, uts)         │
│                 │                                            │
│                 ▼                                            │
│  seed_mart_from_staging  (PROD)                              │
│  INSERT mart_uti FROM stg_uni_tracking_info                  │
│  INSERT mart_ecs FROM stg_ecs_order_info                     │
│  INSERT mart_uts FROM stg_uni_tracking_spath                 │
│                 │                                            │
│                 ▼                                            │
│  run_retention_manually                                      │
│  Execute post_hooks once to trim to 6-month window          │
│  and seed hist tables with pre-existing aged rows           │
│                 │                                            │
│                 ▼                                            │
│  ANALYZE mart_uti, mart_ecs, mart_uts                        │
│                 │                                            │
│                 ▼                                            │
│  switch_live_traffic                                         │
│  Update all user queries: stg_* → mart_*                     │
│                 │                                            │
│                 ▼                                            │
│  monitor_1_week                                              │
│  - check exceptions table daily                              │
│  - check mart row counts                                     │
│  - check hist row counts growing as expected                 │
│                 │                                            │
│                 ▼                                            │
│  drop_stg_tables  (after 1 week clean run)                   │
│  DROP TABLE stg_uni_tracking_info;  (+ ecs, uts)            │
└─────────────────────────────────────┬────────────────────────┘
                                      │
                                      ▼
PHASE 6 — ONGOING OPERATIONS  (recurring, no end date)
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  NIGHTLY (Airflow, off-peak window e.g. 2–5am)               │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  VACUUM DELETE ONLY mart_uni_tracking_info           │    │
│  │  VACUUM DELETE ONLY mart_uni_tracking_spath          │    │
│  │  VACUUM DELETE ONLY mart_ecs_order_info              │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  WEEKLY (conditional — checks unsorted % first)             │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  query svv_table_info.unsorted for mart_ecs          │    │
│  │  IF unsorted > 15%:                                  │    │
│  │    VACUUM SORT ONLY mart_ecs_order_info              │    │
│  │  ELSE: skip (expected ~every 2–4 months)             │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                              │
│  EVERY 1 JAN AND 1 JUL                                       │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  1. CREATE hist_uti_YYYY_HX                          │    │
│  │     CREATE hist_ecs_YYYY_HX                          │    │
│  │     CREATE hist_uts_YYYY_HX                          │    │
│  │                                                      │    │
│  │  2. Add new period to dbt_project.yml vars           │    │
│  │     (hist_periods list) — ONE LINE ONLY              │    │
│  │     Macro generates all SQL automatically            │    │
│  │                                                      │    │
│  │  3. UPDATE mart_uti POST_HOOK 1 cleanup table name   │    │
│  │     (point to new latest hist_uti table)             │    │
│  │                                                      │    │
│  │  4. Redeploy dbt — no SQL editing needed             │    │
│  └──────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

---

## Part B — Runtime Cycle (Every 15 Minutes)

```
┌──────────────────────────────────────────────────────────────┐
│  EXTRACTION  (parallel, ~94s)                                │
│                                                              │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐           │
│  │  extract │      │  extract │      │  extract │           │
│  │   ecs    │      │   uti    │      │   uts    │           │
│  └─────┬────┘      └─────┬────┘      └─────┬────┘           │
│        └────────────────┬┘                  │               │
│                         └──────────────────┘                │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│  dbt: mart_uni_tracking_info  (~100–150s)                    │
│                                                              │
│  a.  ranked CTE  →  dedup within batch                       │
│      ORDER BY update_time DESC, id DESC  (deterministic)     │
│  b.  DELETE WHERE order_id IN (batch)                        │
│      removes old row regardless of age                       │
│  c.  INSERT ranked rows — exactly one row per order_id       │
│  d.  POST_HOOK 1: DELETE stale hist_uti entry                │
│      for reactivated orders (latest hist table only)         │
│  e.  POST_HOOK 2: macro-generated INSERTs → hist_uti_YYYY_HX │
│      NOT EXISTS guard on each INSERT (idempotent retry)      │
│  f.  POST_HOOK 3: safety check — log unmatched rows          │
│      to order_tracking_exceptions (never silent data loss)   │
│  g.  POST_HOOK 4: DELETE aged rows, exclude exceptions rows  │
└──────────────────────────────┬───────────────────────────────┘
                               │
               ┌───────────────┴────────────────┐
               ▼                                ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│  dbt: mart_ecs_order_info│    │  dbt: mart_uni_tracking_spath│
│  (parallel, ~20s)        │    │  (parallel, ~30s)            │
│                          │    │                              │
│  a. INSERT new ecs rows  │    │  a. INSERT new spath events  │
│     from ecs_raw         │    │     from uts_raw             │
│  b. POST_HOOK 1: archive │    │  b. POST_HOOK 1: macro INSERTs│
│     LEFT JOIN anti-join  │    │     → hist_uts_YYYY_HX       │
│     (NULL-safe, not NOT  │    │     NOT EXISTS guard         │
│     IN) → hist_ecs_YYYY  │    │  c. POST_HOOK 2: safety check│
│  c. POST_HOOK 2: DELETE  │    │     → log gaps to exceptions │
│     USING anti-join      │    │  d. POST_HOOK 3: pure time-  │
│     (NULL-safe trim)     │    │     based trim, excl. gaps   │
└──────────────┬───────────┘    └─────────────────┬────────────┘
               └────────────────┬─────────────────┘
                                ▼
                   ┌────────────────────────┐
                   │   cycle complete       │
                   │   ~4 min total         │
                   │   ~11 min headroom     │
                   └────────────────────────┘
```

---

## Part C — Daily Monitoring Cycle

```
┌──────────────────────────────────────────────────────────────┐
│  DAILY MONITORING TASKS  (parallel)                          │
│                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │  test1          │  │  test2          │  │  test3      │  │
│  │  update_time vs │  │  orders in      │  │  orders in  │  │
│  │  MAX(pathTime)  │  │  mart_uti with  │  │  mart_uti   │  │
│  │  alignment      │  │  no spath in    │  │  missing    │  │
│  │                 │  │  mart_uts OR    │  │  ecs row    │  │
│  │                 │  │  hist_uts       │  │             │  │
│  └────────┬────────┘  └────────┬────────┘  └──────┬──────┘  │
│           └───────────────────┬┘                  │         │
│                               └──────────────────┘          │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
              ┌─────────────────────────────┐
              │  write_exceptions           │
              │  INSERT unresolved cases →  │
              │  order_tracking_exceptions  │
              └──────────────┬──────────────┘
                             │
                             ▼
              ┌─────────────────────────────┐
              │  alert_check                │
              │  SELECT COUNT(*) WHERE      │
              │  resolved_at IS NULL        │
              │  → alert if > 0            │
              └─────────────────────────────┘
```

---

## Summary — Task Owners and Order

| Phase | Tasks | Depends on | Owner |
|---|---|---|---|
| 0 | Create hist_* DDL (NOT NULL on order_id), exceptions DDL (UNIQUE constraint) | Nothing | DBA / infra |
| 1a | Write macros/archive_to_hist.sql; populate dbt_project.yml hist_periods vars | Phase 0 complete | dbt dev |
| 1b | Write mart_uti.sql (4 post_hooks, tie-break sort), mart_ecs.sql (DELETE USING), mart_uts.sql (3 post_hooks) | Phase 1a complete | dbt dev |
| 2 | Update Airflow DAG: max_active_runs=1, execution_timeout, remove stg tasks, wire dependencies | Phase 1b complete | Airflow dev |
| 3 | QA seed + validate + perf test + verify safety check fires | Phase 2 on QA | QA |
| 4 | Add daily monitoring tasks + weekly conditional VACUUM SORT sensor to DAG | Phase 2 complete | Airflow dev |
| 5 | Snapshot, seed PROD, switch traffic, monitor, drop stg | Phase 3 passed | DBA + team |
| 6 | Nightly VACUUM, conditional VACUUM SORT, half-year: add yml period + CREATE hist tables + redeploy dbt | Phase 5 live | Ops / Airflow |

## Fix Reference — Issue to Phase Mapping

| Issue | Severity | Implemented in |
|---|---|---|
| Archive routing gap — safety check step 3 | CRITICAL | Phase 1b (mart_uti step 3 post_hook) |
| Macro-driven period routing | CRITICAL + MEDIUM | Phase 1a (macro) |
| master_design.md mart_uts wrong retention | CRITICAL | Doc fix (done) |
| NOT IN → DELETE USING anti-join | HIGH | Phase 1b (mart_ecs post_hooks) |
| NOT NULL on order_id | HIGH | Phase 0 (DDL) |
| Lock window documentation | HIGH | Doc fix (done) |
| max_active_runs=1 + execution_timeout | MEDIUM | Phase 2 (DAG) |
| mart_ecs archive spurious add_time | MEDIUM | Doc fix (done) |
| ranked CTE tie-break secondary sort | MEDIUM | Phase 1b (mart_uti.sql) |
| Exceptions table UNIQUE + NOT EXISTS | LOW | Phase 0 (DDL) + Phase 1b (inserts) |
| Conditional VACUUM SORT for mart_ecs | LOW | Phase 4 + Phase 6 (Airflow sensor) |
