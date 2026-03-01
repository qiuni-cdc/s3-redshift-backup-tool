# Order Tracking Pipeline — Architecture Assessment
**Date:** 2026-03-01
**Author:** Claude (reviewed with Jasleen Tung)
**Purpose:** Full assessment of new Airflow+dbt pipeline vs legacy Kettle pipeline, covering design decisions, index/query coverage, gaps, backfill plan, and production readiness.

---

## 1. Context

### Legacy (Current in Production)
- **PROD MySQL** (`kuaisong` schema): live data, 10–15 day rolling window
- **DW MySQL** (`uniods` schema): historical DW, populated by Kettle ETL jobs
- **Problem:** Core fact tables approaching 100M+ rows. Multi-table joins and cross-table analytics hitting single-node MySQL performance limits.

### New Implementation (QA, not yet in PROD)
- **MySQL PROD** → S3 (parquet, incremental CDC) → **Redshift raw** → **dbt mart models** → **hist archive tables**
- Airflow orchestrates 15-min incremental cycles + daily monitoring + daily VACUUM
- Three fact tables: `ecs_order_info`, `uni_tracking_info`, `uni_tracking_spath`
- Dimension tables: **not yet designed (gap)**

---

## 2. Is the New Design the Best Solution?

### Overall Verdict: **Yes — architecturally correct and significantly better than Kettle**

The core technology choice is right. MySQL single-node at 100M+ rows is the exact problem Redshift MPP columnar storage was built for. DISTKEY co-location, zone map skipping, and columnar I/O will give 10–50x improvement on analytical workloads.

### What the Design Gets Right

| Aspect | Assessment |
|--------|-----------|
| **Technology choice** | Redshift MPP for 100M+ row analytical workloads — correct |
| **DISTKEY(order_id) on all 3 marts** | 3-way JOIN `uti ↔ ecs ↔ uts` always co-located, zero cross-node shuffle |
| **delete+insert by order_id** | Mirrors Kettle DELETE logic exactly — correct for any order lifecycle |
| **mart_uti: no incremental_predicates** | Avoids 20-day duplicate bug that existed in initial dbt migration |
| **mart_ecs: order_id-driven retention** | Prevents silent drop of long-lifecycle orders on time-based trim |
| **mart_uts: pure time-based retention** | Prevents unbounded growth at 195M spath events/month |
| **Exceptions table safety net** | Rows with open exceptions are NEVER trimmed — no silent data loss |
| **Automated hist splits** | Replaces manual DBA intervention in Kettle — predictable 1 Jan / 1 Jul cadence |
| **Post-hook order** | Reactivation cleanup → archive → safety check → trim — mandatory, correct |
| **DQ monitoring** | Daily tests + email alerts — Kettle had none of this |
| **NULL-safe trim** | DELETE USING LEFT JOIN — fixes a latent Kettle NOT IN NULL bug at 90M+ rows |
| **S3 replay buffer** | Parquet files re-processable if Redshift COPY fails — Kettle had no buffer |

### Functional Gaps vs Kettle

| Gap | Kettle behaviour | New pipeline | Action |
|-----|-----------------|--------------|--------|
| **ECS co-extraction** | Refreshed ECS row on every UTI update via LEFT JOIN | ECS only extracted when new orders appear (add_time-based) | Run `compare_pipelines.py --table ecs` before PROD cutover |
| **spath is_updated=1** | Full DELETE+INSERT per order_id for spath mutations | Append-only extraction | Run `compare_pipelines.py --table uts` before PROD cutover |
| **Dimension tables** | Synced manually or via Kettle | Not yet designed | Design and build before PROD |

---

## 3. Column Coverage Analysis

### Finding: **100% Complete**

All three mart models use `SELECT * EXCLUDE(_rn)` from the raw tables.
The extraction pipeline uses `SELECT *` from MySQL source (no column filtering in `order_tracking_hybrid_dbt_pipeline.yml`).

**Flow:** `kuaisong.{table}` → S3 parquet (all columns) → `settlement_public.*_raw` (all columns) → `settlement_ods.mart_*` (all columns)

Every column in the kuaisong production MySQL tables flows through to Redshift marts, including all indexed columns: `state`, `code`, `staff_id`, `type`, `order_status`, `shipping_status`, `tno`, `partner_id`, etc.

**One caveat to verify:** If uniods has columns that were manually added to the DW and don't exist in kuaisong prod (e.g., computed or ETL-derived columns), those will NOT be in Redshift. Unlikely but worth confirming with the DBA.

---

## 4. SORTKEY / DISTKEY Coverage vs uniods Indexes

### 4.1 mart_ecs_order_info — Current SORTKEY: `(partner_id, add_time, order_id)`

**uniods `dw_ecs_order_info` indexes:**
| MySQL Index | Columns | Query pattern |
|------------|---------|--------------|
| `partner_id` | partner_id | `WHERE partner_id = X` |
| `add_time` | add_time | `WHERE add_time BETWEEN A AND B` |
| `idx_order_parcel_type` | order_id, type | `WHERE order_id = X AND type = Y` |
| `idx_orderid` | order_id | `WHERE order_id = X` |
| `extra_order_sn` | extra_order_sn | `WHERE extra_order_sn = X` |
| `zipcode` | zipcode | `WHERE zipcode = X` |

**Coverage assessment: ✅ Excellent — no changes needed**

SORTKEY `(partner_id, add_time, order_id)` is the optimal choice:
- `WHERE partner_id = X` → zone maps skip to partner's rows (~0.3% of table)
- `WHERE partner_id = X AND add_time BETWEEN A AND B` → excellent zone map coverage for the primary analytical pattern
- `WHERE order_id = X` → DISTKEY(order_id) ensures all rows for an order are on one node

**Minor gaps (acceptable):** `zipcode`, `extra_order_sn`, `type` have no zone map coverage. These are operational point-lookups (find one specific order) — full column scan at 100M rows in Redshift columnar storage is ~1–2s, acceptable for ad-hoc queries. If they become frequent, consider a separate lookup/index table.

---

### 4.2 mart_uni_tracking_info — Current SORTKEY: `(update_time, order_id)`

**uniods `dw_uni_tracking_info` indexes:**
| MySQL Index | Columns | Query pattern |
|------------|---------|--------------|
| `update_time` | update_time | `WHERE update_time BETWEEN A AND B` |
| `idx_state_update` | **state, update_time** | `WHERE state = X AND update_time > Y` |
| `state` | state | `WHERE state = X` |
| `tno` | tno | `WHERE tno = X` (tracking number lookup) |
| `idx_orderid` | order_id | `WHERE order_id = X` |
| `second_delivery_sn` | second_delivery_sn | `WHERE second_delivery_sn = X` |

**Coverage assessment: ✅ Good — current SORTKEY is correct**

The composite `idx_state_update (state, update_time)` reveals that `WHERE state = X AND update_time > Y` is a frequent query (e.g., "all delivered orders this week", "all orders in state 3 today"). Our SORTKEY `(update_time, order_id)` doesn't include `state` in zone maps.

**Why we keep the current SORTKEY anyway:**
- Changing to `(state, update_time)` would break the incremental extraction pattern (`WHERE update_time > watermark` — the pipeline's most critical write pattern)
- `state` is a `tinyint` with ~10–20 distinct values. In Redshift columnar storage, filtering `state = X` reads only the state column blocks across 30M rows — this is fast (~1–2s) even without zone maps
- The incremental pipeline writes benefit more from `(update_time, order_id)` than analytics benefit from zone maps on state

**Minor gaps (acceptable):** `tno` (tracking number) and `second_delivery_sn` have no zone map coverage — these are operational point-lookups, acceptable as full column scans.

---

### 4.3 mart_uni_tracking_spath — Current SORTKEY: `(pathTime, order_id)`

**uniods `dw_uni_tracking_spath` indexes:**
| MySQL Index | Columns | Query pattern |
|------------|---------|--------------|
| `pathTime` | pathTime | `WHERE pathTime BETWEEN A AND B` |
| `order_id` | order_id | `WHERE order_id = X` |
| `code` | code | `WHERE code = X` (event type filter) |
| **`idx_order_status_time_driver`** | **order_id, code, pathTime, staff_id** | `WHERE order_id=X AND code=Y AND pathTime BETWEEN A AND B` |

**Coverage assessment: ⚠️ Gap identified — SORTKEY change recommended**

The composite `idx_order_status_time_driver (order_id, code, pathTime, staff_id)` is the most telling index in all three tables. It reveals that **event type (`code`) is a primary filter dimension alongside time**. Common analytical queries:
- "All delivery attempts (`code=5`) in the last 30 days"
- "All failed scans (`code=8`) for a region this week"
- "All RTS events (`code=12`) in January"
- Driver performance: "events by `staff_id` + `code` in a period"

**With current SORTKEY `(pathTime, order_id)` at 1.17B rows:**
A query like `WHERE code = 5 AND pathTime > X` uses zone maps on `pathTime` (good) but then scans all ~195M events per month looking for the specific code. At steady state that's scanning hundreds of millions of rows to find a subset.

### ⚡ Recommended Change: mart_uts SORTKEY → `(pathTime, code, order_id)`

```sql
-- Current (in mart_uni_tracking_spath.sql)
sort=['pathTime', 'order_id']

-- Recommended
sort=['pathTime', 'code', 'order_id']
```

**Why this is strictly better:**
- Zone maps on `pathTime` — skip all blocks before query window ✅ (unchanged)
- Zone maps on `code` within time window — skip irrelevant event types ✅ (new gain)
- `order_id` still in sort key for point lookups ✅ (unchanged)
- Incremental pipeline queries by `pathTime > watermark` — first sort column unchanged ✅
- DISTKEY(order_id) unchanged — JOIN co-location preserved ✅

**Impact at scale:** At 1.17B rows steady state, `WHERE code=5 AND pathTime > X` would go from scanning ~195M rows/month to scanning only the ~X% of rows with that code within the time window. Delivery events (`code=5`) may be 10–15% of all spath rows — that's a 7–10x speedup on the most common analytical query.

**How to apply:** This requires recreating the Redshift table (ALTER SORTKEY is not supported for compound sort keys in Redshift without a full VACUUM REINDEX). Since the table is empty in QA it's a simple change now. On PROD, recreate the table before the initial backfill load.

---

## 5. Backfill Plan

### Scale Reality (from uniods AUTO_INCREMENT values)
| Table | Approx rows | Notes |
|-------|------------|-------|
| dw_ecs_order_info | ~100M | Order creation records |
| dw_uni_tracking_info | ~100M | Latest tracking state per order |
| dw_uni_tracking_spath | ~487M | Every scan event — largest table |
| **Total** | **~687M rows** | Needs dedicated backfill DAG |

The 15-min incremental DAG **cannot** handle this — at 100K rows/cycle it would take months to catch up.

### Critical Decisions Before Backfill

**1. How far back?**
- Prod MySQL only has 10–15 days of data
- uniods has full historical — if you want history older than 10–15 days, you must extract from uniods
- Decision needed: extract from kuaisong (prod) + uniods (historical), or kuaisong only?

**2. Full history or 6-month window?**
- Backfilling everything loads data into mart tables — rows older than 6 months will be immediately archived to hist tables on the first dbt run
- This is correct behaviour but means hist tables must exist and be correctly configured before the first backfill dbt run
- For spath at 487M rows with 6-month retention, only the most recent 6 months (~195M rows) stay in mart_uts — the rest goes to hist_uts_*

**3. Data source for backfill**

Two options depending on business requirement:
```
Option A: kuaisong (prod) only — last 10-15 days
  Simple. Use existing extraction pipeline logic. No uniods access needed.
  Result: Redshift starts with 10-15 days of data, grows forward from there.

Option B: uniods (DW) + kuaisong — full historical
  Complex. Need to extract from uniods MySQL first, then hand off to kuaisong incremental.
  Result: Full history in Redshift from day 1.
  Recommended if the business needs historical analysis.
```

### Recommended Backfill Architecture (Option B — full history)

```
Phase 1 — Historical load from uniods (dedicated backfill DAG)
│
├── Chunk by time window: 1 month per Airflow task
├── 3 tables run in parallel (ecs, uti, uts)
├── Each chunk: MySQL uniods extract → S3 parquet → Redshift COPY
├── Order: ecs first → uti → uts (preserves mart dependency ordering)
├── Estimated wall clock at PROD speed:
│     ecs (~100M):  ~4–6 hours chunked
│     uti (~100M):  ~4–6 hours chunked
│     uts (~487M):  ~12–18 hours chunked
│     Total parallel: ~12–18 hours
└── Checkpoint: watermark saved per chunk — restartable on failure

Phase 2 — dbt initial mart run (after all raw tables loaded)
│
├── Run dbt mart models once (full load mode — is_incremental() = False first run)
├── mart_uti: all ~100M rows processed, 6-month retention fires, older rows → hist_uti
├── mart_ecs: all ~100M rows processed, older-than-active orders → hist_ecs
├── mart_uts: all ~487M rows processed, 6-month retention fires, older rows → hist_uts
└── Pre-requisite: ALL hist tables must exist before this runs

Phase 3 — Validation gate (before going live)
│
├── compare_pipelines.py --table ecs
├── compare_pipelines.py --table uti
├── compare_pipelines.py --table uts
└── Pass criteria: <0.1% row count gap, <1% value mismatch on key columns

Phase 4 — Watermark handoff
│
├── Record MySQL MAX(update_time) / MAX(add_time) / MAX(pathTime) at BACKFILL START
├── After dbt run completes, set S3 watermarks to these captured values
└── Start 15-min incremental DAG — picks up exactly where backfill left off

Phase 5 — Live validation (first 7 days)
│
└── Run compare_pipelines.py daily to catch any extraction gaps
```

### Backfill DAG Design Requirements (not yet built)

The backfill DAG needs to:
1. Accept `start_date` and `end_date` parameters (one month at a time)
2. Connect to uniods MySQL (not kuaisong) — different host/credentials
3. No 100K row LIMIT cap — use larger batches (500K–1M) for speed
4. Write to the same S3 prefix as the incremental DAG (same raw table target)
5. After all chunks complete, trigger a one-time full dbt run
6. Checkpoint each chunk completion — restartable without re-extracting completed chunks

---

## 6. Dimension Tables (Not Yet Designed)

The migration goal explicitly includes "key dimension tables." Currently **zero dimension tables** are designed or implemented. Before PROD, identify and design:

| Likely dimensions | Sync strategy | Notes |
|-------------------|--------------|-------|
| Partners / partner_id | Full replace daily | Low cardinality, rarely changes |
| Drivers / staff_id | Full replace daily | Medium cardinality |
| Warehouses | Full replace daily | Low cardinality |
| Status code mappings | Full replace (or static) | Look up state, code, order_status labels |
| Geographic lookup (province, city) | Full replace | Used in ecs for address queries |

Dimension tables don't need CDC watermarks — a simple `TRUNCATE + INSERT` nightly is correct. They need DISTKEY(join_key) to ensure co-location with the fact tables they join to.

---

## 7. Open Issues (from pipeline_review_issues.md)

Issues that should be resolved before PROD cutover:

| Issue | Description | Priority |
|-------|-------------|----------|
| **CDC upper bound** | `AND update_time < end_time` not enforced in SQL — 5-min buffer computed but never applied | **High** |
| **Issue 6** | mart_uts archive NOT IN guard too broad (order_id only, should be order_id+pathTime) | **High** |
| **Issue 7** | mart_ecs missing safety check before trim (no ARCHIVE_ROUTING_GAP guard like mart_uti has) | **High** |
| **Issue 8** | 2026_h1 hist period not defined — deadline ~Aug 2026 | High (scheduled) |
| **Issue 9** | NOT IN with potential NULLs in trim steps | Medium |
| **mart_uts SORTKEY** | Change to `(pathTime, code, order_id)` — see Section 4.3 | **High** |
| **Dimension tables** | None designed yet — required for full migration | **High** |
| **Backfill DAG** | Not built — required for initial PROD load | **High** |
| **ECS co-extraction gap** | Verify with compare_pipelines.py --table ecs | High |
| **spath is_updated=1 gap** | Verify with compare_pipelines.py --table uts | High |
| **Issue 4** | VACUUM SORT unconditional — blocked on DBA svv_table_info permission | Low |
| **Issue 5** | ECS post-creation field changes not captured | Low / monitor |
| **Issue 10** | stg_ecs comment says "merge" | Cosmetic |
| **Issue 11** | Monitoring DAG docstring says 2am, schedule is 3am | Cosmetic |

---

## 8. Production Readiness Checklist

### Must-Do Before PROD Cutover

- [ ] **Fix mart_uts SORTKEY** → `(pathTime, code, order_id)` in mart_uni_tracking_spath.sql
- [ ] **Fix CDC upper bound** → add `AND update_time < end_time` to extraction query
- [ ] **Fix Issue 6** → mart_uts archive NOT IN guard (order_id + pathTime)
- [ ] **Fix Issue 7** → mart_ecs safety check before trim
- [ ] **Build backfill DAG** → chunked monthly extract from uniods + watermark handoff
- [ ] **Design dimension tables** → partners, drivers, warehouses, status lookups
- [ ] **Run compare_pipelines.py --table ecs** → quantify and decide on ECS co-extraction gap
- [ ] **Run compare_pipelines.py --table uts** → quantify and decide on spath is_updated=1 gap
- [ ] **Create all hist tables on PROD** → 2025_h2 and 2026_h1 sets (DDL in prerequisites.md)
- [ ] **Create order_tracking_exceptions on PROD** → DDL in prerequisites.md
- [ ] **Validate backfill** → row count + value spot-check via compare_pipelines.py
- [ ] **Performance test on PROD cluster** → cycle time < 10 min, VACUUM < 2 hours
- [ ] **First week monitoring** → daily compare_pipelines.py for 7 days post-cutover

### Already Done ✅
- [x] 15-min incremental DAG running on QA
- [x] Daily monitoring DAG running on QA (4 DQ tests + email alerts)
- [x] Daily VACUUM DAG running on QA
- [x] SSH tunnel timeout fix (`timeout 300` on extraction tasks)
- [x] Redshift correlated subquery fixes in monitoring DQ checks
- [x] Alert on new exceptions only (no alert fatigue from historical exceptions)
- [x] DISTKEY(order_id) on all 3 mart tables
- [x] mart_ecs SORTKEY `(partner_id, add_time, order_id)` — optimal
- [x] mart_uti SORTKEY `(update_time, order_id)` — correct for write pattern
- [x] delete+insert by order_id (no incremental_predicates on mart_uti and mart_ecs)
- [x] Exceptions table safety net
- [x] Automated 6-month hist splits with archive idempotency
- [x] NULL-safe DELETE USING LEFT JOIN on mart_ecs trim

---

## 9. Performance Comparison: New vs Legacy

| Dimension | Legacy Kettle + MySQL DW | New Airflow + Redshift | Notes |
|-----------|--------------------------|----------------------|-------|
| **Query speed (100M+ rows)** | MySQL single-node, minutes | Redshift MPP, seconds | 10–50x improvement expected |
| **3-way JOIN performance** | Full table scans on MySQL | DISTKEY co-location, local joins | No cross-node shuffle |
| **Partner-scoped queries** | Full table scan or index seek | SORTKEY zone maps, ~0.3% of rows | Key improvement |
| **Event-type queries (spath)** | Composite index on (order_id, code, pathTime) | Zone maps on (pathTime, code) after fix | Requires SORTKEY fix |
| **Historical data access** | uniods (current hist only) | mart + hist tables (full history) | Unified access |
| **Retention maintenance** | Manual DBA intervention | Automated post_hooks every cycle | Major improvement |
| **Data freshness** | Near-real-time (Kettle polling) | 15-min incremental | Acceptable for analytics |
| **Fault tolerance** | No replay buffer | S3 parquet replay buffer | Major improvement |
| **NULL safety** | NOT IN (latent bug) | DELETE USING LEFT JOIN | Correctness fix |
| **Observability** | None | DQ tests + exceptions table + email alerts | Major improvement |

---

## 10. Summary

**The new design is the right architecture for this migration.** The core decisions — Redshift MPP, DISTKEY co-location, delete+insert by order_id, exceptions safety net, automated hist splits — are all correct and will perform significantly better than the legacy Kettle+MySQL DW for the stated goals.

**Three things need immediate attention:**
1. **SORTKEY on mart_uts** → change to `(pathTime, code, order_id)` before backfill (easy fix, large analytical impact)
2. **Backfill DAG** → the biggest missing piece; needed before PROD cutover
3. **Dimension tables** → not designed at all; needed for full analytics use case

Once these are addressed and the pre-PROD checklist is complete, the pipeline will be a genuine improvement over Kettle in every meaningful dimension.
