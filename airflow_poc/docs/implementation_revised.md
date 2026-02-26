# Order Tracking Pipeline — Revised Implementation Design

**Created**: 2026-02-24
**Status**: Design approved, pending implementation

---

## 1. Problem Statement

- `stg_uni_tracking_info` has a duplicate bug for long-lifecycle orders (>20 days)
- Data is spread across staging and an unbuilt mart — no single clean queryable layer
- Staging was serving two conflicting purposes: fast dedup working layer AND long-term history

---

## 2. Core Fix (applies to both options)

**Root cause**: 20-day `incremental_predicates` window causes duplicates for orders whose
previous row is older than 20 days.

**Fix**: remove `incremental_predicates`. DELETE becomes `WHERE order_id IN (batch)` —
always correct regardless of order lifecycle length. This mirrors exactly what the old
Kettle pipeline did (`WHERE a.order_id = b.t_order_id` — no time window, ever).

---

## 3. Two Architecture Options

### Option A — With staging layer (conservative)

```
Raw (2-day buffer)
    ↓
stg_uni_tracking_info  [internal, not user-facing]
  - DELETE by order_id (no incremental_predicates)
  - Retention post_hook: 20-day trim keeps table ~10-14M rows
  - Purpose: dedup working buffer
    ↓
Mart  [user-facing, all queries here]
  mart_uni_tracking_info  — 6-month, one row per order
  mart_ecs_order_info     — 6-month, one row per order
  mart_uni_tracking_spath — 6-month, full scan history

stg_ecs and stg_uts: removed (no dedup needed, raw → mart directly)
```

### Option B — No staging layer (simpler)

```
Raw (2-day buffer)
    ↓
Mart  [user-facing, all queries here]
  mart_uni_tracking_info  — 6-month, one row per order
  mart_ecs_order_info     — 6-month, one row per order
  mart_uni_tracking_spath — 6-month, full scan history

No staging layer at all. mart_uti does the dedup inline (same ranked CTE).
```

---

## 4. Option A — With Staging

### Why stg_uti still exists in Option A

stg_uti with retention post_hook keeps the table permanently at ~10–14M rows.
This guarantees constant DELETE performance as the mart grows. stg_uti feeds
operational dedup; mart accumulates the long-term history.

### stg_uni_tracking_info config

```python
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['update_time', 'order_id'],
        -- no incremental_predicates: DELETE by order_id, always correct
        post_hook=[
            "DELETE FROM {{ this }} WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM {{ this }})"
        ]
    )
}}
```

Full rationale: `stg_uti_dedup_final.md`

### mart_uni_tracking_info config (Option A)

Reads from raw directly (same source as stg_uti, independently).

```python
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['update_time', 'order_id'],
        -- no incremental_predicates: DELETE by order_id
        -- table grows to 90M rows at 6 months but DELETE reads only order_id column
    )
}}

with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    {% if is_incremental() %}
    where update_time > (select coalesce(max(update_time), 0) - 1800 from {{ this }})
    {% endif %}
),
ranked as (
    select *,
        row_number() over (partition by order_id order by update_time desc) as _rn
    from filtered
)
select * exclude(_rn) from ranked where _rn = 1
```

### What stg_uti is in Option A

- 20-day active orders only (completed orders age out via retention trim)
- Internal pipeline artifact — no user queries
- Constant size, constant performance

### Trade-off

| | Option A |
|---|---|
| Models | 4 (stg_uti + 3 mart) |
| stg_uti purpose | Dedup buffer, guarantees mart DELETE is always on a small batch |
| stg_ecs / stg_uts | Removed |
| Risk | Slightly more complexity (two uti tables) |

---

## 5. Option B — No Staging

### What changes

stg_uti is removed entirely. mart_uti does everything inline:
- Same `ranked` CTE dedup
- Same DELETE by order_id
- Same INSERT
- No retention trim — mart keeps full 6-month history

### mart_uni_tracking_info config (Option B)

Identical to Option A mart config. The dedup logic is the same — just lives in the mart
model directly instead of in a staging model first.

```python
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['update_time', 'order_id'],
    )
}}

with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    {% if is_incremental() %}
    where update_time > (select coalesce(max(update_time), 0) - 1800 from {{ this }})
    {% endif %}
),
ranked as (
    select *,
        row_number() over (partition by order_id order by update_time desc) as _rn
    from filtered
)
select * exclude(_rn) from ranked where _rn = 1
```

### Why Option B likely works

DELETE by order_id on a large mart table reads **only the order_id column** (Redshift columnar).
DISTKEY(order_id) means the hash join is local per node — no cross-node shuffle.

At 90M rows (6 months):
- Read order_id column: 90M × 8 bytes = 720MB → ~180MB compressed
- DISTKEY across 3 nodes: ~60MB per node
- Sub-second I/O — overhead (compilation, zone map updates, INSERT) dominates, not the scan

The 84–123s measured for stg_uti at 11.9M rows is dominated by fixed overhead, not scan cost.
Scaling from 14M to 90M rows likely adds seconds, not minutes.

### What Option B is equivalent to

The old Kettle pipeline exactly:
```
Extract uti by update_time → temp table
DELETE FROM dw_uni_tracking_info WHERE order_id = temp.order_id   ← no time window
INSERT FROM temp table
```

Same pattern, Redshift instead of MySQL. MySQL used B-tree indexes for fast order_id lookup;
Redshift uses columnar + DISTKEY — different mechanism, same outcome.

### Trade-off

| | Option B |
|---|---|
| Models | 3 (mart only) |
| stg_uti | Gone |
| stg_ecs / stg_uts | Gone |
| Risk | DELETE performance at 90M rows — needs production measurement to confirm |

---

## 6. Shared Mart Models (both options)

### mart_ecs_order_info

```python
{{
    config(
        materialized='incremental',
        dist='order_id',
        sort=['partner_id', 'add_time', 'order_id'],
    )
}}

select *
from settlement_public.ecs_order_info_raw
{% if is_incremental() %}
where add_time > (select coalesce(max(add_time), 0) - 1800 from {{ this }})
{% endif %}
```

Append only — each order_id written once at creation. No DELETE needed.

**SORTKEY rationale**: primary query pattern always filters `WHERE partner_id IN (...)`.
With 300+ partners, leading on `partner_id` cuts the scanned rows to ~0.3% before the JOIN.
`add_time` as second key supports time-range queries within a partner.
Trade-off: rows arrive in `add_time` order not `partner_id` order → periodic `VACUUM SORT`
needed to maintain zone map effectiveness.

### mart_uni_tracking_spath

```python
{{
    config(
        materialized='incremental',
        dist='order_id',
        sort=['pathTime', 'order_id'],
    )
}}

select *
from settlement_public.uni_tracking_spath_raw
{% if is_incremental() %}
where pathTime > (select coalesce(max(pathTime), 0) - 1800 from {{ this }})
{% endif %}
```

Append only — new scan events only. No DELETE needed.

**SORTKEY rationale**: primary query pattern filters `WHERE pathtime >= X`. Leading on
`pathTime` allows zone maps to prune all blocks before the query date — the most impactful
filter in the primary query pattern.

---

## 7. Primary Query Pattern

The most frequent query against the old uniods ODS:

```sql
SELECT ...
FROM   dw_ecs_order_info        eoi
JOIN   dw_uni_tracking_spath    uts ON eoi.order_id = uts.order_id
JOIN   dw_uni_tracking_info     uti ON eoi.order_id = uti.order_id
WHERE  uts.code      = 200
AND    eoi.partner_id IN (382)
AND    uts.pathtime  >= unix_timestamp('2025-07-17 00:00:00')
```

### How our mart physical design serves this query

| Step | Operation | Mechanism | Why fast |
|---|---|---|---|
| 1 | Scan mart_uts WHERE pathTime >= X | SORTKEY(pathTime) zone maps | Skips all blocks before the date — most blocks pruned |
| 2 | Filter code = 200 on remaining rows | Column scan | Small set after pathTime prune |
| 3 | JOIN mart_ecs WHERE partner_id IN (382) | DISTKEY(order_id) + SORTKEY(partner_id) | Local join per node + zone maps cut ecs to ~0.3% of rows |
| 4 | JOIN mart_uti | DISTKEY(order_id) | Local join per node, no shuffle |

**DISTKEY(order_id) on all three tables is the single most important physical design
decision** — the entire query is a 3-way JOIN on order_id. Co-located joins means
zero cross-node network shuffle regardless of result set size.

### Why this query is faster on Redshift than old MySQL

- MySQL: row-store + B-tree indexes. Each row read = all columns. Index traversal overhead.
- Redshift: columnar scan reads only needed columns. Zone maps prune whole blocks.
  DISTKEY eliminates join shuffle. Purpose-built for this join+filter+aggregate pattern.

### Implication for known gaps

- `partner_id` is set at order creation and never changes → Gap 1 (ecs updates) does
  not affect this query
- `code` on spath events is immutable (a delivered scan stays delivered) → Gap 2
  (spath event updates) does not affect this query

Both gaps are likely low-impact for the primary query pattern. Confirm with
`compare_pipelines.py` before investing in fixes.

---

## 8. Performance Comparison

Based on PROD benchmarks (2026-02-23, `qa_prod_comparison.md`):

### Option A — With staging

| Model | Table size | Operation | Estimated PROD time |
|---|---|---|---|
| stg_uni_tracking_info | ~10–14M (constant) | order_id DELETE + INSERT + retention trim | ~90–130s |
| mart_uni_tracking_info | grows to 90M at 6 months | order_id DELETE + INSERT | ~30–60s |
| mart_ecs_order_info | grows over time | append only | ~10–20s |
| mart_uni_tracking_spath | grows to 360M at 6 months | append only | ~30s |
| **dbt wall time (4 threads)** | | bottleneck = stg_uti | **~130s** |
| **End-to-end** | | extraction ~94s + dbt ~130s | **~3m44s** |

### Option B — No staging

| Model | Table size | Operation | Estimated PROD time |
|---|---|---|---|
| mart_uni_tracking_info | grows to 90M at 6 months | order_id DELETE + INSERT | ~100–150s* |
| mart_ecs_order_info | grows over time | append only | ~10–20s |
| mart_uni_tracking_spath | grows to 360M at 6 months | append only | ~30s |
| **dbt wall time (4 threads)** | | bottleneck = mart_uti | **~150s** |
| **End-to-end** | | extraction ~94s + dbt ~150s | **~4m04s** |

*mart_uti at 90M rows — estimated based on columnar + DISTKEY analysis. Needs production
measurement to confirm. Both options comfortably within 15-min schedule.

---

## 9. Side-by-Side Comparison

| | Option A (with staging) | Option B (no staging) |
|---|---|---|
| Total models | 4 | 3 |
| Layers | raw → stg_uti → mart | raw → mart |
| stg_uti DELETE scan | ~10–14M rows (constant forever) | removed |
| mart_uti DELETE scan | ~10–14M rows (small batch from raw) | ~90M rows at steady state |
| Performance certainty | High — table size known, benchmarked | Needs measurement at 90M rows |
| Equivalent to old Kettle | Partial | Exact |
| Complexity | Slightly more (two uti tables) | Minimal |
| Recommended when | Performance of 90M DELETE unconfirmed | Performance confirmed on PROD |

---

## 10. Recommendation

**Start with Option B.** It is simpler, fewer models, exact equivalent of the proven old
pipeline pattern. The DELETE performance concern is based on theoretical scaling — in practice,
Redshift columnar + DISTKEY makes order_id DELETE fast even at large table sizes.

**Fallback to Option A if** mart_uti DELETE at 90M rows takes >5 min on PROD measurement.
Add stg_uti back as a 20-day dedup buffer and the problem is solved with constant performance.

---

## 11. Mart Retention and Splitting

### Retention — decide now

Retention controls how fast tables grow. Without a trim, mart_uts reaches 720M rows at
1 year. With a 6-month trim, all tables stay at constant steady-state sizes.

**Recommended: add 6-month retention post_hook to all three mart tables.**

```sql
-- mart_uti: trim orders not updated in 6 months
DELETE FROM mart_uni_tracking_info
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM mart_uni_tracking_info)

-- mart_ecs: trim orders created more than 6 months ago
DELETE FROM mart_ecs_order_info
WHERE add_time < (SELECT COALESCE(MAX(add_time), 0) - 15552000 FROM mart_ecs_order_info)

-- mart_uts: trim scan events older than 6 months
DELETE FROM mart_uni_tracking_spath
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM mart_uni_tracking_spath)
```

Steady-state table sizes with 6-month retention:

| Table | Rows at steady state |
|---|---|
| mart_uti | ~90M (one row per unique order in 6 months) |
| mart_ecs | ~36M (new orders only, ~200K/day) |
| mart_uts | ~360M (2M events/day × 180 days) |

### Splitting — plan later, not now

The old Kettle pipeline split by year: `dw_uti_2024`, `dw_uti_2025`.
With 6-month retention trims in place, table sizes are bounded and splitting is not needed
for the foreseeable future.

**When splitting becomes relevant**: if VACUUM cost or query latency degrades at steady-state
size, or if the business requires retention >6 months (e.g., 2 years for audit).

**Pattern when needed** (future work):
- Yearly tables: `mart_uti_2025`, `mart_uti_2026`
- Union view: `mart_uni_tracking_info` as a view over all yearly tables
- No pipeline logic changes — only table routing changes

**Trigger for this work**: if any mart table approaches 500M rows or VACUUM takes >30 min.
With 6-month retention, mart_uts at ~360M rows is the first candidate to watch.

---

## 12. DAG Flow (both options)

```
Extraction (parallel, 3 tasks)
  extract_ecs  →  ecs_order_info_raw
  extract_uti  →  uni_tracking_info_raw
  extract_uts  →  uni_tracking_spath_raw
        ↓
Option A — dbt run (4 threads):
  stg_uni_tracking_info  (dedup buffer)
  mart_uni_tracking_info  \
  mart_ecs_order_info      ├── all run in parallel
  mart_uni_tracking_spath /

Option B — dbt run (3 threads):
  mart_uni_tracking_info  \
  mart_ecs_order_info      ├── all run in parallel
  mart_uni_tracking_spath /
```

No Airflow gate tasks. No spath gate. No uti_cleanup_task.

---

## 13. Known Gaps vs Old Pipeline

### Gap 1: ECS post-creation updates

Old pipeline refreshed ecs on every uti update via JOIN. Our pipeline cannot detect ecs
changes — no `update_time` column in `ecs_order_info`.

Primary query filters on `partner_id` (set at creation, immutable) — gap likely does not
affect the main query pattern.

**Action before fixing**: run `compare_pipelines.py --table ecs` on a recent date.
If mismatches are low → accept as known limitation. If high → design extraction-level fix.

### Gap 2: spath event updates

Old pipeline did full DELETE+INSERT per order for spath (handles `is_updated = 1`).
Our pipeline is append-only — updated spath events not recaptured.

Primary query filters on `code` (immutable once scanned) — gap likely does not affect
the main query pattern.

**Action before fixing**: run `compare_pipelines.py --table uts` on a recent date.
If mismatches are low → accept. If high → change extraction watermark to `time_modified`
and change mart_uts to delete+insert by composite key (`order_id + traceSeq + pathTime`).

---

## 14. Implementation Steps

### Phase 1 — Core fix (immediate)
- [ ] If Option A: update `stg_uni_tracking_info.sql` (remove `incremental_predicates`,
      remove both post_hooks, add retention post_hook)
- [ ] If Option B: delete `stg_uni_tracking_info.sql` entirely
- [ ] Remove `stg_ecs_order_info.sql` and `stg_uni_tracking_spath.sql`
- [ ] Test on QA

### Phase 2 — Build mart models
- [ ] Create `mart_uni_tracking_info.sql` (with 6-month retention post_hook)
- [ ] Create `mart_ecs_order_info.sql` (with 6-month retention post_hook)
- [ ] Create `mart_uni_tracking_spath.sql` (with 6-month retention post_hook)
- [ ] Update DAG to include mart models

### Phase 3 — Validate performance
- [ ] Measure mart_uti DELETE time on PROD at current table size
- [ ] Project to 90M rows — confirm Option B is viable or fall back to Option A

### Phase 4 — Validate against old pipeline
- [ ] Run `compare_pipelines.py --table ecs` — measure Gap 1 severity
- [ ] Run `compare_pipelines.py --table uts` — measure Gap 2 severity
- [ ] Decide on fixes based on results

### Phase 5 — Migration
- [ ] Snapshot existing staging data if historical data needs preserving
- [ ] Seed mart tables from existing staging before switching user queries
- [ ] Switch all user queries from staging → mart
- [ ] Confirm stg_* tables can be dropped
