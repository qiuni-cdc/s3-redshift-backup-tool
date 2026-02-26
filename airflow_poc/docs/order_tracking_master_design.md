# Order Tracking Pipeline — Master Design Document

**Created**: 2026-02-25
**Branch**: jasleen_qa
**Supersedes**: `implementation_revised.md`, `stg_uti_dedup_final.md`, `uti_dedup_design.md`

---

## Table of Contents

1. [Legacy Pipeline (Kettle)](#1-legacy-pipeline-kettle)
2. [Problems with the dbt Migration](#2-problems-with-the-dbt-migration)
3. [Design Evolution — Approaches Considered](#3-design-evolution--approaches-considered)
4. [Final Architecture](#4-final-architecture)
5. [Table Physical Design](#5-table-physical-design)
6. [Retention Strategy](#6-retention-strategy)
7. [Historical Layer](#7-historical-layer)
8. [Mart Splitting — Why Yearly and Not 6-Month](#8-mart-splitting--why-yearly-and-not-6-month)
9. [Order_id-Driven Retention — Keeping All Three Tables Aligned](#9-order_id-driven-retention--keeping-all-three-tables-aligned)
10. [Post-Hook Execution Order](#10-post-hook-execution-order)
11. [Primary Query Patterns](#11-primary-query-patterns)
12. [Data Quality Tests](#12-data-quality-tests)
13. [Known Risks and Edge Cases](#13-known-risks-and-edge-cases)
14. [Performance Analysis](#14-performance-analysis)
15. [DAG Flow](#15-dag-flow)
16. [Implementation Phases](#16-implementation-phases)

---

## 1. Legacy Pipeline (Kettle)

### Architecture

```
MySQL Source
    ↓
get_orders_for_del.ktr
  → Extract uti by update_time window + LEFT JOIN ecs (full table)
  → Write to tmp_dw_order_for_del

concat.ktr
  → DELETE FROM dw_ecs_order_info     WHERE order_id IN (tmp)
  → DELETE FROM dw_uni_tracking_info  WHERE order_id IN (tmp)
  → INSERT INTO dw_ecs_order_info     FROM tmp
  → INSERT INTO dw_uni_tracking_info  FROM tmp

spath_tmp.ktr
  → UNION ALL uti + spath order_ids from tmp
  → Pull ALL spath rows for those order_ids

spath_concat.ktr
  → DELETE FROM dw_uni_tracking_spath WHERE order_id IN (tmp)
  → INSERT INTO dw_uni_tracking_spath FROM tmp
```

### Key properties of the legacy design

| Property | Detail |
|---|---|
| DELETE strategy | `WHERE order_id IN (batch)` — no time window, ever |
| Dedup mechanism | DELETE the old row first, then INSERT the new row |
| spath handling | Full DELETE + full INSERT per order_id (handles is_updated = 1) |
| Table splits | Manual, ad-hoc: named by UTI update_time range, e.g. `dw_ecs_order_info_archived_20220909_20240501`. No fixed cadence — split when table size became unmanageable. All three tables split together by the same UTI update_time boundary. |
| ECS refresh | Co-extracted on every uti update via JOIN — always in sync |

### Why it worked

DELETE by `order_id` is always correct regardless of how old the previous row is.
No time window means no lifecycle length assumption. An order active for 3 years
is handled identically to an order active for 3 days.

Yearly table splits were needed because **MySQL is row-based and single-node** —
tables grow unboundedly and B-tree index traversal degrades with volume.

---

## 2. Problems with the dbt Migration

### Problem 1 — Duplicate bug in stg_uni_tracking_info

The dbt implementation replaced the order_id DELETE with a time-windowed DELETE using
`incremental_predicates`:

```python
incremental_predicates=[
    this ~ ".update_time > (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM " ~ this ~ ")"
]
```

This restricts the DELETE to the last 20 days. Any order whose previous row is older
than 20 days will not be deleted — and after INSERT, staging has two rows for that order_id.

```
Order X active since day 0.
Old row in staging: update_time = day 62.
Today (day 90): new update arrives.

DELETE window covers day 70–90 only → day 62 row NOT deleted.
INSERT adds new row (update_time = day 89).

Staging now has:
  row 1: update_time = day 62  ← stale duplicate
  row 2: update_time = day 89  ← correct new row
```

**Confirmed data**: ~20K orders past 20 days across 32 partners. Max confirmed lifecycle: ~90 days.

### Problem 2 — Why incremental_predicates was added

It was added for **performance**: without it, the DELETE scans the full staging table,
which grows unboundedly. The performance fix introduced a correctness bug.

### Problem 3 — Data spread across layers

Staging was serving two conflicting purposes:
- Fast dedup working layer (needs to be small)
- Long-term history (needs to be large)

User queries were hitting staging directly — a table never designed for that purpose.
No clean, stable mart layer existed.

---

## 3. Design Evolution — Approaches Considered

### Approach 1 — spath gate + uti_cleanup_task (first proposal)

**What it was**: An Airflow conditional gate — query stg_spath after each dbt run.
If any order in the batch has spath events older than 20 days, open the gate and run
a separate cleanup task that deletes stale duplicates in the 20-day to 6-month band.

```
dbt_run → spath_gate_task → uti_cleanup_task (conditional)
```

**Why rejected**:
- Required Airflow XCom, conditional task logic, two new Python callables
- Gate query scans stg_spath every cycle
- Breaks if stg_spath retention is shortened below 20 days
- Higher operational complexity for the same correctness outcome
- The root cause (incremental_predicates time window) was not fixed — it was worked around

---

### Approach 2 — Two-band post_hooks

**What it was**: Two GROUP BY scans after each dbt run, covering 20–90 day and
90-day to 6-month bands, to find and delete duplicate rows in those bands.

**Why rejected**:
- Same correctness outcome as approach 1
- More code, higher I/O (GROUP BY on full staging table each cycle)
- Harder to reason about
- Root cause still not fixed

---

### Approach 3 — Wide incremental_predicates (100-day window)

**What it was**: Widen the time window from 20 days to 100 days to cover all confirmed
order lifecycles (max 90 days).

**Why rejected**:
- DELETE scans ~50M rows at steady state → projected ~315s per cycle
- Still theoretically wrong for orders dormant >100 days
- Table still grows unboundedly

---

### Approach 4 — incremental_predicates (20-day) + retention post_hook

**What it was**: Keep the 20-day DELETE window but add a retention post_hook that
trims the table to 20 days after every cycle — keeping the table permanently compact.

**Why rejected on further review**:
Once the retention post_hook keeps the table at 20-day size, all rows in the table
are already within the 20-day window. Zone maps from `incremental_predicates` prune
nothing — all rows are already within the boundary. The time window adds complexity
with zero performance benefit, and residual correctness risk remains.

---

### Approach 5 — No incremental_predicates + retention post_hook **(CHOSEN for stg_uti)**

**What it is**: Remove `incremental_predicates` entirely. DELETE becomes
`WHERE order_id IN (batch)` — always correct, no time window assumption.
Add retention post_hook to keep the table compact for performance.

```python
config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    dist='order_id',
    sort=['update_time', 'order_id'],
    # No incremental_predicates — DELETE is: WHERE order_id IN (batch)
    post_hook=[
        "DELETE FROM {{ this }} WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM {{ this }})"
    ]
)
```

**Why this works**:
- Correctness: DELETE by order_id catches the old row regardless of how old it is
- Performance: retention post_hook keeps the table at ~10–14M rows permanently
- These two concerns are solved by two separate mechanisms — no tension between them
- Mirrors exactly what the old Kettle pipeline did (DELETE by order_id, no time window)

**Cycle flow**:
```
Step 1 — DELETE:    WHERE order_id IN (batch)    → removes old row for every order in batch
Step 2 — INSERT:    new rows for the batch       → exactly one row per order_id
Step 3 — POST_HOOK: WHERE update_time < MAX - 20 days → trims completed/dormant orders
```

No duplicate at any point. ✓

---

### Approach 6 — Remove stg_uti entirely, mart_uti reads from raw directly **(FINAL CHOICE)**

**What it is**: Skip the staging layer. mart_uti does the same dedup inline — identical
ranked CTE and DELETE+INSERT logic, just living in the mart model instead of staging.

**Why this is better**:
- Mirrors old Kettle exactly — no intermediate layer between raw and final tables
- Data lives in one place — no staging + mart split for the same data
- Fewer models (3 instead of 4)
- Simpler to reason about, maintain, and query

**Fallback**: If DELETE at 90M rows proves too slow on PROD measurement, reintroduce
stg_uti as a 20-day dedup buffer (Option A in `implementation_revised.md`). stg_uti feeds
mart_uti, keeping mart_uti DELETE on a small batch. Table size guaranteed constant.

---

### Approach 7 — mart layer with independent time-based retention

**What it was**: Each mart table trims by its own timestamp:
- mart_ecs: `WHERE add_time < MAX - 6 months`
- mart_uts: `WHERE pathTime < MAX - 6 months`
- mart_uti: `WHERE update_time < MAX - 6 months`

**Why rejected**:
A long-lifecycle order (created Jan 2024, still active Aug 2025) would be:
- Trimmed from mart_ecs (add_time = Jan 2024, > 6 months ago) ✗
- Trimmed from mart_uts (all spath events from early 2024, > 6 months ago) ✗
- Still in mart_uti (update_time = Aug 2025, within 6 months) ✓

The 3-way JOIN on order_id breaks — an active order is missing from ecs and uts.

---

### Approach 8 — Order_id-driven retention, mart_uti as anchor **(FINAL CHOICE)**

**What it is**: mart_uti determines what is "active". mart_ecs and mart_uts trim
based on order_id membership in mart_uti, not their own timestamps.

```sql
-- mart_ecs: keep rows for all orders still in mart_uti
DELETE FROM mart_ecs_order_info
WHERE order_id NOT IN (SELECT order_id FROM mart_uni_tracking_info)

-- mart_uts: trim old events only for inactive orders
DELETE FROM mart_uni_tracking_spath
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM mart_uni_tracking_spath)
  AND order_id NOT IN (SELECT order_id FROM mart_uni_tracking_info)
```

**Why this works**:
- All three active mart tables always contain the same set of order_ids
- 3-way JOIN is always consistent — no missing rows in any table
- When mart_uti trims an order, mart_ecs and mart_uts trim it simultaneously
- Orders move between active and historical layer **as a unit**

---

## 4. Final Architecture

```
MySQL Source (extraction every 15 min)
    ├── uni_tracking_info_raw
    ├── ecs_order_info_raw
    └── uni_tracking_spath_raw
            ↓
Active Mart Layer  (user-facing, 6-month rolling window)
    ├── mart_uni_tracking_info   — one row per active order, delete+insert
    ├── mart_ecs_order_info      — one row per active order, append-only
    └── mart_uni_tracking_spath  — all spath events for active orders, append-only
            ↓ (archive-before-trim, fires on retention)
Historical Layer  (permanent, append-only, yearly splits)
    ├── hist_uni_tracking_info   — last known state of dormant orders (single table → yearly splits)
    ├── hist_ecs_order_info_YYYY — ecs rows for orders archived in year YYYY
    └── hist_uni_tracking_spath_YYYY — spath events for orders archived in year YYYY
```

**No staging layer. No lookup tables. 3 active models + 3 historical tables.**

### What was eliminated and why

| Removed | Reason |
|---|---|
| stg_uni_tracking_info | Replaced by mart_uti with inline dedup — mirrors Kettle |
| stg_ecs_order_info | ECS confirmed append-only, no dedup needed, raw → mart directly |
| stg_uni_tracking_spath | Same — append-only, raw → mart directly |
| mart_tno_lookup | tno is a filter on mart_uti, not a join key — no lookup table needed |
| spath_gate_task / uti_cleanup_task | Root cause fixed — no Airflow gate needed |

---

## 5. Table Physical Design

### mart_uni_tracking_info

```python
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['update_time', 'order_id'],
        # No incremental_predicates — DELETE is always WHERE order_id IN (batch)
        post_hook=[
            -- Step 1: clean stale hist entries for reactivated orders
            """DELETE FROM hist_uni_tracking_info
               WHERE order_id IN (
                   SELECT order_id FROM {{ this }}
                   WHERE update_time >= (SELECT COALESCE(MAX(update_time), 0) - 900 FROM {{ this }})
               )""",
            -- Step 2: archive orders about to be trimmed
            """INSERT INTO hist_uni_tracking_info
               SELECT * FROM {{ this }}
               WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }})""",
            -- Step 3: trim active mart
            """DELETE FROM {{ this }}
               WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }})"""
        ]
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

**DISTKEY(order_id)**: 3-way JOIN is always on order_id — co-location means zero cross-node shuffle.
**SORTKEY(update_time, order_id)**: zone maps support time-range queries and retention trim.
**ranked CTE**: deduplicates within-batch duplicates before INSERT.

---

### mart_ecs_order_info

```python
{{
    config(
        materialized='incremental',
        dist='order_id',
        sort=['partner_id', 'add_time', 'order_id'],
        post_hook=[
            -- Archive: rows for inactive orders, routed by add_time period
            -- Selection criterion is order_id NOT IN mart_uti (NOT add_time age)
            -- add_time range is for routing to the correct hist table only
            """INSERT INTO hist_ecs_order_info_YYYY_HX
               SELECT ecs.*
               FROM   {{ this }} ecs
               LEFT   JOIN mart_uni_tracking_info uti ON ecs.order_id = uti.order_id
               WHERE  uti.order_id IS NULL
               AND    ecs.add_time >= [period_start]
               AND    ecs.add_time <  [period_end]
               AND    ecs.order_id NOT IN (SELECT order_id FROM hist_ecs_order_info_YYYY_HX)""",
            -- Trim: DELETE USING anti-join (NULL-safe, Redshift-optimised)
            """DELETE FROM {{ this }}
               USING (
                   SELECT ecs.order_id
                   FROM   {{ this }} ecs
                   LEFT   JOIN mart_uni_tracking_info uti ON ecs.order_id = uti.order_id
                   WHERE  uti.order_id IS NULL
               ) to_trim
               WHERE {{ this }}.order_id = to_trim.order_id"""
        ]
    )
}}

select *
from settlement_public.ecs_order_info_raw
{% if is_incremental() %}
where add_time > (select coalesce(max(add_time), 0) - 1800 from {{ this }})
{% endif %}
```

**Append-only**: ECS confirmed — no updates after order creation. No DELETE in the dbt model.
**SORTKEY(partner_id, add_time, order_id)**: primary query always filters by partner_id —
leading on partner_id cuts the scanned rows to ~0.3% before the JOIN.
**Retention**: order_id-driven — trims only when order is inactive in mart_uti.

---

### mart_uni_tracking_spath

```python
{{
    config(
        materialized='incremental',
        dist='order_id',
        sort=['pathTime', 'order_id'],
        post_hook=[
            -- Archive: pure time-based, all spath events older than 6 months
            -- regardless of whether the order is active or not.
            -- No order_id NOT IN mart_uti condition — that was the rejected approach.
            -- routed to correct YYYY_HX table by pathTime range
            """INSERT INTO hist_uni_tracking_spath_YYYY_HX
               SELECT * FROM {{ this }}
               WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }})
               AND   pathTime >= [period_start]
               AND   pathTime <  [period_end]
               AND   order_id NOT IN (SELECT order_id FROM hist_uni_tracking_spath_YYYY_HX)""",
            -- Trim: pure time-based, no order_id condition
            """DELETE FROM {{ this }}
               WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }})"""
        ]
    )
}}

select *
from settlement_public.uni_tracking_spath_raw
{% if is_incremental() %}
where pathTime > (select coalesce(max(pathTime), 0) - 1800 from {{ this }})
{% endif %}
```

**Append-only**: spath events are immutable once written.
**SORTKEY(pathTime, order_id)**: primary query filters `WHERE pathTime >= X` — zone maps
skip all blocks before the query date, most impactful filter in the primary query.
**Retention**: pure time-based — spath events older than 6 months are trimmed regardless
of order activity. Active orders with old spath events have those events moved to hist_uts.
The primary query always filters `WHERE pathTime >= recent date` so old spath events are
not needed in the active mart. This keeps mart_uts capped at ~1.17B rows permanently.

---

## 6. Retention Strategy

### Why mart_uti drives retention for all three tables

Each table has a different natural timestamp (`update_time`, `add_time`, `pathTime`).
If each table trims independently by its own timestamp, a long-lifecycle order can be
trimmed from mart_ecs and mart_uts while still active in mart_uti — breaking the 3-way JOIN.

**mart_uti is the single source of truth for what is "active".**

| Table | Retention mechanism | Effect |
|---|---|---|
| mart_uti | 6-month update_time trim | Trims orders dormant > 6 months |
| mart_ecs | order_id NOT IN mart_uti | Trims only when mart_uti trims the order |
| mart_uts | 6-month pathTime + order_id NOT IN mart_uti | Trims old events only for inactive orders |

### Result

All three active mart tables always contain the same set of order_ids. The 3-way JOIN
never has a missing row, regardless of order lifecycle length.

---

## 7. Historical Layer

### Purpose

Orders trimmed from the active mart are preserved permanently in the historical layer.
hist_uti holds the **last known state** of each dormant order.
hist_ecs and hist_uts hold the corresponding ecs and spath data for those orders.

### Archive-before-trim pattern

Rows are copied to historical tables in the mart_uti post_hook **before** the retention
trim fires. The order matters — see §10.

### Table configs

**hist_uni_tracking_info** (single table, split to yearly later)
```python
config(
    materialized='incremental',
    dist='order_id',
    sort=['order_id'],
    # no retention — permanent archive
)
```

**hist_ecs_order_info_YYYY** (yearly tables, split by add_time)
```python
config(
    materialized='incremental',
    dist='order_id',
    sort=['add_time', 'order_id'],
)
```

**hist_uni_tracking_spath_YYYY** (yearly tables, split by pathTime)
```python
config(
    materialized='incremental',
    dist='order_id',
    sort=['pathTime', 'order_id'],
)
```

### Orders moving to historical as a unit

When mart_uti trims an order:
1. hist_uti receives the order's last known state
2. hist_ecs receives the order's ecs row (mart_ecs trims by order_id NOT IN mart_uti)
3. hist_uts receives the order's spath events (mart_uts trims by order_id NOT IN mart_uti)

All three historical tables are updated at the same time, with the same set of order_ids.
This guarantees hist tables are always aligned — no order can be in hist_uti but missing
from hist_ecs or hist_uts.

---

## 8. Mart Splitting — Legacy Manual Splits vs New 6-Month Splits

### How the legacy system split tables

Legacy splits were **manual and ad-hoc**, triggered when a table became unmanageable.
There was no fixed cadence. The split boundary was always the UTI update_time of the
last order in the batch at the time of the split. Example:

```
dw_ecs_order_info_archived_20220909_20240501
  → contains orders whose UTI update_time falls between 2022-09-09 and 2024-05-01
  → no order in this table was updated after May 1 2024

dw_uni_tracking_spath_archived_20220909_20240501
  → same order set, same UTI update_time boundary
```

All three tables were split together using the **same UTI update_time boundary**.
UTI update_time was always the anchor — ECS and spath archives were scoped to the
same order_id set as the UTI archive, not by their own add_time or pathTime.

Implications of ad-hoc splits:
- No consistent period per table — boundary dates differed each time
- Querying across archived periods required knowing which table to look in
- No automation — a manual DBA operation each time
- Naming convention carried the date range in the table name, making it the
  only way to discover the contents of each archive

### Why MySQL needed splits at all

| MySQL characteristic | Impact |
|---|---|
| Row-based storage | Every query reads full rows — I/O scales with row count |
| B-tree indexes | Degrade with volume, slow to maintain on large tables |
| Single node | No parallelism — all work on one machine |
| JOINs | Data redistribution is expensive, degrades with table size |

MySQL tables with no splits grow unboundedly and degrade. Splits were the
operational solution to keep table sizes manageable — driven by pain, not schedule.

### Why Redshift does not have the same problem

| Redshift characteristic | Impact |
|---|---|
| Columnar storage | Queries read only the columns they need — I/O is fraction of row-store |
| Zone maps | SORTKEY enables block-level min/max pruning — equivalent to index but scales |
| MPP architecture | Parallel processing across nodes — more data = same speed per node |
| DISTKEY co-location | JOINs are local per node — zero cross-node shuffle for order_id JOINs |

A 500M row Redshift table with proper DISTKEY + SORTKEY queries faster than a 10M
row MySQL table with poor indexes. Splitting in Redshift is for **operational
management** (archival, drop old data, audit) — not for query performance.

### Active mart: no splits needed

6-month retention post_hooks keep all three active mart tables permanently compact.
Tables never grow beyond steady-state size regardless of how long the pipeline runs.
No manual intervention. No ad-hoc split operations.

| Table | Steady-state rows | Why |
|---|---|---|
| mart_uti | ~90–175M | One row per active order, 6-month rolling window |
| mart_ecs | ~90–175M | Mirrors mart_uti (order_id-driven retention) |
| mart_uts | ~1.17B | 195.3M events/month × 6 months |

### Historical layer: fixed 6-month splits

hist tables are never trimmed — they grow permanently. Fixed 6-month splits (_YYYY_h1,
_YYYY_h2) replace the legacy's ad-hoc date-range naming with a predictable, queryable
structure:

- **Consistent boundaries**: 1 Jan and 1 Jul — same cadence every year
- **Operational management**: drop a period to reclaim space without touching others
- **Query routing**: historical queries scoped to a known 6-month table
- **No lookup required**: naming convention alone tells you which table holds a given period

vs legacy ad-hoc: to find orders from Q1 2023, you had to know the split date chosen at that time and look for the table whose date range straddled it.

### Split column and anchor

All three hist tables split by the same UTI update_time boundary — identical to the
legacy approach, now on a fixed schedule:

| Table | Split column | Why |
|---|---|---|
| hist_uti_YYYY_HX | `update_time` at archival | UTI is the anchor — defines the active/dormant boundary |
| hist_ecs_YYYY_HX | `add_time` | Immutable — used for routing only; order_id ties it to the UTI period |
| hist_uts_YYYY_HX | `pathTime` | Immutable — used for routing only |

Same principle as the legacy: all three tables for a given period contain the same
set of order_ids. UTI is always the anchor.

---

## 9. Order_id-Driven Retention — Keeping All Three Tables Aligned

### The problem it solves

Without alignment, independent time-based retention breaks the 3-way JOIN:

```
Order X:  add_time = Jan 2024, update_time = today (still active)

mart_uti:   KEEPS order X  ✓   (update_time = today, within 6 months)
mart_ecs:   TRIMS order X  ✗   (add_time = Jan 2024, > 6 months)
mart_uts:   TRIMS order X  ✗   (pathTime = Jan 2024, > 6 months)

Primary query JOIN fails — active order missing from ecs and uts
```

### The solution

mart_uti determines the active set. mart_ecs and mart_uts only trim rows for orders
that mart_uti has already trimmed.

```
mart_uti trims order X (update_time aged out)
    → archive-before-trim fires for all three tables
    → hist_uti, hist_ecs, hist_uts all receive order X simultaneously
    → mart_ecs trims order X (order_id NOT IN mart_uti)
    → mart_uts trims order X (pathTime < 6 months AND order_id NOT IN mart_uti)

All three active mart tables: order X gone. All three hist tables: order X present.
```

### Benefit for hist splits

Since all three tables move together, hist splits use a single anchor — UTI update_time
at archival determines the period:

```
Orders archived in 2026-h1 → hist_uti_2026_h1, hist_ecs_2026_h1, hist_uts_2026_h1
Orders archived in 2026-h2 → hist_uti_2026_h2, hist_ecs_2026_h2, hist_uts_2026_h2
```

One period key, consistent across all three tables. No cross-period lookups needed.
Same logic as the legacy design — UTI update_time anchors the archive — now on a
predictable fixed schedule instead of ad-hoc.

---

## 10. Post-Hook Execution Order

Post-hooks within mart_uti must execute in this order:

```
1. Delete stale hist entries for reactivated orders
      (order was archived but got a new update — remove old hist row)
2. Archive rows about to be trimmed
      (copy to hist BEFORE deleting from active mart)
3. Trim active mart
      (delete aged-out rows from mart_uti)
```

mart_uti post_hooks must complete **before** mart_ecs and mart_uts post_hooks run,
because mart_ecs and mart_uts use `order_id NOT IN (SELECT order_id FROM mart_uti)`
as their trim condition — they depend on mart_uti's current state.

Ensure DAG dependency: mart_uti must finish before mart_ecs and mart_uts post_hooks fire.
In dbt with 4 threads, set mart_ecs and mart_uts as downstream dependencies of mart_uti.

---

## 11. Primary Query Patterns

### Pattern 1 — Main operational query (3-way JOIN)

```sql
SELECT ...
FROM   mart_ecs_order_info        ecs
JOIN   mart_uni_tracking_spath    uts ON ecs.order_id = uts.order_id
JOIN   mart_uni_tracking_info     uti ON ecs.order_id = uti.order_id
WHERE  ecs.partner_id IN (382)
AND    uts.pathTime   >= unix_timestamp('2025-07-17 00:00:00')
AND    uts.code       = 200
```

| Step | Operation | Mechanism | Why fast |
|---|---|---|---|
| 1 | Scan mart_uts WHERE pathTime >= X | SORTKEY(pathTime) zone maps | Skips all blocks before date |
| 2 | Filter code = 200 | Column scan on small remaining set | Minimal rows after step 1 |
| 3 | JOIN mart_ecs WHERE partner_id IN (382) | DISTKEY(order_id) + SORTKEY(partner_id) | Local join + zone maps cut ecs to ~0.3% |
| 4 | JOIN mart_uti | DISTKEY(order_id) | Local join, zero shuffle |

### Pattern 2 — tno (tracking number) filter

```sql
SELECT ...
FROM   mart_uni_tracking_info     uti
JOIN   mart_ecs_order_info        ecs ON uti.order_id = ecs.order_id
JOIN   mart_uni_tracking_spath    uts ON uti.order_id = uts.order_id
WHERE  uti.tno = 'SF1234567890'
```

tno is a **filter** on mart_uti, not a join key. DISTKEY(order_id) join is unchanged.
The tno column scan on mart_uti reads one column (~90M × ~20 bytes compressed) — fast.
No separate lookup table needed.

### Pattern 3 — Historical query

```sql
SELECT ...
FROM   hist_uni_tracking_info_2024 uti
JOIN   hist_ecs_order_info_2024    ecs ON uti.order_id = ecs.order_id
JOIN   hist_uni_tracking_spath_2024 uts ON uti.order_id = uts.order_id
WHERE  ecs.partner_id IN (382)
```

Same DISTKEY co-location as active mart. Yearly table scoped to one year.

---

## 12. Data Quality Tests

### Test 1 — update_time vs max(pathTime) alignment

For every active order, `update_time` in mart_uti should equal `MAX(pathTime)` in mart_uts.
A mismatch indicates a backdated update_time, missed extraction, or uti/uts sync drift.

```sql
SELECT
    uti.order_id,
    uti.update_time,
    MAX(uts.pathTime)                   AS max_path_time,
    uti.update_time - MAX(uts.pathTime) AS diff_seconds
FROM mart_uni_tracking_info uti
JOIN mart_uni_tracking_spath uts ON uti.order_id = uts.order_id
GROUP BY uti.order_id, uti.update_time
HAVING uti.update_time <> MAX(uts.pathTime)
ORDER BY ABS(uti.update_time - MAX(uts.pathTime)) DESC
```

Every order in the system has at least one creation event in uts. INNER JOIN is correct —
no NULL handling needed.

**Run**: as a daily Airflow monitoring task. Alert if row count > 0.

### Test 2 — Orders in mart_uti with no spath anywhere

An order in mart_uti with no rows in mart_uts AND no rows in hist_uts is a genuine
data integrity problem (order exists but zero spath events ever recorded).

```sql
-- Part A: orders with spath in active mart — check time alignment (Test 1 above)

-- Part B: orders with NO spath in active mart — must exist in hist_uts
SELECT uti.order_id
FROM mart_uni_tracking_info uti
LEFT JOIN mart_uni_tracking_spath uts     ON uti.order_id = uts.order_id
LEFT JOIN hist_uni_tracking_spath hist    ON uti.order_id = hist.order_id
WHERE uts.order_id  IS NULL
  AND hist.order_id IS NULL
```

**Interpretation**:
- Returns rows → genuine missing data, investigate
- Returns zero but some orders only in hist_uts → expected (long-lifecycle order, spath events aged to hist)

---

## 13. Known Risks and Edge Cases

### Risk 1 — Backdated update_time (bad operation)

**Scenario**: Someone manually sets `update_time` to a past date in MySQL.
The incremental extraction (`WHERE update_time >= source_cutoff`) misses the row because
the timestamp now falls below the cutoff. mart_uti retains the stale pre-backdate value.

**Detection**: Test 1 (§12) catches this — update_time will diverge from MAX(pathTime).

**Mitigation**: No automatic fix. Daily wide-window re-extraction (last 30 days) can be
added as a separate Airflow task to catch backdating within 30 days. For backdating beyond
30 days, manual investigation is required after Test 1 alerts.

### Risk 2 — Reactivated orders (dormant > 6 months, then new update)

**Scenario**: An order trimmed from mart_uti (archived to hist_uti) gets a new update.

**What happens**:
- Extraction catches the new update (update_time = today, within source_cutoff) ✓
- mart_uti DELETE: order_id not in mart_uti → no-op
- mart_uti INSERT: order re-enters mart_uti with new update_time ✓
- Post-hook step 1: stale hist_uti entry deleted (order is active again) ✓

New update is NOT missed. hist_uti is cleaned up automatically.

### Risk 3 — ECS post-creation updates (Gap 1)

The old Kettle pipeline co-extracted ECS on every uti update (via JOIN), so ECS was
always refreshed. Our pipeline has no update_time in ecs_order_info — changes after
order creation are not detected.

**Impact**: Low — primary query filters on partner_id (immutable at creation).
**Action**: Run `compare_pipelines.py --table ecs`. Accept if mismatch rate is low.

### Risk 4 — spath event updates (Gap 2)

Old Kettle handled `is_updated = 1` via full DELETE+INSERT per order for spath.
Our pipeline is append-only — updated spath events are not recaptured.

**Impact**: Low — primary query filters on code (immutable once scanned).
**Action**: Run `compare_pipelines.py --table uts`. If mismatch rate is high,
change mart_uts to delete+insert by composite key (order_id + traceSeq + pathTime).

### Risk 5 — Post-hook execution order violated

If mart_ecs or mart_uts post_hooks run before mart_uti post_hook completes, they will
trim the wrong set of orders (mart_uti has not yet trimmed, so order_id NOT IN mart_uti
returns an empty set — nothing is trimmed).

**Mitigation**: Enforce DAG dependency — mart_uti must complete before mart_ecs and
mart_uts models run. See §10.

---

## 14. Performance Analysis

### PROD benchmarks (2026-02-23)

| Test | stg_uti rows | Batch rows | PROD time |
|---|---|---|---|
| 15-min incremental (no post_hooks) | 11.9M | 5,062 | 84s |
| Full-day delta | 12.5M | 608K | 166s |
| Actual end-to-end cycle | 11.9M | 1,897 | 123s |

### Projected performance — final design (Option B, no staging)

| Model | Table size | Operation | Estimated time |
|---|---|---|---|
| mart_uti | ~90M at steady state | order_id DELETE + INSERT | ~100–150s |
| mart_ecs | ~36M | append-only INSERT | ~10–20s |
| mart_uts | ~360M | append-only INSERT | ~30s |
| **dbt wall time (4 threads)** | | bottleneck = mart_uti | **~150s** |
| **End-to-end** | | extraction ~94s + dbt ~150s | **~4m04s** |

Comfortably within 15-minute schedule.

### Why mart_uti DELETE is fast at 90M rows

Redshift DELETE WHERE order_id IN (batch) reads **only the order_id column** — columnar storage.

```
order_id column: 90M × 8 bytes = 720MB → ~180MB compressed
DISTKEY(order_id): hash join is local per node — no cross-node network shuffle
Per node (3 nodes): ~60MB I/O for the join
```

The 84–123s measured for stg_uti at 11.9M rows is dominated by fixed overhead
(query compilation, zone map updates, INSERT), not scan cost. Scaling from 14M to
90M rows adds seconds, not minutes.

### Why Redshift does not need ad-hoc splits (active mart)

| Concern | MySQL/Kettle solution | Redshift equivalent |
|---|---|---|
| Table too large to scan | Manual ad-hoc split when pain hits | Columnar + zone maps prune before scan |
| JOIN too slow | Split + index | DISTKEY co-location — zero shuffle |
| Index maintenance | Smaller tables reduce B-tree cost | No indexes — zone maps are automatic |
| Single node bottleneck | Splits reduce per-table size | MPP — parallelism is native |

6-month splits on hist tables are for **operational management only**, not performance.
The fixed cadence replaces the legacy's reactive, manual splitting with a scheduled,
predictable operation.

---

## 15. DAG Flow

```
Extraction (parallel, every 15 min)
  extract_ecs  →  ecs_order_info_raw
  extract_uti  →  uni_tracking_info_raw
  extract_uts  →  uni_tracking_spath_raw
        ↓
dbt run — mart_uti  (must complete first, drives retention anchor)
        ↓
dbt run — mart_ecs, mart_uts  (parallel, post_hooks depend on mart_uti state)
        ↓
Monitoring — daily alignment check (Test 1 + Test 2 from §12)
```

No Airflow gate tasks. No XCom. No conditional cleanup tasks.

---

## 16. Implementation Phases

### Phase 1 — Core fix (immediate)

- [ ] Remove `stg_uni_tracking_info.sql` (Option B chosen)
- [ ] Remove `stg_ecs_order_info.sql`
- [ ] Remove `stg_uni_tracking_spath.sql`
- [ ] Create `mart_uni_tracking_info.sql` with inline dedup + post_hooks (§5)
- [ ] Create `mart_ecs_order_info.sql` with order_id-driven retention (§5)
- [ ] Create `mart_uni_tracking_spath.sql` with order_id-driven retention (§5)
- [ ] Update DAG: mart_uti as dependency for mart_ecs and mart_uts

### Phase 2 — Historical layer

- [ ] Create `hist_uni_tracking_info` (single table, no retention)
- [ ] Create `hist_ecs_order_info` (receives rows when mart_ecs trims)
- [ ] Create `hist_uni_tracking_spath` (receives rows when mart_uts trims)
- [ ] Validate archive-before-trim fires correctly on QA

### Phase 3 — Validate performance

- [ ] Measure mart_uti DELETE time on PROD at current table size
- [ ] Project to 90M rows — confirm Option B is viable
- [ ] If >5 min at 90M: reintroduce stg_uti as 20-day dedup buffer (Option A fallback)

### Phase 4 — Data quality monitoring

- [ ] Implement Test 1 (update_time vs max pathTime) as daily Airflow task
- [ ] Implement Test 2 (orders missing from uts) as daily Airflow task
- [ ] Run `compare_pipelines.py --table ecs` — measure Gap 1 severity
- [ ] Run `compare_pipelines.py --table uts` — measure Gap 2 severity

### Phase 5 — Migration

- [ ] Snapshot existing staging data to backup table before switching
- [ ] Seed mart tables from existing staging
- [ ] Switch all user queries from staging → mart
- [ ] Monitor for 1 week before dropping stg_* tables

### Phase 6 — hist 6-month splits (operational, every Jan 1 and Jul 1)

- [ ] Create new hist_*_YYYY_HX tables before each period boundary
- [ ] Update post_hook routing to include the new period
- [ ] Update mart_uti step 1 cleanup to point to the new latest hist_uti table
- [ ] First split needed: 1 Jul 2026 (create hist_*_2026_h2 tables)

---

*This document supersedes all previous design docs for the order tracking pipeline.*
*For the specific stg_uti dedup analysis, see `stg_uti_dedup_final.md` (archived).*
