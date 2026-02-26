# Order Tracking Pipeline — Final Design

**Created**: 2026-02-25
**Status**: Pending Approval, Pending Implementation

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Active Mart — Table by Table](#2-active-mart--table-by-table)
3. [Retention Strategy](#3-retention-strategy)
4. [Post-Hook Logic — Full Detail](#4-post-hook-logic--full-detail)
   - [4a. Macro Pattern — period routing without hand-editing SQL](#4a-macro-pattern--period-routing-without-hand-editing-sql)
5. [Reactivation Handling](#5-reactivation-handling)
6. [Full 15-Minute Cycle](#6-full-15-minute-cycle)
7. [Worked Examples](#7-worked-examples)
8. [Historical Layer](#8-historical-layer)
9. [Why 6-Month Splits](#9-why-6-month-splits)
10. [Exceptions Table](#10-exceptions-table)
11. [Query Patterns](#11-query-patterns)
12. [Data Quality Tests](#12-data-quality-tests)
13. [Table Sizes — Real Volumes](#13-table-sizes--real-volumes)
14. [DAG Dependency](#14-dag-dependency)
15. [Operational Calendar](#15-operational-calendar)
16. [VACUUM Strategy](#16-vacuum-strategy)
17. [ANALYZE Strategy](#17-analyze-strategy)
18. [Table Health Monitoring](#18-table-health-monitoring)
19. [Post-Hook Failure Handling](#19-post-hook-failure-handling)
20. [Initial Load Procedure](#20-initial-load-procedure)
21. [hist Table Lifecycle](#21-hist-table-lifecycle)
22. [Known Gaps](#22-known-gaps)

---

## 1. Architecture

```
MySQL Source
    ├── uni_tracking_info_raw
    ├── ecs_order_info_raw
    └── uni_tracking_spath_raw
                │
                ▼  (extracted every 15 min)
┌──────────────────────────────────────────────────────────┐
│                    ACTIVE MART LAYER                     │
│                                                          │
│  mart_uni_tracking_info   one row per active order       │
│  mart_ecs_order_info      one row per active order       │
│  mart_uni_tracking_spath  6-month spath events           │
└──────────────────────┬───────────────────────────────────┘
                       │  (archive-before-trim, on retention)
                       ▼
┌──────────────────────────────────────────────────────────┐
│                   HISTORICAL LAYER                       │
│                                                          │
│  hist_uni_tracking_info_YYYY_HX   6-month splits         │
│  hist_ecs_order_info_YYYY_HX      6-month splits         │
│  hist_uni_tracking_spath_YYYY_HX  6-month splits         │
└──────────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│                  EXCEPTIONS TABLE                        │
│                                                          │
│  order_tracking_exceptions   rare edge cases surfaced    │
│                              for manual review           │
└──────────────────────────────────────────────────────────┘
```

**Naming convention**: `_YYYY_h1` = Jan–Jun, `_YYYY_h2` = Jul–Dec

**No staging layer. No lookup tables. Raw → mart directly.**

---

## 2. Active Mart — Table by Table

### 2a. mart_uni_tracking_info

**Purpose**: one row per active order, always the latest tracking state.

**Strategy**: `delete+insert`
- Every cycle: DELETE old row for each order in batch, INSERT new row
- DELETE is `WHERE order_id IN (batch)` — no time window, always correct regardless of order age

**Dedup**: `ranked` CTE handles within-batch duplicates:

```sql
with filtered as (
    select *
    from uni_tracking_info_raw
    where update_time > (select coalesce(max(update_time), 0) - 1800 from {{ this }})
),
ranked as (
    select *,
        row_number() over (
            partition by order_id
            order by update_time desc, id desc   -- id (or any stable PK) breaks ties
                                                 -- deterministically when update_time
                                                 -- is identical across two source rows
        ) as _rn
    from filtered
)
select * exclude(_rn) from ranked where _rn = 1
```

**Why the secondary sort key matters**: if two source rows share the same `order_id`
and `update_time`, `row_number()` with no tie-break is non-deterministic — Redshift
can return either row and the result may differ between runs. A stable secondary column
(auto-increment `id`, row creation timestamp, or any immutable field) guarantees a
consistent, reproducible result every run.

**Physical design**:
- `DISTKEY(order_id)` — 3-way JOIN always on order_id, zero cross-node shuffle
- `SORTKEY(update_time, order_id)` — zone maps for time-range queries and retention trim

**Retention**: 6-month `update_time` trim → archives to `hist_uti_YYYY_HX`

---

### 2b. mart_ecs_order_info

**Purpose**: one row per active order, order creation metadata (partner, creation time, etc.)

**Strategy**: append-only
- ECS confirmed write-once — no updates after order creation
- New orders appended each cycle, no DELETE in the dbt model
- Retention handled by post_hook

**Physical design**:
- `DISTKEY(order_id)` — JOIN co-location
- `SORTKEY(partner_id, add_time, order_id)` — primary query always filters by `partner_id`.
  Leading on `partner_id` cuts the scanned rows to ~0.3% of the table before the JOIN.

**Retention**: order_id-driven — mart_ecs trims an order only when mart_uti has already
trimmed it. A long-lifecycle order (created Jan 2024, still active today) stays in mart_ecs
regardless of how old its `add_time` is. This ensures the 3-way JOIN never breaks for
active orders.

**Why not time-based**: mart_ecs has ONE row per order. If trimmed by `add_time > 6 months`,
an active long-lifecycle order would have zero rows in mart_ecs — the 3-way JOIN returns
nothing for that order. Order_id-driven retention prevents this.

---

### 2c. mart_uni_tracking_spath

**Purpose**: 6-month rolling window of spath events for all orders.

**Strategy**: append-only
- New spath events appended each cycle, no DELETE in the dbt model
- Retention handled by post_hook

**Physical design**:
- `DISTKEY(order_id)` — JOIN co-location
- `SORTKEY(pathTime, order_id)` — primary query filters `WHERE pathTime >= X`.
  Zone maps skip all blocks before the query date.

**Retention**: pure time-based — `WHERE pathTime < 6-month cutoff`. No order_id dependency.

**Why pure time-based (not order_id-driven)**:
- mart_uts holds many rows per order. Order_id-driven retention would keep ALL spath
  events for every active order indefinitely — a 2-year-old active order accumulates
  2 years of spath events in the active mart. Table size becomes unpredictable.
- Pure time-based caps mart_uts at a predictable ~1.17B rows permanently.
- Primary query always filters `WHERE pathTime >= X` — old spath events for active
  orders are not needed in the active mart. They are safely stored in hist_uts.

---

## 3. Retention Strategy

### The core rule

**mart_uti drives what is "active".** mart_ecs uses mart_uti membership as its trim
condition. mart_uts uses pure time-based trim independently.

| Table | Retention mechanism | Drives what |
|---|---|---|
| mart_uti | `update_time < MAX - 6 months` | Defines the active order set |
| mart_ecs | `order_id NOT IN (mart_uti)` | Stays aligned with mart_uti |
| mart_uts | `pathTime < MAX - 6 months` | Independent, time-capped |

### Why mart_ecs cannot use time-based retention

```
Order X: created Jan 2024, still actively updating Aug 2025

Time-based (WRONG):
  mart_ecs trims order X — add_time = Jan 2024 > 6 months ago ✗
  mart_uti keeps order X — update_time = Aug 2025 ✓
  3-way JOIN broken — active order has no ecs row

Order_id-driven (CORRECT):
  mart_ecs checks: is order X in mart_uti? YES → keep it ✓
  3-way JOIN intact regardless of add_time age
```

### Why mart_uts uses pure time-based retention

mart_uts holds many spath events per order. Order_id-driven retention would keep
ALL historical spath events for every active order — unbounded growth at 195M rows/month.
Pure 6-month cap keeps mart_uts at ~1.17B rows permanently. Old spath events for
active orders are in hist_uts and accessible for historical queries.

---

## 4. Post-Hook Logic — Full Detail

### mart_uni_tracking_info post_hooks (4 steps, in order)

```sql
-- STEP 1: Delete stale hist_uti entry for reactivated orders
-- An archived order re-enters mart_uti when it gets a new update.
-- Its old hist entry is now stale — clean it before archiving new rows.
--
-- Orders do not sit dormant for more than ~1 year, so the stale entry is
-- always in the latest hist_uti table (the most recently written period).
-- One DELETE is sufficient — no need to scan older tables.
--
-- DISTKEY(order_id) — DELETE is a co-located local lookup, no cross-node scan.
-- Reactivated set is tiny — this DELETE is a cheap no-op most cycles.
--
-- Update this table name every 1 Jan and 1 Jul (see §15 Operational Calendar).
DELETE FROM hist_uni_tracking_info_2025_h2   -- latest hist_uti table
WHERE order_id IN (
    SELECT order_id FROM mart_uni_tracking_info
    WHERE update_time >= (SELECT COALESCE(MAX(update_time), 0) - 900
                          FROM mart_uni_tracking_info)
);

-- STEP 2: Archive rows about to be trimmed to correct 6-month hist table
-- Generated by dbt macro archive_to_hist — do not hand-edit periods here.
-- Add new periods only in dbt_project.yml vars (see §4a Macro Pattern).
INSERT INTO hist_uni_tracking_info_2026_h1   -- route by update_time range
SELECT * FROM mart_uni_tracking_info
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000
                     FROM mart_uni_tracking_info)
AND   update_time >= unix_timestamp('2026-01-01')
AND   update_time <  unix_timestamp('2026-07-01');

INSERT INTO hist_uni_tracking_info_2026_h2
SELECT * FROM mart_uni_tracking_info
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000
                     FROM mart_uni_tracking_info)
AND   update_time >= unix_timestamp('2026-07-01')
AND   update_time <  unix_timestamp('2027-01-01');
-- (macro generates one INSERT per period defined in dbt_project.yml)

-- STEP 3: Safety check — catch any rows that fall outside defined period windows.
-- If a row's update_time does not match any period, it is NOT archived and must
-- NOT be deleted. Log it to exceptions and exclude it from the trim.
INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at, notes)
SELECT
    m.order_id,
    'ARCHIVE_ROUTING_GAP',
    CURRENT_TIMESTAMP,
    'update_time outside all defined hist periods — excluded from trim'
FROM mart_uni_tracking_info m
WHERE m.update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000
                       FROM mart_uni_tracking_info)
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_info_2026_h1 WHERE order_id = m.order_id)
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_info_2026_h2 WHERE order_id = m.order_id)
  AND NOT EXISTS (
      SELECT 1 FROM order_tracking_exceptions
      WHERE order_id = m.order_id
        AND exception_type = 'ARCHIVE_ROUTING_GAP'
        AND resolved_at IS NULL
  );
-- (macro generates one NOT EXISTS clause per period)

-- STEP 4: Trim active mart — exclude rows flagged as ARCHIVE_ROUTING_GAP
DELETE FROM mart_uni_tracking_info
WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000
                     FROM mart_uni_tracking_info)
  AND order_id NOT IN (
      SELECT order_id FROM order_tracking_exceptions
      WHERE exception_type = 'ARCHIVE_ROUTING_GAP'
        AND resolved_at IS NULL
  );
```

**Step order is critical**:
- Step 1 before step 2: reactivated orders have recent `update_time`, step 2 won't
  archive them (not aged out). But their stale hist entry must be cleaned first.
- Step 2 before step 3: archive rows before validating — step 3 checks what was archived.
- Step 3 before step 4: safety check determines which rows are safe to trim.
- Step 4 never trims a row that was not confirmed archived.

---

### §4a. Macro Pattern — period routing without hand-editing SQL

Hardcoding period dates in SQL is a data loss risk: a forgotten period means rows
are not archived but ARE trimmed. Use a dbt macro to generate the per-period INSERTs
from a central config list. Adding a new period = one line in `dbt_project.yml`.

**`dbt_project.yml`** — the only file updated every 6 months:
```yaml
vars:
  hist_periods:
    - name: '2025_h2'
      start: '2025-07-01'
      end:   '2026-01-01'
    - name: '2026_h1'
      start: '2026-01-01'
      end:   '2026-07-01'
    - name: '2026_h2'        # add this line on 1 Jul 2026
      start: '2026-07-01'
      end:   '2027-01-01'
```

**`macros/archive_to_hist.sql`**:
```jinja
{% macro archive_to_hist(source_table, ts_col, hist_prefix) %}
  {% set periods = var('hist_periods') %}
  {% set cutoff %}
    (SELECT COALESCE(MAX({{ ts_col }}), 0) - 15552000 FROM {{ source_table }})
  {% endset %}

  {% for p in periods %}
  INSERT INTO {{ hist_prefix }}_{{ p.name }}
  SELECT * FROM {{ source_table }}
  WHERE {{ ts_col }} <  {{ cutoff }}
    AND {{ ts_col }} >= unix_timestamp('{{ p.start }}')
    AND {{ ts_col }} <  unix_timestamp('{{ p.end }}')
    AND order_id NOT IN (SELECT order_id FROM {{ hist_prefix }}_{{ p.name }});
  {% endfor %}

  -- Safety: catch rows outside all defined periods
  INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at, notes)
  SELECT order_id, 'ARCHIVE_ROUTING_GAP', CURRENT_TIMESTAMP,
         'Timestamp outside all hist periods — excluded from trim'
  FROM {{ source_table }}
  WHERE {{ ts_col }} < {{ cutoff }}
  {% for p in periods %}
    AND order_id NOT IN (SELECT order_id FROM {{ hist_prefix }}_{{ p.name }})
  {% endfor %}
    AND order_id NOT IN (
        SELECT order_id FROM order_tracking_exceptions
        WHERE exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL
    );
{% endmacro %}
```

**Usage in mart_uti.sql post_hook**:
```python
post_hook=[
    "{{ archive_to_hist(this, 'update_time', 'hist_uni_tracking_info') }}",
    """DELETE FROM {{ this }}
       WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }})
         AND order_id NOT IN (
             SELECT order_id FROM order_tracking_exceptions
             WHERE exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL
         )"""
]
```

---

### mart_ecs_order_info post_hooks (2 steps, runs after mart_uti)

```sql
-- STEP 1: Archive ecs rows for inactive orders — route by add_time range
-- Uses LEFT JOIN anti-join instead of NOT IN:
--   NOT IN returns NULL (not FALSE) if mart_uti contains any NULL order_id.
--   LEFT JOIN anti-join is NULL-safe and faster at 90M+ rows.
-- Generated by dbt macro archive_to_hist — add new periods in dbt_project.yml only.
INSERT INTO hist_ecs_order_info_2026_h1
SELECT ecs.*
FROM   mart_ecs_order_info        ecs
LEFT   JOIN mart_uni_tracking_info uti ON ecs.order_id = uti.order_id
WHERE  uti.order_id IS NULL
AND    ecs.add_time >= unix_timestamp('2026-01-01')
AND    ecs.add_time <  unix_timestamp('2026-07-01')
AND    ecs.order_id NOT IN (SELECT order_id FROM hist_ecs_order_info_2026_h1);

INSERT INTO hist_ecs_order_info_2026_h2
SELECT ecs.*
FROM   mart_ecs_order_info        ecs
LEFT   JOIN mart_uni_tracking_info uti ON ecs.order_id = uti.order_id
WHERE  uti.order_id IS NULL
AND    ecs.add_time >= unix_timestamp('2026-07-01')
AND    ecs.add_time <  unix_timestamp('2027-01-01')
AND    ecs.order_id NOT IN (SELECT order_id FROM hist_ecs_order_info_2026_h2);
-- (macro generates one INSERT per period)

-- STEP 2: Trim — DELETE USING anti-join (NULL-safe, Redshift-optimised)
DELETE FROM mart_ecs_order_info
USING (
    SELECT ecs.order_id
    FROM   mart_ecs_order_info        ecs
    LEFT   JOIN mart_uni_tracking_info uti ON ecs.order_id = uti.order_id
    WHERE  uti.order_id IS NULL
) to_trim
WHERE mart_ecs_order_info.order_id = to_trim.order_id;
```

**Must run after mart_uti**: the LEFT JOIN anti-join condition depends on
mart_uti being in its final state for this cycle.

**Why DELETE USING instead of NOT IN**: `NOT IN (subquery)` returns NULL — not FALSE —
if the subquery contains any NULL value. One NULL row_id in mart_uti means nothing
gets deleted. `DELETE USING` with a LEFT JOIN is NULL-safe and Redshift resolves it
as an efficient hash anti-join co-located on DISTKEY(order_id).

---

### mart_uni_tracking_spath post_hooks (3 steps, independent)

```sql
-- STEP 1: Archive old spath events to correct 6-month hist table
-- Pure time-based — no order_id condition. All spath events older than 6 months
-- are archived regardless of whether the order is active or dormant.
-- Generated by dbt macro archive_to_hist — add new periods in dbt_project.yml only.
-- NOT EXISTS guard prevents duplicates on retry.
INSERT INTO hist_uni_tracking_spath_2026_h1   -- route by pathTime range
SELECT * FROM mart_uni_tracking_spath
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000
                  FROM mart_uni_tracking_spath)
AND   pathTime >= unix_timestamp('2026-01-01')
AND   pathTime <  unix_timestamp('2026-07-01')
AND   order_id NOT IN (SELECT order_id FROM hist_uni_tracking_spath_2026_h1
                       WHERE pathTime >= unix_timestamp('2026-01-01'));

INSERT INTO hist_uni_tracking_spath_2026_h2
SELECT * FROM mart_uni_tracking_spath
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000
                  FROM mart_uni_tracking_spath)
AND   pathTime >= unix_timestamp('2026-07-01')
AND   pathTime <  unix_timestamp('2027-01-01')
AND   order_id NOT IN (SELECT order_id FROM hist_uni_tracking_spath_2026_h2
                       WHERE pathTime >= unix_timestamp('2026-07-01'));
-- (macro generates one INSERT per period)

-- STEP 2: Safety check — catch spath events outside all defined period windows.
-- Logs unmatched rows to exceptions. Trim (step 3) excludes them — no silent loss.
INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at, notes)
SELECT DISTINCT
    m.order_id,
    'ARCHIVE_ROUTING_GAP_UTS',
    CURRENT_TIMESTAMP,
    'pathTime outside all defined hist_uts periods — excluded from trim'
FROM mart_uni_tracking_spath m
WHERE m.pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000
                    FROM mart_uni_tracking_spath)
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_spath_2026_h1 WHERE order_id = m.order_id
                  AND pathTime = m.pathTime)
  AND NOT EXISTS (SELECT 1 FROM hist_uni_tracking_spath_2026_h2 WHERE order_id = m.order_id
                  AND pathTime = m.pathTime)
  AND NOT EXISTS (
      SELECT 1 FROM order_tracking_exceptions
      WHERE order_id = m.order_id
        AND exception_type = 'ARCHIVE_ROUTING_GAP_UTS'
        AND resolved_at IS NULL
  );

-- STEP 3: Trim old spath events — exclude any flagged as ARCHIVE_ROUTING_GAP_UTS
DELETE FROM mart_uni_tracking_spath
WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000
                  FROM mart_uni_tracking_spath)
  AND order_id NOT IN (
      SELECT order_id FROM order_tracking_exceptions
      WHERE exception_type = 'ARCHIVE_ROUTING_GAP_UTS'
        AND resolved_at IS NULL
  );
```

**Independent of mart_uti**: pure time-based, no order_id dependency, can run in
parallel with mart_ecs.

**No NOT IN NULL-trap risk on trim**: the trim condition is a pure time-based
`pathTime < cutoff`, not a subquery against mart_uti. The only subquery is the
exceptions table exclusion, which is always a small set.

---

## 5. Reactivation Handling

A reactivated order is one that was dormant for >6 months (archived to hist), then
receives a new update.

### mart_uti reactivation — handled automatically in code

```
Order Z archived to hist_uti in Feb 2026.
April 2026: new update arrives.

Extraction:    Captures new update (update_time = April 2026) ✓
DELETE:        order_id = Z, not in mart_uti → no-op
INSERT:        Order Z re-enters mart_uti with update_time = April 2026 ✓
POST_HOOK 1:   Order Z in new-batch set → stale hist_uti entry deleted ✓
POST_HOOK 2:   update_time = April 2026, not aged out → no-op
POST_HOOK 3:   not trimmed → stays in mart_uti ✓
```

hist_uti is always clean. No manual action needed.

---

### mart_ecs reactivation — surfaced via exceptions table

When an order reactivates, mart_ecs has no row for it — mart_ecs trimmed the row
when the order went dormant, and the ECS extraction watermark (`add_time >= cutoff`)
will not re-extract an old order creation row.

**This is not handled in code.** Handling it in code requires:
- UNION views across all hist_ecs_YYYY_HX tables (restore step)
- Deleting the stale hist_ecs entry after restore (cleanup step)
- Tracking which hist_ecs table holds the row (metadata table)

For a rare edge case, this complexity is not justified. Instead:

```sql
-- Daily exceptions check: order in mart_uti but missing from mart_ecs
INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at)
SELECT
    uti.order_id,
    'REACTIVATED_ORDER_MISSING_ECS',
    CURRENT_TIMESTAMP
FROM mart_uni_tracking_info uti
LEFT JOIN mart_ecs_order_info ecs ON uti.order_id = ecs.order_id
WHERE ecs.order_id IS NULL
  AND uti.order_id NOT IN (
      SELECT order_id FROM order_tracking_exceptions
      WHERE exception_type = 'REACTIVATED_ORDER_MISSING_ECS'
      AND resolved_at IS NULL
  );
```

When triggered, the manual fix is straightforward:

```sql
-- 1. Restore ecs row from hist
INSERT INTO mart_ecs_order_info
SELECT * FROM hist_ecs_order_info_YYYY_HX
WHERE order_id IN (
    SELECT order_id FROM order_tracking_exceptions
    WHERE exception_type = 'REACTIVATED_ORDER_MISSING_ECS'
    AND resolved_at IS NULL
);

-- 2. Remove stale hist_ecs entry
DELETE FROM hist_ecs_order_info_YYYY_HX
WHERE order_id IN (
    SELECT order_id FROM order_tracking_exceptions
    WHERE exception_type = 'REACTIVATED_ORDER_MISSING_ECS'
    AND resolved_at IS NULL
);

-- 3. Mark resolved
UPDATE order_tracking_exceptions
SET resolved_at = CURRENT_TIMESTAMP
WHERE exception_type = 'REACTIVATED_ORDER_MISSING_ECS'
AND resolved_at IS NULL;
```

---

### mart_uts reactivation — no action needed

When an order reactivates:
- New spath events append to mart_uts ✓
- Old spath events remain in hist_uts (historical record, not stale) ✓
- No cleanup needed — hist_uts is a permanent archive, having old spath events
  there is correct and expected behaviour

---

### Reactivation summary

| Table | Handling | Mechanism |
|---|---|---|
| mart_uti | Automatic | Post_hook step 1: DELETE from latest hist_uti table — sufficient because orders never dormant > ~1 year |
| mart_ecs | Exceptions table | Daily check surfaces missing ecs rows, manual fix |
| mart_uts | No action needed | Old spath in hist_uts is correct, new spath appends |

**Why uti uses DELETE-only cleanup but ecs uses an exceptions table**:
- mart_uti: cleanup is a single DELETE against the latest hist_uti table.
  Safe to run blindly — if the order isn't there, it's a no-op. One line, no logic.
- mart_ecs: cleanup requires INSERT + DELETE (restore the ecs row from hist, then remove
  the stale hist entry). INSERT is not safe to run blindly — you need to find the right
  hist table first. That lookup chain is complex for a rare edge case.
  Exceptions table + manual fix is lower risk and simpler to maintain.

---

## 6. Full 15-Minute Cycle

```
① Extraction (parallel, ~94s)
   extract_ecs  → ecs_order_info_raw
   extract_uti  → uni_tracking_info_raw
   extract_uts  → uni_tracking_spath_raw

② dbt: mart_uni_tracking_info  (must complete before mart_ecs)
   a. ranked CTE: dedup within-batch, keep latest (update_time DESC, id DESC) per order_id
   b. DELETE WHERE order_id IN (batch) — removes old row regardless of age
   c. INSERT ranked results — exactly one row per order_id
   d. POST_HOOK step 1: DELETE stale hist_uti entry for reactivated orders (latest hist table)
   e. POST_HOOK step 2: archive aged-out rows → hist_uti_YYYY_HX (macro-routed by update_time)
                        NOT EXISTS guard prevents duplicate archive on retry
   f. POST_HOOK step 3: safety check — log any unmatched rows to exceptions table
                        (rows outside defined period windows never silently dropped)
   g. POST_HOOK step 4: trim aged-out rows — excludes anything flagged in step 3

③ dbt: mart_ecs_order_info  (after mart_uti, parallel with ④)
   a. INSERT new orders from ecs_order_info_raw
   b. POST_HOOK step 1: archive rows for inactive orders → hist_ecs_YYYY_HX
                        uses LEFT JOIN anti-join (NULL-safe), NOT IN avoided
   c. POST_HOOK step 2: DELETE USING anti-join trim — keeps only orders in mart_uti

④ dbt: mart_uni_tracking_spath  (after mart_uti, parallel with ③)
   a. INSERT new spath events from uni_tracking_spath_raw
   b. POST_HOOK step 1: archive old spath → hist_uts_YYYY_HX (macro-routed by pathTime)
                        NOT EXISTS guard; pure time-based, no order_id dependency
   c. POST_HOOK step 2: safety check — log unmatched spath rows to exceptions
   d. POST_HOOK step 3: pure time-based trim — excludes anything flagged in step 2

⑤ Monitoring (daily Airflow task, conditional VACUUM weekly)
   - Test 1: update_time vs max(pathTime) alignment → exceptions table
   - Test 2: orders missing from uts anywhere → exceptions table
   - Test 3: reactivated orders missing from mart_ecs → exceptions table
   - VACUUM check: query svv_table_info.unsorted; trigger VACUUM SORT for mart_ecs only if >15%
```

**Estimated total**: ~4 minutes. Headroom: ~11 minutes in 15-minute schedule.

---

## 7. Worked Examples

### Example A — Normal order

Order X last updated 3 days ago. New update arrives.

| Step | Action | Result |
|---|---|---|
| Extraction | update_time = today, within source_cutoff | Captured ✓ |
| DELETE | order_id = X, finds existing row → deletes | Old row gone |
| INSERT | New row with update_time = today | One row ✓ |
| POST_HOOKs | Not aged out, not reactivated | No-ops |

---

### Example B — Long-lifecycle order (old row > 20 days)

Order X active 90 days. Old row has update_time = day 62. New update arrives day 89.

| Step | Action | Result |
|---|---|---|
| DELETE | order_id = X → finds row at day 62, deletes it | day 62 row gone |
| INSERT | New row update_time = day 89 | One row ✓ |
| POST_HOOKs | day 89 not aged out | No-ops |

No duplicate. Old row found and removed regardless of age. ✓

---

### Example C — Order aging out (dormant > 6 months)

Order Y last updated 185 days ago. No new update this cycle.

| Step | Action | Result |
|---|---|---|
| Extraction | Order Y not in batch | Not extracted |
| DELETE / INSERT | Not in batch | No-ops |
| mart_uti POST_HOOK 1 | Not in new-batch set → no-op on hist cleanup | No-op |
| mart_uti POST_HOOK 2 | update_time = 185 days < cutoff → archived to hist_uti_YYYY_HX | In hist ✓ |
| mart_uti POST_HOOK 3 | Row was archived → not flagged in exceptions | Safety pass ✓ |
| mart_uti POST_HOOK 4 | Not in exceptions → trimmed from mart_uti | Gone from active ✓ |
| mart_ecs POST_HOOK 1 | LEFT JOIN anti-join: order Y absent from mart_uti → archived to hist_ecs_YYYY_HX | In hist ✓ |
| mart_ecs POST_HOOK 2 | DELETE USING anti-join: order Y absent from mart_uti → trimmed | Aligned ✓ |
| mart_uts POST_HOOK 1 | pathTime < cutoff → archived to hist_uts_YYYY_HX (pure time-based) | In hist ✓ |
| mart_uts POST_HOOK 2 | No unmatched rows → safety pass | Safety pass ✓ |
| mart_uts POST_HOOK 3 | Pure time-based trim | Aligned ✓ |

All three hist tables receive Order Y simultaneously. Active mart clean. ✓

---

### Example D — Reactivated order (mart_uti — automatic fix)

Order Z archived to hist_uti in Feb 2026. New update arrives April 2026.

| Step | Action | Result |
|---|---|---|
| Extraction | update_time = April 2026, within cutoff | Captured ✓ |
| DELETE | order_id = Z, not in mart_uti → no-op | — |
| INSERT | Order Z re-enters mart_uti | Active again ✓ |
| POST_HOOK 1 | Order Z in new-batch → stale hist_uti entry deleted | hist_uti clean ✓ |
| POST_HOOK 2/3 | Not aged out | No-ops |

---

### Example E — Reactivated order (mart_ecs — exceptions table)

Same Order Z. mart_ecs had its row trimmed in Feb 2026 when order went dormant.

| Step | Action | Result |
|---|---|---|
| mart_ecs POST_HOOK | Order Z IS in mart_uti → not trimmed again | No trim |
| mart_ecs INSERT | add_time = old (Dec 2025), not in extraction window | Not re-inserted |
| State | mart_uti has Order Z, mart_ecs does not | Missing ecs row |
| Daily test | Detects: order_id in mart_uti, not in mart_ecs | Logged to exceptions table |
| Manual fix | Restore from hist_ecs_YYYY_HX, delete stale hist entry, mark resolved | Clean ✓ |

Rare case. Surfaced and fixed manually. No complex code. ✓

---

### Example F — Reactivated order (mart_uts — no action needed)

Same Order Z. mart_uts had its old spath events trimmed to hist_uts in Feb 2026.

| Step | Action | Result |
|---|---|---|
| New spath events | Appended to mart_uts for Order Z | Recent spath in active mart ✓ |
| Old spath events | Remain in hist_uts_YYYY_HX | Correct historical record ✓ |
| Action needed | None | — |

Old spath in hist is not stale — it is the permanent historical record. ✓

---

## 8. Historical Layer

### Structure

All three hist tables follow the same 6-month split convention:

```
hist_uni_tracking_info_2026_h1    (Jan–Jun 2026)
hist_uni_tracking_info_2026_h2    (Jul–Dec 2026)
hist_ecs_order_info_2026_h1       (Jan–Jun 2026)
hist_ecs_order_info_2026_h2       (Jul–Dec 2026)
hist_uni_tracking_spath_2026_h1   (Jan–Jun 2026)
hist_uni_tracking_spath_2026_h2   (Jul–Dec 2026)
```

Consistent structure across all three. Same split cadence. Easier to reason about,
maintain, and query.

### What each hist table contains

| Table | Contains | Write pattern |
|---|---|---|
| hist_uti_YYYY_HX | Last known state of dormant orders | Write-once at archival |
| hist_ecs_YYYY_HX | ECS row for archived orders | Write-once at archival |
| hist_uts_YYYY_HX | All spath events older than 6 months | Write-once at archival |

All hist tables are **permanent and append-only**. Rows are never updated after archival.

### Split column per table

| Table | Split column | Why |
|---|---|---|
| hist_uti_YYYY_HX | `update_time` at archival | Frozen when written — order stays in same table forever |
| hist_ecs_YYYY_HX | `add_time` | Immutable — order creation time never changes |
| hist_uts_YYYY_HX | `pathTime` | Immutable — spath event time never changes |

### hist_uti as the anchor for historical queries

All three hist tables for a given 6-month period contain the same set of order_ids
because all three tables archive together (when mart_uti trims an order, mart_ecs
and mart_uts trim it in the same cycle). Historical queries are always consistent:

```sql
SELECT ...
FROM   hist_uni_tracking_info_2026_h1  uti
JOIN   hist_ecs_order_info_2026_h1     ecs ON uti.order_id = ecs.order_id
JOIN   hist_uni_tracking_spath_2026_h1 uts ON uti.order_id = uts.order_id
WHERE  ecs.partner_id = 382
```

DISTKEY(order_id) on all hist tables — JOIN is co-located, zero shuffle.

---

## 9. Why 6-Month Splits

### Real data volumes

| Source | Peak monthly volume |
|---|---|
| uni_tracking_info_raw | 29.3M rows/month |
| ecs_order_info_raw | 30.9M rows/month |
| uni_tracking_spath_raw | 195.3M rows/month |

### What this means per 6-month hist table

| hist table | Rows per 6-month table |
|---|---|
| hist_uti_YYYY_HX | ~30–90M |
| hist_ecs_YYYY_HX | ~90–175M |
| hist_uts_YYYY_HX | **~1.17B** |

At yearly splits, hist_uts would reach ~2.34B rows per table — unmanageable.
6-month splits cap each hist_uts table at ~1.17B rows.

### MySQL needed 6-month splits for performance. Redshift does not.

| Concern | MySQL | Redshift |
|---|---|---|
| Large table scan | Full row reads, slow | Columnar — reads only needed columns |
| Index maintenance | B-tree degrades with volume | Zone maps are automatic |
| JOIN cost | Data redistribution, expensive | DISTKEY co-location, zero shuffle |
| Single node limit | Yes — splits reduce per-table size | No — MPP is native |

Active mart tables never need splits — retention caps them permanently.
6-month splits on hist tables are for **operational management only**
(drop a period, archive to S3, compliance) — not for query performance.

---

## 10. Exceptions Table

All data quality issues are surfaced to a single table. One place to monitor.

```sql
CREATE TABLE order_tracking_exceptions (
    order_id        BIGINT        NOT NULL,
    exception_type  VARCHAR(100)  NOT NULL,
    detected_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at     TIMESTAMP,
    notes           VARCHAR(500),
    UNIQUE (order_id, exception_type)   -- informational in Redshift (not enforced);
                                        -- enforcement is via NOT EXISTS in the INSERT
) DISTKEY(order_id) SORTKEY(detected_at);
```

**Enforcement note**: Redshift does not enforce UNIQUE constraints at write time.
Always use a `NOT EXISTS` guard in every INSERT to this table:
```sql
INSERT INTO order_tracking_exceptions (order_id, exception_type, notes)
SELECT ..., 'EXCEPTION_TYPE', 'detail'
WHERE NOT EXISTS (
    SELECT 1 FROM order_tracking_exceptions
    WHERE order_id      = :oid
      AND exception_type = 'EXCEPTION_TYPE'
      AND resolved_at   IS NULL
);
```

### Exception types

| exception_type | Detected by | Root cause |
|---|---|---|
| `REACTIVATED_ORDER_MISSING_ECS` | order in mart_uti, not in mart_ecs | Order dormant >6m, ecs row trimmed, order reactivated |
| `UTI_UTS_TIME_MISMATCH` | update_time ≠ max(pathTime) | Backdated update_time, missed extraction, sync drift |
| `ORDER_MISSING_SPATH` | order in mart_uti, no spath in mart_uts or hist_uts | Genuine data integrity issue |

### Alert rule

```sql
-- Fire alert if any unresolved exceptions exist
SELECT COUNT(*) FROM order_tracking_exceptions WHERE resolved_at IS NULL;
-- Expected: 0
```

---

## 11. Query Patterns

### Primary query — 3-way JOIN

```sql
SELECT ...
FROM   mart_ecs_order_info        ecs
JOIN   mart_uni_tracking_spath    uts  ON ecs.order_id = uts.order_id
JOIN   mart_uni_tracking_info     uti  ON ecs.order_id = uti.order_id
WHERE  ecs.partner_id  IN (382)
AND    uts.pathTime    >= unix_timestamp('2025-07-17 00:00:00')
AND    uts.code        = 200
```

| Step | Mechanism | Effect |
|---|---|---|
| pathTime >= X | SORTKEY(pathTime) zone maps | Skips all blocks before the date |
| partner_id filter | SORTKEY(partner_id) zone maps | Cuts ecs to ~0.3% of rows |
| JOIN mart_ecs | DISTKEY(order_id) | Zero cross-node shuffle |
| JOIN mart_uti | DISTKEY(order_id) | Zero cross-node shuffle |

### tno (tracking number) filter

```sql
SELECT ...
FROM   mart_uni_tracking_info     uti
JOIN   mart_ecs_order_info        ecs  ON uti.order_id = ecs.order_id
JOIN   mart_uni_tracking_spath    uts  ON uti.order_id = uts.order_id
WHERE  uti.tno = 'SF1234567890'
```

`tno` is a filter on mart_uti — columnar scan on tno column. No separate lookup table.
DISTKEY(order_id) join unchanged.

### Historical query

```sql
SELECT ...
FROM   hist_uni_tracking_info_2026_h1   uti
JOIN   hist_ecs_order_info_2026_h1      ecs  ON uti.order_id = ecs.order_id
JOIN   hist_uni_tracking_spath_2026_h1  uts  ON uti.order_id = uts.order_id
WHERE  ecs.partner_id = 382
```

Same physical design as active mart. Scoped to one 6-month table.

### Query spanning active + historical

If a query needs both recent and historical data for the same order:

```sql
SELECT * FROM mart_uni_tracking_spath  WHERE order_id = :oid
UNION ALL
SELECT * FROM hist_uni_tracking_spath_2026_h1 WHERE order_id = :oid
UNION ALL
SELECT * FROM hist_uni_tracking_spath_2025_h2 WHERE order_id = :oid
-- add periods as needed
```

DISTKEY(order_id) on all tables — each leg of the UNION is a co-located lookup.

---

## 12. Data Quality Tests

All tests run daily as Airflow tasks. Results written to `order_tracking_exceptions`.

### Test 1 — update_time vs max(pathTime) alignment

Every order has at least one spath creation event. `update_time` in mart_uti should
equal `MAX(pathTime)` in mart_uts. Mismatch = backdated update_time, missed extraction,
or uti/uts sync drift.

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

**Expected**: zero rows.

---

### Test 2 — Orders missing from spath anywhere

An order in mart_uti with no spath in mart_uts is expected if spath events aged to
hist_uts. But missing from both mart_uts AND hist_uts is a genuine integrity problem.

```sql
SELECT uti.order_id
FROM mart_uni_tracking_info uti
LEFT JOIN mart_uni_tracking_spath  uts  ON uti.order_id = uts.order_id
LEFT JOIN hist_uni_tracking_spath  hist ON uti.order_id = hist.order_id
WHERE uts.order_id  IS NULL
  AND hist.order_id IS NULL
```

Note: `hist` here is a UNION view across all `hist_uts_YYYY_HX` tables.
**Expected**: zero rows.

---

### Test 3 — Reactivated orders missing from mart_ecs

```sql
SELECT uti.order_id
FROM mart_uni_tracking_info uti
LEFT JOIN mart_ecs_order_info ecs ON uti.order_id = ecs.order_id
WHERE ecs.order_id IS NULL
```

**Expected**: zero rows. Any rows → logged as `REACTIVATED_ORDER_MISSING_ECS`.

---

## 13. Table Sizes — Real Volumes

Peak monthly volumes: uti = 29.3M, ecs = 30.9M, uts = 195.3M rows.

### Active mart (steady state)

| Table | Rows | Basis |
|---|---|---|
| mart_uti | ~90–175M | Unique active orders, 6-month window |
| mart_ecs | ~90–175M | Mirrors mart_uti (order_id-driven retention) |
| mart_uts | ~1.17B | 195.3M/month × 6 months |

### Historical (per 6-month table)

| Table | Rows per YYYY_HX | Annual accumulation |
|---|---|---|
| hist_uti_YYYY_HX | ~30–90M | ~60–180M |
| hist_ecs_YYYY_HX | ~90–175M | ~180–350M |
| hist_uts_YYYY_HX | ~1.17B | ~2.34B |

---

## 14. DAG Dependency

```
Extraction (parallel)
        ↓
mart_uni_tracking_info        ← must complete first
        ↓
mart_ecs_order_info ──┐       ← depends on mart_uti (order_id NOT IN mart_uti)
                      ├──── parallel
mart_uni_tracking_spath─┘     ← independent (pure time-based), runs after mart_uti
```

**Why mart_ecs depends on mart_uti**: the LEFT JOIN anti-join condition in mart_ecs
post_hook reads mart_uti's current state. If mart_uti is still running, the condition
returns wrong results.

**Why mart_uts can run alongside mart_ecs**: mart_uts uses pure time-based retention
with no dependency on mart_uti state.

### DAG configuration — required settings

```python
dag = DAG(
    'order_tracking_pipeline',
    schedule_interval='*/15 * * * *',
    max_active_runs=1,           # REQUIRED: prevents concurrent cycles overlapping
    catchup=False,
)

# Per-task execution timeout — catches runaway cycles before the next one starts
dbt_mart_uti = BashOperator(
    task_id='dbt_mart_uti',
    execution_timeout=timedelta(minutes=10),
    ...
)
```

**Why `max_active_runs=1` is required**: if mart_uti takes longer than 15 minutes
(large backfill, Redshift contention), the next scheduled run starts. The second run's
`MAX(update_time)` watermark query reads mart_uti while the first run is mid-write,
producing incorrect extraction windows. `max_active_runs=1` ensures cycles are always
serial.

### Table lock window — known trade-off

Redshift's `delete+insert` strategy holds an **exclusive table-level lock** on mart_uti
for the duration of the DELETE, INSERT, and all post_hooks. Readers block during this
window.

```
Estimated lock duration per cycle:  ~2–3 minutes
Cycle frequency:                    every 15 minutes
Lock fraction of wall clock:        ~13–20%
```

**Mitigations**:
- WLM queue separation (§16): route reads to a separate queue so they queue rather than starve
- Redshift Concurrency Scaling: route read queries to auto-scaling clusters during write windows
- Accept it: for internal operational queries (not SLA-bound), a 2-min block every 15 min is tolerable
- If unacceptable: reintroduce Option A (stg_uti as 20-day dedup buffer; mart_uti updates from staging on a slower, off-peak cadence)

---

## 15. Operational Calendar

### Before launch (Day 0)

Create all hist tables for the current and previous 6-month period before deploying
mart models. The retention post_hook fires on the very first run — hist tables must
exist before that.

```sql
-- Create for current and adjacent periods
CREATE TABLE hist_uni_tracking_info_2025_h2  (...);
CREATE TABLE hist_uni_tracking_info_2026_h1  (...);
CREATE TABLE hist_ecs_order_info_2025_h2     (...);
CREATE TABLE hist_ecs_order_info_2026_h1     (...);
CREATE TABLE hist_uni_tracking_spath_2025_h2 (...);
CREATE TABLE hist_uni_tracking_spath_2026_h1 (...);
```

### Every 1 July and 1 January

Create the new 6-month hist tables, update post_hook routing, and rotate the
mart_uti stale-entry cleanup window:

```
1 Jul 2026:
  Create hist_*_2026_h2 tables.
  Add 2026-h2 routing to archive post_hooks.
  mart_uti step 1: update table name → DELETE from hist_uni_tracking_info_2026_h1

1 Jan 2027:
  Create hist_*_2027_h1 tables.
  Add 2027-h1 routing to archive post_hooks.
  mart_uti step 1: update table name → DELETE from hist_uni_tracking_info_2026_h2

(repeat every 6 months — always the single latest hist_uti table)
```

### Ongoing monitoring

| Frequency | Task |
|---|---|
| Every cycle (15 min) | Post_hooks archive and trim automatically |
| Daily | Run Test 1, Test 2, Test 3 → write to exceptions table |
| Daily | Alert if exceptions table has unresolved rows |
| Monthly | Review mart_uts VACUUM cost — reduce retention to 3 months if VACUUM >1 hour |

---

---

## 16. VACUUM Strategy

Redshift does not physically delete rows immediately. DELETEs mark rows as ghost rows —
they remain on disk and count against I/O until VACUUM reclaims them. With high-frequency
retention trims every 15 minutes, ghost rows accumulate fast and degrade performance.

### Why VACUUM is critical here

Every 15-minute cycle fires DELETE statements on mart_uti, mart_ecs, and mart_uts.
Without VACUUM:
- Ghost rows bloat table size on disk
- Zone map effectiveness degrades (ghost rows occupy blocks, reducing pruning precision)
- Query and DELETE scan times increase
- Redshift auto-vacuum runs during quiet periods but may not keep pace with 96 cycles/day

### VACUUM type per table

| Table | VACUUM type | Reason |
|---|---|---|
| mart_uti | `VACUUM DELETE ONLY` | High DELETE rate every cycle — reclaim ghost rows |
| mart_ecs | `VACUUM DELETE ONLY` | Periodic large deletes (order_id-driven trim) |
| mart_uts | `VACUUM DELETE ONLY` | High DELETE rate every cycle — reclaim ghost rows |
| mart_ecs | `VACUUM SORT ONLY` | Rows arrive in add_time order, not partner_id — zone maps degrade |
| hist tables | `VACUUM DELETE ONLY` | Rare — only if rows are ever deleted from hist |

### mart_ecs SORTKEY degradation — special case

mart_ecs has `SORTKEY(partner_id, add_time, order_id)`. New orders arrive in `add_time`
order (chronological) but `partner_id` is random across new rows. This means new rows
land in the unsorted region — zone maps on `partner_id` degrade as unsorted rows accumulate.

```sql
-- Check unsorted percentage for mart_ecs
SELECT  tbl_rows, unsorted, sortkey1
FROM    svv_table_info
WHERE   "table" = 'mart_ecs_order_info';
```

When `unsorted` > 10–15%, run:
```sql
VACUUM SORT ONLY mart_ecs_order_info;
```

Expected cadence: weekly or after large batch loads. Tracks with load volume.

### Recommended VACUUM schedule

```
mart_uti:   VACUUM DELETE ONLY — daily (nightly, off-peak)
mart_ecs:   VACUUM DELETE ONLY — weekly
            VACUUM SORT ONLY   — conditional, NOT weekly (see below)
mart_uts:   VACUUM DELETE ONLY — daily (nightly, off-peak)
hist tables: VACUUM on demand — only after manual deletes or corrections
```

**mart_ecs VACUUM SORT — conditional trigger, not fixed cadence**:
At ~200K new orders/day in random `partner_id` order, the 15% unsorted threshold is
reached approximately every 65–130 days — not weekly. Running VACUUM SORT weekly wastes
cluster resources. Use an Airflow sensor that checks `svv_table_info.unsorted` and
triggers VACUUM SORT only when the threshold is crossed:

```python
# Airflow task: conditional VACUUM SORT for mart_ecs
def vacuum_sort_if_needed(conn):
    result = conn.execute("""
        SELECT unsorted FROM svv_table_info
        WHERE "table" = 'mart_ecs_order_info'
    """).fetchone()
    unsorted_pct = result[0] if result else 0
    if unsorted_pct > 15:
        conn.execute("VACUUM SORT ONLY mart_ecs_order_info")
        return f"VACUUM SORT triggered at {unsorted_pct:.1f}% unsorted"
    return f"Skipped — unsorted at {unsorted_pct:.1f}%"
```

Run this check weekly. The VACUUM SORT itself fires only when actually needed.

### VACUUM monitoring query

```sql
-- Check ghost rows and unsorted rows across all mart tables
SELECT
    "table",
    tbl_rows,
    unsorted,
    stats_off,
    diststyle,
    sortkey1
FROM svv_table_info
WHERE "table" IN (
    'mart_uni_tracking_info',
    'mart_ecs_order_info',
    'mart_uni_tracking_spath'
)
ORDER BY "table";
```

**Thresholds to act on**:
- `unsorted` > 15% → run VACUUM SORT ONLY
- `stats_off` > 10 → run ANALYZE (see §17)
- Ghost rows = `tbl_rows - rows_in_data` — estimate by comparing SELECT COUNT(*) vs tbl_rows in svv_table_info

### WLM (Workload Management)

Run VACUUM in a separate WLM queue from the pipeline. VACUUM competes for cluster
resources. Schedule nightly VACUUM in an off-peak window that does not overlap
with the 15-minute extraction cycle.

```
Pipeline WLM queue:  high priority, short-running queries
VACUUM WLM queue:    low priority, maintenance window (e.g. 2–5am)
```

---

## 17. ANALYZE Strategy

Redshift uses table statistics to build query execution plans. Stale statistics
cause poor plans — full scans instead of zone map pruning, wrong join order.

### When ANALYZE is needed

- After the initial full load (before any queries run)
- After bulk inserts that change >10% of the table
- When `stats_off` in `svv_table_info` is > 10

### ANALYZE schedule

```sql
-- Run after initial load
ANALYZE mart_uni_tracking_info;
ANALYZE mart_ecs_order_info;
ANALYZE mart_uni_tracking_spath;
```

For ongoing operations, Redshift auto-analyzes after significant changes.
Manual ANALYZE is only needed if query plans degrade unexpectedly.

### Check if ANALYZE is needed

```sql
SELECT "table", stats_off
FROM svv_table_info
WHERE "table" IN (
    'mart_uni_tracking_info',
    'mart_ecs_order_info',
    'mart_uni_tracking_spath'
)
ORDER BY stats_off DESC;
```

`stats_off > 10` → run ANALYZE on that table.

---

## 18. Table Health Monitoring

### Primary health check query

Run weekly. Surfaces any table requiring VACUUM or ANALYZE.

```sql
SELECT
    "table",
    tbl_rows                                AS total_rows,
    unsorted                                AS unsorted_pct,
    stats_off                               AS stats_staleness,
    ROUND(size / 1024.0, 2)                AS size_gb,
    sortkey1,
    diststyle
FROM svv_table_info
WHERE schema = 'settlement_ods'
ORDER BY size DESC;
```

### Ghost row estimate

```sql
-- Ghost rows = rows marked deleted but not yet vacuumed
-- Approximated by comparing query count vs svv_table_info count
SELECT
    'mart_uni_tracking_info'        AS tbl,
    COUNT(*)                        AS live_rows
FROM mart_uni_tracking_info
UNION ALL
SELECT 'mart_ecs_order_info', COUNT(*) FROM mart_ecs_order_info
UNION ALL
SELECT 'mart_uni_tracking_spath', COUNT(*) FROM mart_uni_tracking_spath;
-- Compare with tbl_rows in svv_table_info — difference = ghost rows
```

### Zone map effectiveness check

```sql
-- Run EXPLAIN on primary query to verify zone map pruning
EXPLAIN
SELECT COUNT(*)
FROM mart_uni_tracking_spath
WHERE pathTime >= unix_timestamp('2026-01-01');
-- Look for "rows removed by filter" in the plan — high removal = zone maps working
```

If zone maps are not pruning (full scan despite SORTKEY), table needs VACUUM SORT.

### Monitoring summary

| Metric | Source | Threshold | Action |
|---|---|---|---|
| Unsorted % | svv_table_info.unsorted | > 15% | VACUUM SORT ONLY |
| Stats staleness | svv_table_info.stats_off | > 10 | ANALYZE |
| Ghost rows | tbl_rows vs COUNT(*) | > 20% | VACUUM DELETE ONLY |
| Table size | svv_table_info.size | Unexpected growth | Check retention post_hook |
| Unresolved exceptions | order_tracking_exceptions | > 0 | Investigate + fix |

---

## 19. Post-Hook Failure Handling

Each post_hook is a separate SQL statement. If a step fails partway through,
the pipeline leaves the tables in a partially processed state.

### Failure scenarios and recovery

Post_hooks now have 4 steps (mart_uti) / 3 steps (mart_uts) / 2 steps (mart_ecs).
Each step is a separate committed SQL statement. dbt stops running subsequent
post_hooks in the list if a step raises an error.

**Scenario 1: Archive (step 2) succeeds, safety check (step 3) or trim (step 4) fails**

```
State:  hist_YYYY_HX has the rows ✓ (archived)
        mart_uti still has the same rows (trim not yet fired)

Fix:    On next successful cycle:
        - Archive step re-runs but NOT EXISTS guard skips already-archived rows ✓
        - Safety check re-runs — same result (rows are in hist, no exceptions logged)
        - Trim fires cleanly ✓
        No data loss. Duplicate in hist never occurs due to NOT EXISTS guard.
```

**Scenario 2: Trim (step 4) fires, but archive (step 2) was never reached**

Data loss without the safety net.

```
Prevention (structural): dbt runs post_hooks in list order. Step 4 cannot run
before step 2 unless step 2 was deliberately removed from the list.
Step 3 (safety check) between archive and trim provides a second layer —
if a row is not in hist, it is logged to exceptions and EXCLUDED from trim.
```

The safety check in step 3 is the primary guard against this scenario. An
`ARCHIVE_ROUTING_GAP` exception alerts the team before any data is lost.

**Scenario 3: mart_ecs LEFT JOIN anti-join against partial mart_uti**

The mart_ecs trim uses `LEFT JOIN mart_uti WHERE uti.order_id IS NULL`. If mart_uti
is still mid-write, this anti-join sees a partial active set — it may incorrectly
identify active orders as inactive and trim them prematurely.

```
Prevention: DAG enforces mart_uti completes before mart_ecs starts (§14).
            max_active_runs=1 prevents concurrent cycles from interleaving.
```

**Scenario 4: Concurrent cycle starts before current one finishes**

```
Prevention: max_active_runs=1 on the Airflow DAG (§14).
            execution_timeout per task ensures runaway cycles are killed
            before the next scheduled run.
```

### Post_hook idempotency

All archive INSERTs use a NOT EXISTS guard — safe to retry any number of times:
```sql
-- mart_uti archive step (idempotent):
INSERT INTO hist_uni_tracking_info_2026_h1
SELECT * FROM mart_uni_tracking_info
WHERE update_time < cutoff
  AND update_time >= unix_timestamp('2026-01-01')
  AND update_time <  unix_timestamp('2026-07-01')
  AND order_id NOT IN (SELECT order_id FROM hist_uni_tracking_info_2026_h1);
-- If re-run after partial failure: already-archived rows are skipped. ✓
```

Trim DELETEs are naturally idempotent — re-running when rows are already gone is a
no-op. The trim condition (`update_time < MAX - 6 months`) is deterministic and
produces the same result on every retry.

---

## 20. Initial Load Procedure

### What happens on the first dbt run

The first run is a full load (`is_incremental()` = false). All rows in raw tables
are loaded into mart tables. The retention post_hook fires immediately after —
any rows older than 6 months are archived to hist tables and trimmed from mart.

**hist tables must exist before the first run.** See §15 (Day 0 steps).

### Seeding from existing staging tables

If `stg_uni_tracking_info`, `stg_ecs_order_info`, `stg_uni_tracking_spath` exist
with data, seed the mart tables from staging before switching queries:

```sql
-- Seed mart_uti from existing staging
INSERT INTO mart_uni_tracking_info
SELECT * FROM stg_uni_tracking_info;

-- Seed mart_ecs from existing staging
INSERT INTO mart_ecs_order_info
SELECT * FROM stg_ecs_order_info;

-- Seed mart_uts from existing staging
INSERT INTO mart_uni_tracking_spath
SELECT * FROM stg_uni_tracking_spath;
```

Then run the retention post_hooks manually to trim to 6-month window and populate
hist tables before the live pipeline starts.

### First run retention behaviour

On the first run, if raw tables contain data older than 6 months:
- mart_uti loads all data → retention post_hook archives rows > 6 months to hist_uti → trims them
- mart_ecs loads all data → retention post_hook archives inactive orders to hist_ecs → trims
- mart_uts loads all data → retention post_hook archives events > 6 months to hist_uts → trims

After the first run, all three active mart tables hold only the 6-month window. ✓

### ANALYZE after initial load

Always run ANALYZE on all three mart tables after the initial full load before any
production queries are served:

```sql
ANALYZE mart_uni_tracking_info;
ANALYZE mart_ecs_order_info;
ANALYZE mart_uni_tracking_spath;
```

---

## 21. hist Table Lifecycle

### Retention policy for hist tables

hist tables are permanent by design — they are the long-term record of all
orders that have passed through the system. There is no automated deletion.

Dropping a hist table means permanently losing the data for that period.
Only drop a hist table when:
- The period is beyond the agreed business retention window (e.g. 3 years)
- Data has been archived to cold storage (S3 Glacier or equivalent) first
- Approval has been obtained from the data owner

### Archiving hist tables to S3 before dropping

```sql
-- Unload hist table to S3 before dropping
UNLOAD ('SELECT * FROM hist_uni_tracking_spath_2025_h2')
TO 's3://your-bucket/archive/hist_uts_2025_h2/'
IAM_ROLE 'arn:aws:iam::...'
FORMAT AS PARQUET;

-- Verify S3 file count before dropping table
DROP TABLE hist_uni_tracking_spath_2025_h2;
```

### hist table growth over time

| Year | hist_uts tables | Total hist_uts rows |
|---|---|---|
| End of 2026 | 2026_h1, 2026_h2 | ~2.34B |
| End of 2027 | + 2027_h1, 2027_h2 | ~4.68B |
| End of 2028 | + 2028_h1, 2028_h2 | ~7.0B |

Plan for S3 archival of the oldest hist_uts table approximately every 2 years
based on storage cost vs access frequency.

### hist table access pattern

hist tables are queried infrequently (historical lookups, audit, reporting).
They do not need to be in the same Redshift cluster as the active mart if costs
become a concern — they can be moved to a separate cluster or Redshift Spectrum
(query S3 directly via external tables).

---

## 22. Known Gaps

### Gap 1 — ECS post-creation updates

The old Kettle pipeline co-extracted ECS on every uti update via JOIN, so ECS
fields were always refreshed. The new pipeline has no `update_time` in
`ecs_order_info` — changes to ECS fields after order creation are not detected.

**Impact**: Low — primary query filters on `partner_id` (set at creation, immutable).

**Action**: Run `compare_pipelines.py --table ecs`. If mismatch rate is low, accept
as known limitation. If high, design an extraction-level fix (e.g. full daily ECS
refresh for orders active in mart_uti).

### Gap 2 — spath event updates

The old Kettle pipeline handled `is_updated = 1` via full DELETE + INSERT per order
for spath. The new pipeline is append-only — updated spath events are not recaptured.

**Impact**: Low — primary query filters on `code` (immutable once scanned).

**Action**: Run `compare_pipelines.py --table uts`. If mismatch rate is high, change
mart_uts to delete+insert by composite key (`order_id + traceSeq + pathTime`).

### Gap 3 — Backdated update_time

Bad operational practice in source MySQL — someone manually sets `update_time`
to a past date. The incremental extraction (`WHERE update_time >= source_cutoff`)
misses the row. mart_uti retains the pre-backdate value indefinitely.

**Detection**: Test 1 (§12) catches this — `update_time` diverges from `MAX(pathTime)`.

**Mitigation**: No automatic fix. Test 1 alerts on divergence. Manual re-extraction
of the affected order_id resolves it.

### Gap 4 — Late-arriving data with old timestamps

If a row appears in MySQL with an `update_time` that falls below `source_cutoff`
(e.g. a partner system backfills old data with original timestamps), incremental
extraction misses it.

**Detection**: Not automatically detected — Test 1 may surface it indirectly.

**Mitigation**: If this is a known risk, add a daily wide-window re-extraction
job (last 30 days) as a separate Airflow task. Covers the majority of backfill scenarios.

---

*For full design history, approaches considered and rejected, see `order_tracking_master_design.md`.*
