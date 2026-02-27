# Order Tracking Pipeline — Logic Walkthrough v2

**Updated**: 2026-02-27
**Purpose**: Explains the reasoning and logic behind every pipeline stage — the WHAT and WHY.
Read this to understand how the system works and why it was designed this way.
For the HOW (code-level walkthrough), see `code_walkthrough_v2.md`.

---

## Table of Contents

1. [The Problem We Are Solving](#1-the-problem-we-are-solving)
2. [Pipeline Architecture — The Big Picture](#2-pipeline-architecture--the-big-picture)
3. [The Three Source Tables](#3-the-three-source-tables)
4. [Extraction Layer — CDC Strategy and Watermarks](#4-extraction-layer--cdc-strategy-and-watermarks)
5. [Raw Tables — The Landing Zone](#5-raw-tables--the-landing-zone)
6. [dbt Staging Models — Not Used in Main Pipeline](#6-dbt-staging-models--not-used-in-main-pipeline)
7. [dbt Mart Models — The Active Dataset](#7-dbt-mart-models--the-active-dataset)
8. [mart_uni_tracking_info — Delete+Insert Anchor](#8-mart_uni_tracking_info--deleteinsert-anchor)
9. [mart_ecs_order_info — Order-ID-Driven Retention](#9-mart_ecs_order_info--order-id-driven-retention)
10. [mart_uni_tracking_spath — Pure Time-Based Retention](#10-mart_uni_tracking_spath--pure-time-based-retention)
11. [Retention Logic — Why the Three Tables Are Different](#11-retention-logic--why-the-three-tables-are-different)
12. [Post-Hook Sequences — Why Order Matters](#12-post-hook-sequences--why-order-matters)
13. [Historical Layer — 6-Month Splits](#13-historical-layer--6-month-splits)
14. [Reactivation Handling](#14-reactivation-handling)
15. [Redshift Physical Design](#15-redshift-physical-design)
16. [DAG Dependency and Sequencing](#16-dag-dependency-and-sequencing)
17. [Monitoring DAG — Daily Data Quality Checks](#17-monitoring-dag--daily-data-quality-checks)
18. [VACUUM DAG — Ghost Row Management](#18-vacuum-dag--ghost-row-management)
19. [The 15-Minute Cycle End-to-End](#19-the-15-minute-cycle-end-to-end)

---

## 1. The Problem We Are Solving

An order tracking system generates continuous, high-volume updates. An order is created,
moves through logistics states (picked up, in transit, out for delivery, delivered), and
eventually goes dormant. Each state change produces new records across multiple source tables.

We need to answer questions like:
- What is the current status of all active orders for partner X?
- Which orders had a scan event in the last 7 days?
- What was the full tracking history of order Y?

These questions require three conditions to hold simultaneously at all times:
1. Each order has exactly one current-status row — no duplicates, no stale rows.
2. All spath scan events for an order are available.
3. Order metadata (partner, creation time) is always reachable for any active order.

The challenge: orders have varying lifetimes. Some complete in 3 days, others remain active
for 90+ days. At peak, 195M spath events and 29M tracking-info updates arrive per month.
The system must handle both correctly and efficiently.

---

## 2. Pipeline Architecture — The Big Picture

```
MySQL Source (kuaisong schema)
  ├── ecs_order_info       (add_time, ~500K/day normal, ~2M/day peak)
  ├── uni_tracking_info    (update_time, ~500K/day normal, ~2M/day peak)
  └── uni_tracking_spath   (pathTime, ~2M/day normal, ~8M/day peak)
         │
         │  Airflow: extract every 15 min via timestamp_only CDC
         │  Lower bound: watermark from S3
         │  Upper bound: now - 5min buffer
         ▼
RAW TABLES (settlement_public schema, Redshift)
  ├── ecs_order_info_raw
  ├── uni_tracking_info_raw
  └── uni_tracking_spath_raw
         │
         │  trim_raw: uti_raw and uts_raw trimmed at 24h
         │             ecs_raw NOT trimmed by add_time (see §5)
         │
         │  dbt: incremental delete+insert
         ▼
ACTIVE MART (settlement_ods schema, Redshift)
  ├── mart_uni_tracking_info   — one row per active order, latest tracking state
  ├── mart_ecs_order_info      — one row per active order, order creation metadata
  └── mart_uni_tracking_spath  — 6-month rolling window of spath scan events
         │
         │  archive-before-trim via dbt post-hooks
         ▼
HISTORICAL LAYER (settlement_ods schema, Redshift)
  ├── hist_uni_tracking_info_YYYY_HX   — 6-month splits
  ├── hist_ecs_order_info_YYYY_HX      — 6-month splits
  └── hist_uni_tracking_spath_YYYY_HX  — 6-month splits
         │
         │  daily DQ checks write anomalies here
         ▼
EXCEPTIONS TABLE
  └── order_tracking_exceptions   — rare data integrity issues for manual review
```

Three separate Airflow DAGs run this system:
- `order_tracking_hybrid_dbt_sync`: the main 15-minute pipeline (extract + dbt + trim)
- `order_tracking_vacuum`: VACUUM DELETE ONLY + SORT ONLY, runs at 2am UTC daily
- `order_tracking_daily_monitoring`: 4 DQ checks, runs at 3am UTC daily

---

## 3. The Three Source Tables

```
ecs_order_info         uni_tracking_info      uni_tracking_spath
───────────────        ─────────────────      ──────────────────
order_id (PK)          order_id               order_id
add_time (ts)          update_time (ts)       pathTime (ts)
partner_id             tno                    code
...                    status                 traceSeq
                       ...                    ...

One row per order      One row per order       Many rows per order
Written once,          Updated on every        Written once per event,
never changes          status change           never changes
```

Key asymmetry that drives all design decisions:
- `ecs` is write-once: an order's creation metadata never changes. Timestamp `add_time` is when
  the order was created — it does not advance as the order progresses.
- `uti` changes frequently: `update_time` advances with every status change. One row per order
  at all times, always the latest state.
- `uts` is append-only: every scan event produces a new row. An order accumulates many spath
  rows over its lifetime. They are never modified.

All three tables join on `order_id`. An order appears in all three simultaneously from
creation until it is archived to the historical layer.

---

## 4. Extraction Layer — CDC Strategy and Watermarks

### Strategy: timestamp_only with both lower and upper bounds

The CDC strategy is `timestamp_only`. Each extraction run pulls rows where:

```
WHERE ts > watermark_unix AND ts < end_time
```

- **Lower bound**: the watermark stored in S3. The watermark value is an ISO datetime string
  (e.g. `'2026-02-27 04:15:17'`), which the extraction framework converts to a unix epoch
  integer for the SQL query. This conversion is critical — MySQL unix epoch comparisons are
  faster and unambiguous.

- **Upper bound**: `end_time = now - 5 minutes`. The 5-minute buffer prevents pulling rows
  from in-flight transactions that have not yet committed. This is computed once per cycle
  by the `calculate_sync_window` task and passed to all three extraction tasks as `--end-time`.

The extraction window is typically 15 minutes wide (the DAG schedule interval), but can
be longer if a cycle was slow or if there was downtime.

### Why wall clock time, not Airflow's data_interval_start

The pipeline uses `datetime.utcnow()` (wall clock) to compute `end_time`, not Airflow's
`data_interval_start`. Using `data_interval_start` caused stale windows when Airflow ran
a backlogged scheduled slot — for example, a February 21 slot running on February 24 would
produce a window ending February 21, missing 3 days of data.

Since the watermark handles idempotency (the lower bound is always where we left off),
strict Airflow interval tracking is not needed. Wall clock time is always correct.

### First-run seeding

On the very first run, watermarks do not exist in S3. Without seeding, the extraction
framework defaults the lower bound to unix epoch 0 and would attempt to crawl every row
ever written — potentially hundreds of millions of rows, hitting the 100K batch LIMIT
per cycle, and falling further behind with every run.

To prevent this cold-start crawl, `seed_watermarks_to_now.py` is run once before the
pipeline is activated. It writes a v2.0 watermark JSON to S3 with `last_timestamp = now`.
The next run starts from "now" and only processes new rows going forward.

### Three tables extracted in parallel

The three extraction tasks (`extract_ecs`, `extract_uti`, `extract_uts`) run in parallel
within an Airflow TaskGroup. Each runs `python -m src.cli.main sync pipeline` with its own
table name and the shared `--end-time` from XCom. Extraction writes a JSON result file
to `/tmp/hybrid_{table}_{date}_{time}.json` for validation downstream.

### Validation gates the dbt layer

After extraction, `validate_extractions()` reads the three JSON result files. If any
extraction returned `status != 'success'` — due to a connection error, timeout, or data
issue — `validate_extractions()` raises a `ValueError`, which marks the task as failed and
blocks all downstream dbt tasks from running. This prevents dbt from running incremental
models against an incomplete or stale raw table.

Zero-row extractions are NOT failures. At off-peak hours it is legitimate for a 15-minute
window to have no new rows. These are logged as warnings and the cycle continues normally.

---

## 5. Raw Tables — The Landing Zone

Raw tables are append-only landing zones. Each extraction cycle appends new rows to the raw
table. They are not deduplicated and they accumulate data across cycles.

Raw tables serve as a replay buffer — if a dbt run fails, the raw data is still present
and can be reprocessed by the next cycle or by a manual re-run. The 24-hour retention
window is sufficient: the pipeline runs every 15 minutes, so the last 24 hours contains
the last 96 extraction cycles.

### Why ecs_order_info_raw is NOT trimmed by add_time

The `trim_raw` task deletes rows older than 24 hours from `uni_tracking_info_raw` and
`uni_tracking_spath_raw`. It explicitly skips `ecs_order_info_raw`.

The reason: `add_time` on `ecs_order_info` is the order creation time, not the extraction
time. An order created 3 months ago but extracted yesterday has `add_time = 3 months ago`.
If we trimmed `ecs_order_info_raw` by `add_time`, we would delete perfectly valid raw rows
whose orders were only recently extracted — before mart_ecs had a chance to process them.

`ecs_order_info_raw` is instead cleaned up indirectly: mart_ecs's post-hook trims inactive
orders by order_id (not by add_time), so once mart_ecs has processed a row, the raw entry
for that order is no longer needed. Manual or separate cleanup of raw ecs rows can occur
after mart_ecs confirms the order is present.

---

## 6. dbt Staging Models — Not Used in Main Pipeline

Staging models (`stg_ecs_order_info`, `stg_uni_tracking_info`, `stg_uni_tracking_spath`)
exist in the dbt project and are kept up to date, but the main DAG
(`order_tracking_hybrid_dbt_sync`) does NOT run them.

The main pipeline goes raw → mart directly. Staging was originally designed as an
intermediate deduplication layer, but was superseded by in-mart deduplication (the ranked
CTE within each mart model). The mart models are self-sufficient: they read from raw,
deduplicate with `row_number()`, and apply the correct incremental strategy.

The staging models remain in the project for:
- Historical reference and comparison
- Alternative query patterns that need a 20-day compact working set
- Testing and validation scenarios

Their design is documented in `stg_uti_dedup_final.md`.

---

## 7. dbt Mart Models — The Active Dataset

All three mart models use dbt's `incremental` materialization with the `delete+insert`
strategy. Each model runs as part of every 15-minute cycle.

The incremental logic works like this on each run:
1. Compute a `source_cutoff`: the maximum relevant timestamp already in the mart, minus a
   lookback buffer (30 minutes for uti and uts, 2 hours for ecs from the raw table).
2. Read only rows from raw that are newer than `source_cutoff`.
3. Deduplicate within the batch using `row_number()`.
4. DELETE the old rows for those keys from the mart.
5. INSERT the new deduplicated rows.
6. Run post-hooks to archive aged-out rows and trim the mart.

The 30-minute lookback buffer (step 1) means each cycle re-reads a small overlap from the
previous cycle. This is intentional: if a row arrived slightly late in the last extraction
window, it is still picked up in the current cycle without any special handling.

---

## 8. mart_uni_tracking_info — Delete+Insert Anchor

`mart_uni_tracking_info` (mart_uti) stores exactly one row per active order — the latest
tracking state. It is the central anchor of the entire pipeline.

### Why delete+insert (not upsert/merge)

uti has one row per order and that row changes with every status update. When a new update
arrives for order X, the old row must be replaced cleanly. `delete+insert` does this:

1. DELETE the old row: `WHERE order_id IN (batch)` — finds the old row regardless of age.
2. INSERT the deduplicated new row.

The critical point: the DELETE has NO time-window constraint. An order active for 90 days
has its 89-day-old row deleted just as cleanly as an order active for 2 hours. This was the
root cause of a major historical bug: an earlier design used `incremental_predicates` to
restrict DELETE to a 20-day window for performance. Orders with last updates older than 20
days were not found by the DELETE, leaving stale duplicate rows. The fix was to remove the
time window entirely — correctness over performance.

### Source cutoff: mart-relative, 30 minutes

```sql
source_cutoff = MAX(update_time) FROM mart_uni_tracking_info - 1800
```

This queries the mart itself (not the raw table). It represents where the mart is currently
at, minus a 30-minute buffer. Rows in raw that are newer than this are the current batch.

### Deduplication within batch

The mart model uses `row_number() OVER (PARTITION BY order_id ORDER BY update_time DESC)`
to select only the latest row per order_id within the batch window. If the same order
received two status updates in the 30-minute window, only the most recent one enters the mart.

### Retention: 6-month rolling window on update_time

Rows where `update_time < (MAX(update_time) - 15552000 seconds)` are considered aged out.
These are orders that have not had a status update in 6 months — they are dormant.

The 4-step post-hook sequence handles them (see §12).

### Four post-hooks (order is critical)

1. **Stale hist cleanup**: delete any existing hist_uti entry for orders that just re-entered
   the mart this cycle (reactivated orders). Must run BEFORE archive.
2. **Archive to hist**: copy aged-out rows to the correct 6-month hist_uti table.
3. **Safety check**: log any aged-out rows that were not matched by any hist period to
   `order_tracking_exceptions`. These rows are excluded from trim.
4. **Trim**: delete aged-out rows from mart_uti. Excludes exception rows.

---

## 9. mart_ecs_order_info — Order-ID-Driven Retention

`mart_ecs_order_info` (mart_ecs) stores exactly one row per active order — the order creation
metadata (partner_id, add_time, etc.). The data is written once and never changes.

### Why ecs uses a different retention strategy

The naive approach would be to trim mart_ecs by `add_time`: "delete ecs rows whose add_time
is more than 6 months ago." This is wrong for long-lifecycle orders.

Example: Order X was created January 2024. It is still actively receiving status updates in
September 2025. Its `add_time` aged past the 6-month cutoff in July 2024, but its
`update_time` is current. An `add_time`-based trim would remove Order X from mart_ecs, even
though mart_uti still has it. The 3-way JOIN for Order X would return no results — the order
becomes invisible to all queries.

The fix: mart_ecs only trims rows for orders that are no longer in mart_uti. If mart_uti has
order X, mart_ecs keeps it. The retention boundary is mart_uti membership, not add_time age.

### Source cutoff: raw-relative, 2 hours

```sql
source_cutoff = MAX(add_time) FROM ecs_order_info_raw - 7200
```

Note: this queries the raw table (not the mart). This is the key difference from mart_uti.
`ecs_order_info` is write-once — new rows in the raw table are always NEW orders (new
add_time values). The 2-hour lookback from raw's latest is sufficient to catch any late-
arriving new orders from the previous cycle without re-reading historical orders.

### Incremental predicates on mart: 2-hour DELETE window

The `incremental_predicates` setting restricts the DELETE to the last 2 hours of mart_ecs:

```sql
WHERE mart_ecs.add_time > (SELECT COALESCE(MAX(add_time), 0) - 7200 FROM mart_ecs)
```

Since ecs is write-once, old orders (add_time more than 2 hours ago) can never appear in
a new extraction batch. The DELETE window is safe to restrict — it never touches historical
rows, and zone maps on SORTKEY(partner_id, add_time) make the DELETE fast.

### Two post-hooks

1. **Archive inactive orders**: LEFT JOIN anti-join against mart_uti. Orders absent from
   mart_uti are inactive — archive them to the appropriate hist_ecs period table.
2. **Trim**: DELETE USING with the same LEFT JOIN anti-join. Uses NULL-safe LEFT JOIN
   instead of `NOT IN` to avoid the SQL NULL trap (one NULL in mart_uti would make `NOT IN`
   return NULL for every row, preventing any deletions).

### DAG dependency: mart_ecs runs AFTER mart_uti

The post-hooks use `ref('mart_uni_tracking_info')` — they LEFT JOIN against the mart_uti
table to determine which orders are inactive. This `ref()` creates a dbt compile-time
dependency that forces mart_ecs to run after mart_uti. The same holds in the Airflow DAG
where `dbt_mart_ecs` is explicitly downstream of `dbt_mart_uti`.

---

## 10. mart_uni_tracking_spath — Pure Time-Based Retention

`mart_uni_tracking_spath` (mart_uts) stores spath scan events — many rows per order,
representing the full 6-month history of carrier scan events.

### Why uts uses pure time-based retention (not order_id-driven)

If mart_uts used order_id-driven retention like mart_ecs (keep all spath rows for orders
still in mart_uti), it would keep every scan event for every active long-lifecycle order
forever. An order active for 2 years would retain 2 years of spath events in the active
mart, which is unbounded growth.

At 195M spath events per month, pure 6-month retention keeps mart_uts at a stable ceiling:
```
195M events/month × 6 months = ~1.17B rows maximum — stable and predictable
```

Old spath events for still-active orders move to hist_uts after 6 months. This is
acceptable because the primary query pattern always filters `WHERE pathTime >= X` — old
spath events are not needed in the active mart. Historical lookups can union with hist_uts.

### Composite unique key: (order_id, traceSeq, pathTime)

uts is not one-row-per-order. The composite key identifies each unique scan event. The
`row_number()` deduplication partitions by `(order_id, traceSeq)` and picks the latest
`pathTime` — handles the case where the same scan event arrives twice (extraction overlap).

### Three post-hooks

1. **Archive to hist_uts**: copy spath events older than 6 months to the appropriate
   hist_uts period table. Idempotent via `order_id NOT IN` guard scoped to the period range.
2. **Safety check**: log spath events older than 6 months that were not matched by any
   defined period to `order_tracking_exceptions` as `ARCHIVE_ROUTING_GAP_UTS`.
3. **Trim**: delete aged-out spath events, excluding exception rows.

### Independent of mart_uti

mart_uts post-hooks do NOT join against mart_uti (unlike mart_ecs). Retention is purely
time-based on `pathTime`. mart_uts can run in parallel with mart_ecs — both run after
mart_uti, but there is no dependency between them.

---

## 11. Retention Logic — Why the Three Tables Are Different

The three tables require three different retention strategies because of their data shapes:

| Table    | Rows per order | Timestamp meaning       | Retention strategy       | Rationale                                        |
|----------|----------------|------------------------|--------------------------|--------------------------------------------------|
| mart_uti | 1              | update_time = activity | Time-based (6 months)    | update_time directly measures order activity     |
| mart_ecs | 1              | add_time = creation    | Order_id-driven (mart_uti membership) | add_time does not measure activity — long-lifecycle orders would be wrongly trimmed |
| mart_uts | Many           | pathTime = event time  | Pure time-based (6 months pathTime) | Order_id-driven would cause unbounded growth |

The relationship between the three retention strategies:

1. mart_uti trims first (the anchor). Its `update_time` is the most accurate activity signal.
2. mart_ecs trims only for orders that mart_uti has already trimmed. Guarantees JOIN integrity.
3. mart_uts trims old events by time regardless of order activity. Old events for active orders
   are in hist_uts — accessible for historical queries.

The result: all three active mart tables always contain the same set of active order_ids.
The 3-way JOIN is always internally consistent.

---

## 12. Post-Hook Sequences — Why Order Matters

### mart_uti (4 steps)

The 4-step sequence in mart_uti post-hooks must execute in exactly this order:

**Step 1 — Stale hist cleanup (reactivation)**:
Before archiving, check if any order that just re-entered the mart (new update_time this
cycle) has a stale entry in the current hist_uti table. If so, delete it. A reactivated
order should not have an outdated hist entry — hist must reflect only truly inactive orders.
This MUST run before step 2 (archive): if step 2 ran first, the stale hist entry would
block the archive idempotency guard and the new archive entry would be skipped.

**Step 2 — Archive aged-out rows**:
Copy rows where `update_time < (MAX - 6 months)` to the appropriate hist_uti period table.
A `NOT EXISTS` guard makes this idempotent — if the pipeline retries after a partial failure,
already-archived rows are skipped.

**Step 3 — Safety check**:
Any row that aged out but was not matched by any period in step 2 (e.g., its update_time
falls in a gap between defined period windows) is logged to `order_tracking_exceptions` as
`ARCHIVE_ROUTING_GAP`. These rows are excluded from the trim in step 4 — no silent data
loss. An alert fires and the operator adds the missing period routing block.

**Step 4 — Trim aged-out rows**:
Delete rows where `update_time < (MAX - 6 months)`, excluding any order_ids flagged in
exceptions. Step 2 must always run before step 4 — if trim ran first, there would be nothing
left to archive.

### mart_ecs (2 steps)

1. **Archive inactive orders**: LEFT JOIN anti-join against mart_uti to find orders absent
   from the active mart. Archive to hist_ecs.
2. **Trim**: DELETE USING the same anti-join pattern.

Why LEFT JOIN instead of `NOT IN`: `NOT IN` with a subquery returns NULL (not FALSE) if
any NULL exists in the subquery result. At 90M+ rows in mart_uti, one NULL order_id would
cause `NOT IN` to skip all deletions silently. `DELETE USING` with a LEFT JOIN anti-join
is NULL-safe and is resolved by Redshift as a co-located hash anti-join.

### mart_uts (3 steps)

1. **Archive old spath events**: time-based — archive events where `pathTime < (MAX - 6 months)`.
2. **Safety check**: log any events outside all defined period windows to exceptions as
   `ARCHIVE_ROUTING_GAP_UTS`.
3. **Trim**: delete time-aged events, excluding exception rows.

The universal principle across all three tables: **archive first, validate second, trim last**.
At every point in time, data exists somewhere — never deleted before it is archived.

---

## 13. Historical Layer — 6-Month Splits

When an order is trimmed from the active mart, it is archived to a historical table. The
historical layer uses 6-month splits:

- `_YYYY_h1` covers January through June of that year
- `_YYYY_h2` covers July through December of that year

Current active tables: `hist_*_2025_h2` (Jul 2025 – Jan 2026) and `hist_*_2026_h1`
(Jan 2026 – Jul 2026).

### Why 6-month splits (not yearly, not monthly)

At 195M spath events per month, a full-year hist_uts table would hold ~2.34B rows. Problems:
- VACUUM on a 2.34B-row table takes hours
- Dropping old data requires dropping a table that spans 2 full years
- UNLOAD to S3 for cold archival takes very long

6-month splits cap hist_uts at ~1.17B rows. Dropping one 6-month table removes exactly
6 months of data cleanly. VACUUM scope is bounded.

Monthly splits would be manageable for uts (195M rows/table is fine) but add operational
overhead: 12 tables per year instead of 2, 12 routing entries in post-hooks instead of 2.
6-month is the right balance between manageability and operational simplicity.

Note: 6-month splits are needed for operational management, not for query performance.
Redshift zone maps on SORTKEY(pathTime) efficiently skip blocks regardless of table size.

### Orders move to historical as a unit

With mart_uti as the anchor, all three tables archive and trim the same order in the same cycle:
- mart_uti retention fires for Order X (update_time > 6 months old) → archived to hist_uti
- mart_ecs post-hook: Order X now NOT IN mart_uti → archived to hist_ecs, same cycle
- mart_uts post-hook: spath events for Order X are old AND Order X is gone from mart_uti →
  archived to hist_uts

All three hist tables receive Order X in the same 6-month period table. Historical queries
join hist_uti, hist_ecs, hist_uts for a single period and always find a complete record.
If tables archived independently, Order X's ecs row might be in `hist_ecs_2025_h2` while
its uti row is in `hist_uti_2026_h1` — historical queries would need to search across
periods. Moving as a unit eliminates this.

---

## 14. Reactivation Handling

A reactivated order is one that was archived (dormant for 6+ months) and then receives
a new update. Each table handles reactivation differently.

### mart_uti — fully automatic

The new update arrives in the extraction batch (new update_time). The dbt model:
- DELETE: `order_id IN (batch)` — the order is not in mart_uti (was trimmed), so this is a no-op.
- INSERT: the order re-enters mart_uti with its fresh update_time.

Post-hook step 1 then detects the reactivated order (recent update_time) and deletes its
stale entry from the current hist_uti table. Since orders are never dormant for more than
~1 year, the stale entry is always in the most recent hist_uti table.

Result: mart_uti is correct, hist_uti is clean. Fully automatic.

### mart_ecs — surfaced via exceptions, manual fix required

When the order originally went dormant 6+ months ago, mart_ecs trimmed its ecs row
(order not in mart_uti → archived to hist_ecs, deleted from mart_ecs).

When the order reactivates:
- mart_uti now has the order again (re-inserted this cycle)
- mart_ecs post-hook checks: is this order_id NOT IN mart_uti? No — it IS in mart_uti, so
  mart_ecs does not attempt to trim it. But the ecs row was already trimmed months ago.
- The ecs extraction only picks up new orders (add_time >= source_cutoff). The original
  order creation (add_time = old date) is not re-extracted.
- Result: mart_uti has the order, mart_ecs does not. The 3-way JOIN fails for this order.

This is detected by monitoring Test 3 (see §17). The fix is a manual 2-step SQL operation:
restore the ecs row from hist_ecs, delete the stale hist entry, mark resolved.

This is intentionally NOT handled in code. The complexity of a code fix (UNION view across all
hist_ecs tables, conditional INSERT, hist cleanup, metadata tracking) for a rare edge case
is not worth the maintenance burden. Simple monitoring + rare manual fix is the right trade.

### mart_uts — no action needed

Old spath events were archived to hist_uts when the order went dormant. When the order
reactivates, new spath events append to mart_uts normally. The old hist_uts events are not
stale — they are the correct historical record. No cleanup needed.

---

## 15. Redshift Physical Design

### DISTKEY(order_id) on all three mart tables

Every query against this pipeline is a 3-way JOIN on order_id:

```sql
FROM mart_ecs ecs
JOIN mart_uts uts ON ecs.order_id = uts.order_id
JOIN mart_uti uti ON ecs.order_id = uti.order_id
```

In Redshift, a JOIN requires both sides of the join key to be on the same compute node, or
Redshift redistributes data across nodes — an expensive network operation. With
`DISTKEY(order_id)` on all three tables, rows with the same order_id land on the same node.
The 3-way JOIN executes entirely locally: zero network shuffle, zero data movement.

The same DISTKEY applies to all hist tables, so historical queries have the same benefit.

### SORTKEY choices

Each table's SORTKEY is chosen for the column most commonly used in range filters:

**mart_uti: SORTKEY(update_time, order_id)**
The retention post-hook deletes rows `WHERE update_time < cutoff`. Zone maps on `update_time`
let Redshift skip all recent blocks and scan only old ones. Without this, the retention trim
would scan the entire table every cycle.

**mart_ecs: SORTKEY(partner_id, add_time, order_id)**
The primary query always filters `WHERE partner_id = X`. Leading on `partner_id` means zone
maps cut the scanned rows to ~0.3% of the table (1 partner out of 300+) before the JOIN
fires. `add_time` as second key supports time-range queries within a partner.

Note: new rows arrive in `add_time` order but `partner_id` is random. The unsorted region
grows over time, degrading zone maps on `partner_id`. Daily VACUUM SORT ONLY restores order.

**mart_uts: SORTKEY(pathTime, order_id)**
The primary query filters `WHERE pathTime >= X`. Zone maps on `pathTime` skip all blocks
before the query date — the most impactful single filter. Rows naturally arrive in `pathTime`
order, so the sort is self-maintaining.

---

## 16. DAG Dependency and Sequencing

The main DAG task dependency chain:

```
check_time_drift
       │
calculate_sync_window
       │
extraction (parallel TaskGroup)
  ├── extract_ecs
  ├── extract_uti
  └── extract_uts
       │
validate_extractions   ← raises on failure, blocks all dbt tasks
       │
trim_raw_tables        ← deletes uti_raw and uts_raw rows older than 24h
       │
dbt_mart_uti           ← must complete before ecs and uts
       │
  ┌────┴────┐
dbt_mart_ecs   dbt_mart_uts   (parallel)
  └────┬────┘
       │
  dbt_test               ← schema.yml uniqueness/not_null/relationship tests
       │
  summary                ← TriggerRule.ALL_DONE (runs even if tests fail)
```

### Why mart_uti runs first

mart_ecs post-hooks LEFT JOIN against mart_uti. They must see mart_uti in its final state
for this cycle — with all new rows inserted and all aged-out rows already trimmed. If mart_ecs
ran in parallel with mart_uti, the anti-join would read a mid-cycle mart_uti and could make
incorrect archive/trim decisions.

mart_uts does NOT join against mart_uti, but it still runs after mart_uti for consistent
cycle ordering — both mart_ecs and mart_uts always see the same mart_uti state within a cycle.

### Three separate DAGs

The pipeline deliberately separates concerns across DAGs:
- **Main DAG (*/15)**: extraction, raw trim, and dbt mart updates. Fast, frequent, must not
  be blocked by slow operations.
- **Vacuum DAG (2am daily)**: VACUUM must run with autocommit and outside transaction blocks.
  Running it inside the main 15-minute DAG would either block other tasks or fail.
- **Monitoring DAG (3am daily)**: DQ checks run once per day (sufficient for catching daily
  drift). Separating them means a slow DQ query does not delay a 15-minute extraction cycle.

---

## 17. Monitoring DAG — Daily Data Quality Checks

The `order_tracking_daily_monitoring` DAG runs 4 checks at 3am UTC every day.
Four checks run in a parallel TaskGroup (`dq_checks`), followed by `check_unresolved_exceptions`.

### Test 0 — Extraction count comparison (MySQL vs Redshift raw)

This is the only check that raises immediately (does not write to exceptions). It queries
both MySQL source and all three `*_raw` tables in Redshift for the same 2-hour closed window:

```
Window: [now - 2h - 5min, now - 5min)
```

The 5-minute buffer matches the extraction buffer used by `calculate_sync_window`.

If `MySQL count > Redshift raw count` for any table, rows were dropped during extraction
or Redshift COPY. This is an infrastructure alert — the pipeline is losing data.

Note: `uni_tracking_info` raw count can legitimately exceed MySQL count (the same order
may be extracted multiple times across cycles if it updates frequently). The alarm triggers
only when raw count is less than MySQL count.

### Test 1 — UTI/UTS time alignment

For every order active in the last 24 hours (DQ lookback window), `update_time` in mart_uti
should equal `MAX(pathTime)` in mart_uts for that order. A mismatch indicates: backdated
`update_time`, a missed extraction cycle, or uti/uts sync drift.

Unresolved mismatches are written to `order_tracking_exceptions` as `UTI_UTS_TIME_MISMATCH`.
The `NOT EXISTS` guard prevents duplicate entries on re-run.

### Test 2 — Orders missing from spath everywhere

For every order active in the last 24 hours, there should be at least one spath event in
mart_uts OR any hist_uts table. An order with no spath events anywhere is a genuine data
integrity issue — every shipped order should generate at least one carrier scan.

Checked across: mart_uts + all configured `HIST_UTS_TABLES`. Logged as `ORDER_MISSING_SPATH`.

Note: when new hist_uts tables are created (January 1 and July 1 each year), the
`HIST_UTS_TABLES` list in `order_tracking_daily_monitoring.py` must be manually updated.

### Test 3 — Reactivated orders missing from mart_ecs

For every order active in the last 24 hours (recent update_time in mart_uti), there should
be a matching row in mart_ecs. A missing ecs row means the order was archived to hist_ecs
when it went dormant, then reactivated, but the ecs extraction did not re-extract the
original creation row (because add_time is old, outside the extraction window).

Logged as `REACTIVATED_ORDER_MISSING_ECS`. Manual fix: restore from hist_ecs, delete stale
hist entry, mark resolved.

### Exceptions consolidation

`check_unresolved_exceptions` runs after all 4 DQ tasks complete (TriggerRule.ALL_DONE —
runs even if one DQ task fails). It queries `order_tracking_exceptions` for any unresolved
rows and raises if any exist, triggering an email alert. This ensures both new exceptions
AND pre-existing unresolved ones are flagged on every run.

---

## 18. VACUUM DAG — Ghost Row Management

The `order_tracking_vacuum` DAG runs at 2am UTC daily, 1 hour before the monitoring DAG.

Redshift does not physically remove deleted rows during a DELETE operation. Ghost rows
(tombstoned rows) accumulate and consume disk space. They also degrade zone map effectiveness
because blocks with many ghost rows have a wider min/max range. The main pipeline runs
96 DELETE cycles per day (one every 15 minutes) — without VACUUM, ghost rows accumulate
rapidly.

**VACUUM DELETE ONLY** on mart_uti, mart_uts, mart_ecs: reclaims ghost rows from the
daily DELETE cycles. Runs unconditionally every day.

**VACUUM SORT ONLY** on mart_ecs: mart_ecs has `SORTKEY(partner_id, add_time, order_id)`.
New orders arrive in `add_time` order but `partner_id` is random — the unsorted region grows
with every insertion. Zone maps on `partner_id` (the primary filter in all partner queries)
degrade as the unsorted percentage rises. VACUUM SORT ONLY rebuilds the sort order. It runs
after `VACUUM DELETE ONLY` (a more efficient order for Redshift's internal processing).

VACUUM uses raw psycopg2 with `conn.autocommit = True`. Redshift requires VACUUM to run
outside a transaction. Airflow's `PostgresHook` is not used here — it enables IAM auth
which triggers a boto3 RDS token fetch that fails with `NoRegionError` in this environment.

---

## 19. The 15-Minute Cycle End-to-End

Every 15 minutes:

**Step 1 — Time drift check (~5s)**
Query MySQL for its current unix timestamp. Compare against Airflow's wall clock. Log the
drift. The result is passed to `calculate_sync_window` via XCom.

**Step 2 — Calculate sync window (~1s)**
Compute the extraction upper bound: `to_ts = utcnow() - 5 minutes`. Push to XCom.
All three extraction tasks read this value as their `--end-time` argument.

**Step 3 — Extraction (~94s, parallel)**
Three extraction tasks run simultaneously. Each reads from MySQL:
```sql
WHERE ts > watermark_unix AND ts < end_time
```
Results are appended to the three `*_raw` tables in Redshift. Per-table result JSON files
are written to `/tmp/` for downstream validation.

**Step 4 — Validate extractions (~2s)**
Read the three JSON files. Any `status != 'success'` raises and blocks dbt. Zero-row
extractions are warnings only.

**Step 5 — Trim raw tables (~5s)**
Delete rows from `uni_tracking_info_raw` and `uni_tracking_spath_raw` where their timestamp
is older than 24 hours. `ecs_order_info_raw` is skipped (see §5).

**Step 6 — dbt: mart_uni_tracking_info (~140s)**
Read raw, deduplicate, delete+insert into mart, then run 4 post-hooks (reactivation cleanup,
archive to hist_uti, safety check, trim). Must complete before steps 7 and 8.

**Step 7 + 8 — dbt: mart_ecs_order_info and mart_uni_tracking_spath (~50s, parallel)**
Both run after mart_uti. mart_ecs archives and trims inactive orders using mart_uti as the
activity anchor. mart_uts archives and trims old spath events by time alone.

**Step 9 — dbt test (~20s)**
Runs `dbt test --select mart`: schema.yml uniqueness, not_null, and relationship tests
scoped to the three mart models. Staging tests are excluded — staging models are not run
in this pipeline and their relationship tests would produce false failures.

**Step 10 — Summary (always)**
TriggerRule.ALL_DONE: prints extraction counts, trim counts, cycle configuration, and
overall status regardless of whether upstream tasks succeeded or failed.

**Total estimated cycle time**: ~4 minutes. The 15-minute schedule has ~11 minutes of headroom.
`max_active_runs=1` prevents concurrent DAG runs from overlapping if a cycle runs long.

---

*For code-level detail — specific function names, SQL patterns, config values, file paths —
see `code_walkthrough_v2.md`.*
