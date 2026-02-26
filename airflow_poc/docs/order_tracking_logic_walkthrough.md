# Order Tracking Pipeline — Logical Walkthrough

**Created**: 2026-02-25
**Purpose**: Explains the reasoning and logic behind every design decision.
Read this to understand *why* the system works the way it does.
For *what* the system does, see `order_tracking_final_design.md`.

---

## Table of Contents

1. [The Problem We Are Solving](#1-the-problem-we-are-solving)
2. [The Three Source Tables and Their Relationships](#2-the-three-source-tables-and-their-relationships)
3. [Why delete+insert for uti](#3-why-deleteinsert-for-uti)
4. [The Original Bug — Why the Time Window Was Wrong](#4-the-original-bug--why-the-time-window-was-wrong)
5. [Why DISTKEY(order_id) Is the Central Decision](#5-why-distkeyorder_id-is-the-central-decision)
6. [Why Each Table Has a Different SORTKEY](#6-why-each-table-has-a-different-sortkey)
7. [The Retention Problem — Why Independent Trim Breaks the JOIN](#7-the-retention-problem--why-independent-trim-breaks-the-join)
8. [Why mart_uti Is the Anchor](#8-why-mart_uti-is-the-anchor)
9. [Why mart_ecs Uses Order_id-Driven Retention](#9-why-mart_ecs-uses-order_id-driven-retention)
10. [Why mart_uts Uses Pure Time-Based Retention](#10-why-mart_uts-uses-pure-time-based-retention)
11. [The Post-Hook Sequence — Why Order Matters](#11-the-post-hook-sequence--why-order-matters)
12. [How Reactivation Works — Logic Step by Step](#12-how-reactivation-works--logic-step-by-step)
13. [Why Orders Move to Historical as a Unit](#13-why-orders-move-to-historical-as-a-unit)
14. [Why 6-Month Splits for Historical Tables](#14-why-6-month-splits-for-historical-tables)
15. [The Exceptions Table — Why Not Handle in Code](#15-the-exceptions-table--why-not-handle-in-code)
16. [How a Query Executes — Physical Walkthrough](#16-how-a-query-executes--physical-walkthrough)
17. [The 15-Minute Cycle — End to End](#17-the-15-minute-cycle--end-to-end)

---

## 1. The Problem We Are Solving

An order tracking system generates continuous updates. An order is created, goes
through multiple logistics states (picked up, in transit, out for delivery, delivered),
and eventually becomes dormant. Each state change produces a new record in the
tracking system.

We need to answer questions like:
- What is the current status of all active orders for partner X?
- Which orders for partner X had a scan event in the last 7 days?
- What was the full tracking history of order Y?

These questions require three things to be true simultaneously:
1. Each order has exactly one current status row (no duplicates)
2. All spath scan events for an order are available
3. Order metadata (partner, creation time) is always reachable for any active order

The challenge is that orders have different lifetimes — some complete in 3 days,
others stay active for 90+ days. At peak, 195M spath events and 29M uti updates
arrive per month. The system must handle all of this correctly and efficiently.

---

## 2. The Three Source Tables and Their Relationships

```
uni_tracking_info_raw        ecs_order_info_raw         uni_tracking_spath_raw
─────────────────────        ──────────────────         ──────────────────────
order_id  (PK-ish)           order_id  (PK)             order_id
update_time                  add_time                   pathTime
tno                          partner_id                 code
status                       ...                        traceSeq
...                                                     ...

One row per order             One row per order          Many rows per order
(latest status)               (created once,             (each scan event)
Updated on every              never changes)
status change
```

**Key relationship**: all three tables join on `order_id`. An order appears in
all three simultaneously from creation until it is archived.

**Key asymmetry**:
- uti: one row per order, changes frequently (update_time advances with each status)
- ecs: one row per order, written once and never changed
- uts: many rows per order, written once per scan event, never changed

This asymmetry drives every design decision that follows.

---

## 3. Why delete+insert for uti

uti has one row per order and that row changes. When a new status update arrives
for order X, we need the old row gone and the new row in place — exactly one row
for order X at all times.

dbt's `delete+insert` strategy achieves this:
1. DELETE the old row for every order_id in the incoming batch
2. INSERT the new rows for those order_ids

The DELETE is `WHERE order_id IN (batch)` — it finds and removes the old row
regardless of how old it is. An order active for 90 days has its 89-day-old row
deleted just as cleanly as an order active for 2 days. No time window needed.

ecs and uts do not need delete+insert:
- ecs is written once and never changes — append only
- uts events are immutable — new events append, old events stay

---

## 4. The Original Bug — Why the Time Window Was Wrong

The previous dbt implementation used `incremental_predicates` to add a time
constraint to the DELETE:

```sql
WHERE order_id IN (batch)
AND   update_time > (MAX - 20 days)   ← the time window
```

This was added for performance — restricting the DELETE to the last 20 days means
Redshift can use zone maps on `update_time` to skip all older blocks, scanning far
fewer rows.

**The bug**: any order whose last row is older than 20 days is not found by the DELETE.
The old row survives. Then INSERT adds the new row. Staging now has two rows for the
same order_id.

```
Order active since day 0.
Old row: update_time = day 62.
Today (day 90): new update.

DELETE window: day 70 to day 90.
day 62 is outside the window → old row NOT deleted.
INSERT adds new row (update_time = day 89).

staging now has:
  row 1: update_time = day 62  ← stale duplicate
  row 2: update_time = day 89  ← correct
```

~20K orders across 32 partners were confirmed to be in this state.

**The fix**: remove the time window entirely. DELETE becomes:
```sql
WHERE order_id IN (batch)
```

This always finds the old row regardless of age. The performance concern is addressed
separately by the retention post_hook (see §8) — by keeping the table compact, the
DELETE scan is fast even without zone map pruning.

This mirrors exactly what the old Kettle pipeline did — it always deleted by `order_id`
with no time window, and it was always correct.

---

## 5. Why DISTKEY(order_id) Is the Central Decision

Every query against this system is a 3-way JOIN on `order_id`:

```sql
FROM   mart_ecs  ecs
JOIN   mart_uts  uts  ON ecs.order_id = uts.order_id
JOIN   mart_uti  uti  ON ecs.order_id = uti.order_id
```

In Redshift, a JOIN between two tables requires both sides of the join key to be
on the same node, or Redshift has to broadcast/redistribute data across nodes —
an expensive network operation.

By setting `DISTKEY(order_id)` on all three tables, Redshift places rows with the
same `order_id` on the same node across all three tables. The 3-way JOIN executes
entirely locally — zero data moves between nodes.

```
Node 1: mart_ecs rows where order_id hash → node 1
         mart_uts rows where order_id hash → node 1
         mart_uti rows where order_id hash → node 1
         JOIN happens locally ✓

Node 2: mart_ecs rows where order_id hash → node 2
         mart_uts rows where order_id hash → node 2
         mart_uti rows where order_id hash → node 2
         JOIN happens locally ✓
```

This is the single most important physical design decision. All other optimisations
are secondary to this.

The same DISTKEY is applied to all hist tables — historical queries have the same
co-location benefit.

---

## 6. Why Each Table Has a Different SORTKEY

Redshift's SORTKEY determines how rows are physically ordered on disk. This enables
zone maps — each disk block stores the min and max value of the sort column. When a
query has a range filter on the sort column, Redshift can skip entire blocks without
reading them.

Each table leads with the column that is most commonly filtered in queries:

**mart_uti: SORTKEY(update_time, order_id)**
The retention post_hook deletes rows `WHERE update_time < cutoff`. Zone maps on
`update_time` let Redshift skip all recent blocks and go directly to old rows.
Without this, the retention trim scans the entire table every cycle.

**mart_ecs: SORTKEY(partner_id, add_time, order_id)**
The primary query always filters `WHERE partner_id IN (382)`. With 300+ partners,
leading on `partner_id` means zone maps cut the scanned rows to ~0.3% of the table
before the JOIN fires. `add_time` as second key supports time-range queries within
a partner.

Note: rows arrive in `add_time` order (chronological) but `partner_id` is random
in new rows. This means the unsorted region grows and zone maps on `partner_id`
degrade over time — periodic VACUUM SORT is needed to restore sort order.

**mart_uts: SORTKEY(pathTime, order_id)**
The primary query filters `WHERE pathTime >= X`. Zone maps on `pathTime` skip all
blocks before the query date — the most impactful single filter in the primary query.
Rows naturally arrive in `pathTime` order so the sort is self-maintaining.

---

## 7. The Retention Problem — Why Independent Trim Breaks the JOIN

With 6-month retention, the naive approach is to trim each table by its own
natural timestamp:

```sql
mart_uti: DELETE WHERE update_time < (MAX - 6 months)
mart_ecs: DELETE WHERE add_time    < (MAX - 6 months)
mart_uts: DELETE WHERE pathTime    < (MAX - 6 months)
```

This seems correct but breaks for long-lifecycle orders:

```
Order X: created Jan 2024 (add_time = Jan 2024)
         still receiving updates in Sep 2025 (update_time = Sep 2025)

In March 2025 (6 months after Jan 2024):
  mart_ecs independent trim: add_time = Jan 2024, now > 6 months → TRIMMED
  mart_uti trim:             update_time = Mar 2025, within 6 months → KEPT

Primary query in Sep 2025:
  mart_uti has Order X ✓
  mart_ecs has NO row for Order X ✗
  JOIN returns nothing for Order X — active order invisible to queries
```

The root problem: `add_time` (when the order was created) and `update_time` (when
it was last updated) measure different things. An order can be created long ago
but still be actively updating. Independent time-based retention on `add_time`
incorrectly treats a still-active order as old.

The fix: mart_ecs must use mart_uti's definition of "active" — not its own
timestamp. If mart_uti still has Order X, mart_ecs must keep it too.

---

## 8. Why mart_uti Is the Anchor

mart_uti stores the current status of every active order. Its retention trim
(`WHERE update_time < MAX - 6 months`) directly measures order activity — an
order is trimmed from mart_uti when it has not been updated in 6 months.
This is the most accurate signal for "is this order still active".

By making mart_ecs and mart_uts retention dependent on mart_uti membership:

```sql
-- mart_ecs: only trim when mart_uti has trimmed the order
DELETE FROM mart_ecs WHERE order_id NOT IN (SELECT order_id FROM mart_uti)

-- mart_uts: only trim old events for orders that mart_uti has trimmed
DELETE FROM mart_uts
WHERE pathTime < 6-month cutoff
AND   order_id NOT IN (SELECT order_id FROM mart_uti)
```

All three tables always contain the same set of active order_ids. The 3-way JOIN
is always consistent regardless of order lifecycle length.

mart_uti trims an order → in the same cycle, mart_ecs and mart_uts trim it too →
all three hist tables receive the order simultaneously → active mart is clean,
hist layer is complete.

---

## 9. Why mart_ecs Uses Order_id-Driven Retention

mart_ecs has exactly ONE row per order. If that row is trimmed, the order has
zero representation in mart_ecs. The next query looking for that order_id via
the 3-way JOIN returns no results — the order is effectively invisible even if
mart_uti and mart_uts still have data for it.

Order_id-driven retention prevents this by tying mart_ecs lifetime to mart_uti:
- Order X is in mart_uti → order X stays in mart_ecs, no matter how old `add_time` is
- Order X leaves mart_uti → order X leaves mart_ecs in the same cycle

The result: mart_ecs size mirrors mart_uti size (~90–175M rows). It is bounded,
not permanently growing, and always aligned with the active order set.

---

## 10. Why mart_uts Uses Pure Time-Based Retention

mart_uts has MANY rows per order — every scan event from every carrier, for the
full lifetime of the order. A long-lifecycle order active for 2 years accumulates
potentially thousands of spath events.

If mart_uts used order_id-driven retention like mart_ecs:
```sql
-- order_id-driven for mart_uts (NOT what we use — explained why)
DELETE FROM mart_uts
WHERE order_id NOT IN (SELECT order_id FROM mart_uti)
-- This keeps ALL spath events for ALL active orders forever
```

A 2-year-old active order keeps all its spath events from 2 years ago in
mart_uts. At 195M events/month, mart_uts would grow far beyond 1.17B rows —
unpredictable and unmanageable.

Pure time-based retention keeps mart_uts at a predictable ceiling:
```
195.3M events/month × 6 months = ~1.17B rows maximum — forever stable
```

The trade-off: old spath events for active long-lifecycle orders move to hist_uts
after 6 months. For the primary query pattern (`WHERE pathTime >= X`), this is
not a problem — queries always filter by recent pathTime. Old spath events are
accessible in hist_uts for historical lookups.

The key insight: mart_uts and mart_ecs have different data shapes.
mart_ecs: one row per order — order_id-driven retention is safe (bounded by order count)
mart_uts: many rows per order — order_id-driven retention is unsafe (unbounded by event count)

---

## 11. The Post-Hook Sequence — Why Order Matters

Four operations happen in mart_uti post_hooks every cycle. The order is not
arbitrary — each step depends on the state left by the previous one.

```
Step 1: Delete stale hist_uti entries for reactivated orders
Step 2: Archive aged-out rows to hist_uti  (with NOT EXISTS guard)
Step 3: Safety check — log any unmatched rows to exceptions, exclude from trim
Step 4: Trim aged-out rows from mart_uti   (excludes exceptions rows)
```

**Why step 1 must be first**:

A reactivated order (was dormant, now has a new update) has just been re-inserted
into mart_uti in this cycle. It has a new `update_time = now` — step 2 will NOT
archive it (it's not aged out). But hist_uti still has the OLD state of this order
from when it was last archived.

If we don't clean the stale hist entry before running, hist_uti ends up with a
permanently stale row for an order that is now active again.

Step 1 detects orders in the current batch (new update_time) and deletes their
hist entries:
```sql
DELETE FROM hist_uti WHERE order_id IN (new batch this cycle)
```

**Why step 2 uses a NOT EXISTS guard**:

Post_hooks can be retried on failure. Without a guard, a retry of step 2 would
insert duplicate rows into hist. The NOT EXISTS guard makes the archive idempotent:
already-archived rows are skipped on re-run.

**Why step 3 exists (the safety check)**:

Archive INSERTs in step 2 are hardcoded to specific period windows (e.g. 2026_h1,
2026_h2). If a row's timestamp falls outside all defined windows, step 2 silently
skips it. Without step 3, step 4 would delete that row from mart — permanent data
loss with no error.

Step 3 catches any row that step 2 missed and logs it to `order_tracking_exceptions`.
Step 4 then reads the exceptions table and excludes those rows from the trim. The
row stays in mart until the routing is fixed and the exception is resolved.

**Why step 2 must be before step 4**:

Step 4 deletes rows from mart_uti. If step 2 (archive) runs after step 4 (trim),
the rows are already gone — nothing to archive. Data is permanently lost.

Step 2 copies rows to hist first. Step 3 validates. Step 4 then removes from mart.
At every point in time, the data exists somewhere — never lost.

**Why mart_ecs uses DELETE USING instead of NOT IN**:

mart_ecs trim originally used `WHERE order_id NOT IN (SELECT order_id FROM mart_uti)`.
In SQL, `NOT IN` returns NULL — not FALSE — if the subquery returns any NULL value.
One NULL `order_id` in mart_uti means the entire condition evaluates to NULL for
every row, and nothing gets deleted.

`DELETE USING` with a LEFT JOIN anti-join is the correct pattern:
```sql
DELETE FROM mart_ecs
USING (
    SELECT ecs.order_id
    FROM   mart_ecs LEFT JOIN mart_uti ON ecs.order_id = uti.order_id
    WHERE  uti.order_id IS NULL
) stale
WHERE mart_ecs.order_id = stale.order_id;
```
This is NULL-safe. Redshift resolves it as a hash anti-join co-located on
DISTKEY(order_id) — no cross-node shuffle, same performance as the original NOT IN.

**mart_uts has 3 steps (not 4)**:

mart_uts trim is pure time-based — no subquery against mart_uti. The NULL trap
does not apply. The step count is: archive (step 1), safety check (step 2), trim
(step 3).

**The same archive-before-trim principle applies to mart_ecs and mart_uts**:
always archive first, validate second, trim last.

---

## 12. How Reactivation Works — Logic Step by Step

A reactivated order is one that was archived (dormant for 6+ months) and then
receives a new update. Three different things happen across the three tables.

### mart_uti — fully automatic

The new update arrives in the extraction batch. Its `update_time` is recent
(within source_cutoff). The extraction picks it up.

In the dbt model:
- DELETE: `order_id IN (batch)` — order not in mart_uti (was trimmed), no-op
- INSERT: order re-enters mart_uti with new `update_time` ✓

In post_hook step 1:
- The reactivated order is in the new batch
- DELETE runs against the latest hist_uti_YYYY_HX table — stale entry removed ✓

**Why the latest table is sufficient**: orders are never dormant for more than ~1 year.
An archived order that reactivates was archived recently and is in the latest (most
recently written) hist_uti table. No need to check older tables.

**Why DELETE-only is safe without knowing exactly which row exists**: DISTKEY(order_id)
means the DELETE is a co-located local lookup. If the order is not in this table,
the DELETE is a no-op. Reactivated set is tiny — this is cheap most cycles.

hist_uti is clean. mart_uti has the current state. Fully automatic, no manual step.

### mart_ecs — surfaced via exceptions table

When the order went dormant 6+ months ago:
- mart_uti trimmed it → mart_ecs trimmed it (order_id NOT IN mart_uti)
- mart_ecs archived the ecs row to hist_ecs_YYYY_HX ✓

When the order reactivates:
- mart_uti now has the order again (new insert this cycle)
- mart_ecs post_hook checks: is order_id NOT IN mart_uti? → order IS in mart_uti → not trimmed
- BUT the ecs row was already trimmed from mart_ecs 6+ months ago
- The ecs extraction only picks up new rows (`WHERE add_time >= source_cutoff`) — the
  original order creation (add_time = old) is not re-extracted

Result: mart_uti has the order. mart_ecs does not. 3-way JOIN for this order returns nothing.

**Why not fix this in code**: fixing it requires querying all hist_ecs_YYYY_HX tables
to find the row, inserting it back into mart_ecs, then deleting the now-stale hist_ecs
entry. This creates a chain of dependencies (UNION view, restore step, hist cleanup,
metadata tracking) for a rare edge case. The complexity cost outweighs the benefit.

**Why this is different from mart_uti reactivation**:
mart_uti uses a simple DELETE across the last 2 hist_uti tables. DELETE is safe to run
blindly — hitting the wrong table finds 0 rows, which is a no-op.
mart_ecs requires INSERT (restore the ecs row from hist) THEN DELETE (remove the stale
hist entry). INSERT is NOT safe to run blindly — inserting into the wrong mart table or
inserting a duplicate row causes real data problems. You need to know which hist_ecs
table holds the row before doing anything. That knowledge requires a lookup across all
hist_ecs tables, which is the complexity we are trying to avoid.
In short: brute-force DELETE = safe. Brute-force INSERT = not safe.

Instead, a daily test detects it:
```sql
-- Order in mart_uti but missing from mart_ecs
SELECT uti.order_id FROM mart_uti uti
LEFT JOIN mart_ecs ecs ON uti.order_id = ecs.order_id
WHERE ecs.order_id IS NULL
```

The fix when it fires is a two-step manual operation: restore from hist_ecs, delete
the stale hist entry, mark resolved. This is rare — an order must have been dormant
for 6+ months and then reactivated, which is uncommon in a live tracking system.

### mart_uts — no action needed

When the order went dormant:
- Old spath events were trimmed from mart_uts (time-based, pathTime < 6-month cutoff)
- They were archived to hist_uts_YYYY_HX — permanent record ✓

When the order reactivates:
- New spath events come in and append to mart_uts ✓
- Old spath events in hist_uts remain — they are not stale, they are the correct
  historical record of what happened before the order went dormant

No cleanup needed. The reactivated order has new spath events in mart_uts and
historical spath events in hist_uts. Both are correct.

---

## 13. Why Orders Move to Historical as a Unit

With mart_uti as the anchor, all three tables trim the same order in the same cycle:

```
Cycle N: mart_uti retention fires → Order X update_time < 6-month cutoff
  mart_uti post_hook: archives Order X to hist_uti → trims from mart_uti
  mart_ecs post_hook: Order X now NOT IN mart_uti → archives to hist_ecs → trims
  mart_uts post_hook: Order X spath events old AND Order X NOT IN mart_uti → archives to hist_uts → trims
```

After cycle N, Order X exists in hist_uti, hist_ecs, and hist_uts — and nowhere
in the active mart. All three hist tables receive Order X in the same 6-month
window table (because they all archive in the same cycle, triggered by mart_uti).

This guarantees historical queries are always consistent:

```sql
-- This query always works — all three tables have Order X in the same period
SELECT ...
FROM   hist_uti_2026_h1 uti
JOIN   hist_ecs_2026_h1 ecs ON uti.order_id = ecs.order_id
JOIN   hist_uts_2026_h1 uts ON uti.order_id = uts.order_id
WHERE  uti.order_id = :order_x
```

If orders moved to hist independently, Order X could be in hist_uti_2026_h1 but
its ecs row might be in hist_ecs_2025_h2 (trimmed earlier by add_time) — a historical
query would need to search across multiple periods to reassemble one order's data.
Moving as a unit eliminates this entirely.

---

## 14. Why 6-Month Splits for Historical Tables

At peak volumes (195.3M spath events/month), a hist_uts table covering a full year
would hold ~2.34B rows. A single table at that size:
- Requires very long VACUUM runs
- Requires long UNLOAD operations when archiving to S3
- Makes dropping old data (a single DROP TABLE) risky — one table holds 2 years of data

6-month splits cap each hist_uts table at ~1.17B rows. Dropping hist_uts_2025_h2 drops
exactly 6 months of data cleanly. VACUUM runs are scoped to one 6-month table.

**Why not monthly splits**: at 195M rows/month, monthly tables are fine for uts
(195M is manageable) but adds operational overhead — 12 tables to create per year
instead of 2, 12 post_hook routing entries instead of 2. 6-month is the right balance.

**Why Redshift does not need more frequent splits for performance**:
MySQL needed 6-month splits because it is row-based, single-node, and index-dependent.
Redshift is columnar, MPP, and uses zone maps. A 1.17B row hist_uts table with
`SORTKEY(pathTime)` and a query filtering `WHERE pathTime >= X` skips all older
blocks automatically. The query scans only the relevant blocks — split or not,
the I/O is the same.

Splits are for **operational management** (lifecycle, archival, dropping), not
for query performance in Redshift.

---

## 15. The Exceptions Table — Why Not Handle in Code

The mart_ecs reactivation gap is a real data integrity issue that needs to be
handled. There are two ways to handle it:

**Option 1 — Handle in post_hook code**

```
mart_uti post_hook detects reactivated orders
→ queries hist_ecs_all (UNION of all hist_ecs_YYYY_HX tables) to find ecs row
→ inserts ecs row back into mart_ecs
→ deletes stale hist_ecs entry from whichever table it's in
→ needs to track which hist_ecs table holds each order_id
```

This works but requires:
- A UNION view across all hist_ecs tables (grows as new tables are added)
- A restore INSERT running every 15-minute cycle even when there are no reactivations
- A hist_ecs cleanup step (DELETE from specific table — can't delete from a view)
- Metadata tracking for which table holds which order_id

**Option 2 — Exceptions table**

Run a daily check. Write exceptions to a table. Fix manually when they appear.

This works because:
- Reactivations (dormant >6 months, then active again) are rare in a live
  tracking system. Most dormant orders stay dormant — delivered packages don't
  typically restart their tracking journey after 6 months.
- The fix when it fires is simple and deterministic — a 2-step manual SQL operation.
- The exceptions table captures the gap explicitly, creating an audit trail.

**The principle**: don't add complex defensive code for rare cases. Surface them,
monitor them, fix them simply when they occur. Simple code + good monitoring is
more maintainable than complex code that silently handles edge cases.

---

## 16. How a Query Executes — Physical Walkthrough

The most frequent query:

```sql
SELECT ecs.partner_id, uts.code, COUNT(*) AS cnt
FROM   mart_ecs_order_info        ecs
JOIN   mart_uni_tracking_spath    uts  ON ecs.order_id = uts.order_id
JOIN   mart_uni_tracking_info     uti  ON ecs.order_id = uti.order_id
WHERE  ecs.partner_id  = 382
AND    uts.pathTime    >= unix_timestamp('2026-01-01')
AND    uts.code        = 200
GROUP BY ecs.partner_id, uts.code
```

**Step 1 — mart_uts scan with pathTime filter**

Redshift reads mart_uts. SORTKEY(pathTime) means blocks are ordered by time.
Zone maps store the min/max pathTime for each block. All blocks where
`max(pathTime) < 2026-01-01` are skipped entirely without reading.

Only blocks that overlap with the query date range are read. At 195M events/month,
this prune typically skips 80–90% of the table.

**Step 2 — code = 200 filter on the remaining rows**

After pathTime pruning, `code = 200` is applied as a columnar filter on the
remaining rows. Redshift reads only the `code` column (not the full row).

**Step 3 — JOIN with mart_ecs on order_id**

DISTKEY(order_id): rows with the same order_id are on the same node in both tables.
The JOIN executes locally — Redshift does a hash join on the order_id column only.
No data moves between nodes.

mart_ecs SORTKEY(partner_id) zone maps: the `partner_id = 382` filter is applied
during the mart_ecs scan. Zone maps skip all blocks where partner_id is not 382.
With 300+ partners, this cuts the mart_ecs scan to roughly 1/300 of the table (~0.3%).

**Step 4 — JOIN with mart_uti on order_id**

Same DISTKEY co-location. mart_uti is hit last — in this query it is mainly needed
to confirm the order is active. The join is local, reading only the order_id column.

**Total I/O**: a fraction of what MySQL would need. Zone maps eliminate most blocks
before any row data is read. The 3-way JOIN has zero network cost.

---

## 17. The 15-Minute Cycle — End to End

Every 15 minutes, the pipeline runs in this sequence:

**Phase 1 — Extraction (~94 seconds, parallel)**

Three Airflow tasks run simultaneously:
- `extract_uti`: pulls rows from `uni_tracking_info_raw` where `update_time >= source_cutoff`
- `extract_ecs`: pulls rows from `ecs_order_info_raw` where `add_time >= source_cutoff`
- `extract_uts`: pulls rows from `uni_tracking_spath_raw` where `pathTime >= source_cutoff`

`source_cutoff` is computed as `MAX(update_time) - buffer` — a small lookback to
catch any rows that arrived slightly late in the previous cycle.

**Phase 2 — mart_uti (~100–150 seconds)**

This runs first and must complete before mart_ecs starts.

1. **ranked CTE**: deduplicates within-batch. If the same order_id appears twice
   in the extraction window (e.g. two status changes in 15 minutes), only the
   most recent one (highest `update_time`) is kept.

2. **DELETE**: `WHERE order_id IN (batch)`. Removes the previous row for every
   order in the batch. No time window — finds the old row regardless of age.

3. **INSERT**: the deduped batch rows enter mart_uti. Exactly one row per order_id.

4. **Post_hook step 1 — reactivation cleanup**: checks if any orders in the new
   batch also have entries in hist_uti (they were archived previously but are now
   active again). Deletes those stale hist entries.

5. **Post_hook step 2 — archive**: copies rows with `update_time < MAX - 6 months`
   to the appropriate `hist_uti_YYYY_HX` table, routed by update_time range.

6. **Post_hook step 3 — trim**: deletes the same aged-out rows from mart_uti.

**Phase 3 — mart_ecs and mart_uts (~30–50 seconds, parallel)**

Both run after mart_uti completes. They can run alongside each other.

**mart_ecs**:
1. INSERT new orders from the extraction batch (new order_ids not yet in mart_ecs)
2. Post_hook — archive: copies ecs rows for orders no longer in mart_uti to hist_ecs_YYYY_HX
3. Post_hook — trim: removes those same ecs rows from mart_ecs

**mart_uts**:
1. INSERT new spath events from the extraction batch
2. Post_hook — archive: copies spath events older than 6 months to hist_uts_YYYY_HX
3. Post_hook — trim: removes those aged spath events from mart_uts

**Phase 4 — Daily monitoring (separate Airflow task, runs once/day)**

Three tests query the active mart tables and write any failures to `order_tracking_exceptions`:
- Test 1: update_time in mart_uti should equal MAX(pathTime) in mart_uts per order_id
- Test 2: every order in mart_uti should have spath rows in mart_uts or hist_uts
- Test 3: every order in mart_uti should have a row in mart_ecs

An alert fires if `order_tracking_exceptions` has any unresolved rows.

**Total cycle time**: ~4 minutes. The 15-minute schedule has ~11 minutes of headroom.

---

*This document explains the reasoning. For the complete specification, see `order_tracking_final_design.md`.*
