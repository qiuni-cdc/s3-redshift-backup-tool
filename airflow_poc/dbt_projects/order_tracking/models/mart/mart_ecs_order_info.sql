{#
    Source cutoff: 2 hours from raw — catches any late-arriving new orders from the previous
                   cycle without re-reading historical orders (raw-relative, not mart-relative).
    Retention: order_id-driven — a row is trimmed only when mart_uni_tracking_info has
               trimmed the same order_id.

    Strategy: delete+insert, NO incremental_predicates
    - add_time is set at order creation and never changes. New batches only contain NEW orders.
    - No incremental_predicates: DELETE is WHERE order_id IN (batch) using DISTKEY(order_id)
      co-location — each node checks only its own order_ids, near-zero cost for new orders.
    - ECS is write-once — new order_ids are not in the mart yet, so DELETE is a no-op for
      normal cycles. On retry (same order re-extracted), DELETE removes the duplicate correctly.
    - Why NOT incremental_predicates: the sort key leads on partner_id (for downstream query
      performance). A WHERE add_time > MAX-7200 filter cannot use zone maps with partner_id
      leading — it touches ~1 block per partner (300+ partners) instead of just the last few
      blocks. This made DELETE slower, not faster. DISTKEY hash join is the right mechanism.

    Why order_id-driven retention (NOT time-based on add_time):
    - mart_ecs has ONE row per order. Time-based trim would drop long-lifecycle orders
      (e.g. created Jan 2024, still active Aug 2025) when their add_time ages past 6 months.
    - This would break the 3-way JOIN: mart_uti has the order, mart_ecs does not.
    - Order_id-driven: trim mart_ecs only when mart_uti has already trimmed the order.
      The 3-way JOIN is always intact for any active order regardless of add_time age.

    Post-hooks (3 steps, run AFTER mart_uni_tracking_info):
      Step 1a/b: Archive ecs rows for inactive orders (not in mart_uti) to correct hist table.
                 Uses LEFT JOIN anti-join (NULL-safe — avoids NOT IN NULL trap at 90M+ rows).
                 LEFT JOIN idempotency guard on order_id (one row per order, safe for ecs).
      Step 1c: Safety check — catch inactive orders whose add_time falls outside all defined
                 hist_ecs periods. Logs ARCHIVE_ROUTING_GAP_ECS to exceptions and excludes
                 them from trim (step 2) — no silent data loss. Mirrors mart_uti step 3.
      Step 2: Trim — DELETE USING anti-join (NULL-safe, Redshift hash anti-join on DISTKEY).
                 Excludes ARCHIVE_ROUTING_GAP_ECS exceptions — never trim an unarchived row.

    DAG dependency: mart_ecs MUST run after mart_uni_tracking_info completes.
    The LEFT JOIN in post_hooks reads mart_uti's final state for this cycle.
    Using ref('mart_uni_tracking_info') in post_hooks creates the dbt compile-time dependency.
#}



{# STEP 1a: Archive inactive orders to 2025_h2 (Jul 2025 – Jan 2026).
   LEFT JOIN anti-join: orders absent from mart_uti = no longer active = safe to archive.
   NULL-safe: LEFT JOIN WHERE uti.order_id IS NULL handles NULL order_ids correctly.
   Idempotency guard: LEFT JOIN on order_id (one row per order in ecs — safe to use order_id).
   ADD a step 1b block here when data starts aging into the next period (see dbt_project.yml). #}
{%- set _ph1a = "INSERT INTO {{ var('mart_schema') }}.hist_ecs_order_info_2025_h2 SELECT ecs.* FROM {{ this }} ecs LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id LEFT JOIN {{ var('mart_schema') }}.hist_ecs_order_info_2025_h2 h ON h.order_id = ecs.order_id WHERE uti.order_id IS NULL AND ecs.add_time >= extract(epoch from '2025-07-01'::timestamp) AND ecs.add_time < extract(epoch from '2026-01-01'::timestamp) AND h.order_id IS NULL" -%}

{# STEP 1c: Safety check — catch inactive orders whose add_time falls outside all defined
   hist_ecs periods. Without this guard, orders with add_time outside all periods are silently
   deleted by step 2 with no record. Mirrors mart_uti step 3 and mart_uts step 2.
   Uses LEFT JOIN anti-pattern for the dedup check (Redshift: no correlated subqueries in INSERT).
   ADD a NOT (add_time BETWEEN ...) block for each new period added in step 1b.
   Update the NOT (...) condition when adding new hist_ecs periods. #}
{%- set _ph1c = "INSERT INTO {{ var('mart_schema') }}.order_tracking_exceptions (order_id, exception_type, detected_at, notes) SELECT DISTINCT ecs.order_id, 'ARCHIVE_ROUTING_GAP_ECS', CURRENT_TIMESTAMP, 'Inactive ECS order not archived — add_time outside all defined hist_ecs periods. Excluded from trim.' FROM {{ this }} ecs LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id LEFT JOIN {{ var('mart_schema') }}.order_tracking_exceptions ex ON ex.order_id = ecs.order_id AND ex.exception_type = 'ARCHIVE_ROUTING_GAP_ECS' AND ex.resolved_at IS NULL WHERE uti.order_id IS NULL AND NOT (ecs.add_time >= extract(epoch from '2025-07-01'::timestamp) AND ecs.add_time < extract(epoch from '2026-01-01'::timestamp)) AND ex.order_id IS NULL" -%}

{# STEP 2: Trim — DELETE USING anti-join (NULL-safe).
   Removes ecs rows for orders no longer in mart_uti (trimmed as inactive).
   Excludes ARCHIVE_ROUTING_GAP_ECS exceptions — never trim an unarchived row.
   DELETE USING LEFT JOIN is NULL-safe and efficient via DISTKEY(order_id) co-location
   — the hash anti-join is always local, no cross-node shuffle. #}
{%- set _ph2 = "DELETE FROM {{ this }} USING (SELECT ecs.order_id FROM {{ this }} ecs LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id LEFT JOIN {{ var('mart_schema') }}.order_tracking_exceptions ex ON ex.order_id = ecs.order_id AND ex.exception_type = 'ARCHIVE_ROUTING_GAP_ECS' AND ex.resolved_at IS NULL WHERE uti.order_id IS NULL AND ex.order_id IS NULL) to_trim WHERE {{ this }}.order_id = to_trim.order_id" -%}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['partner_id', 'add_time', 'order_id'],
        post_hook=[_ph1a, _ph1c, _ph2]
    )
}}

/*
    Mart model for ecs_order_info — one row per active order, order creation metadata.
    - Unique key: order_id
    - Source scan: 2h from raw's MAX(add_time) — catches late-arriving new orders safely
    - Strategy: delete+insert, no incremental_predicates
                DELETE = WHERE order_id IN (batch) via DISTKEY(order_id) hash join
                ECS is write-once: DELETE is a near no-op on normal cycles (new orders
                not yet in mart), correct on retries (removes duplicate before re-insert)
    - Retention: order_id-driven — trimmed only when mart_uti trims the same order
    - Post-hooks: 3 steps (archive inactive, safety check, trim with DELETE USING anti-join)
    - DISTKEY(order_id): JOIN co-location with mart_uti and mart_uts
    - SORTKEY(partner_id, add_time, order_id): partner_id filter cuts to ~0.3% of rows
                for downstream queries (WHERE partner_id = X)
    - DAG dependency: runs AFTER mart_uni_tracking_info (enforced by ref() in post_hooks)
*/

with filtered as (
    select *
    from settlement_public.ecs_order_info_raw
    {% if is_incremental() %}
    where add_time > (
        select coalesce(max(add_time), 0) - 7200
        from settlement_public.ecs_order_info_raw
    ) -- 2h source scan from raw's latest (raw-relative, not mart-relative)
    {% if var('source_end_time', none) %}
    and add_time <= {{ var('source_end_time') }} -- optional cap for testing
    {% endif %}
    {% else %}
    -- First run: load everything from raw
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by add_time desc
        ) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
