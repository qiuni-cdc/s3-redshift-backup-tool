{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Delete window: 2 hours — handles extraction retries via incremental_predicates on add_time.
    Retention: order_id-driven — a row is trimmed only when mart_uni_tracking_info has
               trimmed the same order_id.

    Strategy: delete+insert
    - add_time is set at order creation and never changes. New batches only contain NEW orders.
    - 2-hour incremental_predicates window safely handles duplicate extraction: old orders
      (add_time > 2 hours ago) never appear in new batches, so DELETE never touches them.
    - ECS is write-once — the UPDATE branch of delete+insert is harmless (same data).

    Why order_id-driven retention (NOT time-based on add_time):
    - mart_ecs has ONE row per order. Time-based trim would drop long-lifecycle orders
      (e.g. created Jan 2024, still active Aug 2025) when their add_time ages past 6 months.
    - This would break the 3-way JOIN: mart_uti has the order, mart_ecs does not.
    - Order_id-driven: trim mart_ecs only when mart_uti has already trimmed the order.
      The 3-way JOIN is always intact for any active order regardless of add_time age.

    Post-hooks (2 steps, run AFTER mart_uni_tracking_info):
      Step 1a/b: Archive ecs rows for inactive orders (not in mart_uti) to correct hist table.
                 Uses LEFT JOIN anti-join (NULL-safe — avoids NOT IN NULL trap at 90M+ rows).
                 NOT IN guard: idempotent on retry.
      Step 2: Trim — DELETE USING anti-join (NULL-safe, Redshift hash anti-join on DISTKEY).

    DAG dependency: mart_ecs MUST run after mart_uni_tracking_info completes.
    The LEFT JOIN in post_hooks reads mart_uti's final state for this cycle.
    Using ref('mart_uni_tracking_info') in post_hooks creates the dbt compile-time dependency.
#}

{%- set source_cutoff_query -%}
    select coalesce(max(add_time), 0) - 1800 from {{ this }}
{%- endset -%}

{%- set source_cutoff = 0 -%}
{%- if execute and is_incremental() -%}
    {%- set result = run_query(source_cutoff_query) -%}
    {%- if result and result.columns[0][0] -%}
        {%- set source_cutoff = result.columns[0][0] -%}
    {%- endif -%}
{%- endif -%}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['partner_id', 'add_time', 'order_id'],
        incremental_predicates=[
            this ~ ".add_time > (SELECT COALESCE(MAX(add_time), 0) - 7200 FROM " ~ this ~ ")"
        ],
        post_hook=[

            -- STEP 1a: Archive inactive orders to 2025_h2 (Jul 2025 – Jan 2026).
            -- LEFT JOIN anti-join: orders absent from mart_uti = trimmed from active = inactive.
            -- NULL-safe: LEFT JOIN WHERE uti.order_id IS NULL correctly handles NULL order_ids.
            -- NOT IN guard: idempotent — skips order_ids already in hist on retry.
            "INSERT INTO {{ var('mart_schema') }}.hist_ecs_order_info_2025_h2 SELECT ecs.* FROM {{ this }} ecs LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id WHERE uti.order_id IS NULL AND ecs.add_time >= extract(epoch from '2025-07-01'::timestamp) AND ecs.add_time < extract(epoch from '2026-01-01'::timestamp) AND ecs.order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.hist_ecs_order_info_2025_h2)",

            -- STEP 1b: Archive inactive orders to 2026_h1 (Jan 2026 – Jul 2026).
            "INSERT INTO {{ var('mart_schema') }}.hist_ecs_order_info_2026_h1 SELECT ecs.* FROM {{ this }} ecs LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id WHERE uti.order_id IS NULL AND ecs.add_time >= extract(epoch from '2026-01-01'::timestamp) AND ecs.add_time < extract(epoch from '2026-07-01'::timestamp) AND ecs.order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.hist_ecs_order_info_2026_h1)",

            -- STEP 2: Trim — DELETE USING anti-join.
            -- Removes ecs rows for orders no longer in mart_uti (i.e. trimmed as inactive).
            -- DELETE USING with LEFT JOIN is NULL-safe and efficient via DISTKEY(order_id)
            -- co-location — the hash anti-join is always local, no cross-node shuffle.
            "DELETE FROM {{ this }} USING (SELECT ecs.order_id FROM {{ this }} ecs LEFT JOIN {{ ref('mart_uni_tracking_info') }} uti ON ecs.order_id = uti.order_id WHERE uti.order_id IS NULL) to_trim WHERE {{ this }}.order_id = to_trim.order_id"

        ]
    )
}}

/*
    Mart model for ecs_order_info — one row per active order, order creation metadata.
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: delete+insert with 2-hour incremental_predicates on add_time (retry safety)
    - Retention: order_id-driven — trimmed only when mart_uti trims the same order
    - Post-hooks: 2 steps (archive inactive, trim with DELETE USING anti-join)
    - DISTKEY(order_id): JOIN co-location with mart_uti and mart_uts
    - SORTKEY(partner_id, add_time, order_id): partner_id filter cuts to ~0.3% of rows
    - DAG dependency: runs AFTER mart_uni_tracking_info (enforced by ref() in post_hooks)
*/

with filtered as (
    select *
    from settlement_public.ecs_order_info_raw
    {% if is_incremental() %}
    where add_time > {{ source_cutoff }} -- 30-min source scan: only latest extraction batch
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
            order by add_time desc, id desc  -- id breaks ties deterministically
        ) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
order by add_time, order_id
