{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Delete window: 2 hours — handles extraction retries via incremental_predicates on pathTime.
    Retention: pure time-based — 6 months (15552000 seconds) on pathTime.

    Strategy: delete+insert
    - Composite unique key: (order_id, traceSeq, pathTime)
    - New spath events (new pathTime) always land in recent range — incremental_predicates
      on pathTime safely handles retries without touching historical events.
    - Historical events have different pathTime → never matched for deletion.

    Why pure time-based retention (NOT order_id-driven):
    - mart_uts holds MANY rows per order (every scan event in 6 months).
    - Order_id-driven retention would keep ALL spath events for every active order
      indefinitely — unbounded table growth at 195M rows/month.
    - Pure 6-month pathTime cap keeps mart_uts at ~1.17B rows permanently.
    - Old spath events for active orders are in hist_uts — accessible for historical queries.
    - Primary query always filters WHERE pathTime >= X — old spath events not needed in mart.

    Post-hooks (3 steps, independent of mart_uni_tracking_info):
      Step 1a/b: Archive spath events older than 6 months to correct hist_uts table.
                 NOT IN guard: idempotent on retry (checks order_id within the period range).
      Step 2: Safety check — log events outside all defined period windows to exceptions.
              Trim (step 3) excludes them — no silent data loss.
      Step 3: Pure time-based trim — excludes ARCHIVE_ROUTING_GAP_UTS exceptions.

    DAG dependency: independent — can run in parallel with mart_ecs_order_info.
    Both run after mart_uni_tracking_info (mart_uts uses pure time-based, no mart_uti read,
    but DAG still runs after mart_uti for cycle ordering).
#}

{%- set source_cutoff_query -%}
    select coalesce(max(pathTime), 0) - 1800 from {{ this }}
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
        unique_key=['order_id', 'traceSeq', 'pathTime'],
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['pathTime', 'order_id'],
        incremental_predicates=[
            this ~ ".pathTime > (SELECT COALESCE(MAX(pathTime), 0) - 7200 FROM " ~ this ~ ")"
        ],
        post_hook=[

            -- STEP 1a: Archive to 2025_h2 (Jul 2025 – Jan 2026).
            -- NOT IN guard filters by order_id within the period range to ensure we only
            -- skip order_ids that already have events archived in this specific period.
            "INSERT INTO {{ var('mart_schema') }}.hist_uni_tracking_spath_2025_h2 SELECT * FROM {{ this }} WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }}) AND pathTime >= extract(epoch from '2025-07-01'::timestamp) AND pathTime < extract(epoch from '2026-01-01'::timestamp) AND order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.hist_uni_tracking_spath_2025_h2 WHERE pathTime >= extract(epoch from '2025-07-01'::timestamp))",

            -- STEP 1b: Archive to 2026_h1 (Jan 2026 – Jul 2026).
            "INSERT INTO {{ var('mart_schema') }}.hist_uni_tracking_spath_2026_h1 SELECT * FROM {{ this }} WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }}) AND pathTime >= extract(epoch from '2026-01-01'::timestamp) AND pathTime < extract(epoch from '2026-07-01'::timestamp) AND order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.hist_uni_tracking_spath_2026_h1 WHERE pathTime >= extract(epoch from '2026-01-01'::timestamp))",

            -- STEP 2: Safety check — catch spath events aged out but outside all defined periods.
            -- Checks (order_id, pathTime) pair in each hist table — specific enough for spath.
            -- Any ARCHIVE_ROUTING_GAP_UTS alert = a missing period in post_hooks (config error).
            "INSERT INTO {{ var('mart_schema') }}.order_tracking_exceptions (order_id, exception_type, detected_at, notes) SELECT DISTINCT m.order_id, 'ARCHIVE_ROUTING_GAP_UTS', CURRENT_TIMESTAMP, 'pathTime outside all defined hist_uts periods — excluded from trim' FROM {{ this }} m WHERE m.pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }}) AND NOT EXISTS (SELECT 1 FROM {{ var('mart_schema') }}.hist_uni_tracking_spath_2025_h2 WHERE order_id = m.order_id AND pathTime = m.pathTime) AND NOT EXISTS (SELECT 1 FROM {{ var('mart_schema') }}.hist_uni_tracking_spath_2026_h1 WHERE order_id = m.order_id AND pathTime = m.pathTime) AND NOT EXISTS (SELECT 1 FROM {{ var('mart_schema') }}.order_tracking_exceptions WHERE order_id = m.order_id AND exception_type = 'ARCHIVE_ROUTING_GAP_UTS' AND resolved_at IS NULL)",

            -- STEP 3: Trim spath events older than 6 months.
            -- Pure time-based: no order_id dependency, no mart_uti read.
            -- Excludes any event flagged as ARCHIVE_ROUTING_GAP_UTS.
            "DELETE FROM {{ this }} WHERE pathTime < (SELECT COALESCE(MAX(pathTime), 0) - 15552000 FROM {{ this }}) AND order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.order_tracking_exceptions WHERE exception_type = 'ARCHIVE_ROUTING_GAP_UTS' AND resolved_at IS NULL)"

        ]
    )
}}

/*
    Mart model for uni_tracking_spath — 6-month rolling window of spath events.
    - Unique key: (order_id, traceSeq, pathTime)
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: delete+insert with 2-hour incremental_predicates on pathTime (retry safety)
    - Retention: pure time-based 6-month window on pathTime (~1.17B rows at steady state)
    - Post-hooks: 3 steps (archive, safety check, trim)
    - DISTKEY(order_id): JOIN co-location with mart_uti and mart_ecs
    - SORTKEY(pathTime, order_id): zone maps skip all blocks before the query date
    - Independent of mart_uti state — can run in parallel with mart_ecs
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_spath_raw
    {% if is_incremental() %}
    where pathTime > {{ source_cutoff }} -- 30-min source scan: only latest extraction batch
    {% if var('source_end_time', none) %}
    and pathTime <= {{ var('source_end_time') }} -- optional cap for testing
    {% endif %}
    {% else %}
    -- First run: load everything from raw
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id, traceSeq
            order by pathTime desc
        ) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
