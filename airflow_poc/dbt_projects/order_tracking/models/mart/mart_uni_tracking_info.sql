{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Retention: 6 months (15552000 seconds) on update_time.

    Strategy: delete+insert
    - No incremental_predicates: DELETE is WHERE order_id IN (batch), no time constraint.
    - Always correct for any order lifecycle — long-lifecycle orders (90+ days) handled
      without duplicates because DELETE finds the old row regardless of its age.
    - Retention kept by post_hook (step 4) rather than by incremental_predicates.

    Post-hooks (4 steps, order is critical):
      Step 1: Clean stale hist_uti entry for reactivated orders.
              Run BEFORE archive — stale hist entry must be removed before re-archiving.
      Step 2a/b: Archive aged-out rows to correct 6-month hist table (routed by update_time).
              NOT IN guard makes archive idempotent — safe to retry after partial failure.
      Step 3: Safety check — any row aged out but not archived is logged to exceptions.
              Rows in exceptions are EXCLUDED from trim (step 4) — no silent data loss.
      Step 4: Trim aged-out rows. Excludes ARCHIVE_ROUTING_GAP exceptions.

    NOTE: hist table names and period dates in post_hooks are updated every 6 months.
    Update the step 1 table name and add/remove step 2 INSERT blocks in dbt_project.yml
    period rotation (1 Jan and 1 Jul). See: airflow_poc/docs/order_tracking_final_design.md §15.

    See macros/archive_to_hist.sql for the reference macro (used with dbt run-operation,
    not in post_hooks directly due to psycopg2 single-statement execution constraint).
#}


{# STEP 1: Clean stale hist_uti entry for reactivated orders.
   A reactivated order re-enters the mart with a fresh update_time — its previously archived
   hist row is now stale. Delete it before the archive cycle writes a new one.
   Orders are never dormant >~1 year so the stale entry is always in the latest hist table.
   UPDATE THIS TABLE NAME on 1 Jan and 1 Jul (always points to the current hist table). #}
{%- set _ph1 = "DELETE FROM {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2 WHERE order_id IN (SELECT order_id FROM {{ this }} WHERE update_time >= (SELECT COALESCE(MAX(update_time), 0) - 900 FROM {{ this }}))" -%}

{# STEP 2a: Archive aged-out rows to 2025_h2 (Jul 2025 – Jan 2026).
   NOT IN guard: idempotent — skips order_ids already in hist on retry.
   ADD a step 2b block here when data starts aging into the next period (see dbt_project.yml). #}
{%- set _ph2a = "INSERT INTO {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2 SELECT * FROM {{ this }} WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }}) AND update_time >= extract(epoch from '2025-07-01'::timestamp) AND update_time < extract(epoch from '2026-01-01'::timestamp) AND order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2)" -%}

{# STEP 3: Safety check — catch rows aged out but not matched by any period.
   Logged to exceptions and excluded from trim (step 4) — no silent data loss.
   Any ARCHIVE_ROUTING_GAP alert = a missing period in post_hooks (config error).
   Fix: add the missing period INSERT block and mark the exception as resolved. #}
{%- set _ph3 = "INSERT INTO {{ var('mart_schema') }}.order_tracking_exceptions (order_id, exception_type, detected_at, notes) SELECT DISTINCT m.order_id, 'ARCHIVE_ROUTING_GAP', CURRENT_TIMESTAMP, 'update_time outside all defined hist_uti periods — excluded from trim' FROM {{ this }} m WHERE m.update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }}) AND NOT EXISTS (SELECT 1 FROM {{ var('mart_schema') }}.hist_uni_tracking_info_2025_h2 WHERE order_id = m.order_id) AND NOT EXISTS (SELECT 1 FROM {{ var('mart_schema') }}.order_tracking_exceptions WHERE order_id = m.order_id AND exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL)" -%}

{# STEP 4: Trim aged-out rows from active mart.
   Excludes rows with open ARCHIVE_ROUTING_GAP exceptions — never trim an unarchived row. #}
{%- set _ph4 = "DELETE FROM {{ this }} WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 15552000 FROM {{ this }}) AND order_id NOT IN (SELECT order_id FROM {{ var('mart_schema') }}.order_tracking_exceptions WHERE exception_type = 'ARCHIVE_ROUTING_GAP' AND resolved_at IS NULL)" -%}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort=['update_time', 'order_id'],
        post_hook=[_ph1, _ph2a, _ph3, _ph4]
    )
}}

/*
    Mart model for uni_tracking_info — one row per active order, latest tracking state.
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: delete+insert, no incremental_predicates (always correct for any lifecycle)
    - Retention: 6-month rolling window on update_time
    - Post-hooks: 4 steps (reactivation cleanup, archive, safety check, trim)
    - DISTKEY(order_id): 3-way JOIN with mart_ecs and mart_uts is always co-located
    - SORTKEY(update_time, order_id): zone maps for time-range queries and retention trim
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    {% if is_incremental() %}
    where update_time > (
        select coalesce(max(update_time), 0) - 1800
        from {{ this }}
    ) -- 30-min source scan: only latest extraction batch
    {% if var('source_end_time', none) %}
    and update_time <= {{ var('source_end_time') }} -- optional cap for testing
    {% endif %}
    {% else %}
    -- First run: load everything from raw (full load).
    -- Retention post_hook fires on first run — hist tables must exist before deploying.
    -- See: airflow_poc/docs/order_tracking_final_design.md §15 (Day 0 steps).
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by update_time desc
        ) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
