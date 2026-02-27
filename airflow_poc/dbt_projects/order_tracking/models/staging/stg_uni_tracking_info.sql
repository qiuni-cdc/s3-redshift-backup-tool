{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Retention: 20 days — keeps staging compact as an active-orders working layer.

    Strategy: delete+insert (not merge)
    - No incremental_predicates: DELETE is WHERE order_id IN (batch) — no time window.
    - Always correct regardless of order lifecycle (0 days or 90 days).
    - Retention post_hook trims rows older than 20 days after every cycle, keeping
      the table at ~10-14M rows so DELETE scans stay fast without zone map pruning.

    Why remove incremental_predicates:
    - Old design: 20-day window on DELETE, two-band post_hooks to fix resulting duplicates.
    - Root cause: orders with lifecycle > 20 days kept stale duplicate rows.
    - Fix: remove the time window entirely. order_id DELETE always finds the old row.
    - Once retention keeps the table at 20-day size, zone map pruning from
      incremental_predicates provides zero benefit — all rows are already in the window.

    See: airflow_poc/docs/stg_uti_dedup_final.md for full analysis.
#}
{%- set source_cutoff_query -%}
    select coalesce(max(update_time), 0) - 1800 from {{ this }}
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
        sort=['update_time', 'order_id'],
        post_hook=[
            "DELETE FROM {{ this }} WHERE update_time < (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM {{ this }})"
        ]
    )
}}

/*
    Staging model for uni_tracking_info (latest tracking state per order)
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: delete+insert with no time-window constraint (always correct)
    - update_time always reflects latest state → ranked dedup keeps only latest per order_id
    - Tie-break: id DESC (stable secondary sort for non-deterministic update_time ties)
    - post_hook: retention trim to 20-day window — keeps table compact at ~10-14M rows
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    {% if is_incremental() %}
    where update_time > {{ source_cutoff }} -- 30-min source scan: only latest extraction batch
    {% if var('source_end_time', none) %}
    and update_time <= {{ var('source_end_time') }} -- optional cap for testing
    {% endif %}
    {% else %}
    -- First run: load everything from raw (process what was extracted)
    -- This ensures that whatever data is in the raw table (from the extraction task) is staged,
    -- even if the extraction happened earlier or covers a historical period.
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
order by update_time, order_id
