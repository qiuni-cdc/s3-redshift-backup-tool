{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Delete cutoff: 20 days — limits DELETE scan on staging via SORTKEY zone maps.

    Strategy: delete+insert (not merge)
    - Orders don't stay active beyond 20 days → any order in a new batch has update_time within 20 days
    - delete+insert with incremental_predicates limits DELETE to 20-day window via SORTKEY zone maps
    - Old rows outside the 20-day window are never re-extracted → no correctness risk
    - Subquery in incremental_predicates evaluated by Redshift at runtime → zone map pruning applies

    Edge case: update_time can change after 2+ months (very rare).
    - Old row (update_time > 20 days ago) not deleted → new version inserted → duplicate order_id
    - post_hook detects and removes the older duplicate after every run
    - post_hook only checks order_ids from the recent batch (not full table scan) → stays fast as table grows
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
        incremental_predicates=[
            this ~ ".update_time > (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM " ~ this ~ ")"
        ],
        post_hook=[
            "DELETE FROM {{ this }} USING (SELECT all_rows.order_id, MAX(all_rows.update_time) AS max_ut FROM {{ this }} all_rows WHERE all_rows.order_id IN (SELECT DISTINCT order_id FROM settlement_public.uni_tracking_info_raw WHERE update_time >= (SELECT COALESCE(MAX(update_time), 0) - 1800 FROM {{ this }})) GROUP BY all_rows.order_id HAVING COUNT(*) > 1) dups WHERE {{ this }}.order_id = dups.order_id AND {{ this }}.update_time < dups.max_ut"
        ]
    )
}}

/*
    Staging model for uni_tracking_info (latest tracking state per order)
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: delete+insert with 20-day incremental_predicates on update_time
    - update_time always reflects latest state → ranked dedup keeps only latest per order_id
    - post_hook removes rare duplicates from late updates (update_time > 20 days old), scoped to recent batch order_ids only
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
        row_number() over (partition by order_id order by update_time desc) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
order by update_time, order_id
