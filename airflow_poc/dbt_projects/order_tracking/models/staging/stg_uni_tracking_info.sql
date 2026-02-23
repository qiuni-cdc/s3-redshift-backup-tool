{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Delete cutoff: 15 days — limits MERGE DELETE scan on staging via incremental_predicates.

    Strategy: delete+insert (not merge)
    - Orders don't stay active beyond 20 days → any order in a new batch has update_time within 20 days
    - delete+insert with incremental_predicates limits DELETE to 20-day window via SORTKEY zone maps
    - Old rows outside the 20-day window are never re-extracted → no correctness risk
    - Subquery in incremental_predicates evaluated by Redshift at runtime → zone map pruning applies
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
        sort='update_time',
        incremental_predicates=[
            this ~ ".update_time > (SELECT COALESCE(MAX(update_time), 0) - 1728000 FROM " ~ this ~ ")"
        ]
    )
}}

/*
    Staging model for uni_tracking_info (latest tracking state per order)
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: merge — DISTKEY co-location, O(batch_size) regardless of table size
    - update_time always reflects latest state → MERGE UPDATE keeps staging current
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    {% if is_incremental() %}
    where update_time > {{ source_cutoff }} -- 30-min source scan: only latest extraction batch
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
