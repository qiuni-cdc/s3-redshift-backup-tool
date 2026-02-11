{# Fetch the cutoff time (max(pathTime) - 14 days in seconds) dynamically #}
{%- set cutoff_time_query -%}
    select coalesce(max(pathTime), 0) - 1209600 from {{ this }}
{%- endset -%}

{%- set cutoff_time = 0 -%}
{%- if execute and is_incremental() -%}
    {%- set result = run_query(cutoff_time_query) -%}
    {%- if result and result.columns[0][0] -%}
        {%- set cutoff_time = result.columns[0][0] -%}
    {%- endif -%}
{%- endif -%}

{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'traceSeq'],
        incremental_strategy='delete+insert',
        dist='order_id',
        sort='pathTime',
        incremental_predicates=[
            "pathTime > " ~ cutoff_time
        ]
    )
}}

/*
    Staging model for uni_tracking_spath (event history)
    - Composite key: order_id + traceSeq
    - Optimized deduplication: strictly one row per key
    - Performance: dist/sort keys added, delete+insert strategy
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_spath_raw
    {% if is_incremental() %}
    where pathTime > (
        select coalesce(max(pathTime), 0) - 300  -- 5 min buffer
        from {{ this }}
    )
    {% else %}
    -- First run: load everything from raw
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (partition by order_id, traceSeq order by pathTime desc) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
