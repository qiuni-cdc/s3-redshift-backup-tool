{# Fetch the max(add_time) from the destination table dynamically to avoid subquery in MERGE predicate #}
{%- set max_add_time_query -%}
    select coalesce(max(add_time), 0) from {{ this }}
{%- endset -%}

{%- set max_add_time = 0 -%}
{%- if execute and is_incremental() -%}
    {%- set result = run_query(max_add_time_query) -%}
    {%- if result and result.columns[0][0] -%}
        {%- set max_add_time = result.columns[0][0] -%}
    {%- endif -%}
{%- endif -%}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        dist='order_id',
        sort='add_time',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.add_time > " ~ max_add_time ~ " - 604800"
        ]
    )
}}

/*
    Staging model for ecs_order_info
    - Deduplicates incoming data (row_number strategy)
    - Handle late-arriving records via merge
    - Optimized with dist/sort keys for fast joins
*/

with filtered as (
    select *
    from settlement_public.ecs_order_info_raw
    {% if is_incremental() %}
    where add_time > (
        select coalesce(max(add_time), 0) - 300  -- 5 min buffer for late arrivals
        from {{ this }}
    )
    {% else %}
    -- First run: only load last 15 minutes (900 seconds)
    where add_time > (EXTRACT(EPOCH FROM GETDATE()) - 900)
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (partition by order_id order by add_time desc) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
