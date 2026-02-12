{# Fetch the cutoff time (max(add_time) - 30 days in seconds) dynamically #}
{%- set cutoff_time_query -%}
    select coalesce(max(add_time), 0) - 2592000 from {{ this }}
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
        unique_key='order_id',
        incremental_strategy='delete+insert',
        dist='order_id',
        sort='add_time',
        incremental_predicates=[
            this ~ ".add_time > " ~ cutoff_time
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
    where add_time > {{ cutoff_time }} -- Uses the 30-day lookback calculated above
    {% else %}
    -- First run: load everything from raw
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
