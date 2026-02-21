{#
    Two separate cutoffs:
    - source_cutoff (30 min): how much raw data to READ — matches 15-min extraction window
    - delete_cutoff (2 hours): how far back to DELETE in staging — safe for ecs_order_info
      because add_time is the ORDER CREATION TIME and never changes. Same-batch overlap
      between consecutive 15-min extractions is at most ~30 min, well within 2 hours.
#}
{%- set source_cutoff_query -%}
    select coalesce(max(add_time), 0) - 1800 from {{ this }}
{%- endset -%}

{%- set delete_cutoff_query -%}
    select coalesce(max(add_time), 0) - 7200 from {{ this }}
{%- endset -%}

{%- set source_cutoff = 0 -%}
{%- set delete_cutoff = 0 -%}
{%- if execute and is_incremental() -%}
    {%- set result = run_query(source_cutoff_query) -%}
    {%- if result and result.columns[0][0] -%}
        {%- set source_cutoff = result.columns[0][0] -%}
    {%- endif -%}
    {%- set result2 = run_query(delete_cutoff_query) -%}
    {%- if result2 and result2.columns[0][0] -%}
        {%- set delete_cutoff = result2.columns[0][0] -%}
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
            this ~ ".add_time > " ~ delete_cutoff
        ]
    )
}}

/*
    Staging model for ecs_order_info (basic order details)
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Delete window: 2 hours (covers same-batch overlaps between consecutive runs)
    - add_time is fixed at order creation — no historical updates expected
*/

with filtered as (
    select *
    from settlement_public.ecs_order_info_raw
    {% if is_incremental() %}
    where add_time > {{ source_cutoff }} -- 30-min source scan: only latest extraction batch
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
