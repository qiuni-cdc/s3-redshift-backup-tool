{# Fetch the cutoff time (max(update_time) - 7 days in seconds) dynamically #}
{%- set cutoff_time_query -%}
    select coalesce(max(update_time), 0) - 604800 from {{ this }}
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
        sort='update_time',
        incremental_predicates=[
            this ~ ".update_time > " ~ cutoff_time
        ]
    )
}}

/*
    Staging model for uni_tracking_info
    - MERGE strategy: updates existing records when state changes
    - Optimized with dist/sort keys for fast joins
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_info_raw
    {% if is_incremental() %}
    where update_time > {{ cutoff_time }} -- Uses the 7-day lookback calculated above
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
