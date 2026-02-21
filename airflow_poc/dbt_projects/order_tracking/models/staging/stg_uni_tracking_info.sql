{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Scans only the latest raw batch for speed.
    No incremental_predicates: delete is by order_id only (no time restriction).
    This is required because uni_tracking_info tracks the LATEST state per order —
    an old staging row can have any update_time (e.g. 6 months ago). A time-based
    delete window could miss it, leaving a duplicate. Deleting only by order_id
    (via DISTKEY) is fast and always correct.
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
        sort='update_time'
    )
}}

/*
    Staging model for uni_tracking_info (latest tracking state per order)
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Delete: by order_id only — no time restriction, safe for any order age
    - update_time is always NOW when modified, so new rows always land in 30-min window
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
