{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).

    Strategy: merge (not delete+insert)
    - MERGE uses DISTKEY (order_id) co-location → directly targets only batch rows
    - Performance is O(batch_size), not O(table_size)
    - delete+insert with IN subquery scans the entire staging table on every DELETE,
      which becomes unusable as staging grows (13.5M rows now → 200M+ rows at 1 year)
    - update_time changes on every order touch → MERGE UPDATE correctly replaces old state
    - No time restriction needed: MERGE matches by order_id directly, no window required
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
        incremental_strategy='merge',
        dist='order_id',
        sort='update_time'
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
