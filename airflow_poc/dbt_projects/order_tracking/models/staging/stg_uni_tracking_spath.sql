{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Scans only the latest raw batch for speed.
    No incremental_predicates: delete is by compound key (order_id, traceSeq) only.
    Since spath is append-only, each (order_id, traceSeq) is unique — no old staging
    rows need time-based filtering to be found/deleted.
#}
{%- set source_cutoff_query -%}
    select coalesce(max(pathTime), 0) - 1800 from {{ this }}
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
        unique_key=['order_id', 'traceSeq', 'pathTime'],
        incremental_strategy='delete+insert',
        dist='order_id',
        sort='pathTime'
    )
}}

/*
    Staging model for uni_tracking_spath (event history)
    - Composite key: order_id + traceSeq
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Delete: by (order_id, traceSeq) only — precise, no time filter needed
    - Append-only source: each (order_id, traceSeq) event is written once
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_spath_raw
    {% if is_incremental() %}
    where pathTime > {{ source_cutoff }} -- 30-min source scan: only latest extraction batch
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
