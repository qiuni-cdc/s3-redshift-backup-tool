{#
    Source cutoff: 30 minutes — matches extraction window (15-min DAG + retry buffer).
    Delete cutoff: 2 hours — limits MERGE DELETE scan on staging via incremental_predicates.

    Strategy: merge (not delete+insert)
    - MERGE uses DISTKEY (order_id) co-location → directly targets only batch rows
    - Performance is O(batch_size), not O(table_size)
    - delete+insert with IN subquery scans the entire staging table on every DELETE,
      which becomes unusable as staging grows (17M rows now → 260M+ rows at 1 year)
    - add_time is static (order creation time) → MERGE UPDATE is a no-op for existing rows

    incremental_predicates:
    - add_time is the SORTKEY → Redshift zone maps skip old blocks during MERGE DELETE
    - New orders always have add_time within last 15 min (extraction window)
    - 2-hour window safely covers the batch with margin; old blocks are pruned
    - Subquery evaluated by Redshift at runtime → zone map pruning applies
#}
{%- set source_cutoff_query -%}
    select coalesce(max(add_time), 0) - 1800 from {{ this }}
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
        sort='add_time',
        incremental_predicates=[
            this ~ ".add_time > (SELECT COALESCE(MAX(add_time), 0) - 7200 FROM " ~ this ~ ")"
        ]
    )
}}

/*
    Staging model for ecs_order_info (basic order details)
    - Unique key: order_id
    - Source scan: 30 min (matches 15-min extraction window + retry buffer)
    - Strategy: merge — DISTKEY co-location, O(batch_size) regardless of table size
    - add_time is fixed at order creation — MERGE UPDATE is harmless for existing rows
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
