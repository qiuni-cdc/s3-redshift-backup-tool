{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'traceSeq'],
        incremental_strategy='merge',
        dist='order_id',
        sort='pathTime'
    )
}}

/*
    Staging model for uni_tracking_spath (event history)
    - Composite key: order_id + traceSeq
    - Optimized deduplication: strictly one row per key
    - Performance: dist/sort keys added
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
