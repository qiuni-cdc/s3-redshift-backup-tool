{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        dist='order_id',
        sort='update_time',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.update_time > (select coalesce(max(update_time), 0) - 604800 from " ~ this ~ ")"
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
    where update_time > (
        select coalesce(max(update_time), 0) - 300  -- 5 min buffer
        from {{ this }}
    )
    {% else %}
    -- First run: only load last 15 minutes (900 seconds)
    where update_time > (EXTRACT(EPOCH FROM GETDATE()) - 900)
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
