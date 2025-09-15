-- Deduplicated parcel detail mart
-- Removes duplicate records per order_id, keeping one record per order_id

{{ config(materialized='table') }}

with staged_data as (
    select * from {{ ref('stg_parcel_detail') }}
),

-- Rank records by order_id to identify duplicates
ranked_data as (
    select 
        *,
        row_number() over (
            partition by order_id 
            order by id desc  -- Keep the highest ID (most recent)
        ) as row_rank
    from staged_data
),

-- Keep only the first record per order_id
deduplicated_data as (
    select *
    from ranked_data
    where row_rank = 1
)

select * from deduplicated_data