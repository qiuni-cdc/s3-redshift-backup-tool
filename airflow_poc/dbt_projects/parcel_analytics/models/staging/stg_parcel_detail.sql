-- Staging model for parcel detail data
-- Simple pass-through of all data

{{ config(materialized='view') }}

-- Pass-through of all data (removed limit for production use)
select * from {{ source('raw_data', 'dw_parcel_detail_tool_raw') }}
where order_id is not null  -- Filter out records without order_id
