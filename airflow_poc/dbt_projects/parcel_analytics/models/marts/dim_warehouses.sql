  -- Warehouse dimension table
  {{ config(materialized='table') }}

  select
      *,
      current_timestamp as dbt_updated_at
  from {{ ref('stg_uni_warehouses_qa') }}