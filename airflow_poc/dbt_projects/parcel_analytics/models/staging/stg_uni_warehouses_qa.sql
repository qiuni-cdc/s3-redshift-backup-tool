  -- Staging model for uni_warehouses
  {{ config(materialized='view') }}

  select
      *
  from {{ source('kuaisong', 'uni_warehouses') }}
  where id is not null