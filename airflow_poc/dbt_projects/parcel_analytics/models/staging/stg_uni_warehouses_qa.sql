  -- Staging model for uni_warehouses
  {{ config(materialized='view') }}

  select
      *
  from {{ source('redshift_raw_data', 'uni_warehouses_ref') }}
  where id is not null