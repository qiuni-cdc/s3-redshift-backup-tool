{% macro redshift__get_columns_in_relation(relation) -%}
  {#-
    Custom get_columns_in_relation for Redshift.
    Uses schema.table instead of database.schema.table in catalog query.
  -#}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale
    from information_schema.columns
    where table_schema = '{{ relation.schema }}'
      and table_name = '{{ relation.identifier }}'
    order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}
