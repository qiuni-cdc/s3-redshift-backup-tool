{% macro get_where_subquery(relation) -%}
    {# Override to exclude database from relation for Redshift #}
    {%- set relation_no_db = relation.include(database=false) -%}
    (select * from {{ relation_no_db }}) dbt_subquery
{%- endmacro %}
