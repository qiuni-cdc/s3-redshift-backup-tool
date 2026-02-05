{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {#-
        Return target database name for dbt internal operations.
        All SQL-generating macros use relation.include(database=false) to exclude
        the database from actual queries (Redshift uses schema.table format only).
    -#}
    {{ target.dbname }}
{%- endmacro %}
