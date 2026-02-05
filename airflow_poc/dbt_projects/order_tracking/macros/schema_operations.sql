{#-
    Custom schema operations for Redshift compatibility.
    Ensures database prefix is not used in schema DDL.
-#}

{#-
    Override generate_schema_name to use custom schema directly (no prefix).
    This allows test failures to be stored in 'settlement_dws' instead of
    'settlement_ods_settlement_dws'.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}

{% macro redshift__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier().schema }}
  {%- endcall -%}
{% endmacro %}

{% macro redshift__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier().schema }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro redshift__check_schema_exists(information_schema, schema) -%}
  {# Check schema exists without using database prefix #}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    select count(*) from pg_namespace where nspname ilike '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro redshift__list_schemas(database) -%}
  {# List schemas without using database prefix #}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct nspname as schema_name
    from pg_namespace
    where nspname not like 'pg_%'
    and nspname != 'information_schema'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{#- Grant handling - avoid using database name -#}
{% macro redshift__get_show_grant_sql(relation) %}
  {# Return empty - Redshift grant checking not needed for test failures #}
  {{ return('select 1 where false') }}
{% endmacro %}

{% macro redshift__copy_grants() %}
  {# No-op for Redshift - avoid permission issues #}
  {{ return('') }}
{% endmacro %}


{#- Persist docs - avoid using database name -#}
{% macro redshift__persist_docs(relation, model, for_relation, for_columns) %}
  {# No-op for Redshift - avoid permission issues with database-level operations #}
{% endmacro %}


{#- Information schema name - return schema only, no database -#}
{% macro redshift__information_schema_name(database) -%}
  information_schema
{%- endmacro %}
