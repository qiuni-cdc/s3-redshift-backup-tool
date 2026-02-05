{#-
    Custom relation operations for Redshift compatibility.
    Uses schema.table instead of database.schema.table.
-#}

{% macro redshift__drop_relation(relation) -%}
  {%- set relation_no_db = relation.include(database=false) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation_no_db }} cascade
  {%- endcall %}
{% endmacro %}

{% macro redshift__truncate_relation(relation) -%}
  {%- set relation_no_db = relation.include(database=false) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation_no_db }}
  {%- endcall %}
{% endmacro %}

{% macro redshift__rename_relation(from_relation, to_relation) -%}
  {%- set from_no_db = from_relation.include(database=false) -%}
  {%- set to_no_db = to_relation.include(database=false) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_no_db }} rename to {{ to_no_db.identifier }}
  {%- endcall %}
{% endmacro %}


{% macro redshift__list_relations_without_caching(schema_relation) %}
  {# List relations without using database prefix in query #}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schemaname as schema,
      'table' as type
    from pg_tables
    where schemaname ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schemaname as schema,
      'view' as type
    from pg_views
    where schemaname ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro redshift__get_relation(database, schema, identifier) %}
  {# Override to not use database in the lookup #}
  {% set relations = adapter.list_relations_without_caching(
    schema_relation=api.Relation.create(database=database, schema=schema)
  ) %}
  {% for relation in relations %}
    {% if relation.identifier|lower == identifier|lower %}
      {{ return(relation) }}
    {% endif %}
  {% endfor %}
  {{ return(none) }}
{% endmacro %}


{% macro redshift__current_timestamp() -%}
  getdate()
{%- endmacro %}


{% macro redshift__get_catalog(information_schema, schemas) -%}
  {#-
    Override get_catalog to not use database prefix in queries.
    This prevents "permission denied for database" errors.
  -#}
  {%- call statement('get_catalog', fetch_result=True) -%}
    select
      null as table_database,
      t.table_schema,
      t.table_name,
      t.table_type,
      c.column_name,
      c.ordinal_position as column_index,
      c.data_type as column_type,
      c.character_maximum_length,
      c.numeric_precision,
      c.numeric_scale,
      null as column_comment
    from information_schema.tables t
    left join information_schema.columns c
      on t.table_schema = c.table_schema
      and t.table_name = c.table_name
    where t.table_schema in (
      {%- for schema in schemas -%}
        '{{ schema }}'{% if not loop.last %}, {% endif %}
      {%- endfor -%}
    )
    order by t.table_schema, t.table_name, c.ordinal_position
  {%- endcall -%}
  {{ return(load_result('get_catalog').table) }}
{%- endmacro %}
