{% macro redshift__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {#-
    Custom create_table_as for Redshift compatibility.
    Excludes database from relation rendering because Redshift doesn't
    support database.schema.table syntax for current database.
  -#}
  {%- set relation_no_db = relation.include(database=false) -%}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get('sort_type', validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get('sort', validator=validation.any[list, basestring]) -%}
  {%- set _backup = config.get('backup') -%}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation_no_db }}
    {{ dist(_dist) }}
    {{ sort(_sort_type, _sort) }}
    {% if _backup == false -%}backup no{%- endif %}
  as (
    {{ compiled_code }}
  );
{%- endmacro %}
