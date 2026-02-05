{% macro ref(model_name) %}
    {#-
        Custom ref macro for Redshift compatibility.
        Returns Relation with database excluded from rendering
        because Redshift doesn't support 3-part naming for current database.
    -#}
    {%- set relation = builtins.ref(model_name) -%}
    {{ return(relation.include(database=false)) }}
{% endmacro %}
