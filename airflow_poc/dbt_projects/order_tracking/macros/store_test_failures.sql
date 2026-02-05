{#
   Override test failure storage for Redshift
   Fixes "permission denied for database dw" error by excluding database prefix
   See: https://docs.getdbt.com/reference/resource-configs/store_failures

   Note: redshift__create_table_as is defined in create_table_as.sql and handles
   the database prefix exclusion for all table creation including test failures.
   We only need the test-specific SQL generation macro here.
#}

{% macro redshift__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) %}
    {# Test SQL generation for Redshift - excludes database prefix in test queries #}
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_if }} as should_warn,
      {{ fail_calc }} {{ error_if }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{% endmacro %}
