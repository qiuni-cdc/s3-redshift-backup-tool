{#
    archive_to_hist — Archive aged-out rows to 6-month hist tables.

    Parameters:
        source_table   : dbt relation ({{ this }}) to archive FROM
        ts_col         : timestamp column name (epoch integer) used for routing
        hist_prefix    : hist table name prefix, e.g. 'hist_uni_tracking_info'
        exception_type : exception_type string written to order_tracking_exceptions
                         (default: 'ARCHIVE_ROUTING_GAP')

    Behaviour:
        1. For each period in var('hist_periods'), INSERT rows from source_table
           into the matching hist table (routed by ts_col range).
           NOT IN guard prevents duplicate inserts on retry.
        2. Safety check: INSERT any rows that did NOT match any period into
           order_tracking_exceptions. These rows are excluded from the trim step.

    Periods are configured in dbt_project.yml:
        vars:
          hist_periods:
            - name: '2025_h2'
              start: '2025-07-01'
              end:   '2026-01-01'
            - name: '2026_h1'
              start: '2026-01-01'
              end:   '2026-07-01'

    Adding a new 6-month period: add ONE entry to hist_periods in dbt_project.yml.
    No SQL editing required.

    Usage (called via dbt run-operation, NOT directly in post_hook):
        dbt run-operation archive_to_hist \
            --args '{"source_table": "settlement_ods.mart_uni_tracking_info",
                     "ts_col": "update_time",
                     "hist_prefix": "settlement_ods.hist_uni_tracking_info"}'

    NOTE: Mart models use EXPLICIT inline SQL in post_hooks (not this macro) because
    dbt post_hook entries must be single SQL statements and psycopg2 (Redshift adapter)
    does not support multi-statement execution in a single execute() call.
    This macro is the reference implementation — use it for ad-hoc backfills and
    one-off archival operations via dbt run-operation.

    Retention cutoff: MAX(ts_col) - 15552000 seconds (180 days / 6 months).
#}
{% macro archive_to_hist(source_table, ts_col, hist_prefix, exception_type='ARCHIVE_ROUTING_GAP') %}

  {% if execute %}
    {% set periods = var('hist_periods') %}
    {% set cutoff_subquery %}(SELECT COALESCE(MAX({{ ts_col }}), 0) - 15552000 FROM {{ source_table }}){% endset %}

    -- Step 1: Archive rows to each hist period
    {% for p in periods %}
      {% set archive_sql %}
        INSERT INTO {{ hist_prefix }}_{{ p.name }}
        SELECT * FROM {{ source_table }}
        WHERE {{ ts_col }} <  {{ cutoff_subquery }}
          AND {{ ts_col }} >= extract(epoch from '{{ p.start }}'::timestamp)
          AND {{ ts_col }} <  extract(epoch from '{{ p.end }}'::timestamp)
          AND order_id NOT IN (SELECT order_id FROM {{ hist_prefix }}_{{ p.name }})
      {% endset %}
      {% do run_query(archive_sql) %}
      {{ log("archive_to_hist: archived to " ~ hist_prefix ~ "_" ~ p.name, info=True) }}
    {% endfor %}

    -- Step 2: Safety check — catch rows outside all defined period windows
    {% set safety_sql %}
      INSERT INTO order_tracking_exceptions (order_id, exception_type, detected_at, notes)
      SELECT DISTINCT
          order_id,
          '{{ exception_type }}',
          CURRENT_TIMESTAMP,
          'Timestamp outside all hist periods — excluded from trim. Source: {{ source_table }}'
      FROM {{ source_table }}
      WHERE {{ ts_col }} < {{ cutoff_subquery }}
      {% for p in periods %}
        AND order_id NOT IN (SELECT order_id FROM {{ hist_prefix }}_{{ p.name }})
      {% endfor %}
        AND order_id NOT IN (
            SELECT order_id FROM order_tracking_exceptions
            WHERE exception_type = '{{ exception_type }}'
              AND resolved_at IS NULL
        )
    {% endset %}
    {% do run_query(safety_sql) %}
    {{ log("archive_to_hist: safety check complete for " ~ source_table, info=True) }}

  {% endif %}

{% endmacro %}
