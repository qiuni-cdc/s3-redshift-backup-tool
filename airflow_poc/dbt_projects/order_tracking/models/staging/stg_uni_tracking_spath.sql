{#
    source_cutoff (30 min): how much raw data to READ — matches 15-min extraction window.
    delete_cutoff (2 hours): how far back to DELETE in staging.

    Safe because:
    - pathTime is part of unique_key AND is the SORTKEY
    - Extraction duplicates share the EXACT same pathTime (always within last 15 min)
    - 2h window covers any duplicate with margin
    - Historical events have different pathTime → never matched for deletion
    - SORTKEY zone maps let Redshift skip 99%+ of old blocks during DELETE

    NOTE: incremental_predicates uses a SQL subquery (not a Jinja variable) because
    config() is evaluated at parse time (execute=False), so run_query() never fires there.
    The subquery is evaluated by Redshift at runtime, giving the correct 2-hour window
    and enabling zone map pruning on the SORTKEY (pathTime).
#}
{%- set source_cutoff_query -%}
    select coalesce(max(pathTime), 0) - 1800 from {{ this }}
{%- endset -%}

{%- set source_cutoff = 0 -%}
{%- if execute and is_incremental() -%}
    {%- set result = run_query(source_cutoff_query) -%}
    {%- if result and result.columns[0][0] -%}
        {%- set source_cutoff = result.columns[0][0] -%}
    {%- endif -%}
{%- endif -%}

{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'traceSeq', 'pathTime'],
        incremental_strategy='delete+insert',
        dist='order_id',
        sort='pathTime',
        incremental_predicates=[
            this ~ ".pathTime > (SELECT COALESCE(MAX(pathTime), 0) - 7200 FROM " ~ this ~ ")"
        ]
    )
}}

/*
    Staging model for uni_tracking_spath (event history)
    - Composite key: order_id + traceSeq + pathTime
    - Source scan: 30 min (read only latest raw batch)
    - Delete window: 2 hours (zone map pruning via SORTKEY)
    - Append-only source: full event history preserved
*/

with filtered as (
    select *
    from settlement_public.uni_tracking_spath_raw
    {% if is_incremental() %}
    where pathTime > {{ source_cutoff }} -- 30-min source scan
    {% if var('source_end_time', none) %}
    and pathTime <= {{ var('source_end_time') }} -- optional cap for testing
    {% endif %}
    {% else %}
    -- First run: load everything from raw
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (partition by order_id, traceSeq order by pathTime desc) as _rn
    from filtered
)

select * exclude(_rn)
from ranked
where _rn = 1
