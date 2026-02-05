/*
    Test: State Consistency Check

    Verifies that uni_tracking_info.state matches the latest
    uni_tracking_spath.code for each order.

    A mismatch indicates sync timing issues or data corruption.
    Returns rows where there's a mismatch (test fails if any rows returned).
*/
{{ config(severity = 'warn') }}

with latest_uts_event as (
    select
        order_id,
        code as latest_event_code,
        traceSeq as latest_seq
    from (
        select
            order_id,
            code,
            traceSeq,
            row_number() over (
                partition by order_id
                order by traceSeq desc
            ) as rn
        from {{ ref('stg_uni_tracking_spath') }}
    ) ranked
    where rn = 1
),

mismatches as (
    select
        uti.order_id,
        uti.state as uti_state,
        uts.latest_event_code as uts_latest_code,
        uts.latest_seq,
        uti.update_time
    from {{ ref('stg_uni_tracking_info') }} uti
    inner join latest_uts_event uts on uti.order_id = uts.order_id
    where uti.state != uts.latest_event_code
      -- Only check recent orders (last 24 hours)
      and uti.update_time >= extract(epoch from current_timestamp - interval '24 hours')
)

-- Return mismatches (test fails if count > 0)
select * from mismatches
