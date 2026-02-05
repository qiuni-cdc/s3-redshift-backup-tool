{{
    config(
        materialized='table',
        tags=['data_quality', 'sequence_validation']
    )
}}

/*
    Sequence Gap Detection for uni_tracking_spath

    Identifies orders where traceSeq is not continuous (0, 1, 2, 3...)
    This model is used by:
    1. Data quality tests
    2. Backfill process to re-fetch missing events
    3. Monitoring dashboards
*/

with event_sequences as (
    select
        order_id,
        traceSeq,
        lag(traceSeq) over (
            partition by order_id
            order by traceSeq
        ) as prev_seq,
        pathTime
    from {{ ref('stg_uni_tracking_spath') }}
),

-- Find gaps (where current seq - previous seq > 1)
gaps as (
    select
        order_id,
        prev_seq + 1 as gap_start,
        traceSeq - 1 as gap_end,
        traceSeq - prev_seq - 1 as missing_count,
        pathTime as detected_at_time
    from event_sequences
    where prev_seq is not null
      and traceSeq - prev_seq > 1
),

-- Also find orders missing traceSeq = 0 (first event)
missing_first_event as (
    select
        order_id,
        0 as gap_start,
        min(traceSeq) - 1 as gap_end,
        min(traceSeq) as missing_count,
        min(pathTime) as detected_at_time
    from {{ ref('stg_uni_tracking_spath') }}
    group by order_id
    having min(traceSeq) > 0
)

select
    order_id,
    gap_start,
    gap_end,
    missing_count,
    detected_at_time,
    'mid_sequence' as gap_type,
    current_timestamp as _detected_at
from gaps

union all

select
    order_id,
    gap_start,
    gap_end,
    missing_count,
    detected_at_time,
    'missing_first_event' as gap_type,
    current_timestamp as _detected_at
from missing_first_event
