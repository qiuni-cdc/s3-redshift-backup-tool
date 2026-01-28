#!/bin/bash

# Parcel Download ETL - Flexible Interval Processing with Dynamic Hours
# Usage: ./parcel_download_rerun_flexible.sh <start_time> <end_time> [interval_hours] [pipeline]
# Example: ./parcel_download_rerun_flexible.sh "2025-11-12 02:00:00" "2025-12-23 19:00:00" 24
# Example: ./parcel_download_rerun_flexible.sh "2025-11-12 02:00:00" "2025-12-23 19:00:00" 6 "us_dw_unidw_2_settlement_dws_pipeline"

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./parcel_download_rerun_flexible.sh <start_time> <end_time> [interval_hours] [pipeline]"
    echo "Example: ./parcel_download_rerun_flexible.sh \"2025-11-12 02:00:00\" \"2025-12-23 19:00:00\" 24"
    echo "Example: ./parcel_download_rerun_flexible.sh \"2025-11-12 02:00:00\" \"2025-12-23 19:00:00\" 6 \"us_dw_unidw_2_settlement_dws_pipeline\""
    exit 1
fi

START_TIME=$1
END_TIME=$2
INTERVAL_HOURS=${3:-24}  # Default to 24 hours
INTERVAL_HOURS=${INTERVAL_HOURS#-}  # Remove negative sign if present (ensure positive)
PIPELINE=${4:-"us_dw_unidw_2_settlement_dws_pipeline_direct"}

# Capture run timestamp for log files
RUN_TIMESTAMP=$(date '+%Y%m%d_%H%M%S')

echo "========================================"
echo "Parcel Download ETL (Flexible Interval)"
echo "From: $START_TIME"
echo "To:   $END_TIME"
echo "Interval: $INTERVAL_HOURS hours"
echo "Pipeline: $PIPELINE"
echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    PROJECT_ROOT="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
else
    # Dev Machine (tianzi user)
    PROJECT_ROOT="$HOME/s3-redshift-backup-tool"
fi

cd "$PROJECT_ROOT/parcel_download_tool_etl" || exit 1

LOG_DIR="$PROJECT_ROOT/parcel_download_tool_etl/backfill_logs"
mkdir -p "$LOG_DIR"

# Activate virtualenv
source "$PROJECT_ROOT/s3_backup_venv/bin/activate" || {
    echo "❌ Failed to activate virtualenv"
    exit 1
}

# Convert to timestamps
CURRENT=$(date -d "$START_TIME" +%s)
END=$(date -d "$END_TIME" +%s)

# Calculate total hours
TOTAL_HOURS=$(( (END - CURRENT) / 3600 ))
if [ $TOTAL_HOURS -le 0 ]; then
    echo "❌ End time must be after start time"
    deactivate
    exit 1
fi

ESTIMATED_CHUNKS=$(( (TOTAL_HOURS + INTERVAL_HOURS - 1) / INTERVAL_HOURS ))

CURRENT_CHUNK=1
HOURS_PROCESSED=0

echo "Total hours to process: $TOTAL_HOURS"
echo "Estimated chunks: $ESTIMATED_CHUNKS"
echo ""

while [ $HOURS_PROCESSED -lt $TOTAL_HOURS ]; do
    # Calculate remaining hours
    REMAINING_HOURS=$((TOTAL_HOURS - HOURS_PROCESSED))

    # Determine window hours: use interval or remaining, whichever is smaller
    if [ $REMAINING_HOURS -lt $INTERVAL_HOURS ]; then
        WINDOW_HOURS=$REMAINING_HOURS
    else
        WINDOW_HOURS=$INTERVAL_HOURS
    fi

    # Calculate window end
    WINDOW_END=$((CURRENT + WINDOW_HOURS * 3600))

    WINDOW_START_FMT=$(date -d "@$CURRENT" "+%Y-%m-%d %H:%M:%S")
    WINDOW_END_FMT=$(date -d "@$WINDOW_END" "+%Y-%m-%d %H:%M:%S")
    WINDOW_START_SAFE=$(date -d "@$CURRENT" "+%Y%m%d_%H%M%S")

    LOG_FILE="$LOG_DIR/backfill_${WINDOW_START_SAFE}_run_${RUN_TIMESTAMP}.log"

    echo "----------------------------------------"
    echo "Processing [$CURRENT_CHUNK/$ESTIMATED_CHUNKS]: $WINDOW_START_FMT to $WINDOW_END_FMT"
    echo "Window duration: $WINDOW_HOURS hours"
    echo "Log: $LOG_FILE"
    echo "----------------------------------------"

    # Run sync for this window with dynamic hours
    python parcel_download_and_sync.py \
        -d "$WINDOW_END_FMT" \
        --hours "-$WINDOW_HOURS" \
        --pipeline "$PIPELINE" \
        > "$LOG_FILE" 2>&1
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Completed successfully"
    else
        echo "❌ Failed - check log file: $LOG_FILE"
        echo "Continue with next chunk? (y/n)"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "Stopping ETL"
            deactivate
            exit 1
        fi
    fi

    # Move to next window
    CURRENT=$WINDOW_END
    HOURS_PROCESSED=$((HOURS_PROCESSED + WINDOW_HOURS))
    CURRENT_CHUNK=$((CURRENT_CHUNK + 1))

    # Small delay between runs
    sleep 2
done

# Deactivate virtualenv
deactivate

echo ""
echo "========================================"
echo "✅ All chunks completed"
echo "Total hours processed: $TOTAL_HOURS"
echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
exit 0
