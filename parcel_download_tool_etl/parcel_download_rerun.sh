#!/bin/bash

# Parcel Download ETL - Hour by Hour Processing
# Usage: ./parcel_download_run.sh <start_time> <end_time> [pipeline]
# Example: ./parcel_download_run.sh "2025-11-04 22:00:00" "2025-11-05 19:00:00"
# Example: ./parcel_download_run.sh "2025-11-04 22:00:00" "2025-11-05 19:00:00" "us_dw_unidw_2_settlement_dws_pipeline"

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./parcel_download_run.sh <start_time> <end_time> [pipeline]"
    echo "Example: ./parcel_download_run.sh \"2025-11-04 22:00:00\" \"2025-11-05 19:00:00\""
    echo "Example: ./parcel_download_run.sh \"2025-11-04 22:00:00\" \"2025-11-05 19:00:00\" \"us_dw_unidw_2_settlement_dws_pipeline\""
    exit 1
fi

START_TIME=$1
END_TIME=$2
PIPELINE=${3:-"us_dw_unidw_2_settlement_dws_pipeline_direct"}

echo "========================================"
echo "Parcel Download ETL"
echo "From: $START_TIME"
echo "To:   $END_TIME"
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

# Convert to timestamps for comparison
CURRENT=$(date -d "$START_TIME" +%s)
END=$(date -d "$END_TIME" +%s)

# Calculate total hours
TOTAL_HOURS=$(( (END - CURRENT) / 3600 ))
if [ $TOTAL_HOURS -le 0 ]; then
    echo "❌ End time must be after start time"
    deactivate
    exit 1
fi

CURRENT_HOUR=1

echo "Total hours to process: $TOTAL_HOURS"
echo ""

while [ $CURRENT -lt $END ]; do
    # Window end is current + 1 hour
    WINDOW_END=$((CURRENT + 3600))

    WINDOW_START_FMT=$(date -d "@$CURRENT" "+%Y-%m-%d %H:%M:%S")
    WINDOW_END_FMT=$(date -d "@$WINDOW_END" "+%Y-%m-%d %H:%M:%S")
    WINDOW_START_SAFE=$(date -d "@$CURRENT" "+%Y%m%d_%H%M%S")

    LOG_FILE="$LOG_DIR/backfill_${WINDOW_START_SAFE}.log"

    echo "----------------------------------------"
    echo "Processing [$CURRENT_HOUR/$TOTAL_HOURS]: $WINDOW_START_FMT to $WINDOW_END_FMT"
    echo "Log: $LOG_FILE"
    echo "----------------------------------------"

    # Run sync for this 1-hour window
    python parcel_download_and_sync.py \
        -d "$WINDOW_END_FMT" \
        --hours "-1" \
        --pipeline "$PIPELINE" \
        > "$LOG_FILE" 2>&1
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Completed successfully"
    else
        echo "❌ Failed - check log file: $LOG_FILE"
        echo "Continue with next hour? (y/n)"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "Stopping ETL"
            deactivate
            exit 1
        fi
    fi

    # Move to next hour
    CURRENT=$((CURRENT + 3600))
    CURRENT_HOUR=$((CURRENT_HOUR + 1))

    # Small delay between runs
    sleep 2
done

# Deactivate virtualenv
deactivate

echo ""
echo "========================================"
echo "✅ All $TOTAL_HOURS hours completed"
echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
exit 0
