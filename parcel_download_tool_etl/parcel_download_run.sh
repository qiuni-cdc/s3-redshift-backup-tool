#!/bin/bash

END_DATE=${1:-""}
TOTAL_HOURS=${2:-"-1"}

echo "Running parcel download hourly loop"
echo "End date: $END_DATE, Total hours: $TOTAL_HOURS"
echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    PROJECT_ROOT="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
else
    # Dev Machine (tianzi user)
    PROJECT_ROOT="$HOME/s3-redshift-backup-tool"
fi

cd "$PROJECT_ROOT/parcel_download_tool_etl" || exit 1

LOG_DIR="$PROJECT_ROOT/parcel_download_tool_etl/parcel_download_hourly_log"
mkdir -p "$LOG_DIR"

# Activate virtualenv
source "$PROJECT_ROOT/s3_backup_venv/bin/activate" || {
    echo "❌ Failed to activate virtualenv"
    exit 1
}

# Calculate how many hours to process (remove negative sign)
HOURS_COUNT=$(echo "$TOTAL_HOURS" | tr -d '-')

# Get base timestamp (use END_DATE or current time)
if [ -z "$END_DATE" ]; then
    BASE_TIMESTAMP=$(date '+%s')
else
    BASE_TIMESTAMP=$(date -d "$END_DATE" '+%s')
fi

# Loop through each 1-hour window from oldest to newest
for ((i=1; i<=HOURS_COUNT; i++)); do
    HOUR_OFFSET=$((TOTAL_HOURS + i))
    WINDOW_END_TIMESTAMP=$((BASE_TIMESTAMP + HOUR_OFFSET * 3600))

    LOG_FILE="$LOG_DIR/window_${i}_$(date '+%Y%m%d_%H%M%S').log"

    echo ""
    echo "=========================================="
    echo "Window $i/$HOURS_COUNT (timestamp: $WINDOW_END_TIMESTAMP)"
    echo "Log: $LOG_FILE"
    echo "=========================================="

    # Run sync for this 1-hour window (pass timestamp directly)
    python parcel_download_and_sync.py -d "$WINDOW_END_TIMESTAMP" --hours "-1" --pipeline "us_dw_unidw_2_settlement_dws_pipeline_direct" > "$LOG_FILE" 2>&1
    EXIT_CODE=$?

    # If failed, stop immediately
    if [ $EXIT_CODE -ne 0 ]; then
        echo "❌ Failed at window $i - stopping"
        deactivate
        exit 1
    fi

    echo "✅ Window $i completed"
done

# Deactivate virtualenv
deactivate

echo ""
echo "✅ All $HOURS_COUNT hours completed successfully"
echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
exit 0