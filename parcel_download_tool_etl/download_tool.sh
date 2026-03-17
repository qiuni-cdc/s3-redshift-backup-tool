#!/bin/bash

# ETL parcel detail tool
# Usage: ./download_tool.sh <beg_datetime> <hours> [--rerun]
# Example: ./download_tool.sh "2024-08-14 10:00:00" "-2"
# Example: ./download_tool.sh "2024-08-14 10:00:00" "-1" --rerun

if [ -d "/home/ubuntu/data-integration" ]; then
    cd /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/parcel_download_tool_etl
    KITCHEN_PATH="/home/ubuntu/data-integration/kitchen.sh"
else
    cd ~/s3-redshift-backup-tool/parcel_download_tool_etl
    KITCHEN_PATH="/home/tianzi/data-integration/kitchen.sh"
fi

BEG_DATETIME=$1
HOURS=$2
RERUN=${3:-""}

echo "Running ETL with datetime: $BEG_DATETIME, hours: $HOURS"

KITCHEN_PID=""

# Kill the entire Kitchen process group (kitchen.sh + Java) on exit
cleanup() {
    if [ -n "$KITCHEN_PID" ]; then
        kill -- -"$KITCHEN_PID" 2>/dev/null
        sleep 1
        kill -9 -- -"$KITCHEN_PID" 2>/dev/null
    fi
}
trap 'cleanup; exit' EXIT TERM INT

mkdir -p ./logs
LOG_FILE="./logs/download_tool_$(date '+%Y%m%d-%H%M%S').log"

# setsid starts Kitchen in its own process group so kill -- -$KITCHEN_PID
# reaches kitchen.sh + Java, regardless of how deeply Java is nested
if [ "$RERUN" = "--rerun" ]; then
    setsid $KITCHEN_PATH \
        -file=./download_tool_job.kjb \
        -level=Basic \
        -param:beg_datetime="$BEG_DATETIME" \
        -param:hours="$HOURS" \
        >> "$LOG_FILE" 2>&1 &
else
    setsid timeout 50m $KITCHEN_PATH \
        -file=./download_tool_job.kjb \
        -level=Basic \
        -param:beg_datetime="$BEG_DATETIME" \
        -param:hours="$HOURS" \
        >> "$LOG_FILE" 2>&1 &
fi

KITCHEN_PID=$!
wait $KITCHEN_PID
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "ETL completed successfully"
else
    echo "ETL failed - check log file: $LOG_FILE"
    exit 1
fi
