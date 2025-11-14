#!/bin/bash

DATE=${1:-""}
HOURS=${2:-"-1"}

echo "Running parcel download with date: $DATE, hours: $HOURS"

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    PROJECT_ROOT="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
else
    # Dev Machine (tianzi user)
    PROJECT_ROOT="$HOME/s3-redshift-backup-tool"
fi

cd "$PROJECT_ROOT/parcel_download_tool_etl" || exit 1

mkdir -p "$PROJECT_ROOT/parcel_download_tool_etl/parcel_download_hourly_log" && \
source "$PROJECT_ROOT/s3_backup_venv/bin/activate" && \
python parcel_download_and_sync.py -d "$DATE" --hours "$HOURS" --pipeline "us_dw_unidw_2_settlement_dws_pipeline_direct" \
    >> "$PROJECT_ROOT/parcel_download_tool_etl/parcel_download_hourly_log/run_$(date '+%Y%m%d_%H%M%S').log" 2>&1

if [ $? -eq 0 ]; then
    echo "Parcel download completed successfully"
    deactivate
else
    echo "Parcel download failed - check log file"
    deactivate
    exit 1
fi