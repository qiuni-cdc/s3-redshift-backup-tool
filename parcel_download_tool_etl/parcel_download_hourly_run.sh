#!/bin/bash

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    PROJECT_ROOT="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
else
    # Dev Machine (tianzi user)
    PROJECT_ROOT="$HOME/s3-redshift-backup-tool"
fi

SCRIPT_DIR="$PROJECT_ROOT/parcel_download_tool_etl"
VENV_PATH="$PROJECT_ROOT/s3_backup_venv"
LOG_DIR="$SCRIPT_DIR/parcel_download_hourly_log"

# Change to script directory
cd "$SCRIPT_DIR" || exit 1

# Create log directory
mkdir -p "$LOG_DIR"

{
    source "$VENV_PATH/bin/activate" && \
    python parcel_download_and_sync.py -d "" --hours "-1" --pipeline "us_dw_unidw_2_settlement_dws_pipeline_direct"
    PYTHON_EXIT_CODE=$?
    deactivate
    exit $PYTHON_EXIT_CODE
} >> "$LOG_DIR/run_$(date '+%Y%m%d_%H%M%S').log" 2>&1