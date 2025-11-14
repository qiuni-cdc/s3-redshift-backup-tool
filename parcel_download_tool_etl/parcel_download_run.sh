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

LOG_DIR="$PROJECT_ROOT/parcel_download_tool_etl/parcel_download_hourly_log"
LOG_FILE="$LOG_DIR/run_$(date '+%Y%m%d_%H%M%S').log"

mkdir -p "$LOG_DIR"

echo "Log file: $LOG_FILE"
echo "Starting at: $(date '+%Y-%m-%d %H:%M:%S')"

# Activate virtualenv
source "$PROJECT_ROOT/s3_backup_venv/bin/activate" || {
    echo "Failed to activate virtualenv"
    exit 1
}

# Run Python script and capture exit code
# Using tee to output to both terminal and log file
python parcel_download_and_sync.py -d "$DATE" --hours "$HOURS" --pipeline "us_dw_unidw_2_settlement_dws_pipeline_direct" 2>&1 | tee "$LOG_FILE"
PYTHON_EXIT_CODE=${PIPESTATUS[0]}

# Deactivate virtualenv
deactivate

echo "Finished at: $(date '+%Y-%m-%d %H:%M:%S')"

if [ $PYTHON_EXIT_CODE -eq 0 ]; then
    echo "✅ Parcel download completed successfully"
    exit 0
else
    echo "❌ Parcel download failed with exit code: $PYTHON_EXIT_CODE"
    echo "Check log file: $LOG_FILE"
    exit 1
fi