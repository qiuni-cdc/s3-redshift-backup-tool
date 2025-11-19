#!/bin/bash

# Nohup wrapper for parcel download hourly loop
# Usage: ./nohup_parcel_download_run.sh [end_datetime] [total_hours]
# Example: ./nohup_parcel_download_run.sh "2025-02-05 23:59:59" "-24"
#
# This script runs parcel_download_run.sh in the background using nohup,
# allowing it to continue running even after SSH disconnection.
#
# Logs are handled by parcel_download_run.sh itself.
#
# To find the process:
#   ps aux | grep parcel_download_run.sh
#
# To stop the process:
#   kill <PID>

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    PROJECT_ROOT="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
else
    # Dev Machine (tianzi user)
    PROJECT_ROOT="$HOME/s3-redshift-backup-tool"
fi

cd "$PROJECT_ROOT/parcel_download_tool_etl" || exit 1

echo "=========================================="
echo "Starting parcel download in background"
echo "End date: ${1:-'not specified'}"
echo "Total hours: ${2:--1}"
echo "=========================================="

# Run parcel_download_run.sh in background with nohup
# Output goes to nohup.out in current directory
nohup bash parcel_download_run.sh "$1" "$2" &

PID=$!

echo "✅ Process started in background"
echo "   PID: $PID"
echo ""
echo "To check if running:"
echo "   ps aux | grep parcel_download_run.sh"
echo ""
echo "To stop the process:"
echo "   kill $PID"
echo ""
echo "To view nohup output:"
echo "   tail -f nohup.out"
echo "=========================================="

# Save PID to file for easy reference
echo $PID > "$PROJECT_ROOT/parcel_download_tool_etl/parcel_download_run.pid"
echo "PID saved to: parcel_download_run.pid"
