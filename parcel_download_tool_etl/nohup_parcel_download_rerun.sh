#!/bin/bash

# Nohup wrapper for parcel download rerun
# Usage: ./nohup_parcel_download_run.sh <start_time> <end_time> [pipeline]
# Example: ./nohup_parcel_download_run.sh "2025-11-04 22:00:00" "2025-11-05 19:00:00"
# Example: ./nohup_parcel_download_run.sh "2025-11-04 22:00:00" "2025-11-05 19:00:00" "us_dw_unidw_2_settlement_dws_pipeline"
#
# This script runs parcel_download_rerun.sh in the background using nohup,
# allowing it to continue running even after SSH disconnection.
#
# To find the process:
#   ps aux | grep parcel_download_rerun.sh
#
# To stop the process:
#   kill <PID>

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./nohup_parcel_download_run.sh <start_time> <end_time> [pipeline]"
    echo "Example: ./nohup_parcel_download_run.sh \"2025-11-04 22:00:00\" \"2025-11-05 19:00:00\""
    echo "Example: ./nohup_parcel_download_run.sh \"2025-11-04 22:00:00\" \"2025-11-05 19:00:00\" \"us_dw_unidw_2_settlement_dws_pipeline\""
    exit 1
fi

START_TIME=$1
END_TIME=$2
PIPELINE=${3:-"us_dw_unidw_2_settlement_dws_pipeline_direct"}

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
echo "Starting parcel download rerun in background"
echo "From: $START_TIME"
echo "To:   $END_TIME"
echo "Pipeline: $PIPELINE"
echo "=========================================="

# Run parcel_download_rerun.sh in background with nohup
nohup bash parcel_download_rerun.sh "$START_TIME" "$END_TIME" "$PIPELINE" > nohup.out 2>&1 &

PID=$!

echo "✅ Process started in background"
echo "   PID: $PID"
echo ""
echo "To check if running:"
echo "   ps aux | grep parcel_download_rerun.sh"
echo ""
echo "To stop the process:"
echo "   kill $PID"
echo ""
echo "To view output:"
echo "   tail -f nohup.out"
echo "=========================================="

# Save PID to file for easy reference
echo $PID > "$PROJECT_ROOT/parcel_download_tool_etl/parcel_download_rerun.pid"
echo "PID saved to: parcel_download_rerun.pid"
