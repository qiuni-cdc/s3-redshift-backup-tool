#!/bin/bash

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    PROJECT_ROOT="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
else
    # Dev Machine (tianzi user)
    PROJECT_ROOT="$HOME/s3-redshift-backup-tool"
fi

SCRIPT_DIR="$PROJECT_ROOT/settle_orders_etl"

# Change to script directory
cd "$SCRIPT_DIR" || exit 1

# Run settle_orders_daily_run.sh in background
nohup bash settle_orders_daily_run.sh > /dev/null 2>&1 &

PID=$!
echo "Settle Orders ETL job started in background with PID $PID"
