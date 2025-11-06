#!/bin/bash

# Detect environment and set paths accordingly
if [ -d "/home/ubuntu/data-integration" ]; then
    # ETL Server (ubuntu user)
    cd /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/parcel_download_tool_etl
    KITCHEN_PATH="/home/ubuntu/data-integration/kitchen.sh"
else
    # Dev Machine (tianzi user)
    cd ~/s3-redshift-backup-tool/parcel_download_tool_etl
    KITCHEN_PATH="/home/tianzi/data-integration/kitchen.sh"
fi

nohup $KITCHEN_PATH \
    -file=./hourly_run_alert.kjb \
    -level=Basic \
    > /dev/null 2>&1 &

echo "ETL job started in background with PID $!"

