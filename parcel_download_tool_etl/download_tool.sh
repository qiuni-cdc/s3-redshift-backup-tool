#!/bin/bash

# ETL parcel detail tool
# Usage: ./download_tool.sh <beg_datetime> <hours>
# Example: ./download_tool.sh "2024-08-14 10:00:00" "-2"

if [ -d "/home/ubuntu/data-integration" ]; then
    cd /home/ubuntu/etl/etl_dw/becky/download_tool_etl  
    KITCHEN_PATH="/home/ubuntu/data-integration/kitchen.sh"
else
    cd ~/s3-redshift-backup-tool/parcel_download_tool_etl 
    KITCHEN_PATH="/home/tianzi/data-integration/kitchen.sh"
fi 

BEG_DATETIME=$1
HOURS=$2

echo "Running ETL with datetime: $BEG_DATETIME, hours: $HOURS"



mkdir -p ~/s3-redshift-backup-tool/parcel_download_tool_etl/logs && \
$KITCHEN_PATH \
    -file=./download_tool_job.kjb \
    -level=Basic \
    -param:beg_datetime="$BEG_DATETIME" \
    -param:hours="$HOURS" \
    >> ~/s3-redshift-backup-tool/parcel_download_tool_etl/logs/download_tool_$(date "+%Y%m%d-%H%M%S").log 2>&1

if [ $? -eq 0 ]; then
    echo "ETL completed successfully"
else
    echo "ETL failed - check log file"
    exit 1
fi

