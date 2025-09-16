#!/bin/bash

# Nohup wrapper for ETL parcel detail tool
# Usage: ./nohup_download_tool.sh [beg_datetime] [hours]
# Example: ./nohup_download_tool.sh "2024-08-14 10:00:00" "-2"

if [ -d "/home/ubuntu/data-integration" ]; then
    cd /home/ubuntu/etl/etl_dw/becky/download_tool_etl 
else
    cd ~/s3-redshift-backup-tool/parcel_download_tool_etl
fi 

nohup ./download_tool.sh "$1" "$2" &
echo "ETL process started in background (PID: $!)"