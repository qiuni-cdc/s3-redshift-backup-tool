#!/bin/bash

cd ~/s3-redshift-backup-tool

mkdir -p parcel_download_hourly_log

{
    source s3_backup_venv/bin/activate && \
    python parcel_download_and_sync.py "" "-0.5"
    PYTHON_EXIT_CODE=$?
    deactivate
    exit $PYTHON_EXIT_CODE
} >> parcel_download_hourly_log/run_$(date '+%Y%m%d_%H%M%S').log 2>&1