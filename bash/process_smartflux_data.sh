#!/bin/bash

ROOT_DIR="/Users/bhupendra/projects/crocus/data/flux_data/data"

LOG_FILE="$ROOT_DIR/daily_flux_data_processing.log"

# log file is reset at the start of each day
> $LOG_FILE

# date in the correct format
TODAY=$(date +"%Y-%m-%d")
YESTERDAY=$(date -v-1d +"%Y-%m-%d")

# NetCDF directory for processed files
NC_DIR="$ROOT_DIR/netcdf"
mkdir -p $NC_DIR

# Sync the raw data from the SmartFlux node (always sync)
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Syncing data from SmartFlux node..." | tee -a $LOG_FILE
rsync -azP -e "ssh -o StrictHostKeyChecking=no" waggle@waggle-dev-node-w096:/home/waggle/bhupendra/flux_data/data/ $ROOT_DIR >> $LOG_FILE 2>&1

# the date range to process aily filesd
START_DATE="2024-07-01"
END_DATE=$YESTERDAY

CURRENT_DATE=$START_DATE

while [[ "$CURRENT_DATE" < "$END_DATE" || "$CURRENT_DATE" == "$END_DATE" ]]; do
    START_DATETIME="${CURRENT_DATE}T00:00:00"
    END_DATETIME="${CURRENT_DATE}T23:59:59"

    # Check if the raw and results NetCDF files exist
    RAW_NC_FILE="$NC_DIR/raw_smartflux_$(date -j -f "%Y-%m-%dT%H:%M:%S" "$START_DATETIME" +"%Y%m%dT%H%M%S")_to_$(date -j -f "%Y-%m-%dT%H:%M:%S" "$END_DATETIME" +"%Y%m%dT%H%M%S").nc"
    RESULTS_NC_FILE="$NC_DIR/results_smartflux_$(date -j -f "%Y-%m-%dT%H:%M:%S" "$START_DATETIME" +"%Y%m%dT%H%M%S")_to_$(date -j -f "%Y-%m-%dT%H:%M:%S" "$END_DATETIME" +"%Y%m%dT%H%M%S").nc"

    # Only process the NetCDF files that do not exist
    if [ ! -f "$RAW_NC_FILE" ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing raw data for $CURRENT_DATE..." | tee -a $LOG_FILE
        python /Users/bhupendra/projects/crocus/code/Python/licor/read_raw_toNC.py --start $START_DATETIME --end $END_DATETIME --root_dir $ROOT_DIR >> $LOG_FILE 2>&1
        
        # Check processing was successful
        if [ ! -f "$RAW_NC_FILE" ]; then
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] ERROR: Raw data processing failed for $CURRENT_DATE!" | tee -a $LOG_FILE
        fi
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Raw data already processed for $CURRENT_DATE, skipping..." | tee -a $LOG_FILE
    fi

    # Only process if the NetCDF not exist
    if [ ! -f "$RESULTS_NC_FILE" ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing results data for $CURRENT_DATE..." | tee -a $LOG_FILE
        python /Users/bhupendra/projects/crocus/code/Python/licor/read_results_toNC.py --start $START_DATETIME --end $END_DATETIME --root_dir $ROOT_DIR >> $LOG_FILE 2>&1

        # Check data processing was successful
        if [ ! -f "$RESULTS_NC_FILE" ]; then
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] ERROR: Results data processing failed for $CURRENT_DATE!" | tee -a $LOG_FILE
        fi
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Results data already processed for $CURRENT_DATE, skipping..." | tee -a $LOG_FILE
    fi

    # the next day
    CURRENT_DATE=$(date -j -v+1d -f "%Y-%m-%d" "$CURRENT_DATE" +"%Y-%m-%d")
done

# Sync the processed NetCDF files to the GCE
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Syncing processed NetCDF files to GCE..." | tee -a $LOG_FILE
rsync -azP -e "ssh -o StrictHostKeyChecking=no" $NC_DIR/*.nc braut@homes.cels.anl.gov:/nfs/gce/projects/crocus-server-admins/licor-flux-data-beta/ >> $LOG_FILE 2>&1
#finish
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing complete!" | tee -a $LOG_FILE
