#!/bin/bash

# The script automates the daily processing of SmartFlux data from a waggle-dev-node
# to the CELS GCE server in directory `/nfs/gce/projects/crocus/CMS-FLX-001-UIC`. 
# The log file is reset everytime the script runs, we rsync the data and process till the day before, 
# Also checks if NetCDF files for raw and results data already exist, if not, it will try to process using
# `read_raw_toNC.py` and `read_results_toNC.py`. Output NetCDF files are saved in CL61 style for ingest. 

ROOT_DIR="/Users/bhupendra/projects/crocus/data/flux_data/data"
LOG_FILE="$ROOT_DIR/daily_flux_data_processing.log"
NC_DIR="$ROOT_DIR/netcdf"
SYNC_SOURCE="waggle@waggle-dev-node-w096:/home/waggle/bhupendra/flux_data/data/"
RAW_PROCESS_SCRIPT="/Users/bhupendra/projects/crocus/code/Python/licor/read_raw_toNC.py"
RESULTS_PROCESS_SCRIPT="/Users/bhupendra/projects/crocus/code/Python/licor/read_results_toNC.py"
PLOT_SCRIPT="/Users/bhupendra/projects/crocus/code/Python/licor/make_quicklooks.py"
GCE_DESTINATION="braut@homes.cels.anl.gov:/nfs/gce/projects/crocus/CMS-FLX-001-UIC/"
FILE_PREFIX="crocus-uic-smartflux"


> $LOG_FILE


# Start SSH agent and add the key
#eval $(ssh-agent -s)
#ssh-add ~/.ssh/id_rsa # ~/.ssh/id_rsa_no_passphrase if did not work


TODAY=$(date +"%Y-%m-%d")
YESTERDAY=$(date -v-1d +"%Y-%m-%d")

mkdir -p $NC_DIR

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Syncing data from SmartFlux node..." | tee -a $LOG_FILE
rsync -azP -e "ssh -o StrictHostKeyChecking=no" $SYNC_SOURCE $ROOT_DIR >> $LOG_FILE 2>&1

START_DATE="2024-07-01"
END_DATE=$YESTERDAY
CURRENT_DATE=$START_DATE

while [[ "$CURRENT_DATE" < "$END_DATE" || "$CURRENT_DATE" == "$END_DATE" ]]; do
    START_DATETIME="${CURRENT_DATE}T00:00:00"
    END_DATETIME="${CURRENT_DATE}T23:59:59"

    YEAR_MONTH=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$START_DATETIME" +"%Y%m")

    RAW_NC_FILE="$NC_DIR/rawnc/$YEAR_MONTH/$FILE_PREFIX-raw-$(date -j -f "%Y-%m-%dT%H:%M:%S" "$END_DATETIME" +"%Y%m%dT%H%M%S").nc"
    RESULTS_NC_FILE="$NC_DIR/resnc/$YEAR_MONTH/$FILE_PREFIX-results-$(date -j -f "%Y-%m-%dT%H:%M:%S" "$END_DATETIME" +"%Y%m%dT%H%M%S").nc"

    if [ ! -f "$RAW_NC_FILE" ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing raw data for $CURRENT_DATE..." | tee -a $LOG_FILE
        python $RAW_PROCESS_SCRIPT --start $START_DATETIME --end $END_DATETIME --root_dir $ROOT_DIR >> $LOG_FILE 2>&1
        
        if [ ! -f "$RAW_NC_FILE" ]; then
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] ERROR: Raw data processing failed for $CURRENT_DATE!" | tee -a $LOG_FILE
        fi
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Raw data already processed for $CURRENT_DATE, skipping..." | tee -a $LOG_FILE
    fi

    if [ ! -f "$RESULTS_NC_FILE" ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing results data for $CURRENT_DATE..." | tee -a $LOG_FILE
        python $RESULTS_PROCESS_SCRIPT --start $START_DATETIME --end $END_DATETIME --root_dir $ROOT_DIR >> $LOG_FILE 2>&1

        if [ ! -f "$RESULTS_NC_FILE" ]; then
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] ERROR: Results data processing failed for $CURRENT_DATE!" | tee -a $LOG_FILE
        fi
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Results data already processed for $CURRENT_DATE, skipping..." | tee -a $LOG_FILE
    fi

    # Generate Quick looks
    if [ -f "$RESULTS_NC_FILE" ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Generating quick-lookd for $RESULTS_NC_FILE..." | tee -a $LOG_FILE
        python $PLOT_SCRIPT --nc_file "$RESULTS_NC_FILE" --output_dir "$NC_DIR/resnc/$YEAR_MONTH" --prefix "quicklook" | tee -a $LOG_FILE 2>&1
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Quick-look plots generated and saved." | tee -a $LOG_FILE
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] ERROR: Results NetCDF file not found for $CURRENT_DATE!" | tee -a $LOG_FILE
    fi

    CURRENT_DATE=$(date -j -v+1d -f "%Y-%m-%d" "$CURRENT_DATE" +"%Y-%m-%d")
done

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Syncing processed NetCDF files to GCE..." | tee -a $LOG_FILE
rsync -azP -e "ssh -o StrictHostKeyChecking=no" $NC_DIR/* $GCE_DESTINATION >> $LOG_FILE 2>&1

# eval $(ssh-agent -k)

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing complete!" | tee -a $LOG_FILE


