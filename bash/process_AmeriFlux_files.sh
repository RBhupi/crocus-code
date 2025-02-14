#!/bin/bash

# The script automates daily processing of SmartFlux flux data from a waggle-dev-node
# to the CELS GCE server in directory `/nfs/gce/projects/crocus/CMS-FLX-001-UIC`. 
# The log file is reset every time the script runs. We rsync the data and process till the day before. 
# The script processes AmeriFlux-style CSV files using `combin_results_fluxnet_csv.py`
# and ensures that files are processed correctly, even if timestamps vary.

ROOT_DIR="/Users/bhupendra/projects/crocus/data/flux_data/data"
LOG_FILE="$ROOT_DIR/ameriflux_processing.log"
FP_DIR="$ROOT_DIR/AmeriFlux"
FLUX_PROCESS_SCRIPT="/Users/bhupendra/projects/crocus/code/Python/licor/ameriflux_data_processing.py"
FP_PREFIX="US-CU1_HH"

> $LOG_FILE


TODAY=$(date +"%Y-%m-%d")
YESTERDAY=$(date -v-1d +"%Y-%m-%d")

mkdir -p $FP_DIR

START_DATE="2024-07-01"
END_DATE=$YESTERDAY
CURRENT_DATE=$START_DATE

while [[ "$CURRENT_DATE" < "$END_DATE" || "$CURRENT_DATE" == "$END_DATE" ]]; do
    START_DATETIME="${CURRENT_DATE}T00:00:00"
    END_DATETIME="${CURRENT_DATE}T23:59:59"

    YEAR_MONTH=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$START_DATETIME" +"%Y%m")
    DATE_ONLY=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$START_DATETIME" +"%Y%m%d")

    # Check if any file exists for the given date in the filename
    FLUX_CSV_FILE=$(ls $FP_DIR/${FP_PREFIX}_${DATE_ONLY}*.csv 2>/dev/null | head -n 1)

    # Process flux data if no file exists for the given date
    if [ -z "$FLUX_CSV_FILE" ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Processing flux data for $CURRENT_DATE..." | tee -a $LOG_FILE
        python $FLUX_PROCESS_SCRIPT --start $START_DATETIME --end $END_DATETIME --root_dir $ROOT_DIR --prefix $FP_PREFIX >> $LOG_FILE 2>&1

        # Check again after processing
        FLUX_CSV_FILE=$(ls $FP_DIR/${FP_PREFIX}_${DATE_ONLY}*.csv 2>/dev/null | head -n 1)

        if [ -z "$FLUX_CSV_FILE" ]; then
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] ERROR: Flux data processing failed for $CURRENT_DATE!" | tee -a $LOG_FILE
        else
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] Flux data processed and saved: $FLUX_CSV_FILE" | tee -a $LOG_FILE
        fi
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S")] Flux data already processed for $CURRENT_DATE, skipping..." | tee -a $LOG_FILE
    fi

    CURRENT_DATE=$(date -j -v+1d -f "%Y-%m-%d" "$CURRENT_DATE" +"%Y-%m-%d")
done

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Done! Check the log file for details: $LOG_FILE" | tee -a $LOG_FILE
