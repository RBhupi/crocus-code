#!/bin/bash

# Root directory. this is rsync from smartflux
ROOT_DIR="/Users/bhupendra/projects/crocus/data/flux_data/data"

# Calculate the previous month in YYYY/MM format
YEAR_MONTH=$(date -v-1m +"%Y/%m")

# Define the directory where NetCDF files are stored
NC_DIR="$ROOT_DIR/netcdf"

# Ensure the NetCDF directory exists
mkdir -p $NC_DIR

# Define the NetCDF output file paths for raw data and results
RAW_DATA_FILE="$NC_DIR/smartflux_rawdata_${YEAR_MONTH//\//_}.nc"
RESULTS_FILE="$NC_DIR/smartflux_data_${YEAR_MONTH//\//_}.nc"

# Define the log file path using the ROOT_DIR and previous month
LOG_FILE="$ROOT_DIR/flux_data_processing_$(date -v-1m +\%Y_\%m).log"

# Sync the data from the Waggle node to your local machine (always sync)
echo "Syncing data from Waggle node..." | tee -a $LOG_FILE
rsync -azP -e "ssh -o StrictHostKeyChecking=no" waggle@waggle-dev-node-w096:/home/waggle/bhupendra/flux_data/data/ $ROOT_DIR >> $LOG_FILE 2>&1

# Check if the raw data file for the previous month does not exist, then process
if [ ! -f "$RAW_DATA_FILE" ]; then
    # Run the raw data processing script for the previous month
    echo "Processing raw data for $YEAR_MONTH..." | tee -a $LOG_FILE
    python /Users/bhupendra/projects/crocus/code/Python/licor/read_raw_toNC.py --year_month $YEAR_MONTH --root_dir $ROOT_DIR >> $LOG_FILE 2>&1
fi

# Check if the flux computation results file for the previous month does not exist, then process
if [ ! -f "$RESULTS_FILE" ]; then
    # Run the flux computation results script for the previous month
    echo "Processing flux computation results for $YEAR_MONTH..." | tee -a $LOG_FILE
    python /Users/bhupendra/projects/crocus/code/Python/licor/read_results_toNC.py --year_month $YEAR_MONTH --root_dir $ROOT_DIR >> $LOG_FILE 2>&1
fi

# Sync the processed NetCDF files to the GCE
echo "Syncing processed NetCDF fils..." | tee -a $LOG_FILE
rsync -azP -e "ssh -o StrictHostKeyChecking=no" $ROOT_DIR/* braut@homes.cels.anl.gov:/nfs/gce/projects/crocus-server-admins/licor-flux-data-beta/ >> $LOG_FILE 2>&1

# Log completion
echo "Processing complete for $YEAR_MONTH!" | tee -a $LOG_FILE

