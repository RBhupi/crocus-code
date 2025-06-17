#!/bin/bash

# ---------------------- Configuration ----------------------
DATA_CONFIG="/home/braut/crocus-code/beehive-file-ingest/beehive_data_curation_job.yaml"
CL_PROC_CONFIG="/home/braut/crocus-code/beehive-file-ingest/ceil_ingest_jobs.yaml"
LOG_DIR="/home/braut/crocus-code/beehive-file-ingest/logs"

DOWNLOADER_SCRIPT="/home/braut/crocus-code/beehive-file-ingest/crocus_file_curator.py"
CORRECTOR_SCRIPT="/home/braut/crocus-code/beehive-file-ingest/ceil_time_corrector.py"
DAILY_MAKER_SCRIPT="/home/braut/crocus-code/beehive-file-ingest/ceil_make_daily.py"

LATENCY=12
DRY_RUN=false  # Set to true for testing without file writing

ROOT_DIR="/nfs/gce/projects/crocus/data/"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MAIN_LOG="$LOG_DIR/run_ceil_pipeline_${TIMESTAMP}.log"

# ---------------------- Activate Environment ----------------------
source ~/.bashrc
micromamba activate data

# ---------------------- Logging Header ----------------------
{
  echo "------------------------------------------------------------"
  echo "STARTING CEILOMETER INGEST PIPELINE AT $TIMESTAMP"
  echo "Downloader config: $DATA_CONFIG"
  echo "Processor config: $CL_PROC_CONFIG"
  echo "------------------------------------------------------------"
} >> "$MAIN_LOG"

# ---------------------- STEP 1: Download Data ----------------------
echo ">>> STEP 1: Downloading new data..." >> "$MAIN_LOG"
DL_CMD=(python "$DOWNLOADER_SCRIPT" --config "$DATA_CONFIG" --root-dir "$ROOT_DIR")
if [ "$DRY_RUN" = true ]; then
  DL_CMD+=("--dry-run")
fi
"${DL_CMD[@]}" >> "$MAIN_LOG" 2>&1
if [ $? -ne 0 ]; then
  echo "ERROR: Download step failed." >> "$MAIN_LOG"
  exit 1
fi
echo ">>> Data download completed." >> "$MAIN_LOG"

# ---------------------- STEP 2: Time Correction ----------------------
echo ">>> STEP 2: Running time correction..." >> "$MAIN_LOG"
CORRECTOR_CMD=(python "$CORRECTOR_SCRIPT" --config "$CL_PROC_CONFIG" --latency "$LATENCY")
if [ "$DRY_RUN" = true ]; then
  CORRECTOR_CMD+=("--dry-run")
fi
"${CORRECTOR_CMD[@]}" >> "$MAIN_LOG" 2>&1
if [ $? -ne 0 ]; then
  echo "ERROR: Time correction step failed." >> "$MAIN_LOG"
  exit 1
fi
echo ">>> Time correction completed." >> "$MAIN_LOG"

# ---------------------- STEP 3: Create Daily Files ----------------------
echo ">>> STEP 3: Creating daily files..." >> "$MAIN_LOG"
DAILY_CMD=(python "$DAILY_MAKER_SCRIPT" --config "$CL_PROC_CONFIG")
"${DAILY_CMD[@]}" >> "$MAIN_LOG" 2>&1
if [ $? -ne 0 ]; then
  echo "ERROR: Daily file creation step failed." >> "$MAIN_LOG"
  exit 1
fi
echo ">>> Daily file creation completed." >> "$MAIN_LOG"

# ---------------------- Completion ----------------------
echo "Pipeline completed at $(date +"%Y-%m-%d %H:%M:%S")" >> "$MAIN_LOG"
