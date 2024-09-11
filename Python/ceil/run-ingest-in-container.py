import os
import shutil
from datetime import datetime

# Locale root paths
DATA_DIR = "/data/ceil"

RAW_DIR = os.path.join(DATA_DIR, "raw")
INJECT_DIR = os.path.join(DATA_DIR, "ceil_inject")
RAW_ARCHIVED_DIR = os.path.join(DATA_DIR, "archived", "raw_archived")
INGESTED_ARCHIVED_DIR = os.path.join(DATA_DIR, "archived", "ingested_archived")
LOG_FILE = os.path.join(DATA_DIR, "error_log.txt")

# Function to log errors
def log_error(error_msg):
    with open(LOG_FILE, "a") as log_file:
        log_file.write(f"{datetime.now()}: {error_msg}\n")

# Function to check command exit status and log errors
def check_status(command, error_msg):
    if command.returncode != 0:
        log_error(error_msg)
        exit(1)

# Create directories if they don't exist
for directory in [RAW_DIR, INJECT_DIR, RAW_ARCHIVED_DIR, INGESTED_ARCHIVED_DIR]:
    os.makedirs(directory, exist_ok=True)

# Run Python scripts within the container
os.system(f"python /data/ceil/ceil_time-adjustment.py -i {RAW_DIR} -o {DATA_DIR}/ceil/ceil_time-correct -p '*.nc'")
os.system(f"python /data/ceil/ceil_make_daily_files.py -i {DATA_DIR}/ceil/ceil_time-correct -o {INJECT_DIR} -p '*.nc'")

# Push to Crocus server
rsync_command = ["rsync", "-avz", f"{INJECT_DIR}/", f"{CROCUS_USERNAME}@{CROCUS_SERVER}:{CROCUS_TARGET_DIR}"]
rsync_process = subprocess.run(rsync_command)
check_status(rsync_process, "Rsync to Crocus server failed")

# Archive files for later
for file in os.listdir(RAW_DIR):
    shutil.move(os.path.join(RAW_DIR, file), RAW_ARCHIVED_DIR)
for file in os.listdir(INJECT_DIR):
    shutil.move(os.path.join(INJECT_DIR, file), INGESTED_ARCHIVED_DIR)

# Clean up temporary directories
shutil.rmtree(INJECT_DIR)