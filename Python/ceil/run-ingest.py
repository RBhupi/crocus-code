import subprocess
import os
import shutil
from datetime import datetime

# Locale root paths
DATA_DIR = "./ceil"

RAW_DIR = os.path.join(DATA_DIR, "raw")
INJECT_DIR = os.path.join(DATA_DIR, "ceil_inject")
RAW_ARCHIVED_DIR = os.path.join(DATA_DIR, "archived", "raw_archived")
INGESTED_ARCHIVED_DIR = os.path.join(DATA_DIR, "archived", "ingested_archived")
LOG_FILE = os.path.join(DATA_DIR, "error_log.txt")

# Waggle node
WAGGLE_SERVER = "waggle-dev-node-W08D"
WAGGLE_DATA_DIR = "/home/waggle/cl61"

# Crocus server
CROCUS_SERVER = "homes.cels.anl.gov"
CROCUS_TARGET_DIR = "/nfs/gce/projects/crocus/ceil_ingested"
CROCUS_USERNAME = "braut"

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

# Rsync from the node
rsync_command = ["rsync", "-avz", f"{WAGGLE_SERVER}:{WAGGLE_DATA_DIR}/", RAW_DIR]
rsync_process = subprocess.run(rsync_command)
check_status(rsync_process, "Rsync from Waggle node failed")

# Pull Docker image if it does not exist
docker_image_check_command = "docker images rbhupi/ubuntu-ceil | grep -q rbhupi/ubuntu-ceil"
docker_image_check_process = subprocess.run(docker_image_check_command, shell=True)
if docker_image_check_process.returncode != 0:
    docker_pull_command = ["docker", "pull", "rbhupi/ubuntu-ceil"]
    docker_pull_process = subprocess.run(docker_pull_command)
    check_status(docker_pull_process, "Docker pull failed")

# Run Docker
docker_run_command = [
    "docker", "run",
    "-v", f"{os.path.abspath(DATA_DIR)}:/data",
    "rbhupi/ubuntu-ceil",
    "sh", "-c",
    f"python ceil_time-adjustment.py -i /data/ceil/raw -o /data/ceil/ceil_time-correct -p '*.nc' && \
    python ceil_make_daily_files.py -i /data/ceil/ceil_time-correct -o /data/ceil/ceil_inject -p '*.nc'"
]
docker_run_process = subprocess.run(docker_run_command)
check_status(docker_run_process, "Docker run failed")

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
