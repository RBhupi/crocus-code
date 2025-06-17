import os
import re
import glob
import shutil
import argparse
import logging
import yaml
import datetime
from netCDF4 import Dataset, num2date

def load_yaml_config(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)

def extract_timestamp_from_filename(filename):
    match = re.match(r"(\d{19})-", filename)
    if not match:
        return None
    try:
        ns_since_epoch = int(match.group(1))
        dt = datetime.datetime.utcfromtimestamp(ns_since_epoch / 1e9)
        return dt
    except Exception as e:
        logging.error(f"Could not parse timestamp from {filename}: {e}")
        return None

def adjust_time_variable(time_var, mod_time, midnight):
    try:
        times = num2date(time_var[:], units=time_var.units)
        delta_seconds = [(t - times[0]).total_seconds() for t in times]
        mod_time = mod_time.replace(tzinfo=None)
        total_interval = (times[-1] - times[0]).total_seconds()
        seconds_since_midnight = (mod_time - midnight).total_seconds() - total_interval
        adjusted_times = [seconds_since_midnight + delta for delta in delta_seconds]
        time_var[:] = adjusted_times
        time_var.units = f'seconds since {midnight.strftime("%Y-%m-%d 00:00:00")}'
    except Exception as e:
        logging.error(f"Error adjusting time variable: {e}")

def adjust_time_axis(nc_file, mod_time):
    midnight = datetime.datetime(mod_time.year, mod_time.month, mod_time.day)
    if 'time' in nc_file.variables:
        adjust_time_variable(nc_file.variables['time'], mod_time, midnight)
    for group_name in ['monitoring', 'status']:
        if group_name in nc_file.groups:
            group = nc_file.groups[group_name]
            if 'time' in group.variables:
                adjust_time_variable(group.variables['time'], mod_time, midnight)

def process_file(file_path, base_output_dir, latency, processed_log, dry_run):
    filename = os.path.basename(file_path)
    if filename in processed_log:
        logging.info(f"Skipping previously corrected file (in log): {filename}")
        return

    if "_1970" not in filename:
        return

    mod_time = extract_timestamp_from_filename(filename)
    if not mod_time:
        return

    mod_time -= datetime.timedelta(seconds=latency)
    new_datetime_str = mod_time.strftime("%Y%m%d_%H%M%S")
    new_filename = re.sub(r"\d{8}_\d{6}", new_datetime_str, filename)
    out_dir = os.path.join(base_output_dir, mod_time.strftime("%Y/%m/%d"))
    new_file_path = os.path.join(out_dir, new_filename)

    if os.path.exists(new_file_path):
        logging.info(f"Skipping existing corrected file: {new_file_path}")
        processed_log.add(filename)
        return

    if dry_run:
        logging.info(f"[DRY RUN] Would correct: {filename} → {new_filename}")
        logging.info(f"[DRY RUN] Would copy to: {new_file_path}")
        processed_log.add(filename)
        return

    try:
        os.makedirs(out_dir, exist_ok=True)
        shutil.copy(file_path, new_file_path)
        with Dataset(new_file_path, 'r+') as nc:
            adjust_time_axis(nc, mod_time)
        logging.info(f"Corrected: {filename} → {new_filename}")
        processed_log.add(filename)
    except Exception as e:
        logging.error(f"Failed processing {file_path}: {e}")


def recursive_nc_files(root_dir):
    return glob.glob(os.path.join(root_dir, "**", "*.nc"), recursive=True)

def load_processed_log(log_file_path):
    if os.path.exists(log_file_path):
        with open(log_file_path, "r") as f:
            return set(line.strip() for line in f)
    return set()

def save_processed_log(log_file_path, processed_log):
    with open(log_file_path, "w") as f:
        for entry in sorted(processed_log):
            f.write(f"{entry}\n")

def main():
    parser = argparse.ArgumentParser(description="Correct CEIL files with bad 1970 timestamps.")
    parser.add_argument("--config", required=True, help="Path to YAML config.")
    parser.add_argument("--latency", type=int, default=0, help="Latency offset in seconds.")
    parser.add_argument("--log", default="corrected_files.log", help="Log file for processed files.")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing files (preview only).")
    args = parser.parse_args()

    config = load_yaml_config(args.config)

    for job in config.get("jobs", []):
        subfolder = job["subfolder"]
        job_name = job.get("job", "unnamed")
        log_name = f"log_correction_{job_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        job_log_path = os.path.join(subfolder, log_name)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(job_log_path),
                logging.StreamHandler()
            ]
        )

        logging.info(f"Started job: {job_name} in {subfolder}")
        all_files = recursive_nc_files(subfolder)
        processed_log = load_processed_log(args.log)

        for file_path in all_files:
            process_file(file_path, subfolder, args.latency, processed_log, args.dry_run)

        if not args.dry_run:
            save_processed_log(args.log, processed_log)
        else:
            logging.info("[DRY RUN] Skipped updating processed file log.")

if __name__ == "__main__":
    main()
