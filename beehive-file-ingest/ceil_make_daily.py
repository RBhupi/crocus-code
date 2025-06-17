import os
import glob
import yaml
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
import argparse
import logging

def load_yaml_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def make_daily_file(input_dir, output_dir, prefix, date_str, attributes):
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    day_dir = os.path.join(input_dir, year, month, day)

    output_path = os.path.join(output_dir, f"{prefix}{date_str}-000000.nc")
    if os.path.exists(output_path):
        logging.info(f"Daily file already exists, skipping: {output_path}")
        return

    if not os.path.isdir(day_dir):
        logging.warning(f"No directory found for {date_str} → {day_dir}")
        return

    selected_files = sorted(glob.glob(os.path.join(day_dir, "*.nc")))
    if not selected_files:
        logging.warning(f"No files found in {day_dir}")
        return

    try:
        daily_ds = xr.open_mfdataset(selected_files, concat_dim="time", combine="nested")
        daily_ds = daily_ds.sortby("time")

        start_of_day = pd.to_datetime(date_str).floor("D")
        end_of_day = start_of_day + timedelta(days=1)
        daily_ds = daily_ds.sel(time=slice(start_of_day, end_of_day - pd.to_timedelta("1ms")))

        # Inject current file creation date
        attributes["file_creation_date"] = datetime.now().strftime("%Y-%m-%d")

        for key, val in attributes.items():
            daily_ds.attrs[key] = val

        os.makedirs(output_dir, exist_ok=True)
        daily_ds.to_netcdf(output_path)
        logging.info(f"Created daily file: {output_path}")
        daily_ds.close()
    except Exception as e:
        logging.error(f"Failed to process {date_str} in {day_dir}: {e}")


def run_jobs_from_yaml(yaml_path):
    config = load_yaml_config(yaml_path)
    for job in config.get("jobs", []):
        if not all(k in job for k in ("start_date", "subfolder", "output_dir", "output_prefix")):
            logging.warning(f"Skipping job (missing keys): {job}")
            continue

        input_dir = job["subfolder"]
        output_dir = job["output_dir"]
        prefix = job["output_prefix"]
        current_date = datetime.strptime(job["start_date"], "%Y-%m-%d")

        # ⬇️ Handle optional end_date (default to yesterday)
        if "end_date" in job:
            end_date = datetime.strptime(job["end_date"], "%Y-%m-%d")
        else:
            end_date = datetime.today() - timedelta(days=1)

        # Setup logging
        log_file = os.path.join(output_dir, f"log_make_daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        os.makedirs(output_dir, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )

        logging.info(f"Running job: {job.get('job', 'unnamed')} → {current_date.date()} to {end_date.date()}")

        while current_date <= end_date:
            try:
                date_str = current_date.strftime("%Y%m%d")
                make_daily_file(input_dir, output_dir, prefix, date_str, job.get("attributes", {}))
            except Exception as e:
                logging.error(f"Error on {date_str}: {e}")
            current_date += timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate daily netCDF files from time-corrected CEIL data.")
    parser.add_argument("--config", required=True, help="YAML config file with job definitions.")
    args = parser.parse_args()
    run_jobs_from_yaml(args.config)
