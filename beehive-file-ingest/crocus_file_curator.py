import os
import yaml
import logging
import argparse
import datetime
import pandas as pd
import sage_data_client
from typing import Optional, Callable
from beehive_file_fetcher import download_beehive_files
import re
from datetime import timezone


# ========== Logging Setup ==========
def setup_logging(log_path: str, debug: bool):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_path)
    ]

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers
    )

    logging.info(f"[LOGGING] Log file created at: {log_path}")


# ========== Utility Functions ==========
def load_yaml_file(path):
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"[ERROR] Failed to load YAML file: {e}")
        return None


def date_range_loop(start: str, end: str) -> list:
    start_dt = datetime.datetime.fromisoformat(start.replace("Z", ""))
    end_dt = datetime.datetime.fromisoformat(end.replace("Z", ""))
    ranges = []
    for day in pd.date_range(start=start_dt, end=end_dt):
        s = day.strftime("%Y-%m-%dT00:00:00Z")
        e = day.strftime("%Y-%m-%dT23:59:59Z")
        ranges.append((s, e))
    return ranges


def _extract_date_from_filename(filename: str, date_regex: str, date_format: str) -> Optional[datetime]:
    """
    Extracts a date from the filename using regex and the provided datetime format.

    Parameters:
        filename (str): The filename to extract the date from.
        date_regex (str): The regex pattern to locate the date string.
        date_format (str): The datetime format string (e.g., '%Y-%m-%dT%H%M%S').

    Returns:
        datetime: The extracted date as a datetime object, or None if parsing fails.
    """
    try:
        # Search for the date string in the filename using the provided regex
        match = re.search(date_regex, filename)
        if not match:
            logging.warning(f"[WARN] No date found in filename: {filename}")
            return None
        
        date_str = match.group(0)
        
        # Parse the date string using the provided format
        return datetime.datetime.strptime(date_str, date_format)
    except (ValueError, IndexError) as e:
        logging.error(f"[ERROR] Failed to parse date from filename '{filename}': {e}")
        return None


def _create_dated_folder(base_path: str, date: datetime) -> str:
    """
    Constructs a nested directory path based on a datetime object.

    Example: If base_path='/data' and date is 2024-07-01,
    returns '/data/2024/07/01'
    """
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    return os.path.join(base_path, year, month, day)


def parse_job_settings(job):
    end_date = job.get("end_date") or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

    extension = job.get("extension", [])
    if isinstance(extension, str):
        extension = [extension]
    if not extension:
        extension = None

    rename_function = None
    if not job.get("waggle_filename_timestamp", True):
        rename_function = lambda filename: re.sub(r"^\d+-", "", filename)

    return {
        "job_name": job["job"],
        "upload_name": job["upload_name"],
        "vsn": job["vsn"],
        "start_date": job["start_date"],
        "end_date": end_date,
        "date_regex": job.get("date_regex"),
        "date_format": job.get("date_format"),
        "extension": extension,
        "group_by_date": job.get("group_by_date", True),
        "keep_original_path": job.get("keep_original_path", False),
        "mount_dir": job.get("mount_dir", "/data"),
        "subfolder": job.get("subfolder", ""),
        "rename_function": rename_function
    }



### run jobs
def build_output_base(root_dir, upload_name, vsn, site, subfolder):
    site = site or "UNKNOWN"
    vsn_site_path = f"{vsn}-{site}"
    return os.path.join(root_dir, upload_name, vsn_site_path, subfolder)


def resolve_output_path(row, base, date, settings):
    if settings["keep_original_path"]:
        try:
            orig_path = row["meta.original_path"]
            if not orig_path.startswith(settings["mount_dir"]):
                raise ValueError(f"original_path '{orig_path}' does not start with mount_dir '{settings['mount_dir']}'")
            rel_path = orig_path[len(settings["mount_dir"]):].lstrip("/")
            return os.path.join(base, os.path.dirname(rel_path))
        except Exception as e:
            logging.error(f"[ERROR] Cannot construct output path from original_path: {e}")
            return None
    else:
        if settings["group_by_date"]:
            return _create_dated_folder(base, date)
        return base


def process_site_data(df_site, site, settings, root_dir, dry_run, test_run, username, password):
    output_base = build_output_base(root_dir, settings["upload_name"], settings["vsn"], site, settings["subfolder"])
    if test_run:
        df_site = df_site.head(1)

    for _, row in df_site.iterrows():
        filename = row['value']
        date = _extract_date_from_filename(filename, settings["date_regex"], settings["date_format"])
        if date is None:
            continue

        output_dir = resolve_output_path(row, output_base, date, settings)
        if output_dir is None:
            continue

        logging.info(f"[CHECK] → {output_dir}")

        download_beehive_files(
            dataframe=pd.DataFrame([row]),
            destination=output_dir,
            username=username,
            password=password,
            extension_filter=settings["extension"],
            group_by_date=False,  # already handled
            date_regex=settings["date_regex"],
            dry_run=dry_run,
            rename_function=settings["rename_function"]
        )


def run_download_jobs(config, root_dir, dry_run: bool, selected_job: Optional[str] = None, test_run: bool = False):
    username = config.get("username")
    password = config.get("password")
    jobs = config.get("jobs", [])

    if not username or not password:
        logging.error("Username or password not provided in YAML config.")
        return

    for job in jobs:
        settings = parse_job_settings(job)
        if selected_job and settings["job_name"] != selected_job:
            continue

        logging.info(f"[JOB START] {settings['job_name']} | VSN={settings['vsn']} | Upload={settings['upload_name']}")

        for start, end in date_range_loop(settings["start_date"], settings["end_date"]):
            query_filter = {
                "vsn": settings["vsn"],
                "upload_name": settings["upload_name"]
            }

            logging.debug(f"[QUERY] {query_filter} | {start} to {end}")
            try:
                df = sage_data_client.query(start=start, end=end, filter=query_filter)
            except Exception as e:
                logging.error(f"[QUERY ERROR] {e}")
                continue

            if df.empty:
                logging.info(f"[SKIP] No data found for VSN={settings['vsn']} from {start} to {end}")
                continue

            if "meta.site" not in df.columns or "meta.sensor" not in df.columns:
                logging.warning("[WARN] DataFrame missing required metadata columns.")
                continue

            for site in df["meta.site"].dropna().unique():
                df_site = df[df["meta.site"] == site]
                process_site_data(df_site, site, settings, root_dir, dry_run, test_run, username, password)


# ========== CLI Interface ==========
def parse_args():
    parser = argparse.ArgumentParser(description="Beehive Multi-Instrument Daily File Downloader. Works with file-forager plugin.")

    parser.add_argument("--config", required=True, help="Path to YAML job config file")
    parser.add_argument("--root-dir", required=True, help="Root output directory (e.g., /nfs/gce/...)")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without downloading files")
    parser.add_argument("--test-run", action="store_true", help="Limit to one file per day per job")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--job", help="Run only the specified job by name")

    return parser.parse_args()


# ========== Entry Point ==========
def main():
    args = parse_args()
    config = load_yaml_file(args.config)
    if not config:
        return

    log_time = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(args.root_dir, "logs", f"log_{log_time}.log")
    setup_logging(log_path, args.debug)

    run_download_jobs(
        config=config,
        root_dir=args.root_dir,
        dry_run=args.dry_run,
        test_run=args.test_run,
        selected_job=args.job
    )


if __name__ == "__main__":
    main()




