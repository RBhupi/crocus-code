import os
import yaml
import logging
import argparse
import datetime
import pandas as pd
import sage_data_client
from typing import Optional
from beehive_file_fetcher import download_beehive_files


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


# ========== Main Job Executor ==========
def run_download_jobs(config, root_dir, dry_run: bool, selected_job: Optional[str] = None, test_run: bool = False):
    username = config.get("username")
    password = config.get("password")
    jobs = config.get("jobs", [])

    if not username or not password:
        logging.error("Username or password not provided in YAML config.")
        return

    for job in jobs:
        job_name = job.get("job")
        if selected_job and job_name != selected_job:
            continue  # Skip others if --job is set

        upload_name = job["upload_name"]
        vsn = job["vsn"]
        start_date = job["start_date"]
        end_date = job["end_date"]
        date_pattern = job["date_pattern"]
        extension = job.get("extension", "nc")
        group_by_date = job.get("group_by_date", True)

        logging.info(f"[JOB START] {job_name} | VSN={vsn} | Upload={upload_name}")

        for start, end in date_range_loop(start_date, end_date):
            query_filter = {
                "vsn": vsn,
                "upload_name": upload_name
            }

            logging.debug(f"[QUERY] {query_filter} | {start} to {end}")
            try:
                df = sage_data_client.query(start=start, end=end, filter=query_filter)
            except Exception as e:
                logging.error(f"[QUERY ERROR] {e}")
                continue

            if df.empty:
                logging.info(f"[SKIP] No data found for VSN={vsn} from {start} to {end}")
                continue

            if "meta.site" not in df.columns or "meta.sensor" not in df.columns:
                logging.warning("[WARN] DataFrame missing required metadata columns.")
                continue

            for site in df["meta.site"].dropna().unique():
                df_site = df[df["meta.site"] == site]
                sensor = df_site["meta.sensor"].iloc[0]

                subfolder = job.get("subfolder", "")  # NEW

                output_dir = os.path.join(
                    root_dir,
                    upload_name,
                    f"{vsn}-{site}",
                    *subfolder.strip("/").split("/") if subfolder else []
                )


                logging.info(f"[SAVE] {len(df_site)} entries to {output_dir}")

                if test_run:
                    df_site = df_site.head(1)
                    logging.debug(f"[TEST-RUN] Limited to 1 file: {df_site['value'].iloc[0]}")

                download_beehive_files(
                    dataframe=df_site,
                    destination=output_dir,
                    username=username,
                    password=password,
                    extension_filter=extension,
                    group_by_date=group_by_date,
                    date_regex=date_pattern,
                    dry_run=dry_run
                )


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

    log_time = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
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
