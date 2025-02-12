#!/usr/bin/env python3

import os
import glob
import zipfile
import shutil
import re
import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("flux_processing.log", mode="w"),
    ],
)

def extract_zip_files(root_dir, start_dt, end_dt, temp_csv_dir):
    """Extracts all .zip files within a given date and time range."""
    year_month = start_dt.strftime("%Y/%m")
    month_dir = os.path.join(root_dir, "results", year_month)

    if os.path.isdir(month_dir):
        zip_files = glob.glob(os.path.join(month_dir, "*.zip"))
        files_in_range = []

        for zip_file in zip_files:
            match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{6})", os.path.basename(zip_file))
            if match:
                file_datetime_str = match.group(1)
                file_dt = datetime.strptime(file_datetime_str, "%Y-%m-%dT%H%M%S")

                if start_dt <= file_dt <= end_dt:
                    files_in_range.append(zip_file)

        logging.info(f"Found {len(files_in_range)} files in the date range {start_dt} to {end_dt}")

        for zip_file in files_in_range:
            logging.info(f"Extracting {zip_file}")
            try:
                with zipfile.ZipFile(zip_file, "r") as zip_ref:
                    zip_ref.extractall(temp_csv_dir)
            except zipfile.BadZipFile as e:
                logging.error(f"Failed to extract {zip_file}: {e}")

def combine_csv_files(file_paths):
    """Combines multiple CSV files into one DataFrame."""
    dataframes = []

    for file_path in file_paths:
        df = pd.read_csv(file_path)
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df = combined_df.sort_values(by="TIMESTAMP_START").reset_index(drop=True)

    logging.info(f"Combined DataFrame time range: {combined_df['TIMESTAMP_START'].min()} to {combined_df['TIMESTAMP_START'].max()}")
    df_cleaned = combined_df.dropna(axis=1, how='all')
    return combined_df

def process_files(args, start_datetime, end_datetime):
    """Main processing function."""
    temp_csv_dir = os.path.join(args.root_dir, "temp", "csv")
    # delete and remake again
    #shutil.rmtree(temp_csv_dir)
    os.makedirs(temp_csv_dir, exist_ok=True)

    extract_zip_files(args.root_dir, start_datetime, end_datetime, temp_csv_dir)

    csv_files = glob.glob(os.path.join(temp_csv_dir, "output", "eddypro_exp_fluxnet*_exp.csv"))
    if not csv_files:
        logging.error("No CSV files found for the given date range.")
        return

    combined_df = combine_csv_files(csv_files)

    year_month_str = end_datetime.strftime("%Y%m")
    ts_start = combined_df["TIMESTAMP_START"].min()
    ts_end = combined_df["TIMESTAMP_START"].max()

    fp_dir = os.path.join(args.root_dir, "AmeriFlux", year_month_str)
    os.makedirs(fp_dir, exist_ok=True)

    out_filename = f"{args.prefix}_{ts_start}_{ts_end}.csv"
    out_filepath = os.path.join(fp_dir, out_filename)

    combined_df.dropna(axis=1, how='all').to_csv(out_filepath, index=False)
    logging.info(f"Saved combined data to {out_filepath}")
    shutil.rmtree(temp_csv_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process raw data files for a given time range.")
    parser.add_argument(
        "--start",
        dest="start",
        type=str,
        required=True,
        help="Start date and time in the format YYYY-MM-DDTHH:MM:SS, e.g., 2024-08-01T00:00:00."
    )
    parser.add_argument(
        "--end",
        dest="end",
        type=str,
        required=True,
        help="End date and time in the format YYYY-MM-DDTHH:MM:SS, e.g., 2024-08-01T23:59:59."
    )
    parser.add_argument(
        "--root_dir",
        dest="root_dir",
        type=str,
        default="/Users/bhupendra/projects/crocus/data/flux_data/data",
        help="Root directory for data. Default '/Users/bhupendra/projects/crocus/data/flux_data/data'."
    )
    parser.add_argument(
        "--prefix",
        dest="prefix",
        type=str,
        default="US-CU1_HH",
        help="Prefix for output filename."
    )

    args = parser.parse_args()

    start_datetime = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    process_files(args, start_datetime, end_datetime)
