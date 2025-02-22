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

## These are the variables that are in the full_output files, but not in fluxnet files.
## We need to add these to the final output file. Be careful with adding these variables.
## not using this dictionery for now as this cann't be automated due to the units issues.
# TODO: use this dictionery to add new variables to the final output file.
amer_var_full = {
    "datetime": "datetime",
    "air_pressure": "PA",
    "air_temperature": "TA",
    "RH": "RH",
    "co2_strg": "SC",
    "H_strg": "SH",
    "LE_strg": "SLE",
    "VPD": "VPD",
}


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

        logging.info(
            f"Found {len(files_in_range)} files in the date range {start_dt} to {end_dt}"
        )

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

    logging.info(
        f"Combined DataFrame time range: {combined_df['TIMESTAMP_START'].min()} to {combined_df['TIMESTAMP_START'].max()}"
    )
    df_cleaned = clean_df(combined_df)
    df_cleaned = df_cleaned.sort_values(by="TIMESTAMP_START")
    return df_cleaned



# This is highly sensitive function, we need to be very careful while adding new variables.
# We need to make sure that the units are correct and that the variables are not all missing.
# This can be avoided by applying clean_df function at the last step. 
# TODO: use clean_df function at the end.

def combine_full_output_files(file_paths):
    """Combines multiple full output files into one DataFrame."""
    dataframes = []

    for file_path in file_paths:
        df = pd.read_csv(file_path, skiprows=[0, 2])
        df["filename"] = file_path  # Store filename for datetime extraction
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)

    # Extract datetime from filename
    dt_extracted = combined_df['filename'].str.extract(r'(\d{4}-\d{2}-\d{2})T(\d{2})(\d{2})(\d{2})')

    # Combine extracted parts into a proper datetime string
    combined_df['datetime'] = pd.to_datetime(
        dt_extracted[0] + ' ' + dt_extracted[1] + ':' + dt_extracted[2] + ':' + dt_extracted[3], 
        format='%Y-%m-%d %H:%M:%S', errors='coerce'
    )

    # Sort by datetime
    #combined_df = combined_df.sort_values(by="datetime").reset_index(drop=True)

    # Select only the required columns change units, we are creatung new columns here
    combined_df = combined_df[["datetime", "air_pressure", "air_temperature", "RH", "VPD"]]
    
    combined_df["PA"] = combined_df["air_pressure"] / 1000  # convert to kPa
    combined_df["TA"] = combined_df["air_temperature"] - 273.15 #   convert to deg C
    combined_df["VPD"] = combined_df["VPD"] /100 # convert to hPa
    combined_df["RH"] = combined_df["RH"]


    # Format 'PA', 'TA', and 'RH' to 3 decimal places
    combined_df["PA"] = combined_df["PA"].round(3)
    combined_df["TA"] = combined_df["TA"].round(3)
    combined_df["RH"] = combined_df["RH"].round(3)
    combined_df["VPD"] = combined_df["VPD"].round(3)

    combined_df = combined_df.sort_values(by="datetime")
    print(combined_df.head())
    
    # Set datetime as index
    #combined_df.set_index('datetime', inplace=True)
    
    # Clean DataFrame (Assuming `clean_df` is a defined function)
    df_cleaned = clean_df(combined_df)

    return df_cleaned

def add_full_vars_to_df(temp_csv_dir, combined_df):
    # this is for full_output files because we need more variables.
    full_files = glob.glob(
        os.path.join(temp_csv_dir, "output", "eddypro_exp_full_output_*_exp.csv")
    )
    full_files.sort()

    if not full_files:
        logging.error("No full_output files found for the given date range.")
        return

    full_df_filtered = combine_full_output_files(full_files)
    print(full_df_filtered.head())
    combined_df[["PA", "TA", "RH", "VPD"]] = full_df_filtered[["PA", "TA", "RH", "VPD"]]
    print(combined_df.head())
    return combined_df


def clean_df(df):
    """
    Drops columns from a pandas DataFrame where all values are NaN, missing, or NA.
    Replaces empty cells, NA, NaNs, -Inf, Inf with -9999.
    """
    columns_to_drop = []
    for col in df.columns:
        if df[col].isnull().all():  # Check if all values in the column are NaN or NA
            columns_to_drop.append(col)

    df_cleaned = df.drop(columns=columns_to_drop)
    df_cleaned = df_cleaned.dropna(axis=1, how="all")

    # Replace empty cells, NA, NaNs, -Inf, Inf with -9999
    df_cleaned = df_cleaned.replace([np.nan, np.inf, -np.inf], -9999).fillna(-9999)

    df_filtered = keep_columns(df_cleaned)
    #print(df_filtered.columns)

    return df_filtered



def keep_columns(df):
    """Keeps only the standard AmeriFlux variables.
    """

    ameriflux_vars = [
    "TIMESTAMP_START", "TIMESTAMP_END", "TIMESTAMP", "COND_WATER", "DO", "PCH4", "PCO2", "PN2O",
    "PPFD_UW_IN", "TW", "DBH", "LEAF_WET", "SAP_DT", "SAP_FLOW", "T_BOLE", "T_CANOPY", "FETCH_70",
    "FETCH_80", "FETCH_90", "FETCH_FILTER", "FETCH_MAX", "CH4", "CH4_MIXING_RATIO", "CO", "CO2",
    "CO2_MIXING_RATIO", "CO2_SIGMA", "CO2C13", "FC", "FCH4", "FN2O", "FNO", "FNO2", "FO3", "H2O",
    "H2O_MIXING_RATIO", "H2O_SIGMA", "N2O", "N2O_MIXING_RATION", "NO", "NO2", "O3", "SC", "SCH4",
    "SN2O", "SNO", "SNO2", "SO2", "SO3", "FH2O", "G", "H", "LE", "SB", "SG", "SH", "SLE", "PA",
    "PBLH", "RH", "T_SONIC", "T_SONIC_SIGMA", "TA", "VPD", "D_SNOW", "P", "P_RAIN", "P_SNOW",
    "RUNOFF", "STEMFLOW", "THROUGHFALL", "ALB", "APAR", "EVI", "FAPAR", "FIPAR", "LW_BC_IN",
    "LW_BC_OUT", "LW_IN", "LW_OUT", "MCRI", "MTCI", "NDVI", "NETRAD", "NIRV", "PPFD_BC_IN",
    "PPFD_BC_OUT", "PPFD_DIF", "PPFD_DIR", "PPFD_IN", "PPFD_OUT", "PRI", "R_UVA", "R_UVB",
    "REDCIRed", "REP", "SPEC_NIR_IN", "SPEC_NIR_OUT", "SPEC_NIR_REFL", "SPEC_PRI_REF_IN",
    "SPEC_PRI_REF_OUT", "SPEC_PRI_REF_REFL", "SPEC_PRI_TGT_IN", "SPEC_PRI_TGT_OUT",
    "SPEC_PRI_TGT_REFL", "SPEC_RED_IN", "SPEC_RED_OUT", "SPEC_RED_REFL", "SR", "SW_BC_IN",
    "SW_BC_OUT", "SW_DIF", "SW_DIR", "SW_IN", "SW_OUT", "TCARI", "SWC", "SWP", "TS", "TNS", "WTD",
    "MO_LENGTH", "TAU", "U_SIGMA", "USTAR", "V_SIGMA", "W_SIGMA", "WD", "WD_SIGMA", "WS", "WS_MAX",
    "ZL", "GPP", "NEE", "RECO", "FC_SSITC_TEST", "FCH4_SSITC_TEST", "FN2O_SSITC_TEST",
    "FNO_SSITC_TEST", "FNO2_SSITC_TEST", "FO3_SSITC_TEST", "H_SSITC_TEST", "LE_SSITC_TEST",
    "TAU_SSITC_TEST"
]

    existing_columns = [col for col in ameriflux_vars if col in df.columns]

    if existing_columns:
        #print(df[existing_columns].columns) 
        return df[existing_columns]
    else:
        print("No AmeriFlux variables found. Check the original files.")
        return None



def process_files(args, start_datetime, end_datetime):
    """Main processing function."""
    temp_csv_dir = os.path.join(args.root_dir, "temp", "csv")
    # delete and remake again
    # shutil.rmtree(temp_csv_dir)
    os.makedirs(temp_csv_dir, exist_ok=True)

    extract_zip_files(args.root_dir, start_datetime, end_datetime, temp_csv_dir)

    csv_files = glob.glob(
        os.path.join(temp_csv_dir, "output", "eddypro_exp_fluxnet*_exp.csv")
    )
    csv_files.sort()
 

    if not csv_files:
        logging.error("No CSV files found for the given date range.")
        return

    combined_df = combine_csv_files(csv_files)
    combined_df = add_full_vars_to_df(temp_csv_dir, combined_df)
    
    #year_month_str = end_datetime.strftime("%Y%m")
    ts_start = combined_df["TIMESTAMP_START"].min()
    ts_end = combined_df["TIMESTAMP_END"].max()

    fp_dir = os.path.join(args.root_dir, "AmeriFlux")
    os.makedirs(fp_dir, exist_ok=True)

    out_filename = f"{args.prefix}_{ts_start}_{ts_end}.csv"
    out_filepath = os.path.join(fp_dir, out_filename)

    combined_df.dropna(axis=1, how="all").to_csv(out_filepath, index=False)
    logging.info(f"Saved combined data to {out_filepath}")
    shutil.rmtree(temp_csv_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process raw data files for a given time range."
    )
    parser.add_argument(
        "--start",
        dest="start",
        type=str,
        #required=True,
        default="2024-08-01T00:00:00",
        help="Start date and time in the format YYYY-MM-DDTHH:MM:SS, e.g., 2024-08-01T00:00:00.",
    )
    parser.add_argument(
        "--end",
        dest="end",
        type=str,
        #required=True,
        default="2024-08-01T23:59:59",
        help="End date and time in the format YYYY-MM-DDTHH:MM:SS, e.g., 2024-08-01T23:59:59.",
    )
    parser.add_argument(
        "--root_dir",
        dest="root_dir",
        type=str,
        default="/Users/bhupendra/projects/crocus/data/flux_data/data",
        help="Root directory for data. Default '/Users/bhupendra/projects/crocus/data/flux_data/data'.",
    )
    parser.add_argument(
        "--prefix",
        dest="prefix",
        type=str,
        default="US-CU1_HH",
        help="Prefix for output filename.",
    )

    args = parser.parse_args()

    start_datetime = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    process_files(args, start_datetime, end_datetime)
