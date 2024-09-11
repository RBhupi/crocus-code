import os
import glob
import shutil
import pandas as pd
import xarray as xr
import zipfile
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("process_log.log", mode="w"),
    ],
)

# Metadata dictionary
var_metadata = {
    "Seconds": {
        "long_name": "Elapsed time in seconds",
        "units": "s",
        "description": "Time elapsed in seconds from the start of the recording",
    },
    "Sequence_Number": {
        "long_name": "Sequence number",
        "units": "NA",
        "description": "Index value, increments every 3.3 ms (1/300s)",
    },
    "Diagnostic_Value": {
        "long_name": "Diagnostic value",
        "units": "NA",
        "description": "Diagnostic value 0-8191",
    },
    "Diagnostic_Value_2": {
        "long_name": "Secondary diagnostic value",
        "units": "NA",
        "description": "Diagnostic value 0 or 1 (sync clocks=1).",
    },
    "DS_Diagnostic_Value": {
        "long_name": "DS diagnostic value",
        "units": "NA",
        "description": "Diagnostic value for the DS channel",
    },
    "CO2_Absorptance": {
        "long_name": "CO2 absorptance",
        "units": "NA",
        "description": "Absorptance measurement for CO2",
    },
    "H2O_Absorptance": {
        "long_name": "H2O absorptance",
        "units": "NA",
        "description": "Absorptance measurement for H2O",
    },
    "CO2_mmol_m^3": {
        "long_name": "CO2 concentration",
        "units": "mmol m-3",
        "description": "Carbon dioxide concentration in mmol per cubic meter",
    },
    "CO2_mg_m^3": {
        "long_name": "CO2 mass concentration",
        "units": "mg m-3",
        "description": "Mass concentration of carbon dioxide in mg per cubic meter",
    },
    "H2O_mmol_m^3": {
        "long_name": "H2O concentration",
        "units": "mmol m-3",
        "description": "Water vapor concentration in mmol per cubic meter",
    },
    "H2O_g_m^3": {
        "long_name": "H2O mass concentration",
        "units": "g m-3",
        "description": "Mass concentration of water vapor in g per cubic meter",
    },
    "Temperature_C": {
        "long_name": "Temperature",
        "units": "C",
        "description": "Ambient temperature in degrees Celsius",
    },
    "Pressure_kPa": {
        "long_name": "Pressure",
        "units": "kPa",
        "description": "Atmospheric pressure in kilopascals",
    },
    "Cooler_Voltage_V": {
        "long_name": "Cooler voltage",
        "units": "V",
        "description": "Voltage applied to the cooler",
    },
    "Chopper_Cooler_Voltage_V": {
        "long_name": "Chopper cooler voltage",
        "units": "V",
        "description": "Voltage applied to the chopper cooler",
    },
    "Vin_SmartFlux_V": {
        "long_name": "Vin SmartFlux",
        "units": "V",
        "description": "Input voltage for SmartFlux",
    },
    "CO2_umol_mol": {
        "long_name": "CO2 mole fraction",
        "units": "umol mol-1",
        "description": "Mole fraction of CO2 in micromoles per mole",
    },
    "H2O_mmol_mol": {
        "long_name": "H2O mole fraction",
        "units": "mmol mol-1",
        "description": "Mole fraction of water vapor in mmol per mole",
    },
    "Dew_Point_C": {
        "long_name": "Dew point temperature",
        "units": "C",
        "description": "Dew point temperature in degrees Celsius",
    },
    "CO2_Signal_Strength": {
        "long_name": "CO2 signal strength",
        "units": "NA",
        "description": "Signal strength measurement for CO2",
    },
    "H2O_Sample": {
        "long_name": "H2O sample signal",
        "units": "NA",
        "description": "Sample signal for water vapor",
    },
    "H2O_Reference": {
        "long_name": "H2O reference signal",
        "units": "NA",
        "description": "Reference signal for water vapor",
    },
    "CO2_Sample": {
        "long_name": "CO2 sample signal",
        "units": "NA",
        "description": "Sample signal for carbon dioxide",
    },
    "CO2_Reference": {
        "long_name": "CO2 reference signal",
        "units": "NA",
        "description": "Reference signal for carbon dioxide",
    },
    "Vin_DSI_V": {
        "long_name": "Vin DSI",
        "units": "V",
        "description": "Input voltage for DSI",
    },
    "U_m_s": {
        "long_name": "U wind speed",
        "units": "m s-1",
        "_FillValue": -9999.0,
        "description": "Horizontal wind speed as measured toward the direction in line with the north spar",
    },
    "V_m_s": {
        "long_name": "V wind speed",
        "units": "m s-1",
        "_FillValue": -9999.0,
        "description": "Horizontal wind speed as measured toward the direction of 90° counterclockwise from the north spar",
    },
    "W_m_s": {
        "long_name": "W wind speed",
        "units": "m s-1",
        "_FillValue": -9999.0,
        "description": "Vertical wind speed as measured up the mounting shaft",
    },
    "T_C": {
        "long_name": "Temperature",
        "units": "C",
        "_FillValue": -9999.0,
        "description": "Temperature in degrees Celsius",
    },
    "Anemometer_Diagnostics": {
        "long_name": "Anemometer diagnostics",
        "units": "NA",
        "description": "Diagnostics for the anemometer",
    },
    "CHK": {
        "long_name": "Checksum",
        "units": "NA",
        "description": "Data integrity checksum",
    },
}


def extract_all_zip_files_for_month(root_dir, year_month, temp_csv_dir):
    """Extracts all .ghg files for a given month."""
    month_dir = os.path.join(root_dir, "raw", year_month)
    if os.path.isdir(month_dir):
        zip_files = glob.glob(os.path.join(month_dir, "*.ghg"))
        logging.info(f"Found {len(zip_files)} ZIP files in {month_dir}")
        for zip_file in zip_files:
            logging.info(f"Extracting {zip_file}")
            try:
                with zipfile.ZipFile(zip_file, "r") as zip_ref:
                    zip_ref.extractall(temp_csv_dir)
            except zipfile.BadZipFile as e:
                logging.error(f"Failed to extract {zip_file}: {e}")


def sanitize_column_name(name):
    """Sanitize column names."""
    return (
        name.strip()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
    )


def read_data_file(data_file_path):
    """Reads the .data file and returns a DataFrame."""
    logging.info(f"Reading data file {data_file_path}")
    try:
        with open(data_file_path, "r") as file:
            lines = file.readlines()
    except FileNotFoundError as e:
        logging.error(f"Data file not found: {e}")
        return None, None

    # Extract metadata
    file_metadata = {}
    for line in lines[:6]:
        if ":" in line:
            key, value = line.split(":", 1)
            file_metadata[key.strip()] = value.strip()

    # Read data into DataFrame
    try:
        df = pd.read_csv(data_file_path, skiprows=7, sep="\t")
    except pd.errors.ParserError as e:
        logging.error(f"Failed to parse data file {data_file_path}: {e}")
        return None, file_metadata

    # Sanitize and rename columns
    df.columns = [sanitize_column_name(col) for col in df.columns]
    rename_dict = {
        original: key
        for key, metadata in var_metadata.items()
        for original in df.columns
        if original in metadata.get("long_name", "").replace(" ", "_")
    }
    df.rename(columns=rename_dict, inplace=True)

    # Combine 'Date', 'Time', and 'Nanoseconds' into 'time'
    try:
        df["time"] = (
            pd.to_datetime(df["Date"] + " " + df["Time"], format="%Y-%m-%d %H:%M:%S:%f")
            - pd.Timestamp("1970-01-01")
        ).dt.total_seconds()  # + df['Nanoseconds'] / 1e9  # Add nanoseconds as fractional seconds

        df["time"] = df["time"].astype("float64")
        df.set_index("time", inplace=True)

        df.drop(
            columns=["Date", "Time", "Nanoseconds", "DATAH"],
            inplace=True,
            errors="ignore",
        )
    except Exception as e:
        logging.error(f"Error processing time index in {data_file_path}: {e}")
        return None, file_metadata

    logging.info(f"Read and processed {data_file_path} with {len(df)} records")
    return df, file_metadata


def read_metadata_file(metadata_file_path):
    """Reads metadata from a file."""
    logging.info(f"Reading metadata file {metadata_file_path}")
    desired_keys = {
        "site_name": r"site_name\s*=\s*(.+)",
        "altitude": r"altitude\s*=\s*(.+)",
        "latitude": r"latitude\s*=\s*(.+)",
        "longitude": r"longitude\s*=\s*(.+)",
        "station_name": r"station_name\s*=\s*(.+)",
        "logger_id": r"logger_id\s*=\s*(.+)",
        "acquisition_frequency": r"acquisition_frequency\s*=\s*(.+)",
        "file_duration": r"file_duration\s*=\s*(.+)",
        "instr_1_manufacturer": r"instr_1_manufacturer\s*=\s*(.+)",
        "instr_1_model": r"instr_1_model\s*=\s*(.+)",
        "instr_1_sn": r"instr_1_sn\s*=\s*(.+)",
        "instr_2_manufacturer": r"instr_2_manufacturer\s*=\s*(.+)",
        "instr_2_model": r"instr_2_model\s*=\s*(.+)",
        "instr_2_sn": r"instr_2_sn\s*=\s*(.+)",
    }

    metadata = {}

    try:
        with open(metadata_file_path, "r") as file:
            for line in file:
                for key, pattern in desired_keys.items():
                    match = re.search(pattern, line)
                    if match:
                        metadata[key] = match.group(1).strip()
    except FileNotFoundError as e:
        logging.error(f"Metadata file not found: {e}")
        return None

    logging.info(f"Extracted metadata from {metadata_file_path}")
    #print(metadata)
    #exit()
    return metadata


def combine_data_files(data_file_paths, metadata_file_paths):
    """Combines all .data files into a single DataFrame."""
    dataframes = []
    combined_metadata = {}

    for data_file_path, metadata_file_path in zip(data_file_paths, metadata_file_paths):
        df, file_metadata = read_data_file(data_file_path)
        if df is not None:
            dataframes.append(df)
        else:
            logging.warning(f"Skipping file {data_file_path} due to read errors")

        metadata = read_metadata_file(metadata_file_path)
        if metadata:
            combined_metadata.update(file_metadata)
            combined_metadata.update(metadata)
        else:
            logging.warning(f"Metadata extraction failed for {metadata_file_path}")

    if not dataframes:
        logging.error("No dataframes were created, cannot continue with empty data")
        return None, None

    combined_df = pd.concat(dataframes, ignore_index=False)
    combined_df.sort_index(inplace=True)

    # Handle mixed types in 'Anemometer_Diagnostics'
    if 'Anemometer_Diagnostics' in combined_df.columns:
        # Try converting to integer, replace invalid entries with a default value
        combined_df['Anemometer_Diagnostics'] = pd.to_numeric(combined_df['Anemometer_Diagnostics'], errors='coerce').fillna(-9999).astype(int)


    logging.info(
        f"Combined DataFrame time range: {combined_df.index.min()} to {combined_df.index.max()}"
    )
    return combined_df, combined_metadata



def df_to_xarray(df, metadata):
    """Converts the DataFrame to an xarray Dataset."""
    if df is None or df.empty:
        logging.error("Empty DataFrame cannot be converted to xarray Dataset")
        return None

    ds = xr.Dataset.from_dataframe(df)

    ds = ds.assign_coords(time=("time", df.index))

    for key, value in metadata.items():
        ds.attrs[key] = value

    for var_name in df.columns:
        if var_name in var_metadata:
            ds[var_name].attrs.update(var_metadata[var_name])
        else:
            logging.warning(f"Variable {var_name} not found in var_metadata")

    ds.time.attrs.update(
        {
            "long_name": "time",
            "units": "seconds since 1970-01-01 00:00:00",
            "calendar": "standard",
        }
    )

    logging.info(
        f"Converted to xarray Dataset with time range: {ds['time'].min().values} to {ds['time'].max().values}"
    )
    return ds


def process_files_for_month(root_dir, year_month):
    """Processes files and creates a NetCDF file."""
    temp_data_dir = os.path.join(root_dir, "temp", "data")
    os.makedirs(temp_data_dir, exist_ok=True)

    extract_all_zip_files_for_month(root_dir, year_month, temp_data_dir)

    data_files = glob.glob(os.path.join(temp_data_dir, "*.data"))
    metadata_files = [f.replace(".data", ".metadata") for f in data_files]

    if not data_files:
        logging.error("No data files found for the given month.")
        return

    combined_df, metadata = combine_data_files(data_files, metadata_files)
    if combined_df is None:
        logging.error("No data to process into NetCDF")
        return

    combined_ds = df_to_xarray(combined_df, metadata)
    if combined_ds is None:
        logging.error("Failed to convert data to xarray Dataset")
        return

    nc_dir = os.path.join(root_dir, "netcdf")
    os.makedirs(nc_dir, exist_ok=True)
    netcdf_filename = f"smartflux_rawdata_{year_month.replace('/', '_')}.nc"
    netcdf_filepath = os.path.join(nc_dir, netcdf_filename)

    try:
        # Save with compression
        encoding = {
            var: {"zlib": True, "complevel": 5} for var in combined_ds.data_vars
        }
        combined_ds.to_netcdf(
            netcdf_filepath, unlimited_dims=["time"], encoding=encoding
        )
        logging.info(f"Written combined dataset to {netcdf_filepath}")
    except Exception as e:
        logging.error(f"Failed to write NetCDF file {netcdf_filepath}: {e}")

    shutil.rmtree(temp_data_dir)


# Main execution
root_dir = "/Users/bhupendra/projects/crocus/data/flux_data/data"
year_month = "2024/08"

process_files_for_month(root_dir, year_month)
