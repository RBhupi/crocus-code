import os
import re
import requests
import pandas as pd
from typing import List, Optional, Callable
import logging

# ========== Logging Setup ==========

logging.basicConfig( level=logging.INFO, format="%(asctime)s - %(message)s")


def download_beehive_files(
    dataframe: pd.DataFrame,
    destination: str,
    username: str,
    password: str,
    extension_filter: Optional[str] = None,
    group_by_date: bool = True,
    skip_if_exists: bool = True,
    rename_function: Optional[Callable[[str], str]] = None,
    date_regex: Optional[str] = None,
    dry_run: bool = False,
) -> List[str]:
    """
    Downloads files listed in a Beehive query DataFrame.

    Organizes downloaded files into subdirectories based on dates using regex pattern.

    Parameters:
        dataframe (pd.DataFrame): DataFrame from `sage_data_client.query()` that includes a 'value' column with file URLs.
        destination (str): Root directory for saving downloaded files.
        username (str): Beehive (Waggle) authorized username.
        password (str): Beehive (Waggle) authorized password.
        extension_filter (str, optional): File extension to include (e.g., 'nc').
        group_by_date (bool): Whether to store files in subfolders like /YYYYMM/YYYYMMDD/.
        skip_if_exists (bool): If True, skips downloading files already present.
        rename_function (Callable, optional): Function to rename files before saving.
        date_regex (str, optional): Regex pattern to extract 'YYYYMMDD' from filenames.

    Returns:
        List[str]: List of full paths to successfully downloaded files.
    """
    if dataframe.empty or "value" not in dataframe.columns:
        logging.info("[INFO] No valid file URLs found in DataFrame.")
        return []

    urls = dataframe["value"]
    if extension_filter:
        urls = urls[urls.str.endswith(extension_filter)]
    urls = urls.unique()

    extract_date = _compile_date_extractor(date_regex) if group_by_date else None
    saved_files = []

    for url in urls:
        original_filename = os.path.basename(url)
        filename = rename_function(original_filename) if rename_function else original_filename

        if group_by_date:
            if not extract_date:
                raise ValueError("[ERROR] Date grouping is enabled but no date regex was provided.")
            date_str = extract_date(filename)
            if not date_str:
                logging.info(f"[WARN] No date found in filename: {filename}. Skipping this file.")
                continue
            target_folder = _create_dated_folder(destination, date_str)
        else:
            target_folder = destination

        os.makedirs(target_folder, exist_ok=True)
        target_path = os.path.join(target_folder, filename)

        if skip_if_exists and os.path.exists(target_path):
            logging.info(f"[SKIP] File already exists: {target_path}")
            continue

        if dry_run:
            logging.info(f"[DRY-RUN] Would download: {url} to {target_path}")
            saved_files.append(target_path)
            continue

        if _fetch_and_save_file(url, target_path, username, password):
            saved_files.append(target_path)

    logging.info(f"\n[SUMMARY] {len(saved_files)} file(s) downloaded.")
    return saved_files


# === Helper Functions ===
def _compile_date_extractor(pattern: str) -> Callable[[str], Optional[str]]:
    """
    Compiles a regex pattern to extract a date string (YYYYMMDD) from a filename.

    Returns a function that takes a filename and returns the first match.
    """
    compiled = re.compile(pattern)

    def extract(filename: str) -> Optional[str]:
        match = compiled.search(filename)
        return match.group(1) if match else None

    return extract


def _create_dated_folder(base_path: str, date_str: str) -> str:
    """
    Constructs a nested directory path based on a YYYYMMDD date string.

    Example: If base_path='/data' and date_str='20240509',
    returns '/data/202405/20240509'
    """
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    return os.path.join(base_path, f"{year}{month}", f"{year}{month}{day}")


def _fetch_and_save_file(url: str, filepath: str, username: str, password: str) -> bool:
    """
    Downloads a file from a given URL using basic auth and saves it to disk.
    """
    try:
        response = requests.get(url, auth=(username, password), timeout=30)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"[DOWNLOADED] {filepath}")
            return True
        else:
            logging.info(f"[ERROR] HTTP {response.status_code} for {url}")
    except Exception as e:
        logging.info(f"[ERROR] Exception downloading {url}: {e}")
    return False




# import sage_data_client

# # Step 1: Query Beehive
# df = sage_data_client.query(
#     start="-1d",
#     filter={
#         "plugin": ".*file-forager:0.*",
#         "vsn": "W09A",
#         "upload_name": "cl61_files",
#         "site": "ATMOS",
#         "sensor": "vaisala_cl61",
#     }
# )

# # Step 2: Download files into organized folders
# download_beehive_files(
#     dataframe=df,
#     destination="/Users/bhupendra/temp/cl61-test",
#     username="bhupendraraut",
#     password="REMOVED",
#     extension_filter="nc",
#     group_by_date=True,
#     date_regex=r"_(\d{8})_\d{6}"
# )
