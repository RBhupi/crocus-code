# Ingest scripts for ingesting files from beehive.


## Beehive File Downloader
The downloader is modular and works with any plugin files stored in Beehive (e.g., SmartFlux, Mobotix, Parsivel), not just file-forager.

## crocus file curator
Automatically downloads and sorts files from the [SAGE Beehive](https://sagecontinuum.org) uploaded by the **file-forager** plugin using configurable YAML file organized by site, VSN, and date.
Minor changes will need to use it for other plugins.

---

### For File-Forager Use
For instruments using the `file-forager` plugin:
- The tool queries Beehive using `upload_name` (e.g., `cl61_files`)
- Each uploaded file contains metadata (`site`, `sensor`, etc.) used to organize output
- The file name is expected to contain a **date pattern** for folder structure

---

## Step-by-Step Instructions

### Step 1: Create a YAML Config

Create a file like `download_jobs.yaml` with the following structure:

```yaml
username: your_waggle_username
password: your_waggle_password

jobs:
  - job: download-cl61-w09a
    upload_name: cl61_files
    instrument: ceilometer
    vsn: W09A
    start_date: "2025-05-05"
    end_date: "2025-05-09"
    date_pattern: "_(\\d{8})_\\d{6}"
    extension: nc
    subfolder: netcdf/raw
    group_by_date: true
```

### Step 2: Understand How Each Field Affects Output
| YAML Field      | Description                                                               |
| --------------- | ------------------------------------------------------------------------- |
| `upload_name`   | Matches the plugin name (e.g., `cl61_files`) used in Beehive              |
| `vsn`           | Node ID that produced the data (e.g., `W09A`)                             |
| `start_date`    | Start of query window (format: `"YYYY-MM-DD"`)                            |
| `end_date`      | End of query window                                                       |
| `date_pattern`  | Regex pattern to extract date (`YYYYMMDD`) from filename                  |
| `extension`     | File type to filter (e.g., `nc`, `txt`, `zip`)                            |
| `subfolder`     | Optional mid-path between node-site and date folders (e.g., `netcdf/raw`) |
| `group_by_date` | If true, organizes files under `/YYYYMM/YYYYMMDD/` directories            |


### Step 3: Folder Structure
Files are downloaded and saved to:
```bash
{root_dir}/foraged-data/{upload_name}/{vsn}-{site}/{subfolder}/YYYYMM/YYYYMMDD/
```

Example: For a file named: `cmscl6004_20250509_085340.nc`, With settings following:

```
    upload_name: cl61_files
    vsn: W09A
    site: ATMOS
    subfolder: netcdf/raw
```
It will be saved to:
```
/nfs/gce/projects/crocus/data/foraged-data/cl61_files/W09A-ATMOS/netcdf/raw/202505/20250509/
```

### Step 4. test-run or dry-run

```
python crocus_file_curator.py \
  --config download_jobs.yaml \
  --root-dir ./local-output \
  --job download-cl61-w09a \
  --dry-run \
  --test-run \
  --debug
```


### Step 5. Run the script
```
python crocus_file_curator.py \
  --config download_jobs.yaml \
  --root-dir /nfs/gce/projects/crocus/data \
  --dry-run \
  --test-run \
  --debug \
  --job download-cl61-w09a
```

