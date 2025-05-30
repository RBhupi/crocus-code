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

***REMOVED***
***REMOVED***
***REMOVED***
***REMOVED***
***REMOVED***
    start_date: "2025-05-05"
    end_date: "2025-05-09"
    date_pattern: "_(\\d{8})_\\d{6}"
***REMOVED***
    subfolder: netcdf/raw
***REMOVED***
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
***REMOVED***
***REMOVED***
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

# 🗂 Preserving Original File Paths During Download

A powerful option that allows users to **preserve the original file path structure** as on the waggle node. This is useful when you want to mirror the exact directory layout from the data collection environment onto your local file system.

---

## 🔧 Feature: `keep_original_path`

When enabled, this feature uses the `meta.original_path` field from each file’s metadata to determine **where the file should be stored** under your `root_dir`.
This field is the direct consequence of the mount directory in your job script for  `file-forager` app.

### ✅ What it does:
- It **replaces** mount directory (default `/data/`) with your provided `root_dir`.
- It **recreates the same internal folder structure and filename**.

### 🚫 What it requires:
- The `meta.original_path` **must exist** and **must start with a valid mount directory** (e.g. `/data` or `/if_you/change_it`).
- If this condition is not met, the job will raise an error and stop — this ensures file paths are correct and intentional.

---

## 🔧 YAML Configuration

You must set two extra fields in the job definition:

```yaml
keep_original_path: true
mount_dir: /data  # Or whatever the original upload mount path was
```

---

## ✅ Example Job Configuration

```yaml
***REMOVED***
  - job: cl61-download
***REMOVED***
***REMOVED***
    start_date: 2025-05-10T00:00:00Z
    end_date: 2025-05-12T23:59:59Z
    date_pattern: .*\.nc
***REMOVED***
    mount_dir: /data
```

---

## 📁 Example Path Transformation

Assume the following:

- `root_dir` is set to `/my_download_dir/`
- `meta.original_path` is `/data/netcdf/cmscl6004_20250513_021340.nc`
- `mount_dir` is `/data`

Then the file will be saved to:

```
/my_download_dir/netcdf/cmscl6004_20250513_021340.nc
```

The `/data/` portion of the original path is replaced by your `root_dir`.
The group_by_date option is ignored.

---

## ⚠️ Important Notes

- If `meta.original_path` is missing or doesn't start with the specified `mount_dir`, the job will fail with an error.
- This feature is **per job**. If some jobs don't require path preservation, just omit `keep_original_path`.

