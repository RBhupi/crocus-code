import os
import glob
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
import argparse
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# this function need to be broken in to smaller functions
def process_files(start_date, end_date, input_dir, output_dir):
    current_date = start_date
    while current_date <= end_date:
        try:
            prev_date_str = (current_date - timedelta(days=1)).strftime('%Y%m%d')
            date_str = current_date.strftime('%Y%m%d')
            next_date_str = (current_date + timedelta(days=1)).strftime('%Y%m%d')

            time_units = f'seconds since {date_str} 00:00:00'
            encoding = {'time': {'units': time_units, 'calendar': 'standard', 'dtype': 'float64'}}

            prev_files_pattern = os.path.join(input_dir, f'*{prev_date_str}*.nc')
            current_files_pattern = os.path.join(input_dir, f'*{date_str}*.nc')
            next_files_pattern = os.path.join(input_dir, f'*{next_date_str}*.nc')

            prev_day_files = sorted(glob.glob(prev_files_pattern))
            current_day_files = sorted(glob.glob(current_files_pattern))
            next_day_files = sorted(glob.glob(next_files_pattern))

            selected_files = current_day_files
            if prev_day_files:
                selected_files.insert(0, prev_day_files[-1])
            if next_day_files:
                selected_files.append(next_day_files[0])

            if selected_files:
                daily_ds = xr.open_mfdataset(selected_files, combine='by_coords')

                start_of_day = pd.to_datetime(date_str).floor('D')
                end_of_day = start_of_day + timedelta(days=1)
                daily_ds = daily_ds.sel(time=slice(start_of_day, end_of_day - pd.to_timedelta('1ms')))

                output_path = os.path.join(output_dir, f'daily_file_{date_str}.nc')
                daily_ds.to_netcdf(output_path, encoding=encoding)
                logging.info(f'Saved daily data for {date_str} to {output_path}')

                daily_ds.close()
            else:
                logging.warning(f'No files for {date_str}')
        except Exception as e:
            logging.error(f'Error processing files for {date_str}: {e}')
        
        current_date += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make daily netCDF files.")
    parser.add_argument("--start", help="First day YYYY-MM-DD format.", required=True)
    parser.add_argument("--end", help="End day YYYY-MM-DD format.", required=True)
    parser.add_argument("--input", help="Directory.", required=True)
    parser.add_argument("--output", help="Directory.", required=True)

    args = parser.parse_args()

    # Convert to datetime objects
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    # make out put  dir
    os.makedirs(args.output_dir, exist_ok=True)

    process_files(start_date, end_date, args.input_dir, args.output_dir)
