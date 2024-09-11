import pandas as pd
import xarray as xr
import glob
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def read_smartflux_summary(file_path):
    # Read the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Extract headers, units, and data
    header_line = lines[0].strip()
    unit_line = lines[1].strip()
    data_lines = lines[2:]

    # Split header and units by tab
    headers = header_line.split('\t')
    units = unit_line.split('\t')

    # Read data into a DataFrame
    data = [line.strip().split('\t') for line in data_lines]
    df = pd.DataFrame(data, columns=headers)
    
    # Parse the date and time columns and combine them into a single datetime column
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df.set_index('datetime', inplace=True)

    # Drop the original date and time columns as they are no longer needed
    df.drop(columns=['date', 'time'], inplace=True)
    
    # Convert the data to the appropriate data types
    for column in df.columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    # Convert the DataFrame to an xarray Dataset
    ds = xr.Dataset.from_dataframe(df)
    
    # Add units as attributes to the Dataset variables
    for header, unit in zip(headers, units):
        if header in ds.variables:
            ds[header].attrs['units'] = unit

    return ds

def drop_all_nan_columns(ds):
    # Identify variables that are entirely NaN and drop them
    nan_vars = [var for var in ds.data_vars if ds[var].isnull().all()]
    ds = ds.drop_vars(nan_vars)
    
    return ds



def create_plots(ds, output_pdf):
    # Convert xarray dataset to pandas dataframe for easier plotting
    df = ds.to_dataframe().reset_index()

    # Define variable groups for pair plots and panel plots
    pair_plot_vars = [
        ['Tau', 'H', 'LE', 'co2_flux'],
        ['qc_Tau', 'qc_H', 'qc_LE', 'qc_co2_flux'],
        ['co2_molar_density', 'co2_mole_fraction', 'co2_mixing_ratio', 'co2_flux'],
        ['h2o_molar_density', 'h2o_mole_fraction', 'h2o_mixing_ratio', 'h2o_flux']
    ]

    time_series_vars = [
        ['Tau', 'H', 'LE', 'co2_flux'],
        ['sonic_temperature', 'air_temperature', 'RH', 'VPD'],
        ['wind_speed', 'max_wind_speed', 'wind_dir'],
        ['u_rot', 'v_rot', 'w_rot']
    ]

    # Create PDF
    with PdfPages(output_pdf) as pdf:
        # Pair plots
        for var_group in pair_plot_vars:
            plt.figure(figsize=(10, 8))
            sns.pairplot(df[var_group])
            plt.suptitle(f'Pair Plot: {", ".join(var_group)}')
            pdf.savefig()
            plt.close()

        # Overlay time series plots
        for var_group in time_series_vars:
            plt.figure(figsize=(14, 7))
            for var in var_group:
                if var in df.columns:
                    plt.plot(df['datetime'], df[var], label=var)
            plt.legend()
            plt.title(f'Time Series: {", ".join(var_group)}')
            plt.xlabel('Time')
            plt.ylabel('Value')
            pdf.savefig()
            plt.close()

        # Panel plots for related variables
        panel_plot_groups = [
            ['Tau', 'qc_Tau', 'H', 'qc_H'],
            ['LE', 'qc_LE', 'co2_flux', 'qc_co2_flux'],
            ['h2o_flux', 'qc_h2o_flux', 'co2_v-adv', 'h2o_v-adv'],
            ['air_temperature', 'air_pressure', 'air_density', 'air_heat_capacity']
        ]

        for var_group in panel_plot_groups:
            fig, axs = plt.subplots(len(var_group), 1, figsize=(10, 15))
            for ax, var in zip(axs, var_group):
                if var in df.columns:
                    ax.plot(df['datetime'], df[var])
                    ax.set_title(var)
                    ax.set_xlabel('Time')
                    ax.set_ylabel(f'{var} ({ds[var].attrs.get("units", "")})')
            plt.tight_layout()
            pdf.savefig()
            plt.close()


file_paths = glob.glob('/Users/bhupendra/projects/crocus/data/flux_data/data/summaries/2024-07-*EP-Summary.txt')
file_paths.sort()
# Read each file into an xarray Dataset
datasets = [read_smartflux_summary(file_path) for file_path in file_paths]

# Concatenate all datasets along the time dimension
ds = xr.concat(datasets, dim='datetime')

ds = drop_all_nan_columns(ds)

create_plots(ds, '/Users/bhupendra/projects/crocus/output/plots/smartflux_summary_july2024.pdf')



