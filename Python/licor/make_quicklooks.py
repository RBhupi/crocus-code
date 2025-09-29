import argparse
import os
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from windrose import WindroseAxes

def plot_ts(ds, variables, output_dir, output_file):
    """
    Plots the time series in panel plot, includes basic stat.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


    fig, axes = plt.subplots(6, 2, figsize=(15, 20))
    axes = axes.flatten()
    fig.suptitle('CMS-FLX-001-UIC Tower (LI-COR METEK)', fontsize=16)


    for i, var in enumerate(variables):
        if var in ds.variables:
            df = ds[var].to_dataframe().reset_index()
            df = df.dropna()


            if 'time' in df.columns:
                ax = axes[i]
                sns.lineplot(x='time', y=var, data=df, ax=ax, color='black')


                long_name = ds[var].attrs.get('long_name', var)
                units = ds[var].attrs.get('units', '')

                ax.set_title(f'{long_name} [{units}]')
                ax.set_xlabel('Time')
                ax.set_ylabel(f'{long_name} [{units}]')
                ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='grey')

                # Calculate statistics
                mean = df[var].mean()
                median = df[var].median()
                std = df[var].std()

                # Annotate the plot with statistics
                ax.axhline(mean, color='red', linestyle='dotted', linewidth=1, label=f'Mean: {mean:.2f}')
                ax.axhline(mean + std, color='blue', linestyle='dotted', linewidth=1, label=f'Std: {std:.2f}')
                ax.axhline(mean - std, color='blue', linestyle='dotted', linewidth=1)
                ax.legend()
            else:
                print(f"Variable '{var}' does not have a time dimension.")

    plt.tight_layout()

    plt.savefig(output_file)
    plt.close()




def main():
    parser = argparse.ArgumentParser(description="Generate quick-look plots from NetCDF files.")
    parser.add_argument(
        "--nc_file",
        dest="nc_file",
        type=str,
        required=True,
        help="Path to the NetCDF file for the day."
    )
    parser.add_argument(
        "--output_dir",
        dest="output_dir",
        type=str,
        required=True,
        help="Directory to save the quick-look plots."
    )
    parser.add_argument(
        "--prefix",
        dest="prefix",
        type=str,
        default="ts",
        help="Prefix for the plot files."
    )

    args = parser.parse_args()

    # Load the NetCDF file
    ds = xr.open_dataset(args.nc_file)

    # Define variables to plot in the timeseries
    variables_to_plot = [
        "TKE", "Tdew", "specific_humidity", "LE", "h2o_flux", 
        "H", "co2_flux", "bowen_ratio", "air_temperature", "L",
        'wind_dir', 'wind_speed'
    ]

    # Output filenames
    base_filename = os.path.basename(args.nc_file).replace(".nc", "")
    ts_output_file = os.path.join(args.output_dir, f"{args.prefix}-{base_filename}.png")

    plot_ts(ds, variables_to_plot, output_dir=args.output_dir, output_file=ts_output_file)

    print(f"Time series plot saved to: {ts_output_file}")

if __name__ == "__main__":
    main()
