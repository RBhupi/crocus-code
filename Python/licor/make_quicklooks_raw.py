import argparse
import os
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

# variables in raw data
variables_to_plot = [
    "CO2_mmol_m^3",
    "H2O_mmol_m^3",
    "Temperature_C",
    "Pressure_kPa",
    "U_m_s",
    "V_m_s",
    "W_m_s"
]

def plot_timeseries(ax, time, data, title, units):
    """Plot time series data with title and units, using gray line style."""
    ax.plot(time, data, label=title, color='gray', linewidth=1.5)
    ax.set_title(f"{title} ({units})")
    ax.set_ylabel(f"{title} ({units})")
    ax.grid(True, linestyle='--', linewidth=0.5, color='grey')
    ax.xaxis.set_major_locator(MaxNLocator(6))  # Show fewer x-axis ticks
    # format the x-axis ticks as hours
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

def plot_histogram_with_stats(ax, data, title, units):
    """Plot histogram with mean, median, and standard deviation indicators."""
    mean_val = np.mean(data)
    median_val = np.median(data)
    std_val = np.std(data)
    
    sns.histplot(data, bins=20, kde=True, ax=ax, color='skyblue')
    ax.axvline(mean_val, color='red', linestyle='--', label=f"Mean: {mean_val:.2f}")
    ax.axvline(median_val, color='green', linestyle='--', label=f"Median: {median_val:.2f}")
    ax.axvline(mean_val + std_val, color='purple', linestyle=':', label=f"Mean + 1 SD: {mean_val + std_val:.2f}")
    ax.axvline(mean_val - std_val, color='purple', linestyle=':', label=f"Mean - 1 SD: {mean_val - std_val:.2f}")
    ax.set_title(f"{title} Histogram ({units})")
    ax.legend()
    ax.grid(True, linestyle='--', linewidth=0.5, color='grey')

def generate_quicklook(nc_file, output_dir, prefix="quicklook"):
    ds = xr.open_dataset(nc_file)
    base_filename = os.path.basename(nc_file).replace(".nc", "")
    output_file = os.path.join(output_dir, f"{prefix}_{base_filename}.png")
    
    # Extract date
    date_str = pd.to_datetime(ds.time.values[0]).strftime('%Y-%m-%d')
    
  
    fig, axes = plt.subplots(4, 2, figsize=(18, 15))
    fig.suptitle(f"Quick-Look for {date_str}", fontsize=16)
    axes = axes.flatten()

    #  series 
    i = 0
    for var in variables_to_plot:
        if var in ds:
            df = ds[var].to_dataframe().dropna().reset_index()
            long_name = ds[var].attrs.get("long_name", var)
            units = ds[var].attrs.get("units", "")
            plot_timeseries(
                ax=axes[i],
                time=pd.to_datetime(df['time']),  # Use time as datetime
                data=df[var],
                title=long_name,
                units=units
            )
            i += 1

    # Histogram 
    if "W_m_s" in ds.variables:
        df_w = ds["W_m_s"].to_dataframe().dropna()
        plot_histogram_with_stats(
            ax=axes[i],
            data=df_w["W_m_s"],
            title=ds["W_m_s"].attrs.get("long_name", "W_m_s"),
            units=ds["W_m_s"].attrs.get("units", "m/s")
        )

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_file, dpi=150)
    plt.close()
    print(f"Quick-look plot saved in: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate quick-look plots from processed NetCDF data.")
    parser.add_argument("--nc_file", type=str, required=True, help="Path to the NetCDF file.")
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save the quick-look plots.")
    parser.add_argument("--prefix", type=str, default="quicklook", help="Prefix for the plot files.")
    args = parser.parse_args()

    generate_quicklook(args.nc_file, args.output_dir, args.prefix)
