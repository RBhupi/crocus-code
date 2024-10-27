import argparse
import os
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from windrose import WindroseAxes
from matplotlib.colors import ListedColormap
import seaborn as sns
import pandas as pd

qc_cmap = ListedColormap(['green', 'yellow', 'red'])

def generate_quicklook(nc_file, output_dir, prefix="quicklook"):
    ds = xr.open_dataset(nc_file)
    base_filename = os.path.basename(nc_file).replace(".nc", "")
    output_file = os.path.join(output_dir, f"{prefix}_{base_filename}.png")
    
    flux_vars_with_qc = {
        "H": "qc_H",
        "LE": "qc_LE",
        "co2_flux": "qc_co2_flux",
        "h2o_flux": "qc_h2o_flux",
        "Tau": "qc_Tau"
    }
    meteorological_vars = ["air_temperature", "wind_speed", "wind_dir", "RH", "VPD"]

    total_plots = len(flux_vars_with_qc) + len(meteorological_vars) + 2
    rows = (total_plots + 1) // 2
    fig, axes = plt.subplots(rows, 2, figsize=(16, 4 * rows))
    fig.suptitle(f'Quick-Look for {pd.to_datetime(ds["time"].values[0]).strftime("%Y-%m-%d")}', fontsize=12)
    axes = axes.flatten()
    
    i = 0
    for var, qc_var in flux_vars_with_qc.items():
        if var in ds:
            df = ds[[var, qc_var]].to_dataframe().dropna().reset_index()
            df['time'] = pd.to_datetime(df['time']).dt.strftime('%H:%M:%S')
            axes[i].scatter(df['time'], df[var], c=df[qc_var], cmap=qc_cmap, s=50, vmin=0, vmax=2)
            axes[i].plot(df['time'], df[var], color='gray', alpha=0.5)
            axes[i].set_title(f"{ds[var].attrs.get('long_name', var)} [{ds[var].attrs.get('units', '')}]")
            axes[i].grid(True, linestyle='--', alpha=0.5)
            axes[i].xaxis.set_major_locator(MaxNLocator(6))  # Set max 6 ticks on x-axis
            i += 1

    for var in meteorological_vars:
        if var in ds:
            df = ds[[var]].to_dataframe().dropna().reset_index()
            df['time'] = pd.to_datetime(df['time']).dt.strftime('%H:%M:%S')
            axes[i].plot(df['time'], df[var], color='grey', alpha=0.7)
            axes[i].set_title(f"{ds[var].attrs.get('long_name', var)} [{ds[var].attrs.get('units', '')}]")
            axes[i].grid(True, linestyle='--', alpha=0.5)
            axes[i].xaxis.set_major_locator(MaxNLocator(6))  # Set max 6 ticks on x-axis
            i += 1

    if "wind_speed" in ds and "wind_dir" in ds:
        df_wind = ds[["wind_speed", "wind_dir"]].to_dataframe().dropna()
        ax_wind = WindroseAxes.from_ax(fig=fig, rect=[0.7, -0.03, 0.25, 0.25])
        cmap = plt.get_cmap('viridis')  # Get colormap object, i got error without this
        ax_wind.bar(df_wind['wind_dir'], df_wind['wind_speed'], normed=True, opening=0.8, edgecolor='white', cmap=cmap)

    if "w_rot" in ds:
        df_w_rot = ds["w_rot"].to_dataframe().dropna()
        sns.histplot(df_w_rot["w_rot"], bins=10, kde=True, ax=axes[i], color='skyblue')
        axes[i].set_title('Vertical Wind Component (w_rot)')
        axes[i].grid(True, linestyle='--', alpha=0.5)
    
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(output_file, dpi=150)
    plt.close()
    print(f"Quick-look plot saved in: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate quick-look plots from NetCDF files.")
    parser.add_argument("--nc_file", type=str, required=True, help="Path to the NetCDF file for the day.")
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save the quick-look plots.")
    parser.add_argument("--prefix", type=str, default="quicklook", help="Prefix for the plot files.")
    args = parser.parse_args()

    generate_quicklook(args.nc_file, args.output_dir, args.prefix)
