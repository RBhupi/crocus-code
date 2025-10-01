#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process and plot CL61 Lidar NetCDF files using ACT.
"""

import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import glob
import os
import act

import argparse

#%%
def load_data(file_pattern):
    """
    Load data using glob pattern and return xarray dataset.
    """
    file_paths = glob.glob(file_pattern)
    if not file_paths:
        raise FileNotFoundError(f"No files matched: {file_pattern}")
    ds = xr.open_mfdataset(file_paths, combine='by_coords', parallel=True, chunks={"time": 100})
    return ds


def correct_data(ds):
    """
    Apply ACT corrections to key lidar variables.
    """
    variables = ['beta_att', 'p_pol', 'x_pol']
    for var in variables:
        if var != 'linear_depol_ratio':
            ds = act.corrections.correct_ceil(ds, var_name=var)
    return ds


def preprocess_data(ds):
    """
    Add km versions of range and cloud heights, swap dims.
    """
    ds = ds.assign(range_km=ds['range'] / 1000)
    ds = ds.assign(sky_condition_cloud_layer_heights_km=ds['sky_condition_cloud_layer_heights'] / 1000)
    ds['range_km'].attrs['units'] = 'km'
    ds = ds.swap_dims({'range': 'range_km'})
    return ds


def plot_cloud_heights(ax, ds, color='black'):
    """
    Plots cloud layer heights as scatter markers.
    """
    if 'sky_condition_cloud_layer_heights_km' not in ds:
        return

    cloud_heights = ds['sky_condition_cloud_layer_heights_km'].values
    times = ds['time'].values

    if len(cloud_heights.shape) == 2:
        times = np.repeat(times, cloud_heights.shape[1])
        heights = cloud_heights.flatten()
    else:
        heights = cloud_heights
        times = np.tile(times, 1)

    mask = ~np.isnan(heights)
    ax.scatter(
        times[mask],
        heights[mask],
        marker=1,
        c=color,
        s=5,
        linewidths=1
    )


def plot_data(ds, output_file=None):
    """
    Create 4-panel plot from dataset and optionally save to file.
    """
    fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(12, 12), sharex=True)
    ylim = (0, 8)
    date_title = f"CROCUS CL61 {np.datetime_as_string(ds['time'].values[100], unit='D')}"
    fig.suptitle(date_title, fontsize=16)

    ds['beta_att'].plot(ax=axes[0], x='time', y='range_km', cmap='YlGnBu', robust=True)
    plot_cloud_heights(axes[0], ds, color='deeppink')
    axes[0].set_title('Attenuated Volume Backscatter Coefficient')
    axes[0].set_xlabel('')
    axes[0].set_ylim(ylim)

    ds['p_pol'].plot(ax=axes[1], x='time', y='range_km', cmap='Carbone42', robust=True, vmin=-8, vmax=8)
    axes[1].set_title('Parallel-Polarized Component of Backscattered Light')
    axes[1].set_xlabel('')
    axes[1].set_ylim(ylim)

    ds['x_pol'].plot(ax=axes[2], x='time', y='range_km', cmap='Carbone42', robust=True, vmin=-8, vmax=8)
    axes[2].set_title('Cross-Polarized Component of Backscattered Light')
    axes[2].set_xlabel('')
    axes[2].set_ylim(ylim)

    ds['linear_depol_ratio'].plot(ax=axes[3], x='time', y='range_km', cmap='PuBuGn',
                                  vmin=0, vmax=0.7, robust=True)
    plot_cloud_heights(axes[3], ds, color='deeppink')
    axes[3].set_title('Linear Depolarization Ratio of Backscatter Volume')
    axes[3].set_ylim(ylim)

    plt.tight_layout()
    if output_file:
        plt.savefig(output_file)
        print(f"Saved plot to: {output_file}")
    else:
        plt.show()
    plt.close(fig)


def process_files(files, output_dir):
    """
    Loop through all matching files and generate plots.
    """

    for file in files:
        print(f"Processing {file}")
        ds = xr.open_dataset(file)
        ds = correct_data(ds)
        ds = preprocess_data(ds)

        # Create output filename
        time_str = np.datetime_as_string(ds['time'].values[0], unit='m').replace(':', '')
        base_name = os.path.basename(file).replace('.nc', f'_{time_str}.png')
        output_path = os.path.join(output_dir, base_name)

        plot_data(ds, output_file=output_path)

# %%

# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CL61 plots.")
    parser.add_argument(
        '--input_dir',
        type=str,
        required = True,
        help='input dir for NetCDF .'
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        required=True,
        help='Directory output'
    )
    args = parser.parse_args()
    
    input_pattern = '/Users/bhupendra/projects/crocus/data/cl61/cl61-correct-time-daily-test-delete/*.nc'
    output_dir = '/Users/bhupendra/projects/crocus/plots/cl61/dailiy-test-delete'
    os.makedirs(output_dir, exist_ok=True)
    files = glob.glob(input_pattern+"*.nc")
    process_files(files, output_dir)
