import os
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from windrose import WindroseAxes

variable_groups = {
    'corrected_fluxes': [
        'Tau', 'H', 'LE', 'co2_flux', 'h2o_flux'
    ],
    'storage_fluxes': [
        'H_strg', 'LE_strg', 'co2_strg', 'h2o_strg', 'ch4_strg', 'none_strg'
    ],
    'vertical_advection_fluxes': [
        'co2_vadv', 'h2o_vadv'
    ],
    'turbulence': [
        'u*', 'TKE', 'L', 'z_d_per_L', 'bowen_ratio', 'T*'
    ],
    'uncorrected_fluxes_and_spectral_correction_factors_scf': [
        'un_Tau', 'Tau_scf', 'un_H', 'H_scf', 'un_LE', 'LE_scf',
        'un_co2_flux', 'co2_scf', 'un_h2o_flux', 'h2o_scf'
    ],
    'variances': [
        'u_var', 'v_var', 'w_var', 'ts_var', 'co2_var', 'h2o_var'
    ],
    'covariances': [
        'w_per_ts_cov', 'w_per_co2_cov', 'w_per_h2o_cov'
    ],
    'custom_variables': [
        'vin_sf_mean', 'co2_mean', 'h2o_mean', 'dew_point_mean'
    ]
}


def plot_basic_statistics(ds, variable_groups, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for group_name, variables in variable_groups.items():
        plt.figure(figsize=(12, 12))
        
        for i, var in enumerate(variables):
            if var in ds.variables:
                df = ds[var].to_dataframe().reset_index()
                df = df.dropna()

                ax = plt.subplot(len(variables), 1, i+1)
                sns.histplot(df[var], kde=True, color='skyblue', ax=ax)
                
                long_name = ds[var].attrs.get('long_name', var)
                units = ds[var].attrs.get('units', '')
                
                plt.title(f'{long_name} Distribution ({units})')
                plt.xlabel(f'{long_name} ({units})')
                plt.ylabel('Frequency')
                plt.grid(True, which='both', linestyle='--', linewidth=0.5, color='grey')

                mean = df[var].mean()
                median = df[var].median()
                std = df[var].std()

                plt.axvline(mean, color='red', linestyle='dotted', linewidth=1, label=f'Mean: {mean:.2f}')
                plt.axvline(median, color='green', linestyle='dotted', linewidth=1, label=f'Median: {median:.2f}')
                plt.axvline(mean + std, color='blue', linestyle='dotted', linewidth=1, label=f'Std: {std:.2f}')
                plt.axvline(mean - std, color='blue', linestyle='dotted', linewidth=1)

                plt.legend()
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/{group_name}_distribution.png')
        plt.close()



def plot_timeseries(ds, variable_groups, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    for group_name, variables in variable_groups.items():
        plt.figure(figsize=(18, 15))
        
        for i, var in enumerate(variables):
            if var in ds.variables:
                df = ds[var].to_dataframe().reset_index()
                df = df.dropna()

                if 'time' in df.columns:
                    plt.subplot(len(variables), 1, i+1)
                    sns.lineplot(x='time', y=var, data=df, color='black')
                    
                    long_name = ds[var].attrs.get('long_name', var)
                    units = ds[var].attrs.get('units', '')
                    
                    plt.title(f'{long_name} Time Series ({units})')
                    plt.xlabel('Time')
                    plt.ylabel(f'{long_name} ({units})')
                    plt.grid(True, which='both', linestyle='--', linewidth=0.5, color='grey')

                    mean = df[var].mean()
                    std = df[var].std()

                    plt.axhline(mean, color='red', linestyle='dotted', linewidth=1, label=f'Mean: {mean:.2f}')
                    plt.axhline(mean + std, color='blue', linestyle='dotted', linewidth=1, label=f'Std: {std:.2f}')
                    plt.axhline(mean - std, color='blue', linestyle='dotted', linewidth=1)
                    plt.legend()
                else:
                    print(f"Variable '{var}' does not have a time dimension.")
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/{group_name}_timeseries.png')
        plt.close()



def plot_windroses(ds, variable_groups, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    wind_dir = ds['wind_dir'].to_dataframe().reset_index().dropna()
    wind_dir_values = wind_dir['wind_dir'].values
    
    for group_name, variables in variable_groups.items():
        for var in variables:
            if var in ds.variables:
                df = ds[var].to_dataframe().reset_index()
                df = df.dropna()
                
                if 'wind_dir' in df.columns and var in df.columns:
                    plt.figure(figsize=(10, 8))
                    ax = WindroseAxes.from_ax()
                    ax.bar(wind_dir_values, df[var].values, normed=True, opening=0.8, edgecolor='white')
                    
                    # Access variable attributes
                    long_name = ds[var].attrs.get('long_name', var)
                    units = ds[var].attrs.get('units', '')
                    
                    ax.set_title(f'Windrose of {long_name} ({units})')
                    ax.set_legend(title="Frequency (%)", bbox_to_anchor=(1, 0, 0.5, 1))
                    
                    plt.savefig(f'{output_dir}/{group_name}_{var}_windrose.png')
                    plt.close()


ds = xr.open_dataset('/Users/bhupendra/projects/crocus/data/flux_data/data/netcdf/smartflux_data_2024_07.nc')

output_dir = '/Users/bhupendra/projects/crocus/output/plots/flux_plots/'

plot_basic_statistics(ds, variable_groups, output_dir)
plot_timeseries(ds, variable_groups, output_dir)
plot_windroses(ds, variable_groups, output_dir)

