import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import seaborn as sns

# Define the output directory
output_dir = "/Users/bhupendra/projects/crocus/output/plots/flux_plots/CO2-SensHeat/"

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Path pattern to include all files from the year 2024
file_pattern = "/Users/bhupendra/projects/crocus/data/flux_data/data/netcdf/resnc/2024??/*.nc"

# Load all the 2024 datasets using xarray and combine them by time
ds = xr.open_mfdataset(file_pattern, combine='by_coords')

# Convert time to pd.DatetimeIndex
time = pd.to_datetime(ds['time'].values, unit='s')

# Create a pandas DataFrame with selected variables
df = pd.DataFrame({
    'time': time,
    'H': ds['H'].values,  # Sensible Heat Flux
    'CO2_flux': ds['co2_flux'].values,  # CO2 Flux
    'wind_speed': ds['wind_speed'].values,  # Wind Speed
    'wind_dir': ds['wind_dir'].values  # Wind Direction
})

# Convert time from UTC to CST (America/Chicago)
df['time'] = time.tz_localize('UTC').tz_convert('America/Chicago')

# Extract time of day (ignoring date)
df['time_of_day'] = df['time'].dt.strftime('%H:%M')

# Group by time of day and calculate mean for each time of the day across all days
diurnal_means = df.groupby('time_of_day').mean()

# Sort by time of day for proper ordering
diurnal_means = diurnal_means.sort_index()

# Retrieve units from the dataset
units = {
    'H': ds['H'].units if 'units' in ds['H'].attrs else '',
    'CO2_flux': ds['co2_flux'].units if 'units' in ds['co2_flux'].attrs else '',
    'wind_speed': ds['wind_speed'].units if 'units' in ds['wind_speed'].attrs else 'm/s',
    'wind_dir': ds['wind_dir'].units if 'units' in ds['wind_dir'].attrs else 'degrees'
}

# Plot Diurnal Means
fig, axes = diurnal_means[['H', 'CO2_flux', 'wind_speed', 'wind_dir']].plot(
    subplots=True,  # Create subplots for each column
    layout=(2, 2),  # Arrange plots in 2 rows and 2 columns
    figsize=(15, 10),  # Set figure size
    grid=True,  # Enable grid for readability
    sharex=True  # Share x-axis for all subplots
)

# Flatten axes if it's a 2D array, else keep it as is
if isinstance(axes, np.ndarray) and axes.ndim == 2:
    axes = axes.flatten()

# Set titles and y-axis labels using units from the dataset
axes[0].set_title('Sensible Heat Flux (H)')
axes[0].set_ylabel(f'Sensible Heat ({units["H"]})')

axes[1].set_title('CO2 Flux')
axes[1].set_ylabel(f'CO2 Flux ({units["CO2_flux"]})')

if len(axes) > 2:
    axes[2].set_title('Wind Speed')
    axes[2].set_ylabel(f'Wind Speed ({units["wind_speed"]})')

if len(axes) > 3:
    axes[3].set_title('Wind Direction')
    axes[3].set_ylabel(f'Wind Direction ({units["wind_dir"]})')

# Set x-axis labels for time of day and rotate them for better readability
for ax in axes:
    ax.set_xlabel('Locale Time of Day')
    ax.set_xticks(range(0, len(diurnal_means.index), 4))  # Set ticks at regular intervals
    ax.set_xticklabels(diurnal_means.index[::4], rotation=45)  # Rotate x-tick labels

plt.tight_layout()
plt.subplots_adjust(top=0.92)
plt.suptitle('Diurnal Mean for Sensible Heat, CO2 Flux, Wind Speed, and Wind Direction (2024)')

# Save the plot to a PDF file
plt.savefig(os.path.join(output_dir, 'diurnal_means.pdf'))
plt.close()

# Diurnal Means with Standard Deviation Plot
diurnal_stats = df.groupby('time_of_day').agg(['mean', 'std']).sort_index()

fig, axes = plt.subplots(2, 2, figsize=(11, 10))
fig.suptitle('Diurnal Means with Standard Deviation (2024)', fontsize=16)

variables = ['H', 'CO2_flux', 'wind_speed', 'wind_dir']
titles = ['Sensible Heat Flux (H)', 'CO2 Flux', 'Wind Speed', 'Wind Direction']

for i, ax in enumerate(axes.flatten()):
    var = variables[i]
    ax.plot(diurnal_stats.index, diurnal_stats[var]['mean'], label='Mean')
    ax.fill_between(diurnal_stats.index, 
                    diurnal_stats[var]['mean'] - diurnal_stats[var]['std'], 
                    diurnal_stats[var]['mean'] + diurnal_stats[var]['std'], 
                    alpha=0.3, label='Std Dev')
    ax.set_title(titles[i])
    ax.set_xlabel('Time of Day')
    ax.set_ylabel(f'{titles[i]} ({units[var]})')
    ax.set_xticks(range(0, len(diurnal_stats.index), 4))
    ax.set_xticklabels(diurnal_stats.index[::4], rotation=45)
    ax.grid(True)
    ax.legend()

plt.tight_layout()
plt.subplots_adjust(top=0.9)
plt.savefig(os.path.join(output_dir, 'diurnal_means_with_std.pdf'))
plt.close()

# Percentile Plot for Diurnal Fluxes
percentiles = [10, 50, 90]
diurnal_percentiles = df.groupby('time_of_day').quantile([p / 100 for p in percentiles]).unstack(level=-1).sort_index()

fig, axes = plt.subplots(2, 2, figsize=(15, 10))
fig.suptitle('Percentile Plots for Diurnal Fluxes (2024)', fontsize=16)

for i, ax in enumerate(axes.flatten()):
    var = variables[i]
    for p in percentiles:
        ax.plot(diurnal_percentiles.index, diurnal_percentiles[(var, p / 100)], label=f'{p}th Percentile')
    ax.set_title(titles[i])
    ax.set_xlabel('Time of Day')
    ax.set_ylabel(f'{titles[i]} ({units[var]})')
    ax.set_xticks(range(0, len(diurnal_percentiles.index), 4))
    ax.set_xticklabels(diurnal_percentiles.index[::4], rotation=45)
    ax.grid(True)
    ax.legend()

plt.tight_layout()
plt.subplots_adjust(top=0.9)
plt.savefig(os.path.join(output_dir, 'percentile_plots.pdf'))
plt.close()

# Polar Plot for Wind Direction and Fluxes
fig, axes = plt.subplots(1, 2, figsize=(15, 6), subplot_kw={'projection': 'polar'})
fig.suptitle('Fluxes as a Function of Wind Direction (JAS 2024)', fontsize=16)

flux_vars = ['H', 'CO2_flux']
flux_titles = ['Sensible Heat Flux', 'CO2 Flux']

# Convert wind direction from degrees to radians
df['wind_dir_rad'] = np.deg2rad(df['wind_dir'])

# Define limits for the flux values to control outliers
flux_limits = {
    'H': (-100, 400),           # Example limit for sensible heat flux
    'CO2_flux': (-100, 100)      # Example limit for CO2 flux
}

# Create a colormap and normalize for the color scale based on time of day
norm = plt.Normalize(0, 23)
sm = plt.cm.ScalarMappable(cmap='viridis', norm=norm)
sm.set_array([])

for i, ax in enumerate(axes):
    var = flux_vars[i]
    
    # Clip the flux values to the defined limits to control outliers
    lower, upper = flux_limits[var]
    clipped_data = df[var].clip(lower=lower, upper=upper)
    
    # Use seaborn scatter plot on a polar axis
    sc = ax.scatter(df['wind_dir_rad'], clipped_data, 
                    c=pd.to_datetime(df['time_of_day'], format='%H:%M').dt.hour, 
                    cmap='viridis', alpha=0.7, edgecolor='none', s=20, norm=norm)
    
    ax.set_title(f'{flux_titles[i]} vs Wind Direction')
    ax.set_ylabel(f'{flux_titles[i]} ({units[var]})')  # Add units from the dataset
    ax.set_theta_zero_location('N')  # Set 0 degrees (north) at the top
    ax.set_theta_direction(-1)  # Set the direction to be clockwise
    ax.grid(True)

# Add a colorbar for the time of day at the bottom
cbar = fig.colorbar(sm, ax=axes, orientation='horizontal', fraction=0.05, pad=0.2)
cbar.set_label('LT (Hour)')

# Adjust layout to make space for colorbar and prevent overlap
plt.tight_layout(rect=[0, 0.15, 1, 0.95])
plt.subplots_adjust(top=0.85)
plt.savefig(os.path.join(output_dir, 'polar_plots.pdf'))
plt.close()


# Q-Q Plot for Weekday vs Weekend
df['weekday'] = df['time'].dt.weekday
df['is_weekend'] = df['weekday'] >= 5

weekend_quantiles = df[df['is_weekend']].groupby('time_of_day').quantile(0.5)
weekday_quantiles = df[~df['is_weekend']].groupby('time_of_day').quantile(0.5)

fig, axes = plt.subplots(1, 2, figsize=(15, 6))
fig.suptitle('Weekday vs Weekend Differences (JAS 2024)', fontsize=16)

for i, ax in enumerate(axes):
    var = flux_vars[i]
    hours_of_day = pd.to_datetime(weekday_quantiles.index, format='%H:%M').hour
    ax.plot(hours_of_day, weekday_quantiles[var], marker='o', color='blue', label='Weekday', linestyle='-', alpha=0.7)
    ax.plot(hours_of_day, weekend_quantiles[var], marker='x', color='orange', label='Weekend', linestyle='-', alpha=0.7)
    ax.set_title(f'{flux_titles[i]}')
    ax.set_xlabel(f'Time of the Day')
    ax.set_ylabel(f'{flux_titles[i]} ({units[var]})')
    ax.grid(True)
    ax.legend()

plt.tight_layout(rect=[0, 0.15, 1, 0.95])
plt.subplots_adjust(top=0.85)
plt.savefig(os.path.join(output_dir, 'qq_plots.pdf'))
plt.close()
