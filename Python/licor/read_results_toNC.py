#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 14:58:26 2024

@author: bhupendra
"""

import os
import glob
import zipfile
import shutil
import re
import pandas as pd
import numpy as np
from datetime import datetime
import xarray as xr

import argparse


# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("process_log.log", mode="w"),
    ],
)


# this dict was the most critical for getting this data into netcdf.
# The names and units are changes to adapt to netcdf requirements.
# https://www.licor.com/env/support/EddyPro/topics/output-files-full-output.html
var_metadata = {
    "filename": {
        "long_name": "file name",
        "units": "NA",
        "description": "Name of the file",
    },
    "date": {
        "long_name": "date",
        "units": "NA",
        "description": "Date of the measurement",
    },
    "time": {
        "long_name": "time",
        "units": "NA",
        "description": "Time of the measurement",
    },
    "DOY": {
        "long_name": "day of year",
        "units": "ddd.ddd",
        "description": "Day of the year",
    },
    "daytime": {
        "long_name": "daytime flag",
        "units": "1=daytime",
        "description": "Daytime flag (1=daytime)",
    },
    "file_records": {
        "long_name": "file records",
        "units": "#",
        "description": "Number of records in the file",
    },
    "used_records": {
        "long_name": "used records",
        "units": "#",
        "description": "Number of records used",
    },
    "Tau": {
        "long_name": "momentum flux",
        "units": "kg m-1s-2",
        "description": "Momentum flux",
    },
    "qc_Tau": {
        "long_name": "quality control momentum flux",
        "units": "#",
        "description": "Quality control for momentum flux",
    },
    "rand_err_Tau": {
        "long_name": "random error momentum flux",
        "units": "kg m-1s-2",
        "description": "Random error in momentum flux",
    },
    "H": {
        "long_name": "sensible heat flux",
        "units": "W m-2",
        "description": "Sensible heat flux",
    },
    "qc_H": {
        "long_name": "quality control sensible heat flux",
        "units": "#",
        "description": "Quality control for sensible heat flux",
    },
    "rand_err_H": {
        "long_name": "random error sensible heat flux",
        "units": "W m-2",
        "description": "Random error in sensible heat flux",
    },
    "LE": {
        "long_name": "latent heat flux",
        "units": "W m-2",
        "description": "Latent heat flux",
    },
    "qc_LE": {
        "long_name": "quality control latent heat flux",
        "units": "#",
        "description": "Quality control for latent heat flux",
    },
    "rand_err_LE": {
        "long_name": "random error latent heat flux",
        "units": "W m-2",
        "description": "Random error in latent heat flux",
    },
    "co2_flux": {
        "long_name": "CO2 flux",
        "units": "µmol s-1m-2",
        "description": "CO2 flux",
    },
    "qc_co2_flux": {
        "long_name": "quality control CO2 flux",
        "units": "#",
        "description": "Quality control for CO2 flux",
    },
    "rand_err_co2_flux": {
        "long_name": "random error CO2 flux",
        "units": "µmol s-1m-2",
        "description": "Random error in CO2 flux",
    },
    "h2o_flux": {
        "long_name": "H2O flux",
        "units": "mmol s-1m-2",
        "description": "H2O flux",
    },
    "qc_h2o_flux": {
        "long_name": "quality control H2O flux",
        "units": "#",
        "description": "Quality control for H2O flux",
    },
    "rand_err_h2o_flux": {
        "long_name": "random error H2O flux",
        "units": "mmol s-1m-2",
        "description": "Random error in H2O flux",
    },
    "ch4_flux": {
        "long_name": "CH4 flux",
        "units": "µmol s-1m-2",
        "description": "CH4 flux",
    },
    "qc_ch4_flux": {
        "long_name": "quality control CH4 flux",
        "units": "#",
        "description": "Quality control for CH4 flux",
    },
    "rand_err_ch4_flux": {
        "long_name": "random error CH4 flux",
        "units": "µmol s-1m-2",
        "description": "Random error in CH4 flux",
    },
    "none_flux": {
        "long_name": "none flux",
        "units": "µmol s-1m-2",
        "description": "None flux",
    },
    "qc_none_flux": {
        "long_name": "quality control none flux",
        "units": "#",
        "description": "Quality control for None flux",
    },
    "rand_err_none_flux": {
        "long_name": "random error none flux",
        "units": "µmol s-1m-2",
        "description": "Random error in None flux",
    },
    "H_strg": {
        "long_name": "sensible heat storage",
        "units": "W m-2",
        "description": "Sensible heat storage",
    },
    "LE_strg": {
        "long_name": "latent heat storage",
        "units": "W m-2",
        "description": "Latent heat storage",
    },
    "co2_strg": {
        "long_name": "CO2 storage",
        "units": "µmol s-1m-2",
        "description": "CO2 storage",
    },
    "h2o_strg": {
        "long_name": "H2O storage",
        "units": "mmol s-1m-2",
        "description": "H2O storage",
    },
    "ch4_strg": {
        "long_name": "CH4 storage",
        "units": "µmol s-1m-2",
        "description": "CH4 storage",
    },
    "none_strg": {
        "long_name": "none storage",
        "units": "µmol s-1m-2",
        "description": "None storage",
    },
    "co2_vadv": {
        "long_name": "CO2 vertical advection",
        "units": "µmol s-1m-2",
        "description": "CO2 vertical advection",
    },
    "h2o_vadv": {
        "long_name": "H2O vertical advection",
        "units": "mmol s-1m-2",
        "description": "H2O vertical advection",
    },
    "ch4_vadv": {
        "long_name": "CH4 vertical advection",
        "units": "µmol s-1m-2",
        "description": "CH4 vertical advection",
    },
    "none_vadv": {
        "long_name": "none vertical advection",
        "units": "µmol s-1m-2",
        "description": "None vertical advection",
    },
    "co2_molar_density": {
        "long_name": "CO2 molar density",
        "units": "mmol m-3",
        "description": "CO2 molar density",
    },
    "co2_mole_fraction": {
        "long_name": "CO2 mole fraction",
        "units": "µmol mol_a-1",
        "description": "CO2 mole fraction",
    },
    "co2_mixing_ratio": {
        "long_name": "CO2 mixing ratio",
        "units": "µmol mol_d-1",
        "description": "CO2 mixing ratio",
    },
    "co2_time_lag": {
        "long_name": "CO2 time lag",
        "units": "s",
        "description": "CO2 time lag",
    },
    "co2_def_timelag": {
        "long_name": "CO2 default time lag",
        "units": "1=default",
        "description": "CO2 default time lag flag",
    },
    "h2o_molar_density": {
        "long_name": "H2O molar density",
        "units": "mmol m-3",
        "description": "H2O molar density",
    },
    "h2o_mole_fraction": {
        "long_name": "H2O mole fraction",
        "units": "mmol mol_a-1",
        "description": "H2O mole fraction",
    },
    "h2o_mixing_ratio": {
        "long_name": "H2O mixing ratio",
        "units": "mmol mol_d-1",
        "description": "H2O mixing ratio",
    },
    "h2o_time_lag": {
        "long_name": "H2O time lag",
        "units": "s",
        "description": "H2O time lag",
    },
    "h2o_def_timelag": {
        "long_name": "H2O default time lag",
        "units": "1=default",
        "description": "H2O default time lag flag",
    },
    "ch4_molar_density": {
        "long_name": "CH4 molar density",
        "units": "mmol m-3",
        "description": "CH4 molar density",
    },
    "ch4_mole_fraction": {
        "long_name": "CH4 mole fraction",
        "units": "µmol mol_a-1",
        "description": "CH4 mole fraction",
    },
    "ch4_mixing_ratio": {
        "long_name": "CH4 mixing ratio",
        "units": "µmol mol_d-1",
        "description": "CH4 mixing ratio",
    },
    "ch4_time_lag": {
        "long_name": "CH4 time lag",
        "units": "s",
        "description": "CH4 time lag",
    },
    "ch4_def_timelag": {
        "long_name": "CH4 default time lag",
        "units": "1=default",
        "description": "CH4 default time lag flag",
    },
    "none_molar_density": {
        "long_name": "none molar density",
        "units": "mmol m-3",
        "description": "None molar density",
    },
    "none_mole_fraction": {
        "long_name": "none mole fraction",
        "units": "µmol mol_a-1",
        "description": "None mole fraction",
    },
    "none_mixing_ratio": {
        "long_name": "none mixing ratio",
        "units": "µmol mol_d-1",
        "description": "None mixing ratio",
    },
    "none_time_lag": {
        "long_name": "none time lag",
        "units": "s",
        "description": "None time lag",
    },
    "none_def_timelag": {
        "long_name": "none default time lag",
        "units": "1=default",
        "description": "None default time lag flag",
    },
    "sonic_temperature": {
        "long_name": "sonic temperature",
        "units": "K",
        "description": "Sonic temperature",
    },
    "air_temperature": {
        "long_name": "air temperature",
        "units": "K",
        "description": "Air temperature",
    },
    "air_pressure": {
        "long_name": "air pressure",
        "units": "Pa",
        "description": "Air pressure",
    },
    "air_density": {
        "long_name": "air density",
        "units": "kg m-3",
        "description": "Air density",
    },
    "air_heat_capacity": {
        "long_name": "air heat capacity",
        "units": "J kg-1K-1",
        "description": "Air heat capacity",
    },
    "air_molar_volume": {
        "long_name": "air molar volume",
        "units": "m+3mol-1",
        "description": "Air molar volume",
    },
    "ET": {
        "long_name": "evapotranspiration",
        "units": "mm hour-1",
        "description": "Evapotranspiration",
    },
    "water_vapor_density": {
        "long_name": "water vapor density",
        "units": "kg m-3",
        "description": "Water vapor density",
    },
    "e": {
        "long_name": "vapor pressure",
        "units": "Pa",
        "description": "Vapor pressure",
    },
    "es": {
        "long_name": "saturation vapor pressure",
        "units": "Pa",
        "description": "Saturation vapor pressure",
    },
    "specific_humidity": {
        "long_name": "specific humidity",
        "units": "kg kg-1",
        "description": "Specific humidity",
    },
    "RH": {
        "long_name": "relative humidity",
        "units": "%",
        "description": "Relative humidity",
    },
    "VPD": {
        "long_name": "vapor pressure deficit",
        "units": "Pa",
        "description": "Vapor pressure deficit",
    },
    "Tdew": {
        "long_name": "dew point temperature",
        "units": "K",
        "description": "Dew point temperature",
    },
    "u_unrot": {
        "long_name": "unrotated u wind component",
        "units": "m s-1",
        "description": "Unrotated u wind component",
    },
    "v_unrot": {
        "long_name": "unrotated v wind component",
        "units": "m s-1",
        "description": "Unrotated v wind component",
    },
    "w_unrot": {
        "long_name": "unrotated w wind component",
        "units": "m s-1",
        "description": "Unrotated w wind component",
    },
    "u_rot": {
        "long_name": "rotated u wind component",
        "units": "m s-1",
        "description": "Rotated u wind component",
    },
    "v_rot": {
        "long_name": "rotated v wind component",
        "units": "m s-1",
        "description": "Rotated v wind component",
    },
    "w_rot": {
        "long_name": "rotated w wind component",
        "units": "m s-1",
        "description": "Rotated w wind component",
    },
    "wind_speed": {
        "long_name": "wind speed",
        "units": "m s-1",
        "description": "Wind speed",
    },
    "max_wind_speed": {
        "long_name": "maximum wind speed",
        "units": "m s-1",
        "description": "Maximum wind speed",
    },
    "wind_dir": {
        "long_name": "wind direction",
        "units": "deg from north",
        "description": "Wind direction",
    },
    "yaw": {"long_name": "yaw", "units": "deg", "description": "Yaw"},
    "pitch": {"long_name": "pitch", "units": "deg", "description": "Pitch"},
    "roll": {"long_name": "roll", "units": "deg", "description": "Roll"},
    "u*": {
        "long_name": "friction velocity",
        "units": "m s-1",
        "description": "Friction velocity",
    },
    "TKE": {
        "long_name": "turbulent kinetic energy",
        "units": "m+2s-2",
        "description": "Turbulent kinetic energy",
    },
    "L": {"long_name": "Obukhov length", "units": "m", "description": "Obukhov length"},
    "z_d_per_L": {
        "long_name": "stability parameter",
        "units": "#",
        "description": "Stability parameter",
    },
    "bowen_ratio": {
        "long_name": "Bowen ratio",
        "units": "K",
        "description": "Bowen ratio",
    },
    "T*": {
        "long_name": "temperature scale",
        "units": "K",
        "description": "Temperature scale",
    },
    "model": {"long_name": "model", "units": "0=KJ/1=KM/2=HS", "description": "Model"},
    "x_peak": {"long_name": "x peak", "units": "m", "description": "X peak"},
    "x_offset": {"long_name": "x offset", "units": "m", "description": "X offset"},
    "x_10%": {"long_name": "x 10%", "units": "m", "description": "X 10%"},
    "x_30%": {"long_name": "x 30%", "units": "m", "description": "X 30%"},
    "x_50%": {"long_name": "x 50%", "units": "m", "description": "X 50%"},
    "x_70%": {"long_name": "x 70%", "units": "m", "description": "X 70%"},
    "x_90%": {"long_name": "x 90%", "units": "m", "description": "X 90%"},
    "un_Tau": {
        "long_name": "uncorrected momentum flux",
        "units": "kg m-1s-2",
        "description": "Uncorrected momentum flux",
    },
    "Tau_scf": {
        "long_name": "momentum flux spectral correction factor",
        "units": "#",
        "description": "Momentum flux spectral correction factor",
    },
    "un_H": {
        "long_name": "uncorrected sensible heat flux",
        "units": "W m-2",
        "description": "Uncorrected sensible heat flux",
    },
    "H_scf": {
        "long_name": "sensible heat flux spectral correction factor",
        "units": "#",
        "description": "Sensible heat flux spectral correction factor",
    },
    "un_LE": {
        "long_name": "uncorrected latent heat flux",
        "units": "W m-2",
        "description": "Uncorrected latent heat flux",
    },
    "LE_scf": {
        "long_name": "latent heat flux spectral correction factor",
        "units": "#",
        "description": "Latent heat flux spectral correction factor",
    },
    "un_co2_flux": {
        "long_name": "uncorrected CO2 flux",
        "units": "µmol s-1m-2",
        "description": "Uncorrected CO2 flux",
    },
    "co2_scf": {
        "long_name": "CO2 flux spectral correction factor",
        "units": "#",
        "description": "CO2 flux spectral correction factor",
    },
    "un_h2o_flux": {
        "long_name": "uncorrected H2O flux",
        "units": "mmol s-1m-2",
        "description": "Uncorrected H2O flux",
    },
    "h2o_scf": {
        "long_name": "H2O flux spectral correction factor",
        "units": "#",
        "description": "H2O flux spectral correction factor",
    },
    "un_ch4_flux": {
        "long_name": "uncorrected CH4 flux",
        "units": "µmol s-1m-2",
        "description": "Uncorrected CH4 flux",
    },
    "ch4_scf": {
        "long_name": "CH4 flux spectral correction factor",
        "units": "#",
        "description": "CH4 flux spectral correction factor",
    },
    "un_none_flux": {
        "long_name": "uncorrected none flux",
        "units": "µmol s-1m-2",
        "description": "Uncorrected None flux",
    },
    "none_scf": {
        "long_name": "none flux spectral correction factor",
        "units": "#",
        "description": "None flux spectral correction factor",
    },
    "spikes_hf": {
        "long_name": "spikes high frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Spikes high frequency",
    },
    "amplitude_resolution_hf": {
        "long_name": "amplitude resolution high frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Amplitude resolution high frequency",
    },
    "drop_out_hf": {
        "long_name": "drop out high frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Drop out high frequency",
    },
    "absolute_limits_hf": {
        "long_name": "absolute limits high frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Absolute limits high frequency",
    },
    "skewness_kurtosis_hf": {
        "long_name": "skewness and kurtosis high frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Skewness and kurtosis high frequency",
    },
    "skewness_kurtosis_sf": {
        "long_name": "skewness and kurtosis single frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Skewness and kurtosis single frequency",
    },
    "discontinuities_hf": {
        "long_name": "discontinuities high frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Discontinuities high frequency",
    },
    "discontinuities_sf": {
        "long_name": "discontinuities single frequency",
        "units": "8u/v/w/ts/co2/h2o/ch4/none",
        "description": "Discontinuities single frequency",
    },
    "timelag_hf": {
        "long_name": "time lag high frequency",
        "units": "8co2/h2o/ch4/none",
        "description": "Time lag high frequency",
    },
    "timelag_sf": {
        "long_name": "time lag single frequency",
        "units": "8co2/h2o/ch4/none",
        "description": "Time lag single frequency",
    },
    "attack_angle_hf": {
        "long_name": "attack angle high frequency",
        "units": "8aa",
        "description": "Attack angle high frequency",
    },
    "non_steady_wind_hf": {
        "long_name": "non-steady wind high frequency",
        "units": "8U",
        "description": "Non-steady wind high frequency",
    },
    "u_spikes": {
        "long_name": "u wind component spikes",
        "units": "#",
        "description": "U wind component spikes",
    },
    "v_spikes": {
        "long_name": "v wind component spikes",
        "units": "#",
        "description": "V wind component spikes",
    },
    "w_spikes": {
        "long_name": "w wind component spikes",
        "units": "#",
        "description": "W wind component spikes",
    },
    "ts_spikes": {
        "long_name": "sonic temperature spikes",
        "units": "#",
        "description": "Sonic temperature spikes",
    },
    "co2_spikes": {
        "long_name": "CO2 spikes",
        "units": "#",
        "description": "CO2 spikes",
    },
    "h2o_spikes": {
        "long_name": "H2O spikes",
        "units": "#",
        "description": "H2O spikes",
    },
    "ch4_spikes": {
        "long_name": "CH4 spikes",
        "units": "#",
        "description": "CH4 spikes",
    },
    "none_spikes": {
        "long_name": "none spikes",
        "units": "#",
        "description": "None spikes",
    },
    "head_detect_LI7200": {
        "long_name": "head detect LI7200",
        "units": "#",
        "description": "Head detect LI7200",
    },
    "t_out_LI7200": {
        "long_name": "temperature out LI7200",
        "units": "#",
        "description": "Temperature out LI7200",
    },
    "t_in_LI7200": {
        "long_name": "temperature in LI7200",
        "units": "#",
        "description": "Temperature in LI7200",
    },
    "aux_in_LI7200": {
        "long_name": "auxiliary in LI7200",
        "units": "#",
        "description": "Auxiliary in LI7200",
    },
    "delta_p_LI7200": {
        "long_name": "delta pressure LI7200",
        "units": "#",
        "description": "Delta pressure LI7200",
    },
    "chopper_LI7200": {
        "long_name": "chopper LI7200",
        "units": "#",
        "description": "Chopper LI7200",
    },
    "detector_LI7200": {
        "long_name": "detector LI7200",
        "units": "#",
        "description": "Detector LI7200",
    },
    "pll_LI7200": {
        "long_name": "phase-locked loop LI7200",
        "units": "#",
        "description": "Phase-locked loop LI7200",
    },
    "sync_LI7200": {
        "long_name": "synchronization LI7200",
        "units": "#",
        "description": "Synchronization LI7200",
    },
    "chopper_LI7500": {
        "long_name": "chopper LI7500",
        "units": "#",
        "description": "Chopper LI7500",
    },
    "detector_LI7500": {
        "long_name": "detector LI7500",
        "units": "#",
        "description": "Detector LI7500",
    },
    "pll_LI7500": {
        "long_name": "phase-locked loop LI7500",
        "units": "#",
        "description": "Phase-locked loop LI7500",
    },
    "sync_LI7500": {
        "long_name": "synchronization LI7500",
        "units": "#",
        "description": "Synchronization LI7500",
    },
    "not_ready_LI7700": {
        "long_name": "not ready LI7700",
        "units": "#",
        "description": "Not ready LI7700",
    },
    "no_signal_LI7700": {
        "long_name": "no signal LI7700",
        "units": "#",
        "description": "No signal LI7700",
    },
    "re_unlocked_LI7700": {
        "long_name": "re-unlocked LI7700",
        "units": "#",
        "description": "Re-unlocked LI7700",
    },
    "bad_temp_LI7700": {
        "long_name": "bad temperature LI7700",
        "units": "#",
        "description": "Bad temperature LI7700",
    },
    "laser_temp_unregulated_LI7700": {
        "long_name": "laser temperature unregulated LI7700",
        "units": "#",
        "description": "Laser temperature unregulated LI7700",
    },
    "block_temp_unregulated_LI7700": {
        "long_name": "block temperature unregulated LI7700",
        "units": "#",
        "description": "Block temperature unregulated LI7700",
    },
    "motor_spinning_LI7700": {
        "long_name": "motor spinning LI7700",
        "units": "#",
        "description": "Motor spinning LI7700",
    },
    "pump_on_LI7700": {
        "long_name": "pump on LI7700",
        "units": "#",
        "description": "Pump on LI7700",
    },
    "top_heater_on_LI7700": {
        "long_name": "top heater on LI7700",
        "units": "#",
        "description": "Top heater on LI7700",
    },
    "bottom_heater_on_LI7700": {
        "long_name": "bottom heater on LI7700",
        "units": "#",
        "description": "Bottom heater on LI7700",
    },
    "calibrating_LI7700": {
        "long_name": "calibrating LI7700",
        "units": "#",
        "description": "Calibrating LI7700",
    },
    "motor_failure_LI7700": {
        "long_name": "motor failure LI7700",
        "units": "#",
        "description": "Motor failure LI7700",
    },
    "bad_aux_tc1_LI7700": {
        "long_name": "bad auxiliary temperature channel 1 LI7700",
        "units": "#",
        "description": "Bad auxiliary temperature channel 1 LI7700",
    },
    "bad_aux_tc2_LI7700": {
        "long_name": "bad auxiliary temperature channel 2 LI7700",
        "units": "#",
        "description": "Bad auxiliary temperature channel 2 LI7700",
    },
    "bad_aux_tc3_LI7700": {
        "long_name": "bad auxiliary temperature channel 3 LI7700",
        "units": "#",
        "description": "Bad auxiliary temperature channel 3 LI7700",
    },
    "box_connected_LI7700": {
        "long_name": "box connected LI7700",
        "units": "#",
        "description": "Box connected LI7700",
    },
    "mean_value_RSSI_LI7200": {
        "long_name": "mean value RSSI LI7200",
        "units": "#",
        "description": "Mean value RSSI LI7200",
    },
    "mean_value_LI7500": {
        "long_name": "mean value LI7500",
        "units": "#",
        "description": "Mean value LI7500",
    },
    "u_var": {
        "long_name": "u component variance",
        "units": "m+2s-2",
        "description": "U component variance",
    },
    "v_var": {
        "long_name": "v component variance",
        "units": "m+2s-2",
        "description": "V component variance",
    },
    "w_var": {
        "long_name": "w component variance",
        "units": "m+2s-2",
        "description": "W component variance",
    },
    "ts_var": {
        "long_name": "sonic temperature variance",
        "units": "K+2",
        "description": "Sonic temperature variance",
    },
    "co2_var": {
        "long_name": "CO2 variance",
        "units": "--",
        "description": "CO2 variance",
    },
    "h2o_var": {
        "long_name": "H2O variance",
        "units": "--",
        "description": "H2O variance",
    },
    "ch4_var": {
        "long_name": "CH4 variance",
        "units": "--",
        "description": "CH4 variance",
    },
    "none_var": {
        "long_name": "none variance",
        "units": "--",
        "description": "None variance",
    },
    "w_per_ts_cov": {
        "long_name": "w and ts covariance",
        "units": "m s-1K ",
        "description": "W and ts covariance",
    },
    "w_per_co2_cov": {
        "long_name": "w and CO2 covariance",
        "units": "--",
        "description": "W and CO2 covariance",
    },
    "w_per_h2o_cov": {
        "long_name": "w and H2O covariance",
        "units": "--",
        "description": "W and H2O covariance",
    },
    "w_per_ch4_cov": {
        "long_name": "w and CH4 covariance",
        "units": "--",
        "description": "W and CH4 covariance",
    },
    "w_per_none_cov": {
        "long_name": "w and none covariance",
        "units": "--",
        "description": "W and none covariance",
    },
    "vin_sf_mean": {
        "long_name": "vin_sf mean",
        "units": "--",
        "description": "Vin_sf mean",
    },
    "co2_mean": {"long_name": "CO2 mean", "units": "--", "description": "CO2 mean"},
    "h2o_mean": {"long_name": "H2O mean", "units": "--", "description": "H2O mean"},
    "dew_point_mean": {
        "long_name": "dew point mean",
        "units": "--",
        "description": "Dew point mean",
    },
    "co2_signal_strength_7500_mean": {
        "long_name": "CO2 signal strength 7500 mean",
        "units": "--",
        "description": "CO2 signal strength 7500 mean",
    },
}




def read_metadata_file(metadata_file_path):
    """Reads metadata from a raw file. Tested."""
    logging.info(f"Reading metadata file {metadata_file_path}")
    desired_keys = {
        "site_name": r"site_name\s*=\s*(.+)",
        "altitude": r"altitude\s*=\s*(.+)",
        "latitude": r"latitude\s*=\s*(.+)",
        "longitude": r"longitude\s*=\s*(.+)",
        "station_name": r"station_name\s*=\s*(.+)",
        "logger_id": r"logger_id\s*=\s*(.+)",
        "acquisition_frequency": r"acquisition_frequency\s*=\s*(.+)",
        "file_duration": r"file_duration\s*=\s*(.+)",
        "instr_1_manufacturer": r"instr_1_manufacturer\s*=\s*(.+)",
        "instr_1_model": r"instr_1_model\s*=\s*(.+)",
        "instr_1_sn": r"instr_1_sn\s*=\s*(.+)",
        "instr_2_manufacturer": r"instr_2_manufacturer\s*=\s*(.+)",
        "instr_2_model": r"instr_2_model\s*=\s*(.+)",
        "instr_2_sn": r"instr_2_sn\s*=\s*(.+)",
    }

    metadata = {}

    try:
        with open(metadata_file_path, "r") as file:
            for line in file:
                for key, pattern in desired_keys.items():
                    match = re.search(pattern, line)
                    if match:
                        metadata[key] = match.group(1).strip()
    except FileNotFoundError as e:
        logging.error(f"Metadata file not found: {e}")
        return None

    logging.info(f"Extracted metadata from {metadata_file_path}")
    #print(metadata)
    #exit()
    return metadata




def extract_metadata_from_raw(root_dir, reference_datetime):
    """Extracts metadata from a .ghg file from raw dir."""
    year_month = reference_datetime.strftime("%Y/%m")
    reference_date_str = reference_datetime.strftime("%Y-%m-%d")
    raw_dir = os.path.join(root_dir, "raw", year_month)
    temp_meta_dir = os.path.join(root_dir, "temp", "temp_meta")

    if not os.path.isdir(raw_dir):
        logging.error(f"Raw directory {raw_dir} not found.")
        return None

    os.makedirs(temp_meta_dir, exist_ok=True)

    # Find the first .ghg file on the same day (ignor time)
    ghg_files = sorted(glob.glob(os.path.join(raw_dir, f"{reference_date_str}T*.ghg")))
    if ghg_files:
        ghg_file = ghg_files[0]  # Pick the first .ghg file found
        logging.info(f"Using .ghg file {ghg_file} for metadata extraction")

        try:
            # Unzip the .ghg file into the temp_meta directory
            with zipfile.ZipFile(ghg_file, 'r') as zip_ref:
                zip_ref.extractall(temp_meta_dir)
        except zipfile.BadZipFile as e:
            logging.error(f"Failed to extract {ghg_file}: {e}")
            shutil.rmtree(temp_meta_dir)
            return None

        # Look for the .metadata 
        metadata_files = sorted(glob.glob(os.path.join(temp_meta_dir, "*.metadata")))
        if metadata_files:
            metadata_file = metadata_files[0] # there should be only one 
            metadata = read_metadata_file(metadata_file)

            # Clean up
            shutil.rmtree(temp_meta_dir)
            logging.info(f"Metadata extracted successfully from {metadata_file}")
            return metadata
        else:
            logging.error(f"No .metadata file found in {ghg_file}")
            shutil.rmtree(temp_meta_dir)
            return None
    else:
        logging.error(f"No .ghg files found for the date {reference_date_str}.")
        shutil.rmtree(temp_meta_dir)
        return None



def extract_zip_files(root_dir, start_dt, end_dt, temp_csv_dir):
    """Extracts all .zip files within a given date and time range."""
    year_month = start_dt.strftime("%Y/%m")

    month_dir = os.path.join(root_dir, "results", year_month)

    if os.path.isdir(month_dir):
        # Find all .zip files
        zip_files = glob.glob(os.path.join(month_dir, "*.zip"))

        # Filter files for date range
        files_in_range = []
        for zip_file in zip_files:
            # Extract date from (assumes filename structure YYYY-MM-DDTHHMMSS)
            match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{6})", os.path.basename(zip_file))
            if match:
                file_datetime_str = match.group(1)
                file_dt = datetime.strptime(file_datetime_str, "%Y-%m-%dT%H%M%S")

                if start_dt <= file_dt <= end_dt:
                    files_in_range.append(zip_file)

        logging.info(f"Found {len(files_in_range)} files in the date range {start_dt} to {end_dt}")

        for zip_file in files_in_range:
            logging.info(f"Extracting {zip_file}")
            try:
                with zipfile.ZipFile(zip_file, "r") as zip_ref:
                    zip_ref.extractall(temp_csv_dir)
            except zipfile.BadZipFile as e:
                logging.error(f"Failed to extract {zip_file}: {e}")


def read_and_attach_headers(file_path, var_metadata):
    # Read the CSV file skipping the first three rows and without headers
    df = pd.read_csv(file_path, skiprows=[0, 1, 2], header=None)

    # Extract the variable names from the metadata dictionary
    headers = list(var_metadata.keys())

    # Attach the headers to the DataFrame
    df.columns = headers

    # Combine 'date' and 'time' columns into a single datetime column
    df["time"] = pd.to_datetime(df["date"] + " " + df["time"])

    # Convert 'time' to seconds since 1970-01-01
    df["time"] = (df["time"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

    # Drop the original 'date' column
    df = df.drop(columns=["date"])

    logging.info(f"Read and processed {file_path}")
    logging.info(f"Data time range: {df['time'].min()} to {df['time'].max()}")

    return df


def drop_missing_columns(ds):
    nan_vars = [var for var in ds.data_vars if ds[var].isnull().all()]
    ds = ds.drop_vars(nan_vars)

    return ds


def combine_csv_files(file_paths, var_metadata):
    dataframes = []

    for file_path in file_paths:
        # Read each file and attach headers
        df = read_and_attach_headers(file_path, var_metadata)
        dataframes.append(df)

    # Concatenate all DataFrames along the rows
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Sort the DataFrame by 'time'
    combined_df = combined_df.sort_values(by="time").reset_index(drop=True)

    logging.info(f"Combined DataFrame time range: {combined_df['time'].min()} to {combined_df['time'].max()}")

    return combined_df


def df_to_xarray(df, var_metadata):
    # Convert the DataFrame to an xarray
    ds = xr.Dataset.from_dataframe(df.set_index("time"))

    # Attach metadata to each variable
    for var_name in df.columns:
        if var_name in var_metadata:
            ds[var_name].attrs["long_name"] = var_metadata[var_name]["long_name"]
            ds[var_name].attrs["units"] = var_metadata[var_name]["units"]
            ds[var_name].attrs["description"] = var_metadata[var_name]["description"]

    # Set CF standard for 'time'
    ds.time.attrs["long_name"] = "time"
    ds.time.attrs["units"] = "seconds since 1970-01-01 00:00:00"
    ds.time.attrs["calendar"] = "standard"

    logging.info(f"Converted to xarray Dataset with time range: {ds['time'].min().values} to {ds['time'].max().values}")
    ds = drop_missing_columns(ds)
    return ds


def process_files(args, start_datetime, end_datetime, var_metadata):
    """Processes files and creates a NetCDF file for the specified date range."""
    root_dir = args.root_dir
    prefix = args.prefix
    temp_csv_dir = os.path.join(root_dir, "temp", "csv")
    os.makedirs(temp_csv_dir, exist_ok=True)

    extract_zip_files(root_dir, start_datetime, end_datetime, temp_csv_dir)

    csv_files = glob.glob(os.path.join(temp_csv_dir, "output", "eddypro_exp_full_output*_exp.csv"))
    if not csv_files:
        logging.error("No CSV files found for the given date range.")
        return

    combined_df = combine_csv_files(csv_files, var_metadata)
    combined_ds = df_to_xarray(combined_df, var_metadata)

    year_month_str = end_datetime.strftime("%Y%m")
    nc_dir = os.path.join(root_dir, "netcdf", "resnc", year_month_str)
    os.makedirs(nc_dir, exist_ok=True)

    # Make output file name
    start_str = start_datetime.strftime("%Y%m%dT%H%M%S")
    end_str = end_datetime.strftime("%Y%m%dT%H%M%S")
    netcdf_filename = f"{prefix}-results-{end_str}.nc"
    netcdf_filepath = os.path.join(nc_dir, netcdf_filename)

    # Remove the existing file
    if os.path.exists(netcdf_filepath):
        os.remove(netcdf_filepath)

    combined_ds.to_netcdf(netcdf_filepath, unlimited_dims=["time"])

    logging.info(f"Written combined dataset to {netcdf_filepath}")

    shutil.rmtree(temp_csv_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process raw data files for a given time range.")
    parser.add_argument(
        "--start",
        dest="start",
        type=str,
        required=True,
        help="Start date and time in the format YYYY-MM-DDTHH:MM:SS, e.g., 2024-08-01T00:00:00."
    )
    parser.add_argument(
        "--end",
        dest="end",
        type=str,
        required=True,
        help="End date and time in the format YYYY-MM-DDTHH:MM:SS, e.g., 2024-08-01T23:59:59."
    )
    parser.add_argument(
        "--root_dir",
        dest='root_dir',
        type=str,
        default="/Users/bhupendra/projects/crocus/data/flux_data/data",
        help="Root directory for data. Default '/Users/bhupendra/projects/crocus/data/flux_data/data'."
    )
    parser.add_argument(
        "--prefix",
        dest='prefix',
        type=str,
        default="crocus-uic-smartflux",
        help="Prefix for output filename."
    )


    args = parser.parse_args()

    start_datetime = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    process_files(args, start_datetime, end_datetime, var_metadata)
    
