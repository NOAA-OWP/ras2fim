#!/usr/bin/env python3

import os

import argparse
import datetime
import fnmatch
import keepachangelog
import numpy as np
import pandas as pd
import platform
# import pyproj
import pytz
import rasterio
import re
import sys

# import r2f_validators as val
sys.path.append('..')
import ras2fim.src.r2f_validators as val

from datetime import datetime as dt
from ras2fim.src.errors import ModelUnitError
from pathlib import Path




####################################################################
def confirm_models_unit(proj_crs,str_ras_path_arg):
    '''
    - calls two other functions to infer units from ras models and -p projection.
    - raises an exception if units do not match.
    - return the unit if they match
    '''

    unit=None
    try:
        unit_from_ras = model_unit_from_ras_prj(str_ras_path_arg)
        unit_from_crs = model_unit_from_crs(proj_crs)

        if unit_from_ras == unit_from_crs: #if both are the same, return one of them
            unit= unit_from_crs

        elif unit_from_ras != unit_from_crs:
            raise ModelUnitError ("Specified projection (with -p) is in '%s' but the unit specified in RAS models"
                                  " prj files is in '%s'. Check your models or -p entry and try again."
                                  %(unit_from_crs,unit_from_ras))

    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit


####################################################################
def model_unit_from_crs(proj_crs):
    '''
    get the unit( meter or feet) from -p EPSG input:
    return either 'meter' or 'feet'; Otherwise raises an error
    '''

    try:
        unit = proj_crs.axis_info[0].unit_name
        if "foot" in unit:
            unit='feet'
        elif 'metre' in unit:
            unit='meter'
        else:
            raise ModelUnitError("Make sure you have entered a correct projection code.\
                                 The projection unit must be in feet or meter.")
    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit


####################################################################
def model_unit_from_ras_prj(str_ras_path_arg):
    '''
    -- return either 'meter' or 'feet'; Otherwise raises an error
    This function read prj file of HEC-RAS models in a dataset and records the unit (either Metric or US customary):
        - raises and exception if a mixed use of Metric and US customary units encounters
        - returns a unit (either Metric or US customary) when only one unit has been used for the entire dataset.
        - The function assumes always a unit has been specified in HEC-RAS prj file. If not, a unit must be added into
        prj file and then use ras2fim.
    '''

    unit=None
    ras_prj_files = []
    if os.path.isdir(str_ras_path_arg):
        for root, dirnames, filenames in os.walk(str_ras_path_arg):
            for filename in fnmatch.filter(filenames,'*.[Pp][Rr][Jj]'):
                with open(os.path.join(root,filename)) as f:
                    first_file_line = f.readline()

                # skip projection files
                if any(x in first_file_line for x in ['PROJCS','GEOGCS','DATUM','PROJECTION']):
                    continue
                ras_prj_files.append(os.path.join(root,filename))
    elif os.path.isfile(str_ras_path_arg):
        ras_prj_files.append(str_ras_path_arg)

    units_found = []
    for ras_prj_file in ras_prj_files:
        with open(ras_prj_file) as f:
            file_contents = f.read()

        if re.search("SI Unit", file_contents, re.I):
            units_found.append("meter")
        elif re.search("English Unit", file_contents,re.I):
            units_found.append("feet")


    try:
        if len(set(units_found))==0: #if no unit specified in any of the RAS models
            raise ModelUnitError("At least one of the HEC-RAS models must have a unit specified in prj file."
                                 " Check your RAS models prj files and try again. ")

        elif len(set(units_found))==1:
            unit=units_found[0]

        elif len(set(units_found))==2 :
            raise ModelUnitError("Ras2fim only accepts HEC-RAS models with similar units (either U.S. Customary or "
                                 "International/Metric). The provided dataset uses a mix of these units. "
                                 "Verify your HEC-RAS models units and try again.")

    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit

####################################################################
# Scrapes the top changelog version (most recent version listed in our repo)
def get_changelog_version(changelog_path):
    changelog = keepachangelog.to_dict(changelog_path)
    return list(changelog.keys())[0]


####################################################################
def convert_rating_curve_to_metric(ras2rem_dir):

    src_path = os.path.join(ras2rem_dir, 'rating_curve.csv')
    df = pd.read_csv(src_path)

    # convert to metric if needed
    if 'stage_m' not in df.columns: # if no meters, then only Imperial units are in the file
        df["stage_m"] = ["{:0.2f}".format(h * 0.3048) for h in df["stage_ft"]]
        df["discharge_cms"] = ["{:0.2f}".format(q * 0.0283168) for q in df["discharge_cfs"]]
        df.to_csv(os.path.join(src_path), index=False)

        # REM will also be in Imperial units if the incoming SRC was
        rem_path = os.path.join(ras2rem_dir,'rem.tif')
        with rasterio.open(rem_path) as src:
            raster = src.read()
            raster = np.multiply(raster, np.where(raster != 65535,0.3048,1)) # keep no-data value as-is
            output_meta = src.meta.copy()
        with rasterio.open(rem_path, 'w', **output_meta, compress="LZW") as dest:
            dest.write(raster)

    return

####################################################################
def is_windows():

    plt = platform.system()
    return ("Windows" in plt)


####################################################################
def fix_proj_path_error():
    
    # delete this environment variable because the updated library we need
    # is included in the rasterio wheel
    try:
        # There is a known issue with rasterio and enviroment PROJ_LIB
        # an error of `PROJ: proj_create_from_database: Cannot find proj.db`
        # It does not stop anything it annoying. This is known hack for windows for it

        #print("os.environ['PROJ_LIB''] is " + os.environ["PROJ_LIB"])
        #if (os.environ["PROJ_LIB"]):
            #del os.environ["PROJ_LIB"]

        """
        if (os.environ["PROJ_LIB"]):
            if (is_windows()) :
                # first get the user and we have to build up a path
                user_home_path = os.path.expanduser("~")

                # TODO: There could be other paths.
                anaconda3_proj_path = os.path.join(user_home_path, r'anaconda3\envs\ras2fim\Library\share\proj')
                if (os.path.exists(anaconda3_proj_path)):
                    os.environ["PROJ_LIB"] = anaconda3_proj_path
        elif (os.environ["PROJ_LIB"]):
            del os.environ["PROJ_LIB"]
        """

    except Exception as e:
        #print(e)
        pass

##########################################################################
def find_model_unit_from_rating_curves(r2f_hecras_outputs_dir):
    all_rating_files = list(Path(r2f_hecras_outputs_dir).rglob('*rating_curve.csv'))
    try:
        sample_rating_file = pd.read_csv(all_rating_files[0])
        SecondColumnTitle = sample_rating_file.columns[1]
        pattern = r"\((.*?)\)"  # Matches text between parentheses in column 2
        stage_unit = re.search(pattern, SecondColumnTitle).group(1).strip().lower()
        if stage_unit == 'ft':
            model_unit = 'feet'
        elif stage_unit == 'm':
            model_unit = 'meter'
        else:
            raise ValueError("Rating curve values should be either in feet or meter. Check the results")

        return model_unit
    except ValueError as e:
        print("Error:", e)
        sys.exit(1)
    except:
        print("Error: Make sure you have specified a correct input directory with has at least one '*.rating curve.csv' file.")
        sys.exit(1)


####################################################################
def get_stnd_date(use_utc = False):
    
    # return YYMMDD as in 230725

    if (use_utc == True):
        # UTC Time
        str_date = dt.utcnow.strftime("%y%m%d")
    else:
        str_date = dt.now.strftime("%y%m%d")

    return str_date


####################################################################
def print_date_time_duration(start_dt, end_dt):
    '''
    Process:
    -------
    Calcuates the diffenence in time between the start and end time
    and prints is as:
    
        Duration: 4 hours 23 mins 15 secs
    
        (note: local time not UTC)
    
    -------
    Returns:
        Duration as a formatted string
        
    '''
    time_delta = (end_dt - start_dt)
    total_seconds = int(time_delta.total_seconds())

    total_days, rem_seconds = divmod(total_seconds, 60 * 60 * 24)        
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"
    
    duration_msg = "Duration: " + time_fmt
    print(duration_msg)
    
    return duration_msg


####################################################################
def get_stnd_r2f_output_folder_name(huc_number, crs):

    '''
    Inputs:
        - huc_number (str)
        - crs (str):  ie) ESPG:2277 or ESRI:107239. Note, must start with ESRI or EPSG (non case-sensitive)

    '''
    
    # returns pattern of {HUC}_{CRS_number}_{stnd date}. e.g 12090301_2277_230725

    # -------------------
    if (len(str(huc_number)) != 8):
        raise ValueError("huc number is not eight characters in length")

    if (huc_number.isnumeric() == False):
        raise ValueError("huc number is not a number")

    # -------------------
    # validate and split out the crs number.
    is_valid_crs, err_msg, crs_number = val.is_valid_crs(crs)

    if (is_valid_crs == False):
        raise ValueError(err_msg)
    
    std_date = get_stnd_date(True) # in UTC

    folder_name = f"{huc_number}_{crs_number}_{std_date}"

    return folder_name

