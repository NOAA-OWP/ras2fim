#!/usr/bin/env python3

import os
import pyproj
import fnmatch
import re
from errors import ModelUnitError
import sys



def print_date_time_duration(start_dt, end_dt):
    '''
    Process:
    -------
    Calcuates the diffenence in time between the start and end time
    and prints is as:
    
        Duration: 4 hours 23 mins 15 secs
    
    -------
    Usage:
        from utils.shared_functions import FIM_Helpers as fh
        fh.print_current_date_time()
    
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

    Units_Found = []
    for ras_prj_file in ras_prj_files:
        with open(ras_prj_file) as f:
            file_contents = f.read()

        if re.search("SI Unit", file_contents, re.I):
            Units_Found.append("meter")
        elif re.search("English Unit", file_contents,re.I):
            Units_Found.append("feet")


    try:
        if len(set(Units_Found))==0: #if no unit specified in any of the RAS models
            raise ModelUnitError("At least one of the HEC-RAS models must have a unit specified in prj file."
                                 " Check your RAS models prj files and try again. ")

        elif len(set(Units_Found))==1:
            unit=Units_Found[0]

        elif len(set(Units_Found))==2 :
            raise ModelUnitError("Ras2fim excepts HEC-RAS models with similar unit (either US Customary or "
                                 "International/Metric). The provided dataset has a mix use of these units. "
                                 "Verify you HEC-RAS models units and try again. ")

    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit
