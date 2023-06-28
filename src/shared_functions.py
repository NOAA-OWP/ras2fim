#!/usr/bin/env python3

import os
import pyproj
import fnmatch
from re import search
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

def find_models_unit(proj_crs,str_ras_path_arg):
    try:
        unit_from_ras = model_unit_from_ras_prj(str_ras_path_arg)
        unit_from_crs = model_unit_from_crs(proj_crs)
        if unit_from_ras != unit_from_crs:
            raise ModelUnitError ("Given projection (-p) is not consistent with the units "
                                  "specified in RAS models prj file. Check your -p entry and try again.")

        elif unit_from_ras == unit_from_crs: #if both are the same, return one of them
            return unit_from_ras

        #if one is not found and the other found, return the found one.
        elif unit_from_ras is None and unit_from_crs is not None:
            return unit_from_crs

        elif unit_from_crs is None and unit_from_ras is not None:
            return unit_from_ras


    except ModelUnitError as e:
        print(e)
        sys.exit(1)


def model_unit_from_crs(proj_crs):
    #get the unit( meter or feet) from -p EPSG input:
    unit = None
    try:
        unit = proj_crs.axis_info[0].unit_name
        if "foot" in unit:
            unit='feet'
        elif 'metre' in unit:
            unit='meter'
        # else:
        #     raise ModelUnitError("Make sure you have entered a correct projection code.\
        #                          The projection code must be in feet or meter.")
        return unit
    except ModelUnitError as e:
        print(e)


def model_unit_from_ras_prj(str_ras_path_arg):
    '''
    Here is the method Andy used to identify Ras model units:

    b_is_geom_metric = False # default to English Units
    if os.path.exists(str_read_prj_file_path):
        with open(str_read_prj_file_path) as f_prj:
            file_contents_prj = f_prj.read()
        if re.search(r'SI Units', file_contents_prj, re.I):
            b_is_geom_metric = True

    '''
    unit=None
    matches = []
    for root, dirnames, filenames in os.walk(str_ras_path_arg):
        for filename in fnmatch.filter(filenames,'*.[Pp][Rr][Jj]'):
            with open(os.path.join(root,filename)) as f:
                first_file_line = f.read()

            # skip projection files
            if any(x in first_file_line for x in ['PROJCS','GEOGCS','DATUM','PROJECTION']):
                continue

            matches.append(os.path.join(root,filename))

    identical_array = []
    for match in matches:
        with open(match) as f:
            file_contents = f.read()

        if search("SI Units", file_contents):
            identical_array.append("meter")
        elif search("English Units", file_contents):
            identical_array.append("foot")
        else:
            identical_array.append("manual")


    if ("manual" in identical_array) or (identical_array.count(identical_array[0]) != len(identical_array)):
        print(" -- ALERT: units were inconsistant for models found, investigate and correct or manually set -v flag appropritely")
        print(identical_array)
        raise SystemExit(0)

    else:
        if identical_array[0] == "meter":
            unit = "meter"
        elif identical_array[0] == "foot":
            unit = "feet"
    return unit
