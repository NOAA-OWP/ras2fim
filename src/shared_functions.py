#!/usr/bin/env python3

import os
#import inspect
import keepachangelog
import numpy as np
import pandas as pd
import platform
import rasterio
from tqdm import tqdm

from concurrent.futures import as_completed


####################################################################
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

        if (is_windows()) :
            # first get the user and we have to build up a path
            user_home_path = os.path.expanduser("~")
            anaconda3_proj_path = os.path.join(user_home_path, r'anaconda3\envs\ras2fim\Library\share\proj')
            os.environ["PROJ_LIB"] = anaconda3_proj_path
    except Exception as e:
        #print(e)
        pass
