import shutil
import os
from pathlib import Path
import numpy as np
import pandas as pd
from rasterio.merge import merge
import rasterio
import argparse
import time
import datetime

import shared_variables as sv

def fn_ras2rem_make_rating_curve(r2f_hecras_dir, r2f_ras2rem_dir):

    '''
    Args:
        r2f_hecras_dir: directory containing all fim raster  (the huc model 05_hecras_outputs)
        r2f_ras2rem_dir: directory to write output file (rating_curve.csv)

    Returns: rating_curve.csv file
    '''
    print("Making rating curve")
    rating_curve_df = pd.DataFrame()

    all_rating_files = Path(r2f_hecras_dir).rglob('*rating_curve.csv')

    for file in all_rating_files:
        featureid=file.name.split("_rating_curve.csv")[0]
        this_file_df=pd.read_csv(file)
        this_file_df["feature-id"]=featureid
        rating_curve_df=rating_curve_df.append(this_file_df)

    #rating_curve_df.rename(columns={"AvgDepth(m)":"stage (m)","Flow(cms)":"Discharge (m3s-1)"}, inplace = True)
    #rating_curve_df=rating_curve_df[["feature-id","stage (m)","Discharge (m3s-1)"]]
    rating_curve_df.rename(columns={"AvgDepth(ft)":"stage (ft)","Flow(cfs)":"Discharge (cfs)"}, inplace=True)
    rating_curve_df=rating_curve_df[["feature-id","stage (ft)","Discharge (cfs)"]]

    rating_curve_df.to_csv(os.path.join(r2f_ras2rem_dir,"rating_curve.csv"), index = False)


def fn_make_rems(r2f_hecras_dir, r2f_ras2rem_dir):
    '''
    Args:
        r2f_hecras_dir: directory containing all fim raster  (the huc model 05_hecras_outputs)
        r2f_ras2rem_dir: directory to write output file (rating_curve.csv)

    Returns: ras2rem.tif file
    '''

    print("Making rem (mosaicing)")
    
    all_tif_files = list(Path(r2f_hecras_dir).rglob('*/Depth_Grid/*.tif'))
    rem_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-1], all_tif_files))
    rem_values = np.unique(rem_values)

    for rem_value in rem_values:

        raster_to_mosiac = []
        this_rem_tif_files= [file  for file in all_tif_files if file.name.endswith("-%s.tif"%rem_value)]
        for p in this_rem_tif_files:
            raster = rasterio.open(p)
            raster_to_mosiac.append(raster)
        mosaic, output = merge(raster_to_mosiac)

        #replace values of the raster with rem value, assuming there is no chance of having negative values
        mosaic = np.where(mosaic != raster.nodata, np.float64(rem_value)/10, raster.nodata)

        #prepare meta data
        output_meta = raster.meta.copy()
        output_meta.update(
            {"driver": "GTiff",
             "height": mosaic.shape[1],
             "width": mosaic.shape[2],
             "transform": output,
             "dtype": rasterio.float64
             }
        )

        # TODO: come up with better variable name then "m"
        with rasterio.open(os.path.join(r2f_ras2rem_dir,"{}_rem.tif".format(rem_value)), "w", **output_meta) as m:
            m.write(mosaic)

    #now make the final rem
    all_rem_files = list(Path(r2f_ras2rem_dir).rglob('*_rem.tif'))
    raster_to_mosiac = []

    print("Merging rems")
    for p in all_rem_files:
        raster = rasterio.open(p)
        raster_to_mosiac.append(raster)
    mosaic, output = merge(raster_to_mosiac, method = "min")

    output_meta = raster.meta.copy()
    output_meta.update(
        {"driver": "GTiff",
         "height": mosaic.shape[1],
         "width": mosaic.shape[2],
         "transform": output,
         }
    )

    # TODO: come up with better variable name then "m"
    with rasterio.open(os.path.join(r2f_ras2rem_dir,"rem.tif"), "w", **output_meta) as m:
        m.write(mosaic)

    # finally delete unnecessary files to clean up
    for raster in raster_to_mosiac:
        raster.close()

    for p in all_rem_files:
        os.remove(p)


def fn_run_ras2rem(r2f_huc_output_dir):
    
    ####################################################################
    # Input validation and variable setup

    # The subfolders like 05_ and 06_ are referential from here.
    # -o  (ie 12090301_meters_2277_test_1) or some full custom path
    # We need to remove the the last folder name and validate that the parent paths are valid
    is_invalid_path = False
    if ("\\" in r2f_huc_output_dir):  # submitted a full path
        if (os.path.exists(r2f_huc_output_dir) == False): # full path must exist
            is_invalid_path = True
    else: # they provide just a child folder (base path name)
        r2f_huc_output_dir = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS, r2f_huc_output_dir)
        if (os.path.exists(r2f_huc_output_dir) == False): # child folder must exist
            is_invalid_path = True

    if (is_invalid_path == True):
        raise ValueError('The -o folder must exist and appears to be invalid.')

    # We need the two subdirectories 
    r2f_hecras_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    r2f_ras2rem_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_RAS2REM)

    ####################################################################
    ####  Start processing ######
    print("+=================================================================+")
    print("|                       Run ras2rem                               |")
    print("  --- RAS2FIM HUC owp_ras2fim_model Path: " + str(r2f_huc_output_dir))    
    print("  --- RAS2FIM ras2fim HECRES Input Path: " + str(r2f_hecras_dir))
    print("  --- RAS2REM Output Path: " + str(r2f_ras2rem_dir))
    print("+-----------------------------------------------------------------+")    

    flt_start_ras2rem = time.time()

    # 05_hecras_output must exist by now, but 06_ras2rem might not
    # and if it does, remove it so it can be rebuilt.

    if os.path.exists(r2f_ras2rem_dir):
        shutil.rmtree(r2f_ras2rem_dir)
    os.mkdir(r2f_ras2rem_dir)

    fn_ras2rem_make_rating_curve(r2f_hecras_dir, r2f_ras2rem_dir)
    fn_make_rems(r2f_hecras_dir, r2f_ras2rem_dir)

    flt_end_ras2rem = time.time()
    flt_time_pass_ras2rem = (flt_end_ras2rem - flt_start_ras2rem) // 1
    time_pass_ras2rem = datetime.timedelta(seconds=flt_time_pass_ras2rem)
    print('Compute Time: ' + str(time_pass_ras2rem))


if __name__=="__main__":

    # Sample usage:
    # Using all defaults:
    #     python run_ras2rem.py -o 12090301_meters_2277_test_22

    #  - The -o arg is required, but can be either a full path (as shown above), or a simple folder name but not the "06_" folder name. Use the primary huc ras2fim output directory.
    #        ie) -o c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2  (the 6 folders of 01, 02... 05 will be under this folder can not be overriden).
    #            OR
    #            -o 12090301_meters_2277_test_3  (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)
    
    # If the "06_" folder exists, it will be removed and overwritten.

    parser = argparse.ArgumentParser(description='==== Run RAS2REM ===')

    parser.add_argument('-o',
                        dest = "r2f_huc_output_dir",
                        help = r'REQUIRED: This can be used in one of two ways. You can submit either a full path' \
                               r' such as c:\users\my_user\Desktop\myoutput OR you can add a simple final folder name.' \
                                ' Please see the embedded notes in the __main__ section of the code for details and  examples.',
                        required = True,
                        type = str) 

    args = vars(parser.parse_args())
    
    fn_run_ras2rem(**args)



