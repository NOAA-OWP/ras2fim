#!/usr/bin/env python3

import os

import argparse
import fiona
fiona.supported_drivers
import fiona.transform
import glob
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.mask

import shared_variables as sv
import shared_functions as sf

from datetime import datetime
from pathlib import Path
from rasterio.merge import merge


# This function geopackage of Feature IDs that correspond to the extent
# of the Depth Grids (and subsequent REMs that also match the Depth Grids)
def make_catchments(huc_num,
                    r2f_huc_parent_dir,
                    national_ds_path = sv.INPUT_NWM_CATCHMENTS_FILE,
                    src_nwm_catchments_file = sv.INPUT_NWM_CATCHMENTS_FILE):
    
    '''
    Overview
    ----------
    Using rem tif outputs from ras2rem, and the file names of those tif in the 05_hecras_output folders
    and its subfolders, we create a list of unique feature id's being used.

    Next, using the nwm_catchments.gkpg, we clip it down to the relavent HUC8 to manage volume and speed,
    then extract all catchment polygons matching related feature ids.  We reproject the new filtered
    catchemnts.gkpg to match the projection from one of the REM's and save the .gpkg    

    Parameters
    ----------
    
    - huc_num : str
        Self explanitory
            
    - r2f_huc_parent_dir : str
        The partial or full path to the ras2fim output HUC directory. That folder must already have a fully populated with 
        depth grid tifs. 
        This value can be the value of just the the output_ras2fim_models huc subfolder, ie 12090301_meters_2277_test_3.
           (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)
        OR it can be a full path to the ras2fim huc output folder. ie) c:/my_ras2fim_outputs/12090301_meters_2277_test_3.
        Either way, it needs at least the populated 05_hecras_output subfolder.
            
    - national_ds_path : str
        This needs to be the full path of the X-National_Dataset folder. It does not need to be that folder name, but must
        have the key input files in it. 
        
    '''
    
    ####################################################################
    ####  Some validation of input, but setting up pathing ######

    def __validate_make_catchments(huc_num,
                                   r2f_huc_parent_dir,
                                   national_ds_path = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR):
        
            # -w   (ie 12090301)    
        if (len(huc_num) != 8):
            raise ValueError("the -w flag (HUC8) is not 8 characters long")
        if (huc_num.isnumeric() == False): # can handle leading zeros
            raise ValueError("the -w flag (HUC8) does not appear to be a HUC8")

        
        # The subfolders like 05_ and 06_ are referential from here.
        # -o  (ie 12090301_meters_2277_test_1) or some full custom path
        # We need to remove the the last folder name and validate that the parent paths are valid
        is_invalid_path = False
        if ("\\" in r2f_huc_parent_dir):  # submitted a full path
            if (os.path.exists(r2f_huc_parent_dir) == False): # full path must exist
                is_invalid_path = True
        else: # they provide just a child folder (base path name)
            r2f_huc_parent_dir = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS, r2f_huc_parent_dir)
            if (os.path.exists(r2f_huc_parent_dir) == False): # child folder must exist
                is_invalid_path = True

        if (is_invalid_path == True):
            raise ValueError('The -p arg (parent [ras2fim HUC output] ) folder does not exist. Please correct and retry.')

        # -n  (ie: inputs\\X-National_Datasets) 
        if (os.path.exists(national_ds_path) == False):
            raise ValueError("the -n arg (inputs x national datasets path arg) does not exits. Please correct and retry.")

        # the relavent WBD HUC8 gpkg
        wbd_huc8_dir = os.path.join(national_ds_path, sv.INPUT_WBD_HUC8_DIR)
        wbd_huc8_file = os.path.join(wbd_huc8_dir, f"HUC8_{huc_num}.gpkg")
        if (os.path.exists(wbd_huc8_file) == False):
            raise Exception(f"The {wbd_huc8_file} file does not exist and is required.")

        # The source nwm file
        src_nwm_catchments_file = os.path.join(national_ds_path, sv.INPUT_NWM_CATCHMENTS_FILE) 
        if (os.path.exists(src_nwm_catchments_file) == False):
            raise Exception(f"The {src_nwm_catchments_file} file does not exist and is required.")

        # AND the 05 directory must already exist 
        r2f_hecras_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
        if (os.path.exists(r2f_hecras_dir) == False):
            raise Exception(f"The ras2fim huc output, {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} subfolder does not exist." \
                            f" Ensure ras2fim has been run and created a valid {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} folder.")
        
        # And the 06 directory must already exist ( maybe it wasn't processed yet)
        r2f_ras2rem_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_RAS2REM)
        if (os.path.exists(r2f_ras2rem_dir) == False):
            raise Exception(f"The ras2fim huc output, {sv.R2F_OUTPUT_DIR_RAS2REM} subfolder does not exist." \
                            f" Ensure ras2rem has been run and created a valid {sv.R2F_OUTPUT_DIR_RAS2REM} folder.")

        # only return the variables created or modified
        return r2f_huc_parent_dir, \
               r2f_hecras_dir, \
               r2f_ras2rem_dir, \
               wbd_huc8_file, \
               src_nwm_catchments_file


    r2f_huc_parent_dir, r2f_hecras_dir, r2f_ras2rem_dir, \
        wbd_huc8_file, src_nwm_catchments_file = __validate_make_catchments (huc_num,
                                                                             r2f_huc_parent_dir,
                                                                             national_ds_path )

    catchments_subset_file = os.path.join(r2f_ras2rem_dir, "ras2fim_catchments.gpkg")


    ####################################################################
    ####  Start processing ######
    start_dt = datetime.now()    

    print(" ")
    print("+=================================================================+")
    print("|                 CREATE CATCHMENTS                               |")
    print("+-----------------------------------------------------------------+")
    print("  ---(w) HUC-8: " + huc_num)
    print("  ---(p) PARENT RAS2FIM HUC DIRECTORY: " + r2f_huc_parent_dir)
    print("  ---(n) PATH TO NATIONAL DATASETS: " + national_ds_path)    
    print("===================================================================")
    print(" ")

    print ("Getting list of feature IDs")
    # Make a list of all tif file in the 05_  Depth_Grid (focusing on the depth not the feature ID)
    all_depth_grid_tif_files=list(Path(r2f_hecras_dir).rglob('*/Depth_Grid/*.tif'))
    depth_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-1], all_depth_grid_tif_files))
    depth_values=np.unique(depth_values)

    # Make a list of all unique feature id,
    # TODO: We might just be able to take the first part of of the .tif files (before the -xxx)
    # and that should be the list of the feature IDs.
    all_feature_ids = []
    for depth_value in depth_values:
        rem_tif_files = [file  for file in all_depth_grid_tif_files if file.name.endswith("-%s.tif"%depth_value)]
        for p in rem_tif_files:
            feature_id = str(p).split('\\')[-1].split('-')[0] 
            all_feature_ids.append(int(feature_id))

    all_feature_ids = list(set(all_feature_ids)) # makes the list unique

    num_features = len(all_feature_ids)
    if (num_features == 0):
        raise Exception("no feature ids were found. Please ensure that ras2fim has been run and the 05_hecras_output" \
                        " has depth grid tifs in the pattern of {featureID-{depth value}.tif. ie) 5789848-18.tif")
    print(f"Number of unique feature ID is {num_features}")

    # The following file needs to point to a NWM catchments shapefile (local.. not S3 (for now))
    
    print()
    print("Subsetting NWM Catchments from HUC8, no buffer")
    print()

    huc8_wbd_db = gpd.read_file(wbd_huc8_file)
    nwm_df = gpd.read_file(src_nwm_catchments_file, mask = huc8_wbd_db)
    
    print("Getting all relevant catchment polys")
    print()
    filtered_catchments_df = nwm_df.loc[nwm_df['ID'].isin(all_feature_ids)]
    nwm_filtered_df = gpd.GeoDataFrame.copy(filtered_catchments_df)

    # We need to project the output gpkg to match the incoming raster projection.
    print(f"Reprojecting filtered nwm_catchments to rem rasters crs")

    # Use the first discovered depth file as all rems' should be the same. 
    with rasterio.open(all_depth_grid_tif_files[0]) as rem_raster:
        raster_crs = rem_raster.crs.wkt
        print(f"..... ras2rem rasters crs is : {raster_crs}")

    reproj_nwm_filtered_df = nwm_filtered_df.to_crs(raster_crs)

    reproj_nwm_filtered_df.to_file(catchments_subset_file, driver='GPKG')

    print()
    print("ras2catchment processing complete")
    print(f"Catchment file saved as {catchments_subset_file}")
    sf.print_date_time_duration(start_dt, datetime.now())
    print("===================================================================")    
    print("")    

if __name__=="__main__":

    #try:
    #    #print("os.environ['PROJ_LIB''] is " + os.environ["PROJ_LIB"])
    #    if (os.environ["PROJ_LIB"]):
    #        del os.environ["PROJ_LIB"]
    #except Exception as e:
    #    print(e)


    # Sample usage:
    # Using all defaults:
    #     python ras2catchments.py -w 12090301 -p 12090301_meters_2277_test_22

    # Override every optional argument (and of course, you can override just the ones you like)
    #     python ras2catchments.py -w 12090301 -p C:\ras2fim_data_rob_folder\output_ras2fim_models_2222\12090301_meters_2277_test_2
    #          -n E:\X-NWS\X-National_Datasets
    
    #  - The -p arg is required, but can be either a full path (as shown above), or a simple folder name.Either way, it must have the
    #        and the 05_hecras_output and 06_ras2rem folder and populated
    #        ie) -p c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2
    #            OR
    #            -p 12090301_meters_2277_test_3  (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)
    
    parser = argparse.ArgumentParser(description='========== Create catchments for specified existing output_ras2fim_models folder ==========')

    parser.add_argument('-w',
                        dest = "huc_num",
                        help = 'REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required = True,
                        type = str)

    parser.add_argument('-p',
                        dest = "r2f_huc_parent_dir",
                        help = r'REQUIRED: This can be used in one of two ways. You can submit either a full path' \
                               r' such as c:\users\my_user\Desktop\myoutput OR you can add a simple ras2fim output huc folder name.' \
                                ' Please see the embedded notes in the __main__ section of the code for details and  examples.',
                        required = True,
                        type = str) 

    parser.add_argument('-n',
                        dest = "national_ds_path",
                        help = r'OPTIONAL: path to national datasets: Example: E:\X-NWS\X-National_Datasets.' \
                               r' Defaults to c:\ras2fim_data\inputs\X-National_Datasets.',
                        default = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                        required = False,
                        type = str)

    args = vars(parser.parse_args())
    
    make_catchments(**args)

