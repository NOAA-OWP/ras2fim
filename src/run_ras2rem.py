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
import sys
import re
import glob
import tqdm
import multiprocessing as mp
from multiprocessing import Pool
import shared_variables as sv


def fn_make_rating_curve(r2f_hecras_dir, r2f_ras2rem_dir):

    '''
    Args:
        r2f_hecras_dir: directory containing all fim raster  (the huc model 05_hecras_outputs)
        r2f_ras2rem_dir: directory to write output file (rating_curve.csv)

    Returns: rating_curve.csv file
    '''
    print("Making rating curve")
    rating_curve_df = pd.DataFrame()

    all_rating_files = list(Path(r2f_hecras_dir).rglob('*rating_curve.csv'))
    if len(all_rating_files) == 0:
        print("Error: Make sure you have specified a correct input directory with at least one '*.rating curve.csv' file.")
        sys.exit(1)
    #find unit of outputs(ft vs m) using a sample rating curve file
    try:
        sample_rating_file = pd.read_csv(all_rating_files[0])
        SecondColumnTitle = sample_rating_file.columns[1]
        pattern = r"\((.*?)\)"  # Matches text between parentheses in column 2
        stage_unit = re.search(pattern, SecondColumnTitle).group(1).strip().lower()
        if stage_unit == 'ft':
            current_flow_unit, target_flow_unit='cfs','cfs'
        elif stage_unit == 'm':
            current_flow_unit, target_flow_unit='cms','cms'  #'m3s-1'
        else:
            raise ValueError("Rating curve values should be either in feet or meter. Check the results")
    except ValueError as e:
        print("Error:", e)
        sys.exit(1)
    except:
        print("Error: Make sure you have specified a correct input directory with has at least one '*.rating curve.csv' file.")
        sys.exit(1)

    for file in all_rating_files:
        featureid = file.name.split("_rating_curve.csv")[0]
        this_file_df = pd.read_csv(file)
        this_file_df["feature_id"] = featureid

        # add data that works with the existing inundation hydro table format requirements
        this_file_df['HydroID'] = featureid
        huc = re.search(r'HUC_(\d{8})', str(file))[1] # assumes the filename and folder structure stays the same
        this_file_df['HUC'] = huc
        this_file_df['LakeID'] = -999
        this_file_df['last_updated'] = ''
        this_file_df['submitter'] = ''
        this_file_df['obs_source'] = ''
        rating_curve_df = rating_curve_df.append(this_file_df)

    rating_curve_df.rename(columns={"AvgDepth(%s)"%stage_unit:"stage_%s"%stage_unit,\
                                     "Flow(%s)"%current_flow_unit:"discharge_%s"%target_flow_unit}, inplace=True)
    
    rating_curve_df = rating_curve_df[['feature_id', 'stage_%s'%stage_unit, 'discharge_%s'%target_flow_unit, 
                                     'HydroID', 'HUC', 'LakeID', 'last_updated', 'submitter', 'obs_source']]
    
    rating_curve_df.to_csv(os.path.join(r2f_ras2rem_dir,"rating_curve.csv"), index = False)


def fn_generate_tif_for_each_rem(tpl_request):
    rem_value = tpl_request[0]
    Input_dir = tpl_request[1]
    Output_dir = tpl_request[2]

    all_tif_files=glob.glob(Input_dir + "/**/Depth_Grid/*.tif", recursive=True)
    raster_to_mosiac = []
    this_rem_tif_files= [file  for file in all_tif_files if os.path.basename(file).endswith("-%s.tif"%rem_value)]
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
         "dtype": rasterio.float64,
         "compress":"LZW"
         }
    )
    with rasterio.open(os.path.join(Output_dir,"{}_rem.tif".format(rem_value)), "w", **output_meta) as tiffile:
        tiffile.write(mosaic)
    return  rem_value

def fn_make_rems(r2f_hecras_dir, r2f_ras2rem_dir):
    '''
    Args:
        r2f_hecras_dir: directory containing all fim raster  (the huc model 05_hecras_outputs)
        r2f_ras2rem_dir: directory to write output file (rating_curve.csv)

    Returns: ras2rem.tif file
    '''
    
    all_tif_files=glob.glob(r2f_hecras_dir + "/**/Depth_Grid/*.tif", recursive=True)
    if len(all_tif_files)==0:
        print("Error: Make sure you have specified a correct input directory with at least one '*.tif' file.")
        sys.exit(1)

    rem_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-1], all_tif_files))
    rem_values = np.unique(rem_values).tolist()

    print("+-----------------------------------------------------------------+")
    print('Making %d tif files for %d rem values'%(len(rem_values),len(rem_values)))
    #make argument for multiprocessing
    rem_info_arguments = []
    for rem_value in rem_values:
        rem_info_arguments.append((rem_value,r2f_hecras_dir,r2f_ras2rem_dir))


    num_processors = (mp.cpu_count() - 1)
    with Pool(processes = num_processors) as executor:    
        #pool = Pool(processes = num_processors)
        list_of_returned_results = list(tqdm.tqdm(executor.imap(fn_generate_tif_for_each_rem, rem_info_arguments),
                                            total = len(rem_values),
                                            desc='Creating REMs',
                                            bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                            ncols=67 ))
        #pool.close()
        #pool.join()

    #now make the final rem
    print("+-----------------------------------------------------------------+")
    print('Merging all rem files to create the final rem')

    all_rem_files = list(Path(r2f_ras2rem_dir).rglob('*_rem.tif'))
    raster_to_mosiac = []

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
         "compress":"LZW"
         }
    )

    with rasterio.open(os.path.join(r2f_ras2rem_dir,"rem.tif"), "w", **output_meta) as tiffile:
        tiffile.write(mosaic)

    # finally delete unnecessary files to clean up
    for raster in raster_to_mosiac:
        raster.close()

    for p in all_rem_files:
        os.remove(p)


def fn_run_ras2rem(r2f_huc_parent_dir):
    
    ####################################################################
    # Input validation and variable setup

    # The subfolders like 05_ and 06_ are referential from here.
    # -o  (ie 12090301_meters_2277_test_1) or some full custom path
    # We need to remove the last folder name and validate that the parent paths are valid
    is_invalid_path = False
    if ("\\" in r2f_huc_parent_dir):  # submitted a full path
        if (os.path.exists(r2f_huc_parent_dir) == False): # full path must exist
            is_invalid_path = True
    else: # they provide just a child folder (base path name)
        r2f_huc_parent_dir = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS, r2f_huc_parent_dir)
        if (os.path.exists(r2f_huc_parent_dir) == False): # child folder must exist
            is_invalid_path = True

    if (is_invalid_path == True):
        raise ValueError(f"The -p arg '{r2f_huc_parent_dir}' folder does not exist. Please check if ras2fim has been run" \
                         " for the related huc and verify the path.")

    # AND the 05 directory must already exist 
    r2f_hecras_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    if (os.path.exists(r2f_hecras_dir) == False):
        raise ValueError(f"The {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} folder does not exist." \
                         f" Please ensure ras2fim has been run and created a valid {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} folder.")
    
    r2f_ras2rem_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_RAS2REM)

    if os.path.exists(r2f_ras2rem_dir):
        shutil.rmtree(r2f_ras2rem_dir)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(2) # 2 seconds

    os.mkdir(r2f_ras2rem_dir)


    ####################################################################
    ####  Start processing ######
    print("+=================================================================+")
    print("|                       Run ras2rem                               |")
    print("  --- RAS2FIM ras2fim HUC folder path: " + str(r2f_huc_parent_dir))    
    print("  --- RAS2FIM ras2fim HECRES Input Path: " + str(r2f_hecras_dir))
    print("  --- RAS2REM ras2rem Output Path: " + str(r2f_ras2rem_dir))
    print("+-----------------------------------------------------------------+")    

    flt_start_ras2rem = time.time()

    fn_make_rating_curve(r2f_hecras_dir, r2f_ras2rem_dir)
    fn_make_rems(r2f_hecras_dir, r2f_ras2rem_dir)

    flt_end_ras2rem = time.time()
    flt_time_pass_ras2rem = (flt_end_ras2rem - flt_start_ras2rem) // 1
    time_pass_ras2rem = datetime.timedelta(seconds=flt_time_pass_ras2rem)
    print('Compute Time: ' + str(time_pass_ras2rem))


if __name__=="__main__":

    # Sample usage:
    # Using all defaults:
    #     python run_ras2rem.py -p 12090301_meters_2277_test_22

    #  - The -p arg is required, but can be either a ras2fim models huc folder name (as shown above), or a fully pathed.
    #        Either way, it must have the 05_hecras_output and it must be populated.
    #
    #        ie) -p c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2
    #            OR
    #            -p 12090301_meters_2277_test_3  (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)

    # *** NOTE: If the "06_ras2rem" folder exists, it will be deleted and a new one created.

    parser = argparse.ArgumentParser(description='==== Run RAS2REM ===')

    parser.add_argument('-p',
                        dest = "r2f_huc_parent_dir",
                        help = r'REQUIRED:'
                               r'This should be the path to the folder containing the ras2fim "05_hecras_output" subfolder. '
                               'The ras2rem results will be created in a folder called "06_ras2rem" in the same parent directory.\n' \
                               r' There are two options: 1) Providing a full path' \
                               r' 2) Providing only huc folder name, when following AWS data structure.' \
                                ' Please see the embedded notes in the __main__ section of the code for details and examples.',
                        required = True,
                        type = str) 

    args = vars(parser.parse_args())
    
    fn_run_ras2rem(**args)



