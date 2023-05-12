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
    rating_curve_df = pd.DataFrame()

    all_rating_files = Path(r2f_hecras_dir).rglob('*rating_curve.csv')

    for file in all_rating_files:
        featureid=file.name.split("_rating_curve.csv")[0]
        this_file_df=pd.read_csv(file)
        this_file_df["feature-id"]=featureid
        rating_curve_df=rating_curve_df.append(this_file_df)

    rating_curve_df.rename(columns={"AvgDepth(m)":"stage (m)","Flow(cms)":"Discharge (m3s-1)"}, inplace = True)
    rating_curve_df=rating_curve_df[["feature-id","stage (m)","Discharge (m3s-1)"]]
    rating_curve_df.to_csv(os.path.join(r2f_ras2rem_dir,"rating_curve.csv"), index = False)


def fn_ras2rem_make_rem(r2f_hecras_dir, r2f_ras2rem_dir):
    '''
    Args:
        r2f_hecras_dir: directory containing all fim raster  (the huc model 05_hecras_outputs)
        r2f_ras2rem_dir: directory to write output file (rating_curve.csv)

    Returns: ras2rem.tif file
    '''

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

    r2f_hecras_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    r2f_ras2rem_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_RAS2REM)

    print("+=================================================================+")
    print("|                          Run ras2rem                            |")
    print("+-----------------------------------------------------------------+")
    print("  ---(i) RAS2FIM HECRES Input Path: " + str(r2f_hecras_dir))
    print("  ---(o) RAS2REM Output Path: " + str(r2f_ras2rem_dir))

    flt_start_ras2rem = time.time()

    if os.path.exists(r2f_ras2rem_dir):
        shutil.rmtree(r2f_ras2rem_dir)
    os.mkdir(r2f_ras2rem_dir)

    fn_ras2rem_make_rating_curve(r2f_hecras_dir, r2f_ras2rem_dir)
    fn_ras2rem_make_rem(r2f_hecras_dir, r2f_ras2rem_dir)

    flt_end_ras2rem = time.time()
    flt_time_pass_ras2rem = (flt_end_ras2rem - flt_start_ras2rem) // 1
    time_pass_ras2rem = datetime.timedelta(seconds=flt_time_pass_ras2rem)
    print('Compute Time: ' + str(time_pass_ras2rem))


def __init_and_run(r2f_huc_output_dir,
                   base_ras2fim_path = sv.DEFAULT_BASE_DIR):

    ####################################################################
    ####  Some validation of input, but mostly setting up pathing ######
    # -b   (ie c:\ras2fim)
    if (os.path.isdir(base_ras2fim_path) == False):
        raise ValueError("the -bp arg (base path) does not appear to be a folder.")

    # -o  (ie C:\ras2fim_data\output_ras2fim_models\12090301_meters_2277_test_1)
    # The subfolders like 05_ and 06_ are referential from here.
    if (os.path.exists(r2f_huc_output_dir) == False):  # in case we get a full path incoming
        r2f_huc_output_dir = os.path.join(base_ras2fim_path, "output_ras2fim_models", r2f_huc_output_dir)
        # we don't need to validate the basic path and the child folder need not yet exist. We built
        # up the path ourselves.

    fn_run_ras2rem(r2f_huc_output_dir)


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='==== Run RAS2REM ===')

    parser.add_argument('-bp',
                        dest = "base_ras2fim_path",
                        help = 'OPTIONAL: The base local of all of ras2fim folder (ie.. inputs, OWP_ras_models, output_ras2fim_models, etc).' \
                              r' Defaults to C:\ras2fim_data.',
                        required = False,
                        default = "c:\\ras2fim_data",
                        type = str)

    parser.add_argument('-o',
                        dest = "r2f_huc_output_dir",
                        help = r'REQUIRED: The name of the r2f huc output folder to be created in the outputs_ras2fim_models folder.'\
                               r' Example: my_12090301_test_2. It wil be added to the -bp (base path) and the' \
                               r' hardcoded value of ..ouput_ras2fim_models.. to become something like' \
                               r' c:\ras2fim_data\output_ras2fim_models\my_12090301_test_2.' \
                               r' NOTE: you can use a full path if you like and we will not override it.',
                        required = True,
                        type = str) 

    '''
    parser.add_argument('-i',
                        dest="input_path",
                        help=r'REQUIRED: path to fim_rasters',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest="ras2rem_output_dir",
                        help=r'REQUIRED: path to write ras2rem output files',
                        required=True,
                        metavar='DIR',
                        type=str)
    '''

    args = vars(parser.parse_args())
    
    __init_and_run(**args)



