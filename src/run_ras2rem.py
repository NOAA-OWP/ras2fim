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

def fn_ras2rem_make_rating_curve(Input_dir,Output_dir ):
    '''
    Args:
        Input_dir: directory containing all fim raster
        Output_dir: directory to write output file (rating_curve.csv)

    Returns: rating_curve.csv file
    '''
    rating_curve_df=pd.DataFrame()

    all_rating_files=Path(Input_dir).rglob('*rating_curve.csv')
    for file in all_rating_files:
        featureid=file.name.split("_rating_curve.csv")[0]
        this_file_df=pd.read_csv(file)
        this_file_df["feature-id"]=featureid
        rating_curve_df=rating_curve_df.append(this_file_df)
    rating_curve_df.rename(columns={"AvgDepth(m)":"stage (m)","Flow(cms)":"Discharge (m3s-1)"},inplace=True)
    rating_curve_df=rating_curve_df[["feature-id","stage (m)","Discharge (m3s-1)"]]
    rating_curve_df.to_csv(os.path.join(Output_dir,"rating_curve.csv"),index=False)

def fn_ras2rem_make_rem(Input_dir,Output_dir):
    '''
    Args:
        Input_dir: directory containing all fim raster
        Output_dir: directory to write output file (ras2rem.tif)

    Returns: ras2rem.tif file
    '''

    all_tif_files=list(Path(Input_dir).rglob('*/Depth_Grid/*.tif'))
    rem_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-1], all_tif_files))
    rem_values=np.unique(rem_values)
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
        with rasterio.open(os.path.join(Output_dir,"{}_rem.tif".format(rem_value)), "w", **output_meta) as m:
            m.write(mosaic)

    #now make the final rem
    all_rem_files = list(Path(Output_dir).rglob('*_rem.tif'))
    raster_to_mosiac = []

    for p in all_rem_files:
        raster = rasterio.open(p)
        raster_to_mosiac.append(raster)
    mosaic, output = merge(raster_to_mosiac, method="min")

    output_meta = raster.meta.copy()
    output_meta.update(
        {"driver": "GTiff",
         "height": mosaic.shape[1],
         "width": mosaic.shape[2],
         "transform": output,
         }
    )
    with rasterio.open(os.path.join(Output_dir,"rem.tif"), "w", **output_meta) as m:
        m.write(mosaic)

    # finally delete unnecessary files to clean up
    for raster in raster_to_mosiac:
        raster.close()
    for p in all_rem_files:
        os.remove(p)
        


def fn_run_ras2rem(Input_dir,Output_dir):
    flt_start_ras2rem = time.time()
    print("+=================================================================+")
    print("|                          Run ras2rem                            |")
    print("+-----------------------------------------------------------------+")
    print("  ---(i) RAS2REM Input Path: " + str(Input_dir))
    print("  ---(o) RAS2REM Output Path: " + str(Output_dir))

    if os.path.exists(Output_dir):
        shutil.rmtree(Output_dir)
    os.mkdir(Output_dir)

    fn_ras2rem_make_rating_curve(Input_dir,Output_dir)
    fn_ras2rem_make_rem(Input_dir,Output_dir)

    flt_end_ras2rem = time.time()
    flt_time_pass_ras2rem = (flt_end_ras2rem - flt_start_ras2rem) // 1
    time_pass_ras2rem = datetime.timedelta(seconds=flt_time_pass_ras2rem)
    print('Compute Time: ' + str(time_pass_ras2rem))



if __name__=="__main__":
    parser = argparse.ArgumentParser(description='==== Run RAS2REM ===')

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


    args = vars(parser.parse_args())

    Input_dir=args['input_path']
    Output_dir=args['ras2rem_output_dir']

    fn_run_ras2rem(Input_dir,Output_dir)


