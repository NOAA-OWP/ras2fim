import shutil
import os
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
from rasterio.merge import merge
import rasterio
import rasterio.mask
import re
import fiona
import fiona.transform
from shapely.geometry import Polygon
from rasterio import features

def make_rating_curve(Input_dir,Output_dir ):
    rating_curve_df=pd.DataFrame()

    all_rating_files=Path(Input_dir).rglob('*rating_curve.csv')
    for file in all_rating_files:
        featureid=file.name.split("_rating_curve.csv")[0]
        this_file_df=pd.read_csv(file)
        this_file_df["feature_id"]=featureid

        # add data that works with the existing inundation hydro table format requirements
        this_file_df['HydroID'] = featureid
        huc = re.search(r'HUC_(\d{8})',str(file))[1] # assumes the filename and folder structure stays the same
        this_file_df['HUC'] = huc
        this_file_df['LakeID'] = -999
        this_file_df['last_updated'] = ''
        this_file_df['submitter'] = ''
        this_file_df['obs_source'] = ''
        rating_curve_df=rating_curve_df.append(this_file_df)
    rating_curve_df.rename(columns={"AvgDepth(m)":"stage","Flow(cms)":"discharge_cms"},inplace=True)
    
    rating_curve_df.to_csv(os.path.join(Output_dir,"rating_curve.csv"),index=False)

def make_rem(Input_dir,Output_dir):
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


if __name__=="__main__":
    # OMG this proj lib is sooooo annoying and messes up things getting a crs properly!
    try:
        del os.environ["PROJ_LIB"]
    except Exception as e:
        print(e)
        
    Input_dir=r"C:\Users\laura.keys\Documents\starter_rem_data\ras2fim_for_ESIP_AWS\sample-dataset\output_iowa\05_hecras_output"
    Output_dir=r"C:\Users\laura.keys\Documents\starter_rem_data\ras2fim_for_ESIP_AWS\sample-dataset\output_iowa\06_ras2rem_output"

    if os.path.exists(Output_dir):
        shutil.rmtree(Output_dir)
    os.mkdir(Output_dir)

    make_rating_curve(Input_dir,Output_dir)
    make_rem(Input_dir,Output_dir)
