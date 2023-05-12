
import os

import argparse
import fiona
import fiona.transform
import glob
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.mask
import re
import shutil
import time

import shared_variables as sv

from osgeo import gdal
from pathlib import Path
from rasterio import features
from rasterio.merge import merge
from shapely.geometry import Polygon


# This function creates a geopackage of Feature IDs from the raster that
# matches the Depth Grid processing extents from ras2fim Step 5.
def vectorize(path):
    with rasterio.open(path) as src:
        rast = src.read()

    shapes = features.shapes(rast,transform=src.transform)

    geoms = []
    vals = []
    for shp,val in shapes:
      geoms.append(Polygon(shp["coordinates"][0]))
      vals.append(val)

    df = pd.DataFrame(vals)
    df.columns=["comid"]
    df["HydroID"] = vals
    
    gdf = gpd.GeoDataFrame(df,crs=src.crs,geometry=geoms)
    gdf = gdf.dissolve('comid')    
    gdf.to_file(path.replace(".tif",".gpkg"),driver="GPKG",layer="catchments") #,crs=gdf.crs)
    return


def load_nwm_huc_catchments(huc_num, national_ds_dir, src_wbd_huc8_dir, src_nwm_catchments_file, r2f_ras2rem_dir):

    src_wbd_huc8_extent_file = os.path.join(src_wbd_huc8_dir, f"HUC8_{huc_num}.gpkg")

    # now use that single huc poly to clip against the nwm_catchments file
    # Find intersecting nwm_catchments
    print("Subsetting NWM Catchments from HUC8, no buffer", flush=True)

    # TODO.. projection

    # TODO: throws error = Error: string indices must be integers
    nwm_catchments_df = gpd.read_file(src_nwm_catchments_file, mask = src_wbd_huc8_extent_file)
    #nwm_catchments_df = gpd.read_file(src_nwm_catchments_file)   
    #src_nwm_catchments_file = r"C:\ras2fim_data\inputs\X-National_Datasets\nwm_catchments_subset\nwm_catchments_clip.shp"
    #nwm_catchments_df = gpd.read_file(src_nwm_catchments_file)       

    # TODO: Shape files are much slower to load. Can we change this to a gkpg? Can fiona open a gpkg?
    nwm_catchments_subset_file = os.path.join(r2f_ras2rem_dir, "nwm_catchments_subset.shp")

    # TODO: How do we handle projection
    nwm_catchments_df.to_file(nwm_catchments_subset_file)

    # TODO: fix this (and projection)
    '''
    if len(nwm_catchments) > 0:
        nwm_catchments.to_file(subset_nwm_catchments, driver=getDriver(subset_nwm_catchments), index=False, crs=DEFAULT_FIM_PROJECTION_CRS)
    else:
        print ("No NWM catchments within HUC " + str(hucCode) + " boundaries.")
        sys.exit(0)
    del nwm_catchments
    '''
    return nwm_catchments_subset_file

# This function creates a raster and geopackage of Feature IDs that correspond to the extent
# of the Depth Grids (and subsequent REMs that also match the Depth Grids)
def make_catchments(huc_num, r2f_huc_output_dir, base_ras2fim_path, national_ds_dir):
    
    start_time = time.time()    

    print(" ")
    print("+=================================================================+")
    print("|                 CREATE CATCHMENTS                               |")
    print("+-----------------------------------------------------------------+")
    print("  ---(w) HUC-8: " + huc_num)
    print("  ---(b) BASE_RAS2FIM_PATH: " + base_ras2fim_path)
    print("  ---(o) OUTPUT DIRECTORY: " + r2f_huc_output_dir)
    print("  ---(n) PATH TO NATIONAL DATASETS: " + national_ds_dir)     
    print("===================================================================")
    print(" ")

    src_nwm_catchments_file = os.path.join(base_ras2fim_path, sv.INPUT_NWM_CATCHMENTS_FILE)
    src_wbd_huc8_dir = os.path.join(base_ras2fim_path, sv.INPUT_WBD_HUC8_DIR)
    r2f_hecras_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    r2f_ras2rem_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_RAS2REM)

    all_tif_files=list(Path(r2f_hecras_dir).rglob('*/Depth_Grid/*.tif'))
    rem_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-1], all_tif_files))
    rem_values=np.unique(rem_values)

    # setting up the clipped shapefile for the catchments
    all_comids = []
    for rem_value in rem_values:
        this_rem_tif_files = [file  for file in all_tif_files if file.name.endswith("-%s.tif"%rem_value)]
        for p in this_rem_tif_files:
            comid = str(p).split('\\')[-1].split('-')[0] # (or match what's between Depth_Grid and -, for
            all_comids.append(int(comid))

    all_comids = list(set(all_comids))
    print(f"Looking through {all_comids}")

    nwm_catchments_subset_file = load_nwm_huc_catchments(huc_num,
                                                         national_ds_dir,
                                                         src_wbd_huc8_dir,
                                                         src_nwm_catchments_file, 
                                                         r2f_ras2rem_dir)
    

    # The following file needs to point to a NWM catchments shapefile (local.. not S3 (for now))
    ## nwm_comid_polygons = sv.INPUT_NWM_CATCHMENTS_FILE

    # The following file should be rewired to point to a subset polygon shape in a temp directory
    # TODO: upgrade to clip against some full new CONUS dataset to pull the poly's we need. Not sure i understand what they are yet (Rob)
    # temp_sub_polygons = r"c:\users\laura.keys\documents\subset_polys.shp" # clipped to relevant

    # TODO: WIP

    '''
    with fiona.open(nwm_comid_polygons) as shapefile:
        print(".. getting all relevant shapes")
        with fiona.open(temp_sub_polygons,"w",driver=shapefile.driver, \
                        schema=shapefile.schema,crs=shapefile.crs) as dest:
            for x in shapefile:
                if x["properties"]["FEATUREID"] in all_comids:
                    print(f"...... writing {x['properties']['FEATUREID']}")
                    dest.write(x)
    '''
    # Create a seperate file for each feature in the nwm_catchments that matches
    # a comm id from depth grides
    print(".. getting all relevant shapes")
    with fiona.open(nwm_catchments_subset_file, "w", driver='GPKG') as nwm_catchments:
        for feat in nwm_catchments:
            if feat["properties"]["FEATUREID"] in all_comids:
                print(f"...... writing {feat['properties']['FEATUREID']}")
                nwm_catchments.write(feat)

    r"""

    print("TIME to create subset polygons: " +str(time.time()-start_time))
    for rem_value in rem_values:
        raster_to_mosiac = []
        this_rem_tif_files = [file  for file in all_tif_files if file.name.endswith("-%s.tif"%rem_value)]
        for p in this_rem_tif_files:
            comid = str(p).split('\\')[-1].split('-')[0] # (or match what's between Depth_Grid and -, for an all-system solution)
            print(f"** Addressing {comid}")
            raster = rasterio.open(p)


            #temp_proj_polygons = r"c:\users\laura.keys\documents\temp_sub_proj_polys.shp" # reproj
            if not os.path.exists(temp_proj_polygons):
                print("Projecting COMID polygons from NWM Catchments")
                print(raster.crs)
                df = gpd.read_file(temp_sub_polygons)
                print(df.crs)
                df = df.to_crs(raster.crs)
                print(".. Subset shapefile read in and transformed")
                df.to_file(temp_proj_polygons)
                print(".. and written back out")

            with fiona.open(temp_proj_polygons) as subset_shapefile:
                shapes = [feature["geometry"] for feature in subset_shapefile if feature['properties']['FEATUREID']==int(comid)]
            
            # ... apply polygon as mask to initial raster
            masked_raster, transform = rasterio.mask.mask(raster, shapes, nodata=raster.nodata, filled=True, invert=False)

            long_raster_vals = np.array(masked_raster,dtype=np.int32)
            long_raster_vals[long_raster_vals != raster.nodata] = np.int32(comid)
                
            comid_rem_path = os.path.join(output_dir, "temp_{}_{}.tif".format(rem_value, comid))
            output_meta = raster.meta.copy()
            output_meta.update({"dtype":np.int32})
            with rasterio.open(comid_rem_path, "w", **output_meta) as m:
                m.write(long_raster_vals)
            # append particular comid + depth value path to list
            raster_to_mosiac.append(comid_rem_path)
        # merge all comids at this depth value
        mosaic, output = merge(list(map(rasterio.open,raster_to_mosiac)))

        for p in raster_to_mosiac:
            os.remove(p)
            #pass

        mosaic = mosaic.astype(np.int32)
        output_meta = raster.meta.copy()
        output_meta.update(
            {"driver": "GTiff",
             "height": mosaic.shape[1],
             "width": mosaic.shape[2],
             "transform": output,
             "dtype": rasterio.int32
             }
        )
        # write all comids for this depth value
        comid_rem_path = os.path.join(Output_dir,"{}_comid.tif".format(rem_value))
        with rasterio.open(comid_rem_path, "w", **output_meta) as m:
            m.write(mosaic)
            print(f"++ Writing rem-based comids to {comid_rem_path}")

    # combine all comid files into one single raster and write out
    all_comid_files = list(Path(Output_dir).rglob('*_comid.tif'))
    raster_to_mosiac = []

    for p in all_comid_files:
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
    comid_path = os.path.join(output_dir,"comid.tif")
    with rasterio.open(comid_path, "w", **output_meta) as m:
        m.write(mosaic)
        print(f"** Writing final comid mosaic to {comid_path}")

    # finally delete unnecessary files to clean up
    for raster in raster_to_mosiac:
        raster.close()
    for p in all_comid_files:
        os.remove(p)
        #pass

    print(f"Removing {temp_sub_polygons} and {temp_proj_polygons}")
    temp_sub_poly_files = glob.glob(temp_sub_polygons.replace('.shp','.*'))
    temp_proj_poly_files = glob.glob(temp_proj_polygons.replace('.shp','.*'))
    print(temp_sub_poly_files)
    print(temp_proj_poly_files)
    for f in temp_sub_poly_files:
        os.remove(f)
    for f in temp_proj_poly_files:
        os.remove(f)

    print("TIME to finish raster creation: " +str(time.time()-start_time))
    print("*** Vectorizing COMIDs")
    vectorize(comid_path)
    print("TIME to finish vector creation: " +str(time.time()-start_time))
    """

 
def __init_and_run(huc_num,
                   r2f_huc_output_dir,
                   base_ras2fim_path = sv.DEFAULT_BASE_DIR,
                   national_ds_dir = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR ):

    

    ####################################################################
    ####  Some validation of input, but mostly setting up pathing ######
    # -b   (ie c:\ras2fim)
    if (os.path.isdir(base_ras2fim_path) == False):
        raise ValueError("the -bp arg (base path) does not appear to be a folder.")
    
    # -w   (ie 12090301)    
    if (len(huc_num) != 8):
        raise ValueError("the -w flag (HUC8) is not 8 characters long")
    if (huc_num.isnumeric() == False): # can handle leading zeros
        raise ValueError("the -w flag (HUC8) does not appear to be a HUC8")

    # -o  (ie C:\ras2fim_data\output_ras2fim_models\12090301_meters_2277_test_1)
    # The subfolders like 05_ and 06_ are referential from here.
    if (os.path.exists(r2f_huc_output_dir) == False):  # in case we get a full path incoming
        r2f_huc_output_dir = os.path.join(base_ras2fim_path, "output_ras2fim_models", r2f_huc_output_dir)
        # we don't need to validate the basic path and the child folder need not yet exist. We built
        # up the path ourselves.

    # -n  (ie: inputs\\X-National_Datasets)
    if (os.path.exists(national_ds_dir) == False):   # in case we get a full path incoming
        national_ds_dir = os.path.join(base_ras2fim_path, national_ds_dir)
        if (os.path.exists(national_ds_dir) == False): # fully pathed shoudl be ok, depending on their input value
            raise ValueError("the -n arg (national dataset) does not appear to be a folder.")

    make_catchments(huc_num, r2f_huc_output_dir, base_ras2fim_path, national_ds_dir)


if __name__=="__main__":

    # These paths should be rewired to point to the following:
    # Input directory - results from ras2fim Step 5 that contain depth grids
    # Output directory - desired output directory for feature ID files (currently points to ras2rem outputs)

    #Input_dir=r"C:\Users\laura.keys\Documents\starter_rem_data\ras2fim_for_ESIP_AWS\sample-dataset\output_iowa\05_hecras_output"
    #Output_dir=r"C:\Users\laura.keys\Documents\starter_rem_data\ras2fim_for_ESIP_AWS\sample-dataset\output_iowa\06_ras2rem_output"

    # delete this environment variable because the updated library we need
    # is included in the rasterio wheel
    try:
        del os.environ["PROJ_LIB"]
    except Exception as e:
        print(e)


    parser = argparse.ArgumentParser(description='========== Create catchments for specified existing output_ras2fim_models folder ==========')

    parser.add_argument('-w',
                        dest = "huc_num",
                        help = 'REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required = True,
                        type = str)

    parser.add_argument('-o',
                        dest = "r2f_huc_output_dir",
                        help = r'REQUIRED: The name of the huc output folder has to exist already in the outputs_ras2fim_models folder.'\
                               r' Example: my_12090301_test_2. It wil be added to the -bp (base path) and the' \
                               r' hardcoded value of ..ouput_ras2fim_models.. to become something like' \
                               r' c:\ras2fim_data\output_ras2fim_models\my_12090301_test_2.' \
                               r' NOTE: you can use a full path if you like and we will not override it.' \
                                ' Do not add the subfolder of 05_hecras_output, we will add that.',
                        required = True,
                        type = str)  # TODO: make this default to the outputs_ras2fim_models\{huc}_{units}_{crs} ??

    parser.add_argument('-bp',
                        dest = "base_ras2fim_path",
                        help = 'OPTIONAL: The base local of all of ras2fim folder (ie.. inputs, OWP_ras_models, output_ras2fim_models, etc).' \
                              r' Defaults to C:\ras2fim_data.',
                        required = False,
                        default = r"c:\ras2fim_data",
                        type = str)

    parser.add_argument('-n',
                        dest = "national_ds_dir",
                        help = r'OPTIONAL: path to national datasets: Example: \inputs\my_X-National_Datasets.' \
                               r' Defaults to \inputs\X-National_Datasets (and we and will add the "-bp" flag in front to becomes' \
                               r' C:\ras2fim_data\inputs\my_X-National_Datasetss (the defaults)).' \
                               r' NOTE: you can use a full path if you like and we will not override it.',                               
                        default = r'inputs\X-National_Datasets',
                        required = False,
                        type = str)

    args = vars(parser.parse_args())
    
    __init_and_run(**args)

