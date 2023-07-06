
#!/usr/bin/env python3

import os

import argparse
import datetime as dt
import fiona
import fiona.transform
import geopandas as gpd
import numpy as np
import pandas as pd
import shutil
import sys
import time

import multiprocessing as mp
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor, as_completed
import tqdm

import rasterio
import rasterio.mask
from rasterio.merge import merge
from rasterio import features

import shared_variables as sv
import shared_functions as sf

from pathlib import Path
from shapely.geometry import Polygon
from functools import partial


####################################################################
# This function creates a geopackage of Feature IDs from the raster that
# matches the Depth Grid processing extents ("maxments") from ras2fim Step 5.
def vectorize(mosaic_features_raster_path, changelog_path, model_huc_catalog_path, rating_curve_path):


    if (os.path.exists(rating_curve_path) == False):
        raise Exception(f"The file rating_curve.csv does not exist. It needs to be in the {sv.R2F_OUTPUT_DIR_RAS2REM} subfolder." \
                        f" Ensure ras2fim has been run and created a the file.")

    # as in the r2f_features_{huc_num}.tif  (we can assume it is here are we created it in this file)
    with rasterio.open(mosaic_features_raster_path) as r2f_features_src:
        rast = r2f_features_src.read()

    # -------------------
    # SETUP DATA FROM FEATURE ID RASTER
    shapes = features.shapes(rast, transform = r2f_features_src.transform)

    geoms = []
    vals = []
    for shp, val in shapes:
      geoms.append(Polygon(shp["coordinates"][0]))
      vals.append(val) # feature IDs

    # -------------------
    # Dataframe of Feature ID values to complement the geometries of the polygons derived from raster
    df = pd.DataFrame(vals)
    df.columns = ["feature_id"]
    df["HydroID"] = vals
    
    # -------------------
    # We pass in (potentially) many different depths for each Feature ID, so dissolve them together
    gdf = gpd.GeoDataFrame(df, crs = r2f_features_src.crs, geometry=geoms)
    gdf = gdf.dissolve('feature_id') # ensure that we just have one big polygon per feature ID

    # -------------------
    # rating_curve DATA
    # Load the rating curve first so we always keep the stage values
    # Because ras2fim loads from the 'models' directory and not the model_catalog, it is possible to have
    # rating curve values with no entry in models_catalog which means we are missing some meta data
    # The key is a left join with the rating curve as the base
    rc_df = pd.read_csv(rating_curve_path)

    # Get min and max stage and discharge for each Feature ID, then create fields for stage and flow mins and maxs
    agg_df = rc_df.groupby(["feature_id"]).agg({'stage_m': ['min', 'max'], 'discharge_cms': ['min','max']})
    
    agg_df['start_stage'] = agg_df['stage_m']['min']
    agg_df['end_stage'] = agg_df['stage_m']['max']
    agg_df['start_flow'] = agg_df['discharge_cms']['min']
    agg_df['end_flow'] = agg_df['discharge_cms']['max']

    # Flatten from MultiIndex and keep relevant columns
    agg_df.columns = ["_".join(c).rstrip('_') for c in agg_df.columns.to_flat_index()]
    agg_df = agg_df[['start_stage','end_stage','start_flow','end_flow']]

    # -------------------
    # Join SRC data to polygon GeoDataFrame
    gdf = gdf.merge(agg_df, how="left", left_on="feature_id", right_on="feature_id")
    gdf = gdf.drop_duplicates()    

    # -------------------
    # Use the model data catalog to add data the metadata gkpg
    model_df = pd.read_csv(model_huc_catalog_path)
    gdf = gdf.merge(model_df, how="left", left_on="feature_id", right_on="nhdplus_comid")    

    # There are likely some HydroIDs that are associated with multiple model entries in the model catalog,
    # but we only want one result in our final outputs for now so that we don't confuse our Hydrovis end users.
    gdf = gdf.drop_duplicates(subset=["HydroID"])

    # drop the models_catalog columns we don't want
    # Yes. this might mean new columns in models_catalog will auto appear
    cols_to_drop = []
    if ('nhdplus_comid' in gdf.columns): cols_to_drop.append('nhdplus_comid')
    if ('g_file' in gdf.columns): cols_to_drop.append('g_file')
    if ('crs' in gdf.columns): cols_to_drop.append('crs')
    if ('nhdplus_inital_scrape_namecomid' in gdf.columns): cols_to_drop.append('inital_scrape_name')
    if ('nhdplus_initial_scrape_namecomid' in gdf.columns): cols_to_drop.append('initial_scrape_name')
    if ('notes' in gdf.columns): cols_to_drop.append('notes')
    gdf.drop(cols_to_drop, inplace=True, axis=1)    

    # fix the last_modified to utc not not linux time units
    gdf['last_modified'] = gdf['last_modified'].fillna(-1)  #model_catalog field
    try:
        gdf['last_modified'] = [dt.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d") for t in gdf['last_modified']]
    except Exception as e:
        print("Model catalog date issue. Moving on.")
        print(e)

    # this could change if the model was updated and re-ran against this huc
    gdf.rename(columns = {'last_modified':'hecras_model_last_modified'}, inplace=True)

    # populate the rating_curve field with today's utc date
    gdf['last_updated'] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    # renamed a few columns
    gdf.rename(columns = {'last_updated':'ras2fim_processed_date'}, inplace=True)

    # -------------------
    # Placeholders for other metadata
    # More meta data coming shortly. It will be named models_catalog_{huc}.csv from whatever path
    # has been defined as the OWP_ras_models (c:/ras2fim_data/OWP_ras_models is the default)
    
    gdf['ras_model_type'] = ['1D'] * len(gdf)
    gdf['model_date'] = ["Unknown"] * len(gdf) # where would we find this in the model data?
    gdf['flow_match_score'] = [0] * len(gdf)
    gdf['terrain_match_score'] = [0] * len(gdf)
    gdf['other_notes'] = [""] * len(gdf)

    gdf['ras2fim_version'] = [sf.get_changelog_version(changelog_path)] * len(gdf)

    # ... then we should have all the metadata we need on a per-catchment basis 
    print("** Writing out updated catchments geopackage **")
    gdf.to_file(mosaic_features_raster_path.replace(".tif", "_meta.gpkg"), driver="GPKG", layer="catchments")

    return

####################################################################
# For each feature ID, finds the path for the tif with the max depth value
def __get_maxment(mxmt_args):

    # Becuase of the way we are doing tdqm and multi-proc it can only take one arg but we can make it a positonal list.
    feature_id = mxmt_args[0]
    reproj_nwm_filtered_df = mxmt_args[1]
    r2f_hecras_dir = mxmt_args[2]
    datatyped_rems_dir = mxmt_args[3]

    feature_catchment_df = reproj_nwm_filtered_df[reproj_nwm_filtered_df.ID == feature_id]

    # Pull all relevant depth grid tiffs for this feature ID
    this_feature_id_tif_files = list(Path(r2f_hecras_dir).rglob(f'*/Depth_Grid/{feature_id}-*.tif'))

    # easier to watch progress but can't take last record as the numerics are not zero padded.
    this_feature_id_tif_files.sort()

    # -------------------
    max_rem = -1
    max_rem_file = ""
    # Look through all depth grid tiffs associated with this feature ID and find the max depth value and file
    for p in this_feature_id_tif_files:
        rem = int(str(p).split('\\')[-1].split('-')[1].rstrip('.tif')) # get rem value from filename
        if(rem > max_rem):
            max_rem = rem
            max_rem_file = p

    feature_max_depth_raster = rasterio.open(max_rem_file)

    # -------------------
    # ... apply polygon as mask to initial raster. Will only pull one feature ID at a time. 
    # Clips depth grid to catchment boundary to create "maxment"
    masked_raster, transform = rasterio.mask.mask(feature_max_depth_raster,
                                                  feature_catchment_df.geometry, 
                                                  nodata = feature_max_depth_raster.nodata, 
                                                  filled = True, 
                                                  invert = False)

    # -------------------
    # Force datatypes to be 32-bit integers
    long_raster_vals = np.array(masked_raster, dtype = np.int32)
    long_raster_vals[long_raster_vals != feature_max_depth_raster.nodata] = np.int32(feature_id)
        
    # -------------------
    # write 32-bit integer versions of our feature ID data, to ensure datatyping
    feature_id_rem_path = os.path.join(datatyped_rems_dir, "datatyped_{}_{}.tif".format(feature_id, max_rem))
    output_meta = feature_max_depth_raster.meta.copy()
    output_meta.update({"dtype":np.int32})
    with rasterio.open(feature_id_rem_path, "w", **output_meta) as m:
        m.write(long_raster_vals)

    return feature_id_rem_path


####################################################################
####  Some validation of input, but setting up pathing ######
def __validate_make_catchments(huc_num,
                               r2f_huc_parent_dir,
                               model_huc_catalog_path,
                               national_ds_path):
    
    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    rtn_varibles_dict = {}

    # -------------------
    # -w   (ie 12090301)    
    if (len(huc_num) != 8):
        raise ValueError("the -w flag (HUC8) is not 8 characters long")
    if (huc_num.isnumeric() == False): # can handle leading zeros
        raise ValueError("the -w flag (HUC8) does not appear to be a HUC8")

    # -------------------
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
        raise FileNotFoundError('The -o arg (parent [ras2fim HUC output] ) folder does not exist. Please correct and retry.')
    rtn_varibles_dict["r2f_huc_parent_dir"] = r2f_huc_parent_dir

    # -------------------
    # -n  (ie: inputs\\X-National_Datasets) 
    if (os.path.exists(national_ds_path) == False):
        raise FileNotFoundError("the -n arg (inputs x national datasets path arg) does not exits. Please correct and retry.")

    # -------------------
    # the relavent WBD HUC8 gpkg
    wbd_huc8_dir = os.path.join(national_ds_path, sv.INPUT_WBD_HUC8_DIR)
    wbd_huc8_file = os.path.join(wbd_huc8_dir, f"HUC8_{huc_num}.gpkg")
    if (os.path.exists(wbd_huc8_file) == False):
        raise FileNotFoundError (f"The {wbd_huc8_file} file does not exist and is required.")
    rtn_varibles_dict["wbd_huc8_file"] = wbd_huc8_file

    # -------------------
    # The source nwm file
    src_nwm_catchments_file = os.path.join(national_ds_path, sv.INPUT_NWM_CATCHMENTS_FILE) 
    if (os.path.exists(src_nwm_catchments_file) == False):
        raise FileNotFoundError(f"The {src_nwm_catchments_file} file does not exist and is required.")
    rtn_varibles_dict["src_nwm_catchments_file"] = src_nwm_catchments_file

    # -------------------
    # AND the 05 directory must already exist 
    r2f_hecras_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    if (os.path.exists(r2f_hecras_dir) == False):
        raise FileNotFoundError(f"The ras2fim huc output, {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} subfolder does not exist." \
                        f" Ensure ras2fim has been run and created a valid {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} folder.")
    rtn_varibles_dict["r2f_hecras_dir"] = r2f_hecras_dir

    # -------------------
    # AND the 06 directory must already exist 
    r2f_ras2rem_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_RAS2REM)
    if (os.path.exists(r2f_ras2rem_dir) == False):
        raise FileNotFoundError(f"The ras2fim huc output, {sv.R2F_OUTPUT_DIR_RAS2REM} subfolder does not exist." \
                        f" Ensure ras2fim has been run and created a valid {sv.R2F_OUTPUT_DIR_RAS2REM} folder.")
    rtn_varibles_dict["r2f_ras2rem_dir"] = r2f_ras2rem_dir

    # -------------------
    # adjust the model_catalog file name if applicable
    if ("[]" in model_huc_catalog_path):
        model_huc_catalog_path = model_huc_catalog_path.replace("[]", huc_num)
    if (os.path.exists(model_huc_catalog_path) == False):
        raise FileNotFoundError (f"The -mc models catalog ({model_huc_catalog_path}) does not exist. Please check your pathing.")
    rtn_varibles_dict["model_huc_catalog_path"] = model_huc_catalog_path

    # -------------------
    # And the 07 catchments directory, just remove it if it already exists
    r2f_catchments_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_CATCHMENTS)
    if (os.path.exists(r2f_catchments_dir) == True):
        shutil.rmtree(r2f_catchments_dir)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(2) # 2 seconds

    os.mkdir(r2f_catchments_dir)
    rtn_varibles_dict["r2f_catchments_dir"] = r2f_catchments_dir

    # -------------------
    catchments_subset_file = os.path.join(r2f_catchments_dir, "nwm_catchments_subset.gpkg")
    rtn_varibles_dict["catchments_subset_file"] = catchments_subset_file

    # -------------------
    rating_curve_path = os.path.join(r2f_ras2rem_dir,'rating_curve.csv')
    rtn_varibles_dict["rating_curve_path"] = rating_curve_path

    # -------------------
    # only return the variables created or modified
    return rtn_varibles_dict


####################################################################
# This function geopackage of Feature IDs that correspond to the extent
# of the Depth Grids (and subsequent REMs that also match the Depth Grids)
def make_catchments(huc_num,
                    r2f_huc_parent_dir,
                    national_ds_path = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                    model_huc_catalog_path = sv.RSF_MODELS_CATALOG_PATH,
                    is_verbose = False):
    
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
        the "05_" depth grid tifs. 
        This value can be the value of just the the output_ras2fim huc subfolder, ie 12090301_meters_2277_test_3.
           (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)
        OR it can be a full path to the ras2fim huc output folder. ie) c:/my_ras2fim_outputs/12090301_meters_2277_test_3.
        Either way, it needs at least the populated 05_hecras_output subfolder.
            
    - national_ds_path : str
        This needs to be the full path of the X-National_Dataset folder. 
        
    - model_huc_catalog_path : str
        This is the location of a models_catalog.csv or a subset of it. When the new get models by huc comes online shortly, it
        will create a file called "OWP_ras_models_catalog_{HUC}.csv.  By  default, that will be used here with the pattern in code
        being "models_catalog_[].csv" and will use subsitution to replace the [] with the huc number. This is an optional
        input override but will default to c:/ras2fim_data/OWP_ras_models same as other systems.

    '''

    rtn_varibles_dict = __validate_make_catchments (huc_num,
                                                    r2f_huc_parent_dir,
                                                    model_huc_catalog_path,
                                                    national_ds_path)


    ####################################################################
    ####  Start processing ######
    start_dt = dt.datetime.now()    

    print(" ")
    print("+=================================================================+")
    print("|                 CREATE CATCHMENTS                               |")
    print("+-----------------------------------------------------------------+")
    print("  ---(w) HUC-8: " + huc_num)
    print("  ---(p) PARENT RAS2FIM HUC DIRECTORY: " + r2f_huc_parent_dir)
    print("  ---(n) PATH TO NATIONAL DATASETS: " + national_ds_path)    
    print("  ---(mc) PATH TO MODELS_CATALOG: " + rtn_varibles_dict.get("model_huc_catalog_path"))
    print("===================================================================")
    print(" ")


    print ("Getting list of feature IDs")

    # -------------------
    # Make a list of all tif file in the 05_  Depth_Grid (focusing on the depth not the feature ID)
    # TODO: We can change this to extract from the rating curve file.
    r2f_hecras_dir = rtn_varibles_dict.get("r2f_hecras_dir")
    all_depth_grid_tif_files=list(Path(r2f_hecras_dir).rglob('*/Depth_Grid/*.tif'))
    if (len(all_depth_grid_tif_files) == 0):
        raise Exception("No depth grid tif's found. Please ensure that ras2fim has been run and the 05_hecras_output" \
                        " has depth grid tifs in the pattern of {featureID-{depth value}.tif. ie) 5789848-18.tif")

    # -------------------
    # this is a list of partial file paths but stripping off the end of the file name after the feature id
    feature_id_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-2], all_depth_grid_tif_files))
    if (len(feature_id_values) == 0):
        # in case the file name pattern changed
        raise Exception("Feature Id's are extracted from depth grid tif file names using"\
                        " the pattern of {featureID-{depth value}.tif. ie) 5789848-18.tif." \
                        " No files found matching the pattern.")

    # -------------------
    # Make a list of all unique feature id,
    # strip out the rest of the path keeping just the feature ids
    all_feature_ids = list(set(feature_id_values))
    all_feature_ids = [int(str(x).split('\\')[-1].split('-')[0]) for x in all_feature_ids]
    
    num_features = len(all_feature_ids)
    if (num_features == 0):
        # in case the file name pattern changed
        raise Exception("Feature Id's are extracted from depth grid tif file names using"\
                        " the pattern of {featureID-{depth value}.tif. ie) 5789848-18.tif." \
                        " No files found matching the pattern.")

    print(f"The number of unique feature ID's is {num_features}")

    # The following file needs to point to a NWM catchments shapefile (local.. not S3 (for now))
    
    # -------------------    
    print()
    print("Subsetting NWM Catchments from HUC8, no buffer")
    print()

    # subset the nwm_catchments CONUS gkpg to the huc8 to speed it up
    huc8_wbd_db = gpd.read_file(rtn_varibles_dict.get("wbd_huc8_file"))
    huc8_nwm_df = gpd.read_file(rtn_varibles_dict.get("src_nwm_catchments_file"), mask = huc8_wbd_db)
    
    # -------------------    
    print("Getting all relevant catchment polys")
    print()
    filtered_catchments_df = huc8_nwm_df.loc[huc8_nwm_df['ID'].isin(all_feature_ids)]
    nwm_filtered_df = gpd.GeoDataFrame.copy(filtered_catchments_df)

    # -------------------
    # We need to project the output gpkg to match the incoming raster projection.
    print(f"Reprojecting filtered nwm_catchments to rem rasters crs")

    raster_crs = sv.DEFAULT_RASTER_OUTPUT_CRS

    # Let's create one overall gpkg that has all of the relavent polys, for quick validation
    reproj_nwm_filtered_df = nwm_filtered_df.to_crs(raster_crs)
    reproj_nwm_filtered_df.to_file(rtn_varibles_dict.get("catchments_subset_file"), driver='GPKG')

    # -------------------
    # Create folder for datatyped rems
    # We will of them for now and let the cleanup script remove them for debugging purposes
    datatyped_rems_dir = os.path.join(rtn_varibles_dict.get("r2f_catchments_dir"), "datatyped_feature_rems")
    if (os.path.exists(datatyped_rems_dir)):
        shutil.rmtree(datatyped_rems_dir)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(2) # 2 seconds

    os.mkdir(datatyped_rems_dir)

    # -------------------
    # get maxments.
    #rasters_to_mosaic = []

    # Create a single copy of a REM created by ras2rem so that we make sure our final feature ID tif is
    # really the same size (so that we're not off by a pixel)
    # add nodata REM file to list of rasters to merge, so that the final merged raster has same extent
    print("Creating rem extent file")
    r2f_rem_extent_path = None
    r2f_rem_path = os.path.join(rtn_varibles_dict["r2f_ras2rem_dir"],'rem.tif')
    if os.path.exists(r2f_rem_path): 
        r2f_rem_extent_path = os.path.join(rtn_varibles_dict["r2f_ras2rem_dir"], \
                                           'rem_extent_nodata.tif')
        with rasterio.open(r2f_rem_path) as src:
            rem_raster = src.read()
            # create a raster with exact dimesions of REM but with all nodata (yes, we could do this as 
            # a new numpy n-d array, but then we'd need to setup a lot of parameters to match those of 
            # rem_raster)
            rem_raster = np.where(np.isnan(rem_raster),65535,65535)
            output_meta = src.meta.copy()
            output_meta.update( { "dtype":np.int32, "compress":"LZW" })
            with rasterio.open(r2f_rem_extent_path, 'w', **output_meta) as dst:
                dst.write(rem_raster)
            #rasters_to_mosaic.append(r2f_rem_extent_path)
    else:
        raise Exception(f"{r2f_rem_path} doesn't exist")


    print("Getting maxment files")
    num_processors = (mp.cpu_count() - 1)
    
    # Create a list of lists with the mxmt args for the multi-proc
    mxmts_args = []
    for feature_id in all_feature_ids:
        mxmts_args.append([feature_id, reproj_nwm_filtered_df, r2f_hecras_dir, datatyped_rems_dir])


    rasters_paths_to_mosaic = []
    with Pool(processes = num_processors) as executor:    

        rasters_paths_to_mosaic = list(tqdm.tqdm(executor.imap(__get_maxment, mxmts_args),
                                        total = len(mxmts_args),
                                        desc = f"Processing maxments with {num_processors} workers",
                                        bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                        ncols=100 ))    


    rasters_paths_to_mosaic.append(r2f_rem_extent_path)

    # -------------------
    # Merge all the feature IDs' max depths together
    print("Merging all max depths")
    if (is_verbose):
        print(rasters_paths_to_mosaic)
    mosaic, output = merge(list(map(rasterio.open, rasters_paths_to_mosaic)), method="min")

    # Setup the metadata for our raster
    # use the firt raster's meta data
    with rasterio.open(rasters_paths_to_mosaic[0]) as feature_max_depth_raster:
        output_meta = feature_max_depth_raster.meta.copy()
        output_meta.update(
            {"driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": output,
            "dtype":np.int32,
            }
        )

    # -------------------
    # Write out the raster file which has just the selected features, which is one of our required outputs
    mosaic_features_raster_path = os.path.join(rtn_varibles_dict.get("r2f_catchments_dir"), "r2f_features.tif")
    with rasterio.open(mosaic_features_raster_path, "w", **output_meta, compress="LZW") as m:
        m.write(mosaic)
        print(f"** Writing final features mosaiced to {mosaic_features_raster_path}")

    # Ensure the metric columns exist in the meta file about to be created in vectorize
    r2f_ras2rem_dir = rtn_varibles_dict.get("r2f_ras2rem_dir")
    sf.convert_rating_curve_to_metric(r2f_ras2rem_dir)

    print("*** Vectorizing Feature IDs and creating metadata")
    model_huc_catalog_path = rtn_varibles_dict.get("model_huc_catalog_path")
    current_script_path = os.path.realpath(os.path.dirname(__file__))
    catalog_md_path = os.path.join(current_script_path, '..', 'doc', 'CHANGELOG.md')
    vectorize(mosaic_features_raster_path, catalog_md_path, model_huc_catalog_path, rtn_varibles_dict.get("rating_curve_path"))

    # -------------------    
    # Cleanup the temp files in datatyped_rems_dir, later this will be part of the cleanup system.
    if (is_verbose == False) and (os.path.exists(datatyped_rems_dir)):
            shutil.rmtree(datatyped_rems_dir)
   
    # -------------------    
    print()
    print("ras2catchment processing complete")
    sf.print_date_time_duration(start_dt, dt.datetime.now())
    print("===================================================================")    
    print("")    


####################################################################
if __name__=="__main__":


    # there is a known problem with rasterio and proj_db error
    # this will not stop all of the errors but some (in multi-proc)
    sf.fix_proj_path_error()


    # Sample usage:
    # Using all defaults:
    #     python ras2catchments.py -w 12090301 -o 12090301_meters_2277_test_22

    # Override every optional argument (and of course, you can override just the ones you like)
    #     python ras2catchments.py -w 12090301 -o C:\ras2fim_data_rob_folder\output_ras2fim_models_2222\12090301_meters_2277_test_2
    #          -n E:\X-NWS\X-National_Datasets -mc c:\mydata\robs_model_catalog.csv
    
    #  - The -p arg is required, but can be either a full path (as shown above), or a simple folder name.Either way, it must have the
    #        and the 05_hecras_output and 06_ras2rem folder and populated
    #        ie) -o c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2
    #            OR
    #            -o 12090301_meters_2277_test_2  (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_2)
    
    parser = argparse.ArgumentParser(description='========== Create catchments for specified existing output_ras2fim folder ==========')

    parser.add_argument('-w',
                        dest = "huc_num",
                        help = 'REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required = True,
                        type = str)

    parser.add_argument('-o',
                        dest = "r2f_huc_parent_dir",
                        help = r'REQUIRED: This can be used in one of two ways. You can submit either a full path' \
                               r' such as c:\users\my_user\Desktop\myoutput OR you can add a simple ras2fim output huc folder name.' \
                                ' Please see the embedded notes in the __main__ section of the code for details and  examples.',
                        required = True,
                        type = str) 

    parser.add_argument('-n',
                        dest = "national_ds_path",
                        help = r'OPTIONAL: path to national datasets: Example: E:\X-NWS\X-National_Datasets.' \
                               r' Defaults to c:\ras2fim_data\inputs\X-National_Datasets. This is needed to subset' \
                                ' the nwm_catchments.gkpg.',
                        default = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                        required = False,
                        type = str)

    parser.add_argument('-mc',
                        dest = "model_huc_catalog_path",
                        help = r'OPTIONAL: path to model catalog csv, filtered for the supplied HUC, file downloaded from S3.' \
                               r' Defaults to c:\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog_[].csv and will use subsitution'\
                               r' to replace the [] with the huc number.',
                        default = sv.RSF_MODELS_CATALOG_PATH,
                        required = False,
                        type = str)
    
    parser.add_argument('-v',
                        dest = "is_verbose",
                        help = 'OPTIONAL: if this flag is add (no value required), additional output files will be saved and extra' \
                               ' terminal window text will be displayed (for debugging purposes)',
                        default = False,
                        required = False,
                        action='store_true')    

    args = vars(parser.parse_args())
    
    make_catchments(**args)



