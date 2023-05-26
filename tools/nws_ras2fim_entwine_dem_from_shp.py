# Purpose:
# From a user supplied area shapefile, get the Entwine 3DEP footprints from
# the hobu git-hub repo (geojson).  Using an Entwine pipeline, get the
# bare earth DEM from the requested area.
#
# Output generated:
# DEM of the requested area (first found entwine source only)
# for the first polygon in the user supplied area polygon.
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.08

import geopandas as gpd
import pandas as pd

import pdal
import json

import rasterio
from rasterio.fill import fillnodata

import rioxarray as rxr

import string
import random

import time
import datetime
import os

import argparse

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_get_random_string(int_letter_len_fn, int_num_len_fn):
    """Creates a random string of letters and numbers
    
    Keyword arguments:
    int_letter_len_fn -- length of string letters
    int_num_len_fn -- length of string numbers
    """
    letters = string.ascii_lowercase
    numbers = string.digits

    str_total = ''.join(random.choice(letters) for i in range(int_letter_len_fn))
    str_total += ''.join(random.choice(numbers) for i in range(int_num_len_fn))
    
    return str_total
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def fn_get_entwine_dem_from_shp(str_input_shapefile_path,
                                str_output_path,
                                b_in_feet,
                                flt_search_distance,
                                str_field_name,
                                int_bridge_lidar_class):
    
    wgs = "epsg:4326"
    lambert = "epsg:3857"
    
    if not os.path.exists(str_output_path):
        os.mkdir(str_output_path)
        
    # load the subject request shapefile
    gdf_boundary_prj = gpd.read_file(str_input_shapefile_path)
    
    str_crs_model = str(gdf_boundary_prj.crs)
    
    # convert the shp boundary to lambert projection
    gdf_boundary_lambert = gdf_boundary_prj.to_crs(lambert)
    
    # Get EPT limits from github repository
    str_hobu_footprints = r'https://raw.githubusercontent.com/hobu/usgs-lidar/master/boundaries/boundaries.topojson'
    gdf_entwine_footprints = gpd.read_file(str_hobu_footprints)
    
    # Set the entwine footprint CRS
    gdf_entwine_footprints = gdf_entwine_footprints.set_crs(wgs)
    
    # Convert the footprints to lambert
    gdf_entwine_footprints = gdf_entwine_footprints.to_crs(lambert)
    
    # ------------------------------------
    # determine if the naming field can be used in for the output DEMs
    b_have_valid_label_field = False
    
    # determine if the requested naming field is in the input shapefile
    if str_field_name in gdf_boundary_prj.columns:
        
        if len(gdf_boundary_prj) < 2:
            # no need to convert to series if there is just one polygon
            b_have_valid_label_field = True
            
        else:
            # create a dataframe of just the requested field
            gdf_just_req_field = pd.DataFrame(gdf_boundary_prj, columns = [str_field_name])
    
            # convert the dataframe to a series
            df_just_req_field_series = gdf_just_req_field.squeeze()
    
            # determine if the naming field is unique
            if df_just_req_field_series.is_unique:
                b_have_valid_label_field = True
            else:
                print('No unique values found.  Naming will be random')
    # ------------------------------------
    
    #for i in range(len(gdf_boundary_lambert)):
    int_count = 1
        
    for i in range(len(gdf_boundary_lambert)):
        
        print('Getting LiDAR: ' + str(int_count) + ' of ' + str(len(gdf_boundary_lambert)))
        int_count += 1
        
        gdf_boundary_lambert_single = gdf_boundary_lambert.iloc[i:i+1]
        
        # set the name from input field if available
        if b_have_valid_label_field:
            str_dem_ground = str_output_path + '\\' + str(gdf_boundary_lambert[str_field_name][i]) + '_1.tif'
        else:
            str_unique_tag = fn_get_random_string(2,4)
            str_dem_ground = str_output_path + '\\' + str_unique_tag + '_1.tif'
        
        # get just the current polygon
        
        gdf_boundary_lambert_single.crs = lambert
        
        # clip the footprints to the model limits boundary
        gdf_entwine_footprints_clip = gpd.overlay(gdf_entwine_footprints,
                                                  gdf_boundary_lambert_single,
                                                  how='intersection')
        
        # geometry from the requested polygon as wellKnownText
        boundary_geom_WKT = gdf_boundary_lambert['geometry'][i]  # to WellKnownText
        
        # the bounding box of the requested lambert polygon
        b = boundary_geom_WKT.bounds
        
        if len(gdf_entwine_footprints_clip) > 0:
            # found atleast one entwine dataset
            
            # the first found ept in the requested area
            # TODO - 2021.10.08 - MAC - what if you don't want the first dataset?
            ept_source = gdf_entwine_footprints_clip['url'][0]
            
            # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            # Entwine Pipeline
            
            # note: A square mile area takes about 3+ minutes
            #str_dem_ground = STR_OUTPUT_DIR + STR_SAVE_DEM_NAME + '_1.tif'
            
            str_limits = "Classification[2:2], Classification[" + str(int_bridge_lidar_class) + ":" + str(int_bridge_lidar_class) + "]"
            
            pipeline_dem_ground = {
                "pipeline": [
                    {
                        "type":"readers.ept",
                        'bounds':str(([b[0], b[2]],[b[1], b[3]])),
                        "filename":ept_source,
                        "threads":10,
                        "tag":"readdata"
                    },
                    {   
                        "type":"filters.crop",
                        'polygon':boundary_geom_WKT.wkt
                    },
                    {   
                        "type":"filters.range",
                        "limits": str_limits,
                        "tag":"Ground_Bridge"
                    },
                    {
                        "filename": str_dem_ground,
                        "gdalopts": "tiled=yes,     compress=deflate",
                        "inputs": [ "Ground_Bridge" ],
                        "nodata": -9999,
                        "output_type": "idw",
                        "resolution": 0.6,
                        "type": "writers.gdal"
                    }
                ]}
            
            # execute the pdal pipeline
            
            pipeline = pdal.Pipeline(json.dumps(pipeline_dem_ground))
            pipeline.validate()
            pipeline.execute()
            # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    
            str_dem_ground_filled = str_dem_ground[:-6] + '_2.tif'
            
            # --------------------------------------------------------
            # Fill the gaps of the Entwine DEM
            # Fill NoData cells via rasterio "fillnotdata" method
            with rasterio.open(str_dem_ground) as src:
                profile = src.profile
                arr = src.read(1)
                arr_filled = fillnodata(arr, mask=src.read_masks(1),
                                        max_search_distance=flt_search_distance,
                                        smoothing_iterations=0)
            
            with rasterio.open(str_dem_ground_filled, 'w', **profile) as dest:
                dest.write_band(1, arr_filled)
            
            src.close()
            os.remove(str_dem_ground)
            
            dest.close()
            
            # T/F of if this is closed
            # print(src.closed)
            # print(dest.closed)
            # --------------------------------------------------------
            
            # ************************************************
            # Using RioXarry - translate the DEM back to the requested shapefile's proj
            
            # read the DEM as a "Rioxarray"
            #with rxr.open_rasterio(src, masked=True).squeeze() as lidar_dem:
                
            lidar_dem = rxr.open_rasterio(str_dem_ground_filled, masked=True).squeeze()
            
            # reproject the raster to the same projection as the input shp
            lidar_dem = lidar_dem.rio.reproject(str_crs_model)
            
            if b_in_feet:
                # scale the raster from meters to feet
                lidar_dem = lidar_dem * 3.28084
            
            # write out the raster
            str_dem_out = str_dem_ground[:-6] + '.tif'
            
            #lidar_dem.rio.to_raster(str_dem_ground_filled,compress='LZW',dtype="float32")
            lidar_dem.rio.to_raster(str_dem_out,compress='LZW',dtype="float32")
                
            #lidar_dem.close()
            try:
                lidar_dem.close()
                print('Closed!')
            except:
                print('Did not close')
            # ************************************************
            
            # TODO - MAC - 2021.10.08 - Delete Error
            # os.remove(str_dem_ground_filled)
            
        else:
            print('Index: ' + str(i) + ' - No point clound found')
            
    del gdf_entwine_footprints_clip
    del gdf_boundary_lambert_single
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    
    flt_start_run = time.time()

    parser = argparse.ArgumentParser(description='=============== GET ENTWINE DEM FROM SHAPEFILE =============')
    
    parser.add_argument('-i',
                        dest = "str_input_shapefile_path",
                        help=r'REQUIRED: path to the rquested limits polygon shapefile Example: "C:\temp\terrain_limits_AR.shp" ',
                        required=True,
                        metavar='FILE',
                        type=str)
    
    
    parser.add_argument('-o',
                        dest = "str_output_path",
                        help=r'REQUIRED: directory to save terrain:  Example: D:\terrain',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-v',
                        dest = "b_in_feet",
                        help='OPTIONAL: create vertical data in feet: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    parser.add_argument('-d',
                        dest = "flt_search_distance",
                        help='OPTIONAL: create vertical data in feet: Default=25',
                        required=False,
                        default=25,
                        metavar='INT',
                        type=int)
    
    parser.add_argument('-f',
                        dest = "str_field_name",
                        help='OPTIONAL: unique field from input shapefile used for DEM name',
                        required=False,
                        default='None - will be random',
                        metavar='STRING',
                        type=str)
    
    parser.add_argument('-b',
                        dest = "int_bridge_lidar_class",
                        help='OPTIONAL: classification of bridge data in lidar: Default=13',
                        required=False,
                        default=13,
                        metavar='INT',
                        type=int)
    
    args = vars(parser.parse_args())
    
    str_input_shapefile_path = args['str_input_shapefile_path']
    str_output_path = args['str_output_path']
    b_in_feet = args['b_in_feet']
    flt_search_distance = args['flt_search_distance']
    str_field_name = args['str_field_name']
    int_bridge_lidar_class = args['int_bridge_lidar_class']
    
    print(" ")
    print("+=================================================================+")
    print("|                 GET ENTWINE DEM FROM SHAPEFILE                  |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(i) SHAPEFILE INPUT PATH: " + str_input_shapefile_path)
    print("  ---(o) DEM OUTPUT PATH: " + str_output_path)
    print("  ---[v]   Optional: VERTICAL IN FEET: " + str(b_in_feet)) 
    print("  ---[d]   Optional: SEARCH DISTANCE TO CLOSE DEM GAPS: " + str(flt_search_distance)) 
    print("  ---[f]   Optional: FIELD NAME: " + str(str_field_name))
    print("  ---[b]   Optional: LIDAR CLASSIFICATION: " + str(int_bridge_lidar_class))
    print("+-----------------------------------------------------------------+")

    
    fn_get_entwine_dem_from_shp(str_input_shapefile_path,
                                str_output_path,
                                b_in_feet,
                                flt_search_distance,
                                str_field_name,
                                int_bridge_lidar_class)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~