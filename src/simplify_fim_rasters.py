# Create simplified depth grid rasters from those created from HEC-RAS
#
# Purpose:
# Converts the depth grid terrains to smaller and more simple geotiffs.
# Can convert the projection and the resolution as requiested.  In 
# conformance with InFRM, created grids as 16 Bit Unsigned Integers with
# a specified NoData value
#
# Created by: Andy Carter, PE
# Created: 2021-08-23
# Last revised - 2021-09-09
#
# Uses the 'ras2fim' conda environment
# ************************************************************
import os
import re

import pandas as pd
import geopandas as gpd

import rioxarray as rxr
from rioxarray import merge

import argparse
# ************************************************************

# buffer distance of the input flood polygon - in CRS units
FLT_BUFFER = 15

# ~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_get_features(gdf, int_poly_index):
    """Function to parse features from GeoDataFrame
    in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][int_poly_index]['geometry']]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
def fn_filelist(source, tpl_extenstion):
    # walk a directory and get files with suffix
    # returns a list of file paths
    # args:
    #   source = path to walk
    #   tpl_extenstion = tuple of the extensions to find (Example: (.tig, .jpg))
    #   str_dem_path = path of the dem that needs to be converted
    matches = []
    for root, dirnames, filenames in os.walk(source):
        for filename in filenames:
            if filename.endswith(tpl_extenstion):
                matches.append(os.path.join(root, filename))
    return matches
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

# ---------------------------------
def fn_unique_list(list_input):
    # function to get a unique list of values 
    list_unique = []
    # traverse for all elements
    for x in list_input:
        # check if exists in list_unique or not
        if x not in list_unique:
            list_unique.append(x)
    return(list_unique)
# ---------------------------------

# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@    
# Print iterations progress
def fn_print_progress_bar (iteration,
                           total,
                           prefix = '', suffix = '',
                           decimals = 1,
                           length = 100, fill = '█',
                           printEnd = "\r"):
    """
    from: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    Call in a loop to create terminal progress bar
    Keyword arguments:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 

# **********************************
def fn_create_grid(str_polygon_path,
                   str_file_to_create_path,
                   str_dem_path,
                   str_output_crs,
                   flt_desired_res):
    # function to create InFRM compliant dems
    # args:
    #   str_polygon_path = path to boundary polygon from HEC-RAS
    #   str_file_to_create_path = path to of the file to create
    #   str_dem_path = path of the dem that needs to be converted
    
    # read in the HEC-RAS generatated polygon of the last elevation step
    gdf_flood_limits = gpd.read_file(str_polygon_path)

    # buffer the geodataframe of the flood polygon
    gdf_flood_buffer = gdf_flood_limits.buffer(FLT_BUFFER, 8)

    # get a geodataframe envelope (bounding box)
    gdf_flood_depth_envelope = gdf_flood_buffer.envelope

    # get the coordinates of the envelope
    # note - this is pulling the first polygon found
    coords = fn_get_features(gdf_flood_depth_envelope, 0)

    with rxr.open_rasterio(str_dem_path,masked=True,).rio.clip(coords, from_disk=True) as xds_clipped:
        
        # using rioxarray - clip the HEC-RAS dem to the bounding box
        #xds_clipped = rxr.open_rasterio(str_dem_path,masked=True,).rio.clip(coords, from_disk=True)

        # reproject the DEM to the requested CRS
        xds_clipped_reproject = xds_clipped.rio.reproject(str_output_crs)

        # change the depth values to integers representing 1/10th interval steps
        # Example - a cell value of 25 = a depth of 2.5 units (feet or meters)
        xds_clipped_reproject_scaled = ((xds_clipped_reproject * 10) + 0.5) // 1

        # set the n/a cells to a value of 65535
        xds_clipped_reproject_scaled = xds_clipped_reproject_scaled.fillna(65535)

        # set the nodata value to 65535 - InFRM compliant
        xds_clipped_reproject_scaled = xds_clipped_reproject_scaled.rio.set_nodata(65535)

        # using the merge on a single raster to allow for user supplied
        # raster resolution of the output
        xds_clipped_desired_res = rxr.merge.merge_arrays(xds_clipped_reproject_scaled,
                                                         res=(flt_desired_res),
                                                         nodata=(65535))

        # compress and write out raster as unsigned 16 bit integer - InFRM compliant
        xds_clipped_desired_res.rio.to_raster(str_file_to_create_path,
                                              compress='lzw',
                                              dtype="uint16")
# **********************************

def fn_simplify_fim_rasters(str_input_dir,
                            flt_resolution,
                            str_output_crs):

    print(" ")
    print("+=================================================================+")
    print("|                SIMPLIFIED GEOTIFFS FOR RAS2FIM                  |")
    print("|   Created by Andy Carter, PE of the National Water Center       |")
    print("+-----------------------------------------------------------------+")

    STR_MAP_OUTPUT = str_input_dir
    print("  ---(i) INPUT PATH: " + str(STR_MAP_OUTPUT))
    
    # output interval of the flood depth raster - in CRS units
    FLT_DESIRED_RES = flt_resolution
    print("  ---[r]   Optional: RESOLUTION: " + str(FLT_DESIRED_RES)) 
    
    # requested tile size in lambert units (meters)
    STR_OUTPUT_CRS = str_output_crs
    print("  ---[p]   Optional: PROJECTION OF OUTPUT: " + str(STR_OUTPUT_CRS)) 

    print("===================================================================")

    # get a list of all tifs in the iprovided input directory
    list_raster_dem = fn_filelist(STR_MAP_OUTPUT, ('.TIF', '.tif'))
    
    # create a list of the path to the found tifs
    list_paths = []
    for i in range(len(list_raster_dem)):
        list_paths.append(os.path.split(list_raster_dem[i])[0])
    
    # list of the unique paths to tif locations
    list_unique_paths = []
    list_unique_paths = fn_unique_list(list_paths)
    
    # create a blank pandas dataframe
    list_col_names = ['shapefile_path', 'file_to_create_path', 'current_dem_path']
    df_grids_to_convert = pd.DataFrame(columns = list_col_names)
    
    # for regular expression - everything between quotes
    pattern_parenth = "\((.*?)\)"
    
    for i in list_unique_paths:
        list_current_tifs = []
        
        # get a list of all the tifs in the current path
        list_current_tifs = fn_filelist(i, ('.TIF', '.tif'))
        
        b_create_dems = False
        
        # create a list of all the paths for these tifs
        list_check_path =  []
        for j in list_current_tifs:
            list_check_path.append(os.path.split(j)[0])
        
        # determine if the path to all the tifs are the same (i.e. no nesting)
        list_check_path_unique = fn_unique_list(list_check_path)
        if len(list_check_path_unique) == 1:
            
            b_create_dems = True
            
            # determine the COMID
            list_path_parts = list_check_path_unique[0].split(os.sep)
            str_current_comid = list_path_parts[-3]
            
            # directory to create output dem's
            str_folder_to_create = '\\'.join(list_path_parts[:-2])
            str_folder_to_create = str_folder_to_create + '\\' + 'Depth_Grid'
            os.makedirs(str_folder_to_create, exist_ok=True)
            
        else:
            str_current_comid = ''
            # there are nested TIFs, so don't process
            
        if b_create_dems:
            # determine the path to the boundary shapefile
            list_shp_path = []
            list_shp_path = fn_filelist(i, ('.SHP', '.shp'))
            
            if len(list_shp_path) == 1:
                str_shp_file_path = list_shp_path[0]
            
                for k in list_current_tifs:
    
                    # parse the step of the grid contained in () on filename
                    str_file_name = os.path.split(k)[1]
                    str_dem_name = re.search(pattern_parenth, str_file_name).group(1)
    
                    # parse to digits only
                    str_dem_digits_only = re.sub("[^0-9]","",str_dem_name)
    
                    # remove leading zeros
                    if str_dem_digits_only[0] == "0":
                        str_dem_digits_only = str_dem_digits_only[1:]
                    
                    # file path to write file
                    str_create_filename = str_folder_to_create + "\\" + str_current_comid + '-' +  str_dem_digits_only + '.tif'
                    
                    # create the converted grids
                    dict_new_row = {'current_dem_path':k,
                                    'file_to_create_path':str_create_filename,
                                    'shapefile_path':str_shp_file_path}
                    
                    df_grids_to_convert = df_grids_to_convert.append(dict_new_row, ignore_index=True)    
            else:
                #no shapefile in the dem path found
                b_nothing = True
                
    int_count = 0
    
    # Initial call to print 0% progress
    l = len(df_grids_to_convert)
    print(' ')
    
    str_prefix = "Converting " + str(len(df_grids_to_convert)) + " grids"
    fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 28)
    
    for index, row in df_grids_to_convert.iterrows():
        # TODO - multiprocess the conversion
        fn_create_grid(row['shapefile_path'],
                       row['file_to_create_path'],
                       row['current_dem_path'],
                       STR_OUTPUT_CRS,
                       FLT_DESIRED_RES)
        int_count += 1
        
        fn_print_progress_bar(int_count, l, prefix = str_prefix, suffix = 'Complete', length = 28)

    print(" ") 
    print('COMPLETE')
    print("====================================================================")
    
if __name__ == '__main__':

    
    parser = argparse.ArgumentParser(description='===== CREATE SIMPLIFIED FLOOD DEPTH RASTER FILES (TIF) =====')

    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: directory containing RAS2FIM output:  Example: C:\HUC_10170204',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-r',
                        dest = "flt_resolution",
                        help='OPTIONAL: resolution of output raster (crs units): Default=3',
                        required=False,
                        default=3,
                        metavar='FLOAT',
                        type=float)
    
    parser.add_argument('-p',
                        dest = "str_output_crs",
                        help='OPTIONAL: output coordinate reference zone: Default=EPSG:3857',
                        required=False,
                        default="EPSG:3857",
                        metavar='STRING',
                        type=str)

    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    flt_resolution = args['flt_resolution']
    str_output_crs = args['str_output_crs']
    
    fn_simplify_fim_rasters(str_input_dir,
                            flt_resolution,
                            str_output_crs)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~