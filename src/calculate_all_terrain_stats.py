# This script is utilized to compute terrain comparison statistics to determine
# if the terrain acquired for mapping should be utilized for the generation of
# ras2fim raster depth grids.  It compares every cross section point of the
# HEC-RAS 1D model to the elevation of that point in the provided terrain.
# This script walks a directory to find all the "Rasmap" XML files and
# compute a file containing all the statistics data. (mean, median, std, etc.)
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.24
#
# Uses the 'ras2fim' conda environment

import xml.etree.ElementTree as et
import pathlib
import os

import h5py
import numpy as np

from shapely.geometry import LineString

import geopandas as gpd
import pandas as pd

import time
from time import sleep
import datetime

import rasterio

import multiprocessing as mp
import tqdm

import argparse


# -------------------------
def fn_get_abs_path(str_base_file_abs, str_relative_path):
    
    # convert the absolute input path to a list of parts
    tup_base_filepath = pathlib.Path(str_base_file_abs).parts

    # convert the relative input path to a list of parts
    tup_relative_filepath = pathlib.Path(str_relative_path).parts

    # count '..' in the relative list
    int_occurences = tup_relative_filepath.count('..')

    # add one (for file name) and multiply times (-1)
    int_up_folder = (int_occurences + 1) * (-1)
    
    # base path up to relevant folder + folder and file of relative path
    tup_abs_path = tup_base_filepath[:int_up_folder] + tup_relative_filepath[int_occurences:]
    
    # convert the tuple to a list
    list_abs_path = list(tup_abs_path)
    
    # everything but the drive letter
    str_revised_filepath = "\\".join(list_abs_path[1:])
    
    # drive letter plus the file
    str_revised_filepath = list_abs_path[0] + str_revised_filepath
    
    return str_revised_filepath
# -------------------------


# ~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_paths_from_rasmapper(str_rasmapper_path):

    xml_tree = et.parse(str_rasmapper_path)
    xml_root = xml_tree.getroot()
    
    # get the projection path
    str_projection_filepath = xml_tree.find('RASProjectionFilename').attrib['Filename']

    # get the terrain paths
    xml_terrains = xml_root.find('Terrains')
    list_terrain_paths = []
    for child_terrain in xml_terrains:
        list_terrain_paths.append(child_terrain.attrib['Filename'])

    # get the geom paths
    xml_geoms = xml_root.find('Geometries')
    list_geom_paths = []
    for child_geom in xml_geoms:
        list_geom_paths.append(child_geom.attrib['Filename'])
    
    # determine if prj file is relative or absolute
    if not os.path.isabs(str_projection_filepath):
        str_prj_path_abs = fn_get_abs_path(str_rasmapper_path, str_projection_filepath)
    else:
        str_prj_path_abs = str_projection_filepath
        
    # determine if geom file is relative or absolute
    if not os.path.isabs(list_geom_paths[0]):
        str_geom_path_abs = fn_get_abs_path(str_rasmapper_path, list_geom_paths[0])
    else:
        str_geom_path_abs = list_geom_paths[0]
        
    # determine if terrain file is relative or absolute
    if not os.path.isabs(list_terrain_paths[0]):
        str_terrain_path_abs = fn_get_abs_path(str_rasmapper_path, list_terrain_paths[0])
    else:
        str_terrain_path_abs = list_terrain_paths[0]
    
    # assume that the vrt is in the same folder
    str_terrain_path_abs_vrt = str_terrain_path_abs[:-4] + ".vrt"
    
    list_abs_paths = [str_geom_path_abs, str_prj_path_abs, str_terrain_path_abs_vrt]
    return list_abs_paths
# ~~~~~~~~~~~~~~~~~~~~~~~~~


# '''''''''''''''''''''''''
def fn_calculate_terrain_stats(str_geom_hdf_path,
                               str_projection_path,
                               str_terrain_path):

    hf = h5py.File(str_geom_hdf_path, 'r')

    # XY points of the plan view of the cross section
    arr_xs_points = hf.get('Geometry/Cross Sections/Polyline Points')
    arr_xs_points = np.array(arr_xs_points)
    
    # number of points per plan view cross section
    arr_pnts_per_xs = hf.get('Geometry/Cross Sections/Polyline Parts')
    arr_pnts_per_xs = np.array(arr_pnts_per_xs)
    
    # Attribute data of the cross section (reach, river, etc...)
    arr_xs_attrib = hf.get('Geometry/Cross Sections/Attributes')
    arr_xs_attrib = np.array(arr_xs_attrib)
    
    # number of points per cross section profile
    arr_xs_profile_num_points = hf.get('Geometry/Cross Sections/Station Elevation Info')
    arr_xs_profile_num_points = np.array(arr_xs_profile_num_points)
    
    # cross section station/ elevation values
    arr_xs_station_elev = hf.get('Geometry/Cross Sections/Station Elevation Values')
    arr_xs_station_elev = np.array(arr_xs_station_elev)

    hf.close()
    
    # Create an empty geopandas GeoDataFrame
    gdf_sta_elev_pnts = gpd.GeoDataFrame()
    
    gdf_sta_elev_pnts['geometry'] = None
    gdf_sta_elev_pnts['ras_elev'] = None
    
    gdf_prj = gpd.read_file(str_projection_path)
    gdf_sta_elev_pnts.crs = str(gdf_prj.crs)
    
    int_pnt = 0
    int_start_xs_pnt = 0

    for i in range(len(arr_pnts_per_xs)):
        str_current_xs = str(arr_xs_attrib[i][2].decode('UTF-8'))
        
        # -------------------------------------------
        #get a list of the plan cross section points
        int_pnts_in_plan_xs = arr_pnts_per_xs[i][1]
        int_end_xs_pnt = int_start_xs_pnt + int_pnts_in_plan_xs - 1
        
        list_line_points_x = []
        list_line_points_y = []
        
        for j in range(int_start_xs_pnt, int_end_xs_pnt + 1):
            list_line_points_x.append(arr_xs_points[j][0])
            list_line_points_y.append(arr_xs_points[j][1])
            
        list_xs_points = [xy for xy in zip(list_line_points_x,list_line_points_y)]
        geom_xs_linestring = LineString(list_xs_points)
        int_start_xs_pnt = int_end_xs_pnt + 1
        
        # ````````````````````````````````````````````````
        #get a list of the station - elevation points
        int_prof_xs_start_pnt = arr_xs_profile_num_points[i][0]
        int_prof_pnts_in_xs = arr_xs_profile_num_points[i][1]
        int_prof_xs_end_pnt = int_prof_xs_start_pnt + int_prof_pnts_in_xs
        
        list_xs_station = []
        list_xs_elevation = []
        
        for pnt_index in range(int_prof_xs_start_pnt, int_prof_xs_end_pnt):
            list_xs_station.append(arr_xs_station_elev[pnt_index][0])
            list_xs_elevation.append(arr_xs_station_elev[pnt_index][1])
            
        # sometimes the starting station is not zero
        # need to create separate list starting at zero for shapely interpolate
        list_xs_station_zeroed = [item - list_xs_station[0] for item in list_xs_station]
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # write the points to a point shapefile
        int_index = 0
        for sta in list_xs_station_zeroed:
            # use shapely to interpolate the point location on XS line from station
            geom_interp_pnt = geom_xs_linestring.interpolate(sta)
            flt_elev = list_xs_elevation[int_index]
    
            # add this point to a geopandas dataframe
            gdf_sta_elev_pnts.loc[int_pnt, 'geometry'] = geom_interp_pnt
            gdf_sta_elev_pnts.loc[int_pnt, 'ras_elev'] = flt_elev.item() # item() to convert from numpy value
            
            int_index += 1
            int_pnt += 1
    
    #print('Computing Statistics...')
    
    
    # create two new fields for coordinates
    gdf_sta_elev_pnts['x'] = gdf_sta_elev_pnts.geometry.x
    gdf_sta_elev_pnts['y'] = gdf_sta_elev_pnts.geometry.y
    
    coords =[(x,y) for x,y in zip(gdf_sta_elev_pnts.x, gdf_sta_elev_pnts.y)]
    
    # Sample the raster at every point location and store values in GeoDataFrame
    
    with rasterio.open(str_terrain_path) as terrain_src:
        gdf_sta_elev_pnts['dem_elev'] = [x[0] for x in terrain_src.sample(coords)]
    
    # remove all the points (dataframe rows) where the dem_elev is a nodata value
    # this means there is no surface below this point
    gdf_sta_elev_pnts = gdf_sta_elev_pnts[gdf_sta_elev_pnts.dem_elev != terrain_src.nodata]

    # reset the index of the points
    gdf_sta_elev_pnts = gdf_sta_elev_pnts.reset_index(drop=True)
    
    # delete the 'x' and 'y' fields from gdf
    del gdf_sta_elev_pnts['x']
    del gdf_sta_elev_pnts['y']
    
    # create difference in elevation value
    gdf_sta_elev_pnts['diff_elev'] = gdf_sta_elev_pnts['ras_elev'] - gdf_sta_elev_pnts['dem_elev']
    
    # recast variables to 'float32'
    gdf_sta_elev_pnts['ras_elev'] = pd.to_numeric(gdf_sta_elev_pnts['ras_elev'], downcast="float")
    gdf_sta_elev_pnts['dem_elev'] = pd.to_numeric(gdf_sta_elev_pnts['dem_elev'], downcast="float")
    gdf_sta_elev_pnts['diff_elev'] = pd.to_numeric(gdf_sta_elev_pnts['diff_elev'], downcast="float")
    
    # calculate statistics
    pd_series_stats = gdf_sta_elev_pnts['diff_elev'].describe()
    
    return (pd_series_stats)
# '''''''''''''''''''''''''


# <<<<<<<<<<<<<<<<<<<<<<<
def fn_get_list_of_lists_to_compute(str_path_ras_files):
    # walk a directory and get rasmapper paths
    list_files = []

    for root, dirs, files in os.walk(str_path_ras_files):
        for file in files:
            if file.endswith(".rasmap") or file.endswith(".RASMAP"):
                str_file_path = os.path.join(root, file)
                list_files.append(str_file_path)

    # get a list of lists of the files to process         
    list_of_lists_files_for_stats = []

    for str_rasmap_path in list_files:
        list_current_abs_paths = []

        # get the name of the input file (without extenstion)
        tup_filename = pathlib.Path(str_rasmap_path).parts[-1:]
        str_rasmodel_name = tup_filename[0][:-7]

        # run functiont o get paths (that are absolute)
        list_current_abs_paths = fn_paths_from_rasmapper(str_rasmap_path)

        # add the model name to the list
        list_current_abs_paths.append(str_rasmodel_name)

        # append to create a list of lists
        list_of_lists_files_for_stats.append(list_current_abs_paths)
    
    return(list_of_lists_files_for_stats)
# <<<<<<<<<<<<<<<<<<<<<<<


# ======================
def fn_get_stats_dataseries(list_files_for_stats):
    
    # get a dataseries of the requested data
    list_stats_dataseries = []

    str_path_shp_revised = list_files_for_stats[1][:-4] + '.shp'
    pd_series_stats = fn_calculate_terrain_stats(list_files_for_stats[0],
                                             str_path_shp_revised,
                                             list_files_for_stats[2])

    # change the rame to the ras model
    pd_series_stats.name = list_files_for_stats[3]

    # convert the index name to list
    #list_index = pd_series_stats.index.tolist()

    # create a data series of the the input data
    pd_series_info = pd.Series({'geom_hdf_path':list_files_for_stats[0],
                                'terrain_path':list_files_for_stats[2],
                                'model_name': list_files_for_stats[3]})

    # add the input data to the statistics data series
    pd_series_stats = pd.concat([pd_series_stats, pd_series_info])

    # convert data series to list
    list_stats_dataseries = pd_series_stats.tolist()
    
    sleep(0.01) # this allows the tqdm progress bar to update
    
    return(list_stats_dataseries)
# ======================


# ^^^^^^^^^^^^^^^^^^^^^^^
def fn_calculate_all_terrain_stats(str_input_dir):
    
    flt_start_run = time.time()
    
    print(" ")
    print("+=================================================================+")
    print("|    CALCULATE TERRAIN STATISTICS FOR MULTIPLE HEC-RAS MODELS     |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(i) RAS MAPPER DIRECTORY: " + str_input_dir)
    print("+-----------------------------------------------------------------+")
    
    list_of_list_processed = fn_get_list_of_lists_to_compute(str_input_dir)

    p = mp.Pool(processes = (mp.cpu_count() - 1))
    
    l = len(list_of_list_processed)
    list_return_values = list(tqdm.tqdm(p.imap(fn_get_stats_dataseries, list_of_list_processed),
                                        total = l,
                                        desc='Computing Stats',
                                        bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                        ncols=65))
    
    p.close()
    p.join()
    
    df_stats_values = pd.DataFrame(list_return_values)
    
    # rename columns
    column_names = ['count','mean','std',
                    'min', '25%', '50%',
                    '75%', 'max', 'geom_hdf_path',
                    'terrain_path', 'model_name']

    df_combined_stats = df_stats_values.set_axis(column_names, axis=1)
    
    # save to the same folder that was walked (str_input_dir)
    str_file_output = str_input_dir + "\\" + "terrain_stats.csv"
    df_combined_stats.to_csv(str_file_output)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print(' ')
    print('mean of mean terrain difference: ' + str(df_combined_stats['mean'].mean()))
    print(' ')
    print('Compute Time: ' + str(time_pass))

    print('COMPLETE')
    print("+-----------------------------------------------------------------+")
# ^^^^^^^^^^^^^^^^^^^^^^^


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='===== CALCULATE TERRAIN STATISTICS FOR MULTIPLE HEC-RAS MODELS =====')

    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: directory containing HEC-RAS Mapper Files:  Example: C:\HUC_10170204',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    
    fn_calculate_all_terrain_stats(str_input_dir)