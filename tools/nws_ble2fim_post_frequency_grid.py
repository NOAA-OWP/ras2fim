#!/usr/bin/env python
# coding: utf-8

# This is a post processing script that takes the flood inundation grids from
# HEC-RAS 1D and finds the grid that matches the 1.5 year frequency  from the
# National Water Model 26 year retrospective analysis.  This is done on a
# 'feature-id' basis.  The output is a grid that matches most nearly the 1.5
# year frequency.
#
# Created by: Andy Carter, PE
# Last revised - 2021.04.06
#
# Post Processing

import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio as rio

import os.path

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT

# Path to walk for the rating curves
STR_PATH_CSV_FILES = r'E:\X-NWS\Output_20210322'

# Path to the shapefile of the National Water Model Streams - with attributes
STR_PATH_NWM_STREAMS_SHP = r'E:\X-NWS\HUC_12030106\Output_PreProcess_Conflation\12030106_nwm_streams_ln.shp'

# Path to write the requested grids
STR_PATH_BINARY_GRID = r'E:\X-NWS\Output_20210322_1_5_year'
# ~~~~~~~~~~~~~~~~~~~~~~~~


# ++++++++++++++++++++++++
def fn_find_nearest(array, value):
    # given an array, find the nearest point to value
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx
# ++++++++++++++++++++++++


# '''''''''''''''''''''''''''''''
def fn_split_path(split_path):
    # from a file path, split into a list
    list_path_split = []
    while os.path.basename(split_path):
        list_path_split.append(os.path.basename(split_path))
        split_path = os.path.dirname(split_path)
    list_path_split.reverse()
    return list_path_split
# '''''''''''''''''''''''''''''''


# load in the pre-processed Nation Water Model streams
# Contains the retrospective 26 year discharges
gdf_nwm_streams = gpd.read_file(STR_PATH_NWM_STREAMS_SHP)

# get a list of all CSV files (ending with 'rating_curve.csv') in a directory
list_files = []

for root, dirs, files in os.walk(STR_PATH_CSV_FILES):
    for file in files:
        if file.endswith(".csv") or file.endswith(".CSV"):
            # Note the case sensitive issue
            str_file_path = os.path.join(root, file)
            if str_file_path[-16:] == 'rating_curve.csv':
                list_files.append(str_file_path)

df_output = pd.DataFrame(columns=['feature_id',
                                  '1_5_year_NWM_cfs',
                                  '1_5_year_FIM_cfs',
                                  '1_5_year_FIM_Depth'])


for i in list_files:

    # create list of split file path
    list_path = fn_split_path(i)

    # get the feature_id from the file path split
    str_feature_id = str(list_path[-3])
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # query the geodataframe to single feature id
    gdf_feature_id = gdf_nwm_streams.query("feature_id==@str_feature_id")

    # get the retrospective discharge
    flt_freq_flow = gdf_feature_id.iloc[0]['1_5_year_r']

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # read the csv (rating curve) file into dataframe
    df = pd.read_csv(i)

    # convert flow column to numpy arrary
    arr_flows = df["Flow(cfs)"].to_numpy()

    # get the index of the nearest flow value
    int_profile_idx = fn_find_nearest(arr_flows, flt_freq_flow)

    # get averge depth of the cooresponding nearest flow
    flt_profile_depth = df["AvgDepth(ft)"][int_profile_idx]

    # multiply average depth times 10 and convert to integer
    # to match the output grid name
    int_profile_depth = int(flt_profile_depth * 10)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # get the drive name (Example E:\)
    str_file_path = i[0:2] + "\\\\"

    # Build file path (all except the last two items)
    for j in range(len(list_path) - 2):
        str_file_path += list_path[j] + "\\\\"

    # Create path to desired depth grid
    str_depth_grid_path = str_file_path + "Depth_Grids" + "\\\\"

    str_depth_grid_path += str_feature_id + "-"
    + str(int_profile_depth) + ".tif"

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    df_output = df_output.append({'feature_id': str_feature_id,
                                  '1_5_year_NWM_cfs': flt_freq_flow,
                                  '1_5_year_FIM_cfs': df["Flow(cfs)"][int_profile_idx],
                                  '1_5_year_FIM_Depth': int_profile_depth},
                                 ignore_index=True)

    # Open the grid, covert and save
    if flt_freq_flow > 0:
        if os.path.isfile(str_depth_grid_path):
            with rio.open(str_depth_grid_path) as src:
                depth = src.read()
                profile = src.profile

            # create numpy array 'a'
            a = depth

            # content deposition to reset cells
            a[depth <= 60000] = 0
            a[depth > 60000] = 1

            profile.update(dtype=rio.uint8, nodata=1, compress='lzw')

            str_output_dem = STR_PATH_BINARY_GRID + "\\"

            str_output_dem += str_feature_id + "-"
            + str(int_profile_depth) + "_1_5_year.tif"

            with rio.open(str_output_dem, 'w', nbits=1, **profile) as dest:
                dest.write(a.astype(np.uint8))
