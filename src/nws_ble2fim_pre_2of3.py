#!/usr/bin/env python
# coding: utf-8

# This is the second pre-processing tool that turns HEC-RAS 1D modeling into
# flood inundation mapping products.  This routine is used to conflate the
# national water model streams (feature_id) to the models provided in the
# supplied HEC-RAS files
#
# Created by: Andy Carter, PE
# Last revised - 2021.04.06
#
# PreProcessing - Part 2 of 3

# TODO - 20221.04.03 - Fix the hard coded EPSG

import geopandas as gpd
import pandas as pd
from geopandas.tools import sjoin

from shapely import wkt
from shapely.geometry import LineString, Point, mapping

import xarray as xr
import numpy as np

from fiona import collection
from fiona.crs import from_epsg

import time

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT

# Input - desired HUC 8
STR_HUC8 = "10170204"

# Input - projection of the base level engineering models
BLE_PRJ = "EPSG:26915"

# distance to buffer around modeled stream centerlines
INT_BUFFER_DIST = 300

# From pre-processing 1 of 3
STR_BLE_STREAM_LN = r"E:\X-IowaFloodCenter\RockValley_PreProcess\10170204_ble_streams_ln.shp"
STR_BLE_CROSS_SECTION_LN = r"E:\X-IowaFloodCenter\RockValley_PreProcess\cross_section_LN_from_ras.shp"

STR_OUT_PATH = r"C:\Junk\TestOutput"

# Input - Naitonal Datasets
STR_NATIONAL_DATASET_PATH = r"E:\X-NWS\X-National_Datasets"

# Input - Watershed boundary data geopackage
str_wbd_geopkg_path = STR_NATIONAL_DATASET_PATH + '\\' + 'NHDPlusV21_WBD.gpkg'

# Input - National Water Model stream lines geopackage
str_nwm_flowline_geopkg_path = STR_NATIONAL_DATASET_PATH
+ '\\' + 'nwm_flows.gpkg'

# Input - Recurrance Intervals netCDF
str_netcdf_path = STR_NATIONAL_DATASET_PATH + '\\'
+ 'nwm_v20_recurrence_flows.nc'

# ~~~~~~~~~~~~~~~~~~~~~~~~

# ````````````````````````
# OPTIONS
# option to see all columns of pandas
pd.set_option('display.max_columns', None)

# option to see all columns of pandas
pd.set_option('display.max_rows', None)

# option to turn off the SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

# Geospatial projections
wgs = "epsg:4326"
lambert = "epsg:3857"
nwm_prj = "ESRI:102039"
# ````````````````````````

# Load the geopackage into geodataframe - 1 minute +/-
gdf_ndgplusv21_wbd = gpd.read_file(str_wbd_geopkg_path)

list_huc8 = []
list_huc8.append(STR_HUC8)

# get only the polygons in the given HUC_8
gdf_huc8_only = gdf_ndgplusv21_wbd.query("HUC_8==@list_huc8")
gdf_huc8_only_nwm_prj = gdf_huc8_only.to_crs(nwm_prj)
gdf_huc8_only_ble_prj = gdf_huc8_only.to_crs(BLE_PRJ)

# path of the shapefile to write
str_huc8_filepath = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_huc_12_ar.shp"

# write the shapefile
gdf_huc8_only_ble_prj.to_file(str_huc8_filepath)


# Overlay the BLE streams (from the HEC-RAS models) to the HUC_12 shapefile

# read the ble streams
gdf_ble_streams = gpd.read_file(STR_BLE_STREAM_LN)

# clip the BLE streams to the watersheds (HUC-12)
gdf_ble_streams_intersect = gpd.overlay(
    gdf_ble_streams, gdf_huc8_only_ble_prj, how='intersection')

# path of the shapefile to write
str_filepath_ble_stream = STR_OUT_PATH
+ '\\' + str(list_huc8[0]) + "_ble_streams_ln.shp"

# write the shapefile
gdf_ble_streams_intersect.to_file(str_filepath_ble_stream)

# Get the NWM stream centerlines from the provided geopackage

# Union of the HUC-12 geodataframe - creates shapely polygon
shp_huc8_union_nwm_prj = gdf_huc8_only_nwm_prj.geometry.unary_union

# Create dataframe of the bounding coordiantes
tuple_watershed_extents = shp_huc8_union_nwm_prj.bounds

# Read Geopackage with bounding box filter
gdf_stream = gpd.read_file(str_nwm_flowline_geopkg_path,
                           bbox=tuple_watershed_extents)

# reanme ID to feature_id
gdf_stream = gdf_stream.rename(columns={"ID": "feature_id"})

# Load the netCDF file to pandas dataframe - 15 seconds
ds = xr.open_dataset(str_netcdf_path)
df_all_nwm_streams = ds.to_dataframe()

# get netCDF (recurrance interval) list of streams in the given huc
df_streams_huc_only = df_all_nwm_streams.query("huc8==@list_huc8")

# left join the recurrance stream table (dataFrame) with streams in watershed
# this will remove the streams not within the HUC-8 boundary
df_streams_huc_only = df_streams_huc_only.merge(gdf_stream,
                                                on='feature_id',
                                                how='left')

# Convert the left-joined dataframe to a geoDataFrame
gdf_streams_huc_only = gpd.GeoDataFrame(
    df_streams_huc_only, geometry=df_streams_huc_only['geometry'])

# Set the crs of the new geodataframe
gdf_streams_huc_only.crs = gdf_stream.crs

# project the nwm streams to the ble projecion
gdf_streams_nwm_bleprj = gdf_streams_huc_only.to_crs(BLE_PRJ)

# Create an empty dataframe
df_points_nwm = pd.DataFrame(columns=['geometry', 'feature_id', 'huc_12'])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create points at desired interval along each
# national water model stream
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
int_count = 0

# Multi-Linestrings to Linestrings
gdf_streams_nwm_explode = gdf_streams_nwm_bleprj.explode()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
distance_delta = 67   # distance between points in ble projection units
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

for index, row in gdf_streams_nwm_explode.iterrows():
    str_current_linestring = row['geometry']
    distances = np.arange(0, str_current_linestring.length, distance_delta)
    points = [
        str_current_linestring.interpolate(distance) for distance in distances]
    + [str_current_linestring.boundary[1]]

    for i in points:
        int_count += 1
        df_points_nwm = df_points_nwm.append({'geometry': i,
                                              'feature_id': row['feature_id'],
                                              'huc_12': row['huc12']},
                                             ignore_index=True)

        if (int_count % 500) == 0:
            print(int_count)

# convert dataframe to geodataframe
gdf_points_nwm = gpd.GeoDataFrame(df_points_nwm, geometry='geometry')

# Set the crs of the new geodataframe
gdf_points_nwm.crs = gdf_streams_nwm_bleprj.crs

# path of the shapefile to write
str_filepath_nwm_points = STR_OUT_PATH + '\\' + str(list_huc8[0])
+ "_nwm_points_PT.shp"

# write the shapefile
gdf_points_nwm.to_file(str_filepath_nwm_points)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# read in the model stream shapefile
gdf_segments = gpd.read_file(STR_BLE_STREAM_LN)

# create merged geometry of all streams
shply_line = gdf_segments.geometry.unary_union

# read in the national water model points
gdf_points = gdf_points_nwm

# reproject the points
gdf_points = gdf_points.to_crs(gdf_segments.crs)

# Estimated time = 45 sec

# buffer the merged stream ceterlines - distance to find valid conflation point
buff = shply_line.buffer(300)

# Estimated time = 1 min

# convert shapely to geoDataFrame
buff = gpd.GeoDataFrame(geometry=[buff])

# spatial join - points in polygon
points_in_poly = sjoin(gdf_points, buff, how='left')

# drop all points that are not within polygon
gdf_points_within_buffer = points_in_poly.dropna()

# need to reindex the returned geoDataFrame
gdf_points_within_buffer = gdf_points_within_buffer.reset_index()

# delete the index_right field
del gdf_points_within_buffer['index_right']

# Create an empty dataframe
df_points_snap_to_ble = pd.DataFrame(
    columns=['wkt_geom', 'feature_id', 'huc_12'])


# $$$$$$$$$$$$$$$$$$$$$$
def wkt_loads(x):
    try:
        return wkt.loads(x)
    except Exception:
        return None
# $$$$$$$$$$$$$$$$$$$$$$

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Snap the points along the National Water Model
# streams (within the buffer) to the nearest modeled
# HEC-RAS stream
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# TODO - can this be run in parallel for spped? 2021.04.02
# Estimated Time - One min for every 800 points

int_count = 0

for index, row in gdf_points_within_buffer.iterrows():
    int_count += 1

    if (int_count % 800) == 0:
        print(int_count)

    nwm_point = row['geometry']

    point_project_wkt = (
        shply_line.interpolate(shply_line.project(nwm_point)).wkt)

    df_points_snap_to_ble = df_points_snap_to_ble.append(
        {'wkt_geom': point_project_wkt,
         'feature_id': row['feature_id'],
         'huc_12': row['huc_12']},
        ignore_index=True)

df_points_snap_to_ble['geometry'] = df_points_snap_to_ble.wkt_geom.apply(wkt_loads)
df_points_snap_to_ble = df_points_snap_to_ble.dropna(subset=['geometry'])

# convert dataframe to geodataframe
gdf_points_snap_to_ble = gpd.GeoDataFrame(df_points_snap_to_ble,
                                          geometry='geometry')

# Set the crs of the new geodataframe
gdf_points_snap_to_ble.crs = gdf_segments.crs

# delete the wkt_geom field
del gdf_points_snap_to_ble['wkt_geom']

# write the shapefile
str_filepath_ble_points = STR_OUT_PATH + "\\"
+ str(list_huc8[0]) + "ble_snap_pionts_PT.shp"

gdf_points_snap_to_ble.to_file(str_filepath_ble_points)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Buffer the Base Level Engineering streams 0.1 feet (line to polygon)

gdf_segments_buffer = gdf_segments
gdf_segments['geometry'] = gdf_segments_buffer.geometry.buffer(0.1)

# Spatial join of the points and buffered stream

gdf_ble_points_feature_id = gpd.sjoin(
    gdf_points_snap_to_ble,
    gdf_segments_buffer[['geometry', 'ras_path']],
    how='left',
    op='intersects')

# delete the wkt_geom field
del gdf_ble_points_feature_id['index_right']

# Intialize the variable
gdf_ble_points_feature_id["count"] = 1

df_ble_guess = pd.pivot_table(gdf_ble_points_feature_id,
                              index=["feature_id", "ras_path"],
                              values=["count"],
                              aggfunc=np.sum)

df_test = df_ble_guess.sort_values('count')

str_csv_file = STR_OUT_PATH + '\\' + str(list_huc8[0])
+ "_interim_list_of_streams.csv"

# Write out the table - read back in
# this is to white wash the data type
df_test.to_csv(str_csv_file)
df_test = pd.read_csv(str_csv_file)

# Remove the duplicates and determine the feature_id with the highest count
df_test = df_test.drop_duplicates(subset='feature_id', keep="last")

# Left join the nwm shapefile and the
# feature_id/ras_path dataframe on the feature_id
gdf_nwm_stream_raspath = gdf_streams_nwm_bleprj.merge(df_test,
                                                      on='feature_id',
                                                      how='left')

# path of the shapefile to write
str_filepath_nwm_stream = STR_OUT_PATH + '\\'
+ str(list_huc8[0]) + "_nwm_streams_ln.shp"

# write the shapefile
gdf_nwm_stream_raspath.to_file(str_filepath_nwm_stream)

# Read in the ble cross section shapefile
gdf_ble_cross_sections = gpd.read_file(STR_BLE_CROSS_SECTION_LN)


str_xs_on_feature_id_pt = STR_OUT_PATH + '\\' + str(list_huc8[0])
+ "_nwm_points_on_xs_PT.shp"

# Create new dataframe
column_names = ["feature_id",
                "river",
                "reach",
                "us_xs",
                "ds_xs",
                "peak_flow",
                "ras_path"]

df_summary_data = pd.DataFrame(columns=column_names)

# Clear the dataframe and retain column names
df_summary_data = pd.DataFrame(columns=df_summary_data.columns)

# Loop through all feature_ids in the provided
# National Water Model stream shapefile

int_count = (len(gdf_nwm_stream_raspath))

for i in range(int_count):

    str_feature_id = gdf_nwm_stream_raspath.loc[[i], 'feature_id'].values[0]
    str_ras_path = gdf_nwm_stream_raspath.loc[[i], 'ras_path'].values[0]

    # get the NWM stream geometry
    df_stream = gdf_nwm_stream_raspath.loc[[i], 'geometry']

    # Select all cross sections in new dataframe where WTR_NM = strBLE_Name
    df_xs = gdf_ble_cross_sections.loc[
        gdf_ble_cross_sections['ras_path'] == str_ras_path]

    # Creates a set of points where the streams intersect
    points = df_stream.unary_union.intersection(df_xs.unary_union)

    if points.geom_type == 'MultiPoint':
        # Create a shapefile of the intersected points

        schema = {'geometry': 'Point', 'properties': {}}

        with collection(str_xs_on_feature_id_pt,
                        "w",
                        "ESRI Shapefile",
                        schema, crs=from_epsg(26915)) as output:
            # ~~~~~~~~~~~~~~~~~~~~
            # TODO - 2021.04.06 This is hard coded
            # ~~~~~~~~~~~~~~~~~~~~

            for i in points.geoms:
                output.write({'properties': {},
                              'geometry': mapping(Point(i.x, i.y))})

        df_points = gpd.read_file(str_xs_on_feature_id_pt)

        # SettingWithCopyWarning
        df_xs['geometry'] = df_xs.geometry.buffer(0.1).copy()

        df_point_feature_id = gpd.sjoin(df_points,
                                        df_xs[['geometry',
                                               'max_flow',
                                               'stream_stn',
                                               'river',
                                               'reach',
                                               ]],
                                        how='left', op='intersects')

        # determine Maximum and Minimum stream station
        flt_us_xs = df_point_feature_id['stream_stn'].max()
        flt_ds_xs = df_point_feature_id['stream_stn'].min()

        # determine the peak flow with this stream station limits
        flt_max_q = df_point_feature_id['max_flow'].max()

        str_river = df_point_feature_id['river'].values[0]
        str_reach = df_point_feature_id['reach'].values[0]

        # append the current row to pandas dataframe
        df_summary_data = df_summary_data.append({'feature_id': str_feature_id,
                                                  'river': str_river,
                                                  'reach': str_reach,
                                                  'us_xs': flt_us_xs,
                                                  'ds_xs': flt_ds_xs,
                                                  'peak_flow': flt_max_q,
                                                  'ras_path': str_ras_path},
                                                 ignore_index=True)

# Creates a summary document for each HUC-12.
# Check to see if matching model is found

str_str_qc_csv_File = STR_OUT_PATH + "\\" + STR_HUC8 + "_stream_qc.csv"
df_summary_data.to_csv(str_str_qc_csv_File)
