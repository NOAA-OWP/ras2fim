#!/usr/bin/env python
# coding: utf-8

# This is the third pre-processing tool that turns HEC-RAS 1D modeling into
# flood inundation mapping products.  This routine creates shapefiles to
# visually QC the stream conflation between the model stream shapefiles
# and the national water model stream shaepfiles
#
# Created by: Andy Carter, PE
# Last revised - 2021.04.06
#
# PreProcessing - Part 3 of 3

import pandas as pd
import geopandas as gpd


# option to see all columns of pandas
pd.set_option('display.max_columns', None)

# option to see all columns of pandas
pd.set_option('display.max_rows', None)

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT

# Input - desired HUC 8
STR_HUC8 = "10170204"

STR_OUT_PATH = r"C:\Junk\TestOutput"

# ~~~~~~~~~~~~~~~~~~~~~~~~

list_huc8 = []
list_huc8.append(STR_HUC8)

# Load the national water model lines

str_nwm_stream_lines_filepath = STR_OUT_PATH + '\\'
+ str(list_huc8[0]) + "_nwm_streams_ln.shp"

gdf_nwm_stream_lines = gpd.read_file(str_nwm_stream_lines_filepath)

# load the stream QC lines
str_stream_qc_filepath = STR_OUT_PATH + '\\'
+ str(list_huc8[0]) + "_stream_qc.csv"

df_processed_lines = pd.read_csv(str_stream_qc_filepath)

gdf_non_match = pd.merge(
    gdf_nwm_stream_lines,
    df_processed_lines,
    how='outer',
    indicator=True,
    on='feature_id')

gdf_non_match = gdf_non_match[(gdf_non_match._merge != 'both')]

# path of the shapefile to write
str_filepath_nwm_stream = STR_OUT_PATH + '\\'
+ str(STR_HUC8) + "_no_match_nwm_lines.shp"

# delete the wkt_geom field
del gdf_non_match['_merge']

# write the shapefile
gdf_non_match.to_file(str_filepath_nwm_stream)
