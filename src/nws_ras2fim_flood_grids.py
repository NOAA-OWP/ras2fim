# Create flood inundation data from HEC-RAS
#
# Purpose:
# Create flood inundation rasters and supporting InFRM data from the
# preprocessed HEC-RAS geospatial 1D data.  This creates data per
# feature-id for the National Water Model
#
# Created by: Andy Carter, PE
# Created: 2021-08-12
# Last revised - 2021.08.12
#
# Uses the 'ras2fim' conda environment
# ************************************************************
import multiprocessing as mp
from multiprocessing import Pool
import pandas as pd
import geopandas as gpd
import os

# ras2fim python worker for multiprocessing
import nws_ras2fim_worker
# ************************************************************

# Constant - Folder to write the HEC-RAS folders and files
STR_ROOT_OUTPUT_DIRECTORY = r'D:\Crap\test2'

# Input - desired HUC 8
STR_HUC8 = "10170204"

STR_INPUT_FOLDER = r"D:\Crap\conflation"

list_huc8 = []
list_huc8.append(STR_HUC8)

str_stream_csv = STR_INPUT_FOLDER + '\\' + str(list_huc8[0]) + "_stream_qc.csv"

str_stream_nwm_ln_shp = STR_INPUT_FOLDER + '\\' + str(list_huc8[0]) + "_nwm_streams_ln.shp"
str_huc12_area_shp = STR_INPUT_FOLDER + '\\' + str(list_huc8[0]) + "_huc_12_ar.shp"

# read the two dataframes
df_streams = gpd.read_file(str_stream_csv)
gdf_streams = gpd.read_file(str_stream_nwm_ln_shp)

# convert the df_stream 'feature_id' to int64
df_streams = df_streams.astype({'feature_id': 'int64'})

# left join on feature_id
df_streams_merge = pd.merge(df_streams, gdf_streams, on="feature_id")

# limit the fields
df_streams_merge_2 = df_streams_merge[['feature_id',
                                       'reach',
                                       'us_xs',
                                       'ds_xs',
                                       'peak_flow',
                                       'ras_path_x',
                                       'huc12']]

# rename the ras_path_x column to ras_path
df_streams_merge_2 = df_streams_merge_2.rename(
    columns={"ras_path_x": "ras_path"})

num_processors = (mp.cpu_count() - 1)
p = Pool(processes = num_processors)

df_huc12 = gpd.read_file(str_huc12_area_shp)
int_huc12_index = 0

# Loop through each HUC-12
for i in df_huc12.index:
    str_huc12 = str(df_huc12['HUC_12'][i])
    int_huc12_index += 1
    #print(str_huc12)
    
    # Constant - Folder to write the HEC-RAS folders and files
    str_root_folder_to_create = STR_ROOT_OUTPUT_DIRECTORY + '\\HUC_' + str_huc12
    
    # Select all the 'feature_id' in a given huc12
    df_streams_huc12 = df_streams_merge_2.query('huc12 == @str_huc12')

    # Reset the query index
    df_streams_huc12 = df_streams_huc12.reset_index()
    
    # Create a folder for the HUC-12 area
    os.makedirs(str_root_folder_to_create, exist_ok=True)
    
    # ammend the pandas dataframe
    df_streams_huc12_mod1 = df_streams_huc12 [["feature_id",
                                               "us_xs",
                                               "ds_xs",
                                               "peak_flow",
                                               "ras_path",
                                               "huc12"]]

    # create a list of lists from the dataframe
    list_of_lists_df_streams = df_streams_huc12_mod1.values.tolist()
    
    output = p.map(nws_ras2fim_worker.fn_main_hecras,list_of_lists_df_streams)
    
    print(str_huc12 + ": " + str(len(list_of_lists_df_streams)) + " runs")