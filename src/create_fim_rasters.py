# Create flood inundation data from HEC-RAS
#
# Purpose:
# Create flood inundation rasters and supporting InFRM data from the
# preprocessed HEC-RAS geospatial 1D data.  This creates data per
# feature-id for the National Water Model
#
# Created by: Andy Carter, PE
# Created: 2021-08-12
# Last revised - 2021.10.24
#
# Uses the 'ras2fim' conda environment
# ************************************************************
import multiprocessing as mp
from multiprocessing import Pool
import pandas as pd
import geopandas as gpd
import os
import argparse

import time
import sys
import datetime

# ras2fim python worker for multiprocessing
import worker_fim_rasters
# ************************************************************

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@    
# Print iterations progress
def fn_print_progress_bar (iteration,
                           total,
                           prefix = '', suffix = '',
                           decimals = 0,
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


def fn_create_fim_rasters(str_desired_huc8,
                          str_input_folder,
                          str_output_folder,
                          str_projection_path,
                          str_terrain_path,
                          str_std_input_path,
                          flt_interval,
                          flt_out_resolution,
                          b_terrain_check_only):
    
    flt_start_create_fim = time.time()

    # Hard coded constants for this routine
    
    INT_XS_BUFFER = 2   # Number of XS to add upstream and downstream
    # of the segmented 
    
    # Constant - Toggle the Creation of RAS Map products
    IS_CREATE_MAPS = True
    
    # Constant - number of flood depth profiles to run on the first pass
    INT_NUMBER_OF_STEPS = 75
    
    # Constant - Starting flow for the first pass of the HEC-RAS simulation
    INT_STARTING_FLOW = 1
    
    # Constant - Maximum flow multiplier
    # up-scales the maximum flow from input
    FLT_MAX_MULTIPLY = 1.2
    
    # Constant - buffer of dem around floodplain envelope
    FLT_BUFFER = 15
    

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    print(" ")
    print("+=================================================================+")
    print("|                NWM RASTER LIBRARY FROM HEC-RAS                  |")
    print("+-----------------------------------------------------------------+")

    STR_HUC8 = str_desired_huc8
    print("  ---(w) HUC-8 WATERSHED: " + STR_HUC8)

    STR_INPUT_FOLDER = str_input_folder
    print("  ---(i) INPUT PATH: " + STR_INPUT_FOLDER)
    
    STR_ROOT_OUTPUT_DIRECTORY = str_output_folder
    print("  ---(o) OUTPUT PATH: " + STR_ROOT_OUTPUT_DIRECTORY)
    
    STR_PATH_TO_PROJECTION = str_projection_path
    print("  ---(p) PROJECTION PATH: " + STR_PATH_TO_PROJECTION)
    
    STR_PATH_TO_TERRAIN = str_terrain_path
    print("  ---(t) TERRAIN PATH: " + STR_PATH_TO_TERRAIN)
    
    STR_PATH_TO_STANDARD_INPUT = str_std_input_path
    print("  ---[s]   Optional: Standard Input Path: " + STR_PATH_TO_STANDARD_INPUT)
    
    # Path to the standard plan file text
    STR_PLAN_MIDDLE_PATH = STR_PATH_TO_STANDARD_INPUT + r'\PlanStandardText01.txt'
    STR_PLAN_FOOTER_PATH = STR_PATH_TO_STANDARD_INPUT + r"\PlanStandardText02.txt"
    STR_PROJECT_FOOTER_PATH = STR_PATH_TO_STANDARD_INPUT + r"\ProjectStandardText01.txt"
    
    FLT_INTERVAL = flt_interval
    print("  ---[z]   Optional: Output Elevation Step: " + str(FLT_INTERVAL))
    
    INT_DESIRED_RESOLUTION = flt_out_resolution
    print("  ---[r]   Optional: Output Raster Resolution: " + str(INT_DESIRED_RESOLUTION))
    
    INT_DESIRED_RESOLUTION = flt_out_resolution
    print("  ---[c]   Optional: Terrain Check Only: " + str(b_terrain_check_only))
    
    print("===================================================================")
    
    tpl_input = (STR_HUC8, STR_INPUT_FOLDER, 
                 STR_ROOT_OUTPUT_DIRECTORY, 
                 STR_PATH_TO_PROJECTION, 
                 STR_PATH_TO_TERRAIN,
                 STR_PLAN_MIDDLE_PATH, 
                 STR_PROJECT_FOOTER_PATH, 
                 FLT_INTERVAL,
                 INT_DESIRED_RESOLUTION,
                 INT_XS_BUFFER,
                 IS_CREATE_MAPS,
                 INT_NUMBER_OF_STEPS,
                 INT_STARTING_FLOW,
                 FLT_MAX_MULTIPLY,
                 FLT_BUFFER,
                 STR_PLAN_FOOTER_PATH,
                 b_terrain_check_only)

    
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
    
    # add the settings tuple
    df_streams_merge_2['settings'] = ''
    df_streams_merge_2['settings'] = df_streams_merge_2['settings'].astype(object)

    for index, row in df_streams_merge_2.iterrows():
        df_streams_merge_2.at[index, 'settings'] = tpl_input
    
    # create a pool of processors
    num_processors = (mp.cpu_count() - 1)
    with Pool(processes = num_processors) as executor:
        
        df_huc12 = gpd.read_file(str_huc12_area_shp)
        int_huc12_index = 0
        
        l = len(df_huc12)
        str_prefix = r"Processing HUCs (0 of " + str(l) + "):" 
        fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 27)
        
        # Loop through each HUC-12
        for i in df_huc12.index:
            str_huc12 = str(df_huc12['HUC_12'][i])
            int_huc12_index += 1
            #print(str_huc12)
            str_prefix = r"Processing HUCs (" + str(int_huc12_index) + " of " + str(l) + "):"
            fn_print_progress_bar(int_huc12_index, l, prefix = str_prefix , suffix = 'Complete', length = 27)
            
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
                                                    "huc12",
                                                    "settings"]]
        
            # create a list of lists from the dataframe
            list_of_lists_df_streams = df_streams_huc12_mod1.values.tolist()

            if len(list_of_lists_df_streams) > 0:
                output = executor.map(worker_fim_rasters.fn_main_hecras, list_of_lists_df_streams)
        
    tif_count = 0
    for root, dirs, files in os.walk(STR_ROOT_OUTPUT_DIRECTORY):
        for file in files:    
            if file.endswith('.tif'):
                tif_count += 1
    flt_end_create_fim = time.time()
    flt_time_create_fim = (flt_end_create_fim - flt_start_create_fim) // 1
    time_pass_create_fim = datetime.timedelta(seconds=flt_time_create_fim)
    
    print(" ") 
    print('ALL AREAS COMPLETE')
    print("Number of tif's generated: "+str(tif_count))
    print('Compute Time: ' + str(time_pass_create_fim))
    
    print("====================================================================")
    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='================ NWM RASTER LIBRARY FROM HEC-RAS ==================')
    
    parser.add_argument('-w',
                        dest = "str_desired_huc8",
                        help=r'REQUIRED: the desired huc-8 watershed: Example:  10170204',
                        required=True,
                        metavar='STRING',
                        type=str)

    parser.add_argument('-i',
                        dest = "str_input_folder",
                        help=r'REQUIRED: directory containing pre-processed (2 of 2) data: Example: D:\pre_2_output',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-o',
                        dest = "str_output_folder",
                        help=r'REQUIRED: path to write ras2fim output files: Example: D:\ras_output',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-p',
                        dest = "str_projection_path",
                        help=r'REQUIRED: path the to the projection file: Example: D:\pre_2_output\10170204_ble_streams_ln.prj',
                        required=True,
                        metavar='FILE',
                        type=str)
    
    parser.add_argument('-t',
                        dest = "str_terrain_path",
                        help=r'REQUIRED: path the to hdf5 terrain: Example: D:\04_hecras_terrain',
                        required=True,
                        metavar='FILE',
                        type=str)
    
    parser.add_argument('-s',
                        dest = "str_std_input_path",
                        help=r'  OPTIONAL: path the to the standard inputs: Example: C:\Users\civil\test1\ras2fim\src : Default: working directory',
                        required=False,
                        default=os.getcwd(),
                        metavar='FILE',
                        type=str)
    
    parser.add_argument('-z',
                        dest = "flt_interval",
                        help=r'  OPTIONAL: elevation interval of output grids: Example: 0.2 : Default: 0.5',
                        required=False,
                        default=0.5,
                        metavar='FLOAT',
                        type=float)
    
    parser.add_argument('-r',
                        dest = "flt_out_resolution",
                        help=r'  OPTIONAL: resolution of output grids: Example: 1.5 : Default: 3.0',
                        required=False,
                        default=3.0,
                        metavar='FLOAT',
                        type=float)
    
    parser.add_argument('-c',
                        dest = "b_terrain_check_only",
                        help='OPTIONAL: check terrain only-skip HEC-RAS simulation and mapping: Default=False',
                        required=False,
                        default=False,
                        metavar='T/F',
                        type=str2bool)
    
    args = vars(parser.parse_args())
    # --------------------------------
    
    str_desired_huc8 = args['str_desired_huc8']
    str_input_folder = args['str_input_folder']
    str_output_folder = args['str_output_folder']
    str_projection_path = args['str_projection_path']
    str_terrain_path = args['str_terrain_path']
    str_std_input_path = args['str_std_input_path']
    flt_interval = args['flt_interval']
    flt_out_resolution = args['flt_out_resolution']
    b_terrain_check_only = args['b_terrain_check_only']
    
    fn_create_fim_rasters(str_desired_huc8,
                          str_input_folder,
                          str_output_folder,
                          str_projection_path,
                          str_terrain_path,
                          str_std_input_path,
                          flt_interval,
                          flt_out_resolution,
                          b_terrain_check_only)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~