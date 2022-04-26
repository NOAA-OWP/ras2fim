# This is the first pre-processing tool that turns HEC-RAS 1D modeling into
# flood inundation mapping products.  This routine takes the HEC-RAS models
# in a given directory and creates attributed shapefiles of the stream
# centerline and cross sections
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.31
#
# ras2fim - First pre-processing script
# Uses the 'ras2fim' conda environment

import re
import pandas as pd
import geopandas as gpd

import time
import datetime
from time import sleep

import argparse

import win32com.client
# windows component object model for interaction with HEC-RAS API
# This routine uses RAS60.HECRASController (HEC-RAS v6.0.0 must be
# installed on this machine prior to execution)

import h5py
# h5py for extracting data from the HEC-RAS g**.hdf files

import numpy as np

import os.path
from os import path

import multiprocessing as mp
from multiprocessing import Pool

from shapely.geometry import LineString
from shapely.ops import split, linemerge

from re import search
# ************************


def fn_open_hecras(str_ras_project_path):
    # Function - runs HEC-RAS (active plan) and closes the file

    hec = win32com.client.Dispatch("RAS60.HECRASController")
    #hec.ShowRas()

    # opening HEC-RAS
    hec.Project_Open(str_ras_project_path)

    # to be populated: number and list of messages, blocking mode
    NMsg, TabMsg, block = None, None, True

    # computations of the current plan
    # We need to compute.  Opening RAS Mapper creates the Geom HDF
    v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

    hec.QuitRas()   # close HEC-RAS
    
# ************************


# $$$$$$$$$$$$$$$$$$$$$$$$
def fn_get_active_geom(str_path_hecras_project_fn2):
    # Fuction - gets the path of the active geometry HDF file

    # read the HEC-RAS project file
    with open(str_path_hecras_project_fn2) as f:
        file_contents = f.read()

    # Find the current plan
    pattern = re.compile(r'Current Plan=.*')
    matches = pattern.finditer(file_contents)

    if re.search('Current Plan=', file_contents) is None:
        print(" -- ALERT: Reconnect files for "+str_path_hecras_project_fn2)
        raise SystemExit(0)

    # close the HEC-RAS project file
    f.close()

    for match in matches:
        str_current_plan = match.group(0)[-3:]

    str_path_to_current_plan = str_path_hecras_project_fn2[:-3] + \
        str_current_plan

    # read the current plan
    with open(str_path_to_current_plan) as f:
        file_contents = f.read()

    # Find the current geometry
    pattern = re.compile(r'Geom File=.*')
    matches = pattern.finditer(file_contents)

    # close the HEC-RAS plan file
    f.close()

    for match in matches:
        str_current_geom = match.group(0)[-3:]

    str_path_to_current_geom = str_path_hecras_project_fn2[:-3] + \
        str_current_geom

    return str_path_to_current_geom
# $$$$$$$$$$$$$$$$$$$$$$$$


# @@@@@@@@@@@@@@@@@@@@@@@@
def fn_geodataframe_cross_sections(str_path_hecras_project_fn, STR_CRS_MODEL):
    # Fuction - Creates a GeoDataFrame of the cross sections for the
    # HEC-RAS geometry file in the active plan

    str_path_to_geom_hdf = (fn_get_active_geom(str_path_hecras_project_fn)) + \
        '.hdf'

    if path.exists(str_path_to_geom_hdf):
        # open the geom hdf file
        hf = h5py.File(str_path_to_geom_hdf, 'r')
    else:
        # run hec-ras and then open the geom file
        fn_open_hecras(str_path_hecras_project_fn)
        hf = h5py.File(str_path_to_geom_hdf, 'r')

    # get data from HEC-RAS hdf5 files

    # XY points of the cross section
    n1 = hf.get('Geometry/Cross Sections/Polyline Points')
    n1 = np.array(n1)

    # point maker where each stream points start
    n2 = hf.get('Geometry/Cross Sections/Polyline Parts')
    n2 = np.array(n2)

    # Attribute data of the streams (reach, river, etc...)
    n3 = hf.get('Geometry/Cross Sections/Attributes')
    n3 = np.array(n3)

    # Error handleing: edge case, empty (bad) geo
    if n2.ndim == 0:
        return gpd.GeoDataFrame()

    # Create a list of  number of points per each stream line
    list_points_per_cross_section_line = []
    for row in n2:
        list_points_per_cross_section_line.append(row[1])

    # Get the name of the river, reach and station
    list_river_name = []
    list_reach_name = []
    list_station = []
    
    # Older geom hdf5 files do not have data in Geometry/Cross Sections/Attributes
    
    if n3.ndim > 0:
        # cross sections are in new hdf geom format
        for row in n3:
            list_river_name.append(row[0])
            list_reach_name.append(row[1])
            # Need to check for interpolated cross section
            # They end with a star
            str_xs_name = row[2]
            if str_xs_name[-1] == "*":
                # cross section is interpolated
                str_xs_name = str_xs_name[:-1]
            list_station.append(str_xs_name)
    else:
        # older hdf5 geom format
        n3_river_name = hf.get('Geometry/Cross Sections/River Names')
        n3_river_name = np.array(n3_river_name)
        for str_rivername in n3_river_name:
            list_river_name.append(str_rivername)
        
        n3_reach_name = hf.get('Geometry/Cross Sections/Reach Names')
        n3_reach_name = np.array(n3_reach_name)
        for str_reachname in n3_reach_name:
            list_reach_name.append(str_reachname)
        
        n3_stations = hf.get('Geometry/Cross Sections/River Stations')
        n3_stations = np.array(n3_stations)
        for str_station in n3_stations:
            str_xs_name = str_station
            if str_xs_name[-1] == "*":
                # cross section is interpolated
                str_xs_name = str_xs_name[:-1]
            list_station.append(str_xs_name)
        
    # Get a list of the points
    list_line_points_x = []
    list_line_points_y = []

    for row in n1:
        list_line_points_x.append(row[0])
        list_line_points_y.append(row[1])

    cross_section_points = [xy for xy in zip(list_line_points_x,
                                             list_line_points_y)]

    # Create an empty geopandas GeoDataFrame
    gdf_cross_sections = gpd.GeoDataFrame()
    gdf_cross_sections['geometry'] = None
    gdf_cross_sections['stream_stn'] = None
    gdf_cross_sections['river'] = None
    gdf_cross_sections['reach'] = None
    gdf_cross_sections['ras_path'] = None

    # set projection from input value
    gdf_cross_sections.crs = STR_CRS_MODEL

    # Loop through the cross section lines and create GeoDataFrame
    int_startPoint = 0
    i = 0

    for int_numPnts in list_points_per_cross_section_line:
        # Create linesting data with shapely
        gdf_cross_sections.loc[i, 'geometry'] = LineString(
            cross_section_points[int_startPoint:(int_startPoint+int_numPnts)])

        # River and Reach - these are numpy bytes and
        # need to be converted to strings
        # Note - HEC-RAS truncates values when loaded into the HDF

        gdf_cross_sections.loc[i,
                               'stream_stn'] = list_station[i].decode('UTF-8')

        gdf_cross_sections.loc[i, 'river'] = list_river_name[i].decode('UTF-8')
        gdf_cross_sections.loc[i, 'reach'] = list_reach_name[i].decode('UTF-8')

        str_path_to_geom = str_path_to_geom_hdf[:-4]
        gdf_cross_sections.loc[i, 'ras_path'] = str_path_to_geom

        i += 1
        int_startPoint = int_startPoint + int_numPnts

    return gdf_cross_sections
# @@@@@@@@@@@@@@@@@@@@@@@@



# ++++++++++++++++++++++++
def fn_geodataframe_stream_centerline(str_path_hecras_project_fn, STR_CRS_MODEL):
    # Function - Creates a GeodataFrame of the HEC-RAS stream centerline
    # for the geometry file in the active plan

    str_path_to_geom_hdf = (fn_get_active_geom(str_path_hecras_project_fn)) + \
        '.hdf'

    if path.exists(str_path_to_geom_hdf):
        # open the geom hdf file
        hf = h5py.File(str_path_to_geom_hdf, 'r')
    else:
        # run hec-ras and then open the geom file
        fn_open_hecras(str_path_hecras_project_fn)
        hf = h5py.File(str_path_to_geom_hdf, 'r')


    # XY points of the stream centerlines
    n1 = hf.get('Geometry/River Centerlines/Polyline Points')
    n1 = np.array(n1)

    # point maker where each stream points start
    n2 = hf.get('Geometry/River Centerlines/Polyline Parts')
    n2 = np.array(n2)

    # Error handleing: edge case, empty (bad) geo
    if n2.ndim == 0:
        return gpd.GeoDataFrame()

    # Get the name of the river and reach
    list_river_name = []
    list_reach_name = []
    
    # Attribute data of the streams (reach, river, etc...)
    n3 = hf.get('Geometry/River Centerlines/Attributes')
    n3 = np.array(n3)
    
    # TODO - MAC - 2021.10.31
    # Possible error with multiple rivers / reaches in older hdf5 geom
    
    if n3.ndim == 0:
        # some hdf files do not have Geometry/River Centerlines/Attributes
        # This is due to differences in the HEC-RAS versioning
        # Try an older hdf5 format for geom
        n3_reach = hf.get('Geometry/River Centerlines/Reach Names')
        n3_reach = np.array(n3_reach)
        
        n3_river = hf.get('Geometry/River Centerlines/River Names')
        n3_river = np.array(n3_river)
        
        # reach from older format
        if n3_reach.ndim == 0:
            list_reach_name.append('Unknown-not-found')
        else:
            list_reach_name.append(n3_reach[0])
        
        # river from older format
        if n3_river.ndim == 0:
            list_river_name.append('Unknown-not-found')
        else:
            list_river_name.append(n3_river[0])
    else:
        for row in n3:
            list_river_name.append(row[0])
            list_reach_name.append(row[1])
        

    # Create a list of  number of points per each stream line
    list_points_per_stream_line = []
    for row in n2:
        list_points_per_stream_line.append(row[1])

    # Get a list of the points
    list_line_points_x = []
    list_line_points_y = []

    for row in n1:
        list_line_points_x.append(row[0])
        list_line_points_y.append(row[1])

    stream_points = [xy for xy in zip(list_line_points_x, list_line_points_y)]

    # Create an empty geopandas GeoDataFrame
    gdf_streams = gpd.GeoDataFrame()
    gdf_streams['geometry'] = None
    gdf_streams['river'] = None
    gdf_streams['reach'] = None
    gdf_streams['ras_path'] = None

    # set projection from input value
    gdf_streams.crs = STR_CRS_MODEL

    # Loop through the stream centerlines and create GeoDataFrame
    int_startPoint = 0
    i = 0

    for int_numPnts in list_points_per_stream_line:
        # Create linesting data with shapely
        gdf_streams.loc[i, 'geometry'] = LineString(
            stream_points[int_startPoint:(int_startPoint+int_numPnts)])

        # Write the River and Reach - these are numpy bytes and need to be
        # converted to strings
        # Note - RAS truncates these values in the g01 and HDF files
        gdf_streams.loc[i, 'river'] = list_river_name[i].decode('UTF-8')
        gdf_streams.loc[i, 'reach'] = list_reach_name[i].decode('UTF-8')

        str_path_to_geom = str_path_to_geom_hdf[:-4]
        gdf_streams.loc[i, 'ras_path'] = str_path_to_geom

        i += 1
        int_startPoint = int_startPoint + int_numPnts

    return gdf_streams
# ++++++++++++++++++++++++


# ^^^^^^^^^^^^^^^^^^^^^^^^
def fn_get_active_flow(str_path_hecras_project_fn):
    # Fuction - gets the path of the active geometry HDF file

    # read the HEC-RAS project file
    with open(str_path_hecras_project_fn) as f:
        file_contents = f.read()

    # Find the current plan
    pattern = re.compile(r'Current Plan=.*')
    matches = pattern.finditer(file_contents)

    # close the HEC-RAS project file
    f.close()

    for match in matches:
        str_current_plan = match.group(0)[-3:]

    str_path_to_current_plan = str_path_hecras_project_fn[:-3] + \
        str_current_plan

    # read the current plan
    with open(str_path_to_current_plan) as f:
        file_contents = f.read()

    # Find the current geometry
    pattern = re.compile(r'Flow File=.*')
    matches = pattern.finditer(file_contents)

    # close the HEC-RAS plan file
    f.close()

    # TODO 2021.03.08 - Error here if no flow file in the active plan
    # setting to a default of .f01 - This might not exist
    str_current_flow = 'f01'

    for match in matches:
        str_current_flow = match.group(0)[-3:]

    str_path_to_current_flow = str_path_hecras_project_fn[:-3] + \
        str_current_flow

    return str_path_to_current_flow
# ^^^^^^^^^^^^^^^^^^^^^^^^


# &&&&&&&&&&&&&&&&&&&&&&&&
def fn_get_flow_dataframe(str_path_hecras_flow_fn):
    # Get pandas dataframe of the flows in the active plan's flow file

    # initalize the dataframe
    df = pd.DataFrame()
    df['river'] = []
    df['reach'] = []
    df['start_xs'] = []
    df['max_flow'] = []

    file1 = open(str_path_hecras_flow_fn, 'r')
    lines = file1.readlines()
    i = 0   # number of the current row

    for line in lines:
        if line[:19] == 'Number of Profiles=':
            # determine the number of profiles
            int_flow_profiles = int(line[19:])

            # determine the number of rows of flow - each row has maximum of 10
            int_flow_rows = int(int_flow_profiles // 10 + 1)

        if line[:15] == 'River Rch & RM=':
            str_river_reach = line[15:]  # remove first 15 characters

            # split the data on the comma
            list_river_reach = str_river_reach.split(",")

            # Get from array - use strip to remove whitespace
            str_river = list_river_reach[0].strip()
            str_reach = list_river_reach[1].strip()
            str_start_xs = list_river_reach[2].strip()
            flt_start_xs = float(str_start_xs)

            # Read the flow values line(s)
            list_flow_values = []

            # for each line with flow data
            for j in range(i+1, i+int_flow_rows+1):
                # get the current line
                line_flows = lines[j]

                # determine the number of values on this
                # line from character count
                int_val_in_row = int((len(lines[j]) - 1) / 8)

                # for each value in the row
                for k in range(0, int_val_in_row):
                    # get the flow value (Max of 8 characters)
                    str_flow = line_flows[k*8:k*8+8].strip()
                    # convert the string to a float
                    flt_flow = float(str_flow)
                    # append data to list of flow values
                    list_flow_values.append(flt_flow)

            # Get the max value in list
            flt_max_flow = max(list_flow_values)

            # write to dataFrame
            df = df.append({'river': str_river,
                            'reach': str_reach,
                            'start_xs': flt_start_xs,
                            'max_flow': flt_max_flow}, ignore_index=True)

        i += 1

    file1.close()

    return(df)
# &&&&&&&&&&&&&&&&&&&&&&&&


# ````````````````````````
def fn_gdf_append_xs_with_max_flow(df_xs_fn, df_flows_fn):
    # Function - for a list of cross sections, determine the maximum flow
    # and return as a pandas dataframe

    list_max_flows_per_xs = []

    # for each row in cross section list
    for index, row in df_xs_fn.iterrows():
        max_flow_value = 0

        # for each row in flow break list
        for index2, row2 in df_flows_fn.iterrows():
            # if this is the same river/reach pair
            if row['river'] == row2['river'] and row['reach'] == row2['reach']:
                # if xs station is less than (or equal to) flow break station
                if row['stream_stn'] <= row2['start_xs']:
                    max_flow_value = row2['max_flow']

        list_max_flows_per_xs.append(max_flow_value)

    df_xs_fn['max_flow'] = list_max_flows_per_xs

    return(df_xs_fn)
# ````````````````````````

# ssssssssssssssssssssssss
def fn_cut_stream_downstream(gdf_return_stream_fn, df_xs_fn):
    # Function - split the stream line on the last cross section
    # This to remove the portion of the stream centerline that is
    # downstream of the last cross section; helps with stream conflation
    
    df_xs_fn['stream_stn'] =  df_xs_fn['stream_stn'].astype(float)

    # Get minimum stationed cross section
    flt_ds_xs = df_xs_fn['stream_stn'].min()
    gdf_ds_xs = df_xs_fn.query("stream_stn==@flt_ds_xs")

    # reset the index of the sampled cross section
    gdf_ds_xs = gdf_ds_xs.reset_index()

    # grab the first lines - assumes that the stream is the first stream
    stream_line = gdf_return_stream_fn['geometry'][0]
    xs_line = gdf_ds_xs['geometry'][0]

    # split and return a GeoCollection
    result = split(stream_line, xs_line)
    
    # the last cross section may be at the last stream point - 2021.10.27
    # get a list of items in the returned GeoCollection
    list_wkt_lines = [item for item in result]
    list_lines = []
    
    if len(list_wkt_lines) > 1:

        # merge all the lines except the last line
        for i in range(len(result) -1):
            list_lines.append(result[i])
        
        # Now merge the line with the first segment of the downstream line

        # get first segment of the downstream (last) line
        new_line = LineString([result[i + 1].coords[0], result[i + 1].coords[1]])    
        list_lines.append(new_line)

        # merge the lines
        shp_merged_lines = linemerge(list_lines)
    
        # Revise the geometry with the first line (assumed upstream)
        gdf_return_stream_fn['geometry'][0] = shp_merged_lines

    return gdf_return_stream_fn
# ssssssssssssssssssssssss


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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_create_shapes_from_hecras(str_ras_path_arg, str_shp_out_arg, str_crs_arg):

    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # INPUT
    flt_start_create_shapes_from_hecras = time.time()
    
    print(" ")
    print("+=================================================================+")
    print("|    STREAM AND CROSS SECTION SHAPEFILES FROM HEC-RAS DIRECTORY   |")
    print("|     Created by Andy Carter, PE of the National Water Center     |")
    print("+-----------------------------------------------------------------+")

    # TODO - not contants - lower case - 2021.09.07
    STR_PATH_RAS_FILES = str_ras_path_arg
    print("  ---(i) INPUT PATH: " + STR_PATH_RAS_FILES)
    
    STR_PATH_TO_OUTPUT = str_shp_out_arg
    print("  ---(o) OUTPUT PATH: " + STR_PATH_TO_OUTPUT)
    STR_PATH_TO_OUTPUT += '\\'
    
    STR_CRS_MODEL = str_crs_arg
    print("  ---(p) MODEL PROJECTION: " + STR_CRS_MODEL)
    
    str_path_to_output_streams = STR_PATH_TO_OUTPUT + 'stream_LN_from_ras.shp'
    
    str_path_to_output_cross_sections = STR_PATH_TO_OUTPUT + \
        'cross_section_LN_from_ras.shp'
    
    print("+-----------------------------------------------------------------+")
    # ~~~~~~~~~~~~~~~~~~~~~~~~
    
    # *****MAIN******
    # get a list of all HEC-RAS prj files in a directory
    
    list_files = []
    
    for root, dirs, files in os.walk(STR_PATH_RAS_FILES):
        for file in files:
            if file.endswith(".prj") or file.endswith(".PRJ"):
                # Note the case sensitive issue
                str_file_path = os.path.join(root, file)

                with open(str_file_path) as f:
                    first_file_line = f.read()

                # skip projection files
                if any(x in first_file_line for x in ['PROJCS','GEOGCS','DATUM','PROJECTION']):
                    continue

                list_files.append(str_file_path)
    
    #-----
    # checking to see if 'prj' files are not binary and 
    # valid HEC-RAS prj files.  This should exclude all other
    # prj files
    # skip projection files
           
    textchars = bytearray({7,8,9,10,12,13,27} | set(range(0x20, 0x100)) - {0x7f})
    is_binary_string = lambda bytes: bool(bytes.translate(None, textchars))
    
    str_check = 'Current Plan'
    list_files_valid_prj = []
    
    for str_file_path in list_files:

        if not is_binary_string(open(str_file_path, 'rb').read(1024)):
            file_prj = open(str_file_path, "r")
            b_found_match = False
    
            for line in file_prj:
                if str_check in line:
                    b_found_match = True
                    break
            if b_found_match:
                list_files_valid_prj.append(str_file_path)
            file_prj.close()
    #-----
    
    # Run all the HEC-RAS models that do not have the geom HDF files
    list_models_to_compute = []
    
    for str_prj in list_files:
        #print("processing:"+str_prj)
        str_path_to_geom_hdf = fn_get_active_geom(str_prj) + '.hdf'
        if  not path.exists(str_path_to_geom_hdf):
            # the hdf file does not exist - add to list of models to compute
            list_models_to_compute.append(str_prj)
    
    if len(list_models_to_compute) > 0:
        print('Compute HEC-RAS Models: ' + str(len(list_models_to_compute)))
        
        # create a pool of processors
        num_processors = (mp.cpu_count() - 1)
        p = Pool(processes = num_processors)
        
        # multi-process the HEC-RAS calculation of these models
        p.map(fn_open_hecras,list_models_to_compute)
        
        p.close()
        p.join()
    #-----    
    
    list_geodataframes_stream = []
    list_geodataframes_cross_sections = []
    l = len(list_files_valid_prj)
    
    fn_print_progress_bar(0, l,
                          prefix = 'Reading HEC-RAS output' ,
                          suffix = 'Complete', length = 24)
    i = 0
    
    for ras_path in list_files_valid_prj:
        #print(ras_path)
        gdf_return_stream = fn_geodataframe_stream_centerline(ras_path, STR_CRS_MODEL)
    
        df_flows = fn_get_flow_dataframe(fn_get_active_flow(ras_path))
        df_xs = fn_geodataframe_cross_sections(ras_path, STR_CRS_MODEL)
        if df_xs.empty:
            print("Empty geometry in "+ras_path)
            continue
    
        # Fix interpolated cross section names (ends with *)
        for index, row in df_xs.iterrows():
            str_check = row['stream_stn']
            if str_check[-1] == "*":
                # Overwrite the value to remove '*'
                df_xs.at[index, 'stream_stn'] = str_check[:-1]
    
        df_xs['stream_stn'] = df_xs['stream_stn'].astype(float)
        gdf_xs_flows = fn_gdf_append_xs_with_max_flow(df_xs, df_flows)
    
        gdf_return_stream = fn_cut_stream_downstream(gdf_return_stream, df_xs)
    
        list_geodataframes_stream.append(gdf_return_stream)
        list_geodataframes_cross_sections.append(gdf_xs_flows)
        
        time.sleep(0.03)
        i += 1
        fn_print_progress_bar(i, l,
                              prefix = 'Reading HEC-RAS output',
                              suffix = 'Complete', length = 24)
    
    # Create GeoDataframe of the streams and cross sections
    gdf_aggregate_streams = gpd.GeoDataFrame(
        pd.concat(list_geodataframes_stream, ignore_index=True))
    
    gdf_aggregate_cross_section = gpd.GeoDataFrame(pd.concat(
        list_geodataframes_cross_sections, ignore_index=True))
    
    # Create shapefiles of the streams and cross sections
    gdf_aggregate_streams.to_file(str_path_to_output_streams)
    gdf_aggregate_cross_section.to_file(str_path_to_output_cross_sections)

    print(" ") 
    print('SHAPEFILES CREATED')
    flt_end_create_shapes_from_hecras = time.time()
    flt_time_pass_create_shapes_from_hecras = (flt_end_create_shapes_from_hecras - flt_start_create_shapes_from_hecras) // 1
    time_pass_create_shapes_from_hecras = datetime.timedelta(seconds=flt_time_pass_create_shapes_from_hecras)
    print('Compute Time: ' + str(time_pass_create_shapes_from_hecras))
    
    print("====================================================================") 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    
    parser = argparse.ArgumentParser(description='============ SHAPEFILES FROM HEC-RAS DIRECTORY ============')
    
    parser.add_argument('-i',
                        dest = "str_ras_path_arg",
                        help=r'REQUIRED: path containing the HEC-RAS files: Example C:\HEC\ras_folder',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_shp_out_arg",
                        help=r'REQUIRED: path to write shapefile: Example C:\HEC\ras2fim_precrocess',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-p',
                    dest = "str_crs_arg",
                    help=r'REQUIRED: projection of HEC-RAS models: Example EPSG:26915',
                    required=True,
                    metavar='STRING',
                    type=str)

    args = vars(parser.parse_args())
    
    str_ras_path_arg = args['str_ras_path_arg']
    str_shp_out_arg = args['str_shp_out_arg']
    str_crs_arg = args['str_crs_arg']
    
    fn_create_shapes_from_hecras(str_ras_path_arg, str_shp_out_arg, str_crs_arg)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
