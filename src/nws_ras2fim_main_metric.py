#!/usr/bin/env python
# coding: utf-8

# This tool create the flood inundation mapping (FIM) products for 1D HEC-RAS
# geospatial models.  There are pre-processing scripts that are required
# to create the needed input.  This is keyed to the National Water Model
# stream segementation.
#
# Created by: Andy Carter, PE
# Last revised - 2021.04.06
#
# ***METRIC*** VERSION of the creation of Flood Inundation Mapping

import pandas as pd
import geopandas as gpd

import os
import re
import shutil
import numpy as np

from datetime import date

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

from scipy.interpolate import interp1d

import win32com.client
# windows component object model for interaction with HEC-RAS API
# This routine uses RAS507.HECRASController (HEC-RAS v5.0.7 must be
# installed on this machine prior to execution

import rasterio
from rasterio.mask import mask
# from rasterio.warp import calculate_default_transform, reproject
from rasterio import Affine
from rasterio.enums import Resampling

from shapely.geometry import LineString

import h5py
# h5py for extracting data from the RAS geometry

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT

# Input - desired HUC 8
STR_HUC8 = "10170204"

list_huc8 = []
list_huc8.append(STR_HUC8)

STR_INPUT_FOLDER = r"E:\X-IowaFloodCenter\10170204_PreProcess_20210404"

str_stream_csv = STR_INPUT_FOLDER + '\\' + str(list_huc8[0])
+ "_stream_qc.csv"

str_stream_nwm_ln_shp = STR_INPUT_FOLDER + '\\'
+ str(list_huc8[0]) + "_nwm_streams_ln.shp"

str_huc12_area_shp = STR_INPUT_FOLDER + '\\'
+ str(list_huc8[0]) + "_huc_12_ar_mod1.shp"

# Constant - Folder to write the HEC-RAS folders and files
STR_ROOT_OUTPUT_DIRECTORY = r'E:\X-IowaFloodCenter\10170204_FIM_Out_20210403'

# Constant - projection of the input HEC-RAS
# TODO - Get this from prj file - 2021.03.21
STR_PROJECT_CRS = "EPSG:26915"

# Constant - HEC-RAS Geom - Number of XS to add US and DS
INT_XS_BUFFER = 2   # Number of XS to add upstream and downstream

# Constant - Toggle the Creation of RAS Map products
IS_CREATE_MAPS = True

# Constant - number of flood depth profiles to run on the first pass
INT_NUMBER_OF_STEPS = 75

# Constant - Starting flow for the first pass of the HEC-RAS simulation
INT_STARTING_FLOW = 1

# Constant - Desired average depth interval
FLT_INTERVAL = 0.2

# Constant - Maximum flow multiplier
# up-scales the maximum flow from input
FLT_MAX_MULTIPLY = 1.2

# Constant - resolution of the output raster
# in raster projection units
# If reprojecting to meters this is 3 : In feet this is 9
INT_DESIRED_RESOLUTION = 3

# Constant - buffer of dem around floodplain envelope
FLT_BUFFER = 15

# Constant - For RASMapper - GIS Projection File
STR_PATH_TO_PROJECTION = r'E:\X-IowaFloodCenter\10170204_Project_Inputs\UTM-Zone-15-m.prj'

# Constant - For RASMapper - Path containing the HEC-RAS compliant terrain data
STR_PATH_TO_TERRAIN = r'E:\X-IowaFloodCenter\Terrain_Output\Terrain_Processed\HEC-RAS-HDF'

# Path to the standard plan file text
STR_PLAN_MIDDLE_PATH = r"E:\X-NWS\XX-Standard_Inputs\PlanStandardText01.txt"
STR_PLAN_FOOTER_PATH = r"E:\X-NWS\XX-Standard_Inputs\PlanStandardText02.txt"

STR_PROJECT_FOOTER_PATH = r"E:\X-NWS\XX-Standard_Inputs\ProjectStandardText01.txt"
# ~~~~~~~~~~~~~~~~~~~~~~~~

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


# ^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_create_firstpass_flowlist(int_fn_starting_flow,
                                 int_fn_max_flow,
                                 int_fn_number_of_steps):

    # create a list of flows for the first pass HEC-RAS
    list_first_pass_flows = []

    int_fn_deltaflowstep = int(int_fn_max_flow // (int_fn_number_of_steps-2))

    for i in range(int_fn_number_of_steps):
        list_first_pass_flows.append(int_fn_starting_flow
                                     + (i*int_fn_deltaflowstep))

    return(list_first_pass_flows)
# ^^^^^^^^^^^^^^^^^^^^^^^^^


# --------------------------------
def fn_create_profile_names(list_profiles, str_suffix):

    str_profile_names = 'Profile Names='
    for i in range(len(list_profiles)):
        str_profile_names += str(list_profiles[i]) + str_suffix
        if i < (len(list_profiles)-1):
            str_profile_names = str_profile_names + ','

    return (str_profile_names)
# --------------------------------


# ********************************
def fn_format_flow_values(list_flow):

    int_number_of_profiles = len(list_flow)
    str_all_flows = ''

    int_number_of_new_rows = int((int_number_of_profiles // 10) + 1)
    int_items_in_new_last_row = int_number_of_profiles % 10

    # write out new rows of 10 grouped flows
    if int_number_of_new_rows > 1:
        # write out the complete row of 10
        for j in range(int_number_of_new_rows - 1):
            str_flow = ''
            for k in range(10):
                int_Indexvalue = j*10 + k
                # format to 8 characters that are right alligned
                str_current_flow = '{:>8}'.format(
                    str(list_flow[int_Indexvalue]))
                str_flow = str_flow + str_current_flow
            str_all_flows += str_flow + '\n'

    # write out the last row
    str_flow_last_row = ''
    for j in range(int_items_in_new_last_row):
        int_indexvalue = (int_number_of_new_rows-1) * 10 + j
        str_current_flow = '{:>8}'.format(str(list_flow[int_indexvalue]))
        str_flow_last_row += str_current_flow
    str_all_flows += str_flow_last_row

    return (str_all_flows)
# ********************************


# ................................
def fn_create_flow_file_second_pass(str_ras_project_fn,
                                    list_int_stepflows_fn,
                                    list_step_profiles_fn):

    # Function to create a flow file for the 'second pass'
    str_ras_flow_path = str_ras_project_fn[:-3]+'f01'

    with open(str_ras_flow_path) as f1:
        file_contents = f1.read()

    # Get the parameters
    pattern = re.compile(r'River Rch & RM=.*')
    matches = pattern.finditer(file_contents)

    for match in matches:
        str_riverreach_fn = file_contents[match.start():match.end()]
        str_riverreach_fn = str_riverreach_fn[15:]  # remove first 15 char
        # split the data on the comma
        arr_riverreach = str_riverreach_fn.split(",")

        # Get from array - use strip to remove whitespace
        str_fn_river = arr_riverreach[0].strip()
        str_fn_reach = arr_riverreach[1].strip()
        str_fn_start_xs = arr_riverreach[2].strip()

    pattern = re.compile(r'Flow Title=.*')
    matches = pattern.finditer(file_contents)
    for match in matches:
        str_flow_title = file_contents[match.start():match.end()]
        str_flow_title = str_flow_title[11:]  # remove first 11 characters

    int_fn_num_profiles = len(list_step_profiles_fn)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    f1.close()

    # -------------------------------------
    str_flow_file = ''
    # Write the flow file

    str_flow_file = "Flow Title=" + str_flow_title + '\n'
    str_flow_file += "Program Version=5.07" + '\n'
    str_flow_file += "BEGIN FILE DESCRIPTION:" + '\n'

    str_flow_file += "Flow File - Created from provided HEC-RAS"
    str_flow_file += " models for Flood Inundation Library "
    str_flow_file += "- Andy Carter,PE" + '\n'

    str_flow_file += "END FILE DESCRIPTION:" + '\n'
    str_flow_file += "Number of Profiles= " + str(int_fn_num_profiles) + '\n'

    # write a list of the step depth profiles

    str_flow_file += fn_create_profile_names(list_step_profiles_fn, 'm') + '\n'

    str_flow_file += "River Rch & RM="

    str_flow_file += str_fn_river + "," + str_fn_reach
    + "," + str_fn_start_xs + '\n'

    str_flow_file += fn_format_flow_values(list_int_stepflows_fn) + '\n'

    for i in range(int_fn_num_profiles):
        str_flow_file += "Boundary for River Rch & Prof#="

        str_flow_file += str_fn_river + "," + str_fn_reach
        + ", " + str(i+1) + '\n'

        str_flow_file += "Up Type= 0 " + '\n'
        str_flow_file += "Dn Type= 3 " + '\n'
        str_flow_file += "Dn Slope=0.005" + '\n'

    str_flow_file += "DSS Import StartDate=" + '\n'
    str_flow_file += "DSS Import StartTime=" + '\n'
    str_flow_file += "DSS Import EndDate=" + '\n'
    str_flow_file += "DSS Import GetInterval= 0 " + '\n'
    str_flow_file += "DSS Import Interval=" + '\n'
    str_flow_file += "DSS Import GetPeak= 0 " + '\n'
    str_flow_file += "DSS Import FillOption= 0 " + '\n'

    file = open(str_ras_flow_path, "w")
    file.write(str_flow_file)
    file.close()
# ................................


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def fn_create_rating_curve(list_int_step_flows_fn,
                           list_step_profiles_fn,
                           str_feature_id_fn,
                           str_path_to_create_fn):

    str_file_name = str_feature_id_fn + '_indirect_rating_curve.png'

    # Create a Rating Curve folder
    str_rating_path_to_create = str_path_to_create_fn + '\\Rating_Curve'
    os.makedirs(str_rating_path_to_create, exist_ok=True)

    fig = plt.figure()
    fig.patch.set_facecolor('gainsboro')
    fig.suptitle('FEATURE ID: '
                 + str_feature_id_fn, fontsize=18, fontweight='bold')

    ax = plt.gca()
    today = date.today()

    ax.text(0.98, 0.04, 'Created: ' + str(today),
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes,
            fontsize=6,
            style='italic')

    ax.text(0.98, 0.09, 'Computed from HEC-RAS models',
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=6, style='italic')

    ax.text(0.98, 0.14, 'NOAA - Office of Water Prediction',
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=6, style='italic')

    plt.plot(list_int_step_flows_fn, list_step_profiles_fn)  # creates the line
    plt.plot(list_int_step_flows_fn, list_step_profiles_fn, 'bd')
    # adding blue diamond points on line

    ax.get_xaxis().set_major_formatter(
        tick.FuncFormatter(lambda x, p: format(int(x), ',')))

    plt.xticks(rotation=90)

    plt.ylabel('Average Depth (m)')
    plt.xlabel('Discharge (m^3/s)')

    plt.grid(True)

    str_rating_image_path = str_rating_path_to_create + '\\' + str_file_name
    plt.savefig(str_rating_image_path,
                dpi=300,
                bbox_inches="tight")
    plt.close

    # Create CSV for the rating curve

    d = {'Flow(cms)': list_int_step_flows_fn,
         'AvgDepth(m)': list_step_profiles_fn}

    df_rating_curve = pd.DataFrame(d)

    str_csv_path = str_rating_path_to_create + '\\'
    + str_feature_id_fn + '-rating_curve.csv'

    df_rating_curve.to_csv(str_csv_path, index=False)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


# ||||||||||||||||||||||||
def fn_create_hecras_files(str_feature_id,
                           str_read_geom_file_path,
                           flt_min_range,
                           flt_max_range,
                           int_max_flow,
                           str_output_filepath):

    # read the HEC-RAS geometry file
    with open(str_read_geom_file_path) as f:
        file_contents = f.read()

    # -------------------------------------
    # Find all the cross sections
    pattern = re.compile(r'Type RM Length L Ch R = 1.*')
    # note that Type = '1' is a Cross section

    matches = pattern.finditer(file_contents)

    # create a list of the start location of all the cross sections
    list_start_xs = []

    i = 0
    for match in matches:
        i = i + 1
        list_start_xs.append(match.start())
    # -------------------------------------

    # .....................................
    # Create a list of all the cross section names

    matched = re.findall(r'Type RM Length L Ch R = 1.*', file_contents)

    list_xs_name = []

    k = 0
    for j in matched:
        j_split = (j.split(','))
        j_current_xs = j_split[1].strip()

        list_xs_name.append(j_current_xs)
        k += 1
    # .....................................

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # All text up to the first cross section - Header of the Geom File
    str_header = file_contents[0:list_start_xs[0]]

    # Rename the geometry data - edit the first line
    pattern = re.compile(r'Geom Title=.*')
    geom_matches = pattern.finditer(str_header)

    for match in geom_matches:
        str_header = str_header[match.end()+1:(len(str_header))]
        str_header = "Geom Title=BLE_" + str_feature_id + '\n' + str_header
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # *************************************
    # Read file and determine postions of blank lines
    pattern = re.compile('\n\s*\n')
    matches = pattern.finditer(file_contents)

    list_start_blankline = []

    i = 0
    for match in matches:
        i = i + 1
        list_start_blankline.append(match.start())
    # *************************************

    # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    # Determine the next blank line pointer for the subject cross section
    list_end_xs = []

    for a in list_start_xs:
        b_found_flag = False
        for b in list_start_blankline:
            if b > a and not b_found_flag:
                list_end_xs.append(b)
                b_found_flag = True
    # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    # Using pandas - combine the Cross Section Name,
    # Start pointer and end pointer
    df0 = pd.concat(
        [pd.DataFrame([i], columns=['XS_Name']) for i in list_xs_name],
        ignore_index=True)

    df1 = pd.concat(
        [pd.DataFrame([i], columns=['start_XS']) for i in list_start_xs],
        ignore_index=True)

    df2 = pd.concat(
        [pd.DataFrame([i], columns=['end_XS']) for i in list_end_xs],
        ignore_index=True)

    df3 = pd.concat([df0, df1, df2], axis=1, sort=False)
    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    # -------------------------------------
    # Create file footer
    # From end marker of last cross section to end of file

    str_footer = file_contents[(df3.end_XS.iat[-1]):(len(file_contents))]
    # -------------------------------------

    # .....................................
    # Create the HEC-RAS Geomerty file
    str_geom = str_header

    # Determine the XS index within the valid range
    # TODO - Logic error where int_first_index is not assigned - 2020.08.13
    int_first_index = -1
    int_last_index = -1

    b_found_first_index = False
    for index, row in df3.iterrows():
        if float(row['XS_Name']) >= flt_min_range:
            if float(row['XS_Name']) <= flt_max_range:
                if not b_found_first_index:
                    int_first_index = index
                    b_found_first_index = True
                int_last_index = index

    if int_first_index > -1 and int_last_index > -1:
        # Get the upstream Cross section plus a index buffer
        if (int_first_index - INT_XS_BUFFER) >= 0:
            int_first_index -= INT_XS_BUFFER
        else:
            int_first_index = 0

        # Get the downstream cross section plus a index buffer
        if (int_last_index + INT_XS_BUFFER) <= len(df3):
            int_last_index += INT_XS_BUFFER
        else:
            int_last_index = len(df3)

        # get the name of the upstream cross section
        str_xs_upstream = df3.iloc[int_first_index]['XS_Name']

        for index, row in df3.iterrows():
            if (index >= int_first_index) and (index <= int_last_index):
                str_geom += file_contents[(row['start_XS']):(row['end_XS'])]
                str_geom += "\n\n"

        str_geom += str_footer

        # Write the requested file
        file = open(str_output_filepath + "\\" + str_feature_id + '.g01', "w")
        file.write(str_geom)
        file.close()
        # .....................................

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Get River, reach and Upstream XS for flow file

        pattern = re.compile(r'River Reach=.*')
        matches = pattern.finditer(file_contents)

        for match in matches:
            str_river_reach = file_contents[match.start():match.end()]
            # remove first 12 characters
            str_river_reach = str_river_reach[12:]
            # split the data on the comma
            list_river_reach = str_river_reach.split(",")

            # Get from array - use strip to remove whitespace
            str_river = list_river_reach[0].strip()
            str_reach = list_river_reach[1].strip()
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        # -------------------------------------
        # Write the flow file

        str_flowfile = "Flow Title=BLE_"
        str_flowfile += str_feature_id + '\n'
        str_flowfile += "Program Version=5.07" + '\n'
        str_flowfile += "BEGIN FILE DESCRIPTION:" + '\n'

        str_flowfile += "Flow File - Created from Base Level Engineering"
        str_flowfile += " data for Flood Inundation Library - "
        str_flowfile += "Andy Carter,PE" + '\n'

        str_flowfile += "END FILE DESCRIPTION:" + '\n'

        str_flowfile += "Number of Profiles= "
        + str(INT_NUMBER_OF_STEPS) + '\n'

        # get a list of the first pass flows
        list_firstflows = fn_create_firstpass_flowlist(INT_STARTING_FLOW,
                                                       int_max_flow,
                                                       INT_NUMBER_OF_STEPS)

        str_flowfile += fn_create_profile_names(list_firstflows, 'cms') + '\n'
        # Note - 2021.03.20 - cms is hard coded in above line

        str_flowfile += "River Rch & RM="

        str_flowfile += str_river + "," + str_reach + ","
        + str_xs_upstream + '\n'

        str_flowfile += fn_format_flow_values(list_firstflows) + '\n'

        for i in range(INT_NUMBER_OF_STEPS):
            str_flowfile += "Boundary for River Rch & Prof#="

            str_flowfile += str_river + "," + str_reach + ", "
            + str(i+1) + '\n'

            str_flowfile += "Up Type= 0 " + '\n'
            str_flowfile += "Dn Type= 3 " + '\n'
            str_flowfile += "Dn Slope=0.005" + '\n'

        str_flowfile += "DSS Import StartDate=" + '\n'
        str_flowfile += "DSS Import StartTime=" + '\n'
        str_flowfile += "DSS Import EndDate=" + '\n'
        str_flowfile += "DSS Import GetInterval= 0 " + '\n'
        str_flowfile += "DSS Import Interval=" + '\n'
        str_flowfile += "DSS Import GetPeak= 0 " + '\n'
        str_flowfile += "DSS Import FillOption= 0 " + '\n'

        file = open(str_output_filepath + "\\" + str_feature_id + '.f01', "w")
        file.write(str_flowfile)
        file.close()
        # -------------------------------------

        # **************************************
        # Write the plan file
        str_planfile = "Plan Title=BLE_"
        str_planfile += str_feature_id + '\n'
        str_planfile += "Program Version=5.07" + '\n'
        str_planfile += "Short Identifier=" + 'BLE_' + str_feature_id + '\n'

        # read a file and append to the str_planfile string
        # str_planFooterPath
        # To map the requested Depth Grids

        # read the plan middle input file
        with open(STR_PLAN_MIDDLE_PATH) as f:
            file_contents = f.read()
        str_planfile += file_contents

        # To map the requested Depth Grids
        # Set to 'Run RASMapper=0 ' to not create requested DEMs
        # Set to 'Run RASMapper=-1 ' to create requested DEMs
        if IS_CREATE_MAPS:
            str_planfile += '\n' + r'Run RASMapper=-1 ' + '\n'
        else:
            str_planfile += '\n' + r'Run RASMapper=0 ' + '\n'

        # read the plan footer input file
        with open(STR_PLAN_FOOTER_PATH) as f:
            file_contents = f.read()
        str_planfile += file_contents

        file = open(str_output_filepath + "\\" + str_feature_id + '.p01', "w")
        file.write(str_planfile)
        file.close()
        # **************************************

        # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        # Write the HEC-RAS project file
        str_projectfile = "Proj Title=BLE_"
        str_projectfile += str_feature_id + '\n'

        # read the project footer input file
        with open(STR_PROJECT_FOOTER_PATH) as f:
            file_contents = f.read()
        str_projectfile += file_contents

        file = open(str_output_filepath + "\\" + str_feature_id + '.prj', "w")
        file.write(str_projectfile)
        file.close()
        # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

        str_ras_projectpath = (str_output_filepath + "\\"
                               + str_feature_id + '.prj')

        #######################
        fn_run_hecras(str_ras_projectpath, int_max_flow)
        #######################

        return (str_river)
# ||||||||||||||||||||||||


# +++++++++++++++++++++++++
def fn_create_ras_mapper_xml(str_feature_id_fn,
                             str_ras_projectpath_fn,
                             list_step_profiles_xml_fn):

    # Function to create the RASMapper XML and add the requested
    # DEM's to be created
    # NOTE - str_huc12 from outside of this fuction- need to fix - 2021.03.21

    # Create .rasmap XML file
    str_ras_mapper_file = ''
    # Write the flow file

    str_ras_mapper_file = r'<RASMapper>' + '\n'
    str_ras_mapper_file += r'  <Version>2.0.0</Version>' + '\n'

    str_ras_mapper_file += r'  <RASProjectionFilename Filename="'
    + STR_PATH_TO_PROJECTION + r'" />' + '\n'

    str_ras_mapper_file += r'  <Geometries Checked="True" Expanded="True">'
    + '\n'

    str_ras_mapper_file += r'    <Layer Name="BLE_' + str_feature_id_fn + '"'
    str_ras_mapper_file += r' Type="RASGeometry" Checked="True" '
    str_ras_mapper_file += r'Expanded="True" Filename="'

    str_ras_mapper_file += r'.' + '\\' + str_feature_id_fn
    + '.g01.hdf">' + '\n'

    str_ras_mapper_file += r'      <Layer Type="RAS'
    str_ras_mapper_file += r'River" Checked="True" />' + '\n'
    str_ras_mapper_file += r'      <Layer Type="RASXS" Checked'
    str_ras_mapper_file += r'="True" />' + '\n'
    str_ras_mapper_file += r'    </Layer>' + '\n'
    str_ras_mapper_file += r'  </Geometries>' + '\n'

    str_ras_mapper_file += r'  <Results Expanded="True">' + '\n'
    str_ras_mapper_file += r'    <Layer Name="BLE_'
    str_ras_mapper_file += str_feature_id_fn + '" Type="RAS'
    + 'Results" Expanded="True" Filename=".'
    str_ras_mapper_file += '\\' + str_feature_id_fn + r'.p01.hdf">' + '\n'
    str_ras_mapper_file += '      <Layer Type="RASGeometry" Filename=".'
    str_ras_mapper_file += '\\' + str_feature_id_fn + r'.p01.hdf" />' + '\n'

    int_index = 0

    # Loop through all profiles and create an XML request to map each depth
    # grid in the list_step_profiles_xml_fn
    for i in list_step_profiles_xml_fn:
        str_ras_mapper_file += '      <Layer Name="depth" Type="RAS'
        str_ras_mapper_file += 'ResultsMap" Checked="True" Filename=".'
        str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn
        + '\\' + 'Depth (' + str(i) + 'm).vrt">' + '\n'
        str_ras_mapper_file += '        <LabelFeatures '
        str_ras_mapper_file += 'Checked="True" Center="False" '
        str_ras_mapper_file += 'rows="1" cols="1" r0c0="FID" '
        str_ras_mapper_file += 'Position="5" Color="-16777216" />' + '\n'
        str_ras_mapper_file += '        <MapParameters MapType="depth" Layer'
        str_ras_mapper_file += 'Name="Depth" OutputMode="Stored Current '
        str_ras_mapper_file += 'Terrain" StoredFilename=".' + '\\BLE_'
        + str_feature_id_fn + '\\Depth (' + str(i) + 'm).vrt" '
        str_ras_mapper_file += 'Terrain="' + str_feature_id_fn
        + '" ProfileIndex="' + str(int_index) + '" '
        str_ras_mapper_file += ' ProfileName="'
        + str(i) + 'm" ArrivalDepth="0" />' + '\n'
        str_ras_mapper_file += '      </Layer>' + '\n'

        int_index += 1

    # Get the highest (last profile) flow innundation polygon
    # --------------------
    str_ras_mapper_file += '      <Layer Name="depth" Type="RAS'
    str_ras_mapper_file += 'ResultsMap" Checked="True" Filename=".'

    str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn + '\\'
    + 'Inundation Boundary (' + str(list_step_profiles_xml_fn[-1])

    str_ras_mapper_file += 'm Value_0).shp">' + '\n'
    str_ras_mapper_file += '        <MapParameters MapType="depth" '
    str_ras_mapper_file += 'LayerName="Inundation Boundary"'
    str_ras_mapper_file += ' OutputMode="Stored Polygon'
    str_ras_mapper_file += ' Specified Depth"  StoredFilename=".'
    str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn + '\\'
    + 'Inundation Boundary (' + str(list_step_profiles_xml_fn[-1])
    str_ras_mapper_file += 'm Value_0).shp"  Terrain="' + str_huc12
    + '" ProfileIndex="' + str(len(list_step_profiles_xml_fn)-1)
    str_ras_mapper_file += '"  ProfileName="'
    + str(list_step_profiles_xml_fn[-1]) + 'm"  ArrivalDepth="0" />' + '\n'
    str_ras_mapper_file += '      </Layer>' + '\n'
    # --------------------

    str_ras_mapper_file += r'    </Layer>' + '\n'
    str_ras_mapper_file += r'  </Results>' + '\n'

    str_ras_mapper_file += r'  <Terrains Checked="True" Expanded="True">'
    + '\n'

    str_ras_mapper_file += r'    <Layer Name="' + str_huc12
    + r'" Type="TerrainLayer" Checked="True" Filename="'

    str_ras_mapper_file += STR_PATH_TO_TERRAIN + '\\'
    + str_huc12 + r'.hdf">' + '\n'

    str_ras_mapper_file += r'    </Layer>' + '\n'
    str_ras_mapper_file += r'  </Terrains>' + '\n'

    str_ras_mapper_file += r'</RASMapper>'

    file = open(str_ras_projectpath_fn[:-4] + '.rasmap', "w")
    file.write(str_ras_mapper_file)
    file.close()

# +++++++++++++++++++++++++


# ~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_get_features(gdf, int_poly_index):
    """Function to parse features from GeoDataFrame
    in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][int_poly_index]['geometry']]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~


# """"""""""""""""""""""""""
def fn_create_study_area(str_polygon_path_fn, str_feature_id_poly_fn):

    # Function to create the study limits shapefile (polyline)

    # Folder to store the study area
    # NOTE - str_path_to_create from outside function
    str_studylimits_pathtocreate = str_path_to_create + '\\Study_Area'
    os.makedirs(str_studylimits_pathtocreate, exist_ok=True)

    dst_crs_1 = 'EPSG:4326'  # Projection  for decimal degree limit coordinates
    dst_crs_2 = 'EPSG:3857'  # Output Projection (WGS 84) to write file

    str_output_path = str_studylimits_pathtocreate
    + '\\' + str_feature_id_poly_fn + '_study_area.shp'

    gdf_inputshape = gpd.read_file(str_polygon_path_fn)

    # Buffer the shapefile
    gdf_flood_buffer = gdf_inputshape.buffer(FLT_BUFFER, 8)
    gdf_flood_depth_envelope = gdf_flood_buffer.envelope

    gdf_flood_depth_envelope = gdf_flood_depth_envelope.to_crs(dst_crs_1)
    list_extents = gdf_flood_depth_envelope[0].bounds

    gdf_flood_depth_envelope = gdf_flood_depth_envelope.to_crs(dst_crs_2)
    listPoints = list(gdf_flood_depth_envelope[0].exterior.coords)

    gdf_flood_depth_envelope.to_file(str_output_path)
    gdf_flood_depth_envelope = gpd.read_file(str_output_path)

    gdf_flood_depth_envelope.loc[0, 'geometry'] = LineString(listPoints)
    # Add columns to geopandas

    gdf_flood_depth_envelope['SiteNumber'] = str_feature_id_poly_fn
    gdf_flood_depth_envelope['EXT_MIN_X'] = list_extents[0]
    gdf_flood_depth_envelope['EXT_MIN_Y'] = list_extents[1]
    gdf_flood_depth_envelope['EXT_MAX_X'] = list_extents[2]
    gdf_flood_depth_envelope['EXT_MAX_Y'] = list_extents[3]

    gdf_flood_depth_envelope.to_file(str_output_path)
# """"""""""""""""""""""""""


# ``````````````````````````
def fn_clip_convert_dems(list_step_profiles_dem_fn,
                         str_fn_path_to_create):

    # Function convert RAS Mapper DEMs to InFRM compliant rasters

    # Folder to store the depth grids
    str_depthgrid_path_to_create = str_fn_path_to_create + '\\Depth_Grids'
    os.makedirs(str_depthgrid_path_to_create, exist_ok=True)

    str_output_raster_path = str_depthgrid_path_to_create

    # note that the _clip.tif suffix is from the DEM processing routine
    # and may not be valid for all runs

    # NOTE - str_hecras_path_to_create from outside function - 2021.03.21
    # NOTE - str_feature_id from outside of fuction - 2021.03.21

    # TODO - Error caused by rounding of profile list - 2021.04.05

    str_polygon_path = str_hecras_path_to_create + '\\'
    + "BLE_" + str_feature_id + '\\'

    str_polygon_path += 'Inundation Boundary ('
    + str(list_step_profiles_dem_fn[-1]) + 'm Value_0).shp'

    # Create the study area polyline shapefile
    fn_create_study_area(str_polygon_path,
                         str_feature_id)

    for i in list_step_profiles_dem_fn:

        # ...3585434\HEC-RAS\BLE_3585434\Depth (32.0ft).121002010109_clip.tif
        # NOTE - str_huc12 from outside of this fuction - 2021.03.21

        str_raster_path = str_hecras_path_to_create + '\\'
        + "BLE_" + str_feature_id + '\\'

        # str_raster_path += 'Depth (' + str(i) + 'm).'
        # + str_huc12 + '_clip.tif'

        str_raster_path += 'Depth (' + str(i) + 'm).' + str_huc12 + '.tif'

        # HEC-RAS sometimes does not create a raster

        if os.path.isfile(str_raster_path):
            # Read the overall Depth raster
            src = rasterio.open(str_raster_path)
            # Copy the metadata of the src terrain data
            out_meta = src.meta.copy()

            # Read the shapefils using geopandas
            gdf_flood_depth = gpd.read_file(str_polygon_path)

            # Buffer the shapefile
            gdf_flood_buffer = gdf_flood_depth.buffer(FLT_BUFFER, 8)
            gdf_flood_depth_envelope = gdf_flood_buffer.envelope

            # TODO - mask the raster to remove islands and noise - 2020.07.27

            # currently requests the first polygon in the geometry
            coords = fn_get_features(gdf_flood_depth_envelope, 0)

            # Clip the raster with Polygon
            out_img, out_transform = mask(dataset=src,
                                          shapes=coords,
                                          crop=True)

            # Metadata for the clipped image
            # This uses the out_image height and width
            out_meta.update({"driver": "GTiff",
                             "height": out_img.shape[1],
                             "width": out_img.shape[2],
                             "transform": out_transform, })

            str_current_avg_depth = str(int(i * 10 // 1))

            str_output_raster_name = str_feature_id + '-'
            + str_current_avg_depth + '.tif'

            str_output_raster_path = str_depthgrid_path_to_create
            str_output_raster_path += '\\' + str_output_raster_name

            with rasterio.open(str_output_raster_path,
                               "w",
                               **out_meta) as dest:
                dest.write(out_img)

            src2 = rasterio.open(str_output_raster_path)
            with rasterio.open(str_output_raster_path) as src2:
                depth = src2.read()

            # create numpy array 'a'
            a = depth

            # content deposition to reset the invalid cells to null of -9999
            a[depth <= 0.1] = -9999

            with rasterio.open(str_output_raster_path,
                               'w',
                               **out_meta) as dest:
                dest.write(a)

            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            # reprojecting raster

            # input_raster = gdal.Open(str_output_raster_path)
            # warp = gdal.Warp(str_output_raster_path,
            # input_raster,dstSRS='EPSG:3857')
            # warp = None # Closes the files

            # TODO -removed the rasterio re-projection - 2021.02.27 - MAC

            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            # downsampling the raster
            with rasterio.open(str_output_raster_path) as src4:
                t = src4.transform

                # TODO - 2021.02.27 - If reprojecting to meters this is 3
                # Without reprojection - setting this to 9
                scale = (INT_DESIRED_RESOLUTION / t.a)

                # rescale the metadata
                transform = Affine(t.a * scale,
                                   t.b,
                                   t.c,
                                   t.d,
                                   t.e * scale,
                                   t.f)

                height = int(src4.height // scale)
                width = int(src4.width // scale)

                profile = src4.profile
                profile.update(transform=transform,
                               driver='GTiff',
                               height=height,
                               width=width)
                # Note changed order of indexes, arrays are band,
                # row, col order not row, col, band
                data = src4.read(
                        out_shape=(src4.count, height, width),
                        resampling=Resampling.nearest,
                    )

                with rasterio.open(str_output_raster_path,
                                   'w',
                                   **profile) as dataset:
                    dataset.write(data)
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            # Create a numpy array of the rounded up values
            with rasterio.open(str_output_raster_path) as src5:
                values = src5.read()

            values = ((values * 10) + 0.5)

            # create numpy array 'a'
            a = values

            # content deposition to reset the null cells to 65535
            a[values < 1] = 65535

            # convert numpy datatype to unsigned 16 bit integer
            b = a.astype('uint16')

            # change the null value and the datatype
            with rasterio.open(str_output_raster_path) as src6:
                t = src6.transform

                # rescale the metadata
                transform = Affine(t.a,
                                   t.b,
                                   t.c,
                                   t.d,
                                   t.e,
                                   t.f)
                height = src6.height
                width = src6.width

                profile = src6.profile
                profile.update(dtype='uint16',
                               nodata=65535,
                               compress='lzw')

                # Note changed order of indexes, arrays are band,
                # row, col order not row, col, band
                data = src6.read(
                        out_shape=(src6.count, height, width)
                    )

                with rasterio.open(str_output_raster_path,
                                   'w',
                                   **profile) as dataset:
                    dataset.write(b)
# ``````````````````````````


# ...........................
def fn_run_hecras(str_ras_projectpath, int_peak_flow):

    hec = win32com.client.Dispatch("RAS507.HECRASController")
    hec.ShowRas()

    hec.Project_Open(str_ras_projectpath)   # opening HEC-RAS

    # to be populated: number and list of messages, blocking mode
    NMsg, TabMsg, block = None, None, True

    # computations of the current plan
    v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

    # ID numbers of the river and the reach
    RivID, RchID = 1, 1

    # to be populated: number of nodes, list of RS and node types
    NNod, TabRS, TabNTyp = None, None, None

    # reading project nodes: cross-sections, bridges, culverts, etc.
    v1, v2, NNod, TabRS, TabNTyp = hec.Geometry_GetNodes(RivID,
                                                         RchID,
                                                         NNod,
                                                         TabRS,
                                                         TabNTyp)

    # HEC-RAS ID of output variables: Max channel depth, channel reach length
    int_max_depth_id, int_node_chan_length = 4, 42

    # ----------------------------------
    # Create a list of the simulated flows
    list_flow_steps = []

    int_delta_flow_step = int(int_peak_flow // (INT_NUMBER_OF_STEPS - 2))

    for i in range(INT_NUMBER_OF_STEPS):
        list_flow_steps.append((i*int_delta_flow_step) + INT_STARTING_FLOW)
    # ----------------------------------

    # **********************************
    # Intialize list of the computed average depths
    list_avg_depth = []
    # **********************************

    for intProf in range(INT_NUMBER_OF_STEPS):

        # NumPy array for max depth
        tab_max_depth = np.empty([NNod], dtype=float)

        # NumPy array for average depth
        tab_max_avg_depth = np.empty([NNod], dtype=float)

        # NumPy array for reach length
        tab_channel_length = np.empty([NNod], dtype=float)
        tab_multiply = np.empty([NNod], dtype=float)

        for i in range(0, NNod):        # reading over nodes
            if TabNTyp[i] == "":        # simple cross-section

                # reading single maxdepth
                tab_max_depth[i],
                v1, v2, v3, v4, v5, v6 = hec.Output_NodeOutput(
                    RivID,
                    RchID,
                    i+1,
                    0,
                    intProf+1,
                    int_max_depth_id)

                # reading single channel reach length
                tab_channel_length[i],
                v1, v2, v3, v4, v5, v6 = hec.Output_NodeOutput(
                    RivID,
                    RchID,
                    i+1,
                    0,
                    intProf+1,
                    int_node_chan_length)

        # Revise the last channel length to zero
        tab_channel_length[len(tab_channel_length) - 1] = 0

        k = 0
        for x in tab_max_depth:
            if k != (len(tab_max_depth))-1:
                # get the average depth between two sections
                tab_max_avg_depth[k] = (tab_max_depth[k]
                                        + tab_max_depth[k+1])/2
            k += 1

        # multiply the average Depth with the channel length array
        tab_multiply = tab_max_avg_depth * tab_channel_length

        # average depth on the reach for given profile
        flt_avg_depth = (np.sum(tab_multiply)) / (np.sum(tab_channel_length))
        list_avg_depth.append(flt_avg_depth)

    # ------------------------------------------
    # create two numpy arrays for the linear interpolator
    # arr_avg_depth = np.array(list_avg_depth)

    # f is the linear interpolator
    f = interp1d(list_avg_depth, list_flow_steps)

    # Get the max value of the Averge Depth List
    int_max_depth = int(max(list_avg_depth) // FLT_INTERVAL)

    # Get the min value of Average Depth List
    int_min_depth = int((min(list_avg_depth) // FLT_INTERVAL) + 1)

    list_step_profiles = []

    # Create a list of the profiles at desired increments
    for i in range(int_max_depth - int_min_depth + 1):
        int_depth_interval = (i + int_min_depth) * FLT_INTERVAL

        # round this to nearest 1/10th
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        int_depth_interval = round(int_depth_interval, 1)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        list_step_profiles.append(int_depth_interval)

    # get interpolated flow values of interval depths
    arr_step_flows = f(list_step_profiles)

    # convert the linear interpolation array to a list
    list_step_flows = arr_step_flows.tolist()

    # convert list of interpolated float values to integer list
    list_int_step_flows = [int(i) for i in list_step_flows]

    # ------------------------------------------
    # generate the rating curve data
    fn_create_rating_curve(list_int_step_flows,
                           list_step_profiles,
                           str_feature_id,
                           str_path_to_create)
    # ------------------------------------------

    hec.QuitRas()  # close HEC-RAS

    # append the flow file with one for the second pass at even depth intervals
    fn_create_flow_file_second_pass(str_ras_projectpath,
                                    list_int_step_flows,
                                    list_step_profiles)

    if len(list_int_step_flows) > 0:
        # *************************************************
        fn_create_ras_mapper_xml(str_feature_id,
                                 str_ras_projectpath,
                                 list_step_profiles)
        # *************************************************

        # Run HEC-RAS with the new flow data
        hec.ShowRas()
        hec.Project_Open(str_ras_projectpath)   # opening HEC-RAS

        # to be populated: number and list of messages, blocking mode
        NMsg, TabMsg, block = None, None, True

        # computations of the current plan
        v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

        hec.QuitRas()  # close HEC-RAS

        # *************************************************
        # clip and convert the DEM files
        if IS_CREATE_MAPS:
            fn_clip_convert_dems(list_step_profiles,
                                 str_path_to_create)
        # *************************************************

        # *************************************************
        # creates the model limits boundary polylines
        fn_create_inundation_limits(str_feature_id,
                                    str_ras_projectpath)
        # *************************************************

        '''
        #*************************************************
        #create the FDST metadata (json) file
        fn_createFDST_metadata(strCOMID,list_StepProfiles)
        #*************************************************
        '''
    del hec             # delete HEC-RAS controller
# ...........................


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_create_inundation_limits(str_feature_id_limits_fn,
                                str_ras_projectpath_fn):

    # read the Geometry HDF and gets the boundary lines of the model
    # converts to lines in EPSG 3857

    str_path_to_geom_hdf = str_ras_projectpath_fn[:-4] + '.g01.hdf'

    hf = h5py.File(str_path_to_geom_hdf, 'r')

    n1 = hf.get('Geometry/River Edge Lines/Polyline Points')
    n1 = np.array(n1)

    n2 = hf.get('Geometry/River Edge Lines/Polyline Parts')
    n2 = np.array(n2)

    # Create a list of  number of points per boundar line
    list_points_per_line = []
    for row in n2:
        list_points_per_line.append(row[1])

    list_line_points_x = []
    list_line_points_y = []
    for row in n1:
        list_line_points_x.append(row[0])
        list_line_points_y.append(row[1])
    boundary_points = [xy for xy in zip(list_line_points_x,
                                        list_line_points_y)]

    # Create an empty geopandas GeoDataFrame
    newdata = gpd.GeoDataFrame()
    newdata['geometry'] = None

    # next line in the hard coded projection of the BLE HEC-RAS Models
    newdata.crs = STR_PROJECT_CRS

    # Loop through the two boundary lines
    int_start_point = 0
    i = 0
    for int_num_pnts in list_points_per_line:
        newdata.loc[i, 'geometry'] = LineString(
            boundary_points[int_start_point:(int_start_point
                                             + int_num_pnts - 1)])

        i += 1
        int_start_point = int_start_point + int_num_pnts

    newdata = newdata.to_crs("EPSG:3857")

    # Create a folder for the limits of inundation shapefile
    # NOTE - str_path_to_create is from outside of function - 2021.03.21
    str_inundate_path_to_create = str_path_to_create + '\\Limits_Of_Inundation'
    os.makedirs(str_inundate_path_to_create, exist_ok=True)

    str_out_path = str_inundate_path_to_create + "\\"
    + str_feature_id_limits_fn + '_study_extent.shp'

    newdata.to_file(str_out_path)
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^


# ***********MAIN************
df_huc12 = gpd.read_file(str_huc12_area_shp)
int_huc12_index = 0

# Loop through each HUC-12
for i in df_huc12.index:
    str_huc12 = str(df_huc12['HUC_12'][i])
    int_huc12_index += 1
    print(str_huc12)

    # Constant - Folder to write the HEC-RAS folders and files
    str_root_folder_to_create = STR_ROOT_OUTPUT_DIRECTORY
    + '\\HUC_' + str_huc12

    # Select all the 'feature_id' in a given huc12
    df_streams_huc12 = df_streams_merge_2.query('huc12 == @str_huc12')

    # Reset the query index
    df_streams_huc12 = df_streams_huc12.reset_index()

    # Create a folder for the HUC-12 area
    os.makedirs(str_root_folder_to_create, exist_ok=True)

    for i in range(len(df_streams_huc12)):
        str_feature_id = str(df_streams_huc12.loc[[i], 'feature_id'].values[0])
        flt_us_xs = float(df_streams_huc12.loc[[i], 'us_xs'].values[0])
        flt_ds_xs = float(df_streams_huc12.loc[[i], 'ds_xs'].values[0])
        flt_max_q = float(df_streams_huc12.loc[[i], 'peak_flow'].values[0])
        flt_max_q = flt_max_q * FLT_MAX_MULTIPLY
        str_geom_path = df_streams_huc12.loc[[i], 'ras_path'].values[0]
        int_max_q = int(flt_max_q)

        # create a folder for each feature_id
        str_path_to_create = str_root_folder_to_create + '\\' + str_feature_id
        os.makedirs(str_path_to_create, exist_ok=True)

        # create a HEC-RAS folder
        str_hecras_path_to_create = str_path_to_create + '\\HEC-RAS'
        os.makedirs(str_hecras_path_to_create, exist_ok=True)

        print(str_feature_id + ': ' + str_geom_path + ': ' + str(int_max_q))

        # create the HEC-RAS truncated models
        try:
            # sometimes the HEC-RAS model
            # does not run (example: duplicate points)

            river = fn_create_hecras_files(str_feature_id,
                                           str_geom_path,
                                           flt_ds_xs,
                                           flt_us_xs,
                                           int_max_q,
                                           str_hecras_path_to_create)

            if IS_CREATE_MAPS:
                # delete the RAS_Mapper Output folder
                # and all the created files (DEM GeoTiffs and Shapefiles)

                str_rasmapper_foldertodelete = str_hecras_path_to_create
                + '\\BLE_' + str_feature_id

                try:
                    shutil.rmtree(str_rasmapper_foldertodelete)
                except OSError as e:
                    print("Delete Error: " + str_feature_id)

        except:
            print("HEC-RAS Error: " + str_geom_path)
            # TODO - append to error list - 2031.03.22

print('Complete')
