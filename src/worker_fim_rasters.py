# Create flood inundation data from HEC-RAS - 1D
#
# Purpose:
# Create flood inundation rasters and supporting InFRM data from the
# preprocessed HEC-RAS geospatial 1D data.  This is a worker script
# that is used for Multi-processing.  This is where the "heavy lifting"
# is perfromed
#
# Created by: Andy Carter, PE
# Created: 2021-08-12
# Last revised - 2021.10.19
#
# Uses the 'ras2fim' conda environment
# ************************************************************

import pandas as pd
import geopandas as gpd

import os
import re
import numpy as np
import pyproj
import traceback

from datetime import date

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

from scipy.interpolate import interp1d
import shared_functions as sf
import win32com.client
# windows component object model for interaction with HEC-RAS API
# This routine uses RAS60.HECRASController (HEC-RAS v6.0.0 must be
# installed on this machine prior to execution

import rasterio
from rasterio.mask import mask
# from rasterio.warp import calculate_default_transform, reproject
from rasterio import Affine
from rasterio.enums import Resampling

from shapely.geometry import LineString

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Note: settings from tuple that is the last item in the incoming 'record_requested_stream'

# str_huc8 = tpl_settings[0]
# str_input_folder = tpl_settings[1]
# str_root_output_directory = tpl_settings[2]
# str_path_to_projection = tpl_settings[3]
# str_path_to_terrain = tpl_settings[4]
# str_plan_middle_path = tpl_settings[5]
# str_project_footer_path = tpl_settings[6]
# flt_interval = tpl_settings[7]
# int_desired_resolution = tpl_settings[8]
# int_xs_buffer = tpl_settings[9]
# is_create_maps = tpl_settings[10]
# int_number_of_steps = tpl_settings[11]
# int_starting_flow = tpl_settings[12]
# flt_max_multiply = tpl_settings[13]
# flt_buffer = tpl_settings[14]
# str_plan_footer_path = tpl_settings[15]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_get_features(gdf, int_poly_index):
    """Function to parse features from GeoDataFrame
    in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][int_poly_index]['geometry']]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~

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

# .................................
def fn_append_error(str_f_id_fn,
                    str_geom_path_fn,
                    str_huc12_fn,
                    str_directory_fn, 
                    exception_msg):
    
    # creates a csv file of the errors found during processing
    str_error_path = os.path.join(str_directory_fn, 'error_found.csv')
    
    # if file exists then open it
    if os.path.exists(str_error_path):
        # open the csv
        df_error = pd.read_csv(str_error_path, index_col=0)
        # add the record to the file
        ds_series = pd.Series([str_f_id_fn, str_geom_path_fn, str_huc12_fn, exception_msg],
                             index=['feature_id', 'geom_path', 'huc_12', 'err'])
        df_error = df_error.append(ds_series, ignore_index=True)
    else:
        # create the file and append new row
        df_error = pd.DataFrame([[str_f_id_fn, str_geom_path_fn, str_huc12_fn, exception_msg]],
                                columns=['feature_id', 'geom_path', 'huc_12', 'err'])
    
    # write out the file
    df_error.to_csv(str_error_path)
    # close the dataframe
    del df_error
# .................................


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def fn_create_rating_curve(list_int_step_flows_fn,
                           list_step_profiles_fn,
                           str_feature_id_fn,
                           str_path_to_create_fn,
                           model_unit, 
                           list_step_profiles_wse):
                        #    list_int_step_flows_wse_fn):

    str_file_name = str_feature_id_fn + '_rating_curve.png'

    # Create a Rating Curve folder
    #str_rating_path_to_create = str_path_to_create_fn + '\\Rating_Curve'
    str_rating_path_to_create = str_path_to_create_fn + 'Rating_Curve'
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
    
    if model_unit == 'meter':
        plt.ylabel('Average Depth (m)')
        plt.xlabel('Discharge (m^3/s)')
    else:
        plt.ylabel('Average Depth (ft)')
        plt.xlabel('Discharge (ft^3/s)')

    plt.grid(True)

    str_rating_image_path = str_rating_path_to_create + '\\' + str_file_name
    plt.savefig(str_rating_image_path,
                dpi=300,
                bbox_inches="tight")
    
    plt.cla()
    plt.close('all')

    # Create CSV for the rating curve
    # list_step_profiles_fn is already decimal (1.5)
    if model_unit == 'feet':
        d = {'discharge_cfs': list_int_step_flows_fn,
             'stage_ft': list_step_profiles_fn, 
             'wse_ft': list_step_profiles_wse} #list_int_step_flows_wse_fn}
    else:
        d = {'discharge_cms': list_int_step_flows_fn,
             'stage_m': list_step_profiles_fn,
             'wse_m': list_step_profiles_wse} #list_int_step_flows_wse_fn}


    df_rating_curve = pd.DataFrame(d)

    if model_unit == 'feet': 
        # we need to add meter columns and convert feet to metric
        df_rating_curve["discharge_cms"] = np.round(df_rating_curve["discharge_cfs"].values * 0.3048 ** 3, 3)
        df_rating_curve["stage_m"] = np.round(df_rating_curve["stage_ft"].values * 0.3048, 3)

        #next two lines duplicated so the columns are beside the stage_m columns
        df_rating_curve["stage_mm"] = df_rating_curve["stage_m"] * 1000 # change to millimeters
        df_rating_curve["stage_mm"] = df_rating_curve["stage_mm"].astype('int')

        df_rating_curve["wse_m"] = np.round(df_rating_curve["wse_ft"].values * 0.3048, 3)
    else: ## meters
        # need rounding (precison control even for unit of meters)
        df_rating_curve["discharge_cms"] = np.round(df_rating_curve["discharge_cms"].values, 3)
        df_rating_curve["stage_m"] = np.round(df_rating_curve["stage_m"].values, 3)

        #next two lines duplicated so the columns are beside the stage_m columns
        df_rating_curve["stage_mm"] = df_rating_curve["stage_m"] * 1000 # change to millimeters
        df_rating_curve["stage_mm"] = df_rating_curve["stage_mm"].astype('int')

        df_rating_curve["wse_m"] = np.round(df_rating_curve["wse_m"].values, 3)

    str_csv_path = str_rating_path_to_create + '\\' + str_feature_id_fn + '_rating_curve.csv'

    df_rating_curve.to_csv(str_csv_path, index=False)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# ................................
def fn_create_flow_file_second_pass(str_ras_project_fn,
                                    list_int_stepflows_fn,
                                    list_step_profiles_fn,
                                    model_unit):

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
    if model_unit == 'meter':
        str_flow_file += fn_create_profile_names(list_step_profiles_fn, 'm') + '\n'
    else:
        str_flow_file += fn_create_profile_names(list_step_profiles_fn, 'ft') + '\n'

    str_flow_file += "River Rch & RM="

    str_flow_file += str_fn_river + "," + str_fn_reach + "," + str_fn_start_xs + '\n'

    str_flow_file += fn_format_flow_values(list_int_stepflows_fn) + '\n'

    for i in range(int_fn_num_profiles):
        str_flow_file += "Boundary for River Rch & Prof#="

        str_flow_file += str_fn_river + "," + str_fn_reach + ", " + str(i+1) + '\n'

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

# +++++++++++++++++++++++++
def fn_create_ras_mapper_xml(str_feature_id_fn,
                             str_ras_projectpath_fn,
                             list_step_profiles_xml_fn,
                             model_unit,
                             tpl_settings):
    
    # get settings from tpl_settings
    str_path_to_projection = tpl_settings[3]
    str_path_to_terrain = tpl_settings[4]

    # Function to create the RASMapper XML and add the requested DEM's to be created
    
    # Get huc12 from the output path
    list_path = str_ras_projectpath_fn.split(os.sep)
    # get from output directory - Example: HUC_123456789012
    str_huc12 = str(list_path[-4])
    # remove the 'HUC_' header
    str_huc12 = str_huc12[4:] 

    # Create .rasmap XML file
    str_ras_mapper_file = ''

    str_ras_mapper_file = r'<RASMapper>' + '\n'
    str_ras_mapper_file += r'  <Version>2.0.0</Version>' + '\n'

    str_ras_mapper_file += r'  <RASProjectionFilename Filename="' + str_path_to_projection + r'" />' + '\n'

    str_ras_mapper_file += r'  <Geometries Checked="True" Expanded="True">' + '\n'

    str_ras_mapper_file += r'    <Layer Name="BLE_' + str_feature_id_fn + '"'
    str_ras_mapper_file += r' Type="RASGeometry" Checked="True" '
    str_ras_mapper_file += r'Expanded="True" Filename="'

    str_ras_mapper_file += r'.' + '\\' + str_feature_id_fn + '.g01.hdf">' + '\n'

    str_ras_mapper_file += r'      <Layer Type="RAS'
    str_ras_mapper_file += r'River" Checked="True" />' + '\n'
    str_ras_mapper_file += r'      <Layer Type="RASXS" Checked'
    str_ras_mapper_file += r'="True" />' + '\n'
    str_ras_mapper_file += r'    </Layer>' + '\n'
    str_ras_mapper_file += r'  </Geometries>' + '\n'

    str_ras_mapper_file += r'  <Results Expanded="True">' + '\n'
    str_ras_mapper_file += r'    <Layer Name="BLE_'
    str_ras_mapper_file += str_feature_id_fn + '" Type="RAS' + 'Results" Expanded="True" Filename=".'
    str_ras_mapper_file += '\\' + str_feature_id_fn + r'.p01.hdf">' + '\n'
    str_ras_mapper_file += '      <Layer Type="RASGeometry" Filename=".'
    str_ras_mapper_file += '\\' + str_feature_id_fn + r'.p01.hdf" />' + '\n'

    int_index = 0

    # Loop through all profiles and create an XML request to map each depth
    # grid in the list_step_profiles_xml_fn
    for i in list_step_profiles_xml_fn:
        str_ras_mapper_file += '      <Layer Name="depth" Type="RAS'
        str_ras_mapper_file += 'ResultsMap" Checked="True" Filename=".'
        
        if model_unit=='meter':
            str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn + '\\' + 'Depth (' + str(i) + 'm).vrt">' + '\n'
        else:
            str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn + '\\' + 'Depth (' + str(i) + 'ft).vrt">' + '\n'
        
        str_ras_mapper_file += '        <LabelFeatures '
        str_ras_mapper_file += 'Checked="True" Center="False" '
        str_ras_mapper_file += 'rows="1" cols="1" r0c0="FID" '
        str_ras_mapper_file += 'Position="5" Color="-16777216" />' + '\n'
        str_ras_mapper_file += '        <MapParameters MapType="depth" Layer'
        str_ras_mapper_file += 'Name="Depth" OutputMode="Stored Current '
        
        if model_unit=='meter':
            str_ras_mapper_file += 'Terrain" StoredFilename=".' + '\\BLE_' + str_feature_id_fn + '\\Depth (' + str(i) + 'm).vrt"'
        else:
            str_ras_mapper_file += 'Terrain" StoredFilename=".' + '\\BLE_' + str_feature_id_fn + '\\Depth (' + str(i) + 'ft).vrt"'
        
        str_ras_mapper_file += ' Terrain="' + str_huc12 + '" ProfileIndex="' + str(int_index) + '" '
        str_ras_mapper_file += ' ProfileName="' + str(i) + 'm" ArrivalDepth="0" />' + '\n'
        str_ras_mapper_file += '      </Layer>' + '\n'

        int_index += 1

    # Get the highest (last profile) flow innundation polygon
    # --------------------
    str_ras_mapper_file += '      <Layer Name="depth" Type="RAS'
    str_ras_mapper_file += 'ResultsMap" Checked="True" Filename=".'

    str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn + '\\' + 'Inundation Boundary (' + str(list_step_profiles_xml_fn[-1])

    str_ras_mapper_file += 'm Value_0).shp">' + '\n'
    str_ras_mapper_file += '        <MapParameters MapType="depth" '
    str_ras_mapper_file += 'LayerName="Inundation Boundary"'
    str_ras_mapper_file += ' OutputMode="Stored Polygon'
    str_ras_mapper_file += ' Specified Depth"  StoredFilename=".'
    str_ras_mapper_file += '\\' + 'BLE_' + str_feature_id_fn + '\\' + 'Inundation Boundary (' + str(list_step_profiles_xml_fn[-1])
    str_ras_mapper_file += 'm Value_0).shp"  Terrain="' + str_huc12 + '" ProfileIndex="' + str(len(list_step_profiles_xml_fn)-1)
    str_ras_mapper_file += '"  ProfileName="' + str(list_step_profiles_xml_fn[-1]) + 'm"  ArrivalDepth="0" />' + '\n'
    str_ras_mapper_file += '      </Layer>' + '\n'
    # --------------------

    str_ras_mapper_file += r'    </Layer>' + '\n'
    str_ras_mapper_file += r'  </Results>' + '\n'

    str_ras_mapper_file += r'  <Terrains Checked="True" Expanded="True">' + '\n'

    str_ras_mapper_file += r'    <Layer Name="' + str_huc12 + r'" Type="TerrainLayer" Checked="True" Filename="'

    str_ras_mapper_file += str_path_to_terrain + '\\' + str_huc12 + r'.hdf">' + '\n'

    str_ras_mapper_file += r'    </Layer>' + '\n'
    str_ras_mapper_file += r'  </Terrains>' + '\n'

    str_ras_mapper_file += r'</RASMapper>'

    file = open(str_ras_projectpath_fn[:-4] + '.rasmap', "w")
    file.write(str_ras_mapper_file)
    file.close()
# +++++++++++++++++++++++++

#  Jul 30, 2023 - Deprecated
'''
# """"""""""""""""""""""""""
def fn_create_study_area(str_polygon_path_fn, str_feature_id_poly_fn, tpl_settings):
    
    # get settings from the tpl_settings
    flt_buffer = tpl_settings[14] 

    # Function to create the study limits shapefile (polyline)

    # Folder to store the study area
    # TODO - str_path_to_create from outside function - 2021.08.13
    str_studylimits_pathtocreate = str_path_to_create + '\\Study_Area'
    os.makedirs(str_studylimits_pathtocreate, exist_ok=True)

    dst_crs_1 = 'EPSG:4326'  # Projection  for decimal degree limit coordinates
    dst_crs_2 = 'EPSG:3857'  # Output Projection (WGS 84) to write file

    str_output_path = str_studylimits_pathtocreate + '\\' + str_feature_id_poly_fn + '_study_area.shp'

    gdf_inputshape = gpd.read_file(str_polygon_path_fn)

    # Buffer the shapefile
    gdf_flood_buffer = gdf_inputshape.buffer(flt_buffer, 8)
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
'''

# ...........................
def fn_run_hecras(str_ras_projectpath, int_peak_flow, model_unit, tpl_settings):
    
    # get settings from tpl_settings
    flt_interval = tpl_settings[7]
    int_number_of_steps = tpl_settings[11]
    int_starting_flow = tpl_settings[12]
    
    hec = None

    try:

        hec = win32com.client.Dispatch("RAS60.HECRASController")
        #hec.ShowRas()

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

        # HEC-RAS ID of output variables: Max channel depth, channel reach length, and water surface elevation
        int_max_depth_id, int_node_chan_length, int_water_surface_elev = 4, 42, 2

        # ----------------------------------
        # Create a list of the simulated flows
        # TODO: Maybe rename these variables 
        list_flow_steps = []

        int_delta_flow_step = int(int_peak_flow // (int_number_of_steps - 2))

        for i in range(int_number_of_steps):
            list_flow_steps.append((i*int_delta_flow_step) + int_starting_flow )
        
        # ----------------------------------

        # **********************************
        # intialize list of the computed average depths
        list_avg_depth = []

        # initialize list of water surface elevations
        list_avg_water_surface_elev = []
        
        for int_prof in range(int_number_of_steps):
            
            # get a count of the cross sections in the HEC-RAS model
            int_xs_node_count = 0
            for i in range(0, NNod):
                if TabNTyp[i] == "":
                    int_xs_node_count += 1
        
            # initalize six numpy arrays
            arr_max_depth = np.empty([int_xs_node_count], dtype=float)
            arr_channel_length = np.empty([int_xs_node_count], dtype=float)
            arr_avg_depth = np.empty([int_xs_node_count], dtype=float)
            arr_multiply = np.empty([int_xs_node_count], dtype=float)
            arr_water_surface_elev = np.empty([int_xs_node_count], dtype=float)
            arr_avg_water_surface_elev = np.empty([int_xs_node_count], dtype=float)

            int_count_nodes = 0
        
            for i in range(0, NNod):
                if TabNTyp[i] == "": # this is a XS (not a bridge, culvert, inline, etc...)
                    
                    # reading max depth in cross section
                    arr_max_depth[int_count_nodes], v1, v2, v3, v4, v5, v6 = hec.Output_NodeOutput(RivID, RchID, i+1, 0, int_prof+1, int_max_depth_id)

                    # reading water surface elevation in cross section
                    arr_water_surface_elev[int_count_nodes], v1, v2, v3, v4, v5, v6 = hec.Output_NodeOutput(RivID, RchID, i+1, 0, int_prof+1, int_water_surface_elev)

                    # reading the distance between cross sections (center of channel)
                    arr_channel_length[int_count_nodes], v1, v2, v3, v4, v5, v6 = hec.Output_NodeOutput(RivID, RchID, i+1, 0, int_prof+1, int_node_chan_length)
        
                    int_count_nodes += 1

            # Revise the last channel length to zero
            arr_channel_length[len(arr_channel_length) - 1] = 0
        
            # compute an average depth between cross sections
            k = 0
            for x in arr_max_depth:
                if k != (len(arr_max_depth)) - 1:
                    # get the average depth between two sections
                    arr_avg_depth[k] = (arr_max_depth[k] + arr_max_depth[k+1]) / 2

                k += 1

            # average depth between two cross sections times channel length
            arr_multiply = arr_avg_depth * arr_channel_length
        
            # compute the average depth on the reach
            flt_avg_depth = (np.sum(arr_multiply)) / (np.sum(arr_channel_length))
            list_avg_depth.append(flt_avg_depth)

            # compute an average WSE between cross sections
            k = 0
            for x in arr_water_surface_elev: 
                if k != (len(arr_water_surface_elev)) - 1:

                    # get the average water surface elevation between two sections
                    arr_avg_water_surface_elev[k] = (arr_water_surface_elev[k] + arr_water_surface_elev[k+1]) / 2

                k += 1
            
            # average WSE between two cross sections times channel length
            arr_multiply_wse = arr_avg_water_surface_elev * arr_channel_length
        
            # compute the average WSE on the reach
            flt_avg_wse = (np.sum(arr_multiply_wse)) / (np.sum(arr_channel_length))
            list_avg_water_surface_elev.append(flt_avg_wse)

        # **********************************

        # ------------------------------------------
        # create two numpy arrays for the linear interpolator
        # arr_avg_depth = np.array(list_avg_depth)

        # f is the linear interpolator
        f = interp1d(list_avg_depth, list_flow_steps)

        # Get the max value of the Averge Depth List
        int_max_depth = int(max(list_avg_depth) // flt_interval)
        # Get the min value of Average Depth List
        int_min_depth = int((min(list_avg_depth) // flt_interval) + 1)

        list_step_profiles = []

        # Create a list of the profiles at desired increments
        for i in range(int_max_depth - int_min_depth + 1):
            int_depth_interval = (i + int_min_depth) * flt_interval

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

        int_max_wse = int(max(list_avg_water_surface_elev) // flt_interval)
        int_min_wse = int((min(list_avg_water_surface_elev) // flt_interval) + 1)

        list_step_profiles_wse = []


        #print("***********************")

        #print("list_int_step_flows is ")
        #print(f"...len is {len(list_int_step_flows)}")    
        #print(list_int_step_flows)
        #print()
        #print("list_step_profiles is ")
        #print(f"...len is {len(list_step_profiles)}")    
        #print(list_step_profiles)
        #print()
        #print(f"list_avg_water_surface_elev is {list_avg_water_surface_elev}")
        #print(f"average is {np.mean(list_avg_water_surface_elev)}")
        #print(f"len is {len(list_avg_water_surface_elev)}")  
        #print(f"Min is {np(list_avg_water_surface_elev)}")      

        #print("***********************")

        #print(f"flt_interval is {flt_interval}")
        #print(f"int_min_wse is {int_min_wse}")
        #print(f"int_max_wse is {int_max_wse}")    
        #print(f"int_max_wse - int_min_wse is {int_max_wse - int_min_wse}")  

        #print(f"depth min is {int_min_depth}")
        #print(f"depth max is {int_max_depth}")    
        #print(f"depth max - depth min is {int_max_depth - int_min_depth}")  


        # Create a list of the profiles at desired increments
        #for i in range(int_max_wse - int_min_wse + 1):
        #for i in range(int_max_wse - int_min_wse):
        for i in range(int_max_depth - int_min_depth + 1):
            #print("----------------------------")
            #print(i)
            int_wse_interval = (i + int_min_wse) * flt_interval
            #print(f"int_wse_interval (a) is {int_wse_interval}")

            # round this to nearest 1/10th
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            int_wse_interval = round(int_wse_interval, 1)
            #print(f"int_wse_interval (b) is {int_wse_interval}")        
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            list_step_profiles_wse.append(int_wse_interval)

        #print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")


        # ............
        # Get the feature_id and the path to create
        list_path = str_ras_projectpath.split(os.sep)
        str_feature_id = str(list_path[-3])
        
        # Drive path
        str_path_to_create = list_path[0] + "\\"
        # path excluding file name and last folder
        for i in range(1, len(list_path) - 2):
            str_path_to_create += list_path[i] + "\\"
        # ............
        
        #print("list_step_profiles_wse is ")
        #print(f"...len is {len(list_step_profiles_wse)}")    
        #print(list_step_profiles_wse)
        #print()


        # ------------------------------------------
        # generate the rating curve data
        fn_create_rating_curve(list_int_step_flows, 
                            list_step_profiles, 
                            str_feature_id, 
                            str_path_to_create, 
                            model_unit, 
                            list_step_profiles_wse)
        # ------------------------------------------

        hec.QuitRas()  # close HEC-RAS
        

        # append the flow file with one for the second pass at even depth intervals
        fn_create_flow_file_second_pass(str_ras_projectpath,
                                        list_int_step_flows,
                                        list_step_profiles,
                                        model_unit)


        if len(list_int_step_flows) > 0:
            # *************************************************
            fn_create_ras_mapper_xml(str_feature_id,
                                    str_ras_projectpath,
                                    list_step_profiles,
                                    model_unit,
                                    tpl_settings)
            # *************************************************

            # Run HEC-RAS with the new flow data
            #hec.ShowRas()
            hec.Project_Open(str_ras_projectpath)   # opening HEC-RAS

            # to be populated: number and list of messages, blocking mode
            NMsg, TabMsg, block = None, None, True

            # computations of the current plan
            v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

            hec.QuitRas()  # close HEC-RAS
            
            # *************************************

        '''
            # *************************************************
            # creates the model limits boundary polylines
            fn_create_inundation_limits(str_feature_id,
                                        str_ras_projectpath)
            # *************************************************

            #*************************************************
            #create the FDST metadata (json) file
            fn_createFDST_metadata(strCOMID,list_StepProfiles)
            #*************************************************

        '''
        #del hec             # delete HEC-RAS controller

    except Exception as ex:
        # re-raise it as error handling is farther up the chain
        # but I do need the finally to ensure the hec.QuitRas() is run
        print("++++++++++++++++++++++++")
        print("An exception occurred with the HEC-RAS engine or its parameters.")
        print(f"details: {ex}")
        print()
        # re-raise it for logging (coming)
        raise ex
    
    finally:
        # Especially with multi proc, if an error occurs with HEC-RAS (engine 
        # or values submitted), HEC-RAS will not close itself just becuase of an python
        # exception. This leaves orphaned process threads (visible in task manager)
        # and sometimes visually as well.

        if (hec is not None):
            try:
                hec.QuitRas()   # close HEC-RAS no matter watch
            except Exception as ex2:
                print("--- An error occured trying to close the HEC-RAS window process")
                print(f"--- Details: {ex2}")
                print()
                # do nothng


# ...........................

# ||||||||||||||||||||||||
def fn_create_hecras_files(str_feature_id,
                           str_read_geom_file_path,
                           flt_min_range,
                           flt_max_range,
                           int_max_flow,
                           str_output_filepath,
                           tpl_settings):

    flt_ds_xs = flt_min_range
    flt_us_xs = flt_max_range
    
    # get settings from tpl_settings
    str_plan_middle_path = tpl_settings[5]
    str_project_footer_path = tpl_settings[6]
    int_xs_buffer = tpl_settings[9]
    is_create_maps = tpl_settings[10]
    int_number_of_steps = tpl_settings[11]
    int_starting_flow = tpl_settings[12]
    str_plan_footer_path = tpl_settings[15]

    # get the project (HEC-RAS) file (same name and folder as geom)
    str_read_prj_file_path = str_read_geom_file_path[:-3] + 'prj'
    model_unit = sf.model_unit_from_ras_prj(str_read_prj_file_path)

    with open(str_read_geom_file_path) as f:
        list_all_items = []
        #For each line in geometry
        for line in f:
            #Get each item info (cross Sections, bridges, inline structures)  

            if line.startswith('Type RM Length L Ch R ='):

                # get data on each item
                [item_key, item_data] = line.split('=')

                #clean the data
                item_data = item_data.strip()

                #convert to list
                list_items = item_data.split(",")

                #add all items to one combined list
                list_all_items.append(list_items)

    # create and populate a dataframe of the items (cross Sections, bridges, inline structures)
    df_items = pd.DataFrame(list_all_items, columns=['Type', 'Station', 'LOB', 'Channel', 'ROB'])

    df_start_stop_item = pd.DataFrame(columns=['start', 'end'])

    with open(str_read_geom_file_path) as f:
        file_contents = f.read()

    for index, row in df_items.iterrows():
        str_item_header = "Type RM Length L Ch R = " + str(row['Type'])
        str_item_header += ',' + str(str(row['Station']))
        # 2021.09.01 - Just up to the XS name (as the
        # interpolated values have a '*' at the end of the cross section)

        # Find the requested item
        pattern = re.compile(str_item_header)
        matches = pattern.finditer(file_contents)

        #get the starting point of the item in file
        for match in matches:
            #get the starting position
            int_start_position = match.start()

        if index < len(df_items.index) - 1:

            #build a regex query string to get between two values
            str_query = '(' + str_item_header + ')(.*?)(?=Type RM Length L Ch R)'
            tup_re_match = re.findall(str_query, file_contents, re.DOTALL)

            #returns tuple in two parts - concat them
            str_item = tup_re_match[0][0] + tup_re_match[0][1]

            int_end_position = int_start_position + len(str_item) - 1

        else:
            #parse the most downstream item
            str_remainder = file_contents[int_start_position:]

            #find blank rows
            pattern = re.compile('\n\s*\n')
            matches = pattern.finditer(str_remainder)

            list_start_blankline = []

            i = 0
            for match in matches:
                i = i + 1
                list_start_blankline.append(match.start())

            #ignore blank lines that are in the last 30 characters
            int_max_space = len(str_remainder) - 5
            #int_max_space = len(str_remainder)

            for i in list_start_blankline:
                if i > int_max_space:
                    list_start_blankline.remove(i)

            # the last item in the list_start_blankline is assumed to be the end
            # of that item

            # TODO - 2021.08.31 Error line below - MAC
            # TODO - IndexError: list index out of range
            if len(list_start_blankline) > 0:
                int_end_position = int_start_position + list_start_blankline[-1]
            else:
                int_end_position = int_start_position + int_max_space

        df_start_stop_item = df_start_stop_item.append({'start': int_start_position,
                                                        'end': int_end_position},
                                                       ignore_index=True) 
    df_item_limits = pd.concat([df_items, df_start_stop_item], axis=1, join="inner")
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # All text up to the first cross section - Header of the Geom File
    str_header = file_contents[0:df_item_limits.iloc[0]['start']]

    # Rename the geometry data - edit the first line
    pattern = re.compile(r'Geom Title=.*')
    geom_matches = pattern.finditer(str_header)

    for match in geom_matches:
        str_header = str_header[match.end()+1:(len(str_header))]
        str_header = "Geom Title=BLE_" + str_feature_id + '\n' + str_header
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # -------------------------------------
    # Create file footer
    # From end marker of last cross section to end of file
    str_footer = file_contents[(df_item_limits.iloc[-1]['end']):(len(file_contents))]
    # -------------------------------------

    # .....................................
    # Create the HEC-RAS Geomerty file
    str_geom = str_header

    # Determine the items (XS, bridge, inline) within the valid range
    int_first_index = -1
    int_last_index = -1

    b_found_first_index = False
    
    #---------------
    # Clean up the interpolated cross section
    for index, row in df_item_limits.iterrows():
        # if there is a star in the cross section name
        if '*' in row['Station']:
            str_replace = row['Station']
            list_found = re.findall(r"[-+]?\d*\.\d+|\d+", str_replace)
            str_replace = list_found[0]
            df_item_limits.at[index, 'Station'] = str_replace
    #---------------      
    
    for index, row in df_item_limits.iterrows():
        if float(row['Station']) >= flt_ds_xs:
            if float(row['Station']) <= flt_us_xs:
                if not b_found_first_index:
                    int_first_index = index
                    b_found_first_index = True
                int_last_index = index

    if int_first_index > -1 and int_last_index > -1:
        # Get the upstream Cross section plus a index buffer
        if (int_first_index - int_xs_buffer) >= 0:
            int_first_index -= int_xs_buffer

            # pad upstream until item is a cross section (not bridge, inline, etc.)
            while int(df_item_limits.iloc[int_first_index]['Type']) != 1 or int_first_index == 0:
                int_first_index -= 1
        else:
            int_first_index = 0

        # Get the downstream cross section plus a index buffer
        if (int_last_index + int_xs_buffer) < len(df_item_limits):
            int_last_index += int_xs_buffer

            # pad downstream until item is a cross section (not bridge, inline, etc.)
            # revised 2021.08.10
            while int(df_item_limits.iloc[int_last_index]['Type']) != 1 or int_last_index == len(df_item_limits) - 2:
                int_last_index += 1

        else:
            # revised 2021.08.10
            int_last_index = len(df_item_limits)

        # get the name of the upstream cross section
        str_xs_upstream = df_item_limits.iloc[int_first_index]['Station']

        for index, row in df_item_limits.iterrows():
            if (index >= int_first_index) and (index <= int_last_index):
                str_geom += file_contents[(row['start']):(row['end'])]
                str_geom += "\n\n"

        str_geom += str_footer

        # Write the requested file
        file = open(str_output_filepath + "\\" + str_feature_id + '.g01', "w")
        file.write(str_geom)
        file.close()
        
    # .....................................
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # All text up to the first cross section - Header of the Geom File
    str_header = file_contents[0:df_item_limits.iloc[0]['start']]

    # Rename the geometry data - edit the first line
    pattern = re.compile(r'Geom Title=.*')
    geom_matches = pattern.finditer(str_header)

    for match in geom_matches:
        str_header = str_header[match.end()+1:(len(str_header))]
        str_header = "Geom Title=BLE_" + str_feature_id + '\n' + str_header
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # -------------------------------------
    # Create file footer
    # From end marker of last cross section to end of file
    str_footer = file_contents[(df_item_limits.iloc[-1]['end']):(len(file_contents))]
    # -------------------------------------

    # .....................................
    # Create the HEC-RAS Geomerty file
    str_geom = str_header

    # Determine the items (XS, bridge, inline) within the valid range
    int_first_index = -1
    int_last_index = -1

    b_found_first_index = False
    for index, row in df_item_limits.iterrows():
        if float(row['Station']) >= flt_ds_xs:
            if float(row['Station']) <= flt_us_xs:
                if not b_found_first_index:
                    int_first_index = index
                    b_found_first_index = True
                int_last_index = index

    if int_first_index > -1 and int_last_index > -1:
        # Get the upstream Cross section plus a index buffer
        if (int_first_index - int_xs_buffer) >= 0:
            int_first_index -= int_xs_buffer

            # pad upstream until item is a cross section (not bridge, inline, etc.)
            while int(df_item_limits.iloc[int_first_index]['Type']) != 1 or int_first_index == 0:
                int_first_index -= 1
        else:
            int_first_index = 0

        # Get the downstream cross section plus a index buffer
        if (int_last_index + int_xs_buffer) < len(df_item_limits):
            int_last_index += int_xs_buffer

            # pad downstream until item is a cross section (not bridge, inline, etc.)
            # revised 2021.08.10
            while int(df_item_limits.iloc[int_last_index]['Type']) != 1 or int_last_index == len(df_item_limits) - 2:
                int_last_index += 1

        else:
            # revised 2021.08.10
            int_last_index = len(df_item_limits)

        # get the name of the upstream cross section
        str_xs_upstream = df_item_limits.iloc[int_first_index]['Station']

        for index, row in df_item_limits.iterrows():
            if (index >= int_first_index) and (index <= int_last_index):
                str_geom += file_contents[(row['start']):(row['end'])]
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

    str_flowfile += "Number of Profiles= " + str(int_number_of_steps) + '\n'

    # get a list of the first pass flows
    list_firstflows = fn_create_firstpass_flowlist(int_starting_flow,
                                                   int_max_flow,
                                                   int_number_of_steps)

    str_flowfile += fn_create_profile_names(list_firstflows, 'cms') + '\n'
    # Note - 2021.03.20 - cms is hard coded in above line

    str_flowfile += "River Rch & RM="

    str_flowfile += str_river + "," + str_reach + "," + str_xs_upstream + '\n'

    str_flowfile += fn_format_flow_values(list_firstflows) + '\n'

    for i in range(int_number_of_steps):
        str_flowfile += "Boundary for River Rch & Prof#="

        str_flowfile += str_river + "," + str_reach + ", " + str(i+1) + '\n'

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
    with open(str_plan_middle_path) as f:
        file_contents = f.read()
    str_planfile += file_contents

    # To map the requested Depth Grids
    # Set to 'Run RASMapper=0 ' to not create requested DEMs
    # Set to 'Run RASMapper=-1 ' to create requested DEMs
    if is_create_maps:
        str_planfile += '\n' + r'Run RASMapper=-1 ' + '\n'
    else:
        str_planfile += '\n' + r'Run RASMapper=0 ' + '\n'

    # read the plan footer input file
    with open(str_plan_footer_path) as f:
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

    str_projectfile += "Current Plan=p01" + '\n'
    str_projectfile += "Default Exp/Contr=0.3,0.1" + '\n'

    # set up project as either SI of English Units
    if model_unit == 'meter':
        str_projectfile += 'SI Units' + '\n'
    else:
        # English Units
        str_projectfile += 'English Units' + '\n'

    # read the project footer input file
    with open(str_project_footer_path) as f:
        file_contents = f.read()
    str_projectfile += file_contents

    file = open(str_output_filepath + "\\" + str_feature_id + '.prj', "w")
    file.write(str_projectfile)
    file.close()
    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    str_ras_projectpath = (str_output_filepath + "\\" + str_feature_id + '.prj')


    b_terrain_check_only = tpl_settings[16]
    if not b_terrain_check_only:
        # run the hec-ras model 
        fn_run_hecras(str_ras_projectpath, int_max_flow, model_unit, tpl_settings)
    #######################

    #return (pd_series_stats)
# ||||||||||||||||||||||||

def fn_main_hecras(record_requested_stream):
    
    str_feature_id = str(record_requested_stream[0])
    flt_us_xs = float(record_requested_stream[1])
    flt_ds_xs = float(record_requested_stream[2])
    flt_max_q = float(record_requested_stream[3])
    str_geom_path = str(record_requested_stream[4])
    str_huc12 = str(record_requested_stream[5])
    
    
    # Parse the settings variables from the tuple sent from the main script
    tpl_settings  = record_requested_stream[6]
    
    # Note: settings from tuple that is the last item in the incoming 'record_requested_stream'

    # str_huc8 = tpl_settings[0]
    # str_input_folder = tpl_settings[1]
    # str_root_output_directory = tpl_settings[2]
    # str_path_to_projection = tpl_settings[3]
    # str_path_to_terrain = tpl_settings[4]
    # str_plan_middle_path = tpl_settings[5]
    # str_project_footer_path = tpl_settings[6]
    # flt_interval = tpl_settings[7]
    # int_desired_resolution = tpl_settings[8]
    # int_xs_buffer = tpl_settings[9]
    # is_create_maps = tpl_settings[10]
    # int_number_of_steps = tpl_settings[11]
    # int_starting_flow = tpl_settings[12]
    # flt_max_multiply = tpl_settings[13]
    # flt_buffer = tpl_settings[14]
    # str_plan_footer_path = tpl_settings[15]
    # b_terrain_check_only = tpl_settings[16]
    
    # -------
    # get settings from tpl_settings
    str_root_output_directory = tpl_settings[2]
    flt_max_multiply = tpl_settings[13]
    
    
    flt_max_q = flt_max_q * flt_max_multiply
    int_max_q = int(flt_max_q)
    
    str_root_folder_to_create = str_root_output_directory + '\\HUC_' + str_huc12
    
    # create a folder for each feature_id
    str_path_to_create = str_root_folder_to_create + '\\' + str_feature_id
    os.makedirs(str_path_to_create, exist_ok=True)
    
    # create a HEC-RAS folder
    str_hecras_path_to_create = str_path_to_create + '\\HEC-RAS'
    os.makedirs(str_hecras_path_to_create, exist_ok=True)
    
    #print(str_feature_id + ': ' + str_geom_path + ': ' + str(int_max_q))
    
    #river = fn_create_hecras_files(str_feature_id, str_geom_path, flt_ds_xs, flt_us_xs, int_max_q, str_hecras_path_to_create, tpl_settings)
    
    # create the HEC-RAS truncated models
    try:
        # sometimes the HEC-RAS model
        # does not run (example: duplicate points)

        river = fn_create_hecras_files(str_feature_id, str_geom_path, flt_ds_xs, flt_us_xs, int_max_q, str_hecras_path_to_create, tpl_settings)
    except Exception as ex:
        #print("HEC-RAS Error: " + str_geom_path)
        print("*******************")
        print("Error:")
        print(f"   str_feature_id = {str_feature_id}")        
        errMsg = str(ex) + " \n   " + traceback.format_exc()
        print(errMsg)
        print("   for more details.. see the 05_hecras_output / errors_found.log")
        fn_append_error(str_feature_id, str_geom_path, str_huc12, str_root_output_directory, errMsg)
        print("*******************")        
    
    #return(str_feature_id)
