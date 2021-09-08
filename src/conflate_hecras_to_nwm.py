# This is the second pre-processing tool that turns HEC-RAS 1D modeling into
# flood inundation mapping products.  This routine is used to conflate the
# national water model streams (feature_id) to the models provided in the
# supplied HEC-RAS files
#
# Created by: Andy Carter, PE
# Last revised - 2021.09.08
#
# ras2fim - Second pre-processing script
# Uses the 'ras2fim' conda environment

import geopandas as gpd
import pandas as pd
from geopandas.tools import sjoin

import argparse

from shapely import wkt
from shapely.geometry import LineString, Point, mapping

import xarray as xr
# may need to pip install netcdf4 for xarray
import numpy as np

from fiona import collection
from fiona.crs import from_epsg

import time


# $$$$$$$$$$$$$$$$$$$$$$
def wkt_loads(x):
    try:
        return wkt.loads(x)
    except Exception:
        return None
# $$$$$$$$$$$$$$$$$$$$$$

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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    
    parser = argparse.ArgumentParser(description='===== CONFLATE HEC-RAS TO NATIONAL WATER MODEL STREAMS =====')
    
    parser.add_argument('-w',
                        dest = "str_huc8_arg",
                        help='REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required=True,
                        metavar='STRING',
                        type=str)
    
    parser.add_argument('-i',
                        dest = "str_shp_in_arg",
                        help=r'REQUIRED: Directory containing stream and cross section shapefiles:  Example: D:\ras_shapes',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-o',
                        dest = "str_shp_out_arg",
                        help=r'REQUIRED: path to write output files: Example: D:\conflation_output',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-n',
                        dest = "str_nation_arg",
                        help=r'REQUIRED: path to national datasets: Example: E:\X-NWS\X-National_Datasets',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    args = vars(parser.parse_args())
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # INPUT
    
    print(" ")
    print("+=================================================================+")
    print("|        CONFLATE HEC-RAS TO NATIONAL WATER MODEL STREAMS         |")
    print("|     Created by Andy Carter, PE of the National Water Center     |")
    print("+-----------------------------------------------------------------+")
    
    
    STR_HUC8 = args['str_huc8_arg']
    print("  ---(w) HUC-8: " + STR_HUC8)
    
    STR_BLE_SHP_DIR = args['str_shp_in_arg']
    print("  ---(i) BLE INPUT SHP DIRECTORY: " + STR_BLE_SHP_DIR)
    
    # note the files names are hardcoded in 1 of 2
    STR_BLE_STREAM_LN = STR_BLE_SHP_DIR + '\\' + 'stream_LN_from_ras.shp'
    STR_BLE_CROSS_SECTION_LN = STR_BLE_SHP_DIR + '\\' + 'cross_section_LN_from_ras.shp'
    
    STR_OUT_PATH = args['str_shp_out_arg']
    print("  ---(o) OUTPUT DIRECTORY: " + STR_OUT_PATH)
    
    STR_NATIONAL_DATASET_PATH = args['str_nation_arg']
    print("  ---(n) NATIONAL DATASET LOCATION: " + STR_NATIONAL_DATASET_PATH)
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # distance to buffer around modeled stream centerlines
    INT_BUFFER_DIST = 300
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    INT_DISTANCE_DELTA = 67   # distance between points in ble projection units
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # Input - projection of the base level engineering models
    # get this string from the input shapefiles of the stream
    gdf_stream = gpd.read_file(STR_BLE_STREAM_LN)
    BLE_PRJ = str(gdf_stream.crs)

    # Note that this routine requires three (3) national datasets.
    # (1) the NHD Watershed Boundary dataset
    # (2) the National water model flowlines geopackage
    # (3) the National water model recurrance flows
    
    # Input - Watershed boundary data geopackage
    str_wbd_geopkg_path = STR_NATIONAL_DATASET_PATH + '\\' + 'NHDPlusV21_WBD.gpkg'
    
    # Input - National Water Model stream lines geopackage
    str_nwm_flowline_geopkg_path = STR_NATIONAL_DATASET_PATH + '\\' + 'nwm_flows.gpkg'
    
    # Input - Recurrance Intervals netCDF
    str_netcdf_path = STR_NATIONAL_DATASET_PATH + '\\' + 'nwm_v20_recurrence_flows.nc'
    
    # Geospatial projections
    wgs = "epsg:4326"
    lambert = "epsg:3857"
    nwm_prj = "ESRI:102039"
    # ~~~~~~~~~~~~~~~~~~~~~~~~

    # ````````````````````````
    # option to turn off the SettingWithCopyWarning
    pd.set_option('mode.chained_assignment', None)
    # ````````````````````````
    
    # Load the geopackage into geodataframe - 1 minute +/-
    print("+-----------------------------------------------------------------+")
    print('Loading Watershed Boundary Dataset ~ 60 sec')
    gdf_ndgplusv21_wbd = gpd.read_file(str_wbd_geopkg_path)
    
    list_huc8 = []
    list_huc8.append(STR_HUC8)
    
    # get only the polygons in the given HUC_8
    gdf_huc8_only = gdf_ndgplusv21_wbd.query("HUC_8==@list_huc8")
    gdf_huc8_only_nwm_prj = gdf_huc8_only.to_crs(nwm_prj)
    gdf_huc8_only_ble_prj = gdf_huc8_only.to_crs(BLE_PRJ)
    
    # path of the shapefile to write
    str_huc8_filepath = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_huc_12_ar.shp"
    
    
    # Overlay the BLE streams (from the HEC-RAS models) to the HUC_12 shapefile
    
    # read the ble streams
    gdf_ble_streams = gpd.read_file(STR_BLE_STREAM_LN)
    
    # clip the BLE streams to the watersheds (HUC-12)
    gdf_ble_streams_intersect = gpd.overlay(
        gdf_ble_streams, gdf_huc8_only_ble_prj, how='intersection')
    
    # path of the shapefile to write
    str_filepath_ble_stream = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_ble_streams_ln.shp"
    
    # write the shapefile
    gdf_ble_streams_intersect.to_file(str_filepath_ble_stream)
    
    # ---- Area shapefile of just the HUC_12s that have streams
    # create an array of HUC_12 watersheds that have streams within them
    arr_huc_12_only_with_stream = gdf_ble_streams_intersect.HUC_12.unique()
    
    # convert the array to a pandas dataframe
    df_huc_12_only_with_stream = pd.DataFrame(arr_huc_12_only_with_stream, columns = ['HUC_12'])
    
    # merge the dataframe and geodataframe to get only polygons that have streams
    gdf_huc_12_only_with_stream = pd.merge(gdf_huc8_only_ble_prj,
                                           df_huc_12_only_with_stream,
                                           on='HUC_12', how='inner')
    
    # write the area watershed shapefile
    gdf_huc_12_only_with_stream.to_file(str_huc8_filepath)
    # ----
    
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
    print("+-----------------------------------------------------------------+")
    print('Loading the National Water Model Recurrence Flows ~ 15 sec')
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
    
    # TODO - 2021.08.03 - Quicker to buffer the ble_streams first
    # and get the nwm streams that are inside or touch the buffer?
    
    # Get the total number of points requesting
    total_points = 0
    
    for index, row in gdf_streams_nwm_explode.iterrows():
        str_current_linestring = row['geometry']
        distances = np.arange(0, str_current_linestring.length, INT_DISTANCE_DELTA)
        points = [str_current_linestring.interpolate(distance) for distance in distances] + [str_current_linestring.boundary[1]]
        total_points += len(points)
    
    print("+-----------------------------------------------------------------+")
    fn_print_progress_bar(0, total_points,
                  prefix = 'Creating Points on NWM Streams' ,
                  suffix = 'Complete', length = 18)

    for index, row in gdf_streams_nwm_explode.iterrows():
        str_current_linestring = row['geometry']
        distances = np.arange(0, str_current_linestring.length, INT_DISTANCE_DELTA)
        points = [str_current_linestring.interpolate(distance) for distance in distances] + [str_current_linestring.boundary[1]]
    
        for i in points:
            int_count += 1
            df_points_nwm = df_points_nwm.append({'geometry': i,
                                                  'feature_id': row['feature_id'],
                                                  'huc_12': row['huc12']},
                                                 ignore_index=True)
    
            fn_print_progress_bar(int_count, total_points,
                                  prefix = 'Creating Points on NWM Streams' ,
                                  suffix = 'Complete', length = 18)
            
    # convert dataframe to geodataframe
    gdf_points_nwm = gpd.GeoDataFrame(df_points_nwm, geometry='geometry')
    
    # Set the crs of the new geodataframe
    gdf_points_nwm.crs = gdf_streams_nwm_bleprj.crs
    
    # path of the shapefile to write
    str_filepath_nwm_points = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_nwm_points_PT.shp"
    
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
    
    print("+-----------------------------------------------------------------+")
    print("Buffering stream centerlines ~ 60 sec")
    # buffer the merged stream ceterlines - distance to find valid conflation point
    buff = shply_line.buffer(INT_BUFFER_DIST)
    
    # convert shapely to geoDataFrame
    buff = gpd.GeoDataFrame(geometry=[buff])
    
    # set the CRS of buff
    buff = buff.set_crs(BLE_PRJ)
    
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

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Snap the points along the National Water Model
    # streams (within the buffer) to the nearest modeled
    # HEC-RAS stream
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # TODO - can this be run in parallel for speed? 2021.04.02
    # Estimated Time - One min for every 800 points
    
    int_count = 0
    total_points = len(gdf_points_within_buffer)

    print("+-----------------------------------------------------------------+")
    fn_print_progress_bar(int_count, total_points,
                  prefix = 'Snapping Points:' ,
                  suffix = 'Complete', length = 24)
    
    for index, row in gdf_points_within_buffer.iterrows():
        int_count += 1
    
        fn_print_progress_bar(int_count, total_points,
                              prefix = 'Snapping Points:' ,
                              suffix = 'Complete', length = 24)
    
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
    str_filepath_ble_points = STR_OUT_PATH + "\\" + str(list_huc8[0]) + "ble_snap_points_PT.shp"
    
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
    
    str_csv_file = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_interim_list_of_streams.csv"
    
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
    str_filepath_nwm_stream = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_nwm_streams_ln.shp"
    
    # write the shapefile
    gdf_nwm_stream_raspath.to_file(str_filepath_nwm_stream)
    
    # Read in the ble cross section shapefile
    gdf_ble_cross_sections = gpd.read_file(STR_BLE_CROSS_SECTION_LN)
    
    
    str_xs_on_feature_id_pt = STR_OUT_PATH + '\\' + str(list_huc8[0]) + "_nwm_points_on_xs_PT.shp"
    
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
    
    print("+-----------------------------------------------------------------+")
    print("Determining conflated stream centerlines")
    
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
                            schema, crs=from_epsg(BLE_PRJ[5:])) as output:
                # ~~~~~~~~~~~~~~~~~~~~
                # Slice BLE_PRJ to remove the "epsg:"
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
    
    # Creates a summary documents
    # Check to see if matching model is found
    
    print("+-----------------------------------------------------------------+")
    print("Creating Quality Control Output")
    
    str_str_qc_csv_File = STR_OUT_PATH + "\\" + STR_HUC8 + "_stream_qc.csv"
    df_summary_data.to_csv(str_str_qc_csv_File)
    
    gdf_nwm_stream_lines = gpd.read_file(str_filepath_nwm_stream)
    
    # load the stream QC lines
    df_processed_lines = pd.read_csv(str_str_qc_csv_File)
    
    gdf_non_match = pd.merge(
        gdf_nwm_stream_lines,
        df_processed_lines,
        how='outer',
        indicator=True,
        on='feature_id')
    
    gdf_non_match = gdf_non_match[(gdf_non_match._merge != 'both')]
    
    # path of the shapefile to write
    str_filepath_nwm_stream = STR_OUT_PATH + '\\' + STR_HUC8 + "_no_match_nwm_lines.shp"
    
    # delete the wkt_geom field
    del gdf_non_match['_merge']
    
    # write the shapefile
    gdf_non_match.to_file(str_filepath_nwm_stream)
    
    print()
    print('COMPLETE')
    print("+=================================================================+")