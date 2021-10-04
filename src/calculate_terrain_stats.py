# This script is utilized to compute terrain comparison statistics to determine
# if the terrain acquired for mapping should be utilized for the generation of
# ras2fim raster depth grids.  It compares every cross section point of the
# HEC-RAS 1D model to the elevation of that point in the mapping terrain.
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.01
#
# Uses the 'ras2fim' conda environment

import h5py
import numpy as np
import os

from shapely.geometry import LineString

import geopandas as gpd
import pandas as pd

import rasterio

import argparse

# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg
        return open(arg, 'r')  # return an open file handle
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

# '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
def fn_calculate_terrain_stats(str_geom_hdf_path,
                               str_projection_path,
                               str_shp_out_path,
                               str_terrain_path):

    print('Creating Point Shapefile...')
    
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
    gdf_sta_elev_pnts['xs'] = None
    gdf_sta_elev_pnts['station'] = None
    gdf_sta_elev_pnts['ras_elev'] = None
    gdf_sta_elev_pnts['ras_path'] = None
    
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
            gdf_sta_elev_pnts.loc[int_pnt, 'xs'] = str_current_xs
            gdf_sta_elev_pnts.loc[int_pnt, 'station'] = sta.item() # item() to convert from numpy value
            gdf_sta_elev_pnts.loc[int_pnt, 'ras_elev'] = flt_elev.item() # item() to convert from numpy value
            
            int_index += 1
            int_pnt += 1
    
    print('Computing Statistics...')
    
    gdf_sta_elev_pnts['ras_path'] = str_geom_hdf_path
    
    # create two new fields for coordinates
    gdf_sta_elev_pnts['x'] = gdf_sta_elev_pnts.geometry.x
    gdf_sta_elev_pnts['y'] = gdf_sta_elev_pnts.geometry.y
    
    coords =[(x,y) for x,y in zip(gdf_sta_elev_pnts.x, gdf_sta_elev_pnts.y)]
    
    # Sample the raster at every point location and store values in GeoDataFrame
    with rasterio.open(str_terrain_path) as terrain_src:
        gdf_sta_elev_pnts['dem_elev'] = [x[0] for x in terrain_src.sample(coords)]
    
    # delete the 'x' and 'y' fields from gdf
    del gdf_sta_elev_pnts['x']
    del gdf_sta_elev_pnts['y']
    
    # create difference in elevation value
    gdf_sta_elev_pnts['diff_elev'] = gdf_sta_elev_pnts['ras_elev'] - gdf_sta_elev_pnts['dem_elev']
    
    # recast variables to 'float32'
    gdf_sta_elev_pnts['station'] = pd.to_numeric(gdf_sta_elev_pnts['station'], downcast="float")
    gdf_sta_elev_pnts['ras_elev'] = pd.to_numeric(gdf_sta_elev_pnts['ras_elev'], downcast="float")
    gdf_sta_elev_pnts['dem_elev'] = pd.to_numeric(gdf_sta_elev_pnts['dem_elev'], downcast="float")
    gdf_sta_elev_pnts['diff_elev'] = pd.to_numeric(gdf_sta_elev_pnts['diff_elev'], downcast="float")
    
    # calculate statistics
    int_count = len(gdf_sta_elev_pnts)
    flt_max_difference = gdf_sta_elev_pnts['diff_elev'].max()
    flt_min_difference = gdf_sta_elev_pnts['diff_elev'].min()
    flt_mean_difference = gdf_sta_elev_pnts['diff_elev'].mean()
    flt_rmse_difference = ((gdf_sta_elev_pnts.ras_elev - gdf_sta_elev_pnts.dem_elev) ** 2).mean() ** .5
    
    # 68% between the +/- value of the RMSE
    # 95% between twice the RSME value

    print('Number of HEC-RAS points: ' + str(int_count))
    print('Maximum difference: ' + str(f'{flt_max_difference:.3f}'))
    print('Minimum difference: ' + str(f'{flt_min_difference:.3f}'))
    print('Mean difference: ' + str(f'{flt_mean_difference:.3f}'))
    print('RMSE: ' + str(f'{flt_rmse_difference:.3f}'))


    gdf_sta_elev_pnts.to_file(str_shp_out_path)
    
    tup_stats = (int_count, flt_max_difference, flt_min_difference, flt_mean_difference, flt_rmse_difference)
    return (tup_stats)

    print('COMPLETE')
    
# '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    
    parser = argparse.ArgumentParser(description='===== CALCULATE TERRAIN VS HEC-RAS MODEL STATISTICS =====')
    
    parser.add_argument('-i',
                        dest = "str_geom_hdf_path",
                        help=r'REQUIRED: Path the HEC-RAS geometry hdf5 file: Example: C:\hecras_output\7242917.g01.hdf',
                        required=True,
                        metavar='FILE',
                        type=lambda x: fn_is_valid_file(parser, x))
    
    parser.add_argument('-p',
                        dest = "str_projection_path",
                        help=r'REQUIRED: shapefile on HEC-RAS projection: Example: C:\shapes_from_conflation\10170204_huc_12_ar.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: fn_is_valid_file(parser, x))
    
    parser.add_argument('-o',
                        dest = "str_shp_out_path",
                        help=r'REQUIRED: directory where output shp is written: Example: C:\hecras_output',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-t',
                        dest = "str_terrain_path",
                        help=r'REQUIRED: path to the terrain (geotiff): Example: C:\output_folder\terrain_from_usgs\101702040205.tif',
                        required=True,
                        metavar='FILE',
                        type=lambda x: fn_is_valid_file(parser, x))
    
    args = vars(parser.parse_args())
    
    str_geom_hdf_path = args['str_geom_hdf_path']
    str_projection_path = args['str_projection_path']
    str_shp_out_path = args['str_shp_out_path']
    str_terrain_path = args['str_terrain_path']
    
    print(" ")
    print("+=================================================================+")
    print("|         CALCULATE TERRAIN VS HEC-RAS MODEL STATISTICS           |")
    print("|   Created by Andy Carter, PE of the National Water Center       |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) HEC-RAS GEOMETRY HDF: " + str_geom_hdf_path)
    print("  ---(p) SHAPEFILE ON HEC-RAS PROJECTION: " + str_projection_path)
    print("  ---(o) DEM PUTPUT PATH: " + str_shp_out_path)
    print("  ---(t) TERRAIN TO CHECK (GEOTIFF): " + str_terrain_path)
    print("+-----------------------------------------------------------------+")
    
    fn_calculate_terrain_stats(str_geom_hdf_path, str_projection_path, str_shp_out_path, str_terrain_path)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~