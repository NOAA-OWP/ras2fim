#!/usr/bin/env python
# coding: utf-8
#
# Purpose:
# From a user supplied area shapefile (multiple polygons),
# get the terrain data from the AWS registry of Open Data
# https://registry.opendata.aws/terrain-tiles/
#
# Output generated:
# DEM of the for each polygon.  Will return in the polygon's
# cooredinate reference system
#
# Created by: Andy Carter, PE
# Last revised - 2021.06.22

import geopandas as gpd
import requests
import json
import time

from shapely.geometry import Polygon

import rasterio
from rasterio.merge import merge

from multiprocessing.pool import ThreadPool  
#threadpool to multi-thread the downloading of the multiple files (for speed)

import rioxarray as rxr
import math
import os

# path to input shapefile
STR_LOAD_PATH = r'E:\X_RAS2D\SHP\\'
STR_SHP_FILENAME = 'AOI_lidar_AR.shp'
STR_SHP_FILENAME = 'WBD_HUC12_AR_2277.shp'

# number to downloading threads
INT_THREADS = 24

# set to True to convert vertical data from meters to feet
B_CONVERT_TO_VERT_FT = True

# distance to buffer the requested polygon(s)
FLT_BUFFER_DIST = 500

STR_OUTPUT_DIR = r'E:\X_RAS2D\DEM\AWS_Scrape_10\\'

str_boundary = STR_LOAD_PATH + STR_SHP_FILENAME

wgs = "epsg:4326"
int_zoom = 14 #the most detailed AWS Terrain level available

# load the subject request shapefile
gdf_boundary_prj = gpd.read_file(str_boundary)
str_crs_boundary = str(gdf_boundary_prj.crs) # projection of input area shapefile

# buffer the shapefile
gdf_boundary_prj.geometry = gdf_boundary_prj.geometry.buffer(FLT_BUFFER_DIST)

gdf_boundary_wgs = gdf_boundary_prj.to_crs(wgs)

# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
def fn_deg2num(lat_deg, lon_deg, zoom):
    
    """Return the tile number (x,y) given the Lat/Long and zoom level

    Args:
        lat_deg: Point latitude in decimal degrees
        lon_deg: Point longitude in decimal degrees
        zoom: Tile pyramid zoom-in level 

    Returns:
        Integers of the x/y of the tile
    """
    
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)

# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

# ---------------------------------------------------
def fn_get_features(gdf,int_poly_index):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    return [json.loads(gdf.to_json())['features'][int_poly_index]['geometry']]
# ---------------------------------------------------

# ***************************************************
def fn_download_file(str_file_url_and_path):
    str_url_fn = str(str_file_url_and_path).split('|')[0]
    str_path_fn = str(str_file_url_and_path).split('|')[1]
    
    r = requests.get(str_url_fn)
    
    with open(str_path_fn, 'wb') as f:
        f.write(r.content)
# ***************************************************

# Create an empty geopandas GeoDataFrame
gdf_aws_terrain_tile = gpd.GeoDataFrame()

gdf_aws_terrain_tile['geometry'] = None
gdf_aws_terrain_tile['tile_x'] = None
gdf_aws_terrain_tile['tile_y'] = None
gdf_aws_terrain_tile['zoom_level'] = None
gdf_aws_terrain_tile['poly_id'] = None
gdf_aws_terrain_tile['poly_path'] = None

# set projection
gdf_aws_terrain_tile.crs = wgs

int_tile_count = 0

for index, row in gdf_boundary_prj.iterrows():
    # the index of the polygon on the input layer
    int_gdf_item = index
    
    # get the bounding box for each polygon in geodataframe
    flt_min_long, flt_min_lat, flt_max_long, flt_max_lat =  gdf_boundary_wgs['geometry'][int_gdf_item].bounds
    
    #indecies of tiles from within the bounding box
    int_x_min_tile = fn_deg2num(flt_min_lat,flt_min_long,int_zoom)[0]
    int_x_max_tile = fn_deg2num(flt_max_lat,flt_max_long,int_zoom)[0]

    int_y_min_tile = fn_deg2num(flt_min_lat,flt_min_long,int_zoom)[1]
    int_y_max_tile = fn_deg2num(flt_max_lat,flt_max_long,int_zoom)[1]
    
    list_terrain_paths = []
    
    for x in range(int_x_min_tile, int_x_max_tile + 1):
        for y in range (int_y_max_tile, int_y_min_tile + 1):
            
            # get upper left and lower right coordinates of the tile
            list_tile_coords = [fn_tile2latlon(x, y, int_zoom)[0],
                               fn_tile2latlon(x, y, int_zoom)[1],
                               fn_tile2latlon(x+1, y+1, int_zoom)[0],
                               fn_tile2latlon(x+1, y+1, int_zoom)[1]]
            
            # list of the x coordinates of the tile
            list_x_points = [list_tile_coords[1],
                            list_tile_coords[1],
                            list_tile_coords[3],
                            list_tile_coords[3]]
            
            # list of the y coordinates of the tile
            list_y_points = [list_tile_coords[0],
                            list_tile_coords[2],
                            list_tile_coords[2],
                            list_tile_coords[0]]
            
            # convert the coords to shapeley geometry
            polygon_geom = Polygon(zip(list_x_points, list_y_points))
            
            gdf_aws_terrain_tile.loc[int_tile_count, "geometry"] = polygon_geom
            gdf_aws_terrain_tile.loc[int_tile_count, "tile_x"] = x
            gdf_aws_terrain_tile.loc[int_tile_count, "tile_y"] = y
            gdf_aws_terrain_tile.loc[int_tile_count, "zoom_level"] = int_zoom
            gdf_aws_terrain_tile.loc[int_tile_count, "poly_id"] = int_gdf_item
            gdf_aws_terrain_tile.loc[int_tile_count, "poly_path"] = str_boundary
            
            int_tile_count += 1

            
list_merge_path_to_delete = []

for item_poly in range(int_gdf_item + 1):
    # select all the tiles in gdf_aws_terrain_tile where "poly_id" == i
    gdf_current_poly_tiles = gdf_aws_terrain_tile.loc[gdf_aws_terrain_tile['poly_id'] == item_poly]
    
    # select the current requested polygon
    gdf_current_poly_request = gdf_boundary_wgs.iloc[[item_poly]] # double brackets to return GeoDataFrame
    
    # intersect the two geoDataFrames
    # clip the requested limits to the tiles 
    gdf_clipped_tiles = gpd.overlay(gdf_current_poly_tiles,
                                    gdf_current_poly_request,
                                    how='intersection')
    
    #Create URL list of all the needed tiles
    list_terrain_paths = []
    
    for index, row in gdf_clipped_tiles.iterrows():
        int_tile_x = gdf_clipped_tiles["tile_x"][index]
        int_tile_y = gdf_clipped_tiles["tile_y"][index]
        int_zoom_level = gdf_clipped_tiles["zoom_level"][index]
        
        str_url = r'https://s3.amazonaws.com/elevation-tiles-prod/geotiff/'
        str_url += str(int_zoom_level) + "/"
        str_url += str(int_tile_x) + "/"
        str_url += str(int_tile_y) + ".tif"
        list_terrain_paths.append(str_url)
        
    int_count = 1
    list_filenames_to_merge = []
    list_test_multithread = [] # used for multithreading download
    
    for i in list_terrain_paths:
        str_file_name = "AWS_" + str(int_gdf_item) + '_TileImage_' + str(int_count) + '.tif'
        str_total_path = STR_OUTPUT_DIR + str_file_name
        list_filenames_to_merge.append(str_total_path)
        
        # parsing two args in list with pipe "|" delineator
        list_test_multithread.append(str(i) + "|" + str_total_path)
        int_count += 1

    # ~~~~~~~~~~~~~~~~~~~~~~
    results = ThreadPool(INT_THREADS).map(fn_download_file, list_test_multithread)
    # 2021.06.22 - changed to "map" from "imap_unordered" to ensure that all
    # tiles are downloaded prior to merge
    # ~~~~~~~~~~~~~~~~~~~~~~
    
    # Merge the DEMs in the list_filenames_to_merge
    str_out_tiff_path = STR_OUTPUT_DIR + "AWS_" + str(item_poly) + "_dem_merge.tif"

    d = []
    for file in list_filenames_to_merge:
        src = rasterio.open(file)
        d.append(src)

    out_meta = src.meta.copy()
    mosaic, out_trans = merge(d)
    src.close()

    # Create Metadata of the for the mosaic TIFF
    out_meta.update({"driver": "HFA","height":mosaic.shape[1],"width":mosaic.shape[2],"transform": out_trans,})

    # Write the updated DEM to the specified file path
    with rasterio.open(str_out_tiff_path, "w", **out_meta) as dest:
        dest.write(mosaic)
    
    # remove the downloaded tiles
    int_count = 0
    for item in d:
        d[int_count].close()
        int_count += 1

    for filename in list_filenames_to_merge:
        if os.path.exists(filename):
            os.remove(filename)
            
    src_path = str_out_tiff_path
    
    # ************************************************
    # Using RioXarray - translate the DEM back to the requested shapefile's proj
    
    # read the DEM as a "Rioxarray"
    with rxr.open_rasterio(src_path, masked=True).squeeze() as aws_dem:
        aws_dem_local_proj = aws_dem.rio.reproject(str_crs_boundary)

    if B_CONVERT_TO_VERT_FT:
        # scale the raster from meters to feet
        aws_dem_local_proj = aws_dem_local_proj * 3.28084

    # clip the raster
    geom_coords = fn_get_features(gdf_boundary_prj, item_poly)
    aws_dem_local_proj_clip = aws_dem_local_proj.rio.clip(geom_coords)

    # write out the raster
    str_dem_out = STR_OUTPUT_DIR + "AWS_" + str(item_poly) + "_dem_clip_project.tif"
    aws_dem_local_proj_clip.rio.to_raster(str_dem_out, compress='LZW', dtype="float32")

    list_merge_path_to_delete.append(str_out_tiff_path)
    # ************************************************
    print('Building Terrain: ' + str(item_poly + 1) + ' of ' + str(int_gdf_item + 1))
    
print('Complete')
        
# TODO - 2021.06.10 - Error - Files will not delete
for file_item in list_merge_path_to_delete:
    if os.path.exists(file_item):
        os.remove(file_item)     
        
# Export the shapefile of the tiles.
#str_tile_out = STR_OUTPUT_DIR + "aws_terrain_tile_AR.shp"
#gdf_aws_terrain_tile.to_file(str_tile_out)
