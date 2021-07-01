#!/usr/bin/env python
# coding: utf-8
#
# Purpose:
# From a user supplied area shapefile, get the Entwine 3DEP footprints from
# the hobu git-hub repo (geojson).  Using an Entwine pipeline, get the
# bare earth DEM from the requested area.
#
# Output generated:
# DEM of the requested area (first found entwine source only)
# for the first polygon in the user supplied area polygon.
#
# Created by: Andy Carter, PE
# Last revised - 2021.05.21

import geopandas as gpd

import pdal
import json

import rasterio
from rasterio.fill import fillnodata

import rioxarray as rxr

import time

import os

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Input

# path to input shapefile
STR_LOAD_PATH = r'C:\Test_Bridge_20210423\OutPath\\'
STR_SHP_FILENAME = 'hec_ras_boundary_AR.shp'

# search distance when closing the gaps of the returned DEM
FLT_SEARCH_DIST = 25

# set to True if the source CRS is in feet
B_CONVERT_TO_VERT_FT = True

STR_OUTPUT_DIR = r'C:\Test_Terrain\\'
STR_SAVE_DEM_NAME = 'created_terrain'
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

str_boundary = STR_LOAD_PATH + STR_SHP_FILENAME

wgs = "epsg:4326"
lambert = "epsg:3857"

# load the subject request shapefile
gdf_boundary_prj = gpd.read_file(str_boundary)

str_crs_model = str(gdf_boundary_prj.crs)

# convert the ras boundary to lambert projection
gdf_boundary_lambert = gdf_boundary_prj.to_crs(lambert)

# Get EPT limits from github repository
str_hobu_footprints = r'https://raw.githubusercontent.com/hobu/usgs-lidar/master/boundaries/boundaries.topojson'
gdf_entwine_footprints = gpd.read_file(str_hobu_footprints)

# Set the entwine footprint CRS
gdf_entwine_footprints = gdf_entwine_footprints.set_crs(wgs)

# Convert the footprints to lambert
gdf_entwine_footprints = gdf_entwine_footprints.to_crs(lambert)

# clip the footprints to the model limits boundary
gdf_entwine_footprints = gpd.overlay(gdf_entwine_footprints,
                                     gdf_boundary_lambert,
                                     how='intersection')
# the first geometry from the requested polygon as wellKnownText
boundary_geom_WKT = gdf_boundary_lambert['geometry'][0]  # to WellKnownText

# the bounding box of the requested lambert polygon
b = boundary_geom_WKT.bounds

# the first found ept in the requested area
ept_source = gdf_entwine_footprints['url'][0]


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# Entwine Pipeline

# note: A square mile area takes about 3+ minutes
str_dem_ground = STR_OUTPUT_DIR + STR_SAVE_DEM_NAME + '_1.tif'

pipeline_dem_ground = {
    "pipeline": [
        {
            "type":"readers.ept",
            'bounds':str(([b[0], b[2]],[b[1], b[3]])),
            "filename":ept_source,
            "threads":10,
            "tag":"readdata"
        },
        {   
            "type":"filters.crop",
            'polygon':boundary_geom_WKT.wkt
        },
        {   
            "type":"filters.range",
            "limits": "Classification[2:17]"
        },
        {   
            "type":"filters.range",
            "limits": "Classification![3:8]"
        },
        {   
            "type":"filters.range",
            "limits": "Classification![17:17]",
            "tag":"GroundOnly"
        },
        {
            "filename": str_dem_ground,
            "gdalopts": "tiled=yes,     compress=deflate",
            "inputs": [ "GroundOnly" ],
            "nodata": -9999,
            "output_type": "idw",
            "resolution": 0.6,
            "type": "writers.gdal"
        }
    ]}

# execute the pdal pipeline

pipeline = pdal.Pipeline(json.dumps(pipeline_dem_ground))
pipeline.validate()
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

str_dem_ground_filled = STR_OUTPUT_DIR + STR_SAVE_DEM_NAME + '_2.tif'

# --------------------------------------------------------
# Fill the gaps of the Entwine DEM
# Fill NoData cells via rasterio "fillnotdata" method
with rasterio.open(str_dem_ground) as src:
    profile = src.profile
    arr = src.read(1)
    arr_filled = fillnodata(arr, mask=src.read_masks(1),
                            max_search_distance=FLT_SEARCH_DIST,
                            smoothing_iterations=0)

with rasterio.open(str_dem_ground_filled, 'w', **profile) as dest:
    dest.write_band(1, arr_filled)

src.close
os.remove(str_dem_ground)

dest.close
# --------------------------------------------------------

# ************************************************
# Using RioXarry - translate the DEM back to the requested shapefile's proj

src = str_dem_ground_filled

# read the DEM as a "Rioxarray"
lidar_dem = rxr.open_rasterio(src, masked=True).squeeze()

# reproject the raster to the same projection as the road
lidar_dem_local_proj = lidar_dem.rio.reproject(str_crs_model)

if B_CONVERT_TO_VERT_FT:
    # scale the raster from meters to feet
    lidar_dem_local_proj = lidar_dem_local_proj * 3.28084

# write out the raster
str_dem_out = STR_OUTPUT_DIR + STR_SAVE_DEM_NAME + '.tif'
lidar_dem_local_proj.rio.to_raster(str_dem_out,
                                   compress='LZW',
                                   dtype="float32")

# ************************************************

lidar_dem.close()
lidar_dem_local_proj.close()

time.sleep(1)
os.remove(str_dem_ground_filled)