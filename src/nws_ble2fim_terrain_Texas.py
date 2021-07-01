#!/usr/bin/env python
# coding: utf-8

# Texas Terrain (HUC-12)
#
# Purpose:
# Terrain in Texas is served throught multiple data sources that are
# available through the TNTIS Datahub system.  https://data.tnris.org/
# This script uses a tile index provided from TNRIS to determine what tiles
# are necessary to build a surface model of the needed area.
#
# Output generated:
# Ultimately, a GeoTIFF DEM terrain for each HUC-12
# watershed in the requested inputed shapefile
#
# Created by: Andy Carter, PE
# Last revised - 2021.02.15
#
# Unique terrain preprocessing pipeline - for Texas 1m

import geopandas as gpd
import rasterio

import osr
import gdal

import requests
import zipfile
import io

import os
import shutil
import glob

import urllib

from rasterio.merge import merge
from rasterio.mask import mask

from multiprocessing.pool import ThreadPool

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT

# Path to area polygon shapefile containing the desired HUC-12
STR_HUC12_SHP_PATH = r"E:\XX_Output\Conflation_Output\12040104_huc_12_ar.shp"

# Integer of the desired HUC-8
INT_HUC8 = 12040104

# Path to place each watershed shapefile
STR_EACH_HUC12 = r"E:\X-HCFCD\Terrain_Output"

# Tile index (polygon shapefile) of the avialble tiles in Texas
STR_LIDAR_AR_PATH = r"C:\NWS\Shapefiles\TNRIS_liDAR_Available_AR_4269.shp"

# Quarter Quad (polygon shapefile) Index for Texas
STR_QQ_AR_PATH = r"C:\Test\StratMap_QQ_Index_AR_4269.shp"

# Input - This is the desired projection once finished
INT_DST_CRS = 2278
dst_crs = 'EPSG:' + str(INT_DST_CRS)

# Input - distance of buffer on clip boundary
FLT_BUFFER_DIST = 200
# ~~~~~~~~~~~~~~~~~~~~~~~~

# Read the shapefiles using geopandas
gdf_huc12 = gpd.read_file(STR_HUC12_SHP_PATH)

# Add the HUC8 field as the first 8 chars of the HUC12
gdf_huc12['huc8'] = gdf_huc12['HUC_12'].str[:8]

# Create a new dataframe that is just the HUC12 in the HUC8
df_in_huc8 = gdf_huc12[gdf_huc12.huc8.eq(str(INT_HUC8))]

# Read the shapefiles using geopandas
gdf_qq = gpd.read_file(STR_QQ_AR_PATH)

gdf_lidar_tiles = gpd.read_file(STR_LIDAR_AR_PATH)


# ++++++++++++++++++++++++++
# Function - Get the TNRIS GUID for each dataset
def fn_get_tnris_guid(argument):
        switcher = {
            "usgs-2018-70cm-lavaca-wharton": "642a83e0-c735-4ba6-a89c-85a723a1ad94",
            "usgs-2017-70cm-brazos-freestone-robertson": "b6ea8e3a-c8b7-4d97-b4d1-4eb8172eb87d",
            "fema-2006-140cm-coastal": "8ea19b45-7a66-4e95-9833-f9e89611d106",
            "fema-2011-61cm-comal-guadalupe": "3fbac9d5-d6ec-4201-8261-22d3d0c2684d",
            "ibwc-2006-70cm-cameron": "41bf20d7-7741-4347-ade3-915cee799e88",
            "ibwc-2011-70cm-rio-grande": "27f30e8a-115a-4ad5-ace1-5e2aa4a53a70",
            "usgs-2011-150cm-calhoun-hidalgo-nueces": "6a825941-a80b-4a61-a2b2-1da205f2f28b",
            "fema-2013-60cm-middle-brazos-palo-pinto": "2032756b-ee41-4782-bbca-59b237fe3d9e",
            "cityofgeorgetown-2015-50cm": "04f42cd7-bd81-42d2-bf9e-eca1d04e1936",
            "capcog-2008-140cm-bastrop-fayette-hays": "1f1d417a-26f2-41d5-9787-af392d0f09cf",
            "lcra-2007-140cm": "ab743202-206c-4c37-99b1-9a46db93bd4c",
            "fema-2015-70cm-middle-brazos": "2645daf3-beb7-4137-98cf-c28b97008d89",
            "hgac-2008-1m": "4e306d96-e77f-4794-b818-eba5c85140de",
            "fema-2014-1m-rio-grande": "6405ede8-8b03-411f-9544-cd6b3f6fe7fc",
            "fema-2014-70cm-upper-clear-fork": "34e6ffc2-8854-4047-9c3b-498bf10d032c",
            "fema-2016-70cm-dewitt": "4bd279fc-e358-4cdb-abc0-4e34516b21d3",
            "stratmap-2009-1m-zapata": "73fcedd4-2cfb-46af-83f0-302fae56edaa",
            "stratmap-2009-1m-mcmullen": "73fcedd4-2cfb-46af-83f0-302fae56edaa",
            "stratmap-2009-1m-goliad": "73fcedd4-2cfb-46af-83f0-302fae56edaa",
            "stratmap-2009-1m-dallas": "9aba8b42-4a55-4687-95aa-5a7cf75c970a",
            "stratmap-2009-50cm-tarrant": "7bc5f57d-f679-454f-8fd3-bdffc8d54272",
            "stratmap-2010-1m-lee-leon-madison-milam": "812c6179-28d7-4829-b99e-ae15d4a7cf5b",
            "stratmap-2010-50cm-bexar": "b133455e-0196-4e5d-b69b-a962847afce3",
            "stratmap-2010-50cm-cooke-grayson-montague-wise": "6bb27abd-b30c-4bd0-b429-a570fd999e15",
            "stratmap-2012-50cm-tceq-dam-sites": "fda08eb6-8daa-4daf-bfec-0fb0891cb732",
            "stratmap-2013-50cm-karnes-wilson": "6b475ef7-51e7-4e15-b652-4e942eafa85f",
            "stratmap-2013-50cm-ellis-henderson-hill-johnson-navarro": "6b475ef7-51e7-4e15-b652-4e942eafa85f",
            "stratmap-2014-50cm-fort-bend": "21130f88-dac2-4c36-9d9b-1fd9dc07750e",
            "stratmap-2014-50cm-bandera": "9dabb50e-00a1-46e5-8966-8a54ac424de6",
            "stratmap-2014-50cm-lampasas": "9dabb50e-00a1-46e5-8966-8a54ac424de6",
            "stratmap-2014-50cm-henderson-smith-van-zandt-trinity-river": "18c52542-dc6f-4a12-88b3-59ecc8e6d53d",
            "stratmap-2015-50cm-brazos": "48ce46cb-7aba-406c-8330-f6c5b43389c1",
            "stratmap-2017-50cm-east-texas": "f09f36b9-12b1-4c88-bccb-017afa9bc3d8",
            "stratmap-2011-1m-sabine-shelby-newton": "da097bc5-240e-400e-944f-0e403fbd781f",
            "stratmap-2011-50cm-austin-grimes-walker": "d69e74b0-9c20-4a7d-9b3d-f251eb318bfe",
            "stratmap-2011-50cm-caldwell-gonzales": "760f9068-426e-4c30-9829-177259d1da0f",
            "stratmap-2011-50cm-bell-burnet-mclennan": "02a1485c-2c6e-4f4c-9f52-17c1d59dab59",
            "stratmap-2011-50cm-collin-denton-kaufman": "d9ce5448-30a5-46cf-a246-55ba7c8304f0",
            "stratmap-2011-50cm-blanco-kendall-kerr": "760f9068-426e-4c30-9829-177259d1da0f",
            "stratmap-2017-50cm-central-texas": "0549d3ba-3f72-4710-b26c-28c65df9c70d",
            "stratmap-2017-35cm-chambers-liberty": "12342f12-2d74-44c4-9f00-a5c12ac2659c",
            "stratmap-2017-50cm-jefferson": "12342f12-2d74-44c4-9f00-a5c12ac2659c",
            "stratmap-2018-50cm-crockett": "a8ef3bfc-1e26-4fba-9abe-1b86ecd594e2",
            "usgs-2014-70cm-archer-jack": "cd8246cf-99fa-4504-b178-8c6ddecc1c02",
            "usgs-2016-70cm-middle-brazos-lake-whitney": "5a227e2c-fe2f-4e43-a981-a462c37eb488",
            "usgs-2016-70cm-neches-river-basin": "67af6a3e-2c73-446d-a5b7-243b4b6ae4ec",
            "usgs-2017-70cm-amistad-nra": "bb8bfbba-3f17-46f7-8c6b-f2785e01dd19",
            "stratmap-2018-50cm-upper-coast": "b5bd2b96-8ba5-4dc6-ba88-d88133eb6643",
            "usgs-2018-70cm-south-central": "77f928dc-298b-4b2e-9efd-8e2e16ece2c0",
            "usgs-2016-70cm-brazos-basin": "d55b62b3-fd81-4e15-a034-6fda56cde7de",
            "usgs-2017-70cm-red-river": "ff2f83a7-37e7-46a8-ba6c-d3208a51c778",
            "stratmap-2019-50cm-brown-county": "46b000dd-83b2-4701-b5c6-3f6e18cedccf",
            "stratmap-2019-50cm-missouri-city": "66d886a6-13b9-46a9-936a-bf2ce4f71aaf",
            "usgs-2018-70cm-eastern": "13563a34-6a6d-4171-ad34-fbdfb26165ae",
            "usgs-2018-70cm-south-texas": "6131ecdd-aa26-433e-9a24-97ac1afda7de",
            "usgs-2008-120cm-kenedy-kleberg": "cafa0c1b-5586-49dc-8f6a-cf1fab93362a",
            "usgs-2018-70cm-texas-panhandle": "0d805408-d173-45d1-a820-5e0d9453e972",
            "usgs-2018-70cm-texas-west-central": "de1b77e1-3110-41e7-a6a5-81cbbdda5ec3",
            "usgs-2018-70cm-lower-colorado-san-bernard": "b246f8f7-9c79-4c89-91f7-9c7f44955fca",
            "usgs-2018-50cm-matagorda-bay": "8774ed51-b633-4f03-85ca-94c311ee0a88",
        }
        return switcher.get(argument, "Nothing_Found")
# ++++++++++++++++++++++++++


# ~~~~~~~~~~~~~~~~~~~~~~~~~
# Function - Get the Tile Header for each dataset
# Note that the Tile header is not always equal to the dirname - UGH!
def fn_get_tnris_tile_header(argument):
        switcher = {
            "usgs-2018-70cm-lavaca-wharton": "usgs18-70cm-lavaca-wharton",
            "usgs-2017-70cm-brazos-freestone-robertson": "usgs17-70cm-brazos-freestone-robertson",
            "fema-2006-140cm-coastal": "fema06-140cm-coastal",
            "fema-2011-61cm-comal-guadalupe": "fema11-61cm-comal-guadalupe",
            "ibwc-2006-70cm-cameron": "ibwc06-70cm-cameron",
            "ibwc-2011-70cm-rio-grande": "ibwc11-70cm-rio-grande",
            "usgs-2011-150cm-calhoun-hidalgo-nueces": "usgs11-150cm-calhoun-hidalgo-nueces",
            "fema-2013-60cm-middle-brazos-palo-pinto": "fema13-60cm-middle-brazos-palo-pinto",
            "cityofgeorgetown-2015-50cm": "city-of-georgetown15-50cm",
            "capcog-2008-140cm-bastrop-fayette-hays": "capcog08-140cm-bastrop-fayette-hays",
            "lcra-2007-140cm": "lcra07-140cm",
            "fema-2015-70cm-middle-brazos": "fema15-70cm-middle-brazos",
            "hgac-2008-1m": "hgac08-1m",
            "fema-2014-1m-rio-grande": "fema14-1m-rio-grande",
            "fema-2014-70cm-upper-clear-fork": "fema14-70cm-upper-clear-fork",
            "fema-2016-70cm-dewitt": "fema16-70cm-dewitt",
            "stratmap-2009-1m-zapata": "stratmap09-1m-zapata",
            "stratmap-2009-1m-mcmullen": "stratmap09-1m-mcmullen",
            "stratmap-2009-1m-goliad": "stratmap09-1m-goliad",
            "stratmap-2009-1m-dallas": "stratmap09-1m-dallas",
            "stratmap-2009-50cm-tarrant": "stratmap09-50cm-tarrant",
            "stratmap-2010-1m-lee-leon-madison-milam": "stratmap10-1m-lee-leon-madison-milam",
            "stratmap-2010-50cm-bexar": "stratmap10-50cm-bexar",
            "stratmap-2010-50cm-cooke-grayson-montague-wise": "stratmap10-50cm-cooke-grayson-montague-wise",
            "stratmap-2012-50cm-tceq-dam-sites": "stratmap12-50cm-tceq-dam-sites",
            "stratmap-2013-50cm-karnes-wilson": "stratmap13-50cm-karnes-wilson",
            "stratmap-2013-50cm-ellis-henderson-hill-johnson-navarro": "stratmap13-50cm-ellis-henderson-hill-johnson-navarro",
            "stratmap-2014-50cm-fort-bend": "stratmap14-50cm-fort-bend",
            "stratmap-2014-50cm-bandera": "stratmap14-50cm-bandera",
            "stratmap-2014-50cm-lampasas": "stratmap14-50cm-lampasas",
            "stratmap-2014-50cm-henderson-smith-van-zandt-trinity-river": "stratmap14-50cm-henderson-smith-van-zandt-trinity-river",
            "stratmap-2015-50cm-brazos": "stratmap15-50cm-brazos",
            "stratmap-2017-50cm-east-texas": "stratmap17-50cm-east-texas",
            "stratmap-2011-1m-sabine-shelby-newton": "stratmap11-1m-sabine-shelby-newton",
            "stratmap-2011-50cm-austin-grimes-walker": "stratmap11-50cm-austin-grimes-walker",
            "stratmap-2011-50cm-caldwell-gonzales": "stratmap11-50cm-caldwell-gonzales",
            "stratmap-2011-50cm-bell-burnet-mclennan": "stratmap11-50cm-bell-burnet-mclennan",
            "stratmap-2011-50cm-collin-denton-kaufman": "stratmap11-50cm-collin-denton-kaufman",
            "stratmap-2011-50cm-blanco-kendall-kerr": "stratmap11-50cm-blanco-kendall-kerr",
            "stratmap-2017-50cm-central-texas": "stratmap17-50cm-central-texas",
            "stratmap-2017-35cm-chambers-liberty": "stratmap17-35cm-chambers-liberty",
            "stratmap-2017-50cm-jefferson": "stratmap17-50cm-jefferson",
            "stratmap-2018-50cm-crockett": "stratmap18-50cm-crockett",
            "usgs-2014-70cm-archer-jack": "usgs14-70cm-archer-jack",
            "usgs-2016-70cm-middle-brazos-lake-whitney": "usgs16-70cm-middle-brazos-lake-whitney",
            "usgs-2016-70cm-neches-river-basin": "usgs16-70cm-neches-basin",
            "usgs-2017-70cm-amistad-nra": "usgs17-70cm-amistad-nra",
            "stratmap-2018-50cm-upper-coast": "stratmap18-50cm-upper-coast",
            "usgs-2018-70cm-south-central": "usgs18-70cm-south-central",
            "usgs-2016-70cm-brazos-basin": "usgs16-70cm-brazos-basin",
            "usgs-2017-70cm-red-river": "usgs17-70cm-red-river",
            "stratmap-2019-50cm-brown-county": "stratmap19-50cm-brown-county",
            "stratmap-2019-50cm-missouri-city": "stratmap19-50cm-missouri-city",
            "usgs-2018-70cm-eastern": "usgs18-70cm-eastern",
            "usgs-2018-70cm-south-texas": "usgs18-70cm-south-texas",
            "usgs-2008-120cm-kenedy-kleberg": "usgs08-120cm-kenedy-kleberg",
            "usgs-2018-70cm-texas-panhandle": "usgs18-70cm-panhandle-texas",
            "usgs-2018-70cm-texas-west-central": "usgs18-70cm-tx-west-central",
            "usgs-2018-70cm-lower-colorado-san-bernard": "usgs18-70cm-lower-colorado-san-bernard",
            "usgs-2018-50cm-matagorda-bay": "usgs18-50cm-matagorda-bay",
        }
        return switcher.get(argument, "Nothing_Found")
# ~~~~~~~~~~~~~~~~~~~~~~~~~


# .........................
# Function - Download and unzip as DEM file from TNRIS
def fn_download_tiles(str_tile_url):
    # Download each zip file and extract to the str_dem_download_path
    r = requests.get(str_tile_url)
    check = zipfile.is_zipfile(io.BytesIO(r.content))
    if check:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(str_dem_download_path)
# .........................


# >>>>>>>>>>>>>>>>>>>>>>>>
def fn_get_features(gdf, int_poly_index):
    # Function to parse features from GeoDataFrame in such
    # a manner that rasterio wants them
    import json
    return [json.loads(gdf.to_json())['features'][int_poly_index]['geometry']]
# >>>>>>>>>>>>>>>>>>>>>>>>


for i in range(len(df_in_huc8)):

    print(str(i+1) + ' of ' + str(len(df_in_huc8)))

    list_unique_dataset = []

    str_current_huc = df_in_huc8.iloc[i]['HUC_12']
    print(str_current_huc)

    # get a dataframe of only the current HUC12
    df_currentHUC = df_in_huc8[df_in_huc8.HUC_12.eq(str_current_huc)]

    # create a folder for earch HUC12
    str_path_to_create = STR_EACH_HUC12 + "\\" + str_current_huc
    os.makedirs(str_path_to_create, exist_ok=True)

    # write out a shapefile for the HUC12
    str_shape_file_path = str_path_to_create + "\\" + str_current_huc + '.shp'
    df_currentHUC.to_file(str_shape_file_path)

    df_boundary = gpd.read_file(str_shape_file_path)

    # Re-project the requested data Polygon into same CRS as the dst_crs
    df_boundary = df_boundary.to_crs(epsg=INT_DST_CRS)

    # Buffer the clip boundary - better to do before data request
    df_boundary['geometry'] = df_boundary.geometry.buffer(FLT_BUFFER_DIST,
                                                          resolution=16)

    # Re-project the requested data Polygon into same CRS as the QQ grid
    df_boundary = df_boundary.to_crs(crs=gdf_qq.crs)

    df_clip_qq = gpd.overlay(gdf_qq, df_boundary, how='intersection')
    df_clip_qq.plot(color='green', edgecolor='black', alpha=0.95)

    # Intersection of the TNRIS LiDAR Tiles index with the watershed boundary
    # This takes several seconds
    df_clip_lidar = gpd.overlay(gdf_lidar_tiles,
                                df_boundary,
                                how='intersection')

    # Intersect the QQ and the Lidar tiles
    df_clip_qq_lidar = gpd.overlay(df_clip_lidar,
                                   df_clip_qq,
                                   how='intersection')

    df_clip_qq_lidar.plot(color='orange', edgecolor='black', alpha=0.65)

    list_unique_dataset = (df_clip_qq_lidar.dirname.unique())
    print(list_unique_dataset)

    str_dem_download_path = str_path_to_create + "\\" + "DEM_Download" + "\\"
    os.makedirs(str_path_to_create, exist_ok=True)

    # For each item in the list_unique_dataset
    int_dataset = 0

    # Empty list for valid tiles
    arr_valid_list = []

    # Empty download url list
    arr_master_url_list = []

    while int_dataset < len(list_unique_dataset):
        # ---------------------------
        # Get a geopandas table of the current dataset
        df_new = df_clip_qq_lidar[df_clip_qq_lidar['dirname'].isin([list_unique_dataset[int_dataset]])]

        # Get a list of the unique QQ tiles for this specific dataset
        list_unique_qq = (df_new.stratmap_I.unique())

        # Unique list of DEM tiles for this specific dataset
        list_unique_dem_tiles = (df_new.demname.unique())

        # Append the DEMs for this source to a master list
        arr_valid_list.extend(list_unique_dem_tiles)

        # Create a list of urls of the DEM tiles to
        # download from the TNRIS server
        urls = []

        for i in list_unique_qq:
            # Each source have a different GUID on the AWS
            # Build a URL for the DEMs in this dataset
            str_url_path = (r"https://s3.amazonaws.com/data.tnris.org/")
            str_url_path += fn_get_tnris_guid(list_unique_dataset[int_dataset])
            str_url_path += (r"/resources/")
            str_url_path += fn_get_tnris_tile_header((list_unique_dataset[int_dataset]))
            str_url_path += (r"_") + i + "_dem.zip"

            urls.append(str_url_path)
        # ---------------------------
        # Due to clipping buffers and slivers there are requested tiles
        # that do not exist
        # Remove these tiles from the urls list by making a header request

        for j in urls:
            req = urllib.request.Request(j, method='HEAD')
            try:
                r = urllib.request.urlopen(req)
            except:
                # Remove j from the url list
                urls.remove(j)
                pass
        # -------------------------------
        # add the valid urls to the master list
        arr_master_url_list.extend(urls)
        # -------------------------------
        int_dataset += 1

    # Download the DEM tiles per the constructed urls

    print("Downloading: " + str(len(arr_master_url_list)) + " tiles")
    print(arr_master_url_list)

    # **************************
    # Downloading the DEM tiles from the arr_master_url_list
    i = 0
    results = ThreadPool(10).imap_unordered(fn_download_tiles,
                                            arr_master_url_list)

    for str_requested_tile in results:
        i += 1
        print(i)
    print("Complete")
    # **************************

    # Using list comprehension to add '.img' to each item in the list
    arr_valid_list = [i + '.img' for i in arr_valid_list]

    # Delete all the sub-folders (such as metadata) in this folder
    # added 2021.02.14
    for filename in os.listdir(str_dem_download_path):
        file_path = os.path.join(str_dem_download_path, filename)
        if os.path.isdir(file_path):
            print('Delete Folder: ' + str(file_path))
            shutil.rmtree(file_path)

    # Go through the str_dem_download_path and remove all files that do not
    # match an item in the arr_valid_list.

    for filename in os.listdir(str_dem_download_path):
        if filename not in arr_valid_list:
            file_path = os.path.join(str_dem_download_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    # The USGS files when downloaded and unzipped create two folders
    # titled "Block_metadata" & "Projectwide_metadata"
    # These files, if they exist are removed

    str_block_folder = str_dem_download_path + r"\Block_metadata"
    str_projectwide_folder = str_dem_download_path + r"\Projectwide_metadata"

    if os.path.exists(str_projectwide_folder) and os.path.isdir(str_projectwide_folder):
        shutil.rmtree(str_projectwide_folder)

    if os.path.exists(str_block_folder) and os.path.isdir(str_block_folder):
        shutil.rmtree(str_block_folder)

    # Create the merged DEM from the downloaded files

    # Create a list of all the files to merge
    # https://automating-gis-processes.github.io/CSC18/lessons/L6/raster-mosaic.html

    search_criteria = "*"
    q = os.path.join(str_dem_download_path, search_criteria)
    dem_files = glob.glob(q)

    # Empty list to store opened DEM files
    d = []
    for file in dem_files:
        src = rasterio.open(file)
        d.append(src)

    out_meta = src.meta.copy()

    mosaic, out_trans = merge(d)

    # Create Metadata of the for the mosaic TIFF
    out_meta.update({"driver": "HFA",
                     "height": mosaic.shape[1],
                     "width": mosaic.shape[2],
                     "transform": out_trans, })

    str_out_tiff_path = str_path_to_create + "\\" + "Merge_DEM" + "\\"
    os.makedirs(str_out_tiff_path, exist_ok=True)
    str_out_tiff_path = str_out_tiff_path + str_current_huc + '.tif'

    # Write the updated DEM to the specified file path
    with rasterio.open(str_out_tiff_path, "w", **out_meta) as dest:
        dest.write(mosaic)

    # Read the overall Terrain raster
    src = rasterio.open(str_out_tiff_path)

    # Copy the metadata of the src terrain data
    out_meta = src.meta.copy()

    # Get the projection of the raster
    d = gdal.Open(str_out_tiff_path)
    proj = osr.SpatialReference(wkt=d.GetProjection())
    str_epsg_raster = proj.GetAttrValue('AUTHORITY', 1)

    # re-project the clip polygon into same CRS as terrain raster
    df_boundary = df_boundary.to_crs(epsg=str_epsg_raster)

    # Converts the buffer to a GeoJson version for rasterio
    # currently requests the first polygon in the geometry
    coords = fn_get_features(df_boundary, 0)

    # Clip the raster with Polygon
    out_img, out_transform = mask(dataset=src, shapes=coords, crop=True)

    # Metadata for the clipped image
    # This uses the out_image height and width
    out_meta.update({"driver": "GTiff",
                     "height": out_img.shape[1],
                     "width": out_img.shape[2],
                     "transform": out_transform, })

    str_clip_path = str_path_to_create + "\\" + "Clip_DEM" + "\\"

    os.makedirs(str_clip_path, exist_ok=True)
    str_clip_path = str_clip_path + str_current_huc + '_clip' + '.tif'

    with rasterio.open(str_clip_path, "w", **out_meta) as dest:
        dest.write(out_img)
