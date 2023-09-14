# DEM from USGS WCS Service per polygon shapefile
#
# Purpose:
# Given an area shapefile, sample bare earth terrain data from the
# USGS WCS endpoint for each polygon.  This samples tiles to the requested
# resolution.  Tiles are merged into a single DEM for each polygon.  The
# final DEMs are converted to the requested polygon coordinate reference
# system (CRS).  Vertical values are convereted to feet as default, but
# can be toggled to meters if needed. GeoTiffs are written and can be
# named after a unique field in the input shapefile.
#
# While the USGS may have high resolution terrain (1m), the WCS will
# automatically downsample to larger resolution if requested.
#
# Sample URL to get a single tile:
# https://elevation.nationalmap.gov/arcgis/services/3DEPElevation/ImageServer/WCSServer?
#   SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage&
#   coverage=DEP3Elevation&
#   CRS=EPSG:3857&
#   BBOX=-10911400,3514100,-10908400,3517100&
#   WIDTH=1000&HEIGHT=1000&
#   FORMAT=GeoTiff
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.24
#
# ras2fim - Third pre-processing script (if no terrain provided)
# Uses the 'ras2fim' conda environment


# ************************************************************
import argparse
import datetime
import os
import time
import urllib.request
import warnings
from multiprocessing.pool import ThreadPool

import geopandas as gpd
import pandas as pd
import pyproj
import rasterio
import rioxarray as rxr
from rasterio.merge import merge
from shapely.geometry import Polygon

import shared_functions as sf


# -------------------------------------------------
def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg


# -------------------------------------------------
def fn_download_tiles(list_tile_url):
    """Downloads a requeted URL to a requested file directory

    Keyword arguments:
    list_tile_url -- list of two items
        list_tile_url[0] -- download URL
        list_tile_url[1] -- local directory to store the DEM
    """

    urllib.request.urlretrieve(list_tile_url[0], list_tile_url[1])

    if not os.path.exists(list_tile_url[1]):
        try:
            urllib.request.urlretrieve(list_tile_url[0], list_tile_url[1])
        except urllib.error.HTTPError as exception:
            print(" -- ALERT --")
            print(exception)
            print(" -- ALERT: failed to write out the following DEM tile")
            print(list_tile_url[0])
            print("writing to:" + list_tile_url[1])
            raise SystemExit(0)


# -------------------------------------------------
# Print iterations progress
def fn_print_progress_bar(
    iteration, total, prefix="", suffix="", decimals=0, length=100, fill="█", printEnd="\r"
):
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
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)

    # Print New Line on Complete
    if iteration == total:
        print()


# -------------------------------------------------
def fn_get_usgs_dem_from_shape(
    str_input_path, str_output_dir, int_res, int_buffer, int_tile, model_unit, str_field_name
):
    flt_start_get_usgs_dem = time.time()

    # supress all warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    # input polygon shapefile
    STR_AOI_SHP_PATH = str_input_path

    # output path
    STR_OUTPUT_PATH = str_output_dir

    print(" ")
    print("+=================================================================+")
    print("|                   USGS TERRAIN FROM SHAPEFILE                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT PATH: " + STR_AOI_SHP_PATH)
    print("  ---(o) OUTPUT PATH: " + STR_OUTPUT_PATH)

    # requested return resolution in lambert units (meters)
    INT_RESOLUTION = int_res
    print("  ---[r]   Optional: RESOLUTION: " + str(INT_RESOLUTION) + " meters")

    # buffer of the input polygon in lambert units (meters)
    FLT_BUFFER = int_buffer
    print("  ---[b]   Optional: BUFFER: " + str(FLT_BUFFER) + " meters")

    # requested tile size in lambert units (meters)
    INT_TILE_X = INT_TILE_Y = int_tile
    print("  ---[t]   Optional: TILE SIZE: " + str(INT_TILE_X) + " meters")

    # requested tile size in lambert units (meters)
    STR_FIELD_TO_LABEL = str_field_name
    print("  ---[f]   Optional: FIELD NAME: " + str(STR_FIELD_TO_LABEL))

    print("  --- The Ras Models unit (extracted from given shapefile): " + model_unit)

    print("===================================================================")

    # overlap of the requested tiles in lambert units (meters)
    INT_OVERLAP = 50

    # define the "lambert" espg
    LAMBERT = "epsg:3857"
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    # create output directory
    os.makedirs(STR_OUTPUT_PATH, exist_ok=True)

    # into the folder
    STR_OUTPUT_PATH += "\\"

    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(STR_AOI_SHP_PATH)

    # get crs of the input shapefile
    aoi_prj_crs = pyproj.CRS.from_string(str(gdf_aoi_prj.crs))
    
    # convert the input shapefile to lambert
    gdf_aoi_lambert = gdf_aoi_prj.to_crs(LAMBERT)

    # buffer the polygons in the input shapefile
    gdf_aoi_lambert["geometry"] = gdf_aoi_lambert.geometry.buffer(FLT_BUFFER)

    # determine if the naming field can be used in for the output DEMs
    b_have_valid_label_field = False

    # determine if the requested naming field is in the input shapefile
    if STR_FIELD_TO_LABEL in gdf_aoi_lambert.columns:
        if len(gdf_aoi_lambert) < 2:
            # no need to convert to series if there is just one polygon
            b_have_valid_label_field = True

        else:
            # create a dataframe of just the requested field
            gdf_just_req_field = pd.DataFrame(gdf_aoi_lambert, columns=[STR_FIELD_TO_LABEL])

            # convert the dataframe to a series
            df_just_req_field_series = gdf_just_req_field.squeeze()

            # determine if the naming field is unique
            if df_just_req_field_series.is_unique:
                b_have_valid_label_field = True
            else:
                print("No unique values found.  Naming will be random")

    for index_gdf_int, row_gdf_int in gdf_aoi_lambert.iterrows():
        # the geometry from the requested polygon as wellKnownText
        boundary_geom_WKT = gdf_aoi_lambert["geometry"][index_gdf_int]  # to WellKnownText

        # create geodataframe of just the current row
        gdf_aoi_lambert_current = gpd.GeoDataFrame(gdf_aoi_lambert.iloc[[index_gdf_int]])

        # reset the index
        gdf_aoi_lambert_current = gdf_aoi_lambert_current.reset_index(drop=True)

        # the bounding box of the requested lambert polygon
        b = boundary_geom_WKT.bounds

        # convert the bounding coordinates to integers
        list_int_b = []
        for i in b:
            list_int_b.append(int(i // 1))

        # determine the width and height of the requested polygon
        flt_delta_x = list_int_b[2] - list_int_b[0]
        flt_delta_y = list_int_b[3] - list_int_b[1]

        # determine the number of tiles in the x and y direction
        int_tiles_in_x = (flt_delta_x // (INT_TILE_X - INT_OVERLAP)) + 1
        int_tiles_in_y = (flt_delta_y // (INT_TILE_Y - INT_OVERLAP)) + 1

        list_tile_name = []
        list_geometry = []

        list_point_x = []
        list_point_y = []

        for value_x in range(int_tiles_in_x):
            list_point_x = []
            int_current_start_x = (value_x * (INT_TILE_X - INT_OVERLAP)) + list_int_b[0]
            list_point_x = [
                int_current_start_x,
                int_current_start_x + INT_TILE_X,
                int_current_start_x + INT_TILE_X,
                int_current_start_x,
                int_current_start_x,
            ]

            for value_y in range(int_tiles_in_y):
                list_point_y = []
                int_current_start_y = (value_y * (INT_TILE_Y - INT_OVERLAP)) + list_int_b[1]
                list_point_y = [
                    int_current_start_y,
                    int_current_start_y,
                    int_current_start_y + INT_TILE_Y,
                    int_current_start_y + INT_TILE_Y,
                    int_current_start_y,
                ]

                polygon_geom = Polygon(zip(list_point_x, list_point_y))
                list_geometry.append(polygon_geom)

                str_time_name = str(value_x) + "_" + str(value_y)
                list_tile_name.append(str_time_name)

        # create a pandas dataframe
        df = pd.DataFrame({"tile_name": list_tile_name, "geometry": list_geometry})

        # convert the pandas dataframe to a geopandas dataframe
        gdf_tiles = gpd.GeoDataFrame(df, geometry="geometry")

        # set the tile footprint crs
        gdf_tiles = gdf_tiles.set_crs(LAMBERT)

        # intersect the tiles and the requested polygon
        gdf_intersected_tiles = gpd.overlay(gdf_tiles, gdf_aoi_lambert_current, how="intersection")

        # get a unique list of the intersected tiles
        arr_tiles_intersect = gdf_intersected_tiles["tile_name"].unique()

        # convert the array to a list
        list_tiles_intersect = arr_tiles_intersect.tolist()

        # new geodataframe of the tiles intersected (but not clipped)
        gdf_tiles_intersect_only = gdf_tiles[gdf_tiles["tile_name"].isin(list_tiles_intersect)]

        # reset the remaining tile index
        gdf_tiles_intersect_only = gdf_tiles_intersect_only.reset_index(drop=True)

        # for each tile, create a URL to USGS WCS to get terrain url
        # from the USGS WCS service 'elevation.nationalmap.gov'

        list_str_url = []
        list_str_tile_name = []

        str_URL_header = (
            r"https://elevation.nationalmap.gov/arcgis/services/3DEPElevation/ImageServer/WCSServer?"
        )
        str_URL_query_1 = r'SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage&coverage=DEP3Elevation' \
                          r'&CRS=EPSG:3857&FORMAT=GeoTiff'

        for index, row in gdf_tiles_intersect_only.iterrows():
            list_str_tile_name.append(gdf_tiles_intersect_only["tile_name"][index])

            # the geometry from the requested tile as wellKnownText
            boundary_geom_tile_WKT = gdf_tiles_intersect_only["geometry"][index]  # to WellKnownText

            # the bounding box of the tile
            b_tile = boundary_geom_tile_WKT.bounds

            str_bbox = str(b_tile[0]) + "," + str(b_tile[1]) + "," + str(b_tile[2]) + "," + str(b_tile[3])

            str_URL_query_bbox = "&BBOX=" + str_bbox
            str_URL_query_dim = (
                "&WIDTH=" + str(INT_TILE_X / INT_RESOLUTION) + "&HEIGHT=" + str(INT_TILE_Y / INT_RESOLUTION)
            )

            str_url = str_URL_header + str_URL_query_1 + str_URL_query_bbox + str_URL_query_dim
            list_str_url.append(str_url)

        # get a unique alpha-numeric string
        str_unique_tag = sf.fn_get_random_string(4, 2)

        list_tile_download_path = []

        int_count = 0
        for item_url in list_str_url:
            str_filename = STR_OUTPUT_PATH + str_unique_tag + "_USGS_WCS_DEM_"
            str_filename += str(list_str_tile_name[int_count])
            str_filename += ".tif"
            list_tile_download_path.append(str_filename)
            int_count += 1

        # merge the "list_str_url" and the "list_tile_download_path" to create
        # list of lists for multi-threading
        list_merge_url_file = [list(x) for x in zip(list_str_url, list_tile_download_path)]

        # **************************
        # Multi-threaded download
        # Downloading the DEM tiles from the list_str_url

        # Initial call to print 0% progress
        len_url = len(list_str_url)
        print()

        str_prefix = "Polygon " + str(index_gdf_int + 1) + " of " + str(len(gdf_aoi_lambert))
        fn_print_progress_bar(0, len_url, prefix=str_prefix, suffix="Complete", length=36)

        i = 0
        results = ThreadPool(10).imap_unordered(fn_download_tiles, list_merge_url_file)

        for str_requested_tile in results:
            time.sleep(0.1)
            i += 1
            fn_print_progress_bar(i, len_url, prefix=str_prefix, suffix="Complete", length=36)
        # **************************

        dem_files = list_tile_download_path

        # Empty list to store opened DEM files
        d = []
        for file in dem_files:
            src = rasterio.open(file)
            d.append(src)

        out_meta = src.meta.copy()

        mosaic, out_trans = merge(d)

        # Create Metadata of the for the mosaic TIFF
        out_meta.update(
            {"driver": "HFA", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans}
        )

        str_out_tiff_path = STR_OUTPUT_PATH + str_unique_tag + "_merge_DEM" + ".tif"

        # Write the updated DEM to the specified file path
        with rasterio.open(str_out_tiff_path, "w", **out_meta) as dest:
            dest.write(mosaic)

        # TODO - File is used by another process error - 2021.10.24

        # remove the downloaded tiles
        int_count = 0
        for item in d:
            d[int_count].close()
            int_count += 1

        for filename in list_tile_download_path:
            if os.path.exists(filename):
                os.remove(filename)

        with rxr.open_rasterio(str_out_tiff_path) as usgs_wcs_dem:
            # reproject the raster to the same projection as the input shapefile

            usgs_wcs_local_proj = usgs_wcs_dem.rio.reproject(aoi_prj_crs)
            # rio no longer likes in place reprojections, so we will save to disk temp, then reload

            #usgs_wcs_local_proj = usgs_wcs_dem.rio.write_crs(aoi_prj_crs, inplace=True)

            if model_unit == "feet":
                # scale the raster from meters to feet
                usgs_wcs_local_proj = usgs_wcs_local_proj * 3.28084

            # convert the buffered lambert shapefile to requested polygon projection
            gdf_aoi_prj_with_buffer = gdf_aoi_lambert_current.to_crs(aoi_prj_crs)

            # get the geometry json of the polygon
            str_geom = sf.get_geometry_from_gdf(gdf_aoi_prj_with_buffer, 0)

            usgs_wcs_local_proj_clipped = usgs_wcs_local_proj.rio.clip(str_geom)

            # set the name from input field if available
            if b_have_valid_label_field:
                str_dem_out = STR_OUTPUT_PATH + str(gdf_aoi_lambert_current[STR_FIELD_TO_LABEL][0]) + ".tif"
            else:
                str_dem_out = STR_OUTPUT_PATH + str_unique_tag + "_clip_DEM" + ".tif"

            usgs_wcs_local_proj_clipped.rio.to_raster(str_dem_out, compress="LZW", dtype="float32")

        # remove the merged DEM
        if os.path.exists(str_out_tiff_path):
            os.remove(str_out_tiff_path)

    print("")
    print("ALL AREAS COMPLETE")

    flt_end_get_usgs_dem = time.time()
    flt_time_get_usgs_dem = (flt_end_get_usgs_dem - flt_start_get_usgs_dem) // 1
    time_get_usgs_dem = datetime.timedelta(seconds=flt_time_get_usgs_dem)
    print("Compute Time: " + str(time_get_usgs_dem))

    print("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    flt_start_run = time.time()

    parser = argparse.ArgumentParser(
        description="=================== USGS TERRAIN FROM SHAPEFILE ==================="
    )

    parser.add_argument(
        "-i",
        dest="str_input_path",
        help=r"REQUIRED: path to the input shapefile (polygons) Example: C:\shapefiles\area.shp",
        required=True,
        metavar="FILE",
        type=lambda x: is_valid_file(parser, x),
    )

    parser.add_argument(
        "-o",
        dest="str_output_dir",
        help=r"REQUIRED: directory to write DEM files Example: Example: D:\terrain",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-r",
        dest="int_res",
        help="OPTIONAL: requested sample resolution (meters): Default=3",
        required=False,
        default=3,
        metavar="INTEGER",
        type=int,
    )

    parser.add_argument(
        "-b",
        dest="int_buffer",
        help="OPTIONAL: buffer for each polygon (meters): Default=300",
        required=False,
        default=300,
        metavar="INTEGER",
        type=int,
    )

    parser.add_argument(
        "-t",
        dest="int_tile",
        help="OPTIONAL: requested tile dimensions (meters): Default=1500",
        required=False,
        default=1500,
        metavar="INTEGER",
        type=int,
    )

    parser.add_argument(
        "-f",
        dest="str_field_name",
        help="OPTIONAL: unique field from input shapefile used for DEM name",
        default="HUC_12",
        required=False,
        metavar="STRING",
        type=str,
    )

    args = vars(parser.parse_args())

    str_input_path = args["str_input_path"]
    str_output_dir = args["str_output_dir"]
    int_res = args["int_res"]
    int_buffer = args["int_buffer"]
    int_tile = args["int_tile"]
    str_field_name = args["str_field_name"]

    # find model unit using the given shapefile
    gis_prj_path = str_input_path[0:-3] + "prj"
    with open(gis_prj_path, "r") as prj_file:
        prj_text = prj_file.read()
    proj_crs = pyproj.CRS(prj_text)
    model_unit = sf.model_unit_from_crs(proj_crs)

    fn_get_usgs_dem_from_shape(
        str_input_path, str_output_dir, int_res, int_buffer, int_tile, model_unit, str_field_name
    )

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    print("Compute Time: " + str(time_pass))
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
