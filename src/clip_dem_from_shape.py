# Create clipped DEM files from larger DEM or VRT
#
# Purpose:
# The user will create a large terrain or virtual raster terrain (VRT)
# and store on a local drive.
# This routine will cut the DEM to individual files (GeoTIFF) and if
# requested convert vertical data from feet to meters.  Files can be
# saved using the a unique field in the input shapefile.
#
#
# Created by: Andy Carter, PE
# Created: 2021.09.28
# Last revised - 2021.10.24
#
# Uses the 'ras2fim' conda environment

import argparse
import datetime
import os
import random
import string
import time

import geopandas as gpd
import pandas as pd
import pyproj
import rasterio

import rioxarray
import tqdm

import shared_functions as sf


# ************************************************************

# null value in the exported DEMs
INT_NO_DATA_VAL = -9999

# -------------------------------------------------
def fn_get_features(gdf, int_poly_index):
    """Function to parse features from GeoDataFrame in such
    a manner that rasterio wants them

    Keyword arguments:
        gdf -- pandas geoDataFrame
        int_poly_index -- index of the row in the geoDataFrame
    """
    import json

    return [json.loads(gdf.to_json())["features"][int_poly_index]["geometry"]]


# -------------------------------------------------
def fn_get_random_string(int_letter_len_fn, int_num_len_fn):
    """Creates a random string of letters and numbers

    Keyword arguments:
    int_letter_len_fn -- length of string letters
    int_num_len_fn -- length of string numbers
    """
    letters = string.ascii_lowercase
    numbers = string.digits

    str_total = "".join(random.choice(letters) for i in range(int_letter_len_fn))
    str_total += "".join(random.choice(numbers) for i in range(int_num_len_fn))

    return str_total


# -------------------------------------------------
def fn_is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg
        # return open(arg, 'r')  # return an open file handle


# -------------------------------------------------
def fn_cut_dems_from_shapes(
    str_input_shp_path, str_input_terrain_path, str_output_dir, int_buffer, model_unit, str_field_name
):

    flt_start_run = time.time()

    print(" ")
    print("+=================================================================+")
    print("|         CUT DEMs FROM LARGER DEM PER POLYGON SHAPEFILE          |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) SHAPEFILE INPUT PATH: " + str_input_shp_path)
    print("  ---(t) TERRAIN INPUT PATH: " + str_input_terrain_path)
    print("  ---(o) DEM OUTPUT PATH: " + str_output_dir)
    print("  ---[b]   Optional: BUFFER: " + str(int_buffer))
    print("  ---[f]   Optional: FIELD NAME: " + str(str_field_name))
    print("  --- The Ras Models unit (extracted from given shapefile): " + model_unit)
    print("+-----------------------------------------------------------------+")

    if not os.path.exists(str_output_dir):
        os.mkdir(str_output_dir)

    # get the crs of the VRT/DEM using rasterio
    with rasterio.open(str_input_terrain_path) as raster_vrt:
        str_raster_prj = str(raster_vrt.crs)

    # read in the shapefile to geoDataframe
    gdf_boundary_prj = gpd.read_file(str_input_shp_path)

    # string of the shapefiles coordinate ref system.
    str_shape_crs = gdf_boundary_prj.crs

    # buffer all the polygons in shp CRS units
    gdf_boundary_prj["geometry"] = gdf_boundary_prj.geometry.buffer(int_buffer)

    # ------------------------------------
    # determine if the naming field can be used in for the output DEMs
    b_have_valid_label_field = False

    # determine if the requested naming field is in the input shapefile
    if str_field_name in gdf_boundary_prj.columns:

        if len(gdf_boundary_prj) < 2:
            # no need to convert to series if there is just one polygon
            b_have_valid_label_field = True

        else:
            # create a dataframe of just the requested field
            gdf_just_req_field = pd.DataFrame(gdf_boundary_prj, columns=[str_field_name])

            # convert the dataframe to a series
            df_just_req_field_series = gdf_just_req_field.squeeze()

            # determine if the naming field is unique
            if df_just_req_field_series.is_unique:
                b_have_valid_label_field = True
            else:
                print("No unique values found.  Naming will be random")
    # ------------------------------------

    # reproject the shapefile to the CRS of virtual raster (VRT)
    gdf_boundary_raster_prj = gdf_boundary_prj.to_crs(str_raster_prj)

    for index, row in tqdm.tqdm(
        gdf_boundary_raster_prj.iterrows(),
        total=gdf_boundary_raster_prj.shape[0],
        desc="Clipping Grids",
        bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
        ncols=65,
    ):
        # convert the geoPandas geometry to json
        json_boundary = fn_get_features(gdf_boundary_raster_prj, index)

        with rioxarray.open_rasterio(str_input_terrain_path, masked=True).rio.clip(
            json_boundary, from_disk=True
        ) as xds_clipped:

            # reproject the DEM to the shp CRS
            xds_clipped_reproject = xds_clipped.rio.reproject(str_shape_crs)

            # convert vertical meters to feet
            if model_unit == "feet":
                xds_clipped_reproject = xds_clipped_reproject * 3.28084

            # set the null data values
            xds_clipped_reproject = xds_clipped_reproject.fillna(INT_NO_DATA_VAL)
            xds_clipped_reproject = xds_clipped_reproject.rio.set_nodata(INT_NO_DATA_VAL)

            # set the name from input field if available
            if b_have_valid_label_field:
                str_dem_out = str_output_dir + "\\" + str(gdf_boundary_prj[str_field_name][index]) + ".tif"
            else:
                str_unique_tag = fn_get_random_string(2, 4)
                str_dem_out = str_output_dir + "\\" + str_unique_tag + ".tif"

            # compress and write out data
            xds_clipped_reproject.rio.to_raster(str_dem_out, compress="lzw", dtype="float32")

    print("COMPLETE")
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    print("Compute Time: " + str(time_pass))
    print("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="============== CUT DEMs FROM LARGER DEMS PER POLYGON SHAPEFILE  =============="
    )

    parser.add_argument(
        "-i",
        dest="str_input_shp_path",
        help=r"REQUIRED: path to the input shapefile (polygons) Example: C:\shapefiles\area.shp",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-t",
        dest="str_input_terrain_path",
        help=r"REQUIRED: path to the input DEM terrain (tif or vrt) Example: G:\x-fathom\DEM\temp.vrt",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-o",
        dest="str_output_dir",
        help=r"REQUIRED: directory to write DEM files Example: Example: C:\test\terrain_out",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-b",
        dest="int_buffer",
        help="OPTIONAL: buffer for each polygon (input shape units): Default=300",
        required=False,
        default=300,
        metavar="INTEGER",
        type=int,
    )

    parser.add_argument(
        "-f",
        dest="str_field_name",
        help="OPTIONAL: unique field from input shapefile used for DEM name",
        required=False,
        default="HUC_12",
        metavar="STRING",
        type=str,
    )

    args = vars(parser.parse_args())

    str_input_shp_path = args["str_input_shp_path"]
    str_input_terrain_path = args["str_input_terrain_path"]
    str_output_dir = args["str_output_dir"]
    int_buffer = args["int_buffer"]
    str_field_name = args["str_field_name"]

    # find model unit using the given shapefile
    gis_prj_path = str_input_shp_path[0:-3] + "prj"
    with open(gis_prj_path, "r") as prj_file:
        prj_text = prj_file.read()
    proj_crs = pyproj.CRS(prj_text)
    model_unit = sf.model_unit_from_crs(proj_crs)

    fn_cut_dems_from_shapes(
        str_input_shp_path, str_input_terrain_path, str_output_dir, int_buffer, model_unit, str_field_name
    )

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
