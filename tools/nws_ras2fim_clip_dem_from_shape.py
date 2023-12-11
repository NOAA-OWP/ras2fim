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
# Last revised - 2021.09.28
#
# Uses the 'ras2fim' conda environment

import argparse
import os
import sys

import geopandas as gpd
import pandas as pd
import rasterio
import rioxarray
import tqdm


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_functions as sf


# null value in the exported DEMs
INT_NO_DATA_VAL = -9999


# -------------------------------------------------
def fn_is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg


# -------------------------------------------------
def fn_cut_dems_from_shapes(
    str_input_shp_path, str_input_terrain_path, str_output_dir, int_buffer, b_is_feet, str_field_name
):
    print(" ")
    print("+=================================================================+")
    print("|         CUT DEMs FROM LARGER DEM PER POLYGON SHAPEFILE          |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) SHAPEFILE INPUT PATH: " + str_input_shp_path)
    print("  ---(t) TERRAIN INPUT PATH: " + str_input_terrain_path)
    print("  ---(o) DEM OUTPUT PATH: " + str_output_dir)
    print("  ---[b]   Optional: BUFFER: " + str(int_buffer))
    print("  ---[v]   Optional: VERTICAL IN FEET: " + str(b_is_feet))
    print("  ---[f]   Optional: FIELD NAME: " + str(str_field_name))
    print("+-----------------------------------------------------------------+")

    if not os.path.exists(str_output_dir):
        os.mkdir(str_output_dir)

    # get the crs of the VRT/DEM using rasterio
    with rasterio.open(str_input_terrain_path) as raster_vrt:
        str_raster_prj = str(raster_vrt.crs)

    # read in the shapefile to geoDataframe
    gdf_boundary_prj = gpd.read_file(str_input_shp_path)

    # string of the shapefiles coordinate ref system
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
        bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
        ncols=65,
    ):
        # convert the geoPandas geometry to json
        json_boundary = sf.get_geometry_from_gdf(gdf_boundary_raster_prj, index)

        with rioxarray.open_rasterio(str_input_terrain_path, masked=True).rio.clip(
            json_boundary, from_disk=True
        ) as xds_clipped:
            # reproject the DEM to the shp CRS
            xds_clipped_reproject = xds_clipped.rio.reproject(str_shape_crs)

            # convert vertical meters to feet
            # TODO - 2021.09.28 - why does this double multiplication work??
            if b_is_feet:
                xds_clipped_reproject = xds_clipped_reproject * 3.28084
                xds_clipped_reproject = xds_clipped_reproject * 3.28084

            # set the null data values
            xds_clipped_reproject = xds_clipped_reproject.fillna(INT_NO_DATA_VAL)
            xds_clipped_reproject = xds_clipped_reproject.rio.set_nodata(INT_NO_DATA_VAL)

            # set the name from input field if available
            if b_have_valid_label_field:
                str_dem_out = str_output_dir + "\\" + str(gdf_boundary_prj[str_field_name][index]) + ".tif"
            else:
                str_unique_tag = sf.fn_get_random_string(2, 4)
                str_dem_out = str_output_dir + "\\" + str_unique_tag + ".tif"

            # compress and write out data
            xds_clipped_reproject.rio.to_raster(str_dem_out, compress="lzw", dtype="float32")

    print("COMPLETE")


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
        help="OPTIONAL: buffer for each polygon (input shape units): Default=700",
        required=False,
        default=700,
        metavar="INTEGER",
        type=int,
    )

    parser.add_argument(
        "-v",
        dest="b_is_feet",
        help="OPTIONAL: create vertical data in feet: Default=False",
        required=False,
        default=True,
        action="store_false",
    )

    parser.add_argument(
        "-f",
        dest="str_field_name",
        help="OPTIONAL: unique field from input shapefile used for DEM name",
        required=False,
        metavar="STRING",
        type=str,
    )

    args = vars(parser.parse_args())

    str_input_shp_path = args["str_input_shp_path"]
    str_input_terrain_path = args["str_input_terrain_path"]
    str_output_dir = args["str_output_dir"]
    int_buffer = args["int_buffer"]
    b_is_feet = args["b_is_feet"]
    str_field_name = args["str_field_name"]

    fn_cut_dems_from_shapes(
        str_input_shp_path, str_input_terrain_path, str_output_dir, int_buffer, b_is_feet, str_field_name
    )
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
