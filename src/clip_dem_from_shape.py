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
import time
import traceback

import geopandas as gpd
import pandas as pd
import pyproj
import rasterio
import rioxarray
import tqdm
from geopandas.tools import sjoin

import ras2fim_logger
import shared_functions as sf


# ************************************************************

# Global Variables
# null value in the exported DEMs
INT_NO_DATA_VAL = -9999
# RLOG = ras2fim_logger.RAS2FIM_logger()
RLOG = ras2fim_logger.R2F_LOG


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
    str_input_shp_path,
    str_cross_sections_path,
    str_input_terrain_path,
    str_output_dir,
    int_buffer,
    model_unit,
    str_field_name
):
    flt_start_run = time.time()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|         CUT DEMs FROM LARGER DEM PER POLYGON SHAPEFILE          |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    RLOG.lprint("  ---(i) SHAPEFILE INPUT PATH: " + str_input_shp_path)
    RLOG.lprint("  ---(x) XS SHAPEFILE INPUT PATH: " + str_cross_sections_path)
    RLOG.lprint("  ---(t) TERRAIN INPUT PATH: " + str_input_terrain_path)
    RLOG.lprint("  ---(o) DEM OUTPUT PATH: " + str_output_dir)
    RLOG.lprint("  ---[b]   Optional: BUFFER: " + str(int_buffer))
    RLOG.lprint("  ---[f]   Optional: FIELD NAME: " + str(str_field_name))
    RLOG.lprint("  --- The Ras Models unit (extracted from given shapefile): " + model_unit)
    RLOG.lprint("+-----------------------------------------------------------------+")

    if not os.path.exists(str_output_dir):
        os.mkdir(str_output_dir)

    # get the crs of the VRT/DEM using rasterio
    with rasterio.open(str_input_terrain_path) as raster_vrt:
        str_raster_prj = str(raster_vrt.crs)

    gdf_boundary_prj = gpd.read_file(str_input_shp_path)

    # intersect with model cross sections
    gdf_xs_lines = gpd.read_file(str_cross_sections_path)
    # spatial join cross sections with HUC12s, keeping HUC12 geometry
    gdf_intersection = sjoin(gdf_xs_lines, gdf_boundary_prj.to_crs(gdf_xs_lines.crs), how="right")
    # drop all rows that don't have a cross section with a ras_path (no xs in that huc12)
    gdf_boundary_prj = gdf_intersection.dropna(subset=["ras_path"])
    # keep largest area (arbitrary) of HUC12 that overlaps cross sections so we have unique huc12 entries
    gdf_boundary_prj = gdf_boundary_prj.sort_values(by="Shape_Area").drop_duplicates(subset=["HUC_12"])

    # dissolve all huc12s together (cross sections -should- have same parent path in ras2fim v2,
    #  but this ensures dissolving)
    gdf_boundary_prj['dissolve_index'] = 1
    # in case ras_path isn't just one value, just dissolve everything that's still there
    gdf_boundary_prj = gdf_boundary_prj.dissolve(by="dissolve_index").reset_index()

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
            df_just_req_field_series = df_just_req_field_series.drop_duplicates()

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

        try:
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
                    str_dem_out = f"{str_output_dir}\\{gdf_boundary_prj[str_field_name][index]}.tif"
                else:
                    str_unique_tag = sf.fn_get_random_string(2, 4)
                    str_dem_out = str_output_dir + "\\" + str_unique_tag + ".tif"

                str_dem_out = str_dem_out.replace(".tif", f"_{gdf_boundary_prj['river'][index]}.tif")
                # compress and write out data
                xds_clipped_reproject.rio.to_raster(str_dem_out, compress="lzw", dtype="float32")
        except Exception as e:
            print(e)
            print("No overlap.. skipping")

    RLOG.success("COMPLETE")
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    RLOG.lprint("Compute Time: " + str(time_pass))
    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample:
    # python clip_dem_from_shape.py -i
    #  c:\ras2fim_data\output_ras2fim\12030105_2276_231024\02_shapes_from_conflation\12030105_huc_12_ar.shp
    #  -t C:\ras2fim_data\inputs\3dep_dems\HUC8_10m_5070\HUC8_12030105_dem.tif
    #  -o c:\ras2fim_data\output_ras2fim\12030105_2276_231024\03_terrain -b 300 -f HUC_12

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
        "-x",
        dest="str_cross_sections_path",
        help=r"REQUIRED: path to the input cross sections shapefile (lines) Example: C:\shapefiles\xs.shp",
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
    str_cross_sections_path = args["str_cross_sections_path"]
    str_input_terrain_path = args["str_input_terrain_path"]
    str_output_dir = args["str_output_dir"]
    int_buffer = args["int_buffer"]
    str_field_name = args["str_field_name"]

    # find model unit using the given shapefile
    try:
        gis_prj_path = str_input_shp_path[0:-3] + "prj"
        with open(gis_prj_path, "r") as prj_file:
            prj_text = prj_file.read()
    except Exception:
        prj_text = gpd.read_file(str_input_shp_path).crs

    proj_crs = pyproj.CRS(prj_text)

    model_unit = sf.model_unit_from_crs(proj_crs)

    log_file_folder = args["str_output_dir"]
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        fn_cut_dems_from_shapes(
            str_input_shp_path,
            str_cross_sections_path,
            str_input_terrain_path,
            str_output_dir,
            int_buffer,
            model_unit,
            str_field_name
        )
    except Exception:
        RLOG.critical(traceback.format_exc())
