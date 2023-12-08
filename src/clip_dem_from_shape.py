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
import xarray as xr
import tqdm
from geopandas.tools import sjoin
from shapely.geometry import mapping

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
    str_conflated_models_path,
    str_input_terrain_path,
    str_output_dir,
    int_buffer,
    model_unit,
):
    flt_start_run = time.time()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|         CUT DEMs FROM LARGER DEM PER POLYGON SHAPEFILE          |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    RLOG.lprint("  ---(i) HUC12s SHAPEFILE PATH: " + str_input_shp_path)
    RLOG.lprint("  ---(x) XS SHAPEFILE PATH: " + str_cross_sections_path)
    RLOG.lprint("  ---(x) CONFLATED MODELS LIST PATH: " + str_conflated_models_path)
    RLOG.lprint("  ---(t) TERRAIN INPUT PATH: " + str_input_terrain_path)
    RLOG.lprint("  ---(o) DEM OUTPUT PATH: " + str_output_dir)
    RLOG.lprint("  ---[b]   Optional: BUFFER: " + str(int_buffer))
    RLOG.lprint("  --- The Ras Models unit (extracted from given shapefile): " + model_unit)
    RLOG.lprint("+-----------------------------------------------------------------+")

    if not os.path.exists(str_output_dir):
        os.mkdir(str_output_dir)

    # get the crs of the VRT/DEM using rasterio
    with rasterio.open(str_input_terrain_path) as raster_vrt:
        str_raster_prj = str(raster_vrt.crs)

    gdf_huc12s = gpd.read_file(str_input_shp_path)

    # read models xsections
    gdf_xs_lines = gpd.read_file(str_cross_sections_path)

    #filter xsections only for the conflated models
    conflated_models=pd.read_csv(str_conflated_models_path)
    conflated_mode_ids=conflated_models['model_id'].unique().tolist()

    gdf_xs_lines=gdf_xs_lines.merge(conflated_models, on='ras_path', how='inner') #this filters conflated xsections

    #read dem as Xarray DataArray.we use rio for reproject, crop, and save. The rest is Xr DataArray operations.
    dem = rioxarray.open_rasterio(str_input_terrain_path)
    dem = dem.rio.reproject(gdf_huc12s.crs)

    for model_id in conflated_mode_ids:
        this_model_xsections=gdf_xs_lines[gdf_xs_lines['model_id']==model_id]

        #find HUC12s intersected with this model xsections
        gdf_intersected_hucs=gdf_huc12s[gdf_huc12s.intersects(this_model_xsections.unary_union)]

        #if more than 1 HUC12 is intersected, dissolve them to make a signle domain for the model
        if len(gdf_intersected_hucs)>1:
            gdf_intersected_hucs.loc[:,'dissolve_index'] = 1
            gdf_intersected_hucs = gdf_intersected_hucs.dissolve(by="dissolve_index").reset_index()

        # add the buffer
        #TODO do we need this buffer anymore? using all intersected HUC12s ensured all xsections have dem coverage
        gdf_intersected_hucs.loc[:,"geometry"] = gdf_intersected_hucs.geometry.buffer(int_buffer)

        #now clip dem with rio and the intersected HUC12s
        clipped_dem = dem.rio.clip(gdf_intersected_hucs.geometry.apply(mapping))

        if model_unit == "feet":
            clipped_dem = xr.where(clipped_dem == clipped_dem.rio.nodata,INT_NO_DATA_VAL,clipped_dem * 3.28084)

        #xr.where operation above may remove the attributes (e.g. nodata) so always reassign nodata as below
        clipped_dem = clipped_dem.assign_attrs({'_FillValue':INT_NO_DATA_VAL})

        str_dem_out = str_output_dir + "\\" + str(model_id) + ".tif"
        clipped_dem.rio.to_raster(str_dem_out, compress="lzw", dtype="float32")

    RLOG.success("COMPLETE")
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    RLOG.lprint("Compute Time: " + str(time_pass))
    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample:
    #python .\clip_dem_from_shape.py
    # -x "c:\ras2fim_data\output_ras2fim\12030105_2276_231024\01_shapes_from_hecras\cross_section_LN_from_ras.shp"
    # -i "c:\ras2fim_data\output_ras2fim\12030105_2276_231024\02_shapes_from_conflation\12090301_huc_12_ar.shp"
    # -t "C:\ras2fim_data\inputs\HUC8_12090301_dem.tif"
    # -o desired_output_dir
    # -conflate "c:\ras2fim_data\output_ras2fim\12030105_2276_231024\02_shapes_from_conflation\***_stream_qc.csv"

    parser = argparse.ArgumentParser(
        description="============== CUT DEMs FROM LARGER DEMS PER POLYGON SHAPEFILE  =============="
    )

    parser.add_argument(
        "-x",
        dest="str_cross_sections_path",
        help=r"REQUIRED: path to the HEC-RAS models cross sections shapefile (lines) Example: cross_section_LN_from_ras.shp",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-i",
        dest="str_input_shp_path",
        help=r"REQUIRED: path to the HUC12 polygons shapefile Example: '02_shapes_from_conflation\***_huc_12_ar.shp'",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-conflate",
        dest="str_conflated_models_path",
        help=r"REQUIRED: path to the CSV file containing conflated models",
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


    args = vars(parser.parse_args())

    str_input_shp_path = args["str_input_shp_path"]
    str_cross_sections_path = args["str_cross_sections_path"]
    str_conflated_models_path=args["str_conflated_models_path"]
    str_input_terrain_path = args["str_input_terrain_path"]
    str_output_dir = args["str_output_dir"]
    int_buffer = args["int_buffer"]


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
            str_conflated_models_path,
            str_input_terrain_path,
            str_output_dir,
            int_buffer,
            model_unit
        )
    except Exception:
        RLOG.critical(traceback.format_exc())
