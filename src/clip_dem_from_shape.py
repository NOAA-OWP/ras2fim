# This routine will cut the DEM to HUC12s intersecting individual RAS models.
# The input HUC8 DEM is assumed to have z unit of meter. If RAS model unit is in feet,
# elevation values of the clipped DEM are converted from meter to ft.


import argparse
import datetime
import os
import time
import traceback

import geopandas as gpd
import pandas as pd
import pyproj
import rioxarray
import tqdm
import xarray as xr
from shapely.geometry import mapping

import shared_functions as sf
import shared_variables as sv


RLOG = sv.R2F_LOG


pd.options.mode.chained_assignment = None

# ************************************************************

# Global Variables
# null value in the exported DEMs
INT_NO_DATA_VAL = -9999


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
    str_huc12_path,
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

    RLOG.lprint("  ---(i) HUC12s SHAPEFILE PATH: " + str_huc12_path)
    RLOG.lprint("  ---(x) XS SHAPEFILE PATH: " + str_cross_sections_path)
    RLOG.lprint("  ---(conflate) CONFLATED MODELS LIST PATH: " + str_conflated_models_path)
    RLOG.lprint("  ---(t) TERRAIN INPUT PATH: " + str_input_terrain_path)
    RLOG.lprint("  ---(o) DEM OUTPUT PATH: " + str_output_dir)
    RLOG.lprint("  ---[b] Optional: BUFFER: " + str(int_buffer))
    RLOG.lprint("  --- The Ras Models unit (extracted from given shapefile): " + model_unit)
    RLOG.lprint("+-----------------------------------------------------------------+")

    if not os.path.exists(str_output_dir):
        os.mkdir(str_output_dir)

    # read models xsections
    gdf_xs_lines = gpd.read_file(str_cross_sections_path)

    # read HUC12s
    RLOG.lprint("Reading HUC12 polygons for the entire CONUS...this may take 3 minutes")
    gdf_huc12s = gpd.read_file(str_huc12_path)

    # important to reproject to model crs especially if the inputs
    # HUC12s are for the entire US with geographic crs
    gdf_huc12s.to_crs(gdf_xs_lines.crs, inplace=True)

    # filter xsections only for the conflated models
    conflated_models = pd.read_csv(str_conflated_models_path)
    conflated_mode_ids = conflated_models['model_id'].unique().tolist()

    gdf_xs_lines = gdf_xs_lines.merge(
        conflated_models, on='ras_path', how='inner'
    )  # this filters conflated xsections

    # read dem as Xarray DataArray.use rio for reproject,crop, save. The rest is Xr DataArray operations.
    dem = rioxarray.open_rasterio(str_input_terrain_path)
    dem = dem.rio.reproject(gdf_huc12s.crs)

    # note that because of the large size of the input DEM, there is no benefit in using multiprocessing here
    for model_id in tqdm.tqdm(
        conflated_mode_ids,
        total=len(conflated_mode_ids),
        desc="Clipping DEMs",
        bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
        ncols=65,
    ):
        this_model_xsections = gdf_xs_lines[gdf_xs_lines['model_id'] == model_id]

        # find HUC12s intersected with this model xsections
        gdf_intersected_hucs = gdf_huc12s[gdf_huc12s.intersects(this_model_xsections.unary_union)]

        # if more than 1 HUC12 is intersected, dissolve them to make a signle domain for the model
        if len(gdf_intersected_hucs) > 1:
            gdf_intersected_hucs.loc[:, 'dissolve_index'] = 1
            gdf_intersected_hucs = gdf_intersected_hucs.dissolve(by="dissolve_index").reset_index()

        # add the buffer
        # TODO do we need this buffer anymore?
        #  because using all intersected HUC12s ensured all xsections have dem coverage
        gdf_intersected_hucs.loc[:, "geometry"] = gdf_intersected_hucs.geometry.buffer(int_buffer)

        # now clip dem with rio and the intersected HUC12s
        clipped_dem = dem.rio.clip(gdf_intersected_hucs.geometry.apply(mapping))

        if model_unit == "feet":
            clipped_dem = xr.where(
                clipped_dem == clipped_dem.rio.nodata, INT_NO_DATA_VAL, clipped_dem * 3.28084
            )

        # xr.where operation above may remove the attributes (nodata, crs) so always reassign them as below
        clipped_dem = clipped_dem.assign_attrs({'_FillValue': INT_NO_DATA_VAL})
        if clipped_dem.rio.crs is None:
            clipped_dem.rio.write_crs(gdf_huc12s.crs, inplace=True)

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
    # python .\clip_dem_from_shape.py
    # -x "c:\ras2fim_data\output_ras2fim\***\01_shapes_from_hecras\cross_section_LN_from_ras.shp"
    # -i 'C:\ras2fim_data\inputs\X-National_Datasets\WBD_National.gpkg'
    # -t "C:\ras2fim_data\inputs\HUC8_12090301_dem.tif"
    # -o desired_output_dir
    # -conflate "c:\ras2fim_data\output_ras2fim\***\02_csv_shapes_from_conflation\***_stream_qc.csv"

    parser = argparse.ArgumentParser(
        description="============== CUT DEMs FROM LARGER DEMS PER POLYGON SHAPEFILE  =============="
    )

    parser.add_argument(
        "-x",
        dest="str_cross_sections_path",
        help=r"REQUIRED: path to the HEC-RAS models cross sections shapefile (lines) "
        r"Example: cross_section_LN_from_ras.shp",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-i",
        dest="str_huc12_path",
        help=r"REQUIRED: path to the HUC12 polygons shapefile/gpkg file"
        r"Example: C:\ras2fim_data\inputs\X-National_Datasets\WBD_National.gpkg",
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

    str_huc12_path = args["str_huc12_path"]
    str_cross_sections_path = args["str_cross_sections_path"]
    str_conflated_models_path = args["str_conflated_models_path"]
    str_input_terrain_path = args["str_input_terrain_path"]
    str_output_dir = args["str_output_dir"]
    int_buffer = args["int_buffer"]

    # find model unit using the given shapefile
    try:
        gis_prj_path = str_cross_sections_path[0:-3] + "prj"
        with open(gis_prj_path, "r") as prj_file:
            prj_text = prj_file.read()
    except Exception:
        prj_text = gpd.read_file(str_cross_sections_path).crs

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
            str_huc12_path,
            str_cross_sections_path,
            str_conflated_models_path,
            str_input_terrain_path,
            str_output_dir,
            int_buffer,
            model_unit,
        )
    except Exception:
        RLOG.critical(traceback.format_exc())
