# This routine will cut the DEM to HUC12s intersecting individual RAS models.
# The input HUC8 DEM is assumed to have z unit of meter. If RAS model unit is in feet,
# elevation values of the clipped DEM are converted from meter to ft.

import argparse
import datetime
import os
import shutil
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


# Global Variables
# null value in the exported DEMs
INT_NO_DATA_VAL = -9999
RLOG = sv.R2F_LOG
pd.options.mode.chained_assignment = None


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
    huc8,
    huc12_features_file,
    cross_sections_file_path,
    conflated_models_file_path,
    terrain_file_path,
    output_dir,
    model_unit="",
):
    flt_start_run = time.time()

    # -------------------
    if model_unit == "":
        # find model unit using the given shapefile
        try:
            gis_prj_path = cross_sections_file_path[0:-3] + "prj"
            with open(gis_prj_path, "r") as prj_file:
                prj_text = prj_file.read()
        except Exception:
            prj_text = gpd.read_file(cross_sections_file_path).crs

        proj_crs = pyproj.CRS(prj_text)

        model_unit = sf.model_unit_from_crs(proj_crs)

    # -------------------
    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.notice("|         CUT DEMs FROM LARGER DEM PER POLYGON SHAPEFILE          |")
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint(f"  ---(w) HUC8: {huc8}")
    RLOG.lprint(f"  ---(i) HUC12s FEATURES PATH: {huc12_features_file}")
    RLOG.lprint(f"  ---(x) XS SHAPEFILE PATH: {cross_sections_file_path}")
    RLOG.lprint(f"  ---(conflate) CONFLATED MODELS LIST PATH: {conflated_models_file_path}")
    RLOG.lprint(f"  ---(o) DEM OUTPUT PATH: {output_dir}")

    if "[]" in terrain_file_path:
        terrain_file_path = sv.INPUT_3DEP_DEFAULT_TERRAIN_DEM.replace("[]", huc8)
        RLOG.lprint(f"  ---[t] TERRAIN INPUT PATH (calculated): {terrain_file_path}")
    else:
        RLOG.lprint(f"  ---[t] TERRAIN INPUT PATH : {terrain_file_path}")
        RLOG.lprint(f"  --- The Ras Models unit: {model_unit}")
    RLOG.lprint("+-----------------------------------------------------------------+")

    # -------------------
    # Validation and variable setup
    # TODO complete validation and page setup

    if "[]" in terrain_file_path:  # calculate it based on defaults
        terrain_file_path = sv.INPUT_3DEP_DEFAULT_TERRAIN_DEM.replace("[]", huc8)
        # dem might not yet be on the file system.
        if os.path.exists(terrain_file_path) is False:
            raise ValueError(
                f"The calculated terrain DEM path of {terrain_file_path} does not appear exist.\n"
                f"For NOAA/OWP staff.... this file can likely be downloaded from {sv.S3_INPUTS_3DEP_DEMS}"
            )
    elif terrain_file_path != "":
        if os.path.exists(terrain_file_path) is False:  # might be a full path
            raise ValueError(
                f"The default calculated terrain DEM path of {terrain_file_path} does not appear exist."
            )
    else:
        raise ValueError("terrain DEM path has not been set.")

    # -------------------
    if (model_unit != "feet") and (model_unit != "meter"):
        raise Exception(f"Interal Error: The calcated model unit value of {model_unit} is invalid.")

    # =================================

    # if it does exist, clear it and start over
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(1)  # 1 seconds
    os.mkdir(output_dir)

    # -------------------
    # read models xsections
    gdf_xs_lines = gpd.read_file(cross_sections_file_path)

    # read HUC12s
    RLOG.lprint("Reading HUC12 polygons for CONUS...this may take a few minutes")
    gdf_huc12s = gpd.read_file(huc12_features_file)

    # important to reproject to model crs especially if the inputs
    # HUC12s are for the entire US with geographic crs
    gdf_huc12s.to_crs(gdf_xs_lines.crs, inplace=True)

    # filter xsections only for the conflated models
    conflated_models = pd.read_csv(conflated_models_file_path)
    conflated_model_ids = conflated_models['model_id'].unique().tolist()

    gdf_xs_lines = gdf_xs_lines.merge(
        conflated_models, on='ras_path', how='inner'
    )  # this filters conflated xsections

    # read dem as Xarray DataArray.use rio for reproject,crop, save. The rest is Xr DataArray operations.
    dem = rioxarray.open_rasterio(terrain_file_path)
    dem = dem.rio.reproject(gdf_huc12s.crs)

    # note that because of the large size of the input DEM, there is no benefit in using multiprocessing here
    for model_id in tqdm.tqdm(
        conflated_model_ids,
        total=len(conflated_model_ids),
        desc="Clipping DEMs",
        bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
        ncols=65,
    ):
        RLOG.trace(f"Processing model_id of {model_id}")
        this_model_xsections = gdf_xs_lines[gdf_xs_lines['model_id'] == model_id]

        # find HUC12s intersected with this model xsections
        gdf_intersected_hucs = gdf_huc12s[gdf_huc12s.intersects(this_model_xsections.unary_union)]

        # if more than 1 HUC12 is intersected, dissolve them to make a signle domain for the model
        if len(gdf_intersected_hucs) > 1:
            gdf_intersected_hucs.loc[:, 'dissolve_index'] = 1
            gdf_intersected_hucs = gdf_intersected_hucs.dissolve(by="dissolve_index").reset_index()

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

        str_dem_out = os.path.join(output_dir, str(model_id) + ".tif")
        clipped_dem.rio.to_raster(str_dem_out, compress="lzw", dtype="float32")

    RLOG.success("COMPLETE")
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    RLOG.lprint("Compute Time: " + str(time_pass))
    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample (min args)
    # python .\clip_dem_from_shape.py
    # -w 12090301
    # -x "c:\ras2fim_data\output_ras2fim\***\01_shapes_from_hecras\cross_section_LN_from_ras.shp"
    # -conflate "c:\ras2fim_data\output_ras2fim\***\02_csv_shapes_from_conflation\conflated_ras_models.csv"
    # -o "C:\ras2fim_data\output_ras2fim\12030105_2276_240111\03_terrain"

    parser = argparse.ArgumentParser(
        description="============== CUT DEMs FROM LARGER DEMS PER POLYGON SHAPEFILE  =============="
    )

    parser.add_argument(
        "-w",
        dest="huc8",
        help="REQUIRED: HUC-8 that is being evaluated: Example: 12090301",
        required=True,
        metavar="",
        type=str,
    )  # has to be string so it doesn't strip the leading zero

    parser.add_argument(
        "-x",
        dest="cross_sections_file_path",
        help=r"REQUIRED: path to the HEC-RAS models cross sections shapefile (lines) "
        r"e.g. c:\ras2fim_data\output_ras2fim\***\01_shapes_from_hecras\cross_section_LN_from_ras.shp",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-conflate",
        dest="conflated_models_file_path",
        help=r"REQUIRED: path to the CSV file containing conflated models."
        r" e.g. c:\ras2fim_data\output_ras2fim\***\02_csv_shapes_from_conflation\conflated_ras_models.csv",
        required=True,
        metavar="FILE",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-o",
        dest="output_dir",
        help="REQUIRED: directory to write DEM files."
        " e.g. C:\ras2fim_data\output_ras2fim\12030105_2276_240111\03_terrain",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-i",
        dest="huc12_features_file",
        help="OPTIONAL: path to the HUC12 polygons shapefile/gpkg file."
        r" Example: C:\ras2fim_data\inputs\X-National_Datasets\WBD_National.gpkg\n"
        f"Defaults (huc adjusted) to {sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH}",
        required=False,
        default=sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH,
        metavar="",
        type=lambda x: fn_is_valid_file(parser, x),
    )

    parser.add_argument(
        "-t",
        dest="terrain_file_path",
        help="OPTIONAL: full path to terrain DEM Tif to use for mapping"
        r" e.g C:\ras2fim_data\inputs\dems\ras_3dep_HUC8_10m\HUC8_12030201_dem.tif.\n"
        f" Defaults (huc adjusted) to {sv.INPUT_3DEP_DEFAULT_TERRAIN_DEM}",
        required=False,
        metavar="",
        default=sv.INPUT_3DEP_DEFAULT_TERRAIN_DEM,
        type=str,
    )

    args = vars(parser.parse_args())

    log_file_folder = args["output_dir"]
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
        fn_cut_dems_from_shapes(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
