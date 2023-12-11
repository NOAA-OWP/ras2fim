# Create simplified depth grid rasters from those created from HEC-RAS
#
# Purpose:
# Converts the depth grid terrains to smaller and more simple geotiffs.
# Can convert the projection and the resolution as requiested.  In
# conformance with InFRM, created grids as 16 Bit Unsigned Integers with
# a specified NoData value
#
# Created by: Andy Carter, PE
# Created: 2021-08-23
# Last revised - 2021-10-24
#
# Uses the 'ras2fim' conda environment
# ************************************************************
import argparse
import datetime
import multiprocessing as mp
import os
import pathlib
import re
import time
import traceback
from time import sleep

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr
import tqdm

import shared_functions as sf
import shared_variables as sv


# -------------------------------------------------
# Global Variables
RLOG = sv.R2F_LOG
# MP_LOG = None # the mp version
#    While this code does use multi processing, the function be used inside the MP
#    is not in this script, so MP_LOG system is not required.

# buffer distance of the input flood polygon - in CRS units
FLT_BUFFER = 15


# -------------------------------------------------
def fn_filelist(source, tpl_extenstion):
    # walk a directory and get files with suffix
    # returns a list of file paths
    # args:
    #   source = path to walk
    #   tpl_extenstion = tuple of the extensions to find (Example: (.tig, .jpg))
    #   str_dem_path = path of the dem that needs to be converted
    matches = []
    for root, dirnames, filenames in os.walk(source):
        for filename in filenames:
            if filename.endswith(tpl_extenstion):
                matches.append(os.path.join(root, filename))
    return matches


# -------------------------------------------------
def fn_unique_list(list_input):
    # function to get a unique list of values
    list_unique = []
    # traverse for all elements
    for x in list_input:
        # check if exists in list_unique or not
        if x not in list_unique:
            list_unique.append(x)
    return list_unique


# -------------------------------------------------
def fn_create_grid(list_of_df_row):
    # function to create InFRM compliant dems
    # args:
    #   list_of_df_row = list of a single row in dataframe that contains:
    #     str_polygon_path = path to boundary polygon from HEC-RAS
    #     str_file_to_create_path = path to of the file to create
    #     str_dem_path = path of the dem that needs to be converted
    #     str_output_crs = coordinate ref system of the output raster
    #     flt_desired_res = resolution of the output raster

    (
        str_polygon_path,
        str_file_to_create_path,
        str_dem_path,
        str_output_crs,
        flt_desired_res,
        model_unit,
    ) = list_of_df_row

    # print(f"flt_desired_res is {flt_desired_res}")

    # read in the HEC-RAS generatated polygon of the last elevation step
    gdf_flood_limits = gpd.read_file(str_polygon_path)

    # buffer the geodataframe of the flood polygon
    gdf_flood_buffer = gdf_flood_limits.buffer(FLT_BUFFER, 8)

    # get a geodataframe envelope (bounding box)
    gdf_flood_depth_envelope = gdf_flood_buffer.envelope

    # get the coordinates of the envelope
    # note - this is pulling the first polygon found
    coords = sf.get_geometry_from_gdf(gdf_flood_depth_envelope, 0)

    with rxr.open_rasterio(str_dem_path, masked=True).rio.clip(coords, from_disk=True) as xds_clipped:
        # using rioxarray - clip the HEC-RAS dem to the bounding box
        # xds_clipped = rxr.open_rasterio(str_dem_path,masked=True,).rio.clip(coords, from_disk=True)

        # reproject the DEM to the requested CRS
        xds_clipped_reproject = xds_clipped.rio.reproject(str_output_crs)

        # convert the pixel values to meter, if model unit is in feet
        if model_unit == "feet":
            xds_clipped_reproject = xds_clipped_reproject * 0.3048

        # change the depth values to integers representing 1/10th interval steps
        # Example - a cell value of 25 = a depth of 2.5 units (feet or meters)
        xds_clipped_reproject_scaled = ((xds_clipped_reproject * 10) + 0.5) // 1

        # set the n/a cells to a value of 65535
        xds_clipped_reproject_scaled = xds_clipped_reproject_scaled.fillna(65535)

        # set the nodata value to 65535 - InFRM compliant
        xds_clipped_reproject_scaled = xds_clipped_reproject_scaled.rio.set_nodata(65535)

        # TODO: Sept 9, 2023
        # The plan was to resample the scalled to the desired resolution (ie. 10)
        # But an upgrade in rioxarray means merge is no longer available (ish).
        # It may have been used incorrectly as merge_arrays implies two datasets
        # but there was only one.

        # For now.. just skip attempting to upscale or downscale the resolution
        # Note: If upscaling or downscaling, image sizes need to be adjusted.

        """
        # using the merge on a single raster to allow for user supplied
        # raster resolution of the output
        #xds_clipped_desired_res = rxr.merge.merge_arrays(
        #    xds_clipped_reproject_scaled, res=(flt_desired_res), nodata=(65535)
        #)

        # compress and write out raster as unsigned 16 bit integer - InFRM compliant
        #xds_clipped_desired_res.rio.to_raster(str_file_to_create_path, compress="lzw", dtype="uint16")
        """

        xds_clipped_reproject_scaled.rio.to_raster(str_file_to_create_path, compress="lzw", dtype="uint16")

        sleep(0.01)  # this allows the tqdm progress bar to update

        return 1


# -------------------------------------------------
def fn_simplify_fim_rasters(r2f_hecras_outputs_dir, flt_resolution, str_output_crs, model_unit):
    flt_start_simplify_fim = time.time()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|                SIMPLIFIED GEOTIFFS FOR RAS2FIM                  |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    STR_MAP_OUTPUT = r2f_hecras_outputs_dir
    RLOG.lprint("  ---(i) INPUT PATH (HEC-RAS outputs): " + str(STR_MAP_OUTPUT))
    RLOG.lprint("  --- HEC-RAS outputs unit: " + model_unit)

    # output interval of the flood depth raster - in CRS units
    FLT_DESIRED_RES = flt_resolution
    RLOG.lprint("  ---[r]   Optional: RESOLUTION: " + str(FLT_DESIRED_RES) + "m")

    # requested tile size in lambert units (meters)
    STR_OUTPUT_CRS = str_output_crs
    RLOG.lprint("  ---[p]   Optional: PROJECTION OF OUTPUT: " + str(STR_OUTPUT_CRS))

    RLOG.lprint("===================================================================")

    # create the base 06_metric path
    # change the 05 path to the 06 path and make it
    metric_folder = STR_MAP_OUTPUT.replace(sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT, sv.R2F_OUTPUT_DIR_METRIC)
    os.makedirs(metric_folder, exist_ok=True)

    # get a list of all tifs in the provided input directory
    list_raster_dem = fn_filelist(STR_MAP_OUTPUT, (".TIF", ".tif"))

    # create a list of the path to the found tifs
    list_paths = []
    for i in range(len(list_raster_dem)):
        list_paths.append(os.path.split(list_raster_dem[i])[0])

    # list of the unique paths to tif locations
    list_unique_paths = []
    list_unique_paths = fn_unique_list(list_paths)

    # create a blank pandas dataframe
    list_col_names = ["shapefile_path", "file_to_create_path", "current_dem_path"]
    df_grids_to_convert = pd.DataFrame(columns=list_col_names)

    # for regular expression - everything between quotes
    pattern_parenth = "\((.*?)\)"

    for i in list_unique_paths:
        list_current_tifs = []

        # get a list of all the tifs in the current path
        list_current_tifs = fn_filelist(i, (".TIF", ".tif"))

        b_create_dems = False

        # create a list of all the paths for these tifs
        list_check_path = []
        for j in list_current_tifs:
            list_check_path.append(os.path.split(j)[0])

        # determine if the path to all the tifs are the same (i.e. no nesting)
        list_check_path_unique = fn_unique_list(list_check_path)
        if len(list_check_path_unique) == 1:
            b_create_dems = True

            # determine the COMID
            list_path_parts = list_check_path_unique[0].split(os.sep)
            str_current_comid = list_path_parts[-3]

            # directory to create output dem's
            first_part = "\\".join(list_path_parts[:-5])
            last_part = "\\".join(list_path_parts[-4:-2])
            str_folder_to_create = (
                first_part
                + "\\"
                + sv.R2F_OUTPUT_DIR_METRIC
                + "\\"
                + sv.R2F_OUTPUT_DIR_SIMPLIFIED_GRIDS
                + "\\"
                + last_part
            )
            os.makedirs(str_folder_to_create, exist_ok=True)

            # Path for the depth grid tifs are: e.g
            #    \06_metric\Depth_Grid\HUC_120401010302\1466236\1466236-1.tif (and more tifs)

            # It's twin rating curve from its path in 05 are. e.g)
            #    \05_hecras_output\HUC_120401010302\1466236\Rating_Curve\1466236_rating_curve.csv

        else:
            str_current_comid = ""
            # there are nested TIFs, so don't process

        if b_create_dems:
            # determine the path to the boundary shapefile
            list_shp_path = []
            list_shp_path = fn_filelist(i, (".SHP", ".shp"))

            if len(list_shp_path) == 1:
                str_shp_file_path = list_shp_path[0]

                for k in list_current_tifs:
                    # parse the step of the grid contained in () on filename
                    str_file_name = os.path.split(k)[1]
                    str_dem_name = re.search(pattern_parenth, str_file_name).group(1)

                    # parse to digits only - replaces anythign that is not a number
                    str_dem_digits_only = re.sub("[^0-9]", "", str_dem_name)

                    # remove leading zeros
                    if str_dem_digits_only[0] == "0":
                        str_dem_digits_only = str_dem_digits_only[1:]

                    # which assumes a level of precision of from the file name
                    # of 1 (only one decimal after the dot).
                    # TODO: re-address precision here ??
                    stage_increment_val = float(str_dem_digits_only) / 10

                    # make sure to update the name of the file to be in millimeter
                    # (the "str_dem_digits_only" is 10 times the actual stage, so
                    # first needs to be divided by 10)
                    if model_unit == "feet":
                        stage_m = np.round(stage_increment_val * 0.3048, 3)
                        stage_mm = int(stage_m * 1000)
                        str_dem_digits_only = str(int(stage_mm))
                    else:
                        stage_m = np.round(stage_increment_val, 3)
                        stage_mm = int(stage_m * 1000)
                        str_dem_digits_only = str(int(stage_mm))

                    # file path to write file

                    str_create_filename = (
                        str_folder_to_create + "\\" + str_current_comid + "-" + str_dem_digits_only + ".tif"
                    )

                    # create the converted grids
                    new_rec = {
                        "current_dem_path": k,
                        "file_to_create_path": str_create_filename,
                        "shapefile_path": str_shp_file_path,
                    }
                    df_new_row = pd.DataFrame.from_records([new_rec])
                    df_grids_to_convert = pd.concat([df_grids_to_convert, df_new_row], ignore_index=True)
            # else: # no shapefile in the dem path found

    len_grids_convert = len(df_grids_to_convert)
    if len_grids_convert > 0:
        # add additional columns to the pandas dataframe prior to passing to the
        # multi-processing
        df_grids_to_convert["output_crs"] = STR_OUTPUT_CRS
        df_grids_to_convert["desired_res"] = FLT_DESIRED_RES
        df_grids_to_convert["model_unit"] = model_unit

        list_dataframe_args = df_grids_to_convert.values.tolist()

        p = mp.Pool(processes=(mp.cpu_count() - 2))

        list(
            tqdm.tqdm(
                p.imap(fn_create_grid, list_dataframe_args),
                total=len_grids_convert,
                desc="Convert Grids",
                bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
                ncols=65,
            )
        )

        p.close()
        p.join()

    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Making metric rating curve files")
    all_rating_files = list(pathlib.Path(r2f_hecras_outputs_dir).rglob("*rating_curve.csv"))
    all_rating_curve_df = pd.DataFrame()
    for file in all_rating_files:
        featureid = file.name.split("_rating_curve.csv")[0]

        # read current file (for both metric and U.S. unit fields) and add feature id into it
        this_file_df = pd.read_csv(file)
        this_file_df["feature_id"] = featureid

        # build the new path to folder 06_metric
        list_path_parts = str(file).split(os.sep)
        file_name = list_path_parts[-1]
        first_part = "\\".join(list_path_parts[:-5])
        last_part = "\\".join(list_path_parts[-4:-2])
        str_folder_to_create = (
            first_part
            + "\\"
            + sv.R2F_OUTPUT_DIR_METRIC
            + "\\"
            + sv.R2F_OUTPUT_DIR_METRIC_RATING_CURVES
            + "\\"
            + last_part
        )

        # first make a folder and then save the csv file inside that
        os.makedirs(str_folder_to_create, exist_ok=True)
        this_file_df.to_csv(os.path.join(str_folder_to_create, file_name), index=False)

        # also combine all files into a single file
        all_rating_curve_df = pd.concat([all_rating_curve_df, this_file_df])

    r2f_metric_dir = r2f_hecras_outputs_dir.replace(sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT, sv.R2F_OUTPUT_DIR_METRIC)
    all_rating_curve_df.to_csv(os.path.join(r2f_metric_dir, "all_rating_curves.csv"), index=False)

    RLOG.lprint("Making metric wse for cross sections")
    all_xs_files = list(pathlib.Path(r2f_hecras_outputs_dir).rglob("*cross_sections.csv"))
    all_xs_df = pd.DataFrame()
    for file in all_xs_files:
        # read entire current file (for both metric and U.S. unit fields) note that the
        # files already have feature id.
        this_file_df = pd.read_csv(file)

        # build the new path to folder 06_metric
        list_path_parts = str(file).split(os.sep)
        file_name = list_path_parts[-1]
        first_part = "\\".join(list_path_parts[:-5])
        last_part = "\\".join(list_path_parts[-4:-2])
        str_folder_to_create = (
            first_part
            + "\\"
            + sv.R2F_OUTPUT_DIR_METRIC
            + "\\"
            + sv.R2F_OUTPUT_DIR_METRIC_CROSS_SECTIONS
            + "\\"
            + last_part
        )

        # first make a folder and then save the csv file inside that
        os.makedirs(str_folder_to_create, exist_ok=True)
        this_file_df.to_csv(os.path.join(str_folder_to_create, file_name), index=False)

        # also combine all files into a single file
        all_xs_df = pd.concat([all_xs_df, this_file_df])

    r2f_metric_dir = r2f_hecras_outputs_dir.replace(sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT, sv.R2F_OUTPUT_DIR_METRIC)
    all_xs_df.to_csv(os.path.join(r2f_metric_dir, "all_cross_sections.csv"), index=False)

    RLOG.success("COMPLETE")

    flt_end_simplify_fim = time.time()
    flt_time_simplify_fim = (flt_end_simplify_fim - flt_start_simplify_fim) // 1
    time_pass_simplify_fim = datetime.timedelta(seconds=flt_time_simplify_fim)
    RLOG.lprint("Compute Time: " + str(time_pass_simplify_fim))

    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample (min args)
    # python simplify_fim_rasters.py
    #  -i c:\ras2fim_data\output_ras2fim\12030105_2276_231024\05_hecras_output

    parser = argparse.ArgumentParser(
        description="===== CREATE SIMPLIFIED FLOOD DEPTH RASTER FILES (TIF) ====="
    )

    parser.add_argument(
        "-i",
        dest="r2f_huc_parent_dir",
        help="REQUIRED: The path to the parent folder containing the ras2fim outputs."
        ' Output is created in sub-folders "06_Metric/Depth_Grid" ',
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-r",
        dest="flt_resolution",
        help="OPTIONAL: resolution of output raster (crs units): Default=10",
        required=False,
        default=10,
        metavar="int",
        type=int,
    )

    parser.add_argument(
        "-p",
        dest="str_output_crs",
        help="OPTIONAL: output coordinate reference zone:" f" Default={sv.DEFAULT_RASTER_OUTPUT_CRS}",
        required=False,
        default=sv.DEFAULT_RASTER_OUTPUT_CRS,
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-u",
        dest="model_unit",
        help="OPTIONAL: need to be the value of feet or meter",
        required=False,
        default="feet",
        metavar="STRING",
        type=str,
    )

    args = vars(parser.parse_args())

    r2f_huc_parent_dir = args["r2f_huc_parent_dir"]
    flt_resolution = args["flt_resolution"]
    str_output_crs = args["str_output_crs"]
    model_unit = args["model_unit"]

    log_file_folder = args["r2f_huc_parent_dir"]
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

        # find model_unit of HEC-RAS outputs (ft vs m) using a sample rating curve file
        r2f_hecras_outputs_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
        # model_unit = sf.find_model_unit_from_rating_curves(r2f_hecras_outputs_dir)

        # call main program
        fn_simplify_fim_rasters(r2f_hecras_outputs_dir, flt_resolution, str_output_crs, model_unit)

    except Exception:
        RLOG.critical(traceback.format_exc())
