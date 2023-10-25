# Create flood inundation data from HEC-RAS
#
# Purpose:
# Create flood inundation rasters and supporting InFRM data from the
# preprocessed HEC-RAS geospatial 1D data.  This creates data per
# feature-id for the National Water Model
#
# Created by: Andy Carter, PE
# Created: 2021-08-12
# Last revised - 2021.10.24
#
# Uses the 'ras2fim' conda environment
# ************************************************************
import argparse
import datetime as dt
import multiprocessing as mp
import os
import sys
import traceback
from multiprocessing import Pool

import geopandas as gpd
import pandas as pd

import ras2fim_logger
import shared_functions as sf
import worker_fim_rasters


# Global Variables
RLOG = ras2fim_logger.RAS2FIM_logger()


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
def fn_create_fim_rasters(
    str_desired_huc8,
    str_input_folder,
    str_output_folder,
    str_projection_path,
    str_terrain_path,
    str_std_input_path,
    flt_interval,
    b_terrain_check_only,
    is_verbose=False,
):
    # TODO: Oct 25, 2023, continue with adding the "is_verbose" system
    start_dt = dt.datetime.utcnow()

    # Hard coded constants for this routine

    INT_XS_BUFFER = 2  # Number of XS to add upstream and downstream
    # of the segmented

    # Constant - Toggle the Creation of RAS Map products
    IS_CREATE_MAPS = True

    # Constant - number of flood depth profiles to run on the first pass
    INT_NUMBER_OF_STEPS = 75

    # Constant - Starting flow for the first pass of the HEC-RAS simulation
    INT_STARTING_FLOW = 1

    # Constant - Maximum flow multiplier
    # up-scales the maximum flow from input
    FLT_MAX_MULTIPLY = 1.2

    # Constant - buffer of dem around floodplain envelope
    FLT_BUFFER = 15

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|                NWM RASTER LIBRARY FROM HEC-RAS                  |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    STR_HUC8 = str_desired_huc8
    RLOG.lprint("  ---(w) HUC-8 WATERSHED: " + STR_HUC8)

    STR_INPUT_FOLDER = str_input_folder
    RLOG.lprint("  ---(i) INPUT PATH: " + STR_INPUT_FOLDER)

    STR_ROOT_OUTPUT_DIRECTORY = str_output_folder
    RLOG.lprint("  ---(o) OUTPUT PATH: " + STR_ROOT_OUTPUT_DIRECTORY)

    STR_PATH_TO_PROJECTION = str_projection_path
    RLOG.lprint("  ---(p) PROJECTION PATH: " + STR_PATH_TO_PROJECTION)

    STR_PATH_TO_TERRAIN = str_terrain_path
    RLOG.lprint("  ---(t) TERRAIN PATH: " + STR_PATH_TO_TERRAIN)

    STR_PATH_TO_STANDARD_INPUT = str_std_input_path
    RLOG.lprint("  ---[s]   Optional: Standard Input Path: " + STR_PATH_TO_STANDARD_INPUT)

    # Path to the standard plan file text
    STR_PLAN_MIDDLE_PATH = STR_PATH_TO_STANDARD_INPUT + r"\PlanStandardText01.txt"
    STR_PLAN_FOOTER_PATH = STR_PATH_TO_STANDARD_INPUT + r"\PlanStandardText02.txt"
    STR_PROJECT_FOOTER_PATH = STR_PATH_TO_STANDARD_INPUT + r"\ProjectStandardText01.txt"

    FLT_INTERVAL = flt_interval
    RLOG.lprint("  ---[z]   Optional: Output Elevation Step: " + str(FLT_INTERVAL))
    RLOG.lprint("  ---[c]   Optional: Terrain Check Only: " + str(b_terrain_check_only))

    RLOG.lprint("===================================================================")

    # "" is just a filler (for an old redundant parameter) simply to keep the order of item unchanged.
    # TODO: Oct 25, 2023 - complete the is_verbose system
    tpl_input = (
        STR_HUC8,
        STR_INPUT_FOLDER,
        STR_ROOT_OUTPUT_DIRECTORY,
        STR_PATH_TO_PROJECTION,
        STR_PATH_TO_TERRAIN,
        STR_PLAN_MIDDLE_PATH,
        STR_PROJECT_FOOTER_PATH,
        FLT_INTERVAL,
        "",
        INT_XS_BUFFER,
        IS_CREATE_MAPS,
        INT_NUMBER_OF_STEPS,
        INT_STARTING_FLOW,
        FLT_MAX_MULTIPLY,
        FLT_BUFFER,
        STR_PLAN_FOOTER_PATH,
        b_terrain_check_only,
        is_verbose,
    )

    list_huc8 = []
    list_huc8.append(STR_HUC8)

    str_stream_csv = STR_INPUT_FOLDER + "\\" + str(list_huc8[0]) + "_stream_qc.csv"

    str_stream_nwm_ln_shp = STR_INPUT_FOLDER + "\\" + str(list_huc8[0]) + "_nwm_streams_ln.shp"
    str_huc12_area_shp = STR_INPUT_FOLDER + "\\" + str(list_huc8[0]) + "_huc_12_ar.shp"

    # read the two dataframes
    df_streams = gpd.read_file(str_stream_csv)
    gdf_streams = gpd.read_file(str_stream_nwm_ln_shp)

    # convert the df_stream 'feature_id' to int64
    df_streams = df_streams.astype({"feature_id": "int64"})

    # left join on feature_id
    df_streams_merge = pd.merge(df_streams, gdf_streams, on="feature_id")

    # limit the fields
    df_streams_merge_2 = df_streams_merge[
        ["feature_id", "reach", "us_xs", "ds_xs", "peak_flow", "ras_path_x", "huc12"]
    ]

    # rename the ras_path_x column to ras_path
    df_streams_merge_2 = df_streams_merge_2.rename(columns={"ras_path_x": "ras_path"})

    # add the settings tuple
    df_streams_merge_2["settings"] = ""
    df_streams_merge_2["settings"] = df_streams_merge_2["settings"].astype(object)

    for index, row in df_streams_merge_2.iterrows():
        df_streams_merge_2.at[index, "settings"] = tpl_input

    # create a pool of processors
    num_processors = mp.cpu_count() - 2
    with Pool(processes=num_processors) as executor:
        df_huc12 = gpd.read_file(str_huc12_area_shp)
        int_huc12_index = 0

        len_df_huc12 = len(df_huc12)
        str_prefix = r"Processing HUC12s (0 of " + str(len_df_huc12) + "):"
        fn_print_progress_bar(0, len_df_huc12, prefix=str_prefix, suffix="Complete", length=27)

        # Loop through each HUC-12
        for i in df_huc12.index:
            str_huc12 = str(df_huc12["HUC_12"][i])
            int_huc12_index += 1
            # print(str_huc12)
            str_prefix = r"Processing HUC12s (" + str(int_huc12_index) + " of " + str(len_df_huc12) + "):"
            fn_print_progress_bar(
                int_huc12_index, len_df_huc12, prefix=str_prefix, suffix="Complete", length=27
            )

            # Constant - Folder to write the HEC-RAS folders and files
            str_root_folder_to_create = STR_ROOT_OUTPUT_DIRECTORY + "\\HUC_" + str_huc12

            # Select all the 'feature_id' in a given huc12
            df_streams_huc12 = df_streams_merge_2.query("huc12 == @str_huc12")

            # Reset the query index
            df_streams_huc12 = df_streams_huc12.reset_index()

            # Create a folder for the HUC-12 area
            os.makedirs(str_root_folder_to_create, exist_ok=True)

            # amend the pandas dataframe
            df_streams_huc12_mod1 = df_streams_huc12[
                ["feature_id", "us_xs", "ds_xs", "peak_flow", "ras_path", "huc12", "settings"]
            ]

            # create a list of lists from the dataframe
            list_of_lists_df_streams = df_streams_huc12_mod1.values.tolist()

            try:
                if len(list_of_lists_df_streams) > 0:
                    executor.map(worker_fim_rasters.fn_main_hecras, list_of_lists_df_streams)
            except Exception as pex:
                # It has already been logged in fn_main_hecras
                RLOG.critical(pex)
                executor.terminate()
                RLOG.critical("Pool terminated")
                sys.exit(1)

    tif_count = 0
    for root, dirs, files in os.walk(STR_ROOT_OUTPUT_DIRECTORY):
        for file in files:
            if file.endswith(".tif"):
                tif_count += 1

    RLOG.lprint("")
    RLOG.lprint("ALL AREAS COMPLETE")
    RLOG.lprint("Number of tif's generated: " + str(tif_count))

    dur_msg = sf.print_date_time_duration(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)

    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # TODO: Oct 23, 2023.
    # Research is required. Comparing a test run against a small set of models, it took
    # appx 32 mins to process via ras2fim, but through command line it took 6 mins.
    # Output file count appears to be the same but hash compare will be required to see
    # if the output files are really the same (not.. comparision has to be done on the
    # same day as date stamps can be embedded in files which can throw off the hash compare)
    # This appears to be a previously existing problem.
    # Watch for outputs from step 5c, calculate terrain stats as it goes to the 06 folder as well

    parser = argparse.ArgumentParser(
        description="================ NWM RASTER LIBRARY FROM HEC-RAS =================="
    )

    parser.add_argument(
        "-w",
        dest="str_desired_huc8",
        help=r"REQUIRED: the desired huc-8 watershed: Example:  12090301",
        required=True,
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-i",
        dest="str_input_folder",
        help="REQUIRED: directory containing results of conflation (step 2):"
        r" Example: C:\ras2fim_12090301\02_shapes_from_conflation",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="str_output_folder",
        help=r"REQUIRED: path to write ras2fim output files: Example: C:\ras2fim_12090301\05_hecras_output",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-p",
        dest="str_projection_path",
        help="REQUIRED: path the to the projection file: Example: "
        r"C:\ras2fim_12090301\02_shapes_from_conflation\12090301_huc_12_ar.prj",
        required=True,
        metavar="FILE",
        type=str,
    )

    parser.add_argument(
        "-t",
        dest="str_terrain_path",
        help=r"REQUIRED: path the to hdf5 terrain folder: Example: C:\ras2fim_12090301\04_hecras_terrain",
        required=True,
        metavar="FILE",
        type=str,
    )

    parser.add_argument(
        "-s",
        dest="str_std_input_path",
        help="  OPTIONAL: path the to the standard inputs:"
        r" Example: C:\Users\civil\test1\ras2fim\src : Default: working directory",
        required=False,
        default=os.getcwd(),
        metavar="FILE",
        type=str,
    )

    parser.add_argument(
        "-z",
        dest="flt_interval",
        help=r"  OPTIONAL: elevation interval of output grids: Example: 0.2 : Default: 0.5",
        required=False,
        default=0.5,
        metavar="FLOAT",
        type=float,
    )

    parser.add_argument(
        "-c",
        dest="b_terrain_check_only",
        help="OPTIONAL: check terrain only-skip HEC-RAS simulation and mapping: Default=False",
        required=False,
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-v",
        "--is_verbose",
        help="OPTIONAL: Adding this flag will give additional tracing output."
        "Default = False (no extra output)",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())
    # --------------------------------

    str_desired_huc8 = args["str_desired_huc8"]
    str_input_folder = args["str_input_folder"]
    str_output_folder = args["str_output_folder"]
    str_projection_path = args["str_projection_path"]
    str_terrain_path = args["str_terrain_path"]
    str_std_input_path = args["str_std_input_path"]
    flt_interval = args["flt_interval"]
    b_terrain_check_only = args["b_terrain_check_only"]
    is_verbose = args["is_verbose"]

    log_file_folder = args["str_output_folder"]
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]

        # Assumes RLOG has been added as a global var.
        RLOG.setup(log_file_folder, script_file_name + ".log")

        # call main program
        fn_create_fim_rasters(
            str_desired_huc8,
            str_input_folder,
            str_output_folder,
            str_projection_path,
            str_terrain_path,
            str_std_input_path,
            flt_interval,
            b_terrain_check_only,
            is_verbose,
        )

    except Exception:
        if ras2fim_logger.LOG_SYSTEM_IS_SETUP is True:
            ras2fim_logger.logger.critical(traceback.format_exc())
        else:
            print(traceback.format_exc())
