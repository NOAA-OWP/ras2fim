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
from functools import partial
from multiprocessing import Pool

import geopandas as gpd
import pandas as pd
import tqdm

import shared_functions as sf
import shared_variables as sv
import worker_fim_rasters


# Global Variables
RLOG = sv.R2F_LOG


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
    huc8_num,
    str_output_folder, #str_output_filepath
    model_unit,
    is_verbose=False,
    ):

    # TODO: Oct 25, 2023, continue with adding the "is_verbose" system
    start_dt = dt.datetime.utcnow()

    # Constant - number of flood depth profiles to run on the first pass
    int_fn_starting_flow = 1  # cfs

    # Constant - Starting flow for the first pass of the HEC-RAS simulation
    int_number_of_steps = 76

    RLOG.lprint("  ---(w) HUC-8 WATERSHED: " + huc8_num)

    RLOG.lprint("  ---(o) OUTPUT PATH: " + str_output_folder)

    RLOG.lprint("===================================================================")

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|               CREATING CONFLATED HEC-RAS MODELS                 |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    worker_fim_rasters.create_hecras_files(
        huc8_num,
        int_fn_starting_flow,
        int_number_of_steps,
        str_output_folder,
        model_unit,
    )

   

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|              PROCESSING CONFLATED HEC-RAS MODELS                |")
    RLOG.lprint("|          AND CREATING DEPTH GRIDS FOR HEC-RAS STREAMS           |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    path_created_ras_models = os.path.join(str_output_folder, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)

    names_created_ras_models = os.listdir(path_created_ras_models)
    
    log_file_prefix = "fn_run_hecras"
    fn_main_hecras_partial = partial(
        worker_fim_rasters.fn_run_hecras, RLOG.LOG_DEFAULT_FOLDER, log_file_prefix
    )
    # create a pool of processors
    num_processors = mp.cpu_count() - 2
    with Pool(processes=num_processors) as executor:
        
        for folder in names_created_ras_models:

            folder_mame_splt = folder.split("_")
            project_file_name = folder_mame_splt[1]

            str_ras_projectpath = os.path.join(path_created_ras_models, folder, project_file_name + ".prj")

            list_points_aggregate = [str_ras_projectpath, int_number_of_steps]

            all_x_sections_info = worker_fim_rasters.fn_run_hecras(str_ras_projectpath, int_number_of_steps)

            path_to_all_x_sections_info = os.path.join(str_output_folder,
                                                       sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT,
                                                       folder)

            all_x_sections_info.to_csv(
                os.path.join(path_to_all_x_sections_info, "all_x_sections_info" + "_" + folder + ".csv")
            )

        len_points_agg = len(list_points_aggregate)
        tqdm.tqdm(
            executor.imap(fn_main_hecras_partial, list_points_aggregate),
            total=len_points_agg,
            desc="Points on lines",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
            ncols=67,
        )

    # pool.close()
    # pool.join()

    # Now that multi-proc is done, lets merge all of the independent log file from each
    RLOG.merge_log_files(RLOG.LOG_FILE_PATH, log_file_prefix)

    tif_count = 0
    for root, dirs, files in os.walk(str_output_folder):
        for file in files:
            if file.endswith(".tif"):
                tif_count += 1

    RLOG.lprint("")
    RLOG.success("STEP 5 COMPLETE")

    dur_msg = sf.print_date_time_duration(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)

    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample

    # TODO: RESEARCH REQUIRED. Does the "-c" flag even work?

    # python create_fim_rasters.py -w 12030105
    #  -i c:\ras2fim_data\output_ras2fim\12030105_2276_231024\02_shapes_from_conflation
    #  -o c:\ras2fim_data\output_ras2fim\12030105_2276_231024\05_hecras_output
    #  -p c:\....\12030105_2276_231024\02_shapes_from_conflation\12030105_huc_12_ar.prj
    #  -t c:\ras2fim_data\output_ras2fim\12030105_2276_231024\04_hecras_terrain
    #  -s C:\Users\.....\Documents\NOAA-OWP\Projects\ras2fim\dev-logging\ras2fim\src
    #  -z 0.5  (leave off -c )

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
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        fn_create_fim_rasters(
            str_desired_huc8,
            str_input_folder,
            str_output_folder,
            str_projection_path,
            str_terrain_path,
            flt_interval,
            b_terrain_check_only,
            is_verbose,
        )

    except Exception:
        RLOG.critical(traceback.format_exc())
