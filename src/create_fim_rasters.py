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
import traceback
from functools import partial
from multiprocessing import Pool

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
    unit_output_folder, #str_output_filepath: C:\ras2fim_v2_output\12090301_2277_240109
    model_unit,
#    is_verbose=False,
    ):

    # TODO: Oct 25, 2023, continue with adding the "is_verbose" system
    start_dt = dt.datetime.utcnow()

    # Constant - number of flood depth profiles to run on the first pass
    int_fn_starting_flow = 1  # cfs

    # Constant - Starting flow for the first pass of the HEC-RAS simulation
    int_number_of_steps = 76

    RLOG.lprint("  ---(w) HUC-8 WATERSHED: " + huc8_num)

    RLOG.lprint("  ---(o) OUTPUT PATH: " + unit_output_folder)

    RLOG.lprint("===================================================================")

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|               CREATING CONFLATED HEC-RAS MODELS                 |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    worker_fim_rasters.create_hecras_files(
        huc8_num,
        int_fn_starting_flow,
        int_number_of_steps,
        unit_output_folder,
        model_unit,
    )   

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|              PROCESSING CONFLATED HEC-RAS MODELS                |")
    RLOG.lprint("|          AND CREATING DEPTH GRIDS FOR HEC-RAS STREAMS           |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    path_created_ras_models = os.path.join(unit_output_folder, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)

    names_created_ras_models = os.listdir(path_created_ras_models)

    print(names_created_ras_models)
    log_file_prefix = "fn_run_hecras"
    ls_run_hecras_inputs = []
    for model_folder in names_created_ras_models:

        folder_mame_splt = model_folder.split("_")
        project_file_name = folder_mame_splt[1]

        str_ras_projectpath = os.path.join(path_created_ras_models, model_folder, project_file_name + ".prj")

        run_hecras_inputs = [str_ras_projectpath, 
                             int_number_of_steps,
                             model_folder,
                             unit_output_folder,
                             RLOG.LOG_DEFAULT_FOLDER,
                             log_file_prefix
                             ]
        ls_run_hecras_inputs.append(run_hecras_inputs)

    
    # 
    # fn_main_hecras_partial = #partial(
    #     fn_run_one_ras_model, RLOG.LOG_DEFAULT_FOLDER, log_file_prefix
    #     )
    # create a pool of processors
    num_processors = mp.cpu_count() - 2
    with Pool(processes=num_processors) as executor:
        
        len_points_agg = len(ls_run_hecras_inputs)
        tqdm.tqdm(
            executor.imap(worker_fim_rasters.fn_run_one_ras_model, ls_run_hecras_inputs),
            total=len_points_agg,
            desc="Number of Processed RAS Models",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
            ncols=67,
        )

    # pool.close()
    # pool.join()

    # Now that multi-proc is done, lets merge all of the independent log file from each
    RLOG.merge_log_files(RLOG.LOG_FILE_PATH, log_file_prefix)

    RLOG.lprint("")
    RLOG.success("STEP 5 COMPLETE")

    dur_msg = sf.print_date_time_duration(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)

    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample
    # python create_fim_rasters.py -w 12090301 -u feet
    #  -o c:\ras2fim_data\output_ras2fim\12090301_2276_240108\05_hecras_output

    parser = argparse.ArgumentParser(
        description="================ NWM RASTER LIBRARY FROM HEC-RAS =================="
    )

    parser.add_argument(
        "-w",
        dest="str_huc8_arg",
        help=r"REQUIRED: Desired huc-8 watershed: Example:  12090301",
        required=True,
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-u",
        dest="model_unit",
        help=r"REQUIRED: HEC-RAS models unit: Example:  feet",
        required=True,
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="unit_output_folder",
        help=r"REQUIRED: path to write ras2fim output files: Example: C:\ras2fim_12090301",
        required=True,
        metavar="DIR",
        type=str,
    )

    args = vars(parser.parse_args())
    # --------------------------------

    str_huc8_arg = args["str_huc8_arg"]
    model_unit = args["model_unit"]
    unit_output_folder = args["unit_output_folder"]

    log_file_folder = args["unit_output_folder"]
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
            str_huc8_arg,
            unit_output_folder,
            model_unit,
        #    is_verbose,
        )

    except Exception:
        RLOG.critical(traceback.format_exc())
