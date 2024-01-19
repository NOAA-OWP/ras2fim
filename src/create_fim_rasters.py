# Create flood inundation data from HEC-RAS
#
# Purpose:
# Create flood inundation rasters and supporting InFRM data from the
# preprocessed HEC-RAS geospatial 1D data.  This creates data per
# feature-id for the National Water Model

# Uses the 'ras2fim' conda environment
# ************************************************************
import argparse
import datetime as dt
import multiprocessing as mp
import os
import shutil
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import shared_functions as sf
import shared_variables as sv
import worker_fim_rasters


# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def fn_create_fim_rasters(
    huc8_num,
    unit_output_folder,  # str_output_filepath: C:\ras2fim_v2_output\12090301_2277_240109
    model_unit,
    #    is_verbose=False,
):
    # TODO: Oct 25, 2023, continue with adding the "is_verbose" system
    start_dt = dt.datetime.utcnow()

    path_created_ras_models = os.path.join(unit_output_folder, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)

    # Remove it so it is perfectly clean, no residue from previous runs.
    if os.path.exists(path_created_ras_models):
        shutil.rmtree(path_created_ras_models)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(1)  # 1 seconds

    # re-created later

    # Constant - number of flood depth profiles to run on the first pass
    int_fn_starting_flow = 1  # cfs

    # Constant - Starting flow for the first pass of the HEC-RAS simulation
    int_number_of_steps = 76

    RLOG.lprint("===================================================================")
    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.notice("|               CREATING CONFLATED HEC-RAS MODELS                 |")
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("  ---(w) HUC-8 WATERSHED: " + huc8_num)
    RLOG.lprint("  ---(o) OUTPUT PATH: " + unit_output_folder)

    worker_fim_rasters.create_hecras_files(
        huc8_num, int_fn_starting_flow, int_number_of_steps, unit_output_folder, model_unit
    )
    RLOG.lprint("*** All HEC-RAS Models Created ***")
    RLOG.lprint("")
    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.notice("|              PROCESSING CONFLATED HEC-RAS MODELS                |")
    RLOG.notice("|          AND CREATING DEPTH GRIDS FOR HEC-RAS STREAMS           |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    names_created_ras_models = os.listdir(path_created_ras_models)

    log_file_prefix = "fn_run_hecras"

    ls_run_hecras_inputs = []
    ctr = 0
    for model_folder in names_created_ras_models:
        folder_mame_splt = model_folder.split("_")
        project_file_name = folder_mame_splt[1]

        str_ras_projectpath = os.path.join(path_created_ras_models, model_folder, project_file_name + ".prj")

        run_hecras_inputs = {
            'str_ras_projectpath': str_ras_projectpath,
            'int_number_of_steps': int_number_of_steps,
            'model_folder': model_folder,
            'unit_output_folder': unit_output_folder,
            'log_default_folder': RLOG.LOG_DEFAULT_FOLDER,
            'log_file_prefix': log_file_prefix,
            'index_number': ctr,
            'total_number_models': len(names_created_ras_models),
        }

        ls_run_hecras_inputs.append(run_hecras_inputs)
        ctr += 1

    # create a pool of processors
    num_processors = mp.cpu_count() - 2
    import sys

    with ProcessPoolExecutor(max_workers=num_processors) as executor:
        executor_dict = {}
        for dicts in ls_run_hecras_inputs:
            try:
                future = executor.submit(worker_fim_rasters.fn_run_one_ras_model, **dicts)
                executor_dict[future] = dicts['model_folder']
            except Exception:
                RLOG.critical(traceback.format_exc())
                sys.exit(1)

    # Now that multi-proc is done, lets merge all of the independent log file from each
    RLOG.merge_log_files(RLOG.LOG_FILE_PATH, log_file_prefix)

    RLOG.lprint("")
    RLOG.success(" STEP 5 COMPLETE ")

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

    log_file_folder = os.path.join(args["unit_output_folder"], "logs")
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
