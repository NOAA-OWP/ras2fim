import argparse
import datetime
import multiprocessing as mp
import os
import re
import shutil
import sys
import time
import traceback
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
import tqdm
from rasterio.merge import merge

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = ras2fim_logger.RAS2FIM_logger()


# -------------------------------------------------
def fn_make_rating_curve(r2f_hecras_outputs_dir, r2f_ras2rem_dir, model_unit):
    """
    Args:
        r2f_hecras_outputs_dir: directory containing HEC-RAS outputs
        r2f_ras2rem_dir: directory to write output file (rating_curve.csv)
        model_unit :model unit of HEC-RAS models that is either meter or feet

    Returns: rating_curve.csv file
    """
    print("Making merged rating curve")
    rating_curve_df = pd.DataFrame()

    all_rating_files = list(Path(r2f_hecras_outputs_dir).rglob("*rating_curve.csv"))
    if len(all_rating_files) == 0:
        msg = "Error: Make sure you have specified a correct input directory with at least one"
        "'*.rating curve.csv' file."
        RLOG.critical(msg)
        sys.exit(1)

    for file in all_rating_files:
        featureid = file.name.split("_rating_curve.csv")[0]
        this_file_df = pd.read_csv(file)
        this_file_df["feature_id"] = featureid

        # add data that works with the existing inundation hydro table format requirements
        this_file_df["HydroID"] = featureid

        # assumes the filename and folder structure stays the same
        huc = re.search(r"HUC_(\d{8})", str(file))[1]
        this_file_df["HUC"] = huc
        this_file_df["LakeID"] = -999
        this_file_df["last_updated"] = ""
        this_file_df["submitter"] = ""
        this_file_df["obs_source"] = ""
        rating_curve_df = pd.concat([rating_curve_df, this_file_df])

    # reorder columns
    rating_curve_df = rating_curve_df[
        [
            "feature_id",
            "stage_m",
            "discharge_cms",
            "HydroID",
            "HUC",
            "LakeID",
            "last_updated",
            "submitter",
            "obs_source",
        ]
    ]

    rating_curve_df.to_csv(os.path.join(r2f_ras2rem_dir, "rating_curve.csv"), index=False)


# -------------------------------------------------
def fn_generate_tif_for_each_rem(tpl_request):
    rem_value = tpl_request[0]
    Input_dir = tpl_request[1]
    Output_dir = tpl_request[2]

    # all_tif_files=glob.glob(Input_dir + "*.tif", recursive=True)
    all_tif_files = list(Path(Input_dir).rglob("*.tif"))
    raster_to_mosiac = []
    this_rem_tif_files = [
        file for file in all_tif_files if os.path.basename(file).endswith("-%s.tif" % rem_value)
    ]
    for p in this_rem_tif_files:
        raster = rasterio.open(p)
        raster_to_mosiac.append(raster)
    mosaic, output = merge(raster_to_mosiac)

    # replace values of the raster with rem value, assuming there is no chance of having negative values
    mosaic = np.where(mosaic != raster.nodata, np.float64(rem_value) / 10, raster.nodata)

    # prepare meta data
    output_meta = raster.meta.copy()
    output_meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": output,
            "dtype": rasterio.float64,
            "compress": "LZW",
        }
    )
    with rasterio.open(
        os.path.join(Output_dir, "{}_rem.tif".format(rem_value)), "w", **output_meta
    ) as tiffile:
        tiffile.write(mosaic)
    return rem_value


# -------------------------------------------------
def fn_make_rems(r2f_simplified_grids_dir, r2f_ras2rem_dir):
    """
    Args:
        r2f_simplified_grids_dir: directory containing simplified grids
        r2f_ras2rem_dir: directory to write output rem.tif file

    Returns: rem.tif file
    """

    all_tif_files = list(Path(r2f_simplified_grids_dir).rglob("*.tif"))
    if len(all_tif_files) == 0:
        msg = "Error: Make sure you have specified a correct input directory with at"
        "least one '*.tif' file."
        RLOG.critical(msg)
        sys.exit(1)

    rem_values = list(map(lambda var: str(var).split(".tif")[0].split("-")[-1], all_tif_files))
    rem_values = np.unique(rem_values).tolist()

    print("+-----------------------------------------------------------------+")
    print("Making %d tif files for %d rem values" % (len(rem_values), len(rem_values)))
    # make argument for multiprocessing
    rem_info_arguments = []
    for rem_value in rem_values:
        rem_info_arguments.append((rem_value, r2f_simplified_grids_dir, r2f_ras2rem_dir))

    num_processors = mp.cpu_count() - 2
    with Pool(processes=num_processors) as executor:
        # pool = Pool(processes = num_processors)
        list(
            tqdm.tqdm(
                executor.imap(fn_generate_tif_for_each_rem, rem_info_arguments),
                total=len(rem_values),
                desc="Creating REMs",
                bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                ncols=67,
            )
        )

    # now make the final rem
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Merging all rem files to create the final rem")

    all_rem_files = list(Path(r2f_ras2rem_dir).rglob("*_rem.tif"))
    raster_to_mosiac = []

    for p in all_rem_files:
        raster = rasterio.open(p)
        raster_to_mosiac.append(raster)
    mosaic, output = merge(raster_to_mosiac, method="min")

    output_meta = raster.meta.copy()
    output_meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": output,
            "compress": "LZW",
        }
    )

    with rasterio.open(os.path.join(r2f_ras2rem_dir, "rem.tif"), "w", **output_meta) as tiffile:
        tiffile.write(mosaic)

    # finally delete unnecessary files to clean up
    for raster in raster_to_mosiac:
        raster.close()

    for p in all_rem_files:
        os.remove(p)


# -------------------------------------------------
def fn_run_ras2rem(r2f_huc_parent_dir, model_unit):
    ####################################################################
    # Input validation and variable setup

    # The subfolders like 05_ and 06_ are referential from here.
    # -o  (ie 12090301_meters_2277_test_1) or some full custom path
    # We need to remove the last folder name and validate that the parent paths are valid
    is_invalid_path = False
    if "\\" in r2f_huc_parent_dir:  # submitted a full path
        if os.path.exists(r2f_huc_parent_dir) is False:  # full path must exist
            is_invalid_path = True
    else:  # they provide just a child folder (base path name)
        r2f_huc_parent_dir = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS, r2f_huc_parent_dir)
        if os.path.exists(r2f_huc_parent_dir) is False:  # child folder must exist
            is_invalid_path = True

    if is_invalid_path is True:
        raise ValueError(
            f"The -p arg '{r2f_huc_parent_dir}' folder does not exist. Please check if ras2fim"
            " has been run for the related huc and verify the path."
        )

    # AND the 05 directory must already exist
    r2f_hecras_outputs_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    if os.path.exists(r2f_hecras_outputs_dir) is False:
        raise ValueError(
            f"The {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT} folder does not exist."
            f" Please ensure ras2fim has been run and created a valid {sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT}"
            " folder."
        )

    r2f_simplified_grids_dir = os.path.join(
        r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_METRIC, sv.R2F_OUTPUT_DIR_SIMPLIFIED_GRIDS
    )
    r2f_ras2rem_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_METRIC, sv.R2F_OUTPUT_DIR_RAS2REM)

    if os.path.exists(r2f_ras2rem_dir):
        shutil.rmtree(r2f_ras2rem_dir)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(1)

    os.mkdir(r2f_ras2rem_dir)

    ####################################################################
    #  Start processing
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|                       Run ras2rem                               |")
    RLOG.lprint("  --- (p) RAS2FIM parent output path: " + str(r2f_huc_parent_dir))
    RLOG.lprint("  --- HEC-RAS outputs path: " + str(r2f_hecras_outputs_dir))
    RLOG.lprint("  --- HEC-RAS outputs unit: " + model_unit)
    RLOG.lprint("  --- RAS2FIM 'simplified' depth grids path: " + str(r2f_simplified_grids_dir))
    RLOG.lprint("  --- RAS2REM Outputs directory: " + str(r2f_ras2rem_dir))
    RLOG.lprint("+-----------------------------------------------------------------+")

    flt_start_ras2rem = time.time()

    fn_make_rating_curve(r2f_hecras_outputs_dir, r2f_ras2rem_dir, model_unit)
    fn_make_rems(r2f_simplified_grids_dir, r2f_ras2rem_dir)

    flt_end_ras2rem = time.time()
    flt_time_pass_ras2rem = (flt_end_ras2rem - flt_start_ras2rem) // 1
    time_pass_ras2rem = datetime.timedelta(seconds=flt_time_pass_ras2rem)
    RLOG.lprint("Compute Time: " + str(time_pass_ras2rem))


# -------------------------------------------------
if __name__ == "__main__":
    # Sample usage:
    # Using all defaults:
    #     python run_ras2rem.py -p 12090301_meters_2277_test_22

    #  - The -p arg is required, but can be either a ras2fim models huc folder name (as shown above),
    #        or a fully pathed.
    #        Either way, it must have the 05_hecras_output and it must be populated.
    #
    #        ie) -p c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2
    #            OR
    #            -p 12090301_meters_2277_test_3  (We will use the root default pathing and
    #             become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)

    # There is a known problem with  proj_db error.
    # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db.
    # This will not stop all of the errors but some (in multi-proc).
    sf.fix_proj_path_error()

    parser = argparse.ArgumentParser(description="==== Run RAS2REM ===")

    parser.add_argument(
        "-p",
        dest="r2f_huc_parent_dir",
        help="REQUIRED:"
        "The path to the parent folder containing the ras2fim outputs."
        " The ras2rem results will be created in the folder 06_metric/ras2rem in the same"
        "  parent directory.\n"
        "There are two options:\n"
        " 1) Providing a full path\n"
        " 2) Providing only huc folder name, when following AWS data structure.\n"
        " Please see the embedded notes in the __main__ section of the code for details and examples.",
        required=True,
        metavar="",
        type=str,
    )

    args = vars(parser.parse_args())

    r2f_huc_parent_dir = args["r2f_huc_parent_dir"]

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
        RLOG.setup(log_file_folder, script_file_name + ".log")

        # find model_unit of HEC-RAS outputs (ft vs m) using a sample rating curve file
        r2f_hecras_outputs_dir = os.path.join(r2f_huc_parent_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
        model_unit = sf.find_model_unit_from_rating_curves(r2f_hecras_outputs_dir)

        # call main program
        fn_run_ras2rem(r2f_huc_parent_dir, model_unit)

    except Exception:
        if ras2fim_logger.LOG_SYSTEM_IS_SETUP is True:
            ras2fim_logger.logger.critical(traceback.format_exc())
        else:
            print(traceback.format_exc())
