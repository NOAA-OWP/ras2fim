import argparse
import datetime as dt
import os
import sys
import traceback


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import shared_validators as val
import shared_variables as sv
from shared_functions import get_date_with_milli, get_stnd_date


# Global Variables
RLOG = sv.R2F_LOG

"""
This tool pulls down the required input files required for a specific HUC from S3.
Some files will need to be pull some files / folders that apply to all HUCs
but some files are HUC specific such as files in he WBD_HUC8 and ras_3dep_HUC8_10m.

But.. before it pulls it down, it checks to see if it is already download and will only download
files it is missing. Unless, the user uses the re-download flag, which means all will be downloaded.

Note: You only need to edit the `s3_unit_download_files.lst` (and likely check it in),
  if you want to change the what needs to be downloaded generally for ras2fim.py.

"""


# -------------------------------------------------
def s3_get_unit_inputs(huc8, do_reload, s3_input_path, trg_input_path):
    """
    Process: See notes above.

    Inputs:
        huc8: 12030101
        do_reload (bool): if True, reload all files regardless if they exist or not
        s3_input_path: s3://ras2fim/input
        trg_input_path: C:\ras2fim_data\inputs
    """

    arg_values = locals().copy()

    start_dt = dt.datetime.utcnow()

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.notice("          RUN Unit Benchmark Test ")
    RLOG.lprint(f"  (-w):  HUC8: {huc8} ")
    RLOG.lprint(f"  (-r):  Reload all inputs from s3: {do_reload}")
    RLOG.lprint(f"  (-s):  Source S3 inputs path: {s3_input_path}")
    RLOG.lprint(f"  (-t):  Local Target Inputs folder: {trg_input_path}")
    RLOG.lprint(f" Started (UTC): {get_stnd_date()}")
    print()

    # ----------------
    # validate input variables and setup key variables
    # rd = Return Variables Dictionary
    # Not all inputs need to be returned from rd or reloaded.
    rd = __validate_input(**arg_values)

    # ----------------
    # Loads the list from the config/s3_unit_donwload_files.lst
    # and creates a list of dictionary objects of items that will need to be dowloaded.
    download_file_lst = __get_download_file_list(**arg_values)

    if len(download_file_lst) == 0:
        print()
        RLOG.lprint(f"All applicable input files for {huc8} already exist at {trg_input_path}")

    else:
        # ----------------
        # Download the files
        print()
        RLOG.lprint("Beginning Downloads")
        print()
        bucket_name, ___ = s3_sf.parse_bucket_and_folder_name(s3_input_path)
        downloaded_files_lst = s3_sf.download_files_from_list(bucket_name, download_file_lst, True)
        print()
        RLOG.lprint(f"Number of files successfully downloaded: {len(downloaded_files_lst)}")

    # --------------------
    RLOG.lprint("")
    RLOG.lprint("===================================================================")
    RLOG.success("Search Complete")
    end_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Ended (UTC): {dt_string}")

    # Calculate duration
    time_duration = end_time - start_dt
    RLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    print()
    print(f"log files saved to {RLOG.LOG_FILE_PATH}")
    print()


# -------------------------------------------------
def __get_download_file_list(huc8, do_reload, s3_input_path, trg_input_path):
    """
    Returns a list of dictionary items with the following value:
        s3_file:  ie) s3://ras2fim/inputs/X-National_Datasets/nwm_flows.gpkg
        trg_file: ie) C:\ras2fim_data\inputs\X-National_Datasets\nwm_flows.gpkg
    """

    download_file_lst = []

    # ----------------
    # load the config list of files to download
    raw_file_paths = __load_file_list()

    if s3_input_path.endswith("/") is False:
        s3_input_path += "/"

    for raw_path in raw_file_paths:
        RLOG.trace(f"Creating download list for {raw_path}")

        if "[]" in raw_path:
            # subsitute HUC
            raw_path = raw_path.replace("[]", huc8)

        if raw_path.startswith("\\") or raw_path.startswith("/"):
            raw_path = raw_path[1:]  # strip it off the front

        # Change to win file syntax
        trg_file_path = raw_path.replace("/", "\\")
        trg_file_full_path = os.path.join(trg_input_path, trg_file_path)

        # Change to s3 url syntax
        s3_file_path = raw_path.replace("\\", "/")
        s3_file_full_path = s3_input_path + s3_file_path

        if do_reload is False and os.path.exists(trg_file_full_path) is True:
            # skip downloading it as it is already down and we don't want to reload
            RLOG.lprint(f"---- Skipping {s3_file_full_path}." f" It already exists at {trg_file_full_path}")
            continue
        else:  # either they do want it reloaded or it doesn't exist
            # ensure the file exists in S3.
            if s3_sf.is_valid_s3_file(s3_file_full_path) is False:
                RLOG.critical(f"s3 file of {s3_file_full_path} does not exist.")
                sys.exit(1)

            RLOG.lprint(f"-- Adding to download list - {s3_file_full_path} and {trg_file_full_path}")

            item = {"s3_file": s3_file_full_path, "trg_file": trg_file_full_path}

            download_file_lst.append(item)

    return download_file_lst


# -------------------------------------------------
def __load_file_list():
    # Load the list from the config\s3_unit_download_files.lst
    # Which gives us a list of file or file patterns to be downloaded

    list_file_name = "s3_unit_download_files.lst"

    referential_path = os.path.join(os.path.dirname(__file__), "..", "config", list_file_name)
    list_file_path = os.path.abspath(referential_path)

    if os.path.exists(list_file_path) is False:
        raise Exception(f"The s3 download list file of {list_file_path} can not found")

    raw_file_paths = []  # names and name patterns like dems/ras_3dep_HUC8_10m/HUC8_{}_dem.tif
    with open(list_file_path, "r") as lf:
        for line in lf:
            # Remove the newline character at the end of the liner
            line = line.strip()

            if line.startswith("#"):
                continue

            if line == "":
                continue

            raw_file_paths.append(line)

    if len(raw_file_paths) == 0:
        raise Exception(f"The s3 download list file of {list_file_path} seems to have no valid lines")

    return raw_file_paths


# -------------------------------------------------
#  Some validation of input, but also creating key variables
def __validate_input(huc8, do_reload, s3_input_path, trg_input_path):
    # At this point, there are no return values, so no need for a return dictionary
    # like other script files.

    # -------------------
    # -w   (ie 12090301)
    huc_valid, err_msg = val.is_valid_huc(huc8)
    if huc_valid is False:
        raise ValueError(err_msg)

    # -------------------
    if len(trg_input_path) == 0:
        raise ValueError("Target local inputs (-t) argument can not be empty")

    # -------------------
    # validate that the bucket and folders (prefixes exist)
    if len(s3_input_path) == 0:
        raise ValueError("S3 inputs path (-s) argument can not be empty")

    if s3_sf.is_valid_s3_folder(s3_input_path) is False:
        raise ValueError(f"S3 inputs folder of {s3_input_path} ... does not exist")


# -------------------------------------------------
if __name__ == "__main__":
    # Sample Usage, min args:
    #   python ./tools/s3_get_unit_inputs.py -w 12090301

    # Sample Usage with all args
    #   python ./tools/s3_get_unit_inputs.py -w 12090301 -t C:\ras2fim_data\inputs
    #      -s s3://ras2fim/inputs -r

    parser = argparse.ArgumentParser(description="========== Get HUC specfic inputs ==========")

    parser.add_argument(
        "-w",
        dest="huc8",
        help="REQUIRED: HUC-8 that is being evaluated: Example: 12090301",
        required=True,
        metavar="",
        type=str,
    )  # has to be string so it doesn't strip the leading zero

    parser.add_argument(
        "-t",
        "--trg_input_path",
        help="OPTIONAL: You can override the root folder where the input files will be saved."
        " Past the 'inputs' folder, folder names and pathing are not changeable.\n"
        f"Defaults to {sv.ROOT_DIR_INPUTS}",
        default=sv.ROOT_DIR_INPUTS,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-s",
        "--s3_input_path",
        help=f"OPTIONAL (case-sensitive): full s3 path to all of the ras2fim inputs folders ise at."
        " eg. s3://my_ras2fim_bucket/inputs/\n"
        f" Defaults to {sv.S3_INPUTS_PATH}",
        required=False,
        default=sv.S3_INPUTS_PATH,
        metavar="",
    )

    parser.add_argument(
        "-r",
        "--do_reload",
        help="OPTIONAL: Adding this flag will download all input files overwritting them"
        " if they exist."
        "Default = False (use local files if already downloaded)",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    log_file_folder = os.path.join(args["trg_input_path"], "logs")
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # Creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        log_file_name = f"{script_file_name}_{get_date_with_milli(False)}.log"
        RLOG.setup(os.path.join(log_file_folder, log_file_name))

        s3_get_unit_inputs(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
        sys.exit(1)
