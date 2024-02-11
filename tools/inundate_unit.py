#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import sys
import traceback
from pathlib import Path

import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf
import shared_variables as sv
from shared_functions import get_stnd_date, get_date_time_duration_msg, get_date_with_milli


# Global Variables
RLOG = sv.R2F_LOG

"""
This tool can take auto 

TODO:  explain how this works?
"""

# -------------------------------------------------
def inundate_unit(unit_folder_name,
                  enviro,
                  src_geocurves_path,
                  trg_gval_root,
                  trg_output_override_path,
                  src_benchmark_data_path):

    """
    TODO opening notes
    """

    arg_values = locals().copy()

    start_dt = dt.datetime.utcnow()

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.notice("          RUN inundation_unit ")
    RLOG.lprint(f"  (-u):  Source unit folder name: {unit_folder_name} ")
    RLOG.lprint(f"  (-sg): Source local path for geocurves files: {src_geocurves_path}")    
    RLOG.lprint(f"  (-b):  Source benchmark data path: {src_benchmark_data_path}")        
    RLOG.lprint(f"  (-e):  Environment type: {enviro}")
    RLOG.lprint(f"  (-tg): Local target gval root path: {trg_gval_root}")
    RLOG.lprint(f"  (-to): Local target output override path: {trg_output_override_path}")
    RLOG.lprint(f" Started (UTC): {get_stnd_date()}")        
    print()

    # ----------------
    # validate input variables and setup key variables
    # rd = Return Variables Dictionary
    rd = __validate_input(**arg_values)

    # From unit_folder_name e.g. 12030101_2276_ble_230925
    # remember.. the word "ble" or other inside the folder name is not 
    # about eval source, it is about original HECRAS source.
    # Not all inputs need to be returned from rd or reloaded.
    huc = rd["huc"]  # e.g. 12030101
    unit_id = rd["unit_id"]  # e.g. 12030101_2276_ble
    version_date_as_str = rd["version_date_as_str"] # 231204
    enviro = rd["enviro"]  # PROD / DEV (now upper)
    src_geocurves_path = rd["src_geocurves_path"]
    trg_inun_file_path = rd["trg_inun_file_path"]
    src_benchmark_data_path = rd["src_benchmark_data_path"] # could be S3 or local    
    is_s3_path = rd["is_s3_path"]
    local_benchmark_data_path = rd["local_benchmark_data_path"]

    # ----------------
    # We might be downloaded from S3, 
    # but we get a list of local huc applicable benchmark csv files
    if is_s3_path is True:
        lst_bench_files = get_s3_benchmark_data(huc,
                                                src_benchmark_data_path,
                                                local_benchmark_data_path)
        
    else: # get them locally (list of the huc applicable benchmark csv's)
        print(f"Looking for benchmark files for huc {huc}")

        # count
        # if 0
        # RLOG.

    # calc benchmark sources.
    # print out benchmark sources
        

    # ----------------
    # Check to see if inundation files already exist and ask of overwrite?

    

    print()
    print("===================================================================")
    print("inundate unit processing complete")
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    print(f"Ended (UTC): {dt_string}")
    print(f"log files saved to {RLOG.LOG_FILE_PATH}")

    dur_msg = get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


# -------------------------------------------------
def get_s3_benchmark_data(huc, s3_src_benchmark_data_path, local_benchmark_data_path):

    """
    Process:
        This only works for donwload benchmark data from S3
    TODO: notes how this works

    Output:
        bench_files: list of benchmark files.

    """

    RLOG.lprint(f"Loading benchmark data from S3 for HUC {huc} from {s3_src_benchmark_data_path}")

    # ----------------
    # Download benchmark if needed (just the ones for that HUC)
    # get all benchmark foldes first, then sort it down the the ones with the right HUC
    #bench_huc_folder = s3_sf.get_folder_list(sv.S3_DEFAULT_BUCKET_NAME,
    #                                         "gval/" + sv.S3_GVAL_BENCHMARK_FOLDER,
    #                                         False)

    # we need to split out the bucket and s3 pathing
    bucket_name, s3_folder_path = s3_sf.parse_bucket_and_folder_name(s3_src_benchmark_data_path)    

    bench_files = s3_sf.get_file_list(bucket_name, 
                                      s3_folder_path,
                                      "*" + huc + "*",
                                      False)

    # sort out to keep on the .csv files paths.
    files_to_download = []
    for bench_file in bench_files: # Iterate dictionary items
        if bench_file["url"].endswith(".csv"):
            files_to_download.append(bench_file)

    if len(files_to_download) == 0:
        RLOG.critical("There are no benchmark .csv files for the huc {huc} at {}")
        sys.exit(1)

    down_items = []
    for s3_file in files_to_download:
        item = {}
        s3_key = s3_file["key"]
        s3_file_url = s3_file["url"].replace(f"s3://{bucket_name}", "")
        item["s3_file"] = s3_file_url # stripped of the s3 and bucket name
        # At this point, the url has everything passed the bucket.. ie) /gval/benchmark/ble....

        trg_file = os.path.join(local_benchmark_data_path, s3_key)
        trg_file = trg_file.replace("/", "\\")
        item["trg_file"] = trg_file
        
        # trg_file = trg_file.replace(trg_gval_root, "")
        # Take off the base local pathing so it doesn't show up again.. ie) c:/gval/gval

        down_items.append(item)

    # all of the ingoing down_items are going to be coming back. They all have 
    # two new keys:
    #       - "success": "True" / "False" (string version)
    #       - "fail_reason": empty or whatever ever msg
    # They will be downloaded to their correct local pathing
    # and only the ones that match the HUCs.
    # Each record will log it it downloaded correctly. If verbose, it will also display it
    num_files_to_be_dwn = len(down_items)
    down_items = s3_sf.download_files_from_list(bucket_name, down_items, True)

    # downloaded benchmark files
    bench_files = [] # only successful
    for down_item in down_items:
        if down_item["success"] == "True":
            bench_files.append(down_item["trg_file"])

    print()
    if len(bench_files) == 0:
        RLOG.critical("All benchmark files failed to be downloaded")
        sys.exit(1)
    else:
        RLOG.success('Done loading benchmark')
        RLOG.lprint(f"-- {len(bench_files)} of {num_files_to_be_dwn} downloaded successfully")

    return bench_files


# -------------------------------------------------
#  Some validation of input, but also creating key variables
def __validate_input(unit_folder_name,
                     enviro,
                     src_geocurves_path,
                     trg_gval_root,
                     trg_output_override_path,
                     src_benchmark_data_path):
    
    """
    Summary: Will raise Exception if some are found
   
    TODO: fill in

    Output: dictionary
    """

    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned
    rtn_dict = {}

    # ----------------
    if unit_folder_name == "":
        raise ValueError("unit_folder_name (-u) can not be empty")

    # splits it a six part dictionary
    src_name_dict = s3_sf.parse_unit_folder_name(unit_folder_name)
    if "error" in src_name_dict:
        raise Exception(src_name_dict["error"])

    rtn_dict["huc"] = src_name_dict["key_huc"]
    rtn_dict["unit_id"] = src_name_dict["key_unit_id"] # (12090301_2277_ble)
    rtn_dict["version_date_as_str"] = src_name_dict["key_date_as_str"] # (date string eg: 230811),

    # ----------------
    enviro = enviro.upper()
    if enviro != "PROD" and enviro != "DEV":
        raise ValueError(f"The enviro (-e) arg must be either 'PROD' or 'DEV'")
    rtn_dict["enviro"] = enviro

    # ----------------
    # src_geocurve_files_path
    if src_geocurves_path == "":
        raise ValueError(f"The rating curves directory (-sc) arg can not be empty")

    if src_geocurves_path == "use_default":
        src_geocurves_path = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS,
                                               unit_folder_name,
                                               sv.R2F_OUTPUT_DIR_FINAL_GEOCURVES)

    if os.path.exists(src_geocurves_path) is False:
        raise ValueError(f"The rating curves directory (-sg) of {src_geocurves_path}"
                         " (or the defaulted value) does not exist.")
    
    ct_curves_path = len(list(Path(src_geocurves_path).rglob("*.csv")))
    if ct_curves_path == 0:
        raise ValueError(f"The rating curves directory (-sg) of {src_geocurves_path}"
                         " does not have .gpkg files in it.")

    rtn_dict["src_geocurves_path"] = src_geocurves_path

    # ----------------
    if trg_gval_root == "":
        raise ValueError("target gval root folder (-tg) can not be empty")
    os.makedirs(trg_gval_root, exist_ok=True)

    # ----------------
    # trg_output_override_path
    if enviro == "PROD":  # only base gval root is ok here.
        # build this up to 
        # e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923\inundation_files
        # trg_gval_root should be C:\ras2fim_data\gval or overrode value
        trg_inun_file_path = os.path.join(trg_gval_root,
                                          sv.LOCAL_GVAL_EVALS,
                                          "PROD",
                                          rtn_dict["unit_id"],
                                          rtn_dict["version_date_as_str"],
                                          sv.INUNDATION_ROOT_FOLDER_NAME)
    else:  # DEV or override are fine
        if trg_output_override_path == "":
            # I am sure there is a better way to do this.. but this is easy to read and follow
            # e.g. C:\ras2fim_data\gval\evaluations\DEV\12030105_2276_ble\230923\inundation_files
            trg_inun_file_path = os.path.join(trg_gval_root,
                                            sv.LOCAL_GVAL_EVALS,
                                            "DEV",
                                            rtn_dict["unit_id"],
                                            rtn_dict["version_date_as_str"],
                                            sv.INUNDATION_ROOT_FOLDER_NAME)            
        else:
            trg_inun_file_path = trg_output_override_path
    
    rtn_dict["trg_inun_file_path"] = trg_inun_file_path

    # ----------------
    if src_benchmark_data_path == "":
        raise ValueError("Src benchmark data folder (-b) can not be empty")
    
    is_s3_path = ( (src_benchmark_data_path.startswith("S3://")) 
                or (src_benchmark_data_path.startswith("s3://")) )
    if is_s3_path:
        src_benchmark_data_path = src_benchmark_data_path.replace("S3://", "s3://")
        # if the folder exists, we will download it later.        
        if s3_sf.is_valid_s3_folder(src_benchmark_data_path) is False:
            raise ValueError(f"The s3 path entered of {src_benchmark_data_path} does not exist")

        rtn_dict["local_benchmark_data_path"] = os.path.join(trg_gval_root, sv.LOCAL_GVAL_BENCHMARK_DATA)

    else:  # must pre-exist if it a non s3 url.
        if os.path.exists(src_benchmark_data_path) is False:
            raise ValueError("src benchmark data folder (-b) does not exist")
        rtn_dict["local_benchmark_data_path"] = src_benchmark_data_path

    rtn_dict["is_s3_path"] = is_s3_path
    rtn_dict["src_benchmark_data_path"] = src_benchmark_data_path # could be S3 or local

    return rtn_dict


# -------------------------------------------------
if __name__ == "__main__":
    # ***********************
    # This tool has some optional S3 calls. The default bucket and pathing are for for NOAA/OWP staff only.
    # You are welcome to use this tool with configuration not using S3 calls or have your own s3 bucket
    # with your own AWS account and credentials.
    # ***********************

    # TODO: sample

    # TODO: do we force the geocurve path?

    parser = argparse.ArgumentParser(
        description="Inundating a ras2fim output unit. NOTE: please read notes the top this script"
        " for advanced details how this tools works, arguments, output folder patterns, etc.\n"
        "Note: This tool does not save back to S3 (ask if you want that functionaly added optionally)",
         formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-u",
        "--unit_folder_name",
        help="REQUIRED: e.g. 12030101_2276_ble_230925 (matching the standardize folder naming convention).",
        required=True,
        metavar="",        
    )

    parser.add_argument(
        "-e",
        "--enviro",
        help="REQUIRED: either the word 'PROD' or 'DEV'."
        " This will affect output pathing slightly.\n",
        required=True,
        metavar="",        
    )

    parser.add_argument(
        "-sc",
        "--src_geocurves_path",
        help=f"OPTIONAL: Local folder were the rating curves (.csv) files are at"
          r" e.g C:\my_ras\12030101_2276_ble_230925\final\geocurves.\n"
        f"Defaults to {sv.R2F_DEFAULT_OUTPUT_MODELS}\[unit_name]\{sv.R2F_OUTPUT_DIR_FINAL_GEOCURVES}",
        default="use_default",
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-tg",
        "--trg_gval_root",
        help=r"OPTIONAL: The root folder were the gval is saved to. e.gc:\rob\gval_testing.\n"
        f"Defaults to {sv.LOCAL_GVAL_ROOT}",
        default=sv.LOCAL_GVAL_ROOT,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-to",
        "--trg_output_override_path",
        help="OPTIONAL: You an override the entire pathing structure to put the inundation files"
        "  wherever you like, you will not be forced to any calculated pathing of any kind, however\n"
        " if the (-e) env flag is PROD, you can not override the pathing"
        " only the root gval folder (-tg). In PROD mode, the pathing past the gval root is calculated.\n"
        " See notes at the top of this script for more details.",
        default="",
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-b",
        "--src_benchmark_data_path",
        help="OPTIONAL: The root folder of where the benchmark is located.\n"
        "*** NOTE:This can be a local path OR an S3 path, default will be s3 PROD.\n"
        r" e.g. C:\ras2fim_data\gval\benchmark_data\n"
        " OR s3://ras2fim/gval/benchmark_data\n"
        " If the benchmark data is downloaded from S3, it will put it in the default local gval pathing.\n"
        f"Defaults to {sv.S3_GVAL_BENCHMARK_PATH}",
        default=sv.S3_GVAL_BENCHMARK_PATH,
        required=False,
        metavar="",
    )

    args = vars(parser.parse_args())

    # Yes.. not including the rel_name
    log_file_folder = os.path.join(args["trg_gval_root"], "logs")
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

        inundate_unit(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
        sys.exit(1)
