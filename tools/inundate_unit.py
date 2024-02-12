#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import shutil
import sys
import traceback
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import ras2inundation as ri
import s3_shared_functions as s3_sf
import shared_variables as sv

from shared_functions import get_date_time_duration_msg, get_date_with_milli, get_stnd_date


# Global Variables
RLOG = sv.R2F_LOG

"""

TODO:  explain how this works

This tool can already work in auto mode. No runtime questions are asked.
"""

# ***********************
# NOTICE:
#    Feb 2024: Due to time constraints, most testings of combinations of input arguments have not yet
#        not been done. Testing has only be done against the default happy path with all defaults
#        for optional args.
# ***********************

# -------------------------------------------------
def inundate_unit(
    unit_folder_name,
    enviro,
    src_geocurves_path,
    trg_gval_root,
    trg_output_override_path,
    src_benchmark_data_path,
):
    """
    TODO Processing notes (lots of permuations)

    Inputs:
        unit_folder_name: e.g. 12030101_2276_ble_230925
        enviro: e.g. PROD or DEV
        src_geocurves_path: e.g. C:\ras2fim_data\output_ras2fim\12030101_2276_ble_230925\final\geocurves
        trg_gval_root: e.g. c:\ras2fim_data\gval
        trg_output_override_path: e.g.(blank) or c:\my_ras\inundation_files
        src_benchmark_data_path: e.g. C:\ras2fim_data\gval\benchmark_data
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
    print("NOTE: All output inundation files will be overwritten")
    print()

    # ----------------
    # validate input variables and setup key variables
    # rd = Return Variables Dictionary
    # Not all inputs need to be returned from rd or reloaded.    
    rd = __validate_input(**arg_values)

    # ----------------
    # We might be downloaded from S3,
    # but we get a list of local huc applicable benchmark csv files
    if rd["is_s3_path"] is True:
        lst_bench_files = get_s3_benchmark_data(rd["huc"],
                                                rd["src_benchmark_data_path"],
                                                rd["local_benchmark_data_path"])

    else:  # get them locally (list of the huc applicable benchmark csv's)
        print(f"Looking for benchmark files for huc {rd['huc']}")

        # TODO: load local benchmark files fully pathed.

        # GLOB 

        # lst_bench_files = (some function)
        # count
        # if 0
        # RLOG.

        
    # ----------------
    # We need to keep just the csv for inundation at this point.
    #bench_flow_files = [ i for i in lst_bench_files if Path(i).suffix == ".csv"]
    # Now we iterate the bench_files to find the valid flow files we need.
    bench_flow_files = []
    for b_file in lst_bench_files:
        if Path(b_file).suffix != ".csv":
            continue
        parent_path = Path(b_file)
        parent_dir_name = parent_path.parent.name
        if parent_dir_name in sv.GVAL_VALID_STAGES:
            bench_flow_files.append(b_file)
     

    inundate_files(bench_flow_files,
                   rd["huc"],
                   rd["src_geocurves_path"],
                   rd["trg_inun_file_path"],
                   rd["local_benchmark_data_path"])
        

    print()
    print("===================================================================")
    RLOG.success("Inundate unit processing complete")
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.success(f"Ended (UTC): {dt_string}")
    RLOG.success(f"log files saved to {RLOG.LOG_FILE_PATH}")

    dur_msg = get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()

# -------------------------------------------------
def inundate_files(flow_files,
                   huc,
                   src_geocurves_path,
                   trg_inun_file_path,
                   local_benchmark_data_path):
    
    """
    Process: Iterates the incoming local benchmark files and run's inundatoin on them
    Input:
        flow_files: simple list of all huc applicable benchmark csv.
        src_geocurves_path = unit's geocurve files (usually from final/geocurves)
            - e.g. C:\ras2fim_data\output_ras2fim\12090301_2277_ble_230923\final\geocurves
        huc: 12090301
        trg_inun_file_path:
            e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923
        local_benchmark_data_path: (we use this to re-calc pathing for the output folders)
            e.g. C:\ras2fim_data\gval\benchmark_data. 
    """

    print("--------------------------")
    RLOG.notice(f"Runnning inundation based on geocurves from {src_geocurves_path}")
    RLOG.lprint(f"All output inundation files will created in {trg_inun_file_path}")

    # don't let if fail if one errors out, unless all fail.
    flow_files.sort()
    lst_bench_sources = []    

    for ind, b_file in enumerate(flow_files):
        # the key is that it is sort.
        # Figure out adjusted path
        #   e.g. incoming C:\ras2fim_data\gval\benchmark_data\ble\12030105\100yr\ble_huc_12030105_flows_100yr.csv
        #   becomes: ble\12030105\100yr\ which gets added to the inundation pathing so 
        #   the output pathing becomes C:\ras2fim_data\gval\evaluations\
        #      PROD\12030105_2276_ble\230923\**.gkpg

        dir_to_ben_file = os.path.dirname(b_file)
        ben_file_name = os.path.basename(b_file)
        ref_bench_file_path = dir_to_ben_file.replace(local_benchmark_data_path + "\\", "")
        ben_source = ref_bench_file_path.split("\\")[0]  # ie. ble, nws, ras2fim, etc
        if ben_source not in lst_bench_sources:
            lst_bench_sources.append(ben_source)
            print()
            RLOG.notice(f"----- Inundating files for {ben_source} ---------")

        inun_file_name = ben_file_name.replace(".csv", "_inundation.gpkg")
        # At this point the inun_file name are names such as:
        #    ble_huc_12090301_flows_100yr_inundation.gpkg and
        #    ahps_cbst2_huc_12090301_flows_major_inundation.gpkg
        # We want them down to:
        #    ble_100yr_inundation.gpkg and
        #    ahps_cbst2_major_inundation.gpkg

        # We also need to strip out the values before the phrases before the phrase "_huc",
        # and the huc, and the word flows.

        strip_pattern = f"_huc_{huc}_flows"
        inun_file_name = inun_file_name.replace(strip_pattern, "")
        trg_file_path = os.path.join(trg_inun_file_path, inun_file_name)

        print(f"... Inundation Starting : {b_file}")
        # it will display/log errors and critical errors
        ri.produce_inundation_from_geocurves(src_geocurves_path, b_file, trg_file_path, False)
        # print(f"... Inundation Complete : {b_file}")


# -------------------------------------------------
def get_s3_benchmark_data(huc, s3_src_benchmark_data_path, local_benchmark_data_path):
    """
    Process:
        This only works for donwload benchmark data from S3
    TODO: notes how this works

    Output:
        bench_files: list of benchmark files. (simple list of full pathed .csv benchmark files)

    """

    print("--------------------------")
    RLOG.notice(f"Loading benchmark data from S3 for HUC {huc} from {s3_src_benchmark_data_path}")

    # ----------------
    # Download benchmark if needed (just the ones for that HUC)
    # get all benchmark foldes first, then sort it down the the ones with the right HUC
    # bench_huc_folder = s3_sf.get_folder_list(sv.S3_DEFAULT_BUCKET_NAME,
    #                                         "gval/" + sv.S3_GVAL_BENCHMARK_FOLDER,
    #                                         False)

    # we need to split out the bucket and s3 pathing
    bucket_name, s3_folder_path = s3_sf.parse_bucket_and_folder_name(s3_src_benchmark_data_path)

    bench_files = s3_sf.get_file_list(bucket_name, s3_folder_path, "*" + huc + "*", False)

    # sort out to keep the .csv
    files_to_download = []
    for bench_file in bench_files:  # Iterate dictionary items
        if bench_file["url"].endswith(".csv"):
            files_to_download.append(bench_file)

    if len(files_to_download) == 0:
        RLOG.critical(f"There are no benchmark .csv files for the huc {huc}")
        sys.exit(1)

    down_items = []
    #for s3_file in files_to_download:    
    for s3_file in bench_files:
        item = {}
        s3_key = s3_file["key"]
        s3_file_url = s3_file["url"].replace(f"s3://{bucket_name}", "")
        item["s3_file"] = s3_file_url  # stripped of the s3 and bucket name
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
    down_items = s3_sf.download_files_from_list(bucket_name, down_items, False)

    # downloaded benchmark files
    bench_files = []  # only successful
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
def __validate_input(
    unit_folder_name,
    enviro,
    src_geocurves_path,
    trg_gval_root,
    trg_output_override_path,
    src_benchmark_data_path,
):
    """
    Summary: Will raise Exception if some are found

    TODO: fill in

    Output: dictionary
    """

    # TODO: test perumations of the input args
    print()
    RLOG.notice("NOTE: Some of the testing for non-defaulted args has not yet been completed")
    print()    


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
    rtn_dict["unit_id"] = src_name_dict["key_unit_id"]  # (12090301_2277_ble)
    rtn_dict["version_date_as_str"] = src_name_dict["key_date_as_str"]  # (date string eg: 230811),

    # ----------------
    enviro = enviro.upper()
    if enviro != "PROD" and enviro != "DEV":
        raise ValueError("The enviro (-e) arg must be either 'PROD' or 'DEV'")
    rtn_dict["enviro"] = enviro

    # ----------------
    # src_geocurve_files_path
    if src_geocurves_path == "":
        raise ValueError("The rating curves directory (-sc) arg can not be empty")

    if src_geocurves_path == "use_default":
        src_geocurves_path = os.path.join(
            sv.R2F_DEFAULT_OUTPUT_MODELS, unit_folder_name, sv.R2F_OUTPUT_DIR_FINAL_GEOCURVES
        )

    if os.path.exists(src_geocurves_path) is False:
        raise ValueError(
            f"The rating curves directory (-sg) of {src_geocurves_path}"
            " (or the defaulted value) does not exist."
        )

    ct_curves_path = len(list(Path(src_geocurves_path).rglob("*.csv")))
    if ct_curves_path == 0:
        raise ValueError(
            f"The rating curves directory (-sg) of {src_geocurves_path}" " does not have .csv files in it."
        )

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
        trg_inun_file_path = os.path.join(
            trg_gval_root,
            sv.LOCAL_GVAL_EVALS,
            "PROD",
            rtn_dict["unit_id"],
            rtn_dict["version_date_as_str"]
        )
    else:  # DEV or override are fine
        if trg_output_override_path == "":
            # I am sure there is a better way to do this.. but this is easy to read and follow
            # e.g. C:\ras2fim_data\gval\evaluations\DEV\12030105_2276_ble\230923\inundation_files
            trg_inun_file_path = os.path.join(
                trg_gval_root,
                sv.LOCAL_GVAL_EVALS,
                "DEV",
                rtn_dict["unit_id"],
                rtn_dict["version_date_as_str"]
            )
        else:
            trg_inun_file_path = trg_output_override_path

    rtn_dict["trg_inun_file_path"] = trg_inun_file_path
    if (os.path.exists(trg_inun_file_path)): # empty it
        shutil.rmtree(trg_inun_file_path)

    # ----------------
    if src_benchmark_data_path == "":
        raise ValueError("Src benchmark data folder (-b) can not be empty")

    is_s3_path = (src_benchmark_data_path.startswith("S3://")) or (
        src_benchmark_data_path.startswith("s3://")
    )
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
    rtn_dict["src_benchmark_data_path"] = src_benchmark_data_path  # could be S3 or local

    return rtn_dict


# -------------------------------------------------
if __name__ == "__main__":
    # ***********************
    # This tool has some optional S3 calls. The default bucket and pathing are for for NOAA/OWP staff only.
    # You are welcome to use this tool with configuration not using S3 calls or have your own s3 bucket
    # with your own AWS account and credentials.
    # ***********************

    # Sample with min args:
    #    python ./tools/inundate_unit.py -u 12030103_2276_ble_230923 -e PROD

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
        help="REQUIRED: either the word 'PROD' or 'DEV'." " This will affect output pathing slightly.\n",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-sc",
        "--src_geocurves_path",
        help=f"OPTIONAL: Local folder were the rating curves (.csv) files are at.\n"
        r" e.g C:\my_ras\12030101_2276_ble_230925\final\geocurves."
        f"\nDefaults to {sv.R2F_DEFAULT_OUTPUT_MODELS}\[unit_name]\{sv.R2F_OUTPUT_DIR_FINAL_GEOCURVES}",
        default="use_default",
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-tg",
        "--trg_gval_root",
        help=r"OPTIONAL: The root folder were the gval is saved to. e.g c:\rob\gval_testing."
        f"\nDefaults to {sv.LOCAL_GVAL_ROOT}",
        default=sv.LOCAL_GVAL_ROOT,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-to",
        "--trg_output_override_path",
        help="OPTIONAL: You can override the pathing structure to inundation files"
        " wherever you like, with no calculated folder pathing.\n"
        "However, if the (-e) env flag is PROD, you can not override the pathing"
        " only the root gval folder (-tg).\n"
        "In PROD mode, the pathing past the gval root is calculated.\n"
        "See notes at the top of this script for more details.",
        default="",
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-b",
        "--src_benchmark_data_path",
        help="OPTIONAL: The root folder of where the benchmark is located.\n"
        "*** NOTE:This can be a local path OR an S3 path, default will be s3 PROD.\n"
        r" e.g. C:\ras2fim_data\gval\benchmark_data"
        " OR s3://ras2fim/gval/benchmark_data\n"
        "If the benchmark data is downloaded from S3, it will put it in the default local gval pathing.\n"
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
