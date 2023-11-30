#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import shutil
import sys
import traceback

import colored as cl
import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import ras2fim_logger
import shared_variables as sv
from shared_functions import get_date_with_milli


# Global Variables
RLOG = ras2fim_logger.R2F_LOG


"""
This tool searches any given s3 folder looking for matching file or folder names

NOTES:
   - The product will create a csv of the results. You can define the output folder for
     where the csv will be added, but not the csv name.
     Default output folder: c:\ras2fim_data\tool_outputs

   - The search field can only be used against one file or folder name but can use zero to many
     astericks to search that folder or file name. But the searching itself is recursive.
     ie) rob*.csv is ok, but not models/rob*.csv  (no slashes) wildcard char itself is optional

   - Searching is not case sensitive

   - The wildcard can be used as a zero to many search (ie. *trinity* migth have no chars in
     from of the phrase trinity to be found)

   - The higher the folder level to start your search, like all search tools, means more folders
     and files to check so it will take longer.

"""


####################################################################
def s3_search(s3_path, search_key, output_folder_path=sv.LOCAL_TOOLS_OUTPUT_PATH):
    """
    Process:
        - download all of the files and folder names recursively star

    Inputs:
        - s3_path: full s3 base path: ie) s3://ras2fim-dev/OWP_ras_models/models-12030105-full
        - search_key: Any word, letters, and most chars, with * meaning 0 to many matchs.
            Non-case sensitive.
            ie)
                #search_key = "TRINITY*"  (none... only work if no chars in front of Trinity)
                #search_key = "*TRINITY*"
                #search_key = "*trinity river*"
                #search_key = "*caney*.prj"
                #search_key = "*caney*.g01"
                #search_key = "*caney*.g01*"
                #search_key = "*.g01*"
                #search_key = "*.g01"
                #search_key = "1262811*"
        - output_folder_path: The folder, but not file name, where the output csv will be saved.
    Outputs:
        - a csv with two columns.
             The first is the record name found, including subfolder pathing if applicable.
                ie) 1262811_UNT 211 in Village Cr Washd_g01_1689773310/UNT 211 in Village Cr Washd.prj
             The second column is the full URL to the file.
                ie) s3://ras2fim-dev/OWP_ras_models/models-12030105-full/1262811_UNT 211.... Washd.prj
    """

    start_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    RLOG.lprint("")
    RLOG.lprint("===============================s3_path==================================")
    RLOG.notice("          RUN s3 search tool ")
    RLOG.lprint(f"  (-s3): s3_path {s3_path} ")
    RLOG.lprint(f"  (-key): s3 bucket name {search_key}")
    RLOG.lprint(f"  (-p): output results folder {output_folder_path}")
    RLOG.lprint(f" --- Start: {dt_string} (UTC time) ")
    RLOG.lprint("=================================================================")

    # --------------------
    # It will throw it's own exceptions if required
    rtn_varibles_dict = __validate_input(s3_path, search_key, output_folder_path)
    bucket_name = rtn_varibles_dict["bucket_name"]
    s3_folder_path = rtn_varibles_dict["s3_folder_path"]

    # ----------
    # Call S3 for wildcard search (get list of keys and urls back)
    s3_items = s3_sf.get_records_list(bucket_name, s3_folder_path, search_key)
    if len(s3_items) == 0:
        RLOG.lprint(
            f"{cl.fg('red')}No files or folders found in source folder of {s3_path}."
            f"Note: the pathing is case-sensitive.{cl.attr(0)}"
        )
    else:
        RLOG.lprint(f"{cl.fg('spring_green_2b')}Number of matches found: {len(s3_items)}{cl.attr(0)}")

        # ----------
        # iterate through results to build csv
        outfile_name = f"s3_search_results_{get_date_with_milli(False)}.csv"
        output_file_path = os.path.join(output_folder_path, outfile_name)

        # build dataframe (easier then going dict to csv)
        df = pd.DataFrame(s3_items)
        df.to_csv(output_file_path, index=False)

        RLOG.lprint(f"{cl.fg('spring_green_2b')}Output file as {output_file_path}{cl.attr(0)}")

    # --------------------
    RLOG.lprint("")
    RLOG.lprint("===================================================================")
    RLOG.lprint(f"{cl.fg('spring_green_2b')}Search Complete{cl.attr(0)}")
    end_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Ended (UTC): {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    RLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    RLOG.lprint("")


####################################################################
####  Some validation of input, but also creating key variables ######
def __validate_input(s3_path, search_key, output_folder_path):
    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    rtn_varibles_dict = {}

    # ---------------
    # why is this here? might not come in via __main__
    if output_folder_path == "":
        raise ValueError("Output folder path parameter value can not be empty")

    if not os.path.exists(output_folder_path):  # whole path exists
        # Ensure the output_folder_path does not have a file name on the end.
        if os.path.exists(output_folder_path):
            raise ValueError("Output folder path parameter can not include a file name")

        split_dirs = output_folder_path.split("/")

        print(split_dirs)

        parent_folder = split_dirs[0:-1]
        print(f"parent_folder is {parent_folder}")

        # The parent folder must exist, but we will create the child folder if required.
        if not os.path.exists(parent_folder):
            raise ValueError(
                f"The output folder path submitted is {output_folder_path}."
                " The child folder need not pre-exist, but the parent folder"
                f" of {parent_folder} must pre-exist"
            )
        else:  # then we need to make the child directory
            shutil.mkdir(output_folder_path)

    test_search_key = search_key.replace("*", "")
    if len(test_search_key) <= 3:
        raise ValueError("search key must be at least 3 chars not counting the wildcards")

    # will raise it's own exceptions if needed
    bucket_name, s3_folder_path = s3_sf.is_valid_s3_folder(s3_path)
    rtn_varibles_dict["bucket_name"] = bucket_name
    rtn_varibles_dict["s3_folder_path"] = s3_folder_path

    return rtn_varibles_dict


####################################################################
if __name__ == "__main__":
    # ---- Samples Inputs

    # With min inputs
    #   python3 s3_search_tool.py -key "*Trinity River*" (other parms will default)

    # With max inputs
    #   python3 s3_search_tool.py -s3 s3://ras2fim-dev/OWP_ras_models/my_models -key "*Trinity River*"
    #      -o c:/ras2fim/my_search_outputs
    #   Note.. no file names, just folder path and folder must pre-exists

    parser = argparse.ArgumentParser(
        description="Searching S3 folders", formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-s3",
        "--s3_path",
        help="OPTIONAL: This value starting s3 folder (full s3 path) where searching will be done.\n"
        "ie) s3://ras2fim-dev/OWP_ras_models/my_models.\n"
        "Note: it is a case-sensitive value\n"
        f"Defaults to {sv.S3_OUTPUT_MODELS_FOLDER}",
        default=sv.S3_OUTPUT_MODELS_FOLDER,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-key",
        "--search_key",
        help="OPTIONAL: Value is the file / folder name and optional pattern"
        " (NOT case-sensitive)\n"
        "ie) *Trinity River*  (note: wildcard before and after phrase, position matters)",
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-o",
        "--output_folder_path",
        help="OPTIONAL: The local folder location of where the search results will be saved.\n"
        "File names (in csv) wil be calculated on the fly (dates/time added).\n"
        f"Default to {sv.LOCAL_TOOLS_OUTPUT_PATH}",
        required=False,
        default=sv.LOCAL_TOOLS_OUTPUT_PATH,
        metavar="",
    )

    args = vars(parser.parse_args())

    log_file_folder = os.path.join(args["output_folder_path"], "logs")
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

        s3_search(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
