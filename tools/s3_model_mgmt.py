#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import shutil
import sys
import traceback

import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import ras2fim_logger
import shared_variables as sv
from shared_functions import get_date_with_milli, get_stnd_date, print_date_time_duration


# Global Variables
RLOG = ras2fim_logger.R2F_LOG

"""
This tool does some reconsilation and validation of both the master s3 models catalog and it's
related models folders.

Tests performed:
    1. Look for duplicate "final_name_key" in the master csv. (exact match at this time)

    2. Look for extra records in the master csv that do not have a matching model folder and
    vice-a-versa.

    3. Look for dups for the "initial_scrape_name" column, but only where the ndhplus_comid is not
    empty.

    4. Show a count of raw model folders. (on screen only)

    5. Get the total size of the "model" (s3_models_path) folder (on screen only).
"""


####################################################################
def manage_models(s3_master_csv_path, s3_models_path, output_folder_path):
    """
    Process:
        - TODO:

    Inputs:
        - s3_master_csv_path: full s3 path and name of the master csv.
           ie) s3://my-ras2fim-bucket/OWP_ras_models/OWP_ras_models_catalog.csv

        - s3_models_path: full s3 path where the model folders are at.
           ie) s3://my-ras2fim-bucket/OWP_ras_models/models

        - output_folder_path: The folder, but not file name, where the output csv will be saved.

    Outputs:
        - a csv which as all of the columns from the original master csv but only the rows where
          possible problems exist and extra columns for reasons
            - new column for "is_dup_final_key_name" (True / False)
            - new column for "is_dup_initial_scrape_name" (True / False)
            - new column for "s3_model_folder_not_found" (True / False)
            - new column for "csv_final_key_name_not_found" (True / False)

        Note: if a folder is found in S3 that is not in the csv, a new row will be added to the csv
        with the model_name column being populated with the value of "no csv rec found for folder:
        {folder name}" and the csv_final_key_name_not_found will be True

    """

    start_dt = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.notice("          RUN s3 model management and reconsilation tool")
    RLOG.lprint(f"  (-csv): s3 master csv path {s3_master_csv_path} ")
    RLOG.lprint(f"  (-mp): s3 models folder path {s3_models_path}")
    RLOG.lprint(f"  (-o): output results folder {output_folder_path}")
    RLOG.lprint(f" --- Start: {dt_string} (UTC time) ")
    RLOG.lprint("=================================================================")

    # --------------------
    # It will throw it's own exceptions if required
    rtn_varibles_dict = __validate_input(s3_master_csv_path, s3_models_path, output_folder_path)
    bucket_name = rtn_varibles_dict["bucket_name"]
    s3_folder_path = rtn_varibles_dict["s3_folder_path"]
    target_report_path = os.path.join(output_folder_path, "s3_model_mgmt_report.csv")

    try:
        # ----------
        # Load the master csv
        # calls over to S3 using the aws creds file even though it doesn't use it directly
        df_raw_csv_recs = pd.read_csv(s3_master_csv_path, header=0, encoding="unicode_escape")

        if df_raw_csv_recs.empty:
            RLOG.error("The model catalog appears to be empty or did not load correctly")
            return

        num_raw_csv_rows = len(df_raw_csv_recs)
        RLOG.notice(f"Original row count as loaded is {num_raw_csv_rows}")

        # df_raw_csv_recs["nhdplus_comid"] = df_raw_csv_recs["nhdplus_comid"].astype(str)

        # To cut some volume down, lets get rid of all records where the nhdplus_comid is empty
        # df_filtered_csv_recs = df_raw_csv_recs[df_raw_csv_recs["nhdplus_comid"] != ""]

        # df_raw_csv_recs["nhdplus_comid"].astype(int)

        # Our end report will not have all data, but only remaining error files,
        # so we can drop some records as we go.
        df_filtered_csv_recs = df_raw_csv_recs.dropna(subset=["nhdplus_comid"])

        if df_filtered_csv_recs.empty:
            RLOG.error("No valid records return for after removing recs with an empty ndhplus_comid")
            return

        num_raw_csv_rows_w_comid = len(df_filtered_csv_recs)

        RLOG.notice(
            f"Count of filtered rows with nhdplus_comid not being blank is {num_raw_csv_rows_w_comid}"
        )

        df_filtered_csv_recs = df_filtered_csv_recs.astype({"nhdplus_comid": int})
        # ----------
        # Final csv output (deep copy), we will be adding recs and columns
        df_csv_report = df_filtered_csv_recs.copy()

        # ----------
        # Load the s3 folder list (list of dictionaries)
        # Yes.. I know they technically are not "folders"
        raw_s3_folder_list = s3_sf.get_folder_list(bucket_name, s3_folder_path, True)

        df_raw_s3_folder_list = pd.DataFrame.from_dict(raw_s3_folder_list)

        # remove the "_unprocessed" s3 folder.
        df_raw_s3_folder_list = df_raw_s3_folder_list.drop(
            df_raw_s3_folder_list[df_raw_s3_folder_list["key"] == "_unprocessed"].index
        )

        raw_s3_model_folder_count = len(df_raw_s3_folder_list)

        if raw_s3_model_folder_count == 0:
            raise Exception("No s3 model folders found. Check pathing or configuration.")

        # -----------------
        # Step 1:
        # Look for duplicate "final_name_key" in the df. (exact match at this time)
        df_csv_report = final_name_key_dup_check(df_csv_report)

        # -----------------
        # Step 2:
        # Look for extra records in the master csv that do not have a matching model folder and
        # vice-a-versa.

        # -----------------
        # Step 3:
        # Look for dups for the "initial_scrape_name" column, but only where the ndhplus_comid is not
        # empty.  (Note. we already dropped all empty comid's)

        # -----------------
        # Step 4:
        # Show a count of raw model folders (on screen only).

        # -----------------
        # Step 5:
        # Get the total size of the "model" (s3_models_path) folder (on screen only).

        # -----------------
        # Cleanup
        # We want the final report to only include records that had issues and drop the rest

        RLOG.lprint(f"len of df_csv_report is {len(df_csv_report)}")
        df_csv_report.to_csv(target_report_path, index=False)
        RLOG.notice(f"Raw number of S3 models folders (excluding _unprocessed) = {raw_s3_model_folder_count}")

    except Exception:
        errMsg = "--------------------------------------\n An error has occurred"
        errMsg = errMsg + traceback.format_exc()
        RLOG.critical(errMsg)
        sys.exit(1)

    print()
    RLOG.lprint("--------------------------------------")
    RLOG.success(f"Process completed: {get_stnd_date()}")
    RLOG.success(f"Report csv saved to: {target_report_path}")

    dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


####################################################################
def final_name_key_dup_check(df_csv_report):
    """
    Process:
        Add records to the df_csv_report of full rows where there are duplicates in the final_name_key column
    Output:
        An updated df_csv_report
    """

    # add columns to the dv_csv_report for duplicate final name key
    df_csv_report["has_dup_final_name_key"] = False

    # df_csv_report.sort_values(by=['nhdplus_comid'], inplace=True)
    dups = df_csv_report.groupby(by=["final_name_key"], as_index=False).size()
    dup_list = dups.loc[(dups["size"] > 1)]["final_name_key"].tolist()

    # print(len(dups_purged))
    # print()
    # print(dups_purged)

    if len(dup_list) == 0:
        return df_csv_report

    # print(dups['nhdplus_comid'])

    # Look to see if that row already exists in the df_csv_report.
    # If it does exist, just update the has_dup_final_name_key to True
    # else, add the row, then update it to true.
    # We need to iterate both df's manually as we are possible adding rows
    # that may not yet exist and are not final_name_key dups.

    # for ind in dup_comid_list:
    # df_csv_report.loc[df_csv_report["nhdplus_comid"] == ind]["has_dup_final_name_key"] = True
    # if len(df_csv_report.loc[df_csv_report["nhdplus_comid"] == ind]) > 0:
    #    df_csv_report._set_value(ind, "has_dup_final_name_key", True)

    # df_csv_report['has_dup_final_name_key'] = df_csv_report['nhdplus_comid'].apply(
    #   lambda x: 0 if x.isin(dup_comid_list) else x)

    # df2 = df_csv_report(lambda x: True if x.nhdplus_comid in dup_comid_list else False)
    df_csv_report = df_csv_report.assign(has_dup_final_name_key=lambda x: x["final_name_key"].isin(dup_list))

    return df_csv_report


####################################################################
####  Some validation of input, but also creating key variables ######
def __validate_input(s3_master_csv_path, s3_models_path, output_folder_path):
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

    # will raise it's own exceptions if needed
    bucket_name, s3_folder_path = s3_sf.is_valid_s3_folder(s3_models_path)
    rtn_varibles_dict["bucket_name"] = bucket_name
    rtn_varibles_dict["s3_folder_path"] = s3_folder_path

    # see if the master csv exists
    if s3_sf.is_valid_s3_file(s3_master_csv_path) is False:
        raise ValueError(
            f"The master catalog csv path of {s3_master_csv_path} could not be found in S3."
            " Note: the pathing is case-sensitive"
        )

    return rtn_varibles_dict


####################################################################
if __name__ == "__main__":
    # ---- Samples Inputs
    # All arguments are defaulted, so this is the minimum
    # python s3_model_mgmt.py

    parser = argparse.ArgumentParser(
        description="A tool to help manage the S3 master model csv and model folders",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-csv",
        "--s3_models_path",
        help="OPTIONAL: The full S3 path to the OWP_ras_models folder.\n"
        "ie) s3://ras2fim-dev/OWP_ras_models/my_models\n"
        "Note: it is a case-sensitive value"
        f"Defaults to {sv.S3_OUTPUT_MODELS_FOLDER}",
        default=sv.S3_OUTPUT_MODELS_FOLDER,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-mp",
        "--s3_master_csv_path",
        help="OPTIONAL: The full S3 path to the OWP_ras_models folder.\n"
        "ie) s3://ras2fim-dev/OWP_ras_models/my_models_catalog.csv\n"
        "Note: it is a case-sensitive value\n"
        f"Defaults to {sv.S3_DEFAULT_MODELS_CATALOG_PATH}",
        default=sv.S3_DEFAULT_MODELS_CATALOG_PATH,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-o",
        "--output_folder_path",
        help="OPTIONAL: The local folder location of where the report will be saved.\n"
        "File names (in csv) wil be calculated on the fly (dates/time added).\n"
        "ie) C:\ras2fim_data\tool_outputs\n"
        f"Defaults to {sv.LOCAL_TOOLS_OUTPUT_PATH}",
        default=sv.LOCAL_TOOLS_OUTPUT_PATH,
        required=False,
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

        manage_models(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
