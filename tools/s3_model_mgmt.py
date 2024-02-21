#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import sys
import traceback

import numpy as np
import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import shared_variables as sv
from shared_functions import get_date_time_duration_msg, get_date_with_milli, get_stnd_date


# Global Variables
RLOG = sv.R2F_LOG

"""
This tool does some reconsilation and validation of both the master s3 models catalog and it's
related models folders.

Tests performed:
    1. Show a count of raw model folders. (on screen only)

    2. Look for duplicate "final_name_key" in the master csv. (exact match at this time)

    3. Look for dups for the "initial_scrape_name" column, but only where the ndhplus_comid is not
    empty. (but those would be dropped earlier on)

    4. Look for extra records in the master csv that do not have a matching model folder and
    vice-a-versa.
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

    arg_values = locals().copy()

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
    print()

    # --------------------
    # It will throw it's own exceptions if required
    rd = __validate_input(**arg_values)

    bucket_name = rd["bucket_name"]
    s3_folder_path = rd["s3_models_output_folder"]
    csv_file_name = f"s3_model_mgmt_report_{get_date_with_milli(False)}.csv"
    target_report_path = os.path.join(output_folder_path, csv_file_name)

    try:
        # ----------
        # Load the master csv
        # calls over to S3 using the aws creds file even though it doesn't use it directly
        df_raw_csv_recs = pd.read_csv(s3_master_csv_path, header=0, encoding="unicode_escape")

        if df_raw_csv_recs.empty:
            RLOG.error("The model catalog appears to be empty or did not load correctly")
            return

        num_raw_csv_rows = len(df_raw_csv_recs)

        # Our end report will not have all data, but only remaining error files,
        # so we can drop some records as we go.
        df_csv_report = df_raw_csv_recs.dropna(subset=["nhdplus_comid"])

        if df_csv_report.empty:
            RLOG.error("No valid records return for after removing recs with an empty ndhplus_comid")
            return

        num_raw_csv_rows_w_comid = len(df_csv_report)
        df_csv_report = df_csv_report.astype({"nhdplus_comid": int})

        print("-----------------")
        RLOG.notice("S3 catalog csv stats:")
        RLOG.lprint("  -- Original row count as loaded = " + "{:,}".format(num_raw_csv_rows))
        RLOG.lprint(
            "  -- Count of filtered rows with nhdplus_comid not being blank"
            " = " + "{:,}".format(num_raw_csv_rows_w_comid)
        )

        # ----------
        # Load the s3 folder list (list of dictionaries)
        # Yes.. I know they technically are not "folders"
        raw_s3_folder_list = s3_sf.get_folder_list(bucket_name, s3_folder_path, True)
        raw_s3_model_folder_count = len(raw_s3_folder_list)

        if raw_s3_model_folder_count == 0:
            raise Exception("No s3 model folders found. Check pathing or configuration.")

        df_s3_folder_list = pd.DataFrame.from_dict(raw_s3_folder_list)

        # ----------
        # remove the "_unprocessed" s3 folder.
        df_s3_folder_list = df_s3_folder_list.drop(
            df_s3_folder_list[df_s3_folder_list["key"] == "_unprocessed"].index
        )
        s3_model_folder_count_filtered = len(df_s3_folder_list)

        if s3_model_folder_count_filtered == 0:
            raise Exception("No s3 model folders found after removing _unprocessed")

        print("-----------------")
        RLOG.notice("S3 model folder stats:")
        RLOG.lprint(
            "  -- Raw number of S3 models folders excluding _unprocessed"
            " = " + "{:,}".format(s3_model_folder_count_filtered)
        )

        # -----------------
        # Step 5:
        # Get the total size (in bytes) of the "models" (s3_models_path) folder (on screen only).
        # in MB rounded up
        total_folder_size = s3_sf.get_folder_size(bucket_name, s3_folder_path)
        if total_folder_size > 1000:
            gb_size = total_folder_size / 1028
            disp_size = '{:,.1f}'.format(gb_size) + " gb"
        else:
            disp_size = '{:,.1f}'.format(total_folder_size) + " mb"

        RLOG.lprint(f"  -- Total folder size (inc _unprocessed) = {disp_size}")

        print("")
        print("-----------------")

        # -----------------
        # Step 2:
        # Look for duplicate "final_name_key" in the df. (exact match at this time)
        df_csv_report = dup_check_final_name_key(df_csv_report)

        # -----------------
        # Step 3:
        # Look for dups for the "initial_scrape_name" column, but only where the ndhplus_comid is not
        # empty.  (Note. we already dropped all empty comid's)
        df_csv_report = dup_check_initial_scrape_name(df_csv_report)

        # -----------------
        # Step 4:
        # Look for extra records in the master csv that do not have a matching model folder and
        # vice-a-versa.
        df_csv_report = mismatch_recs(df_csv_report, df_s3_folder_list)

        # -----------------
        # Cleanup
        # We want the final report to only include records that had issues and drop the rest
        df_errors = df_csv_report.loc[
            (df_csv_report["has_dup_final_name_key"] != "False")
            | (df_csv_report["has_dup_initial_scrape_name"] == "True")
            | (df_csv_report["has_model_folder_match"] != "True")
        ]
        # & (df_csv_report["final_name_key"].str.startswith("3_") == False)

        df_errors.to_csv(target_report_path, index=False)

    except Exception:
        errMsg = "--------------------------------------\n An error has occurred"
        errMsg = errMsg + traceback.format_exc()
        RLOG.critical(errMsg)
        sys.exit(1)

    print()
    RLOG.lprint("--------------------------------------")
    RLOG.success(f"Process completed: {get_stnd_date()}")
    RLOG.success(f"Report csv saved to: {target_report_path}")
    dur_msg = get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


####################################################################
def dup_check_final_name_key(df_csv_report):
    """
    Process:
        Update records to the df_csv_report where there are duplicates in the final_name_key column.
        A new column of "has_dup_final_name_key" (True/False) will be added.
    Output:
        An updated df_csv_report
    """

    RLOG.lprint("Checking for duplicates in the final_name_key column")

    # add columns to the dv_csv_report for duplicate final name key
    df_csv_report["has_dup_final_name_key"] = "False"
    df_csv_report["has_dup_final_name_key"] = df_csv_report["has_dup_final_name_key"].astype(str)

    # we need to drop the empty final_name_key records first before doing a groupby
    df_dup_adj = df_csv_report.dropna(subset=["final_name_key"])

    dups = df_dup_adj.groupby(by=["final_name_key"], as_index=False).size()
    dup_list = dups.loc[(dups["size"] > 1)]["final_name_key"].tolist()

    def _calc_dup(row):
        if (pd.isnull(row["final_name_key"])) or (row["final_name_key"] == ""):
            return "skipped"
        elif row["final_name_key"] in dup_list:
            return "True"
        else:
            return "False"

    df_csv_report["has_dup_final_name_key"] = df_csv_report.apply(_calc_dup, axis=1)

    return df_csv_report


####################################################################
def mismatch_recs(df_csv_report, df_raw_s3_folder_list):
    """
    Process:
        - Look for matchs of recs to model folders.
        - Records where the final_name_key is empty will be skipped.
        - A new column of "has_model_folder_match" ("true", "csv_only", "skipped", "model_only")
          will be added.
             - "true" means final_name_key and model folder match found.
             - "csv_only" means in the final_name_key but no model folder found.
             - "skipped" means final_name_key is empty.
             - "model_only" means no matching final_name_key found. We will add a partial new row
               to the csv with
        - If a model folder exists but not a csv rec, then a new rec will be added
          to the csv, with a new column named "s3_model_folder_name" (blank if matched)
    Output:
        An updated df_csv_report
    """

    RLOG.lprint("Looking for mismatches between the csv and the model folders")

    # add two new columns to the dv_csv_report
    df_csv_report["has_model_folder_match"] = ""
    df_csv_report["unmatched_s3_model_folder_name"] = ""

    # If the final_name_key is the value of "skipped", the the has_model_folder_match
    # value should be skipped as well and not attempt to do the merge tests
    df_csv_report["has_model_folder_match"] = np.where(
        df_csv_report["has_dup_final_name_key"] == "skipped", "skipped", ""
    )

    # Merge outer which means there could be extra rows for either df's. The 'indictator=True'
    # flag helps us figure out who matches and who is extra from either dataframe.
    # A new column is added to the df_merged named '_merge' which has one of these values
    # "both", "left_only" or "right_only".
    df_merged = pd.merge(
        df_csv_report,
        df_raw_s3_folder_list,
        left_on="final_name_key",
        right_on="key",
        how="outer",
        indicator=True,
    )

    # drop rows from df_merged that already have the word "skipped" in the has_model_folder_match column
    df_merged = df_merged.drop(df_merged[df_merged["has_model_folder_match"] == "skipped"].index)

    # Updating the has_model_folder_match to "true" when there is a match. Remember, some rows might
    # already have the value of "skipped" if there was no value in the final_name_key column
    df_matched_rows = df_merged[
        ((df_merged["_merge"] == "both") & (df_merged["has_model_folder_match"] == ""))
    ]
    # matched_rows = merged[merged["_merge"] == "both"]
    if len(df_matched_rows) > 0:
        df_csv_report.loc[
            df_csv_report["final_name_key"].isin(df_matched_rows["final_name_key"]), "has_model_folder_match"
        ] = "True"

    # Record has no matching model folder, so we put "csv_only" in it
    df_matched_rows = df_merged[
        ((df_merged["_merge"] == "left_only") & (df_merged["has_model_folder_match"] == ""))
    ]
    if len(df_matched_rows) > 0:
        df_csv_report.loc[
            df_csv_report["final_name_key"].isin(df_matched_rows["final_name_key"]), "has_model_folder_match"
        ] = "csv_only"

    # Record has a model folder but now csv record, so we have to add new rows to the csv
    # which show only the two columns listed below.
    df_matched_rows = df_merged[df_merged["_merge"] == "right_only"]
    if len(df_matched_rows) > 0:
        for ind, row in df_matched_rows.iterrows():
            new_row = {"has_model_folder_match": "model_only", "unmatched_s3_model_folder_name": row["key"]}
            df_csv_report = pd.concat([df_csv_report, pd.DataFrame([new_row])], ignore_index=True)

    return df_csv_report


####################################################################
def dup_check_initial_scrape_name(df_csv_report):
    """
    Process:
        Update records to the df_csv_report where there are duplicates in the initial_scrape_name column.
        A new column of "has_dup_initial_scrape_name" (True/False) will be added.
    Output:
        An updated df_csv_report
    """

    RLOG.lprint("Checking for duplicates in the initial_scrape_name column")

    # add columns to the dv_csv_report for duplicate final name key
    df_csv_report["has_dup_initial_scrape_name"] = "False"

    dups = df_csv_report.groupby(by=["initial_scrape_name"], as_index=False).size()
    dup_list = dups.loc[(dups["size"] > 1)]["initial_scrape_name"].tolist()

    if len(dup_list) == 0:
        return df_csv_report

    df_csv_report = df_csv_report.assign(
        has_dup_initial_scrape_name=lambda x: x["initial_scrape_name"].isin(dup_list)
    )

    df_csv_report["has_dup_initial_scrape_name"] = df_csv_report["has_dup_initial_scrape_name"].astype(str)

    return df_csv_report


####################################################################
####  Some validation of input, but also creating key variables ######
def __validate_input(s3_master_csv_path, s3_models_path, output_folder_path):
    """
    Process:
        Some variables need to be adjusted and some new derived variables are created
        dictionary (key / pair) will be returned
    """

    rtn_dict = {}

    # ---------------
    # why is this here? might not come in via __main__
    if output_folder_path == "":
        raise ValueError("Output folder path parameter value can not be empty")

    # Skip tests for folder output path because logging for this script
    # already added it (same path as logs)

    if s3_sf.is_valid_s3_folder(s3_models_path) is False:
        raise ValueError(f"S3 models path ({s3_models_path}) does not exist")

    bucket_name, s3_output_folder = s3_sf.parse_bucket_and_folder_name(s3_models_path)
    rtn_dict["bucket_name"] = bucket_name
    rtn_dict["s3_models_output_folder"] = s3_output_folder
    # see if the master csv exists
    if s3_sf.is_valid_s3_file(s3_master_csv_path) is False:
        raise ValueError(
            f"The master catalog csv path of {s3_master_csv_path} could not be found in S3."
            " Note: the pathing is case-sensitive"
        )

    return rtn_dict


####################################################################
if __name__ == "__main__":
    # ***********************
    # This tool is intended for NOAA/OWP staff only as it requires access to an AWS S3 bucket with a
    # specific folder structure.
    # If you create your own S3 bucket in your own AWS account, you are free to use this tool.
    # ***********************

    # ---- Samples Inputs
    # All arguments are defaulted, so this is the minimum
    #   python s3_model_mgmt.py
    # All args used
    #   python s3_model_mgmt.py
    #       -csv s3://ras2fim-dev/OWP_ras_models/rob_test_small_model_catalog.csv
    #       -mp s3://ras2fim-dev/OWP_ras_models/models/
    #       -o C:\ras2fim_data\temp

    parser = argparse.ArgumentParser(
        description="A tool to help manage the S3 master model csv and model folders",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-mp",
        "--s3_models_path",
        help="OPTIONAL: The full S3 path to the OWP_ras_models folder.\n"
        "ie) s3://ras2fim-dev/OWP_ras_models/my_models\n"
        "Note: it is a case-sensitive value\n"
        f"Defaults to {sv.S3_OWP_RAS_MODELS_FOLDER}",
        default=sv.S3_OWP_RAS_MODELS_FOLDER,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-csv",
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
