#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import shutil
import sys
import time
import traceback

import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import ras2fim_logger
import shared_validators as val
import shared_variables as sv
from shared_functions import get_stnd_date, print_date_time_duration


# Global Variables
RLOG = ras2fim_logger.R2F_LOG


"""
This tool uses a HUC8 number to call over to an AWS S3 models_catalog.csv and will scan
all records in the models catalog, HUC8's column looking for matches.

NOTE: This script is primarily designed for NOAA/OWP use, but if you have access to your own
S3, models_catalog.csv and models folders, you are welcome to use it. We can not grant access to our
NOAA / OWP S3 bucket at this time.

To run this tool, you must have already ran 'aws configure' and added creds to the S3 bucket. You should
find that you only need to setup yoru machine once with 'aws configure' and not each time you use this tool.


Features for this tool include:
    - Ability to create a list only (csv) without actual downloads (test download).

    - A log file for the output is created with unique date/time stamp.

    - A "verbose" flag can be optionally added as an argument for additional processing details
         (note: don't over use this as it can make errors harder to find). To find an error, simply
         search the log for the word "error"

    - Filters downloads from the src models catalog to look for status of "ready" only. Also filters out
         records where the final_name_key starts with either "1_", "2_" or "3_".

    - This script can find huc numbers in the models catalog "hucs" field regardless of string
         format in that column.
         It does assume that models catalog has ensure that leading zero's exist.

    - This script further filters model records to the matching crs value. Why? ras2fim.py has an input param
         for its crs (-p) and only models matching that crs can be processed at one time. If we have one HUC
         that has some models in one projection and some models in another projection, they must be processed
         via ras2fim.py separately with separate output folders.

    - As the models folders are about to be downloaded, all subfolders in the local "models" folder
         will first be removed, as ras2fim.py processes all folders in that directory.

    - It creates a filtered copy of the S3 OWP_ras_models_catalog.csv showing all folders downloaded, if
         they were successful or not and if not, why. Local models catalog file name will
         be "OWP_ras_models_catalog_{HUC}.csv"

"""


# -------------------------------------------------
class Get_Models_By_Catalog:

    # -------------------------------------------------
    # default values listed in "__main__"  but also here in case code calls direct..
    # aka. not through "__main__"
    def get_models(
        self,
        s3_path_to_catalog_file,
        huc_number,
        projection,
        list_only=False,
        target_owp_ras_models_path=sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
        target_owp_ras_models_csv_file=sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
        is_verbose=False,
    ):
        """
        Overview  (and Processing Steps)
        -----------------------
        - Calls over to S3 using a save aws configure creds file (needs to be in place prior to the run)

        - Reads model catalog csv (from s3_path_to_catalog), looking through the HUC list for matches.
          e.g. OWP_ras_models\OWP_ras_models_catalog.csv

        - If downloading folders...  (ie.. not just the list only)
            - Empty the OWP_ras_models\models (or target models path) folder as we know ras2fim will
              automatically read all folders in that directory.
            - Download all of the folders found using data extracted from columns in the filtered
              OWP_ras_models_catalog.csv file. The S3 "models" folder, MUST exist beside the model
              catalog file.

        - Uses defined filters against the OWP_ras_models_catalog.csv to calculate which models
          to download.

        - If the target_owp_ras_models_csv_file exists, it will be overwritten without warning.

        Inputs
        -----------------------
        - s3_path_to_catalog_file (str) : e.g. s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
          (left as full pathing in case of pathing changes).

        - huc_number (string)

        - projection (string): To calculate which model folders are to be downloaded, you need
          the HUC number and the case-senstive CRS number. The projection value must match the
          'crs' column in the master copy in S3  (OWP_ras_models_catalog.csv)

        - list_only (True / False) : If you override to true, you can get just a csv list and
          not the downloads.

        - target_owp_ras_models_path (str): Local path you want the model folders to be loaded to.
          Default: c\ras2fim_data\OWP_ras_models\models.

        - target_owp_ras_models_csv_file (str): The file name and path of where to save the
          filtered catalog.
          Default: c\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog_[huc number].csv.

        Outputs
        -----------------------
        - All of the filtered actual folders found in the s3 OWP_ras_models based on the models catalog
            are placed in the local emptied (folders only) target_owp_ras_models_path (assuming not
            overwritten to list only).

        - A filtered copy of the "OWP_ras_models_catalog.csv" from S3 will be placed in as set
            by target_owp_ras_models_csv_file and includes list of all downloaded files and their
            attributes. If not overridden, the name will become OWP_ras_models_catalog_{HUC}.csv.
            This file will overwrite a pre-existing file of the same name.
            The new OWP_ras_models_catalog_{HUC}.csv is required for ras2fim.py processing.

        """

        # ----------
        # Validate inputs
        self.__validate_inputs(
            s3_path_to_catalog_file,
            huc_number,
            projection,
            target_owp_ras_models_path,
            target_owp_ras_models_csv_file,
            list_only,
        )

        # NOTICE... logger setup after validation? might get logger errors
        # as the logger needs key values
        # We really don't need to log validation errors
        self.__setup_logs()

        print()
        start_dt = dt.datetime.utcnow()

        RLOG.lprint("****************************************")
        RLOG.notice(" Get ras models folders from s3")
        RLOG.lprint(f" Started (UTC): {get_stnd_date()}")
        RLOG.lprint(f" -- HUC: {huc_number}")
        RLOG.lprint(f" -- CRS: {projection}")
        RLOG.lprint("")
        RLOG.lprint(f" -- Source download path for models: {self.src_owp_model_folder_path}")
        RLOG.lprint(f" -- Target file name and path for the filtered csv: {self.target_filtered_csv_path}")
        RLOG.lprint(f" -- Target path for models: {self.target_owp_ras_models_path}")
        RLOG.lprint(f" -- List only is {list_only}")
        RLOG.lprint("")

        self.list_only = list_only
        self.is_verbose = is_verbose

        # setup an empty variable for scope reasons
        self.df_filtered = pd.DataFrame()
        try:
            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly
            df_all = pd.read_csv(self.s3_path_to_catalog_file, header=0, encoding="unicode_escape")

            if df_all.empty:
                RLOG.error("The model catalog appears to be empty or did not load correctly")
                return

            df_all["nhdplus_comid"] = df_all["nhdplus_comid"].astype(str)

            if self.is_verbose is True:
                RLOG.debug(f"models catalog raw record count = {len(df_all)}")

            # ----------

            filter_msg = (
                "After filtering, there are no valid remaining models to process.\n\n"
                "  Filtering is done via the following rules:\n"
                "    - The submitted huc must be found in catalog 'hucs' field with a matching"
                " crs value (case-sensitive)\n"
                "    - Status has to be 'ready'\n"
                "    - The final_name_key column must not start with the values of 1__, 2__ or 3__\n"
                "  1__, 2__ and 3__ are filtered as required by the pre-processing team.\n"
            )

            # ***** FILTERS  ******
            # look for records that are ready, contains the huc number and does not start with 1_, 2_ or 3_
            # NOTE: for some reason if you change the lines below, now that the pattern of:
            # df_all["final_name_key"].str.startswith("1_") is False).. it fails (not sure why)
            # So I keep the pattern of == False.
            # #### The E712 override has been added to the toml file.
            df_huc = df_all.loc[
                (df_all["status"] == "ready")
                & (df_all["final_name_key"].str.startswith("1_") == False)
                & (df_all["final_name_key"].str.startswith("2_") == False)
                & (df_all["final_name_key"].str.startswith("3_") == False)
                & (df_all["hucs"].str.contains(str(self.huc_number), na=False))
            ]

            if df_huc.empty:
                RLOG.error(filter_msg)
                return

            # ----------
            # Now filter based on CRS
            self.df_filtered = df_huc.loc[(df_huc["crs"] == self.projection)]
            if self.df_filtered.empty:
                RLOG.error(filter_msg)
                return

            # Adding a model_id column starting at number 10001 and incrementing by one
            # Adding new column
            self.df_filtered.sort_values(by=[sv.COL_NAME_FINAL_NAME_KEY], inplace=True)
            self.df_filtered.insert(0, sv.COL_NAME_MODEL_ID, range(10001, 10001 + len(self.df_filtered)))

            if self.is_verbose is True:
                # to see the huc list without column trucations (careful as this could be a huge output)
                with pd.option_context("display.max_columns", None):
                    print("df_huc list")
                    pd.set_option("display.max_colwidth", None)
                    print(self.df_filtered)
                    # don't log this

            # first add two columns, one to say if download succesful (T/F), the other to say
            # download fail reason
            pd.options.mode.chained_assignment = None
            # self.df_filtered.loc[:, COL_NAME_DOWNLOAD_SUCCESS] = ""
            # self.df_filtered.loc[:, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = ""

            # we will save it initially but update it and save it again as it goes
            self.df_filtered.to_csv(self.target_filtered_csv_path, index=False)

            # RLOG.lprint(f"Filtered model catalog saved to : {self.target_filtered_csv_path}")
            # print("")
            # print(
            #    "Note: The csv represents all filtered models folders that are pending to be"
            #    " downloaded.\nThe csv will be updated with statuses after downloads are complete."
            # )

            # ----------
            # If inc_download_folders, otherwise we just stop.  Sometimes a list is wanted but
            # not the downloads
            if self.list_only is True:
                print()
                RLOG.notice("List only as per (-f) flag - no downloads attempted")
                RLOG.notice(f"... {len(self.df_filtered)} valid filtered model records")
            else:
                RLOG.lprint("--------------------------------------")
                # Perform the actual download
                self.df_filtered = s3_sf.download_folders(
                    self.src_owp_model_folder_path,
                    self.target_owp_ras_models_path,
                    self.df_filtered,
                    "final_name_key",
                )

                RLOG.lprint("--------------------------------------")
                cnt = self.df_filtered[sv.COL_NAME_DOWNLOAD_SUCCESS].value_counts()[True]
                self.num_success_downloads = cnt

                if self.num_success_downloads == 0:
                    RLOG.error(
                        "All model download attempts have failed."
                        " Please review the output logs or the filtered csv for skip/error details."
                    )
                else:
                    num_skips = len(self.df_filtered) - self.num_success_downloads
                    RLOG.success(
                        f"Number of models folders successfully downloaded: {self.num_success_downloads}"
                    )
                    if num_skips > 0:
                        RLOG.warning(
                            f"Number of models folders skipped / errored during download: {num_skips}."
                            "Please review the output logs or the filtered csv for skip/error details."
                        )

        except Exception:
            errMsg = "--------------------------------------\n An error has occurred"
            errMsg = errMsg + traceback.format_exc()
            RLOG.critical(errMsg)
            sys.exit(1)

        # resaved with the updates to the download columns
        self.df_filtered.to_csv(self.target_filtered_csv_path, index=False)

        RLOG.lprint("--------------------------------------")
        RLOG.success(f"Get ras models completed: {get_stnd_date()}")
        RLOG.success(f"Filtered model catalog saved to : {self.target_filtered_csv_path}")

        dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        print()

    # -------------------------------------------------
    def __validate_inputs(
        self,
        s3_path_to_catalog_file,
        huc_number,
        projection,
        target_owp_ras_models_path,
        target_owp_ras_models_csv_file,
        list_only,
    ):
        """
        Validates input but also sets up key variables

        If errors are found, an exception will be raised.
        """

        # ---------------
        if huc_number is None:  # Possible if not coming via the __main__ (also possible)
            raise ValueError("huc number not set")

        if len(str(huc_number)) != 8:
            raise ValueError("huc number is not eight characters in length")

        if huc_number.isnumeric() is False:
            raise ValueError("huc number is not a number")

        self.huc_number = huc_number

        # ---------------
        is_valid, err_msg, crs_number = val.is_valid_crs(projection)
        if is_valid is False:
            raise ValueError(err_msg)

        self.projection = projection
        self.crs_number = crs_number

        # ---------------
        if target_owp_ras_models_csv_file == "":
            raise ValueError("target_owp_ras_models_csv_file can not be empty")

        # ---------------
        target_owp_ras_models_path = target_owp_ras_models_path.replace("/", "\\")
        self.target_owp_ras_models_path = target_owp_ras_models_path
        self.target_parent_path = os.path.dirname(target_owp_ras_models_path)

        if list_only is False:
            if os.path.exists(self.target_owp_ras_models_path):
                shutil.rmtree(self.target_owp_ras_models_path)
                # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
                # so sometimes mkdir can fail if rmtree isn't done
                time.sleep(1)  # 1 seconds

            os.mkdir(self.target_owp_ras_models_path)

        # the string may or may not have the [] and that is ok.
        self.target_filtered_csv_path = target_owp_ras_models_csv_file.replace("[]", huc_number)

        # ---------------
        # Extract the base s3 bucket name from the catalog pathing.
        # temp remove the s3 tag
        self.s3_path_to_catalog_file = s3_path_to_catalog_file

        # we need the "s3 part stipped off for now"
        adj_s3_path = self.s3_path_to_catalog_file.replace("s3://", "")
        path_segs = adj_s3_path.split("/")
        self.bucket_name = f"s3://{path_segs[0]}"

        # lets join the path back together (pulling off the model catalog file as well)
        self.src_owp_model_folder_path = ""
        for index, segment in enumerate(path_segs):
            # drop the first and last (the last segment was the model catalog file name)
            if (index == 0) or (index == len(path_segs) - 1):
                continue
            self.src_owp_model_folder_path += segment + "/"

        # The "models" must be beside the models catalog
        self.src_owp_model_folder_path = self.bucket_name + "/" + self.src_owp_model_folder_path
        self.src_owp_model_folder_path += "models"

        # validate that the bucket and folders (prefixes exist)
        if s3_sf.is_valid_s3_folder(self.src_owp_model_folder_path) is False:
            raise ValueError(f"S3 output folder of {self.src_owp_model_folder_path} ... does not exist")

    # -------------------------------------------------
    def __setup_logs(self):
        start_time = dt.datetime.utcnow()
        file_dt_string = start_time.strftime("%y%m%d-%H%M")

        # -------------------
        # setup the logging class (default unit folder path (HUC/CRS))
        file_name = f"get_models_{self.huc_number}_{self.crs_number}-{file_dt_string}.log"
        RLOG.setup(os.path.join(self.target_parent_path, "logs", file_name))


# -------------------------------------------------
if __name__ == "__main__":
    # ***********************
    # This tool is intended for NOAA/OWP staff only as it requires access to an AWS S3 bucket with a
    # specific folder structure.
    # If you create your own S3 bucket in your own AWS account, you are free to use this tool.
    # ***********************

    # ----------------------------
    # Sample Usage (min required args) (also downloads related folders)
    # python3 ./tools/get_ras_models_by_catalog.py -u 12090301 -p ESRI:102739
    #  -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv

    # Sample Usage with most params
    # python3 ./tools/get_ras_models_by_catalog.py -u 12090301 -p ESRI:102739
    #  -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
    #  -tm c:\\ras2fim\\ras_models -tcsv c:\\ras2fim\ras_models\12090301_models_catalog.csv -v

    # Sample Usage to get just the list and not the model folders downloaded (and most defaults)
    # python3 ./tools/get_ras_models_by_catalog.py -u 12090301 -p EPSG:2277
    #   -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv -f

    # ----------------------------
    # NOTE: You need to have aws credentials already run on this machine, at least once
    # per server. This is done usually using `aws configure`.
    # If has been run on this machine before, you will find a config file and credentials file
    #  in your root /.aws directory. ie) /users/{your user name}/.aws

    # NOTE: This script assumes that each of the downloadable "model" folders are in subfolder
    # named "models" and under where the -s / --sub_path_to_catalog file is at.
    # e.g.  s3://(some bucket)/OWP_ras_models/OWP_ras_models_catalog.csv with a "models" folder beside
    # it with all of the "model" folders under that.
    #     ie)  s3://(some bucket)/OWP_ras_models/OWP_ras_models_catalog.csv
    #                                           /models/
    #                                                   /(model number 1 folder)
    #                                                   /(model number 2 folder)
    #                                                   /(model number 3 folder)
    #                                                   /etc
    # And of course, your path doesn't have to match, but the pattern of the csv, "models" folder, and
    # subfolders is critical

    parser = argparse.ArgumentParser(
        description="Communication with aws s3 data services to download OWP_ras_model folders by HUC"
    )

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
    # Note: the actual models folders are assumed to be a folder named "models" beside the models_catalog.csv
    parser.add_argument(
        "-s",
        "--s3_path_to_catalog_file",
        help="REQUIRED: S3 path and file name of models_catalog.",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-u",
        "--huc_number",
        help="REQUIRED: Download model records matching this provided number",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-p",
        "--projection",
        help="REQUIRED: All model folders have an applicable crs value, but one HUC might have some"
        "  models in one crs and another set of models in a different crs. This results in one HUC having"
        " to be processed twice (one per crs).\n\nPlease enter the crs value from the"
        "  OWP_ras_models_catalog.csv to help filter the correct models you need."
        " Example: EPSG:2277 (case sensitive)",
        required=True,
        metavar="",
        type=str,
    )

    parser.add_argument(
        "-f",
        "--list_only",
        help="OPTIONAL: Adding this flag will result in a log file only with the list of potential "
        " downloadable folders, but not actually download the folders. Default = False (download files)",
        required=False,
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-tcsv",
        "--target_owp_ras_models_csv_file",
        help="OPTIONAL: Path and file name of where to save the filtered models catalog csv."
        " Defaults = c:\\ras2fim_data\\OWP_ras_models\\OWP_ras_models_catalog_[huc number].csv.",
        required=False,
        default=sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
        metavar="",
    )

    parser.add_argument(
        "-tm",
        "--target_owp_ras_models_path",
        help="OPTIONAL: Where to download the model folders such the root ras2fim owp_ras_models folders."
        " Defaults = c:\\ras2fim_data\\OWP_ras_models\\models",
        required=False,
        default=sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
        metavar="",
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

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # RLOG setup inside the main program

        # call main program
        obj = Get_Models_By_Catalog()
        obj.get_models(**args)

    except Exception:
        # The logger does not get setup until after validation, so you may get
        # log system errors potentially when erroring in validation
        RLOG.critical(traceback.format_exc())

