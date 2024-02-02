#!/usr/bin/env python3

import argparse
import datetime as dt
import glob
import os
import shutil
import sys
import traceback

import colored as cl
import geopandas as gpd
import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import ras2fim_logger
import shared_variables as sv
from shared_functions import get_date_with_milli, get_stnd_date, print_date_time_duration

# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def create_ras2release(release_name, local_ras2release_path, s3_path_to_output_folder,
                        s3_ras2release_path, skip_save_to_s3):
    """
    # TODO - WIP
    Processing:
        - load the config/ras2release_download_items.lst
            TODO: should we? we process most of them?
            AND: How do we know which folder to output, HydroVIS, FIM
        - 
    Inputs:
        - release_name: e.g. r102
        - local_ras2release_path: e.g. c:/my_ras2fim_dir/releases/
        - s3_path_to_output_folder: e.g. s3://my_ras2fim_bucket/output_ras2fim
        - s3_ras2release_path: e.g. s3://my_ras2fim_bucket/ras2release/
    Outputs:
    """

    start_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    # Todo: migth need some validatation here first ??
    #s3_release_path = s3_ras2release_path + "/" + rel_name

    print("")
    RLOG.lprint("=================================================================")
    RLOG.notice("          RUN ras2release ")
    RLOG.lprint(f"  (-r): release path and folder name = {release_name} ")
    RLOG.lprint(f"  (-l): local ras2release path  = {local_ras2release_path}")    
    RLOG.lprint(f"  (-s): s3 path to output folder name  = {s3_path_to_output_folder}")
    RLOG.lprint(f"  (-t): s3 ras2release path  = {s3_ras2release_path}/(rel_name)")
    RLOG.lprint(f"  (-ss): Skip saving release results to S3 = {skip_save_to_s3}")

    # validate input variables and setup key variables
    # rd = Variables Dictionary
    rd = __validate_input(release_name, s3_path_to_output_folder, local_ras2release_path,
                          s3_ras2release_path, skip_save_to_s3)

    local_rel_folder = rd["local_target_rel_path"]
    local_rel_units_folder = os.path.join(local_rel_folder, "units")
    s3_unit_output_bucket_name = rd["s3_unit_output_bucket_name"]
    s3_unit_output_folder = rd["s3_unit_output_folder"]

    if skip_save_to_s3 is False:
        s3_release_bucket_name = rd["s3_rel_bucket_name"]
        
    print()
    RLOG.lprint(f"Started (UTC): {dt_string}")
    local_unit_folders = __download_units_from_s3 (s3_unit_output_bucket_name,
                                                   s3_unit_output_folder, local_rel_units_folder) 


    # __load_hydrovis_folder
        # __process_geocurves()
            # Just merge

    # __load_fim_folder
        #__process_rating_curves(src_rel_unit_dirs, local_rel_folder)
        
    #if skip_save_to_s3 is True:
    # __save_to_s3(local_wip_folder)

    print()
    print("===================================================================")
    print("ras2release processing complete")
    end_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    print(f"Ended (UTC): {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()


# -------------------------------------------------
def __download_units_from_s3(bucket, s3_path_to_output_folder, local_rel_units_folder):

    """
    Process:
        - Get a list of the output units folder (without the "final" subfolder).
        - Then iterate through them to ensure they all have "final" folders and error ??
        if one does not.
        - Download specific folders and files from their repective "unit" local folders.
    Inputs:
        - bucket: ras2fim-dev
        - s3_path_to_output_folder: e.g. output_ras2fim
        - local_rel_units_folder: e.g. c:/my_ras2fim_dir/ras2fim_releases/r102-test/units
    Output:
        - returns a list of full pathed local unit folders:
          e.g c:/my_ras2fim_dir/ras2fim_releases/r102-test/units/12090301_2277_ble_230923
    """

    print()
    RLOG.lprint("Downloading all s3 unit output folder, but just the 'final' directories")
    print()

    # Get list of output folders (not counting the "final") folders. 
    # We are looking for any output folders that are missing "final" folders are invalid
    # e.g. each item is : output_ras2fim\12090301_2277_ble_230923
    s3_output_unit_folders = s3_sf.get_folder_list(bucket, s3_path_to_output_folder, False)
    if len(s3_output_unit_folders) == 0:
        RLOG.critical(f"No output unit folders were found at {s3_path_to_output_folder}")

    # Create a list of folder paths that have a "final" folder.
    s3_final_folders = []
    for unit_folder in s3_output_unit_folders:
        # we temp add the word "/final" of it so it directly compares to the unit file folder list.
        unit_folder_name = unit_folder["key"]
        full_unit_path = f"{unit_folder['url']}/final"
        
        unit_final_folder = f"{s3_path_to_output_folder}/{unit_folder_name}/final"

        if s3_sf.is_valid_s3_folder(full_unit_path) is False:
            RLOG.warning(f"{s3_path_to_output_folder}/{unit_folder_name} did not"
                         " have a 'final' folder and was skipped.")

        local_target_path = os.path.join(local_rel_units_folder, unit_folder_name)

        item = {"bucket_name": bucket,
                "folder_id": unit_folder_name,
                "s3_src_folder": unit_final_folder,
                "target_local_folder": local_target_path,
            }
        
        s3_final_folders.append(item)
        
    print(f"{cl.fg('light_yellow')}"
          f"Downloading units to {local_rel_units_folder}\n\n"
          f"Note: Downloading can be pretty slow based on the number of unit folders"
           " and the amount of files in them.\n\nIt can take 3 - 10 minutes or more per unit.\n"
           "Multi-threading is included to help download the files as fast as we reasonably can."
          f"{cl.attr(0)}")
    print()

    # returns a list of dictionaries, with the following schema
    # - "folder_id": folder_name or any unique value. e.g. 12030105_2276_ble_230923
    # - "download_success" as either
    #    the string value of 'True' or 'False'
    # - "error_details" - why did it fail
    # For out situation here, it already logged and error and continued. We will not abort the set
    # We don't want to abort the set. But we do want to iterate to see who is still successfull
    rtn_download_details = s3_sf.download_folders(s3_final_folders)
    rtn_unit_folders = []
    for item in rtn_download_details:
        if item["download_success"] == "True": # yes.. string version
            rtn_unit_folders.append(os.path.join(local_rel_units_folder, item["folder_id"]))

    return rtn_unit_folders


# -------------------------------------------------
####  Some validation of input, but also creating key variables ######
def __validate_input(rel_name, s3_path_to_output_folder, local_working_folder_path,
                      s3_ras2release_path, skip_save_to_s3):
    """
    Summary: Will raise Exception if some are found

    Inputs:
        - release_name: e.g. r102
        - local_ras2release_path: e.g. c:/my_ras2fim_dir/releases/
        - s3_path_to_output_folder: e.g. s3://my_ras2fim_bucket/output_ras2fim
        - s3_ras2release_path: e.g. s3://my_ras2fim_bucket/ras2release/

    Output: dictionary
    """

    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    rtn_dict = {}

    # ----------------
    if rel_name == "":
        raise ValueError("rel_name (-r) can not be empty")

    # ----------------
    # test s3 bucket and paths (it will automatically throw exceptions)
    if s3_sf.is_valid_s3_folder(s3_path_to_output_folder) is False:
        raise ValueError(f"S3 path to outputs ({s3_path_to_output_folder}) does not exist")

    
    bucket_name, s3_output_folder = s3_sf.parse_bucket_and_folder_name(s3_path_to_output_folder)
    rtn_dict["s3_unit_output_bucket_name"] = bucket_name
    rtn_dict["s3_unit_output_folder"] = s3_output_folder

    if skip_save_to_s3 is False:
        if s3_sf.is_valid_s3_folder(s3_ras2release_path) is False:
            raise ValueError(f"S3 path to releases ({s3_ras2release_path}) does not exist")

        bucket_name, s3_output_folder = s3_sf.parse_bucket_and_folder_name(s3_ras2release_path)
        rtn_dict["s3_rel_bucket_name"] = bucket_name
        rtn_dict["s3_rel_folder"] = s3_output_folder

    # ----------------
    # ie) c:/ras2fim_data/ras2release/temp/r200
    local_target_rel_path = os.path.join(local_working_folder_path, rel_name)
    rtn_dict["local_target_rel_path"] = local_target_rel_path

    if os.path.exists(local_target_rel_path):

        # TODO: Ask.. it exists, overwrite?

        shutil.rmtree(local_target_rel_path, ignore_errors=True)
    else:
        os.makedirs(local_target_rel_path, exist_ok=True)

    return rtn_dict


# -------------------------------------------------
# def __process_geocurves():


# -------------------------------------------------
def __process_domain_models(local_src_unit_paths, local_wip_folder):

    # TODO: WIP

    print()
    print("*** processing domain models")

    output_folder = os.path.join(local_wip_folder, "domain_models")
    # TODO: incoming files have been renamed.
    merged_domain_model_file = os.path.join(output_folder, "ras2fim_domain_models.gpkg")

    # ----------------
    # Copy all domain model geopackages from
    # the local output unit folders to the ras2release/{rel version}/domain_models folder

    # ----------------
    # Iterate and join the files
    model_files = glob.glob(os.path.join(output_folder, "*_models_domain.gpkg"))

    # TODO: This might have to move to Parquet or similar as gpkg have a 2GB limit, in FIM, we have a
    # parquet file per HUC8. inputs/rating_curve/water_edge_database/calibration_points/

    merged_gkpg_crs = sv.DEFAULT_OUTPUT_CRS

    merged_gkpg = None

    # Add manual tqdm (not multi proc or multi thread)
    # Iterate through input geopackages and compile them
    for i in range(len(model_files)):
        if i == 0:
            # we have to load the first gkpg directly then concat more after.
            # Create an empty GeoDataFrame to store the compiled data
            gkpg_raw = gpd.read_file(model_files[i])
            merged_gkpg = gkpg_raw.to_crs(merged_gkpg_crs)
        else:
            gkpg_raw = gpd.read_file(model_files[i])
            gkpg_adj = gkpg_raw.to_crs(merged_gkpg_crs)
            merged_gkpg = pd.concat([merged_gkpg, gkpg_adj], ignore_index=True)

    merged_gkpg.to_file(merged_domain_model_file, driver="GPKG")


# -------------------------------------------------
# def __process_models_used():


# -------------------------------------------------
def __process_rating_curves(src_rel_unit_dirs, local_rel_folder):
    # TODO: WIP

    """
    Summary:
    Inputs:
        - src_rel_unit_dirs: A list of all of the downloaded unit "final" folders
        - local_rel_folder: The new parent rel folder
    """

    print()
    print("*** processing rating curves")

    output_folder = os.path.join(local_rel_folder, "calibration_rating_curves")

    # TODO: Add rel version name into file?
    merged_rc_table_file = os.path.join(output_folder, "reformat_ras_rating_curve_table.csv")
    merged_rc_points_file = os.path.join(output_folder, "reformat_ras_rating_curve_points.gpkg")

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)

    os.mkdir(output_folder)

    # ----------------
    # Iterate and join the table files
    calib_dir_name = "ras2calibration"
    print(f"calibration rating curve output folder is {output_folder}")

    rc_table_files = []
    for unit_dir, full_src_unit_path in src_rel_unit_dirs.items():
        src_rc_table_file = os.path.join(full_src_unit_path, calib_dir_name,
                                         "ras2calibration_rating_curve_table.csv")
        if os.path.isfile(src_rc_table_file):
            rc_table_files.append(src_rc_table_file)

    if len(rc_table_files) == 0:
        raise Exception("Error: No ras2calibration_rating_curve_table.csv files found.")

    # append the CSV files
    # TODO: Make this a loop and tdqm manually in place (not multi thread or multi proc)
    # but manual tdqm so we can watch progress.
    # what memory sizes
    df_tables = pd.concat([pd.read_csv(file) for file in rc_table_files], ignore_index=True)
    df_tables.to_csv(merged_rc_table_file, index=False)

    print(f"rating curve table files merged to {merged_rc_table_file}")

    # ----------------
    # Iterate and join the point files
    rc_points_files = []
    for unit_dir, full_src_unit_path in src_rel_unit_dirs.items():
        src_rc_point_file = os.path.join(full_src_unit_path, calib_dir_name,
                                         "ras2calibration_rating_curve_points.gpkg")
        if os.path.isfile(src_rc_point_file):
            rc_points_files.append(src_rc_point_file)

    if len(rc_points_files) == 0:
        raise Exception("Error: No ras2calibration_rating_curve_points.gpkg files found.")

    compiled_geopackage = None

    # Add manual tqdm (not multi proc or multi thread)
    # Iterate through input geopackages and compile them
    for i in range(len(rc_points_files)):
        print(f"We are at rc_point_files index of {i}")
        if i == 0:
            # we have to load the first gkpg directly then concat more after.
            # Create an empty GeoDataFrame to store the compiled data
            compiled_geopackage = gpd.read_file(rc_points_files[i])
        else:
            data = gpd.read_file(rc_points_files[i])
            compiled_geopackage = pd.concat([compiled_geopackage, data], ignore_index=True)

    compiled_geopackage.to_file(merged_rc_points_file, driver="GPKG")

    print(f"rating curve point files merged to {merged_rc_points_file}")


# -------------------------------------------------
# def __save_to_s3(local_wip_folder):


# -------------------------------------------------
if __name__ == "__main__":

    # ***********************
    # This tool is intended for NOAA/OWP staff only as it requires access to an AWS S3 bucket with a
    # specific folder structure.
    # If you create your own S3 bucket in your own AWS account, you are free to use this tool.
    # ***********************

    # ---- Samples Inputs
    # Min args:
    #  python ./tools/ras2release.py -r r121
    #  

    # Max args:
    #  python ./tools/ras2release.py -r r121 -w c:/my_release_folder
    #      -s s3://my_ras2fim_bucket/output_ras2fim -t s3://my_ras2fim_bucket/ras2release/
    #  but left off the -ss flag as we do want them uploaded

    # Note: For the -w local target, we will add a folder under it with the -r name.
    #   e.g. c:/my_release_folder/r121
    #   Same is true when we upload the results back to S3 (if applicable)
    #   e.g. s3://my_ras2fim_bucket/ras2release/r121
    

    parser = argparse.ArgumentParser(
        description="Creating a ras2release package", formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-r",
        "--release_name",
        help="REQUIRED: New ras2release name (such as 'r200')."
        " It will become the folder name for the release. You can use any name you like "
        " however, the current pattern is recommended."
        " Current pattern is the last S3 release folder name plus one.\n"
        " eg. Last one was rel_101, then this one becomes r102.",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-w",
        "--local_ras2release_path",
        help="OPTIONAL: local folder where the files/folders will be created."
         " eg. c:/my_ras2fim_dir/releases/ (we add the -r name as a folder)\n"
        f" Defaults to {sv.R2F_OUTPUT_DIR_RELEASES}",
        required=False,
        default=sv.R2F_OUTPUT_DIR_RELEASES,
        metavar="",
    )

    parser.add_argument(
        "-s",
        "--s3_path_to_output_folder",
        help=f"OPTIONAL (case-sensitive): full s3 path to all of the ras2fim unit output folders are at."
         " eg. s3://my_ras2fim_bucket/output_ras2fim/\n"
         f" Defaults to {sv.S3_RAS_UNITS_OUTPUT_PATH}",
        required=False,
        default=sv.S3_RAS_UNITS_OUTPUT_PATH,
        metavar="",
    )

    parser.add_argument(
        "-t",
        "--s3_ras2release_path",
        help="OPTIONAL (case-sensitive): S3 path to ras2release folder."
         " Note: the -r (rel_name) will be added as a s3 folder name automatically.\n"
         " eg. s3://my_ras2fim_bucket/ras2release/ (we add the -r name as a folder)\n"
        f" Defaults to {sv.S3_DEFAULT_RAS2RELEASE_FOLDER}/(given -r rel name)",
        required=False,
        default=sv.S3_DEFAULT_RAS2RELEASE_FOLDER,
        metavar="",
    )

    parser.add_argument(
        "-ss",
        "--skip_save_to_s3",
        help="OPTIONAL: By default, the results of creating a release folder"
         " will be saved to S3.\n"
         " Note: You may need to review the -t flag to ensure it is being saved"
        f" to the S3 bucket and folder you wish.",
        required=False,
        default=False,
        action="store_true")

    args = vars(parser.parse_args())

    # Yes.. not including the rel_name
    log_file_folder = os.path.join(args["local_ras2release_path"], "logs")
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

        create_ras2release(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
    
