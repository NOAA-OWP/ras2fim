#!/usr/bin/env python3

import argparse
import datetime as dt
import glob
import os
import sys

import geopandas as gpd
import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv


####################################################################
def create_ras2release(rel_name, s3_path_to_output_folder, local_working_folder_path, s3_ras2release_path):
    """
    # TODO - WIP
    Summary:
    Inputs:
    Outputs:
    """

    start_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    print("")
    print("=================================================================")
    print("          RUN ras2release ")
    print(f"  (-r): release path and folder name = {rel_name} ")
    print(f"  (-s): s3 path to output folder name  = {s3_path_to_output_folder}")
    print(f"  (-l): local working path  = {local_working_folder_path}")
    print(f"  (-t): s3 ras2release path  = {s3_ras2release_path}")

    # validate input variables and setup key variables
    # vd = Variables Dictionary
    vd = __validate_input(rel_name, s3_path_to_output_folder, local_working_folder_path, s3_ras2release_path)

    # print(f"  s3 ras2release folder  = {varibles_dict["s3_target_path"]}")
    print()
    print(f"Started (UTC): {dt_string}")

    # create list local output folder paths
    local_src_unit_paths = []

    # TODO
    # Question: Do we bring down all "final" folders locally, then move and renamed files?
    # __download_units_from_s3

    # __process_geocurves()
    __process_domain_models(local_src_unit_paths, vd["r2c_local_target_path"])
    # __process_models_used
    #__process_rating_curves(local_src_unit_paths, vd["r2c_local_target_path"])
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


####################################################################
####  Some validation of input, but also creating key variables ######


def __validate_input(rel_name, s3_path_to_output_folder, local_working_folder_path, s3_ras2release_path):
    """
    Summary: Will raise Exception if some are found

    Output: tuple
    """

    # TODO - WIP

    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    variables = {}

    # ----------------
    if rel_name == "":
        raise ValueError("rel_name (-r) can not be empty")

    # ----------------
    # test s3 bucket and path

    # ----------------
    # test local_working_folder_path

    # all local parent folders must exist, but if the child folder does not exist, create it
    # else, just delete and rebuild

    # test that s3 target folder name exists
    variables["r2c_local_target_path"] = f"{local_working_folder_path}\{rel_name}"

    return variables


####################################################################
# def __process_geocurves():

####################################################################
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


####################################################################
# def __process_models_used():


####################################################################
def __process_rating_curves(local_src_unit_paths, local_wip_folder):
    # TODO: WIP

    """
    Summary:
    Inputs:
        - local_src_unit_paths: a list of the unit output folders already downloaded from S3.
        - local_wip_folder: location where the new rel version of the ras2release folder will be created.
          Uploading to S3 will be done in a seperate step.
    """

    print()
    print("*** processing rating curves")

    output_folder = os.path.join(local_wip_folder, "calibration_rating_curves")
    merged_rc_table_file = os.path.join(output_folder, "reformat_ras_rating_curve_table.csv")
    merged_rc_points_file = os.path.join(output_folder, "reformat_ras_rating_curve_points.gpkg")

    # ----------------
    # Copy all ras2calibration_output_geopackages and ras2calibation output table csv's from
    # the local output unit folders to the ras2release/{rel version}/rating_curve folder

    # ----------------
    # Iterate and join the table files
    print(f"src_output_folder is {output_folder}")
    # TODO: incoming files have been renamed.
    rc_table_files = glob.glob(os.path.join(output_folder, "*_ras2calibration_output_table.csv"))

    if len(rc_table_files) == 0:
        raise Exception("Internal Error: No ras2calibration_output_table.csv found.")

    # append the CSV files
    # TODO: Make this a loop and tdqm manually in place (not multi thread or multi proc)
    # but manual tdqm so we can watch progress.
    # what memory sizes
    df_tables = pd.concat([pd.read_csv(file) for file in rc_table_files], ignore_index=True)
    df_tables.to_csv(merged_rc_table_file, index=False)

    # ----------------
    # Iterate and join the table files
    # TODO: incoming files have been renamed.
    rc_point_files = glob.glob(os.path.join(output_folder, "*_ras2calibration_output_geopackage.gpkg"))

    # TODO: This might have to move to Parquet or similar as gpkg have a 2GB limit, in FIM, we have a
    # parquet file per HUC8. inputs/rating_curve/water_edge_database/calibration_points/

    compiled_geopackage = None

    # Add manual tqdm (not multi proc or multi thread)
    # Iterate through input geopackages and compile them
    for i in range(len(rc_point_files)):
        print(f"We are at rc_point_files index of {i}")
        if i == 0:
            # we have to load the first gkpg directly then concat more after.
            # Create an empty GeoDataFrame to store the compiled data
            compiled_geopackage = gpd.read_file(rc_point_files[i])
        else:
            data = gpd.read_file(rc_point_files[i])
            compiled_geopackage = pd.concat([compiled_geopackage, data], ignore_index=True)

    compiled_geopackage.to_file(merged_rc_points_file, driver="GPKG")


####################################################################
# def __save_to_s3(local_wip_folder):


####################################################################
if __name__ == "__main__":
    # ---- Samples Inputs

    parser = argparse.ArgumentParser(
        description="Creating a ras2release package", formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-r",
        "--rel_name",
        help="REQUIRED: New ras2release rel name."
        " You can use any folder name you like. However, the current pattern is recommended,"
        " which is the last S3 release folder name plus one.\n"
        "eg. Last one was rel_100, then this one becomes rel_101.",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-s",
        "--s3_path_to_output_folder",
        help="OPTIONAL: S3 path to output folder." f" Defaults to {sv.S3_DEFAULT_OUTPUT_FOLDER}",
        required=False,
        default=sv.S3_DEFAULT_OUTPUT_FOLDER,
        metavar="",
    )

    parser.add_argument(
        "-w",
        "--local_working_folder_path",
        help="OPTIONAL: local folder where ras2release will be created."
        f" Defaults to {sv.R2F_OUTPUT_DIR_RELEASES}",
        required=False,
        default=sv.R2F_OUTPUT_DIR_RELEASES,
        metavar="",
    )

    parser.add_argument(
        "-t",
        "--s3_ras2release_path",
        help="OPTIONAL: S3 path to ras2release folder, not counting -r rel_name."
        f" Defaults to {sv.S3_DEFAULT_RAS2RELEASE_FOLDER}/(given -r rel name)",
        required=False,
        default=sv.S3_DEFAULT_RAS2RELEASE_FOLDER,
        metavar="",
    )

    args = vars(parser.parse_args())

    create_ras2release(**args)
