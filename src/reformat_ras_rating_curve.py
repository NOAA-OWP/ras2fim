#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = ras2fim_logger.RAS2FIM_logger()


# -----------------------------------------------------------------
# Writes a metadata file into the save directory
# --------------------------
def write_metadata_file(
    output_save_folder,
    start_time_string,
    nwm_shapes_file,
    hecras_shapes_file,
    metric_file,
    geopackage_name,
    csv_name,
    log_name,
    verbose,
):
    """
    Overview:

    Creates a metadata textfile and saves it to the output save folder.

    """

    metadata_content = []
    metadata_content.append(f"Data was produced using reformat_ras_rating_curve.py on {start_time_string}.")
    metadata_content.append(" ")
    metadata_content.append("ras2fim file inputs:")
    metadata_content.append(f"  NWM streamlines from {nwm_shapes_file}")
    metadata_content.append(f"  HECRAS crosssections from {hecras_shapes_file}")
    metadata_content.append(f"  WSE rating curves from {metric_file}")
    metadata_content.append(" ")
    metadata_content.append("Outputs: ")
    metadata_content.append(f"  {geopackage_name} (point location geopackage)")
    metadata_content.append(f"  {csv_name} (rating curve CSV)")
    metadata_content.append(f"  {log_name} (output log textfile, only saved if -l argument is used)")
    metadata_content.append(" ")
    metadata_content.append("CSV column name    Source                  Type            Description")
    metadata_content.append(
        "fid_xs             Calculated in script    String          Combination of NWM feature ID and"
        " HECRAS crosssection name"
    )
    metadata_content.append(
        "feature_id         From geometry files     Number          NWM feature ID associated with the"
        " stream segment"
    )
    metadata_content.append(
        "xsection_name      From geometry files     Number          HECRAS crosssection name"
    )
    metadata_content.append(
        "flow               From rating curve       Number          Discharge value from rating curve in"
        " each directory"
    )
    metadata_content.append(
        "wse                From rating curve       Number          Water surface elevation value from the"
        " rating curve in each directory"
    )
    metadata_content.append(
        "flow_units         Hard-coded              Number          Discharge units"
        " (metric since data is being pulled from metric directory)"
    )
    metadata_content.append(
        "wse_unts           Hard-coded              Number          Water surface elevation units"
        " (metric since data is being pulled from metric directory)"
    )
    metadata_content.append(
        "location_type      User-provided           String          Type of site the data is coming from"
        " (example: IFC or USGS) (optional)"
    )
    metadata_content.append(
        "source             User-provided           String          Source that produced the data"
        " (example: RAS2FIM) (optional)"
    )
    metadata_content.append(
        "timestamp          Calculated in script    Datetime        Describes when this table was compiled"
    )
    metadata_content.append(
        "active             User-provided           True/False      Whether a gage is active (optional)"
    )
    metadata_content.append(
        "huc8               From geometry files     Number          HUC 8 watershed ID that the point"
        " falls in"
    )
    metadata_content.append(
        "ras_model_dir      From geometry files     String          RAS model that the points came from"
    )

    metadata_content.append(" ")

    metadata_name = "README_reformat_ras_rating_curve.txt"
    metadata_path = os.path.join(output_save_folder, metadata_name)

    with open(metadata_path, "w") as f:
        for line in metadata_content:
            f.write(f"{line}\n")

    if verbose is True:
        RLOG.debug(f"Metadata README saved to {metadata_path}")


# -----------------------------------------------------------------
# Functions for handling units
# -----------------------------------------------------------------
def get_unit_from_string(string):
    if "meters" in string or "meter" in string or "m" in string or "metre" in string or "metres" in string:
        return "m"
    elif "feet" in string or "foot" in string or "ft" in string:
        return "ft"
    else:
        return "UNKNOWN"


# -------------------------------------------------
def feet_to_meters(number):
    converted_value = number / 3.281  # meter = feet / 3.281
    return converted_value


# -------------------------------------------------
def meters_to_feet(number):
    converted_value = number / 0.3048  # feet = meters / 0.3048
    return converted_value


# -----------------------------------------------------------------
# Reads, compiles, and reformats the rating curve info for all directories
# -----------------------------------------------------------------
def dir_reformat_ras_rc(
    dir_input_folder_path,
    intermediate_filename,
    int_output_table_label,
    int_output_geopackage_label,
    int_log_label,
    source,
    location_type,
    active,
    verbose,
    nwm_shapes_file,
    hecras_shapes_file,
    metric_file,
):
    """
    Overview:

    Reads, compiles, and reformats the rating curve info for the given directory
    (runs in compile_ras_rating_curves).

    Notes:

        - Automatically overwrites the main outputs (the compiled CSV, geopackage, and log) if they already
          exist in the output folder. If there is a need to keep the existing main outputs, use a different
          output folder.


    Parameters (required):

    - dir_input_folder_path: (str) filepath for folder containing input ras2fim models for the HUC
      (optional arguments set in __main__ or defaults to value from __)

    - intermediate_filename: (str) name of file to store intermediates (set in compile_ras_rating_curves,
      defaults to "intermediate_outputs")

    - int_output_table_label: (str) suffix for intermediate output table (set in compile_ras_rating_curves,
      defaults to "_output_table.csv")

    - int_log_label: (str) suffix for intermediate output table (set in compile_ras_rating_curves,
      defaults to "_log.txt")

    - source: (str) optional input value for the "source" output column
      (i.e. "", "ras2fim", "ras2fim v2.1")

    - location_type: (str) optional input value for the "location_type" output column
     (i.e. "", "USGS", "IFC")

    - active: (str) optional input value for the "active" column (i.e. "", "True", "False")

    - verbose: (bool) option to run verbose code with a lot of print statements
      (optional argument set in __main__)

    - nwm_shapes_file: (str) file from shared_variables to get the NWM shapefiles from

    - hecras_shapes_file: (str) file from shared_variables to get the HECRAS shapefiles from

    - metric_file: (str) file from shared_variables to get the metric output files from

    """

    # Create empty output log

    RLOG.lprint("")
    overall_start_time = datetime.utcnow()
    dt_string = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Started (UTC): {dt_string}")

    output_log = []
    output_log.append(" ")
    output_log.append(f"Directory: {dir_input_folder_path}")

    if verbose is True:
        print()
        RLOG.debug("======================")
        RLOG.debug(f"Directory: {dir_input_folder_path}")
        print()

    # ------------------------------------------------------------------------------------------------
    # Retrieve information from `run_arguments.txt` file

    # Read run_arguments.txt file
    run_arguments_filepath = os.path.join(dir_input_folder_path, "run_arguments.txt")

    # Open the file and read all lines from the file
    try:
        with open(run_arguments_filepath, "r") as file:
            lines = file.readlines()
    except Exception as ex:
        RLOG.error(f"Unable to open run_arguments.txt, skipping directory {dir_input_folder_path}.")
        RLOG.error(f"\n details: {ex}")
        lines = None

    # Search for and extract the model unit and projection from run_arguments.txt
    for line in lines:
        if "model_unit ==" in line:
            model_unit = line.split("==")[1].strip()
        elif "str_huc8_arg ==" in line:
            str_huc8_arg = line.split("==")[1].strip()
        elif "proj_crs ==" in line:
            proj_crs = line.split("==")[1].strip()

    # Standardize the model unit and output unit
    model_unit = get_unit_from_string(model_unit)

    if verbose is True:
        RLOG.debug(
            f"Model settings: model_unit {model_unit} | str_huc8_arg: {str_huc8_arg} | proj_crs: {proj_crs}"
        )

    # Create intermediate output file within directory (only if the run_arguments.txt folder is there)
    if lines is not None:
        intermediate_filepath = os.path.join(dir_input_folder_path, intermediate_filename)
        if not os.path.exists(intermediate_filepath):
            os.mkdir(intermediate_filepath)

    # ------------------------------------------------------------------------------------------------
    # Get compiled rating curves from metric folder
    metric_path = os.path.join(dir_input_folder_path, metric_file)

    rc_path_list = list(Path(metric_path).rglob("*all_cross_sections.csv"))
    if len(rc_path_list) == 0:
        log = "ERROR: No paths in rating curve path list. "
        output_log.append(log)
        RLOG.warning(log)
    elif len(rc_path_list) >= 1:
        rc_path = rc_path_list[0]

    if os.path.isfile(rc_path) is False:
        RLOG.warning(f"No rating curve file available for {dir_input_folder_path}, skipping this directory.")
        output_log.append("Rating curve NOT available.")
        dir_log_filename = int_log_label
        dir_log_filepath = os.path.join(intermediate_filepath, dir_log_filename)

        with open(dir_log_filepath, "w") as f:
            for line in output_log:
                f.write(f"{line}\n")
    else:
        output_log.append("Rating curve CSV available.")

        # ------------------------------------------------------------------------------------------------
        # Manually build filepaths for the geospatial data

        # root_dir = os.path.join(input_folder_path, dir)
        root_dir = dir_input_folder_path  # TODO: clean up

        nwm_all_lines_filename = str_huc8_arg + "_nwm_streams_ln.shp"
        nwm_all_lines_filepath = os.path.join(root_dir, nwm_shapes_file, nwm_all_lines_filename)

        hecras_crosssections_filename = "cross_section_LN_from_ras.shp"
        hecras_crosssections_filepath = os.path.join(
            root_dir, hecras_shapes_file, hecras_crosssections_filename
        )

        if not os.path.exists(nwm_all_lines_filepath):
            msg = f"Error: No file at {nwm_all_lines_filepath}"
            RLOG.warning(msg)
            output_log.append(msg)

        if not os.path.exists(hecras_crosssections_filepath):
            msg = f"Error: No file at {hecras_crosssections_filepath}"
            RLOG.warning(msg)
            output_log.append(msg)

        # ------------------------------------------------------------------------------------------------
        # Intersect NWM lines and HEC-RAS crosssections to get the points
        # (but keep the metadata from the HEC-RAS cross-sections)

        if verbose is True:
            RLOG.debug("")
            RLOG.debug("Reading shapefiles and generating crosssection/streamline intersection points ...")

        # Read shapefiles
        hecras_crosssections_shp = gpd.read_file(hecras_crosssections_filepath)
        nwm_all_lines_shp = gpd.read_file(nwm_all_lines_filepath)

        # Get ras_model_dir from the hecras_crosssections_shp
        ras_path_list = list(hecras_crosssections_shp["ras_path"])

        ras_dir_list = []
        for path in ras_path_list:
            ras_dir = os.path.basename(os.path.dirname(path))
            ras_dir_list.append(ras_dir)

        hecras_crosssections_shp["ras_model_dir"] = ras_dir_list

        # Apply shapefile projection
        hecras_crosssections_shp.crs = proj_crs
        nwm_all_lines_shp.crs = proj_crs

        # Find intersections
        intersections = gpd.overlay(
            nwm_all_lines_shp, hecras_crosssections_shp, how="intersection", keep_geom_type=False
        )

        # Create a GeoDataFrame for the intersection points
        intersection_gdf = gpd.GeoDataFrame(geometry=intersections.geometry, crs=nwm_all_lines_shp.crs)

        # Append attribute table of hecras_crosssections_shp to intersection_points_gdf
        # and fix data type for stream_stn
        intersection_gdf = intersection_gdf.join(intersections.drop(columns="geometry"))
        intersection_gdf = intersection_gdf.astype({"stream_stn": "int"})

        # Combined feature ID and HECRAS cross-section ID to make a new ID
        intersection_gdf["fid_xs"] = (
            intersection_gdf["feature_id"].astype(str) + "_" + intersection_gdf["stream_stn"].astype(str)
        )

        # ------------------------------------------------------------------------------------------------
        # Check for duplicate Feature_ID / Cross-section ID combinations

        # if intersection_gdf['fid_xs'].duplicated().any():

        #     # Print duplicates if verbose
        #     if verbose is True:
        #         duplicated_fids = intersection_gdf.loc[
        #           intersection_gdf['fid_xs'].duplicated() == True]['fid_xs']
        #         print("Duplicate fid_xs values: " + str(set(list(duplicated_fids))))

        #         output_log.append("Duplicate fid_xs values: " + str(set(list(duplicated_fids))))
        #         output_log.append("Duplicate fid_xs values were removed from geopackage.")
        #         output_log.append(" ")

        #     intersection_gdf.drop_duplicates(subset='fid_xs', inplace=True)
        #     print()
        #     print("Duplicate fid_xs values were removed from intersection points geopackage.")

        # ------------------------------------------------------------------------------------------------
        # Read compiled rating curve and append huc8 from intersections
        try:
            rc_df = pd.read_csv(rc_path)
        except Exception as ex:
            msg = f"Unable to read rating curve at path {rc_path}"
            RLOG.warning(msg + f"\n details: {ex}")
            output_log.append(msg)

        # Join some of the geospatial data to the rc_df data
        rc_geospatial_df = pd.merge(
            rc_df,
            intersection_gdf[["fid_xs", "huc8", "ras_model_dir"]],
            left_on="fid_xs",
            right_on="fid_xs",
            how="inner",
        )
        rc_geospatial_df = rc_geospatial_df.astype({"huc8": "object"})

        # ------------------------------------------------------------------------------------------------
        # Build output table

        # Get a current timestamp
        timestamp = dt.datetime.utcnow()

        # Assemble output table
        dir_output_table = pd.DataFrame(
            {
                "fid_xs": rc_geospatial_df["fid_xs"],
                "feature_id": rc_geospatial_df["featureid"],
                "xsection_name": rc_geospatial_df["Xsection_name"],
                "flow": rc_geospatial_df["discharge_cms"],
                "wse": rc_geospatial_df["wse_m"],
                "flow_units": "cms",  # str
                "wse_units": "m",  # str
                "location_type": location_type,  # str
                "source": source,  # str
                "timestamp": timestamp,  # str
                "active": active,  # str
                "huc8": rc_geospatial_df["huc8"],  # str
                "ras_model_dir": rc_geospatial_df["ras_model_dir"],  # str
            }
        )

        # Add necessary columns to the intersections geopackage
        intersection_gdf["location_type"] = location_type
        intersection_gdf["source"] = source
        intersection_gdf["timestamp"] = timestamp
        intersection_gdf["active"] = active
        intersection_gdf["flow_units"] = "cms"
        intersection_gdf["wse_units"] = "m"

        # ------------------------------------------------------------------------------------------------
        # Convert multipoint features to point features # TODO: Use in ras2release compilation script

        # # Function to convert MultiPoint to Point
        # def convert_multipoint_to_point(geom):
        #     if geom.geom_type == 'MultiPoint':
        #         if verbose is True:
        #             print('Multipoint features found. Converting them to Point.')
        #         output_log.append('Multipoint features found. Converting them to Point.')
        #         return Point(geom.centroid)
        #     else:
        #         return geom

        # # If intersection_gdf is a multipoint feature, convert it to a point feature
        # try:
        #     intersection_gdf['geometry'] = intersection_gdf['geometry'].apply(convert_multipoint_to_point)
        # except:
        #     print('Unable to apply function convert_multipoint_to_point to intersection geodataframe.')

        # ------------------------------------------------------------------------------------------------
        # Export dir_output_table, dir_geospatial, and log to the intermediate save folder

        # Write filepath for geopackage
        dir_output_geopackage_filepath = os.path.join(intermediate_filepath, int_output_geopackage_label)

        # Reproject intersection_gdf to output SRC
        shared_variables_crs = sv.DEFAULT_RASTER_OUTPUT_CRS
        intersection_prj_gdf = intersection_gdf.to_crs(shared_variables_crs)

        # Save directory geopackage
        try:
            intersection_prj_gdf.to_file(dir_output_geopackage_filepath, driver="GPKG")
            if verbose is True:
                RLOG.debug("HECRAS-NWM intersection geopackage saved.")
        except Exception as ex:
            msg = "Unable to save HEC-RAS points geopackage."
            RLOG.warning(msg)
            RLOG.warning(f"\n details: {ex}")
            output_log.append(msg)

        # Save output table for directory
        dir_output_table_filename = int_output_table_label
        dir_output_table_filepath = os.path.join(intermediate_filepath, dir_output_table_filename)
        dir_output_table.to_csv(dir_output_table_filepath, index=False)

        # Save log for directory
        dir_log_filename = int_log_label
        dir_log_filepath = os.path.join(intermediate_filepath, dir_log_filename)
        with open(dir_log_filepath, "w") as f:
            for line in output_log:
                f.write(f"{line}\n")

        if verbose is True:
            RLOG.debug("")
            RLOG.debug(f"Saved directory outputs for {dir_input_folder_path}.")

        # Get timestamp for metadata
        start_time_string = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

        # Write README metadata file for the intermediate file
        write_metadata_file(
            intermediate_filepath,
            start_time_string,
            nwm_shapes_file,
            hecras_shapes_file,
            metric_file,
            int_output_geopackage_label,
            int_output_table_label,
            int_log_label,
            verbose,
        )

        if verbose is True:
            RLOG.debug(f"Saved metadata to {intermediate_filepath}.")

    RLOG.lprint("")
    end_time = datetime.utcnow()
    dt_string = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Ended : {dt_string}")
    time_duration = end_time - overall_start_time
    RLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    RLOG.lprint("")


# -----------------------------------------------------------------
# Compiles the rating curve and points from each directory
# -----------------------------------------------------------------
def compile_ras_rating_curves(
    input_folder_path, output_save_folder, save_logs, verbose, num_workers, source, location_type, active
):
    """
    Overview:

    Creates directory list and feeds directories to dir_reformat_ras_rc() inside a multiprocessor.
    Compiles the rating curve and geopackage info from the intermediate data folder and saves a final
    rating curve CSV and geospatial outputs geopackage. Runs from __main__.


    Parameters (required):

    - input_folder_path: (str) filepath for folder containing input ras2fim models

    - output_save_folder: (str) filepath of parent folder in which to create the ras2calibration
      folder containing the output files

    - verbose: (bool) option to run verbose code with a lot of print statements

    - save_logs: (bool) option to save output logs as a textfile

    - num_workers: (int) number of workers to use during parallel processing

    - source: (str) optional input value for the "source" output column
      (i.e. "", "ras2fim", "ras2fim v2.1")

    - location_type: (str) optional input value for the "location_type" output column
     (i.e. "", "USGS", "IFC")

    - active: (str) optional input value for the "active" column (i.e. "", "True", "False")

    """

    # Establish file naming conventions
    int_output_table_label = "ras2calibration_output_table.csv"
    int_geopackage_label = "ras2calibration_output_geopackage.gpkg"
    int_log_label = "log.txt"
    intermediate_filename = sv.R2F_OUTPUT_DIR_RAS2CALIBRATION

    # Record and print start time
    start_time = dt.datetime.utcnow()
    start_time_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    # Settings block
    RLOG.lprint(
        "-----------------------------------------------------------------------------------------------"
    )
    RLOG.lprint("Begin rating curve compilation process.")
    RLOG.lprint("")
    RLOG.lprint(f"Start time: {start_time_string}.")
    RLOG.lprint("")
    RLOG.lprint(f"Verbose: {str(verbose)}")
    RLOG.lprint(f"Save output log to folder: {str(save_logs)}")
    RLOG.lprint(f"Number of workers: {num_workers}")
    RLOG.lprint("")

    # Get default paths from shared variables if they aren't included
    if input_folder_path == "":
        input_folder_path = sv.R2F_DEFAULT_OUTPUT_MODELS  # ras2fim output folder
        RLOG.lprint(f"Using default input folder path: {input_folder_path}")

        if not os.path.exists(input_folder_path):
            RLOG.warning(f"No file exists at {input_folder_path}")

    # Error out if the input folder doesn't exist
    if not os.path.exists(input_folder_path):
        msg = f"No folder at input folder path: {input_folder_path}"
        RLOG.critical(msg)
        sys.exit(msg)

    # Check for output folders

    if output_save_folder == "":  # Using the default filepath
        output_save_folder = sv.R2F_OUTPUT_DIR_RELEASES
        RLOG.lprint(f"Attempting to use default output save folder: {output_save_folder}")

        # Attempt to make default output folder if it doesn't exist, error out if it doesn't work.
        if not os.path.exists(output_save_folder):
            RLOG.lprint(f"No folder found at {output_save_folder}")
            try:
                RLOG.lprint("Creating output folder.")
                os.mkdir(output_save_folder)
            except OSError:
                msg = f"Unable to create default output save folder at {output_save_folder}"
                RLOG.critical(msg)
                RLOG.critical(OSError)
                sys.exit()

        # Assemble the output subfolder filepath
        output_save_subfolder = os.path.join(sv.R2F_OUTPUT_DIR_RELEASES, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION)

        # If the output subfolder already exists, remove it
        if os.path.exists(output_save_subfolder) is True:
            shutil.rmtree(output_save_subfolder)
            # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
            # so sometimes mkdir can fail if rmtree isn't done
            time.sleep(1)

        # Make the output subfolder
        os.mkdir(output_save_subfolder)

    else:  # Using the specified filepath
        # Check that the destination filepath exists. If it doesn't, give error and quit.
        if not os.path.exists(output_save_folder):
            msg = f"Error: No folder found at {output_save_folder}.\n"
            "Create this parent directory or specify a different output folder"
            " using `-o` followed by the directory filepath."
            RLOG.critical(msg)
            sys.exit()

        # Assemble the output subfolder filepath
        output_save_subfolder = os.path.join(output_save_folder, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION)

        # If the output subfolder already exists, remove it
        if os.path.exists(output_save_subfolder) is True:
            shutil.rmtree(output_save_subfolder)
            # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
            # so sometimes mkdir can fail if rmtree isn't done
            time.sleep(1)

        # Make the output subfolder
        os.mkdir(output_save_subfolder)

    # Check job numbers
    total_cpus_requested = num_workers
    total_cpus_available = os.cpu_count() - 2
    if total_cpus_requested > total_cpus_available:
        raise ValueError(
            "Total CPUs requested exceeds your machine's available CPU count minus one. "
            "Please lower the quantity of requested workers accordingly."
        )

    # Get a list of the directories in the input folder path
    dirlist = []

    for dir in os.listdir(input_folder_path):
        if len(dir) < 8:
            continue

        huc_number = dir[0:8]
        if not huc_number.isnumeric():
            RLOG.warning(f"dir of {dir} does not start with a huc_number")
            continue

        dirlist.append(dir)

    # Create empty output log and give it a header
    output_log = []
    output_log.append(f"Processing for reformat_ras_rating_curves.py started at {str(start_time_string)}")
    output_log.append(f"Input directory: {input_folder_path}")

    # ------------------------------------------------------------------------------------------------
    # Assemble filepaths

    nwm_shapes_file = sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF  # "02_shapes_from_conflation"
    hecras_shapes_file = sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS  # "01_shapes_from_hecras"
    metric_file = sv.R2F_OUTPUT_DIR_METRIC  # "06_metric"

    # ------------------------------------------------------------------------------------------------
    # Create a process pool and run dir_reformat_ras_rc() for each directory

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        if verbose is True:
            RLOG.debug("")
            RLOG.debug("--------------------------------------------------------------")
            RLOG.debug("Begin iterating through directories with multiprocessor...")
            RLOG.debug("")

        for dir in dirlist:
            dir_input_folder_path = os.path.join(input_folder_path, dir)
            executor.submit(
                dir_reformat_ras_rc,
                dir_input_folder_path,
                intermediate_filename,
                int_output_table_label,
                int_geopackage_label,
                int_log_label,
                source,
                location_type,
                active,
                verbose,
                nwm_shapes_file,
                hecras_shapes_file,
                metric_file,
            )

    # # Run without multiprocessor
    # for dir in dirlist:
    #     dir_reformat_ras_rc(dir, input_folder_path, intermediate_filename,
    # int_output_table_label, int_log_label,
    # source, location_type, active, verbose,
    # nwm_shapes_file, hecras_shapes_file, metric_file)

    # ------------------------------------------------------------------------------------------------
    # Read in all intermedate files (+ output logs) and combine them

    if verbose is True:
        RLOG.debug("")
        RLOG.debug("--------------------------------------------------------------")
        RLOG.debug("Begin compiling multiprocessor outputs...")
        RLOG.debug("")

    # Get list of intermediate files from path
    int_output_table_files = []
    int_geopackage_files = []
    int_logs = []

    for dir in dirlist:
        intermediate_filepath = os.path.join(input_folder_path, dir, intermediate_filename)

        # Get output table and append to list if path exists
        filename = int_output_table_label
        path = os.path.join(intermediate_filepath, filename)
        if os.path.exists(path):
            int_output_table_files.append(path)

        # Get geopackage filename and append to list if path exists
        filename = int_geopackage_label
        path = os.path.join(intermediate_filepath, filename)
        if os.path.exists(path):
            int_geopackage_files.append(path)

        # Get log filename and append to list if path exists
        filename = int_log_label
        path = os.path.join(intermediate_filepath, filename)
        if os.path.exists(path):
            int_logs.append(path)

    # Read and compile the intermediate rating curve tables
    full_output_table = pd.DataFrame()
    for file_path in int_output_table_files:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            full_output_table = pd.concat([full_output_table, df])
    full_output_table.reset_index(drop=True, inplace=True)

    if len(int_geopackage_files) == 0:
        raise ValueError("no geopackage file paths have been found")

    # Define output projection from shared variables
    compiled_geopackage_CRS = sv.DEFAULT_RASTER_OUTPUT_CRS

    compiled_geopackage = None

    # Iterate through input geopackages and compile them
    for i in range(len(int_geopackage_files)):
        if i == 0:
            # we have to load the first gkpg directly then concat more after.
            # Create an empty GeoDataFrame to store the compiled data
            compiled_geopackage = gpd.read_file(int_geopackage_files[i])
        else:
            data = gpd.read_file(int_geopackage_files[i])
            compiled_geopackage = pd.concat([compiled_geopackage, data], ignore_index=True)

    # Set the unified projection for the compiled GeoDataFrame
    compiled_geopackage.crs = compiled_geopackage_CRS

    # Read and compile all logs
    for file_path in int_logs:
        if os.path.exists(file_path):
            with open(file_path) as f:
                lines = f.readlines()
                for line in lines:
                    output_log.append(line)

    # Remove extra linebreaks from log
    output_log = [s.replace("\n", "") for s in output_log]
    output_log.append(" ")

    # ------------------------------------------------------------------------------------------------
    # Export the output points geopackage and the rating curve table to the save folder

    geopackage_name = "reformat_ras_rating_curve_points.gpkg"
    geopackage_path = os.path.join(output_save_subfolder, geopackage_name)
    compiled_geopackage.to_file(geopackage_path, driver="GPKG")

    csv_name = "reformat_ras_rating_curve_table.csv"
    csv_path = os.path.join(output_save_subfolder, csv_name)
    full_output_table.to_csv(csv_path, index=False)

    # ------------------------------------------------------------------------------------------------
    # Export metadata, print filepaths and save logs

    # Report output pathing
    output_log.append(f"Geopackage initial save location: {geopackage_path}")
    output_log.append(f"Compiled rating curve csv initial save location: {csv_path}")
    if verbose is True:
        RLOG.debug("")
        RLOG.debug(f"Compiled geopackage saved to {geopackage_path}")
        RLOG.debug(f"Compiled rating curve csv saved to {csv_path}")

    # Save output log if the log option was selected
    log_name = "reformat_ras_rating_curve_log.txt"
    if save_logs is True:
        log_path = os.path.join(output_save_subfolder, log_name)

        with open(log_path, "w") as f:
            for line in output_log:
                f.write(f"{line}\n")
        RLOG.lprint(f"Compiled output log saved to {log_path}.")
    else:
        RLOG.lprint("No output log saved.")

    # Write README metadata file
    write_metadata_file(
        output_save_subfolder,
        start_time_string,
        nwm_shapes_file,
        hecras_shapes_file,
        metric_file,
        geopackage_name,
        csv_name,
        log_name,
        verbose,
    )

    # Record end time, calculate runtime, and print runtime
    end_time = dt.datetime.utcnow()
    runtime = end_time - start_time

    RLOG.lprint("")
    RLOG.lprint(f"Process finished. Total runtime: {runtime}")
    RLOG.lprint("--------------------------------------------")


# -------------------------------------------------
if __name__ == "__main__":
    # TODO: Oct 24, 2023: -so flag in the notes below does not appear to exist anymore.
    # Research required and it likely can be removed from the notes below.
    """
    Sample usage:

    # Recommended parameters:
    python reformat_ras_rating_curve.py
        -i 'C:/ras2fim_data/output_ras2fim' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves'
        -v -l

    # Minimalist run (all defaults used):
    python reformat_ras_rating_curve.py

    # Maximalist run (all possible arguments):
    python reformat_ras_rating_curve.py
        -i 'C:/ras2fim_data/output_ras2fim' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves'
        -v -l -j 6 -k -so "ras2fim" -lt "USGS" -ac "True"

    # Run with 6 workers:
    python reformat_ras_rating_curve.py
        -i 'C:/ras2fim_data/output_ras2fim' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves'
        -v -l -j 6

    # Input the data source, location type, and active information using the -so, -lt, and -ac flags:
    python reformat_ras_rating_curve.py
        -i 'C:/ras2fim_data/output_ras2fim' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves'
        -v -so "ras2fim" -lt "USGS" -ac "True"

    Notes:
       - Required arguments: None
       - Optional arguments: use the -i tag to specify the input ras2fim filepath
                                (defaults to c:\ras2fim_data\output_ras2fim)
                             use the -o tag to specify the output save folder
                                (defaults to c:\ras2fim_data\ras2fim_releases\compiled_rating_curves)
                             use the -l tag to save the output log
                             use the -v tag to run in a verbose setting
                             use the -j flag followed by the number to specify number of workers
                             use the -so flag to input a value for the "source" output column
                                 (i.e. "ras2fim", "ras2fim v2.1")
                             use the -lt flag to input a value for the "location_type" output column
                                 (i.e. "USGS", "IFC")
                             use the -ac flag to input a value for the "active" column ("True" or "False")

    """

    # There is a known problem with  proj_db error.
    # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db.
    # This will not stop all of the errors but some (in multi-proc).
    sf.fix_proj_path_error()

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Iterate through a directory containing ras2fim outputs and "
        "compile a rating curve table and rating curve location point file."
    )
    parser.add_argument(
        "-i",
        "--input-path",
        help="Input directory containing ras2fim outputs to process.",
        required=False,
        default="",
    )
    parser.add_argument("-o", "--output-path", help="Output save folder.", required=False, default="")
    parser.add_argument(
        "-l",
        "--log",
        help="Option to save output log to output save folder.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Option to print more updates and descriptions.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-j", "--num-workers", help="Number of concurrent processes", required=False, default=1, type=int
    )
    parser.add_argument(
        "-lt",
        "--location-type",
        help='Input a value for the "location_type" output column (i.e. "USGS", "IFC").',
        required=False,
        default="",
    )
    parser.add_argument(
        "-ac",
        "--active",
        help='Input a value for the "active" column ("True" or "False")',
        required=False,
        default="",
    )
    parser.add_argument(
        "-u",
        "--out-unit",
        help='Option to specify output units. Defaults to "feet".',
        required=False,
        default="feet",
    )

    # Assign variables from arguments
    args = vars(parser.parse_args())
    input_folder_path = args["input_path"]
    output_save_folder = args["output_path"]
    log = bool(args["log"])
    verbose = bool(args["verbose"])
    num_workers = args["num_workers"]
    location_type = str(args["location_type"])
    active = str(args["active"])
    source = "ras2fim"

    log_file_folder = args["output_path"]
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

        # call main program
        compile_ras_rating_curves(
            input_folder_path, output_save_folder, log, verbose, num_workers, source, location_type, active
        )

    except Exception:
        if ras2fim_logger.LOG_SYSTEM_IS_SETUP is True:
            ras2fim_logger.logger.critical(traceback.format_exc())
        else:
            print(traceback.format_exc())
