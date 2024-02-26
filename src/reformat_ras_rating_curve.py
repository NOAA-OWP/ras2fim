#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import sys
import traceback
from pathlib import Path

import geopandas as gpd
import pandas as pd

import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG


# -----------------------------------------------------------------
# Writes a metadata file into the save directory
# -----------------------------------------------------------------
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
        "source             From ras2fim changelog  String          ras2fim version that produced the data"
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
        print()
        RLOG.debug(f"Metadata README saved to {metadata_path}")


# -----------------------------------------------------------------
# Reads, compiles, and reformats the rating curve info for all directories
# -----------------------------------------------------------------
def dir_reformat_ras_rc(src_unit_dir_path, active, verbose):
    """
    Overview:

    Reads, compiles, and reformats the rating curve info for the given directory
    (runs in compile_ras_rating_curves).

    Notes:

        - Automatically overwrites the main outputs (the compiled CSV, geopackage, and log) if they already
          exist in the output folder. If there is a need to keep the existing main outputs, use a different
          output folder. # TODO: Update once I've finalized functionality

    Inputs:
    - src_unit_dir_path: e.g. C:\ras2fim_data\output_ras2fim\12040101_102739_ble_230922
    - active: (str) optional input value for the "active" column (i.e. "", "True", "False")
    - verbose: (bool) option to run verbose code with a lot of print statements
      (optional argument set in __main__)

    """

    arg_values = locals().copy()

    # Create empty output log
    print()
    overall_start_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    print()
    RLOG.lprint("+=================================================================+")
    RLOG.notice("                   Reformat Rating Curves                        ")
    RLOG.lprint(f"--- (-s) ras2fim_huc_dir: {src_unit_dir_path}")
    RLOG.lprint(f"--- (-a) active: {active}")
    RLOG.lprint(f"--- (-v) is verbose: {str(verbose)}")
    RLOG.lprint(f"  Started (UTC): {dt_string}")

    # --------------------
    # validate input variables and setup key variables
    # NOTE: for now.. not all key variables are being setup, migth move some later.
    __validate_input(**arg_values)

    # ---------------
    # splits it a six part dictionary, we don't use all here
    src_name_dict = sf.parse_unit_folder_name(src_unit_dir_path)
    if "error" in src_name_dict:
        raise Exception(src_name_dict["error"])

    huc8 = src_name_dict["key_huc"]
    source_code = src_name_dict["key_source_code"]

    # -----------------------------------------------------------------------------------------
    # Manually build filepaths for the geospatial data
    nwm_streams_folder_path = os.path.join(src_unit_dir_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF)
    hecras_shapes_dir_path = os.path.join(src_unit_dir_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS)
    ratings_curves_dir_name = os.path.join(src_unit_dir_path, sv.R2F_OUTPUT_DIR_CREATE_RATING_CURVES)
    ref_rc_output_dir_path = os.path.join(
        src_unit_dir_path, sv.R2F_OUTPUT_DIR_FINAL, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION
    )
    output_table_file_path = os.path.join(ref_rc_output_dir_path, sv.R2F_OUTPUT_FILE_RAS2CAL_CSV)
    output_gpkg_file_path = os.path.join(ref_rc_output_dir_path, sv.R2F_OUTPUT_FILE_RAS2CAL_GPKG)

    nwm_all_lines_filename = huc8 + "_nwm_streams_ln.shp"
    nwm_all_lines_filepath = os.path.join(nwm_streams_folder_path, nwm_all_lines_filename)
    if not os.path.exists(nwm_all_lines_filepath):
        RLOG.critical(f"Error: No file at {nwm_all_lines_filepath}")
        sys.exit(1)

    hecras_crosssections_filename = "cross_section_LN_from_ras.shp"
    hecras_crosssections_filepath = os.path.join(hecras_shapes_dir_path, hecras_crosssections_filename)
    if not os.path.exists(hecras_crosssections_filepath):
        RLOG.critical(f"Error: No file at {hecras_crosssections_filepath}")
        sys.exit(1)

    # ---------------------------------------------------------------------------------
    # Get ras2fim version and assign to 'source' variable
    changelog_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, 'doc', 'CHANGELOG.md')
    )
    ras2fim_version = sf.get_changelog_version(changelog_path)
    source = "ras2fim_" + ras2fim_version

    # ---------------------------------------------------------------------------------------------
    # Retrieve information from `run_arguments.txt` file
    # Open the unit run_argsments.text file and read all lines from the file
    try:
        run_arguments_filepath = os.path.join(src_unit_dir_path, "run_arguments.txt")
        with open(run_arguments_filepath, "r") as file:
            run_args_file_lines = file.readlines()
    except Exception:
        RLOG.critical("Unable to open run_arguments.txt.")
        RLOG.critical(traceback.format_exc())
        sys.exit(1)

    if run_args_file_lines is not None:
        # Search for and extract the projection from run_arguments.txt
        # only looking for one value at this time, in full form:  ie) EPSG:2277
        for line in run_args_file_lines:
            if "proj_crs ==" in line:
                proj_crs = line.split("==")[1].strip()
                break

        if proj_crs == "":
            RLOG.critical("Unable to find the 'proc_crs' variable value in the run_arguments.txt file")
            sys.exit(1)

    else:
        RLOG.critical("Unable to correctly read the run_arguments.txt")
        RLOG.critical(traceback.format_exc())
        sys.exit(1)

    # e.g. C:\ras2fim_data\output_ras2fim\12030106_2276_ble_240224\ras2calibation
    if not os.path.exists(ref_rc_output_dir_path):
        os.mkdir(ref_rc_output_dir_path)

    # -----------------------------------------------------------------------------------------
    # Intersect NWM lines and HEC-RAS crosssections to get the points
    # (but keep the metadata from the HEC-RAS cross-sections)
    print()
    RLOG.notice("** Reading shapefiles and generating cross section/streamline intersection points ...")
    RLOG.lprint(f"-- Reading {hecras_crosssections_filepath}")
    # Read shapefiles
    hecras_crosssections_shp = gpd.read_file(hecras_crosssections_filepath)

    # Iterate over rows usign the full pathed ras_path column, extracting
    # the parent folder name from each row.
    # C:\ras2fim_data\OWP_ras_models\models-12030106-small\
    # 1291898_UNT705 in EFT Watershed_g01_1701646099\UNT705 in EFT Watershed.g01
    # becomes: 1291898_UNT705 in EFT Watershed_g01_1701646099
    # Create the empty colummn first
    hecras_crosssections_shp["ras_model_dir"] = ""

    for i in range(len(hecras_crosssections_shp)):
        orig_src_model_path = hecras_crosssections_shp.at[i, "ras_path"]
        orig_src_model_folder_name = os.path.basename(os.path.dirname(orig_src_model_path))
        hecras_crosssections_shp.at[i, "ras_model_dir"] = orig_src_model_folder_name

    RLOG.lprint(f"-- Reading {nwm_all_lines_filepath}")
    nwm_all_lines_shp = gpd.read_file(nwm_all_lines_filepath)

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

    # Combined feature ID and HECRAS cross-section ID to make a new ID (e.g. 5791000_189926)
    intersection_gdf["fid_xs"] = (
        intersection_gdf["feature_id"].astype(str) + "_" + intersection_gdf["stream_stn"].astype(str)
    )

    # Add necessary columns to the intersections ge
    intersection_gdf["location_type"] = source_code
    intersection_gdf["source"] = source
    intersection_gdf["timestamp"] = dt_string
    intersection_gdf["active"] = active
    intersection_gdf["flow_units"] = "cfs"
    intersection_gdf["wse_units"] = "ft"

    # we have some columns we don't need.
    intersection_gdf.drop(['ras_path_1', 'ras_path_2', 'huc12', 'huc10'], axis=1, inplace=True)

    # Reproject intersection_gdf_all to output SRC
    shared_variables_crs = sv.DEFAULT_RASTER_OUTPUT_CRS
    intersection_prj_gdf = intersection_gdf.to_crs(shared_variables_crs)

    # Save points geopackage
    try:
        intersection_prj_gdf.to_file(output_gpkg_file_path, driver="GPKG")
        print()
        RLOG.lprint(f"HECRAS-NWM intersection points geopackage saved as {output_gpkg_file_path}.")
    except Exception:
        RLOG.critical("Unable to save HEC-RAS points geopackage.")
        RLOG.critical(traceback.format_exc())
        sys.exit(1)

    # -----------------------------------------------------------------------------------------
    # Get compiled rating curves from unit rating curves folder (06...)

    # At this point we have a df that is an intersected from
    #     01_shapes_from_hecras\cross_section_LN_from_ras.shp and
    #     02_csv_shapes_from_conflation\[huc8 number]_nwm_streams_ln.shp
    # Now we need to iterate through all RC paths which are multiple all_xs_info_fid_*
    # 06_create_rating_curves\10008_UNT 013 IN BCTR\Rating_Curve\all_xs_info_fid_1484758.csv
    # Will merge it to the new intersection_gdf

    RLOG.notice("** Processing model ratings curves")

    rc_path_list = list(Path(ratings_curves_dir_name).rglob("all_xs_info_fid_*"))
    if len(rc_path_list) == 0:
        RLOG.critial("ERROR: No 'all_xs_info_fid_*' files found in rating curve path list.")
        sys.exit(1)

    for i in range(len(rc_path_list)):
        rc_path = rc_path_list[i]

        parent_dir = os.path.dirname(rc_path).split("\\")[-1]
        file_name = os.path.basename(rc_path)
        file_and_parent = parent_dir + "\\" + file_name
        RLOG.lprint(f"-- Processing: {file_and_parent}")

        # ---------------------------------------------------------------------------------
        # Read compiled rating curve and append huc8 from intersections
        try:
            rc_df = pd.read_csv(rc_path)
        except Exception:
            RLOG.critical(f"Unable to read rating curve at path {rc_path}")
            RLOG.critical(traceback.format_exc())
            sys.exit(1)

        # Combined feature ID and HECRAS cross-section ID to make a new ID (e.g. 5791000_189926)
        rc_df["fid_xs"] = rc_df["feature_id"].astype(str) + "_" + rc_df["Xsection_name"].astype(str)

        # Join some of the geospatial data to the rc_df data
        # this is for the csv, but not the gpkg
        rc_geospatial_df = pd.merge(
            rc_df,
            intersection_gdf[["fid_xs", "huc8", "ras_model_dir"]],
            left_on="fid_xs",
            right_on="fid_xs",
            how="inner",
        )

        # Check that merge worked
        if len(rc_geospatial_df) == 0:
            msg = f"No rows survived the merge of rc_geospatial with the rating curve rows for {rc_path}."
            RLOG.critical(msg)
            sys.exit(1)

        # rlog.trace('text') ## goes to the log file but doesn't print!!

        # print("len(rc_df): ") ## debug
        # print(len(rc_df)) ## debug
        # print() ## debug
        # print('rc_df') ## debug
        # print(rc_df["fid_xs"]) ## debug
        # print() ## debug
        # print('intersection_gdf') ## debug
        # print(intersection_gdf["fid_xs"]) ## debug
        # print() ## debug

        rc_geospatial_df = rc_geospatial_df.astype({"huc8": "object"})

        # ---------------------------------------------------------------------------------
        # Build output table

        # Assemble output table
        # Ensure the "source" column always has the phrase 'ras2fim' in it somewhere (fim needs it)
        dir_output_table = pd.DataFrame(
            {
                "fid_xs": rc_geospatial_df["fid_xs"],
                "feature_id": rc_geospatial_df["feature_id"],
                "xsection_name": rc_geospatial_df["Xsection_name"],  # used to be Xsection_name
                "flow_cfs": rc_geospatial_df["discharge_cfs"],
                "wse_ft": rc_geospatial_df["WSE_Feet"], 
                "flow_units": "cfs",  # str
                "wse_units": "ft",  # str 
                "location_type": source_code,  # str
                "source": source,  # str
                "timestamp": dt_string,  # str
                "active": active,  # str
                "huc8": rc_geospatial_df["huc8"],  # str
                "ras_model_dir": rc_geospatial_df["ras_model_dir"],  # str
            }
        )

        # Append to output objects (both the csv and the points gpkg)
        if i == 0:
            dir_output_table_all = dir_output_table
        else:
            dir_output_table_all = pd.concat([dir_output_table_all, dir_output_table])

    # -------------------------------------------------------------------------------------
    # Save output table for directory
    dir_output_table_all.to_csv(output_table_file_path, index=False)
    print()
    RLOG.lprint(f"reformat csv output table saved as {output_table_file_path}.")

    # Get timestamp for metadata
    start_time_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    int_log_label = 'none'

    # Write README metadata file for the intermediate file
    write_metadata_file(
        ref_rc_output_dir_path,
        start_time_string,
        nwm_streams_folder_path,
        hecras_shapes_dir_path,
        ratings_curves_dir_name,
        output_gpkg_file_path,
        output_table_file_path,
        int_log_label,
        verbose,
    )

    print()
    RLOG.success("Complete")
    end_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Ended : {dt_string}")
    time_duration = end_time - overall_start_time
    RLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    print(f"log files saved to {RLOG.LOG_FILE_PATH}")
    print()


# -------------------------------------------------
#  Some validation of input, but also creating key variables ######
def __validate_input(src_unit_dir_path, active, verbose):
    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned
    # Note: No return at this time, but most scripts using this pattern do and
    # this one might later.

    # rtn_dict = {}

    # ---------------
    # why is this here? might not come in via __main__
    if src_unit_dir_path == "":
        raise ValueError("Source src_unit_dir_path (-s) parameter value can not be empty")

    if not os.path.exists(src_unit_dir_path):
        raise ValueError(f"Source unit folder not found at {src_unit_dir_path}")

    # no need to return src_unit_dir_path back

    # TODO: NTH, can move some to the derived varaibles into it from the dir_reformat_ras_rc function
    # but we don't need to.


# -------------------------------------------------
if __name__ == "__main__":
    """
    Sample usage:

    # Minimalist run (all defaults used):
    python reformat_ras_rating_curve.py -s C:\ras2fim_data\output_ras2fim\12030106_2276_ble_240224

    Notes:
       - Required arguments: None
       - Optional arguments: -v     verbose (to make verbose, put -v in the command)
                             -a     value for the "active" column (i.e. "True", "False", ""; defaults to "")

    """

    # There is a known problem with proj_db error.
    # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db.
    # This will not stop all of the errors but some (in multi-proc).
    sf.fix_proj_path_error()

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Reformat the ras2fim rating curve outputs to be used in HAND FIM calibration."
    )

    parser.add_argument(
        "-s",
        "--src_unit_dir_path",
        help="REQUIRED: A full defined path including output unit folder.\n"
        r" ie) c:\my_ras\output\12030202_102739_ble_230810",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        help="OPTIONAL: Use to have more status messages and updates during the run.",
        required=False,
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-a",
        "--active",
        help='OPTIONAL: Input a value for the "active" column ("True" or "False")',
        required=False,
        default="",
    )

    # Assign variables from arguments
    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        src_unit_dir_path = args["src_unit_dir_path"]
        parent_dir = os.path.dirname(src_unit_dir_path)
        log_file_folder = os.path.join(parent_dir, "logs")

        # Creates the log file name as the script name and assumes RLOG has been added as a global var.
        script_file_name = os.path.basename(__file__).split('.')[0]
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        dir_reformat_ras_rc(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
