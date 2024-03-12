#!/usr/bin/env python3

import argparse
import datetime as dt
import glob
import os
import sys
import traceback
from pathlib import Path

import colored as cl
import geopandas as gpd
import gval.utils.exceptions as gue
import pandas as pd
import rioxarray.exceptions as rxe


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import ras2inundation as ri
import s3_shared_functions as s3_sf
from evaluate_ras2fim_unit import evaluate_unit_results

import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG

"""
TODO:  explain how this works


This tool can already work in auto mode. No runtime questions are asked.

TODO: Later, do we optionally load this back up to S3?
   For now.. manually upload C:\ras2fim_data\gval\evaluations\eval_PROD_metrics.csv (to the eval level)
   Also upload the unit level master metrics to S3. e.g.
      C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\12030105_2276_ble_unit_metrics.csv
        (notice not in the version subfolder.)

"""

# ***********************
# NOTICE:
#    March 5 2024: Due to time constraints, most testings of combinations of input arguments have not yet
#        not been done. Testing has only be done against the default happy path with all defaults
#        for optional args.
# ***********************


# -------------------------------------------------
def run_unit_benchmark_tests(
    unit_folder_name,
    enviro,
    src_unit_final_path,
    trg_gval_root,
    trg_output_override_path,
    src_benchmark_data_path,
):
    """
    TODO Processing notes (lots of permuations)

    Inputs:
        - unit_folder_name: e.g. 12030101_2276_ble_230925
        - enviro: e.g. PROD or DEV
        - src_unit_final_path:
           e.g. C:\ras2fim_data\output_ras2fim\12030101_2276_ble_230925\final
           and could be empty
        - trg_gval_root: e.g. c:\ras2fim_data\gval
        - trg_output_override_path: e.g.(blank) or c:\my_ras\inundation_files
        - src_benchmark_data_path:
            e.g. C:\ras2fim_data\gval\benchmark_data or an s3 url
    """

    arg_values = locals().copy()

    start_dt = dt.datetime.utcnow()

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.notice("          RUN Unit Benchmark Test ")
    RLOG.lprint(f"  (-u):  Source unit folder name: {unit_folder_name} ")
    RLOG.lprint(f"  (-sg): Source unit local path for the 'final' folder: {src_unit_final_path}")
    RLOG.lprint(f"  (-b):  Source benchmark data path: {src_benchmark_data_path}")
    RLOG.lprint(f"  (-e):  Environment type: {enviro}")

    if trg_gval_root != "":
        RLOG.lprint(f"  (-tg): Local target gval root path: {trg_gval_root}")
    if trg_output_override_path != "":
        RLOG.lprint(f"  (-to): Local target eval output path: {trg_output_override_path}")

    RLOG.lprint(f" Started (UTC): {sf.get_stnd_date()}")
    print()
    print("NOTE: All output inundation and benchmark results files will be overwritten.")
    print()

    RLOG.notice("********************************************************************")
    RLOG.notice("***  IMPORTANT NOTE about roll up metrics files\n")
    print(" Each unit (ie. 12030105_2276_ble) can have multiple versions over time.")
    print("     ie) 230923  or   240217")
    print(" This tool processes one unit and version at this time.")
    print("     ie) 12030105_2276_ble_230923   or   12030105_2276_ble_240217")
    print(
        " Here is an example of a pre-existing source unit path is" " (unless the path is overridden (-sg)):"
    )
    print(r"     ie). C:\ras2fim_data\output_ras2fim\12030105_2276_ble_230923.")
    print()
    print(
        " When the tool has completed the evaluations, the eval outputs will be saved"
        " in an unit version subfolder (or override argument (-to)."
    )
    print(r"     ie) C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923")
    print()
    print(" This tool creates a couple of metrics (.csv) files.")

    print(
        "   - One file is at the unit and version level which covers all metrics created"
        " during the running of this script."
    )
    print(
        r"       ie) C:\ras2fim_data\gval\evaluations\PROD"
        r"\12030105_2276_ble\240228\12030105_2276_ble_240228_metrics.csv"
    )
    print()
    print(
        "   - The second is a rollup csv at the 'PROD' or 'DEV' level, called a master enviro metrics."
        " It covers all metrics for all evaluations for that enviro."
    )
    print(r"       ie) C:\ras2fim_data\gval\evaluations\PROD\eval_PROD_metrics.csv")
    print(
        "     If you have used the local target eval output path (-to), the master enviro"
        " file will not be created or updated."
    )

    print()
    print(" HOWEVER... The intent is to have a growing master 'eval_(PROD/DEV)_metrics.csv' file.")
    print(
        " And at this time, this script does not have the ability to get the file from somewhere else.\n"
        " You may need to copy it in from somewhere else or let the program build a new one master metrics"
        " file which you can optionally merge manually later."
    )
    print()
    print(" *** Remember to save the new or updated master PROD/DEV metrics to s3 or similar for next time.")
    print()

    msg = (
        f"{cl.fore.SPRING_GREEN_2B}"
        " Do you want to continue?\n\n"
        f"{cl.style.RESET}"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'yes'{cl.style.RESET}\n"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'no' (or any key){cl.style.RESET}"
        " to stop the program.\n"
        f"{cl.fore.SPRING_GREEN_2B}  ?={cl.style.RESET}"
    )

    print()
    resp = input(msg).lower()
    if resp != "yes":
        RLOG.lprint("\n.. Program stopped.\n")
        sys.exit(0)

    RLOG.notice("********************************************************************")

    # TODO: test permuations of the input args
    print()
    RLOG.notice(
        "NOTE: As of Mar 5, 2025, some of the testing for non-defaulted args has" " not yet been completed."
    )
    print()
    print()

    # ----------------
    # validate input variables and setup key variables
    # rd = Return Variables Dictionary
    # Not all inputs need to be returned from rd or reloaded.
    rd = __validate_input(**arg_values)
    huc = rd["huc"]

    # ----------------
    # We might be downloaded from S3,
    # but we get a list of local huc applicable benchmark csv files
    if rd["is_s3_path"] is True:
        get_s3_benchmark_data(huc, rd["s3_src_benchmark_data_path"], rd["local_benchmark_data_path"])
    else:  # get them locally (list of the huc applicable benchmark csv's)
        RLOG.lprint(f"Looking for local benchmark files for huc {huc}")

    # let's build up the glob search pattern to look for the HUC number starting
    # with the src_benchmark_data_path.
    bench_data_root = rd["local_benchmark_data_path"]
    if bench_data_root.endswith("\\") is False:
        bench_data_root += "\\"

    lst_bench_flow_files = glob.glob(f"{bench_data_root}**\\*{huc}_flows_*.csv", recursive=True)
    if len(lst_bench_flow_files) == 0:
        RLOG.critical(
            "No csv benchmark data files were found recursively in the folder"
            f" of {bench_data_root} with the huc of {huc}"
        )
        sys.exit(1)

    # ----------------
    # We need to keep just the csv for inundation at this point.
    # bench_flow_files = [ i for i in lst_bench_files if Path(i).suffix == ".csv"]
    # Now we iterate the bench_files to find the valid flow files we need.
    bench_flow_files = []
    for b_file in lst_bench_flow_files:
        parent_path = Path(b_file)
        parent_dir_name = parent_path.parent.name
        if parent_dir_name in sv.GVAL_VALID_STAGES:
            bench_flow_files.append(b_file)

    inundated_files = inundate_files(
        bench_flow_files, rd["src_geocurves_path"], rd["trg_inundation_path"], rd["local_benchmark_data_path"]
    )

    # There may not necessarily any file after inundation.
    if len(inundated_files) == 0:
        print()
        RLOG.notice(
            "No files were inundated. This occurs generally when there are small amounts"
            "number of geo curve files and no matching features were found in the flow files."
        )
    else:
        # ----------
        metric_files_lst = __run_tests(
            bench_flow_files,
            unit_folder_name,
            rd["src_models_domain_extent_file"],
            rd["trg_inundation_path"],
            rd["local_benchmark_data_path"],
            rd["enviro"],
        )

        # ----------
        # merge this wil the unit master csv
        if len(metric_files_lst) == 0:
            RLOG.notice(
                "No metrics files were created. This can occur when there are small amounts"
                "number of geo curve files and no matching features were found in the flow files."
            )
        else:
            __merge_metrics_files(
                metric_files_lst,
                rd["unit_id"],
                rd["unit_version_as_str"],
                rd["trg_inundation_metrics_file"],
                rd["trg_enviro_metrics_file_path"],
            )

    print()
    print("===================================================================")
    RLOG.success("Unit Benchmark testing processing complete")
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.success(f"Ended (UTC): {dt_string}")
    RLOG.success(f"log files saved to {RLOG.LOG_FILE_PATH}")

    dur_msg = sf.get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


# -------------------------------------------------
def __run_tests(
    bench_flow_files,
    unit_folder_name,
    src_models_domain_extent_file,
    trg_inun_file_path,
    local_benchmark_data_path,
    enviro,
):
    """
    Process: Iterates the incoming local benchmark and run them against evaluate_ras2fim_unit.py
    Input:
        - bench_flow_files: The list of raw benchmark flow files.
        - unit_folder_name:  12090301_2277_ble_230923
        - src_models_domain_extent_file: e.g. C:\ras2fim_data\output_ras2fim\
             12090301_2277_ble_230923\final\models_domain\dissolved_conflated_models.gpkg
        - trg_inun_file_path:
            e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923
        - local_benchmark_data_path: (we use this to re-calc pathing for the output folders)
            e.g. C:\ras2fim_data\gval\benchmark_data.
        - enviro: PROD / DEV
    """

    print("--------------------------")
    RLOG.notice(
        "Runnning unit benchmarks based on unit model domain" f" from  {src_models_domain_extent_file}"
    )
    RLOG.lprint(f"  All output evaluations files will created in {trg_inun_file_path}")
    print()

    # don't let if fail if one errors out, unless all fail.
    bench_flow_files.sort()

    # get root of benchmark as it can be overwridden from cmd line.

    # get the version from the dissolved_conflated_models.gpkg or models_domain.gpkg (from V1)
    # NOTE: I went back and manually added that column and values to the v1 models_domain.gpkg

    unit_details_dct = sf.parse_unit_folder_name(unit_folder_name)

    models_gdf = gpd.read_file(src_models_domain_extent_file)
    code_version = models_gdf.iloc[0]["version"]
    metric_files = []
    trg_inun_file_path = trg_inun_file_path
    for b_flow_file in bench_flow_files:
        bench_name_details = parse_bench_file_name(b_flow_file, local_benchmark_data_path)

        try:
            bench_prefix = bench_name_details["prefix"]
            #  e.g.  ble_100yr  or  nws_ahps_dalt2_minor

            unit_eval_name = unit_folder_name + "--" + bench_prefix
            # becomes ie) 12030105_2276_ble_230923--ble_100yr  or 12030105_2276_ble_230923--nws_ahps_minor

            RLOG.trace(f"Running Benchmark tests for {unit_eval_name}")

            # I know this isn't the prettiest but it will work for now.
            # let's go get the associated .tif - Lets assume that the tif file is the same file
            # name as the csv

            # get folder path from csv
            bench_folder = os.path.dirname(b_flow_file)
            flow_file_name = os.path.basename(b_flow_file)
            bench_extent_file_name = flow_file_name.replace(".csv", ".tif")
            bench_extent_file_name = bench_extent_file_name.replace("_flows_", "_extent_")
            bench_extent_raster_path = os.path.join(bench_folder, bench_extent_file_name)

            eval_output_folder = os.path.join(trg_inun_file_path, bench_prefix)
            # files names are added inside evaluate_unit_results
            # becomes ie: C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923\nws_ahps_dalt2_major

            inun_poly_name = f"{bench_prefix}_inundation.gpkg"
            inundation_poly_path = os.path.join(trg_inun_file_path, inun_poly_name)
            # C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923\ble_100yr_inundation.gpkg

            # A matching inundation file may not necessarily exists.
            if not os.path.exists(inundation_poly_path):
                continue

        except Exception as ex:
            err_msg = f"An error occured while setting up info unit benchmark tests for {unit_folder_name}"
            RLOG.critical(err_msg)
            raise ex

        # Feb 21, 2024: For reasons unknown, when using VSCode debug,
        # it throws exceptions for evaluate_unit_results.
        # Fix: run it via command line, come back, temp disable this part and continue.
        try:

            data_details = f" inundation_poly_path is {inundation_poly_path},"
            f" src_models_domain_extent_file is {src_models_domain_extent_file},"
            f" bench_extent_raster is {bench_extent_raster_path},"
            f" unit_folder_name is {unit_folder_name}"

            evaluate_unit_results(
                inundation_poly_path,
                src_models_domain_extent_file,
                bench_extent_raster_path,
                unit_eval_name,
                eval_output_folder,
            )

        except gue.RastersDontIntersect:

            RLOG.warning(
                f"An issue occured while running gval results for {unit_folder_name};"
                " Rasters don't spatially intersect. This will be very common with ras2fim."
            )
            RLOG.warning(data_details)
            continue

        except rxe.NoDataInBounds:

            RLOG.warning(
                f"An issue occured while running gval results for {unit_folder_name};"
                " No data found in bounds. This is generally acceptable"
                " especially when the number of processed models is small"
            )
            RLOG.warning(data_details)
            continue

        except Exception as ex:
            # re-raise but check if it is includes phrase 'Rasters don't spatially intersect'
            # so we can give a better error message.
            # Add context data and RLOG.

            # Some error messages coming from eval will say No data found in bounds.
            # This is an acceptable and semi common error. It just means there are not
            # any models that are fitting in the benchmark boundaries.
            # We will just log them and continue
            if "Rasters don't spatially intersec" in ex.args[0]:
                RLOG.warning(
                    f"An issue occured while running gval results for {unit_folder_name};"
                    " The rasters don't spatially interset. This is generally acceptable"
                    " especially when the number of processed models is small"
                )
                RLOG.warning(data_details)
                continue

            RLOG.critical(f"An error occured while running gval results for {unit_folder_name};")
            RLOG.critical(data_details)
            raise ex

        # before coping around and merging HUC / GVAL results, pull the "version" column
        # from models_domain (dissoved)
        # and put it in the gval / results. Not availalbe in V1, but manually add to a V1 model_domain
        # metrics output needs the following columns added:

        # huc  (112030105)
        # unit_name   (112030105_2276_ble)
        # unit_version  (230923)
        # code_version  (v2.0.0)
        # benchmark_source  (nws)
        # magnitude  (major)
        # ahps_lid  ( na  or  dalt2)
        # enviro  (PROD or DEV)

        # ud means unit_dictionary (parts)
        ud = sf.parse_unit_folder_name(unit_folder_name)
        if "error" in ud:
            raise Exception(ud["error"])

        metrics_file_path = os.path.join(eval_output_folder, "metrics.csv")

        metrics_df = pd.read_csv(metrics_file_path)
        if "unit_name" not in metrics_df.columns:
            col_id = 0
            metrics_df.insert(col_id, "unit_name", unit_details_dct["key_unit_id"])

        if "unit_version" not in metrics_df.columns:
            col_id = 1
            metrics_df.insert(col_id, "unit_version", unit_details_dct["key_unit_version_as_str"])
            metrics_df["unit_version"] = metrics_df["unit_version"].astype("string")

        if "code_version" not in metrics_df.columns:
            col_id = 2
            metrics_df.insert(col_id, "code_version", code_version)

        if "huc" not in metrics_df.columns:
            col_id = 3
            metrics_df.insert(col_id, "huc", unit_details_dct["key_huc"])

        if "benchmark_source" not in metrics_df.columns:
            col_id = 4
            metrics_df.insert(col_id, "benchmark_source", bench_name_details["source"])

        if "magnitude" not in metrics_df.columns:
            col_id = 5
            metrics_df.insert(col_id, "magnitude", bench_name_details["magnitude"])

        if "ahps_lid" not in metrics_df.columns:
            col_id = 6
            metrics_df.insert(col_id, "ahps_lid", bench_name_details["ahps_lid"])

        if "enviro" not in metrics_df.columns:
            col_id = 7
            metrics_df.insert(col_id, "enviro", enviro)

        metrics_df.to_csv(metrics_file_path, index=False)

        metric_files.append(metrics_file_path)

    return metric_files


# -------------------------------------------------
def __merge_metrics_files(
    metric_files, unit_id, version_date_as_str, trg_inundation_metrics_file, trg_enviro_metrics_file_path
):
    """
    All of the individual benchmark tests folder have their own metrics,
    which is rolled up into a metrics file for this processing run. It should be in the folder
    immediately above where all of the individual benchmark test folders are at.
    If one aleady exist, it will be removed and re-created.

    Another rollup csv may be create. This is a master metrics for the enviro (PROD or DEV)
    Depending on parameters, the trg_enviro_metrics_file_path may be blank. If it does exist
    We will attempt to write to it (usually just append but can replace some of its own unit/version
    records if they exist)

    Inputs:
        - metric_files: a list of the full pathed just created metrics (one per sourc / magnitude)
        - unit_id: 12030105_2276_ble
        - version_date_as_str: e.g. 230923
        - trg_inundation_metrics_file: A full path to the unit version level metrics.
             e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble
                    \230923\12030105_2276_ble_230923_metrics.csv
        - trg_enviro_metrics_file_path:
             e.g. C:\ras2fim_data\gval\evaluations\PROD\eval_PROD_metrics.csv

        (Note: Users can override the name of the version level metrics file, just the root local gval path)
    Output:
        There two metrics file that will be created (at least one).

        The first is at inundation path level (for just this run) (unit_id and version)
            see trg_inundation_metrics_file above

        The second is at the enviro level
            if the trg_enviro_metrics_file_path is not empty:
               See if one already exists, and load it if it does exist, create a new one if not.
            else:
                we are not going to be working with a master enviro file

            When writing to an existing master metrics, and it sees records already existing for the
            current unit_id and version, it will drop them as it is about to replace them.
    """

    # if the unit_version_metrics_file exists, delete it as we will rebuild it.
    if os.path.exists(trg_inundation_metrics_file):
        os.remove(trg_inundation_metrics_file)

    if trg_enviro_metrics_file_path != "":
        if os.path.exists(trg_enviro_metrics_file_path) is True:
            RLOG.lprint(f"Merging new metrics files to enviro master csv at {trg_enviro_metrics_file_path}")
        else:
            RLOG.trace(
                "Enviro master metrics file does not exist. Creating new one"
                f"at {trg_enviro_metrics_file_path}"
            )

    enviro_metrics_df = pd.DataFrame()
    inun_metrics_df = pd.DataFrame()  # unit_version
    for idx, metrics_file in enumerate(metric_files):
        if idx == 0:

            # load the first metrics, load it straight to the inun metrics parent
            inun_metrics_df = pd.read_csv(metrics_file)

            # load the master metrics if one exists and a path exist
            if trg_enviro_metrics_file_path != "":
                if os.path.exists(trg_enviro_metrics_file_path) is True:
                    enviro_metrics_df = pd.read_csv(trg_enviro_metrics_file_path)
                    RLOG.trace(f"Merging new metrics file of {metrics_file}")

                    # drop pre-existing records for this unit and its version
                    indexes_lst = enviro_metrics_df[
                        (enviro_metrics_df['unit_version'].astype("string") == version_date_as_str)
                        & (enviro_metrics_df['unit_name'] == unit_id)
                    ].index
                    enviro_metrics_df.drop(indexes_lst, inplace=True)

                    # Concat to master if one exists
                    con_df = pd.concat([enviro_metrics_df, inun_metrics_df], ignore_index=True)
                    enviro_metrics_df = con_df
                else:  # we are creating the first records for the master metrics
                    enviro_metrics_df = inun_metrics_df.copy()
            # else: - we are not creating an enviro master metrics

        else:
            RLOG.trace(f"Concatenating metrics file of {metrics_file}")
            metrics_df = pd.read_csv(metrics_file)

            # I heard it not good to write directly back to a df progress of concat
            con_df = pd.concat([inun_metrics_df, metrics_df], ignore_index=True)
            inun_metrics_df = con_df

            if trg_enviro_metrics_file_path != "":  # if there is a master (migth not be)
                # concat to the version level metrics as well.
                con_df = pd.concat([enviro_metrics_df, metrics_df], ignore_index=True)
                enviro_metrics_df = con_df

    print()
    if len(trg_inundation_metrics_file) == 0:
        RLOG.critical("Error: no records were added to the inundation level metrics" " (unit id and version)")
        sys.exit(1)

    inun_metrics_df.to_csv(trg_inundation_metrics_file, index=False)
    RLOG.notice(
        "The rolled up metrics file for this specific unit and unit version have"
        f" been created as well. It has been saved to {trg_inundation_metrics_file}."
    )
    print()
    if trg_enviro_metrics_file_path != "":
        enviro_metrics_df.to_csv(trg_enviro_metrics_file_path, index=False)
        RLOG.notice(
            "Created or updated the rolled up enviro master level metrics file"
            f" at {trg_enviro_metrics_file_path}. All new metrics files have been added to this file."
        )
    print()


# -------------------------------------------------
def parse_bench_file_name(file_path, local_benchmark_data_path):
    """
    Process:
      Using the file name, we extract out the benchmark type (ie, ble, ifc)
      and we extract out the magnitude to make a prefix
      (ie: ble_100yr or nws_ahps_major)

      We use the incoming file_name (and path) to figure that out

    Returns:
        A dictionary:
           - prefix: ie) ble_100yr or nws_ahps_dalt2_minor
           - source: ie) ble, nws, ifs
           - ahps_lid: ie) na  or dalt2
           - magnitude: ie) 100yr  or  minor, major, etc

    """
    # adj_file_path: ie) ble\12030105\100py\ble_huc_12030105_extent_100yr.tif
    adj_file_path = file_path.replace(f"{local_benchmark_data_path}\\", "")
    split_paths = adj_file_path.split("\\")
    bench_source = split_paths[0]  # (first level folder name)

    ahps_lid = "na"
    if bench_source == "ble":
        # becomes ble_100yr
        bench_prefix = f"ble_{split_paths[2]}"
        magnitude = split_paths[2]
    elif bench_source == "nws":
        # becomes nws_ahps_dalts_minor
        bench_prefix = f"nws_ahps_{split_paths[2]}_{split_paths[3]}"
        ahps_lid = split_paths[2]
        magnitude = split_paths[3]
    elif bench_source == "ifc":
        # becomes ifc_100yr
        bench_prefix = f"ifc_{split_paths[2]}"
        magnitude = split_paths[2]
    elif bench_source == "ras2fim":
        # becomes ras2f_100yr
        bench_prefix = f"ras2fim_{split_paths[2]}"
        magnitude = split_paths[2]
    elif bench_source == "usgs":
        # becomes nws_nchn3_minor
        bench_prefix = f"usgs_{split_paths[2]}_{split_paths[3]}"
        ahps_lid = split_paths[2]
        magnitude = split_paths[3]
    else:
        raise Exception(f"Invalid benchmark source key found ({bench_source})")

    bench_name_parts = {
        "prefix": bench_prefix,
        "source": bench_source,
        "ahps_lid": ahps_lid,
        "magnitude": magnitude,
    }

    return bench_name_parts


# -------------------------------------------------
def inundate_files(flow_files, src_geocurves_path, trg_inun_file_path, local_benchmark_data_path):
    """
    Process: Iterates the incoming local benchmark files and run's inundation on them
    Input:
        flow_files: simple list of all huc applicable benchmark csv.
        src_geocurves_path = unit's geocurve files (usually from final/geocurves)
            - e.g. C:\ras2fim_data\output_ras2fim\12090301_2277_ble_230923\final\geocurves
               (or geo_rating_curves)
        huc: 12090301
        trg_inun_file_path:
            e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923
        local_benchmark_data_path: (we use this to re-calc pathing for the output folders)
            e.g. C:\ras2fim_data\gval\benchmark_data.
    """

    print("--------------------------")
    RLOG.notice(f"Runnning inundation based on geocurves from {src_geocurves_path}")
    RLOG.lprint(f"  All output inundation files will created in subfolders at {trg_inun_file_path}")

    if not src_geocurves_path.endswith("\\"):
        src_geocurves_path += "\\"

    # don't let if fail if one errors out, unless all fail.
    flow_files.sort()

    # some units only have a few units in them so it may not automatically create any
    # inundation files.
    inundation_files = []

    for bench_file in flow_files:
        # the key is that it is sort.
        # Figure out adjusted path
        #   e.g. incoming C:\ras2fim_data\gval\benchmark_data\ble\
        #        12030105\100yr\ble_huc_12030105_flows_100yr.csv
        #   becomes: ble\12030105\100yr\ which gets added to the inundation pathing so
        #   the output pathing becomes C:\ras2fim_data\gval\evaluations\
        #      PROD\12030105_2276_ble\230923\**.gkpg

        # a dictionary
        bench_name_details = parse_bench_file_name(bench_file, local_benchmark_data_path)

        print()
        RLOG.notice(
            "----- Inundating files for benchmark source :"
            f" {bench_name_details['source']} - {bench_name_details['magnitude']} ---------"
        )

        inun_file_name = bench_name_details["prefix"] + "_inundation.gpkg"
        # At this point the inun_file name are names such as:
        #    ble_100yr_inundation.gpkg and
        #    nws_ahps_cbst2_major_inundation.gpkg

        # strip_pattern = f"_huc_{huc}_flows"
        # inun_file_name = inun_file_name.replace(strip_pattern, "")
        trg_file_path = os.path.join(trg_inun_file_path, inun_file_name)

        print(f"... Inundation Starting for : {bench_file}")
        # it will display/log errors and critical errors
        ri.produce_inundation_from_geocurves(src_geocurves_path, bench_file, trg_file_path, False)
        # print(f"... Inundation Complete : {b_file}")

        # we need to assume it did inundate if applicable
        # Not all will inundate depending if it found matching features in the flow files.
        # Some batches are two small to find matching features.
        if os.path.exists(trg_file_path):
            inundation_files.append(trg_file_path)
        else:
            print()
            RLOG.warning(
                f"An inundation file was not created for {inun_file_name}. One possiblity is"
                " that could be a result of a small number of unit output rating curves."
                " This is not necessarily a concern."
            )

    return inundation_files


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

    if len(bench_files) == 0:
        print()
        RLOG.critical(F"No benchmark files were found for HUC {huc}")
        print()
        sys.exit(1)

    # sort out to keep the .csv
    #    files_to_download = []
    #    for bench_file in bench_files:  # Iterate dictionary items
    #        if bench_file["url"].endswith(".csv") or :
    #            files_to_download.append(bench_file)

    #    if len(files_to_download) == 0:
    #        RLOG.critical(f"There are no benchmark .csv files for the huc {huc}")
    #        sys.exit(1)

    down_items = []
    # for s3_file in files_to_download (yes.. all files in those folders)
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
    print()

    return bench_files


# -------------------------------------------------
#  Some validation of input, but also creating key variables
def __validate_input(
    unit_folder_name,
    enviro,
    src_unit_final_path,
    trg_gval_root,
    trg_output_override_path,
    src_benchmark_data_path,
):
    """
    Summary: Will raise Exception if some are found

    Inputs:
        unit_folder_name: e.g. 12030101_2276_ble_230925
        enviro: e.g. PROD or DEV
        src_unit_final_path: e.g. C:\ras2fim_data\output_ras2fim\12030101_2276_ble_230925\final
        trg_gval_root: e.g. c:\ras2fim_data\gval
        trg_output_override_path: e.g.(blank) or c:\my_ras\inundation_files
        src_benchmark_data_path: e.g. C:\ras2fim_data\gval\benchmark_data

    Output: dictionary
        - rtn_dict["huc"]:
        - rtn_dict["unit_id"]: 12090301_2277_ble
        - rtn_dict["unit_version_as_str"]: 230914

        - rtn_dict["enviro"] = case corrected: PROD or DEV

        - rtn_dict["trg_unit_version_path"]:
             e.g. C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923 or
             complete override from trg_output_override_path or adjustments for gval folder path

        - rtn_dict["src_models_domain_extent_file"]
            C:\ras2fim_data\output_ras2fim\12030105_2276_ble_240304\final\models_domain\
              V1 = models_domain.gpkg    V2 = dissolved_conflated_models.gpkg

        - rtn_dict["src_geocurves_path"]:
            C:\ras2fim_data\output_ras2fim\12030105_2276_ble_240304\final\geo_rating_curves
            or c:\my_ras\inundation_files\12030105_2276_ble_240304\final\geo_rating_curves
            or v1: ...12030105_2276_ble_230915\final\geocurves

        - rtn_dict["trg_inundation_path"] = trg_inundation_path
             e.g. C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923 or
                complete override from trg_output_override_path or adjustments for gval folder path

        - rtn_dict["trg_inundation_metrics_file"]
             e.g. C:\ras2fim_data\gval\evaluations\
                PROD\12030103_2276_ble\230923\12030103_2276_ble_230923_metrics.csv

        - rtn_dict["trg_enviro_metrics_file_path"]
            enviro master metrics (could be empty if n/a)
            e.g. C:\ras2fim_data\gval\evaluations\PROD\eval_PROD_metrics.csv (or similar path)
            (if trg_output_override_path (cmd arg) was used, there is no master metrics to be updated)

        - rtn_dict["is_s3_path"] = True / False

        - rtn_dict["s3_src_benchmark_data_path"]
            could be empty or e.g s3://ras2fim/gval/benchmark_data

        - rtn_dict["local_benchmark_data_path"]
            C:\ras2fim_data\gval\benchmark_data
            It is either the override local location or an S3 path and we will need to copy those
            files here.

    """

    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned
    rtn_dict = {}

    # ----------------
    if unit_folder_name == "":
        raise ValueError("unit_folder_name (-u) can not be empty")

    # splits it a six part dictionary
    src_name_dict = sf.parse_unit_folder_name(unit_folder_name)
    if "error" in src_name_dict:
        raise Exception(src_name_dict["error"])

    rtn_dict["huc"] = src_name_dict["key_huc"]
    rtn_dict["unit_id"] = src_name_dict["key_unit_id"]  # (12090301_2277_ble)
    rtn_dict["unit_version_as_str"] = src_name_dict["key_unit_version_as_str"]  # 230914
    src_unit_folder_name = src_name_dict["key_unit_folder_name"]  # 12090301_2277_ble_230914

    if src_name_dict["key_unit_version_as_dt"] < dt.datetime(2023, 12, 31):
        # is v1
        geocurve_folder_name = "geocurves"
        model_domain_file_name = "models_domain.gpkg"
    else:
        # is v2 or higher
        geocurve_folder_name = "geo_rating_curves"
        model_domain_file_name = "dissolved_conflated_models.gpkg"

    # ----------------
    enviro = enviro.upper()
    if enviro != "PROD" and enviro != "DEV":
        raise ValueError("The enviro (-e) arg must be either 'PROD' or 'DEV'")

    rtn_dict["enviro"] = enviro

    # ----------------
    # Target paths
    is_output_path_override = False  # default
    if enviro == "PROD" or trg_output_override_path == "":
        # if PROD, you can not override much of the pathing, only the base gval
        if trg_gval_root == "":
            raise ValueError("target gval root folder (-tg) can not be empty")

        trg_inundation_path = os.path.join(
            trg_gval_root,
            sv.LOCAL_GVAL_EVALS,
            enviro,
            src_name_dict['key_unit_id'],
            src_name_dict['key_unit_version_as_str'],
        )

    else:  # DEV could still have an override path
        is_output_path_override = trg_output_override_path != ""

        if is_output_path_override:
            trg_inundation_path = trg_output_override_path
        else:  # Not overridden
            # not overridden so we need the gval_root (migth be defaulted)
            if trg_gval_root == "":
                raise ValueError("target gval root folder (-tg) can not be empty")
            trg_inundation_path = os.path.join(
                trg_gval_root,
                sv.LOCAL_GVAL_EVALS,
                enviro,
                src_name_dict['key_unit_id'],
                src_name_dict['key_unit_version_as_str'],
            )

    rtn_dict["trg_inundation_path"] = trg_inundation_path
    # e.g. C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923 or
    # complete override from trg_output_override_path or adjustments for gval folder path

    # ----------------
    # geocurve and model domains paths
    if src_unit_final_path == "use_default":
        src_unit_final_path = os.path.join(
            sv.R2F_DEFAULT_OUTPUT_MODELS, src_unit_folder_name, sv.R2F_OUTPUT_DIR_FINAL
        )

    src_unit_models_file_path = os.path.join(
        src_unit_final_path, sv.R2F_OUTPUT_DIR_DOMAIN_POLYGONS, model_domain_file_name
    )

    src_unit_geocurves_path = os.path.join(src_unit_final_path, geocurve_folder_name)

    if os.path.exists(src_unit_models_file_path) is False:
        raise ValueError(
            f"Source models domain file at {src_unit_models_file_path}"
            " does not exist. Check arguments or pathing."
        )
    rtn_dict["src_models_domain_extent_file"] = src_unit_models_file_path
    # C:\ras2fim_data\output_ras2fim\12030105_2276_ble_240304\final\models_domain\
    # V1 = models_domain.gpkg    V2 = dissolved_conflated_models.gpkg

    if os.path.exists(src_unit_geocurves_path) is False:
        raise ValueError(
            f"Source geocurves folder at {src_unit_geocurves_path}"
            " does not exist. Check arguments or pathing."
        )

    ct_curves_path = len(list(Path(src_unit_geocurves_path).rglob("*.csv")))
    if ct_curves_path == 0:
        raise ValueError(
            f"The rating curves directory of {src_unit_geocurves_path} does not have .csv files in it."
        )
    rtn_dict["src_geocurves_path"] = src_unit_geocurves_path

    # ------------
    # Setup the output metrics

    # ----------------
    # metrics file for the uv metrics file
    uv_metrics_file_name = f"{src_unit_folder_name}_metrics.csv"
    # e.g  12030105_2276_ble_230923_metrics.csv
    trg_inundation_metrics_file = os.path.join(trg_inundation_path, uv_metrics_file_name)
    # e.g. C:\ras2fim_data\gval\evaluations\
    #        PROD\12030103_2276_ble\230923\12030103_2276_ble_230923_metrics.csv
    rtn_dict["trg_inundation_metrics_file"] = trg_inundation_metrics_file

    # ----------------
    # master metrics file for the enviro. It maybe be empty if n/a
    if is_output_path_override is False:
        # I am sure there is a smarter way to do this if/else test
        trg_enviro_metrics_file_path = os.path.join(
            trg_gval_root, sv.LOCAL_GVAL_EVALS, enviro, f"eval_{enviro}_metrics.csv"
        )
    else:  # is_output_path_override is True
        trg_enviro_metrics_file_path = ""  # no master metrics

    rtn_dict["trg_enviro_metrics_file_path"] = trg_enviro_metrics_file_path  # could be empty

    # ----------------
    # benchmark source
    if src_benchmark_data_path == "":
        raise ValueError("Src benchmark data folder (-b) can not be empty")

    is_s3_path = (src_benchmark_data_path.startswith("S3://")) or (
        src_benchmark_data_path.startswith("s3://")
    )

    rtn_dict["is_s3_path"] = is_s3_path

    if is_s3_path:
        s3_src_benchmark_data_path = src_benchmark_data_path.replace("S3://", "s3://")
        # if the folder exists, we will download it later.
        if s3_sf.is_valid_s3_folder(s3_src_benchmark_data_path) is False:
            raise ValueError(f"The s3 path entered of {s3_src_benchmark_data_path} does not exist")

        rtn_dict["s3_src_benchmark_data_path"] = s3_src_benchmark_data_path
        rtn_dict["local_benchmark_data_path"] = os.path.join(trg_gval_root, sv.LOCAL_GVAL_BENCHMARK_DATA)
        # C:\ras2fim_data\gval\benchmark_data  - where it will be saved when pulled down from s3

    else:  # must pre-exist if it a non s3 url.
        if os.path.exists(src_benchmark_data_path) is False:
            raise ValueError("src benchmark data folder (-b) does not exist")
        rtn_dict["s3_src_benchmark_data_path"] = ""
        rtn_dict["local_benchmark_data_path"] = src_benchmark_data_path

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
        "--src_unit_final_path",
        help=f"OPTIONAL: Local unit folder were the 'final' folder is at.\n"
        r"It needs to have the geocurves in final\geocurves (or geo_rating_curves) folder."
        f"It also needs a file named 'models_domain.gpkg' or 'dissolved_conflated_models.gpkg'"
        " in the final\models_domain.\n"
        r" e.g C:\my_ras\12030101_2276_ble_230925\final."
        f"\nDefaults to {sv.R2F_DEFAULT_OUTPUT_MODELS}\[unit_name]\{sv.R2F_OUTPUT_DIR_FINAL}",
        default="use_default",
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-tg",
        "--trg_gval_root",
        help=r"OPTIONAL: The root folder were the gval is saved to. e.g c:\rob\gval_testing."
        " If you use the -to (target output overrid path, any value you put here will be ignored)"
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
        log_file_name = f"{script_file_name}_{sf.get_date_with_milli(False)}.log"
        RLOG.setup(os.path.join(log_file_folder, log_file_name))

        run_unit_benchmark_tests(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
        sys.exit(1)
