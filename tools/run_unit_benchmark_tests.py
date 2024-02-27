#!/usr/bin/env python3

import argparse
import datetime as dt
import glob
import os
import shutil
import sys
import time
import traceback
from pathlib import Path

import geopandas as gpd
import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import ras2inundation as ri
import shared_functions as sf
import shared_variables as sv
import s3_shared_functions as s3_sf

from evaluate_ras2fim_unit import evaluate_unit_results

# Global Variables
RLOG = sv.R2F_LOG

"""
TODO:  explain how this works


This tool can already work in auto mode. No runtime questions are asked.

TODO: Later, do we optionally load this back up to S3?
   For now.. manually upload C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923 (to the unit level)
   Also upload the unit level master metrics to S3. e.g. 
      C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\12030105_2276_ble_unit_metrics.csv  (notice not 
      in the version subfolder.)

"""

# ***********************
# NOTICE:
#    Feb 2024: Due to time constraints, most testings of combinations of input arguments have not yet
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
    unit_metrics,
):
    """
    TODO Processing notes (lots of permuations)

    Inputs:
        unit_folder_name: e.g. 12030101_2276_ble_230925
        enviro: e.g. PROD or DEV
        src_unit_final_path: e.g. C:\ras2fim_data\output_ras2fim\12030101_2276_ble_230925\final
        trg_gval_root: e.g. c:\ras2fim_data\gval
        trg_output_override_path: e.g.(blank) or c:\my_ras\inundation_files
        src_benchmark_data_path: e.g. C:\ras2fim_data\gval\benchmark_data
        unit_metrics: e.g. none or various S3 or local paths
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
        RLOG.lprint(f"  (-to): Local target output override path: {trg_output_override_path}")
    if unit_metrics != "" and unit_metrics != "not_set":
        RLOG.lprint(f"  (-m): Path to unit metrics file: {trg_output_override_path}")        
    RLOG.lprint(f" Started (UTC): {sf.get_stnd_date()}")
    print()
    print("NOTE: All output inundation and benchmark results files will be overwritten")
    print()


    RLOG.notice("********************************************************************")
    RLOG.notice("***  IMPORTANT NOTE about roll up metrics files\n")
    print(" Each unit (ie. 12030105_2276_ble) can have multiple versions over time.")
    print("     ie) 230923 or 240217")    
    print(" This tool processes one unit and version at this time.")
    print("     ie) 12030105_2276_ble_230923 or 12030105_2276_ble_240217")
    print(" Saved at: ")
    print(r"    ie). C:\ras2fim_data\output_ras2fim\12030105_2276_ble_230923.")
    print()
    print(" When it is completing the evaluations, the eval outputs will be saved"
          " in a unit version subfolder.")
    print(r"     ie) C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923")
    print()
    print(" This tool create a couple of metrics (.csv) files created.")
    print("   - One file is at the unit level which covers all metrics for all evaluations"
          "  (benchmark and magnitude) and unit versions (ie. 230923, 240228)")
    print(r"       ie) C:\ras2fim_data\gval\evaluations\PROD"
          r"\12030105_2276_ble\12030105_2276_ble_unit_metrics.csv")
    print()
    print("   - The second file is at the unit version level which covers all metrics created"
                " during this run.")
    print(r"       ie) C:\ras2fim_data\gval\evaluations\PROD"
                r"\12030105_2276_ble\240228\12030105_2276_ble_240228_metrics.csv")
    print()
    if unit_metrics == "" or unit_metrics == "not_set":
        print(" Remember can optionally use the (-m) with to point to an existing"
              " unit level metrics file that can be downloaded or copied and be appended."
              " It can be an s3 url file path or a local file sytem file path.")
    print()
    RLOG.notice("********************************************************************")

    # TODO: test permuations of the input args

    RLOG.notice("NOTE: As of Feb 23, 2023, some of the testing for non-defaulted args has"
                " not yet been completed")
    print()
    print()

    # give the users 10 seconds to read this before continuing
    time.sleep(10)

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
        lst_bench_files = get_s3_benchmark_data(
            huc, rd["src_benchmark_data_path"], rd["local_benchmark_data_path"]
        )

    else:  # get them locally (list of the huc applicable benchmark csv's)
        RLOG.lprint(f"Looking for local benchmark files for huc {huc}")

        # let's build up the glob search pattern to look for the HUC number starting
        # with the src_benchmark_data_path.
        bench_data_root = rd["local_benchmark_data_path"]
        if bench_data_root.endswith("\\") is False:
            bench_data_root += "\\"

        lst_bench_files = glob.glob(f"{bench_data_root}**\\*{huc}*.csv", recursive=True)
        if len(lst_bench_files) == 0:
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
    for b_file in lst_bench_files:
        if Path(b_file).suffix != ".csv":
            continue
        parent_path = Path(b_file)
        parent_dir_name = parent_path.parent.name
        if parent_dir_name in sv.GVAL_VALID_STAGES:
            bench_flow_files.append(b_file)

    inundate_files(
        bench_flow_files,
        huc,
        rd["src_geocurves_path"],
        rd["trg_inun_file_path"],
        rd["local_benchmark_data_path"],
    )

    # ----------------
    # lst_bench_files is all files (not just .csv's)
    # Yes.. this is not optimum to find csv, then calc path
    # then do extent files, and re-calc those paths.
    bench_extent_files = []
    for b_file in lst_bench_files:
        if Path(b_file).suffix != ".tif":
            continue
        b_file_name = os.path.basename(b_file)
        if "extent" not in b_file_name:
            continue
        bench_extent_files.append(b_file)

    # ----------
    __run_tests(
        bench_extent_files,
        unit_folder_name,
        rd["trg_unit_folder"],
        rd["src_models_file"],
        rd["trg_inun_file_path"],
        rd["local_benchmark_data_path"],
        rd["trg_unit_metrics_path"]
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
    bench_extent_files,
    unit_folder_name,
    trg_unit_folder,
    src_models_file,
    trg_inun_file_path,
    local_benchmark_data_path,
    trg_unit_metrics_file_path
):
    """
    Process: Iterates the incoming local benchmark and run them against evaluate_ras2fim_unit.py
    Input:
        bench_extent_files: simple list of all huc applicable benchmark extent files
        src_models_file = unit's file to the unit domain file.
            - e.g. C:\ras2fim_data\output_ras2fim\
                12090301_2277_ble_230923\final\models_domain\dissolved_conflated_models.gpkg
        unit_folder_name: 12030105_2276_ble_230923
        trg_unit_folder: e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble
        trg_inun_file_path:
            e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923
        local_benchmark_data_path: (we use this to re-calc pathing for the output folders)
            e.g. C:\ras2fim_data\gval\benchmark_data.
        trg_unit_metrics_file_path: the name and path of the unit level metrics file to be used:
            e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\12030105_2276_ble_unit_metrics.csv
            (root path "C:\ras2fim_data\gval" and final file 
            name "12030105_2276_ble_unit_metrics.csv" might be different)
    """

    print("--------------------------")
    RLOG.notice(f"Runnning unit benchmarks based on unit model domain from {src_models_file}")
    RLOG.lprint(f"  All output evaluations files will created in {trg_inun_file_path}")
    print()

    # don't let if fail if one errors out, unless all fail.
    bench_extent_files.sort()

    # Figure out the path for each benchmark file and output folder.
    # The inundation files are at gval/evalutions/PROD (or DEV)..
    # ie) C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923\ahps_cart2_major_inundation.gpkg
    # or  C:\ras2fim_data\gval\evaluations\PROD\12030103_2276_ble\230923\ble_100yr_inundation.gpkg

    # The benchmark extent files are at gval/benchmark_data..
    # ie) C:\ras2fim_data\gval\benchmark_data\ble\12030105\100py\ble_huc_12030105_extent_100yr.tif
    # or  C:\ras2fim_data\gval\benchmark_data\nws\12030105
    #       \cart2\minor\ahps_cart2_huc_12030103_extent_major.tif

    # get root of benchmark as it can be overwridden from cmd line.

    # get the version from the dissolved_conflated_models.gpkg or models_domain.gpkg (from V1)
    # NOTE: I went back and manually added that column and values to the v1 models_domain.gpkg
    models_gdf = gpd.read_file(src_models_file)
    code_version = models_gdf.iloc[0]["version"]

    metric_files = []

    for bench_extent_raster in bench_extent_files:
        bench_name_details = parse_bench_file_name(bench_extent_raster, local_benchmark_data_path)

        try:
            bench_prefix = bench_name_details["prefix"]
            # becomes ie) 12030105_2276_ble_230923--ble_100yr  or 12030105_2276_ble_230923--nws_ahps_minor
            # used only for loggin
            unit_eval_name = unit_folder_name + "--" + bench_prefix
            RLOG.trace(f"Running Benchmark tests for {unit_eval_name}")

            # files names are added inside evaluate_unit_results
            # becomes ie: C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923\nws_ahps_dalt2_major
            eval_output_folder = os.path.join(trg_inun_file_path, bench_prefix)

            # C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923\ble_100yr_inundation.gpkg
            # and should already exist on the local drives.
            inun_poly_name = f"{bench_prefix}_inundation.gpkg"
            inundation_poly_path = os.path.join(trg_inun_file_path, inun_poly_name)

            # src_models_file is already and should already exist
            # ie) C:\ras2fim_data\output_ras2fim\
            #     12090301_2277_ble_230923\final\models_domain\dissolved_conflated_models.gpkg

        except Exception as ex:
            err_msg = f"An error occured while setting up info unit benchmark tests for {unit_folder_name}"
            RLOG.critical(err_msg)
            raise ex

        # Feb 21, 2024: For reasons unknown, when using VSCode debug,
        # it throws exceptions for evaluate_unit_results.
        # Fix: run it via command line, come back, temp disable this part and continue.
        try:
            evaluate_unit_results(
                inundation_poly_path, src_models_file, bench_extent_raster, unit_eval_name, eval_output_folder
            )

        except Exception as ex:
            # re-raise but check if it is includes phrase 'Rasters don't spatially intersect'
            # so we can give a better error message.
            # Add context data and RLOG.
            err_msg = f"An error occured while running gval results for {unit_folder_name};"
            f" inundation_poly_path is {inundation_poly_path},"
            f" src_models_file is {src_models_file},"
            f" bench_extent_raster is {bench_extent_raster},"
            f" unit_folder_name is {unit_folder_name}"
            RLOG.critical(err_msg)
            raise ex

        # before coping around and merging HUC / GVAL results, pull the "version" column
        # from models_domain (dissoved)
        # and put it in the gval / results. Not availalbe in V1, but manually add to a V1 model_domain
        # metrics output needs the following columns added:
        # code_version  (v.2.0.0)
        # unit name   (112030105_2276_ble)
        # unit version  (230923)
        # benchmark source  (nws)
        # benchmark magnitude  (major)
        # huc  (112030105)
        # ahps_lid  ( n/a  or  dalt2)

        # ud means unit_dictionary (parts)
        ud = sf.parse_unit_folder_name(unit_folder_name)
        if "error" in ud:
            raise Exception(ud["error"])

        metrics_file_path = os.path.join(eval_output_folder, "metrics.csv")

        metrics_df = pd.read_csv(metrics_file_path)
        if "unit_name" not in metrics_df.columns:
            metrics_df.insert(0, "unit_name", ud["key_unit_id"])

        if "unit_version" not in metrics_df.columns:
            metrics_df.insert(1, "unit_version", ud["key_unit_version_as_str"])
            metrics_df["unit_version"] = metrics_df["unit_version"].astype("string")

        if "code_version" not in metrics_df.columns:
            metrics_df.insert(2, "code_version", code_version)

        if "huc" not in metrics_df.columns:
            metrics_df.insert(3, "huc", ud["key_huc"])

        if "benchmark_source" not in metrics_df.columns:
            metrics_df.insert(4, "benchmark_source", bench_name_details["source"])

        if "magnitude" not in metrics_df.columns:
            metrics_df.insert(5, "magnitude", bench_name_details["magnitude"])

        if "ahps_lid" not in metrics_df.columns:
            metrics_df.insert(6, "ahps_lid", bench_name_details["ahps_lid"])

        metrics_df.to_csv(metrics_file_path, index=False)

        metric_files.append(metrics_file_path)

        # merge this wil the unit master csv
    __merge_metrics_files(metric_files,
                          ud["key_unit_id"],
                          ud["key_unit_version_as_str"],
                          trg_unit_folder,
                          trg_unit_metrics_file_path)


# -------------------------------------------------
def __merge_metrics_files(metric_files, unit_name, unit_version, trg_unit_folder, unit_metrics_path):
    """
    All of the individual benchmark tests folder have their own metrics,
    but we will roll them up to a unit level "master" metrics.
    If it finds records that already exist with that version (ie. 230914),
    it will delete them first so we don't have dup sets of one version records

    There two metrics file that will be created. One at the unit level
        ie) C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\12030105_2276_ble_unit_metrics.csv
    And a second one at the unit_version level (for just this run)
        ie) C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\
            230923\12030105_2276_ble_230923_metrics.csv

    Inputs:
        - metric_files: a list of the full pathed just created metrics (one per sourc / magnitude)
        - unit_name: e.g. 12030105_2276_ble
        - unit_version: e.g. 230923
        - trg_unit_folder: e.g. C:\my_ras2fim\gval_test\evaluations\PROD\12030105_2276_ble
        - unit_metrics_path: A full path to the unit level metrics.
             e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\12030105_2276_ble_unit_metrics.csv

        (Note: Users can override the name of the version level metrics file, just the root local gval path)
    """

    unit_version_metrics_file_path = f"{unit_name}_{unit_version}_metrics.csv"
    unit_version_metrics_path = os.path.join(trg_unit_folder, unit_version, unit_version_metrics_file_path)    

    unit_metrics_df = pd.DataFrame()
    unit_version_metrics_df = pd.DataFrame()
    for idx, metrics_file in enumerate(metric_files):
        if idx == 0:
            if os.path.exists(unit_metrics_path) is True:
                RLOG.trace(
                    f"Merging new metrics file of {metrics_file} to unit master at {unit_metrics_path}"
                )
                unit_metrics_df = pd.read_csv(unit_metrics_path)
                # if the master unit metrics already exists, check to see if it already has records
                # for the incoming version and remove them, as we will replace them.

                #unit_metrics_df = unit_metrics_df.drop(
                #    unit_metrics_df[unit_metrics_df['unit_version'].astype("string") == unit_version].index
                #)

                """
                unit_metrics_df = unit_metrics_df.drop(
                    unit_metrics_df[ 
                        (unit_metrics_df['unit_version'].astype("string") == unit_version) and
                        (unit_metrics_df['unit_name'] == unit_name)
                        ].index
                )
                """
                indexes_lst = unit_metrics_df[ 
                        (unit_metrics_df['unit_version'].astype("string") == unit_version) &
                        (unit_metrics_df['unit_name'] == unit_name)
                        ].index
                unit_metrics_df.drop(indexes_lst , inplace=True)
                #print("dup indexes")
                #print(indexes_lst)



                metrics_df = pd.read_csv(metrics_file)
                # I heard it not good to write directly back to a df progress of concat
                con_df = pd.concat([unit_metrics_df, metrics_df], ignore_index=True)
                unit_metrics_df = con_df
            else:
                # load the first metrics
                RLOG.trace("Unit master metrics file does not exist.")
                RLOG.trace(f"Loading the first one, metrics file of {metrics_file}")
                unit_metrics_df = pd.read_csv(metrics_file)

                # and for the version level too (which we will overwrite if it exists)
                unit_version_metrics_df = pd.read_csv(metrics_file)
        else:
            RLOG.trace(f"Concatenating metrics file of {metrics_file}")
            metrics_df = pd.read_csv(metrics_file)

            # I heard it not good to write directly back to a df progress of concat
            con_df = pd.concat([unit_metrics_df, metrics_df], ignore_index=True)
            unit_metrics_df = con_df

            # concat to the version level metrics as well.
            con_df = pd.concat([unit_version_metrics_df, metrics_df], ignore_index=True)
            unit_version_metrics_df = con_df            

    if len(unit_metrics_df) == 0:
        raise Exception("The unit master metrics file is empty. Please review code")

    print()
    unit_metrics_df.to_csv(unit_metrics_path, index=False)
    RLOG.notice(
        "Created or updated the rolled up unit level metrics file"
        f" at {unit_metrics_path}. All new metrics files have been added to this file."
    )

    print()
    unit_version_metrics_df.to_csv(unit_version_metrics_path, index=False)
    RLOG.notice(
        "The rolled up metrics file for this specific unit and unit version have"
        f" been created as well. It has been saved to {unit_version_metrics_path}."
    )    


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
def inundate_files(flow_files, huc, src_geocurves_path, trg_inun_file_path, local_benchmark_data_path):
    """
    Process: Iterates the incoming local benchmark files and run's inundation on them
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
    RLOG.lprint(f"  All output inundation files will created in subfolders at {trg_inun_file_path}")

    # don't let if fail if one errors out, unless all fail.
    flow_files.sort()

    for b_file in flow_files:
        # the key is that it is sort.
        # Figure out adjusted path
        #   e.g. incoming C:\ras2fim_data\gval\benchmark_data\ble\
        #        12030105\100yr\ble_huc_12030105_flows_100yr.csv
        #   becomes: ble\12030105\100yr\ which gets added to the inundation pathing so
        #   the output pathing becomes C:\ras2fim_data\gval\evaluations\
        #      PROD\12030105_2276_ble\230923\**.gkpg

        # a dictionary
        bench_name_details = parse_bench_file_name(b_file, local_benchmark_data_path)

        print()
        RLOG.notice(
            "----- Inundating files for benchmark source of"
            f" {bench_name_details['source']} - {bench_name_details['magnitude']} ---------"
        )

        inun_file_name = bench_name_details["prefix"] + "_inundation.gpkg"
        # At this point the inun_file name are names such as:
        #    ble_100yr_inundation.gpkg and
        #    nws_ahps_cbst2_major_inundation.gpkg

        # strip_pattern = f"_huc_{huc}_flows"
        # inun_file_name = inun_file_name.replace(strip_pattern, "")
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
    unit_metrics,
):
    """
    Summary: Will raise Exception if some are found

    TODO: fill in

    Output: dictionary
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
    rtn_dict["version_date_as_str"] = src_name_dict["key_unit_version_as_str"]  # (date string eg: 230811),

    # ----------------
    enviro = enviro.upper()
    if enviro != "PROD" and enviro != "DEV":
        raise ValueError("The enviro (-e) arg must be either 'PROD' or 'DEV'")
    rtn_dict["enviro"] = enviro

    # ----------------
    # src_geocurve_files_path
    if src_unit_final_path == "":
        raise ValueError("The unit 'final' directory (-sc) arg can not be empty")

    if src_unit_final_path == "use_default":
        src_geocurves_path = os.path.join(
            sv.R2F_DEFAULT_OUTPUT_MODELS, unit_folder_name, sv.R2F_OUTPUT_DIR_FINAL_GEOCURVES
        )
    else:
        src_geocurves_path = os.path.join(src_unit_final_path, sv.R2F_OUTPUT_DIR_FINAL_GEOCURVES)

    if os.path.exists(src_geocurves_path) is False:
        raise ValueError(
            f"The unit 'final\geocurve' directory (-sc) of {src_geocurves_path}"
            " (or the defaulted value) does not exist."
        )

    ct_curves_path = len(list(Path(src_geocurves_path).rglob("*.csv")))
    if ct_curves_path == 0:
        raise ValueError(
            f"The rating curves directory (-sg) of {src_geocurves_path} does not have .csv files in it."
        )

    rtn_dict["src_geocurves_path"] = src_geocurves_path

    # ----------------
    # get the models domain file from the unit
    if src_unit_final_path == "use_default":
        src_unit_final_path = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS, unit_folder_name)

    # This covers v1 and v2. V1 only had a models_domain.gpkg file
    src_models_folder = os.path.join(
        src_unit_final_path, sv.R2F_OUTPUT_DIR_FINAL, sv.R2F_OUTPUT_DIR_DOMAIN_POLYGONS
    )
    models_file_path = os.path.join(src_models_folder, "dissolved_conflated_models.gpkg")

    if os.path.exists(models_file_path) is False:
        models_file_path = os.path.join(src_models_folder, "models_domain.gpkg")
        if os.path.exists(models_file_path) is False:
            raise ValueError(
                "The file `dissolved_conflated_models.gpkg` or `models_domain`"
                f" can be not be found at {src_models_folder}"
            )

    rtn_dict["src_models_file"] = models_file_path

    # ----------------
    if trg_gval_root == "":
        raise ValueError("target gval root folder (-tg) can not be empty")
    os.makedirs(trg_gval_root, exist_ok=True)

    # ----------------
    # trg_output_override_path
    if enviro == "PROD":  # only base gval root is ok here.
        # trg_unit_folder: e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble
        trg_unit_folder = os.path.join(trg_gval_root, sv.LOCAL_GVAL_EVALS, "PROD", rtn_dict["unit_id"])

        # trg_inun_file_path: e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923
        trg_inun_file_path = os.path.join(trg_unit_folder, rtn_dict["version_date_as_str"])
    else:  # DEV or override are fine
        if trg_output_override_path == "":
            # I am sure there is a better way to do this.. but this is easy to read and follow
            # trg_unit_folder: e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble
            trg_unit_folder = os.path.join(trg_gval_root, sv.LOCAL_GVAL_EVALS, "DEV", rtn_dict["unit_id"])

            # trg_inun_file_path: e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\230923
            trg_inun_file_path = os.path.join(trg_unit_folder, rtn_dict["version_date_as_str"])
        else:
            trg_unit_folder = trg_output_override_path
            trg_inun_file_path = trg_output_override_path

    rtn_dict["trg_unit_folder"] = trg_unit_folder
    rtn_dict["trg_inun_file_path"] = trg_inun_file_path

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

    # ----------------
    # calc path for where the master unit metrics will exist. This may be creating
    # a default one, downloading it from S3, or copy an existing one from somewhere on the file
    rtn_dict["trg_unit_metrics_path"] = __calc_path_unit_metrics_file(unit_metrics,
                                                                      rtn_dict["unit_id"],
                                                                      trg_unit_folder)

    return rtn_dict


# -------------------------------------------------
def __calc_path_unit_metrics_file(unit_metrics, unit_id, trg_unit_folder):
    
    """
    This function will return the a target path and name for the unit level metrics csv file.
    The user can optionally tell us to download or copy a previous version, so new records can be
    appended it (assuming a previous one already existed).
    The previous metrics file path can be from S3 in which we will attempt to download it.
    OR a local drive and we can copy it to the required processing folder. 
    If no previous unit level metrics file exists, it will start a new one.

    In both cases of the unit_metrics arg being passed in, it will keep the provided file name
    but save it to the required local folder pathing (with the root folder being changeable
    via the -tg arg).
    
    e.g. C:\my_ras\gval\evaluations\PROD\12030105_2276_ble\test_rollup_unit_metrics.csv

    This function covers the unit level metrics file only and not the subfolder unit version
    file name.

    Inputs:
        unit_metrics: e.g. "not_set" or s3://ras2fim-dev/test_unit_rollup_metrics.csv (or local path)
        unit_id: e.g. 12030105_2276_ble
        trg_unit_folder: e.g. C:\my_ras\gval\evaluations\PROD\12030105_2276_ble
    """
    # TODO: It woudl be good to have it auto search S3 for a unit metrics file. But..
    # we don't have any input arg for either the root s3 bucket or the bucket and folder to 
    # evaluations.

    default_unit_metrics_file_name = f"{unit_id}_unit_metrics.csv" 

    if unit_metrics == "not_set" or unit_metrics == "":
        # set it to the default local of where it will located.
        trg_unit_metrics_path = os.path.join(trg_unit_folder, default_unit_metrics_file_name)
    else:
        if (unit_metrics.startswith("s3:") or unit_metrics.startswith("S3:")):
            unit_metrics = unit_metrics.replace("S3:", "s3:") # may or not already be in the case
            unit_metrics = unit_metrics.replace("\\", "/") # fix to url forward slashes
            # let's see if the file exists.
            if s3_sf.is_valid_s3_file(unit_metrics) is False:
                # set to default:
                raise ValueError(f"{unit_metrics} does not exist.")
            else:
                # download it and copy to the local default path. (keeping the file name)
                bucket_name, s3_file_path = s3_sf.parse_bucket_and_folder_name(unit_metrics)

                # keep the original file name S3.
                file_name = unit_metrics.rsplit("/", 1)
                trg_unit_metrics_path = os.path.join(trg_unit_folder, file_name)
                s3_sf.download_one_file(bucket_name, s3_file_path, trg_unit_metrics_path)

        else:  
            # unit_metrics value has been supplied (local path)

            # ensure it is a csv file name.
            file_dir = os.path.dirname(unit_metrics)
            file_name = os.path.basename(unit_metrics)
            file_ext = os.path.splitext(unit_metrics)
            file_ext = file_ext[1].lower()
            if ".csv" not in file_name:
                raise ValueError(f"The provided metrics file path (-m) value of {unit_metrics}"
                                    " is not a .csv file.")

            # They gave us a full file local path.  (ie.. not S3)
            if os.path.exists(unit_metrics) is False:

                # They might be telling us what they want the file name to be
                trg_unit_metrics_path = os.path.join(trg_unit_folder, default_unit_metrics_file_name)
                # they could have given us the default value
                if unit_metrics.lower() != trg_unit_metrics_path.lower():
                    RLOG.warning(f"Can not find {unit_metrics}. A metrics file will be created"
                                f" using the default {trg_unit_metrics_path}.")
                    time.sleep(10) # give the user time to react (abort, ignore, whatever)
                # else: already defaulted just above the if test
                
            else:  # file exists, so just copy it over, keep it's name but use our path
                # need to create the dir if not already there.
                if not os.path.isdir(file_dir):
                    os.mkdirs(file_dir)

                # keep the file name but copy to a the default location
                trg_unit_metrics_path = os.path.join(trg_unit_folder, file_name)                        
                shutil.copy2(unit_metrics, trg_unit_metrics_path)
                print()                    
                RLOG.lprint(f"{unit_metrics} was copied to {trg_unit_metrics_path}")
                print()

    return trg_unit_metrics_path


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
        r"It needs to have the geocurves in final\geocurves"
        f" a file named 'models_domain.gpkg' in the final\models_domain.\n"
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

    parser.add_argument(
        "-m",
        "--unit_metrics",
        help="OPTIONAL: This script will create a couple benchmark eval metrics rollup files.\n"
        "One of the metric csv files is at the unit level (not including unit version)."
        " ie) 12030101_2276_ble\n"
        "The unit metrics file may or may not previously exist. If you want this current"
        " set of tests to append it's results to the master unit rollup, please add the path it here."
        " Local pre-existing files will copied to the standard pathing to be updated."
        "*** NOTE:This can be a local path OR an S3 path, default will check S3 to see"
        " if it exists there and download it.\n"
        r" e.g. C:\ras2fim_data\gval\evaluations\PROD\12030105_2276_ble\12030105_2276_ble_unit_metrics.csv"
        r" OR C:\ras2fim_data\test_unit_metrics.csv"
        " OR s3://ras2fim/gval/evaluations/PROD (or"
        " DEV)/12030105_2276_ble/12030105_2276_ble_unit_metrics.csv.",
        default="not_set",
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

        run_unit_benchmark_tests(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
        sys.exit(1)
