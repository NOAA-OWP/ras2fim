#!/usr/bin/env python3

import argparse
import errno
import datetime as dt
import os
import sys
import traceback
from pathlib import Path
from timeit import default_timer as timer

import geopandas as gpd
import pandas as pd
from shapely import wkt


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv
from shared_functions import get_stnd_date, get_date_time_duration_msg, get_date_with_milli


# Global Variables
RLOG = sv.R2F_LOG

# -------------------------------------------------
def produce_inundation_from_geocurves(geocurves_dir, flow_file, output_inundation_poly):
    """
    Produce inundation from RAS2FIM geocurves.

    Args:
        - geocurves_dir: Path to directory containing RAS2FIM unit output
            e.g. C:\ras2fim_data\output_ras2fim\12030106_2276_ble_230926\final\geocurves
        - flow_file: Discharges in CMS as a CSV file. "feature_id" and "discharge" columns
            e.g. C:\ras2fim_data\inputs\X-National_Datasets\nwm21_17C_recurr_100_0_cms.csv
        - output_inundation_poly_dir: C:\ras2fim_data\gval\evaluations\12030106_2276_ble\230926\inundation_polys
    """

    start_dt = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.notice("          RUN Inundation tool")
    RLOG.lprint(f"  (-g): geocurves directory {geocurves_dir} ")
    RLOG.lprint(f"  (-f): flow file {flow_file}")
    RLOG.lprint(f"  (-t): output inundation gkpg {output_inundation_poly}")
    RLOG.lprint(f" --- Start: {dt_string} (UTC time) ")
    RLOG.lprint("=================================================================")
    print()

    # -------------------------
    # Validation
    # Check that geocurves_dir exists
    # Yes.. recheck it here in case this script is not called from cmd line
    if not os.path.exists(geocurves_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), geocurves_dir)

    # check that output file name has extension of gpkg
    if not Path(output_inundation_poly).suffix == '.gpkg':
        raise TypeError("The output file must have gpkg extension.")

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    output_inundation_folder = os.path.split(output_inundation_poly)[0]
    if not os.path.exists(output_inundation_folder):
        os.makedirs(os.path.split(output_inundation_poly)[0])

    # Create dictionary of available feature_id geocurve full paths.
    geocurves_list = list(Path(geocurves_dir).glob("*_rating_curve_geo.csv"))

    if len(geocurves_list) == 0:
        msg = "Error: Make sure you have specified a correct directory with at least one geocurve csv file."
        RLOG.critical(msg)
        raise Exception(msg)

    # -------------------------
    geocurve_path_dictionary, available_feature_id_list = {}, []
    for geocurve_path in geocurves_list:
        feature_id = geocurve_path.name.split("_")[0]
        available_feature_id_list.append(feature_id)
        geocurve_path_dictionary.update({feature_id: {"path": geocurve_path}})

    RLOG.lprint("Completed creating a dictionary of available feature_ids and geocurve files.")

    # -------------------------
    # get ras2fim version from just one of the geocurve files
    ras2fim_version = None
    for _, geocurve_file_path in geocurve_path_dictionary.items():
        sample_geocurve_data = pd.read_csv(geocurve_file_path["path"])
        ras2fim_version = sample_geocurve_data.loc[0, "version"]
        break

    if ras2fim_version:
        RLOG.lprint(f"Derived ras2fim version {ras2fim_version} from geocurve files.")
    else:
        RLOG.warning("Failed to derive ras2fim version from geocurve files.")

    # -------------------------
    # Open flow_file to detemine feature_ids to process
    flow_file_df = pd.read_csv(flow_file)
    flow_file_df['feature_id'] = flow_file_df['feature_id'].astype(str)
    flow_file_df.set_index('feature_id', inplace=True)

    # compile each feature id info (discharge, stage, and geometry) into a dictionary
    RLOG.lprint("Compiling feature_ids info (discharge, stage, geometry) ... ")
    feature_id_polygon_path_dict = {}

    # -------------------------
    for feature_id in available_feature_id_list:
        # Get discharge and path to geometry file
        try:
            discharge_cms = flow_file_df.loc[feature_id, "discharge"]
            geocurve_file_path = geocurve_path_dictionary[str(feature_id)]["path"]
        except KeyError:
            RLOG.warning(
                "An exception was found finding discharge or geocurve_file_path for feature ID"
                f" of {feature_id} [path]"
            )
            continue

        # Use interpolation to find the row in geocurve_df that corresponds to the discharge_value
        geocurve_df = pd.read_csv(geocurve_file_path)
        row_idx = geocurve_df["discharge_cms"].sub(discharge_cms).abs().idxmin()
        subset_geocurve = geocurve_df.iloc[row_idx]
        polygon_geometry = wkt.loads(subset_geocurve["geometry"])
        stage_m = subset_geocurve["stage_m"]

        feature_id_polygon_path_dict.update(
            {feature_id: {"discharge_cms": discharge_cms, "geometry": polygon_geometry, "stage_m": stage_m}}
        )

    # -------------------------
    RLOG.lprint("Creating output gpkg file: " + output_inundation_poly)
    df = pd.DataFrame.from_dict(feature_id_polygon_path_dict, orient='index').reset_index()
    df.rename(columns={'index': 'feature_id'}, inplace=True)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=sv.DEFAULT_RASTER_OUTPUT_CRS)

    # add version number before saving
    gdf['version'] = ras2fim_version
    gdf.to_file(output_inundation_poly, driver="GPKG")

    print()
    RLOG.lprint("--------------------------------------")
    RLOG.success(f"Process completed: {get_stnd_date()}")
    print(f"log files saved to {RLOG.LOG_FILE_PATH}")
    print()
    dur_msg = get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


# -------------------------------------------------
if __name__ == "__main__":
    # Sample Usage

    # Samples can be found in S3 and OWP... data/inundation_review/inundation_nwm_recurr/

    # min args
    #  python ras2inundation.py
    #    -s C:\ras2fim_data\output_ras2fim\12090301_2277_ble_230825
    #    -f C:\ras2fim_data\inputs\X-National_Datasets\nwm21_17C_recurr_100_0_cms.csv
    #    -tf nwm_100_inundation.gpkg

    # Note: for the geocurves files that are required, the system knows that they are in the subfolders
    # of final/geocurves of the unit folder (-s)

    # Parse arguments
    parser = argparse.ArgumentParser(description="Produce Inundation from RAS2FIM geocurves.")

    parser.add_argument(
        "-g", 
        "--geocurves_dir", 
        help="REQUIRED: Path to directory containing RAS2FIM geocurve CSVs\n."
        f"e.g. C:\ras2fim_data\output_ras2fim\12090301_2277_ble_230923\final\geocurves", 
        required=True
    )

    parser.add_argument(
        "-f",
        "--flow_file",
        help="REQUIRED: Discharges in CMS as CSV file. 'feature_id' and 'discharge' columns MUST be supplied.\n"
        "e.g. C:\ras2fim_data\inputs\X-National_Datasets\nwm21_17C_recurr_100_0_cms.csv",
        required=True,
    )

    parser.add_argument(
        "-t",
        "--output_inundation_poly",
        help="REQUIRED: Path and file name to output inundation polygon file (must be a gkpg file).\n"
        r"e.g. C:\ras2fim_data\gval\evaluations\12030105_2276_ble"
        r"\230923\inundation_files\nwm21_17C_recurr_100_0_cms.gpkg",
        required=True,
    )

    args = vars(parser.parse_args())

    # Check that geocurves_dir exists
    output_file_path = args["output_inundation_poly"]
    output_folder_path = os.path.dirname(output_file_path)

    # for logs, come back one dir (parent), then into a logs dir
    log_file_folder = os.path.join(output_folder_path, "logs")
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        log_file_name = f"{script_file_name}_{get_date_with_milli(False)}.log"        
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        produce_inundation_from_geocurves(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
