#!/usr/bin/env python3

import argparse
import errno
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


# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def produce_inundation_from_geocurves(geocurves_dir, flow_file, output_inundation_poly):
    """
    Produce inundation from RAS2FIM geocurves.

    Args:
        geocurves_dir (str): Path to directory containing RAS2FIM geocurve CSVs.
        flow_file (str): Discharges in CMS as a CSV file. "feature_id" and "discharge" columns
         MUST be supplied. output_inundation_poly (str): Path to output inundation polygon.
    """

    # Check that output directory exists. Notify user that output directory will be created if not.
    if not os.path.exists(os.path.split(output_inundation_poly)[0]):
        RLOG.lprint(
            "Parent directory for "
            + os.path.split(output_inundation_poly)[1]
            + " does not exist. Directory/ies will be created."
        )
        os.makedirs(os.path.split(output_inundation_poly)[0])

    # check that output file name has extension of gpkg
    if not Path(output_inundation_poly).suffix == '.gpkg':
        raise TypeError ("The output file must have gpkg extension.")

    # Check that geocurves_dir exists
    if not os.path.exists(geocurves_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), geocurves_dir)

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Create dictionary of available feature_id geocurve full paths.
    geocurves_list = list(Path(geocurves_dir).glob("*_rating_curve_geo.csv"))

    if len(geocurves_list) == 0:
        msg = "Error: Make sure you have specified a correct directory with at least one geocurve csv file."
        RLOG.critical(msg)
        raise Exception(msg)

    geocurve_path_dictionary, available_feature_id_list = {}, []
    for geocurve_path in geocurves_list:
        feature_id = geocurve_path.name.split("_")[0]
        available_feature_id_list.append(feature_id)
        geocurve_path_dictionary.update({feature_id: {"path": geocurve_path}})

    RLOG.lprint("Completed creating a dictionary of available feature_ids and geocurve files.")

    # get ras2fom version from just one of the geocurve files
    ras2fim_version = None
    for _, geocurve_file_path in geocurve_path_dictionary.items():
        sample_geocurve_data = pd.read_csv(geocurve_file_path["path"])
        ras2fim_version = sample_geocurve_data.loc[0, "version"]
        break

    if ras2fim_version:
        RLOG.lprint(f"Derived ras2fim version {ras2fim_version} from geocurve files.")
    else:
        RLOG.warning(f"Failed to derive ras2fim version from geocurve files.")

    # Open flow_file to detemine feature_ids to process
    flow_file_df = pd.read_csv(flow_file)
    flow_file_df['feature_id'] = flow_file_df['feature_id'].astype(str)
    flow_file_df.set_index('feature_id', inplace=True)

    # compile each feature id info (discharge, stage, and geometry) into a dictionary
    RLOG.lprint("Compiling feature_ids info (discharge, stage, geometry) ... ")
    feature_id_polygon_path_dict = {}
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

    RLOG.lprint("Creating output gpkg file: " + output_inundation_poly)
    df = pd.DataFrame.from_dict(feature_id_polygon_path_dict, orient='index').reset_index()
    df.rename(columns={'index': 'feature_id'}, inplace=True)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=sv.DEFAULT_RASTER_OUTPUT_CRS)

    # add version number before saving
    gdf['version'] = ras2fim_version
    gdf.to_file(output_inundation_poly, driver="GPKG")
    RLOG.lprint("  Run Ras2inundation - Completed                                         |")


# -------------------------------------------------
if __name__ == "__main__":
    # Sample Usage

    # can be found in S3 and OWP... data/inundation_review/inundation_nwm_recurr/

    #  python ras2inundation.py
    #    -g C:\ras2fim_data\output_ras2fim\12090301_2277_230825\final\geocurves
    #    -f C:\ras2fim_data\inputs\X-National_Datasets\nwm21_17C_recurr_100_0_cms.csv
    #    -t C:\ras2fim_data\output_ras2fim\12090301_2277_230825\final\inundation.gpkg
    #    -o

    # Parse arguments
    parser = argparse.ArgumentParser(description="Produce Inundation from RAS2FIM geocurves.")
    parser.add_argument(
        "-g", "--geocurves_dir", help="Path to directory containing RAS2FIM geocurve CSVs.", required=True
    )
    parser.add_argument(
        "-f",
        "--flow_file",
        help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
        required=True,
    )
    parser.add_argument(
        "-t", "--output_inundation_poly", help="Path to output inundation polygon file.", required=False
    )

    args = vars(parser.parse_args())

    log_file_folder = args["geocurves_dir"]
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        start = timer()

        # call main program
        produce_inundation_from_geocurves(**args)

        RLOG.lprint(f"Completed in {round((timer() - start)/60, 2)} minutes.")

    except Exception:
        RLOG.critical(traceback.format_exc())
