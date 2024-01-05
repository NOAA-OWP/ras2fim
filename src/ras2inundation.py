import argparse
import errno
import os
from timeit import default_timer as timer

import geopandas as gpd
import pandas as pd


# -------------------------------------------------
def produce_inundation_from_geocurves(geocurves_dir, flow_file, output_inundation_poly, overwrite):
    """
    Produce inundation from RAS2FIM geocurves.

    Args:
        geocurves_dir (str): Path to directory containing RAS2FIM geocurve CSVs.
        flow_file (str): Discharges in CMS as a CSV file. "feature_id" and "discharge" columns
         MUST be supplied. output_inundation_poly (str): Path to output inundation polygon.
        overwrite (bool): Whether to overwrite files if they already exist.
    """

    # Check that output directory exists. Notify user that output directory will be created if not.
    if not os.path.exists(os.path.split(output_inundation_poly)[0]):
        print(
            "Parent directory for "
            + os.path.split(output_inundation_poly)[1]
            + " does not exist. Directory/ies will be created."
        )
        os.makedirs(os.path.split(output_inundation_poly)[0])

    # Check that geocurves_dir exists
    if not os.path.exists(geocurves_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), geocurves_dir)

    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)

    # Create dictionary of available feature_id geocurve full paths.
    geocurves_list = os.listdir(geocurves_dir)
    geocurve_path_dictionary, available_feature_id_list = {}, []
    for geocurve in geocurves_list:
        if ".csv" in geocurve:
            feature_id = geocurve.split("_")[0]
            available_feature_id_list.append(feature_id)
            geocurve_path_dictionary.update({feature_id: {"path": os.path.join(geocurves_dir, geocurve)}})

    # Open flow_file to detemine feature_ids to process
    flow_file_df = pd.read_csv(flow_file)

    # Loop through feature_ids and concatenate into a single dataframe
    iteration = 0
    feature_id_polygon_path_dict = {}
    for feature_id in flow_file_df["feature_id"]:

        if str(feature_id) not in available_feature_id_list:
            # Skip flow values not found in RAS library
            continue
        discharge_cms = flow_file_df.loc[flow_file_df["feature_id"] == feature_id, "discharge"].values[0]
        # Get path to polygon from geocurve
        try:
            geocurve_file_path = geocurve_path_dictionary[str(feature_id)]["path"]
        except KeyError:
            continue

        # Use interpolation to find the row in geocurve_df that corresponds to the discharge_value
        geocurve_df = pd.read_csv(geocurve_file_path)
        row_idx = geocurve_df["discharge_cms"].sub(discharge_cms).abs().idxmin()
        subset_geocurve = geocurve_df.iloc[row_idx:]

        if "filename" in subset_geocurve.columns:
            polygon_filename = subset_geocurve["filename"]
        else:
            prefix = '_'.join(geocurve_file_path.split('\\')[-1].split('_')[:3])
            polygon_filename = f"{prefix}_{int(subset_geocurve.loc[row_idx, 'stage_mm_join'])}_mm.gpkg"

        polygon_path = os.path.join(os.path.split(geocurves_dir)[0], "polys", polygon_filename)

        if os.path.exists(polygon_path):
            feature_id_polygon_path_dict.update(
                {feature_id: {"discharge_cms": discharge_cms, "path": polygon_path}}
            )

        iteration += 1

    # Concatenate entire list of desired polygons into one geodataframe
    print(len(feature_id_polygon_path_dict))
    iteration = 0
    for feature_id in feature_id_polygon_path_dict:
        if iteration == 0:
            gdf = gpd.read_file(feature_id_polygon_path_dict[feature_id]["path"])
            gdf["discharge_cms"] = feature_id_polygon_path_dict[feature_id]["discharge_cms"]
        else:
            new_gdf = gpd.read_file(feature_id_polygon_path_dict[feature_id]["path"])
            new_gdf["discharge_cms"] = feature_id_polygon_path_dict[feature_id]["discharge_cms"]
            gdf = gpd.pd.concat([gdf, new_gdf])

        iteration += 1

    print("Writing final output: " + output_inundation_poly)
    # Now you have the GeoDataFrame `gdf` with polygons, and you can write it to a GeoPackage
    gdf.to_file(output_inundation_poly, driver="GPKG")


# -------------------------------------------------
if __name__ == "__main__":
    # Sample Usage

    #  python ras2inundation.py
    #    -g C:\ras2fim_data\output_ras2fim\12090301_2277_230825\final\geocurves
    #    -f C:\ras2fim_data\inputs\X-National_Datasets\nwm21_17C_recurr_100_0_cms.csv
    #    -t C:\ras2fim_data\output_ras2fim\12090301_2277_230825\final\inundation.gpkg

    # # Parse arguments
    # parser = argparse.ArgumentParser(description="Produce Inundation from RAS2FIM geocurves.")
    # parser.add_argument(
    #     "-g", "--geocurves_dir", help="Path to directory containing RAS2FIM geocurve CSVs.", required=True
    # )
    # parser.add_argument(
    #     "-f",
    #     "--flow_file",
    #     help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',
    #     required=True,
    # )
    # parser.add_argument(
    #     "-t", "--output_inundation_poly", help="Path to output inundation polygon file.", required=False
    # )
    # parser.add_argument("-o", "--overwrite", help="Overwrite files", required=False, action="store_true")
    #
    # args = vars(parser.parse_args())

    args = {
        'geocurves_dir': r'C:\ras2fim_data\output_ras2fim\12040101_102739_230922\final\geocurves',
        'flow_file': r'C:\ras2fim_data\inputs\X-National_Datasets\nwm21_17C_recurr_100_0_cms.csv',
        'output_inundation_poly': r'C:\ras2fim_data\output_ras2fim\12040101_102739_230922\final\inundation.gpkg',
        'overwrite': True
    }

    start = timer()

    produce_inundation_from_geocurves(**args)

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
