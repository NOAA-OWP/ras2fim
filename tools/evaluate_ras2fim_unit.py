import argparse
import os
import sys
import traceback
from datetime import datetime

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr
from geocube.api.core import make_geocube
from gval.utils.loading_datasets import adjust_memory_strategy


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv


RLOG = sv.R2F_LOG

adjust_memory_strategy("normal")


# -------------------------------------------------
def evaluate_unit_results(
    inundation_polygons, model_domain_polygons, benchmark_raster, unit_name, output_dir
):
    """Method to evaluate the model performance of ras2fim output using benchmark data

    Parameters
    ----------
    inundation_polygons: str
        File containing inundation extent polygons of ras2fim model
    model_domain_polygons: str
        File containing the spatial boundaries of ras2fim model
    benchmark_raster: str
        File with benchmark raster data
    unit_name: str
        e.g. 12030105_2276_ble_230923
    output_dir: str
        Directory to save output evaluation files
    """
    print()

    # TODO: Add input validation tests.

    RLOG.notice(f"GVAL evaluation beginning for {unit_name}")

    # Load benchmark, inundation polygon, and model_domain polygon datasets
    RLOG.lprint("Load benchmark, inundation polygon, and model_domain polygon datasets")
    benchmark = rxr.open_rasterio(benchmark_raster, mask_and_scale=True)
    model_results_df = gpd.read_file(inundation_polygons)
    model_domain_df = gpd.read_file(model_domain_polygons)

    # make sure the crs's are the same. models_domain_db shoudl be reprojected to models_results_df.
    model_domain_df = model_domain_df.to_crs(sv.DEFAULT_RASTER_OUTPUT_CRS)
    model_results_df = model_results_df.to_crs(sv.DEFAULT_RASTER_OUTPUT_CRS)

    # Create the raster candidate map
    RLOG.lprint("Creating the raster candidate maps")
    model_results_df['extent'] = 1

    model_result_raster = make_geocube(
        vector_data=model_results_df, measurements=["extent"], resolution=10, output_crs=model_results_df.crs
    )

    model_result_raster = model_result_raster.fillna(0)

    model_result_raster = model_result_raster.rio.clip(
        model_domain_df['geometry'].values, model_domain_df.crs, drop=True, invert=False
    )

    # Convert from xr.Dataset to xr.DataArray
    candidate = model_result_raster.to_array()
    candidate = candidate.rename({'variable': 'band'})
    candidate = candidate.assign_coords({'band': [1]})

    # Write nodata
    candidate = candidate.rio.write_nodata(10, encoded=True)
    benchmark = benchmark.rio.write_nodata(10, encoded=True)

    # Encoding dictionary for agreement map
    pairing_dict = {
        (0, 0): 0,
        (0, 1): 1,
        (0, np.nan): 10,
        (1, 0): 2,
        (1, 1): 3,
        (1, np.nan): 10,
        (4, 0): 4,
        (4, 1): 4,
        (4, np.nan): 10,
        (np.nan, 0): 10,
        (np.nan, 1): 10,
        (np.nan, np.nan): 10,
    }

    # Run evaluation with the above encoding dictionary for the agreement map
    RLOG.lprint("Run evaluation with the above encoding dictionary for the agreement map")
    agreement_map, cross_tabulation_table, metric_table = candidate.gval.categorical_compare(
        benchmark,
        positive_categories=[1],
        negative_categories=[0],
        comparison_function="pairing_dict",
        pairing_dict=pairing_dict,
    )

    # Write nodata
    agreement_map = agreement_map.rio.write_nodata(10)

    # assign metadata
    dt_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
    agreement_map.attrs['process datetime'] = dt_now
    agreement_map.attrs['unit_name'] = unit_name

    metric_table.insert(0, 'process_datetime', dt_now)
    if "process date" in metric_table.columns:
        metric_table.drop("process date", axis=1)

    metadata_csv = pd.DataFrame({'process_datetime': [dt_now], 'unit_name': [unit_name]})

    # Create output directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Save output files
    agreement_map.astype(np.int8).rio.to_raster(os.path.join(output_dir, "agreement_map.tif"), driver="COG")
    metric_table.to_csv(os.path.join(output_dir, "metrics.csv"), index=None)
    metadata_csv.to_csv(os.path.join(output_dir, "meta_data.csv"), index=None)

    RLOG.lprint(f"GVAL evaluation for {unit_name} complete")
    RLOG.lprint(f"Evaluation output files saved to {output_dir}")


# -------------------------------------------------
if __name__ == '__main__':
    """
    Example Usage:

    python evaluate_ras2fim_unit.py
    -i "s3://ras2fim/output_ras2fim/12030105_2276_ble_230923/final/inundation_polys/ble_100yr_inundation.gpkg"
    -m "s3://ras2fim/output_ras2fim/12030105_2276_ble_230923/final/models_domain/models_domain.gpkg"
    -b "s3://ras2fim-dev/gval/benchmark_data/ble/12030105/100yr/ble_huc_12030105_extent_100yr.tif"
    -u "12030105_2276_ble_230923_100yr"
    -o "C:\ras2fim_data\test_batch_eval"

    Note: The paths can be S3 paths or local drive paths.
    """

    # Parse arguments
    parser = argparse.ArgumentParser(description="Produce Inundation from RAS2FIM geocurves.")
    parser.add_argument(
        "-i", "--inundation_polygons", help="Inundation polygons file from ras2fim", required=True
    )
    parser.add_argument(
        "-m", "--model_domain_polygons", help="Model domain polygon from ras2fim", required=True
    )
    parser.add_argument(
        "-b", "--benchmark_raster", help="Benchmark raster from respective source", required=True
    )
    parser.add_argument(
        "-u",
        "--unit_name",
        help="Tag name that refers to a ras2fim run\n" "  e.g. 12030105_2276_ble_230923",
        required=True,
    )

    parser.add_argument("-o", "--output_dir", help='Directory to save output evaluation files', required=True)

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0] + datetime.now().strftime(
            '%Y-%m-%d_%H:%M'
        )
        # assumes RLOG has been added as a global var.
        RLOG.setup(os.path.join(args['output_dir'], script_file_name + ".log"))

        evaluate_unit_results(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
