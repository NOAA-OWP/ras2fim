import os
from datetime import datetime

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr
from geocube.api.core import make_geocube
from gval.utils.loading_datasets import adjust_memory_strategy


adjust_memory_strategy("normal")


def evaluate_model_results(inundation_polygons,
                           model_domain_polygons,
                           benchmark_raster,
                           spatial_unit,
                           output_dir):
    """ Method to evaluate the model performance of ras2fim output using benchmark data

    Parameters
    ----------
    inundation_polygons: str
        File containing inundation extent polygons of ras2fim model
    model_domain_polygons: str
        File containing the spatial boundaries of ras2fim model
    benchmark_raster: str
        File with benchmark raster data
    spatial_unit: str
        Spatial processing unit as used in ras2fim naming conventions
    output_dir: str
        Directory to save output evaluation files
    """

    # Load benchmark, inundation polygon, and model_domain polygon datasets
    benchmark = rxr.open_rasterio(benchmark_raster, mask_and_scale=True)
    model_results_df = gpd.read_file(inundation_polygons)
    model_domain_df = gpd.read_file(model_domain_polygons)

    # Create the raster candidate map
    model_results_df['extent'] = 1

    model_result_raster = make_geocube(
        vector_data=model_results_df,
        measurements=["extent"],
        resolution=10,
        output_crs=model_results_df.crs
    )

    model_result_raster = model_result_raster.fillna(0)

    model_result_raster = model_result_raster.rio.clip(
        model_domain_df['geometry'].values,
        model_domain_df.crs,
        drop=True,
        invert=False
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
    agreement_map, cross_tabulation_table, metric_table = candidate.gval.categorical_compare(
        benchmark,
        positive_categories=[1],
        negative_categories=[0],
        comparison_function="pairing_dict",
        pairing_dict=pairing_dict
    )

    # Write nodata
    agreement_map = agreement_map.rio.write_nodata(10)

    # assign metadata
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M')
    agreement_map.attrs['process datetime'] = dt_now
    agreement_map.attrs['spatial processing unit'] = spatial_unit
    metric_table.insert(0, 'procces datetime', dt_now)
    metadata_csv = pd.DataFrame({'process datetime': [dt_now], 'spatial processing unit': [spatial_unit]})

    # Create output directory if it does not exist
    if not os.path.exists(f"{output_dir}/{spatial_unit}"):
        os.makedirs(f"{output_dir}/{spatial_unit}")

    # Save output files
    agreement_map.astype(np.int8).rio.to_raster(
        f"{output_dir}/{spatial_unit}/agreement_map.tif", driver="COG"
    )
    metric_table.to_csv(f"{output_dir}/{spatial_unit}/metrics.csv", index=None)
    metadata_csv.to_csv(f"{output_dir}/{spatial_unit}/meta_data.csv", index=None)


if __name__ == '__main__':

    import argparse

    # Parse arguments
    parser = argparse.ArgumentParser(description="Produce Inundation from RAS2FIM geocurves.")
    parser.add_argument(
        "-i",
        "--inundation_polygons",
        help="Inundation polygons file from ras2fim",
        required=True
    )
    parser.add_argument(
        "-m",
        "--model_domain_polygons",
        help="Model domain polygon from ras2fim",
        required=True
    )
    parser.add_argument(
        "-b",
        "--benchmark_raster",
        help="Benchmark raster from respective source",
        required=True
    )
    parser.add_argument(
        "-st",
        "--spatial_unit",
        help='Spatial unit associated with ras2fim output',
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help='Directory to save output evaluation files',
        required=True,
    )

    args = vars(parser.parse_args())

    evaluate_model_results(**args)
