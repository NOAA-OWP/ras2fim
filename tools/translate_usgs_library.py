import os
import argparse
import glob
import geopandas as gpd

from timeit import default_timer as timer


def reformat_usgs_fims_to_geocurves(usgs_map_dir, output_dir, catchments):
    # Create output_dir if necessary
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load NWM catchment geopackage
    print("Loading NWM catchments...")
    nwm_catchments_gdf = gpd.read_file(catchments)
    nwm_catchments_crs = nwm_catchments_gdf.crs

    # List shapefiles to reformat
    shapefile_path = os.path.join(usgs_map_dir, "*.shp")
    shapefile_list = glob.glob(shapefile_path)
    for shapefile in shapefile_list:
        fim_gdf = gpd.read_file(shapefile)

        # Reproject fim_gdf to match NWM catchments
        fim_gdf = fim_gdf.to_crs(nwm_catchments_crs)

        # Dissolve all geometries?
        fim_gdf['dissolve'] = 1
        fim_gdf = fim_gdf.dissolve(by="dissolve")   

        # Cut dissolved geometry to align with NWM catchment breakpoints (spatial join)
        union = gpd.overlay(fim_gdf, nwm_catchments_gdf)

        print(union)

        # Use rating curve to interpolate discharge from stage

        # Save as geopackage (temp)
        output_geopackage = os.path.join(output_dir, os.path.split(shapefile)[1].replace('.shp', '.gpkg'))
        union.to_file(output_geopackage, driver="GPKG")

        # Save as CSV
        output_csv = os.path.join(output_dir, os.path.split(shapefile)[1].replace('.shp', '.csv'))
        union.to_csv(output_csv)


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Prototype capability to reformat USGS inundation maps to geocurves."
    )
    parser.add_argument(
        "-d",
        "--usgs_map_dir",
        help="Directory path to USGS FIMs are stored.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        help="Directory path for output geocurves.",
        required=True,
        default=None,
        type=str,
    )

    parser.add_argument(
        "-c",
        "--catchments",
        help="Path to catchments layer that will be used to cut and crosswalk FIM polygons (using NWM for now).",
        required=True,
        default=None,
        type=str,
    )

    start = timer()

    # Extract to dictionary and run
    reformat_usgs_fims_to_geocurves(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
