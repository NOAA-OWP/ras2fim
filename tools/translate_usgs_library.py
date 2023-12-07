import os
import argparse
import glob
import geopandas as gpd
import pandas as pd
import numpy as np

from timeit import default_timer as timer


def reformat_usgs_fims_to_geocurves(usgs_map_gpkg, output_dir, catchments, usgs_rating_curves):
    # Create output_dir if necessary
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load NWM catchment geopackage
    print("Loading datasets...")
    start = timer()
    catchments_gdf = gpd.read_file(catchments)
    catchments_crs = catchments_gdf.crs
    
    # Load USGS rating curves
    print("Loading USGS rating curves...")
    usgs_rc_df = pd.read_csv(usgs_rating_curves)

    # Load USGS FIM Library geopackage
    print("Loading USFS FIM library...")
    usgs_fim_gdf = gpd.read_file(usgs_map_gpkg)

    print(f"Datasets loaded in {round((timer() - start)/60, 2)} minutes.")

    # Get list of all candidate FIM sites to process
    fim_sites = list(usgs_fim_gdf.USGSID.unique())

    # Loop through sites
    for site in fim_sites:

        # Create output directory site
        site_dir = os.path.join(output_dir, site)
        if not os.path.exists(site_dir):
            os.mkdir(site_dir)

        # Subset the entire usgs_fim_gdf library to only one site at a time
        subset_fim_gdf = usgs_fim_gdf.loc[usgs_fim_gdf.USGSID==site]

        # Remove rows with missing geometry  TODO LOG
        subset_fim_gdf = subset_fim_gdf.loc[subset_fim_gdf.geometry!=None]

        # Get list of unique stage values
        site_stages = list(subset_fim_gdf.STAGE.unique())

        # Process each stage
        for site_stage in site_stages:
            try:
                # Subset usgs_rc_df to only gage of interest
                subset_usgs_rc_df = usgs_rc_df.loc[usgs_rc_df.location_id==int(site)]

                # Subset subset_fim_gdf to only stage of interes
                stage_subset_fim_gdf = subset_fim_gdf.loc[subset_fim_gdf.STAGE==site_stage]

                # Reproject fim_gdf to match NWM catchments
                stage_subset_fim_gdf = stage_subset_fim_gdf.to_crs(catchments_crs)

                # Dissolve all geometries?
                stage_subset_fim_gdf['dissolve'] = 1
                stage_subset_fim_gdf = stage_subset_fim_gdf.dissolve(by="dissolve")

                # Cut dissolved geometry to align with catchment breakpoints and associate feature_ids (union)
                union = gpd.overlay(stage_subset_fim_gdf, catchments_gdf)

                # Exit if site-specific rating curve doesn't exist in provided file
                if subset_usgs_rc_df.empty:
                    continue

                # Use subset_usgs_rc_df to interpolate discharge from stage
                interpolated_q = np.interp([site_stage], subset_usgs_rc_df['stage'], subset_usgs_rc_df['flow'])

                # Save as geopackage (temp)
                output_shapefile = os.path.join(site_dir, str(site) + '_' + str(int(site_stage)) + '.shp')
                if union.empty == False:
                    union.to_file(output_shapefile)
                else:
                    continue
            except Exception as e:
                print("Exception")
                print(e)

        # List all recently written geopackages
        shape_path = os.path.join(site_dir, "*.shp")
        shp_list = glob.glob(shape_path)

        # Exit loop and delete site_dir if no shapefiles were produced
        if shp_list == []:
            os.rmdir(site_dir)
            continue
        
        # Merge all site-specific layers
        final_gdf = gpd.read_file(shp_list[0])
        for shp in shp_list:
            gdf = gpd.read_file(shp)
            final_gdf = pd.concat([final_gdf, gdf])

        # Save as CSV (move to very end later, after combining all geopackages)
        output_csv = os.path.join(site_dir, site + '.csv')
        final_gdf.to_csv(output_csv)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Prototype capability to reformat USGS inundation maps to geocurves."
    )
    parser.add_argument(
        "-d",
        "--usgs_map_gpkg",
        help="Path to USGS FIMs Geopackage (original source is Esri GDB).",
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

    parser.add_argument(
        "-rc",
        "--usgs_rating_curves",
        help="Path to rating curves CSV (available from inundation-mapping data).",
        required=True,
        default=None,
        type=str,
    )

    #TODO need to find the right branch catchments layer for cutting for each site. Go with biggest?

    start = timer()

    # Extract to dictionary and run
    reformat_usgs_fims_to_geocurves(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
