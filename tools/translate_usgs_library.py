import os
import argparse
import glob
import geopandas as gpd
import pandas as pd
import numpy as np
import warnings
import shutil

from timeit import default_timer as timer

warnings.filterwarnings('ignore')


def identify_best_branch_catchments(huc8_outputs_dir, subset_fim_gdf):

    # Open branch_polygons and check for overlap with subset_fim_gdf
    branch_polygons = os.path.join(huc8_outputs_dir, 'branch_polygons.gpkg')
    branch_polygons_gdf = gpd.read_file(branch_polygons).to_crs(subset_fim_gdf.crs)
    joined_gdf = branch_polygons_gdf.sjoin(subset_fim_gdf,how='left')
    not_null_rows = joined_gdf['USGSID'].notnull()
    subset_joined_gdf = joined_gdf[not_null_rows]
    branches_of_interest = list(subset_joined_gdf.levpa_id.unique())

    # Get path to branches directory and create paths to all branch catchment in list
    branch_path_list = []
    branches_dir = os.path.join(huc8_outputs_dir, 'branches')
    for branch in branches_of_interest:
        branch_catchments = os.path.join(branches_dir, branch, f'gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch}.gpkg')
        branch_path_list.append(branch_catchments)

    del branch_polygons_gdf, joined_gdf, not_null_rows, subset_joined_gdf, branches_of_interest

    return branch_path_list


def select_best_union(subset_fim_gdf, candidate_geocurves):

    max_real_stage = subset_fim_gdf.STAGE.max()

    # Loop through candidate branches
    winner_count = 0  # Initialize feature_number to 0
    for candidate in candidate_geocurves:

        # Get branch_id
        branch_id = os.path.split(candidate)[1].split('_')[1]

        # Open branch candidate
        candidate_gdf = gpd.read_file(candidate).to_crs(subset_fim_gdf.crs)

        # Subset candidate_gdf to be only the max extent
        max_subset_candidate_gdf = candidate_gdf.loc[candidate_gdf.STAGE == max_real_stage]
        del candidate_gdf

        # RULES
        # Select the layer with more features
        # Could consider layer area as well (bigger area is better?)
        candidate_feature_count = len(max_subset_candidate_gdf)
        if candidate_feature_count > winner_count:
            winner_count = candidate_feature_count
            winner_path = candidate

        del max_subset_candidate_gdf
    del subset_fim_gdf, max_real_stage

    return winner_path 


def reformat_to_hydrovis_geocurves(site, best_match_path, usgs_gages_gdf):
    pass
    # For each feature_id, write a CSV with
    # discharge_cfs, stage_ft, wse_ft, discharge_cms, stage_m, stage_mm, wse_m, geometry, version
    # You also to include the HUC12 in the filename, e.g. 5793592_HUC_120903010404_rating_curve_geo.csv


def reformat_usgs_fims_to_geocurves(usgs_map_gpkg, output_dir, level_path_parent_dir, usgs_rating_curves, usgs_gages_gpkg):
    # Create output_dir if necessary
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    start = timer()

    # Load USGS rating curves
    print("Loading USGS rating curves...")
    usgs_rc_df = pd.read_csv(usgs_rating_curves)

    # Load USGS gages for metadata lookup
    print("Loading USGS gages...")
    usgs_gages_gdf = gpd.read_file(usgs_gages_gpkg)

    fim_domain_gdf = gpd.read_file(usgs_map_gpkg, layer='fim_model_extent')

    print(f"Datasets loaded in {round((timer() - start)/60, 2)} minutes.")

    # Loop through sites
    for index, row in fim_domain_gdf.iterrows():
        site = row['USGSID']
        geometry = row['geometry']

        # if site != "05540500":
        #     continue

        try: 
            int(site)
        except ValueError: 
            continue

        # Create output directory site
        site_dir = os.path.join(output_dir, site)
        if not os.path.exists(site_dir):
            os.mkdir(site_dir) 

        branch_parent_dir = os.path.join(site_dir, 'branches')
        if not os.path.exists(branch_parent_dir):
            os.mkdir(branch_parent_dir)

        # Load USGS FIM Library geopackage
        print("Loading USGS FIM library for site " + site + "...")
        start = timer()
        usgs_fim_gdf = gpd.read_file(usgs_map_gpkg,layer='fim_flood_extents', mask=geometry)
        print(site + f" loaded in {round((timer() - start)/60, 2)} minutes.")

        # Determine HUC8  TODO would be faster if FIM library had HUC8 attribute
        try:
            huc8 = usgs_gages_gdf.loc[usgs_gages_gdf.location_id == site].HUC8.values[0]
        except IndexError as e:
            print(e)
            continue  # TODO log, why?

        # Subset the entire usgs_fim_gdf library to only one site at a time
        subset_fim_gdf = usgs_fim_gdf.loc[usgs_fim_gdf.USGSID==site]

        # Remove rows with missing geometry  TODO LOG
        print("Before removing empty geometry: " + str(len(subset_fim_gdf)))
        subset_fim_gdf = subset_fim_gdf.loc[subset_fim_gdf.geometry!=None]
        print("After removing empty geometry: " + str(len(subset_fim_gdf)))
        print()
        # Identify which level path is best for the site
        huc8_outputs_dir = os.path.join(level_path_parent_dir, huc8)
        if os.path.exists(huc8_outputs_dir):
            branch_path_list = identify_best_branch_catchments(huc8_outputs_dir, subset_fim_gdf)
        else:
            print("Missing branch data, expected: " + huc8_outputs_dir)
            shutil.rmtree(site_dir)
            continue

        # Loop through different catchments, do the below processing, then check for best geometrical match
        branch_id_list = []
        candidate_layers = []
        for catchments in branch_path_list:
            
            branch_id = os.path.split(catchments)[1].split('_')[-1].replace('.gpkg','')
            branch_id_list.append(branch_id)
            branch_output_dir = os.path.join(branch_parent_dir, branch_id)
            if not os.path.exists(branch_output_dir):
                os.mkdir(branch_output_dir)

            # Load catchment geopackage
            catchments_gdf = gpd.read_file(catchments)
            catchments_crs = catchments_gdf.crs

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
                        print("Missing RC for " + site)
                        continue

                    # Use subset_usgs_rc_df to interpolate discharge from stage
                    interpolated_q = np.interp([site_stage], subset_usgs_rc_df['stage'], subset_usgs_rc_df['flow'])

                    # Save as geopackage (temp)
                    output_shapefile = os.path.join(branch_output_dir, str(site) + '_' + branch_id + '_' + str(site_stage).replace(".","_") + '.gpkg')
                    if union.empty == False:
                        union.to_file(output_shapefile, driver='GPKG')
                        del union
                    else:
                        continue
                except Exception as e:
                    print("Exception")
                    print(e)

            del catchments_gdf

            # List all recently written shapefiles
            shape_path = os.path.join(branch_output_dir, "*.gpkg")
            shp_list = glob.glob(shape_path)

            # Exit loop and delete site_dir if no shapefiles were produced
            if shp_list == []:
                os.rmdir(branch_output_dir)
                continue
            
            # Merge all site-specific layers
            final_gdf = gpd.read_file(shp_list[0])
            for shp in shp_list:
                gdf = gpd.read_file(shp)
                final_gdf = pd.concat([final_gdf, gdf])

            output_shape = os.path.join(branch_output_dir, site + '_' + branch_id + '_' + 'merged.gkpg')
            final_gdf.to_file(output_shape, driver='GPKG')
            del final_gdf
            candidate_layers.append(output_shape)

        # Select best match of all the generated FIM/branch unions
        print("Selecting best union for " + site)
        best_match_path = select_best_union(subset_fim_gdf, candidate_layers)
        
        # TODO Need function here to reformat best_match_path data to match HydroVIS format
        reformat_to_hydrovis_geocurves(site, best_match_path, usgs_gages_gdf)
        
        # Save as CSV (move to very end later, after combining all geopackages)
        # output_csv = os.path.join(site_dir, huc8 + "_" + site + '_geocurves.csv')
        # best_match_gdf.to_csv(output_csv)


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Prototype capability to reformat USGS inundation maps to geocurves."
    )
    parser.add_argument(
        "-d",
        "--usgs_map_gpkg",
        help="Path to USGS FIMs GDB (original source is Esri GDB).",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help="Directory path for output geocurves.",
        required=True,
        default=None,
        type=str,
    )

    parser.add_argument(
        "-c",
        "--level_path_parent_dir",
        help="Path to HAND FIM4 parent dictory, e.g. 4.X.X.X.",
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

    parser.add_argument(
        "-g",
        "--usgs_gages_gpkg",
        help="Path to usgs_gages.gpkg (available from inundation-mapping data).",
        required=True,
        default=None,
        type=str,
    )

    #TODO need to find the right branch catchments layer for cutting for each site. Go with biggest?

    start = timer()

    # Extract to dictionary and run
    reformat_usgs_fims_to_geocurves(**vars(parser.parse_args()))

    print(f"Completed in {round((timer() - start)/60, 2)} minutes.")
