import argparse
import errno
import os
import shutil
import traceback
import re
import math

# import warnings
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import shapes
from rasterio.mask import mask
from shapely.geometry import MultiPolygon, Polygon, Point, LineString
from shapely.ops import split
from shapely.validation import make_valid

import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG  # the non mp version

GEOMETRY_COL = 'geometry'
xs_extension = 1000


def create_geocurves(ras2fim_huc_dir:str):
    
    # Get HUC 8
    dir_name = Path(ras2fim_huc_dir).name
    huc_name = re.match("^\d{8}", dir_name).group()
    
    # Read the conflated models list
    conflated_ras_models_csv = Path(ras2fim_huc_dir, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF,"conflated_ras_models.csv")
    conflated_ras_models = pd.read_csv(conflated_ras_models_csv, index_col=0)
    
    nwm_streams_ln_shp = Path(ras2fim_huc_dir, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF, f"{huc_name}_nwm_streams_ln.shp")
    nwm_streams_ln = gpd.read_file(nwm_streams_ln_shp)
    cross_section_ln_shp = Path(ras2fim_huc_dir, sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS, "cross_section_LN_from_ras.shp")
    cross_section_ln = gpd.read_file(cross_section_ln_shp)
    stream_qc_fid_xs_csv = Path(ras2fim_huc_dir, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF, f"{huc_name}_stream_qc_fid_xs.csv")
    stream_qc_fid_xs = pd.read_csv(stream_qc_fid_xs_csv, index_col=0)

    # Loop through each model
    for index, model in conflated_ras_models.iterrows():
        
        RLOG.lprint(model)
        
        model_nwm_streams_ln = nwm_streams_ln[nwm_streams_ln.ras_path == model.ras_path]
        model_stream_qc_fid_xs = stream_qc_fid_xs[stream_qc_fid_xs.ras_path == model.ras_path]
        model_cross_section_ln = cross_section_ln[cross_section_ln.ras_path == model.ras_path]

        # Load max depth boundary
        hecras_output = Path(ras2fim_huc_dir, "05_hecras_output")
        model_output_dir = [f for f in hecras_output.iterdir() if re.match(f"^{model.model_id}_", f.name)][0]
        model_name = model_output_dir.name.split("_")[1]
        model_depths_dir = Path(model_output_dir, model_name)
        max_inundation_shp = [f for f in model_depths_dir.glob("Inundation Boundary*.shp")][0]
        # Deduce the flow profile number
        flow_search = re.search('\(flow\d*_', max_inundation_shp.name).group()
        max_flow = int(re.search('\d+', flow_search).group())

        disconnected_inundation_poly = gpd.read_file(max_inundation_shp).explode(ignore_index=True, index_parts=False)
        main_inundation_poly = disconnected_inundation_poly.iloc[disconnected_inundation_poly.length.idxmax()]
        disconnected_inundation_poly = disconnected_inundation_poly.drop(index=disconnected_inundation_poly.length.idxmax())
                
        RLOG.lprint("  Loading the max inundation extent for each NWM feature")
    
        # Create max flow inundation masks for each NWM reach
        nwm_reach_inundation_masks = []
        for index, nwm_reach in model_nwm_streams_ln.iterrows():
                            
            nwm_reach = gpd.GeoDataFrame(nwm_reach.to_dict(), index=[0]).set_geometry('geometry', crs=model_nwm_streams_ln.crs)
            # Find boundary cross-sections
            boundary_cross_section_ids = find_boundary_xs(
                nwm_reach, 
                model_cross_section_ln
            )
            if not (boundary_cross_section_ids[0] or boundary_cross_section_ids[1]):
                continue
            boundary_cross_sections_df = model_cross_section_ln.loc[model_cross_section_ln['stream_stn'].isin(boundary_cross_section_ids)]
            boundary_cross_sections_df = boundary_cross_sections_df.assign(feature_id=nwm_reach.feature_id)
            # Extend boundary cross-sections because they sometimes don't breach the inundation polygon
            boundary_cross_sections_df.loc[:, 'geometry'] = boundary_cross_sections_df.geometry.apply(lambda row: 
                                                                extend_cross_section(row, xs_extension))
            
            # Use the first cross-section for the first split
            split1_inundation_geom = split(main_inundation_poly.geometry, boundary_cross_sections_df.geometry.iloc[0])
            split1_inundation = gpd.GeoDataFrame(split1_inundation_geom.geoms)
            split1_inundation = split1_inundation.set_geometry(0, crs=disconnected_inundation_poly.crs)
            split1_inundation = split1_inundation.sjoin(nwm_reach)
            # Use the second cross-section for the second split
            split2_inundation_geom = split(split1_inundation.geometry.iloc[0], boundary_cross_sections_df.geometry.iloc[1])
            split2_inundation = gpd.GeoDataFrame(split2_inundation_geom.geoms)
            split2_inundation = split2_inundation.set_geometry(0, crs=disconnected_inundation_poly.crs)
            final_inundation_poly = split2_inundation.sjoin(nwm_reach)
            final_inundation_poly = final_inundation_poly.rename(columns={0:'geometry'})
            final_inundation_poly = final_inundation_poly.set_geometry('geometry', crs=disconnected_inundation_poly.crs)
            final_inundation_poly = final_inundation_poly.assign(profile_num=max_flow)
            # Search for nearby disconnected polygons using a convex hull of the cross-sections
            search_xs = model_cross_section_ln.loc[(model_cross_section_ln.stream_stn > boundary_cross_sections_df.stream_stn.min()) &
                            (model_cross_section_ln.stream_stn < boundary_cross_sections_df.stream_stn.max())]
            search_xs = pd.concat([search_xs, boundary_cross_sections_df])
            search_hull = search_xs.dissolve().geometry.iloc[0].convex_hull
            search_hull = gpd.GeoDataFrame({'geometry': [search_hull]}, crs=boundary_cross_sections_df.crs)
            nearby_polygons = gpd.sjoin(disconnected_inundation_poly, search_hull, how='inner')
            final_inundation_poly.geometry.iloc[0] = MultiPolygon([final_inundation_poly.geometry.iloc[0]] + list(nearby_polygons.geometry))
            nwm_reach_inundation_masks.append(final_inundation_poly)
            
        nwm_reach_inundation_masks = gpd.GeoDataFrame(pd.concat(nwm_reach_inundation_masks, ignore_index=True))
        # Use max depth extent polygon as mask for other depths
        RLOG.lprint("  Getting the inundation extents from each flow")
        depth_tif_list = [f for f in model_depths_dir.iterdir() if f.suffix == '.tif']
        extent_polys_list = []
        for depth_tif in depth_tif_list:

            # This is the regex for the profile number. It finds the numbers after 'flow' in the TIF name
            flow_search = re.search('\(flow\d*\.*\d*_', depth_tif.name).group()
            profile_num = float(re.search('\d+\.*\d*', flow_search).group())

            with rasterio.open(depth_tif) as depth_grid_rast:
                depth_grid_nodata = depth_grid_rast.profile['nodata']
                depth_grid_crs = depth_grid_rast.crs

                # Mask raster using rasterio for each NWM reach
                for index, nwm_feature in nwm_reach_inundation_masks.iterrows():
                    mask_shapes = list([nwm_feature.geometry])
                    masked_inundation, masked_inundation_transform = mask(depth_grid_rast, mask_shapes, crop=True)
                    masked_inundation = masked_inundation[0]
                    # Create binary raster
                    binary_arr = np.where((masked_inundation > 0) & (masked_inundation != depth_grid_nodata), 1, 0).astype("uint8")
                    # if the array only has values of zero, then skip it (aka.. no heights above surface)
                    if np.min(binary_arr) == 0 and np.max(binary_arr) == 0:
                        RLOG.warning(f"depth_grid of {depth_tif} does not have any heights above surface.")
                        continue
                        
                    results = (
                        {"properties": {"extent": 1}, "geometry": s}
                        for i, (s, v) in enumerate(
                            shapes(
                                binary_arr,
                                mask=binary_arr > 0,
                                transform=masked_inundation_transform,
                                connectivity=8
                            )
                        )
                    )

                    # Convert list of shapes to polygon, then dissolve
                    extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=depth_grid_crs)
                    try:
                        #extent_poly_diss = extent_poly.dissolve(by="extent")
                        extent_poly_diss = extent_poly.dissolve()
                        multipoly_inundation = [
                            MultiPolygon([feature]) if type(feature) == Polygon else feature
                            for feature in extent_poly_diss["geometry"]
                        ]
                        if not multipoly_inundation.is_valid:
                            make_valid(multipoly_inundation)
                        extent_poly_diss["geometry"] = multipoly_inundation

                    except AttributeError as ae:
                        # TODO (from v1) why does this happen? I suspect bad geometry. Small extent?
                        RLOG.lprint("^^^^^^^^^^^^^^^^^^")
                        msg = "Warning...\n"
                        msg += f"  huc is {huc_name}; feature_id = {nwm_feature.ID}; depth_grid = {depth_tif}\n"
                        msg += f"  Details: {ae}"
                        RLOG.warning(msg)
                        RLOG.lprint("^^^^^^^^^^^^^^^^^^")
                        continue
                        
                    extent_poly_diss = extent_poly_diss.assign(feature_id=nwm_feature.feature_id,
                                                        profile_num=profile_num)
                    extent_polys_list.append(extent_poly_diss)
            extent_poly_df = gpd.GeoDataFrame(pd.concat(extent_polys_list, ignore_index=True))
            extent_poly_df.to_file(Path(model_output_dir, 'extent_polys.gpkg'), index=False)
    
    return extent_poly_df

    # TODO Join geometries with rating curve CSVs

def find_boundary_xs(nwm_seg_gdf, cross_section_gdf, station_column='stream_stn'):
    
    # Get intersecting cross sections
    fid_cross_sections = gpd.sjoin(nwm_seg_gdf, cross_section_gdf, how='right')
    # Filter to only the intersecting ones
    fid_cross_sections = fid_cross_sections.loc[~np.isnan(fid_cross_sections.feature_id)]
    if len(fid_cross_sections) < 2:
        return None, None
    # Find upper and lower stream stations
    down_xs = fid_cross_sections[station_column].min()
    up_xs = fid_cross_sections[station_column].max()
    # Order cross-sections
    stations_sorted = cross_section_gdf[station_column].sort_values()

    # Count 2 downstream
    down_idx = list(stations_sorted).index(down_xs) - 2
    if down_idx >= 0:
        down_xs_plus2 = stations_sorted.iloc[down_idx]
    else:
        # if this is the last segment in the river, just pick the first station
        down_xs_plus2 = stations_sorted.iloc[0]

    # Count 2 upstream
    up_idx = list(stations_sorted).index(up_xs) + 2
    try:
        up_xs_plus2 = stations_sorted.iloc[up_idx]
    except IndexError:
        # if this is the first segment in the river, just pick the last station
        up_xs_plus2 = stations_sorted.iloc[-1]

    return down_xs_plus2, up_xs_plus2
    
def extend_vector(angle, extension_distance):
    # Note: all of these calculations are done in radians
    y_dif = math.sin(angle) * extension_distance
    x_dif = math.cos(angle) * extension_distance
    return y_dif, x_dif

def extend_cross_section(geom, extension_distance):
    coords = list(geom.coords)
    start_y = (coords[1][1] - coords[0][1])
    start_x = (coords[1][0] - coords[0][0])
    start_slope = 0 if start_x == 0 else start_y / start_x
    end_y = (coords[-2][1] - coords[-1][1])
    end_x = (coords[-2][0] - coords[-1][0])
    end_slope = 0 if end_x == 0 else end_y / end_x
    # Add new points to the line
    y_dif, x_dif = extend_vector(math.atan(start_slope), extension_distance)
    start_pnt = Point(coords[0][0] + math.copysign(x_dif, start_x) * -1, 
                        coords[0][1] + math.copysign(y_dif, start_y) * -1)
    y_dif, x_dif = extend_vector(math.atan(end_slope), extension_distance)
    end_pnt = Point(coords[-1][0] + math.copysign(x_dif, end_x) * -1, 
                    coords[-1][1] + math.copysign(y_dif, end_y) * -1)
    return LineString([start_pnt] + coords + [end_pnt])


# -------------------------------------------------
def manage_geo_rating_curves_production(ras2fim_huc_dir, output_folder, overwrite):
    """
    This function sets up the multiprocessed generation of geo version of feature_id-specific rating curves.

    Args:
        ras2fim_huc_dir (str): Path to HUC8-level directory storing RAS2FIM outputs for a given run.
        version (str): Version number for RAS2FIM version that produced outputs.
        output_folder (str): The path to the output folder where geo rating curves will be written.
    """

    # get the version
    changelog_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, 'doc', 'CHANGELOG.md')
    )
    version = sf.get_changelog_version(changelog_path)
    RLOG.lprint("Version found: " + version)

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.notice("|                   Create GeoCurves                              |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    RLOG.lprint(f"  ---(f) ras2fim_huc_dir: {ras2fim_huc_dir}")
    RLOG.lprint(f"  ---(v) ras2fim version: {version}")
    RLOG.lprint(f"  ---(t) output_folder: {output_folder}")
    RLOG.lprint(f"  ---(o) overwrite: {overwrite}")

    RLOG.lprint("")
    overall_start_time = datetime.utcnow()
    dt_string = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Started (UTC): {dt_string}")

    # Set up output folders. (final outputs folder now created early in the ras2fim.py lifecycle)
    if not os.path.exists(ras2fim_huc_dir):
        RLOG.error(f"{ras2fim_huc_dir} does not exist")
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), ras2fim_huc_dir)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Make geocurves_dir
    geocurves_dir = os.path.join(output_folder, sv.R2F_OUTPUT_DIR_GEOCURVES)

    if os.path.exists(geocurves_dir) and not overwrite:
        RLOG.lprint(
            "The output directory, "
            + geocurves_dir
            + ", already exists. Use the overwrite flag (-o) to overwrite."
        )
        quit()

    if os.path.exists(geocurves_dir):
        shutil.rmtree(geocurves_dir)

    # Either way.. we are makign a new geocurve folder. e.g. If it is overwrite, we deleted
    #  before replacing it so we don't have left over garbage
    os.makedirs(geocurves_dir)

    # Feed into main geocurve creation function
    create_geocurves(ras2fim_huc_dir)


    # Calculate duration
    RLOG.success("Complete")
    end_time = datetime.utcnow()
    dt_string = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Ended (UTC): {dt_string}")
    time_duration = end_time - overall_start_time
    RLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    RLOG.lprint("")


# -------------------------------------------------
if __name__ == "__main__":
    # Sample command (all args)
    # python create_geocurves.py -f 'c:\ras2fim_data\output_ras2fim\12090301_2277_230923'
    #  -j 6
    #  -t 'c:\ras2fim_data\output_ras2fim\12090301_2277_230923\final'
    #  -o

    # Parse arguments
    parser = argparse.ArgumentParser(description="Produce Geo Rating Curves for RAS2FIM")
    parser.add_argument(
        "-f",
        "--ras2fim_output_dir",
        help="REQUIRED: Path to directory containing RAS2FIM unit output (huc/crs)",
        required=True,
    )
    parser.add_argument(
        "-j", "--job_number", help="Number of processes to use", required=False, default=1, type=int
    )
    parser.add_argument(
        "-t", "--output_folder", help="Target: Where the geocurve output folder will be", required=True
    )
    parser.add_argument("-o", "--overwrite", help="Overwrite files", required=False, action="store_true")

    args = vars(parser.parse_args())

    log_file_folder = args["ras2fim_output_dir"]
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split(".")[0]
        # Assumes RLOG has been added as a global var.
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        manage_geo_rating_curves_production(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
