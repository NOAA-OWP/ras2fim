import argparse
import errno
import math
import multiprocessing as mp
import os
import re
import shutil
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

# import warnings
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import tqdm
from rasterio.features import shapes
from rasterio.mask import mask
from shapely.geometry import LineString, MultiPolygon, Point
from shapely.ops import split

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG  # the non mp version
MP_LOG = ras2fim_logger.RAS2FIM_logger()  # the mp version

GEOMETRY_COL = 'geometry'
# This is the distance to extent the boundary cross-sections to ensure that the inundation polygon is split
xs_extension = 1000


# -------------------------------------------------
def find_boundary_xs(nwm_seg_gdf, cross_section_gdf, station_column='stream_stn'):
    # Get intersecting cross sections
    fid_cross_sections = gpd.sjoin(nwm_seg_gdf, cross_section_gdf, how='right')
    # Filter to only the intersecting ones
    fid_cross_sections = fid_cross_sections.loc[~np.isnan(fid_cross_sections.feature_id)]
    if len(fid_cross_sections) < 1:
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


# -------------------------------------------------
def extend_vector(angle, extension_distance):
    # Note: all of these calculations are done in radians
    y_dif = math.sin(angle) * extension_distance
    x_dif = math.cos(angle) * extension_distance
    return y_dif, x_dif


# -------------------------------------------------
def extend_cross_section(geom, extension_distance):
    coords = list(geom.coords)
    start_y = coords[1][1] - coords[0][1]
    start_x = coords[1][0] - coords[0][0]
    start_slope = 0 if start_x == 0 else start_y / start_x
    end_y = coords[-2][1] - coords[-1][1]
    end_x = coords[-2][0] - coords[-1][0]
    end_slope = 0 if end_x == 0 else end_y / end_x
    # Add new points to the line
    y_dif, x_dif = extend_vector(math.atan(start_slope), extension_distance)
    start_pnt = Point(
        coords[0][0] + math.copysign(x_dif, start_x) * -1, coords[0][1] + math.copysign(y_dif, start_y) * -1
    )
    y_dif, x_dif = extend_vector(math.atan(end_slope), extension_distance)
    end_pnt = Point(
        coords[-1][0] + math.copysign(x_dif, end_x) * -1, coords[-1][1] + math.copysign(y_dif, end_y) * -1
    )
    return LineString([start_pnt] + coords + [end_pnt])


# -------------------------------------------------
def mp_process_depth_grid_tif(var_d: dict):
    try:
        depth_tif_win_path = var_d["depth_tif_win_path"]
        all_nwm_reach_inundation_masks_gdf = var_d["all_nwm_reach_inundation_masks_gdf"]
        name_mid = var_d["name_mid"]
        unit_output_path = var_d["unit_output_path"]
        code_version = var_d["code_version"]
        unit_name = var_d["unit_name"]
        unit_version = var_d["unit_version"]
        source_code = var_d["source_code"]
        source1 = var_d["source1"]
        crs = var_d["crs"]
        log_file_prefix = var_d["log_file_prefix"]
        rlog_file_path = var_d["rlog_file_path"]

        file_id = sf.get_date_with_milli()
        log_file_name = f"{log_file_prefix}-{file_id}.log"
        MP_LOG.setup(os.path.join(rlog_file_path, log_file_name))

        MP_LOG.trace(f"... Processing depth tif {depth_tif_win_path.name}")

        # This is the regex for the profile number. It finds the numbers after 'flow' in the TIF name
        flow_search = re.search('\(flow\d*\.*\d*_', depth_tif_win_path.name).group()
        profile_num = float(re.search('\d+\.*\d*', flow_search).group())

        geocurve_df_list = []

        with rasterio.open(depth_tif_win_path) as depth_grid_rast:
            depth_grid_nodata = depth_grid_rast.profile['nodata']
            depth_grid_crs = depth_grid_rast.crs

            # Mask raster using rasterio for each NWM reach
            for ___, nwm_feature in all_nwm_reach_inundation_masks_gdf.iterrows():
                # Load the rating curve
                MP_LOG.trace(
                    f"Processing {name_mid} for {depth_tif_win_path}:"
                    f" feature ID = {nwm_feature.feature_id}"
                )
                rating_curve_dir = Path(
                    unit_output_path,
                    sv.R2F_OUTPUT_DIR_CREATE_RATING_CURVES,
                    name_mid,
                    f'rating_curve_{nwm_feature.feature_id}.csv',
                )

                if rating_curve_dir.exists() is False:
                    continue

                mask_shapes = list([nwm_feature.geometry])
                masked_inundation, masked_inundation_transform = mask(depth_grid_rast, mask_shapes, crop=True)
                masked_inundation = masked_inundation[0]
                # Create binary raster
                binary_arr = np.where(
                    (masked_inundation > 0) & (masked_inundation != depth_grid_nodata), 1, 0
                ).astype("uint8")

                depth_poly_args = (
                    {"properties": {"extent": 1}, "geometry": s}
                    for i, (s, v) in enumerate(
                        shapes(
                            binary_arr,
                            mask=binary_arr > 0,
                            transform=masked_inundation_transform,
                            connectivity=8,
                        )
                    )
                )

                # cur dev (2.0.2.1)
                results_ls = list(depth_poly_args)

                if len(results_ls) == 0:
                    continue

                # Convert list of shapes to polygon, then dissolve
                extent_poly = gpd.GeoDataFrame.from_features(results_ls, crs=depth_grid_crs)
                # -----------------

                try:
                    extent_poly_diss = extent_poly.dissolve(by="extent")

                except AttributeError as ae:
                    # TODO (from v1) why does this happen? I suspect bad geometry. Small extent?
                    msg = "Warning...\n"
                    msg += f"feature_id = {nwm_feature.feature_id}; "
                    msg += f"depth_grid = {depth_tif_win_path}\n"
                    msg += f"  Details: {ae}"
                    MP_LOG.warning(msg)
                    MP_LOG.warning(traceback.format_exc())
                    continue

                # Add the feature_id, profile_num, and code_version columns
                extent_poly_diss = extent_poly_diss.assign(
                    profile_num=profile_num,
                    version=code_version,
                    unit_name=unit_name,
                    unit_version=unit_version,
                    source_code=source_code,
                    source=source1,
                    crs=crs,
                )
                extent_poly_diss = extent_poly_diss.reindex(
                    columns=[
                        'version',
                        'unit_name',
                        'unit_version',
                        'source_code',
                        'source',
                        'crs',
                        'geometry',
                        'profile_num',
                    ]
                )
                # TODO: Does not exist anymore
                # extent_poly_diss = extent_poly_diss.drop(columns='extent')

                rating_curve_df = pd.read_csv(rating_curve_dir)

                # Join the geometry to the rating curve
                feature_id_rating_curve_geo = pd.merge(
                    rating_curve_df, extent_poly_diss, on="profile_num", how="right"
                )
                geocurve_df_list.append(feature_id_rating_curve_geo)

        geocurve_df = pd.concat(geocurve_df_list)
        return geocurve_df

    except Exception:
        if MP_LOG.LOG_SYSTEM_IS_SETUP is True:
            MP_LOG.critical(traceback.format_exc())
            return pd.DataFrame()  # empty
        else:
            print(traceback.format_exc())
            sys.exit(1)


# -------------------------------------------------
def create_geocurves(unit_output_path: str, code_version: str):
    # Get HUC 8
    dir_name = Path(unit_output_path).name
    huc_name = re.match("^\d{8}", dir_name).group()

    # Get the unit name and version
    dir_name_split = dir_name.split('_')
    unit_name = '_'.join(dir_name_split[:-1])
    source_code = unit_name.split('_')[-1]
    unit_version = dir_name_split[-1]
    source1 = sf.get_source_info(source_code)

    # Read the conflated models list
    conflated_ras_models_csv = os.path.join(
        unit_output_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF, "conflated_ras_models.csv"
    )
    conflated_ras_models = pd.read_csv(conflated_ras_models_csv, index_col=0)
    conflated_ras_models.sort_values(by=['model_id'])

    nwm_streams_ln_shp = os.path.join(
        unit_output_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF, f"{huc_name}_nwm_streams_ln.shp"
    )
    nwm_streams_ln = gpd.read_file(nwm_streams_ln_shp)
    nwm_streams_ln.sort_values(by=["feature_id"], inplace=True)

    cross_section_ln_shp = os.path.join(
        unit_output_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS, "cross_section_LN_from_ras.shp"
    )
    cross_section_ln = gpd.read_file(cross_section_ln_shp)

    print()
    print(
        "This parts of this section can be a bit slow. Most can take just a minute or less,"
        " but we have seen a few anomolies take over one hour."
    )
    print()

    RLOG.trace("Start processing conflated models for each model")
    RLOG.lprint(f"-- Number of models to process are {len(conflated_ras_models)}")
    # Loop through each model
    path_geocurve_folder = os.path.join(
        unit_output_path, sv.R2F_OUTPUT_DIR_FINAL, sv.R2F_OUTPUT_DIR_GEOCURVES
    )

    len_conflated_ras_models = len(conflated_ras_models)
    for index, model in conflated_ras_models.iterrows():
        try:
            RLOG.lprint("-----------------------------------------------")
            RLOG.trace(f"-- conflated_ras_models index[{index}] - {model.ras_path}")

            model_nwm_streams_ln = nwm_streams_ln[nwm_streams_ln.ras_path == model.ras_path]
            model_cross_section_ln = cross_section_ln[cross_section_ln.ras_path == model.ras_path]

            model_nws_streams_crs_val = model_nwm_streams_ln.crs

            # Load max depth boundary
            hecras_output = Path(unit_output_path, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
            model_output_dir = [
                f for f in hecras_output.iterdir() if re.match(f"^{model.model_id}_", f.name)
            ][0]
            name_mid = model_output_dir.name
            RLOG.lprint(
                f"Creating geo rating curves for model {name_mid}"
                f"  (Number {index + 1} of {len_conflated_ras_models})"
            )

            model_name0 = name_mid.split("_")[1:]
            model_name = "_".join(model_name0)
            model_depths_dir = Path(model_output_dir, model_name)

            inun_shape_files = list(model_depths_dir.glob("Inundation Boundary*.shp"))
            if len(inun_shape_files) == 0:
                RLOG.error(f"Error: model {name_mid} does not have a Inundation Boudary shape file")
                continue

            max_inundation_shp = inun_shape_files[0]

            # Deduce the flow profile number
            flow_search = re.search('\(flow\d*_', max_inundation_shp.name).group()
            max_flow = int(re.search('\d+', flow_search).group())

            discon_inund_poly = gpd.read_file(max_inundation_shp)
            disconnected_inundation_poly = discon_inund_poly.explode(ignore_index=True, index_parts=False)
            model_crs = disconnected_inundation_poly.crs
            main_inundation_poly = disconnected_inundation_poly.iloc[
                disconnected_inundation_poly.length.idxmax()
            ]
            disconnected_inundation_poly = disconnected_inundation_poly.drop(
                index=disconnected_inundation_poly.length.idxmax()
            )

            RLOG.lprint(f"Loading the max inundation extent for each NWM reach for model {name_mid}")

            # One nwm stream might be split via models to multiple seperate geocurve records
            # Create max flow inundation masks for each NWM reach
            nwm_reach_inundation_masks = []
            for index, nwm_reach in model_nwm_streams_ln.iterrows():
                feature_id = nwm_reach.feature_id
                RLOG.trace(f"-- model_nwm_streams_ln index [{index} - {feature_id}")

                nwm_reach = gpd.GeoDataFrame(nwm_reach.to_dict(), index=[0]).set_geometry(
                    'geometry', crs=model_nwm_streams_ln.crs
                )

                # Find boundary cross-sections
                boundary_cross_section_ids = find_boundary_xs(nwm_reach, model_cross_section_ln)
                if not (boundary_cross_section_ids[0] or boundary_cross_section_ids[1]):
                    continue
                boundary_cross_sections_df = model_cross_section_ln.loc[
                    model_cross_section_ln['stream_stn'].isin(boundary_cross_section_ids)
                ]
                boundary_cross_sections_df = boundary_cross_sections_df.assign(
                    feature_id=nwm_reach.feature_id
                )

                # Extend boundary cross-sections because they sometimes don't breach the inundation polygon
                boundary_cross_sections_df.loc[:, 'geometry'] = boundary_cross_sections_df.geometry.apply(
                    lambda row: extend_cross_section(row, xs_extension)
                )

                # Use the first cross-section for the first split
                split1_inundation_geom = split(
                    main_inundation_poly.geometry, boundary_cross_sections_df.geometry.iloc[0]
                )

                if len(split1_inundation_geom.geoms) == 0:
                    RLOG.error(
                        f"Error: model {name_mid}: model_nwm_streams_ln index [{index}"
                        f" - {feature_id}"
                        " has no geometry in split1_inundation_geom. It appears to be"
                        " assuming the first cross section is inside the inundation"
                        " poly which appears to be false."
                        " More research is required."
                    )
                    continue

                split1_inundation = gpd.GeoDataFrame(split1_inundation_geom.geoms)
                split1_inundation = split1_inundation.set_geometry(0, crs=model_crs)
                split1_inundation = split1_inundation.sjoin(nwm_reach)

                # Use the second cross-section for the second split
                if len(split1_inundation) > 0:
                    split2_inundation_geom = split(
                        split1_inundation.geometry.iloc[0], boundary_cross_sections_df.geometry.iloc[1]
                    )
                    split2_inundation = gpd.GeoDataFrame(split2_inundation_geom.geoms)
                    split2_inundation = split2_inundation.set_geometry(0, crs=model_crs)
                    final_inundation_poly = split2_inundation.sjoin(nwm_reach)
                    final_inundation_poly = final_inundation_poly.rename(columns={0: 'geometry'})
                    final_inundation_poly = final_inundation_poly.set_geometry('geometry', crs=model_crs)
                    final_inundation_poly = final_inundation_poly.assign(profile_num=max_flow)

                    # Search for nearby disconnected polygons using a convex hull of the cross-sections
                    search_xs = model_cross_section_ln.loc[
                        (model_cross_section_ln.stream_stn > boundary_cross_sections_df.stream_stn.min())
                        & (model_cross_section_ln.stream_stn < boundary_cross_sections_df.stream_stn.max())
                    ]
                    search_xs = pd.concat([search_xs, boundary_cross_sections_df])
                    search_hull = search_xs.dissolve().geometry.iloc[0].convex_hull
                    search_hull = gpd.GeoDataFrame(
                        {'geometry': [search_hull]}, crs=boundary_cross_sections_df.crs
                    )
                    nearby_polygons = gpd.sjoin(disconnected_inundation_poly, search_hull, how='inner')
                    final_inundation_poly.geometry.iloc[0] = MultiPolygon(
                        [final_inundation_poly.geometry.iloc[0]] + list(nearby_polygons.geometry)
                    )
                    nwm_reach_inundation_masks.append(final_inundation_poly)

            if len(nwm_reach_inundation_masks) == 0:
                RLOG.warning(
                    f" -- nwm_reach_inundation_masks as no records for model {name_mid} : {model.ras_path}"
                )
                continue

            # nwm_reach_inundation_masks at this point is a list, but using pd.concat
            # it is rolling it up to one dataframe which is fed into a geodataframe
            all_nwm_reach_inundation_masks_gdf = gpd.GeoDataFrame(
                pd.concat(nwm_reach_inundation_masks, ignore_index=True)
            )

            depth_tif_list = [f for f in model_depths_dir.iterdir() if f.suffix == '.tif']

            RLOG.lprint("Getting the inundation extents from each flow (depth grids)")

            if len(depth_tif_list) == 0:
                RLOG.error(
                    f"No depth grids were found for {name_mid}:"
                    f" model_nwm_streams_ln index [{index}]:"
                    f" feature id is {feature_id}"
                )
                continue

            depth_tif_list.sort()

            # Use max depth extent polygon as mask for other depths
            RLOG.lprint(f"Number of depth grid tifs to process is {len(depth_tif_list)}")
            print()

            log_file_prefix = "mp_create_geocurves"
            depth_grid_args = []  # list of dictionaries
            for depth_tif in depth_tif_list:
                arg_item = {
                    "depth_tif_win_path": depth_tif,
                    "all_nwm_reach_inundation_masks_gdf": all_nwm_reach_inundation_masks_gdf,
                    "name_mid": name_mid,
                    "unit_output_path": unit_output_path,
                    "code_version": code_version,
                    "unit_name": unit_name,
                    "unit_version": unit_version,
                    "source_code": source_code,
                    "source1": source1,
                    "crs": model_nws_streams_crs_val,
                    "log_file_prefix": log_file_prefix,
                    "rlog_file_path": RLOG.LOG_DEFAULT_FOLDER,
                }
                depth_grid_args.append(arg_item)

            num_processors = round(math.floor(mp.cpu_count() * 0.85))
            # num_processors = 1
            geocurve_df_list = []
            with ProcessPoolExecutor(max_workers=num_processors) as executor:
                with tqdm.tqdm(
                    total=len(depth_grid_args),
                    bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}% ",
                    desc="Processing Depth Grids",
                    ncols=80,
                ) as pbar:
                    futures = {}
                    for idx, dict_args in enumerate(depth_grid_args):
                        future = executor.submit(mp_process_depth_grid_tif, dict_args)
                        futures[future] = idx

                    for future in as_completed(futures):
                        if future is not None:
                            if not future.exception():
                                gc_df = future.result()
                                if len(gc_df) > 0:
                                    geocurve_df_list.append(gc_df)
                            else:
                                raise future.exception()
                        pbar.update(1)  # advance by 1

            # Now that multi-proc is done, lets merge all of the independent log file from each
            RLOG.merge_log_files(RLOG.LOG_FILE_PATH, log_file_prefix)

            if len(geocurve_df_list) == 0:
                RLOG.error(
                    "An internal error has occured."
                    f" No geocurves gdf's were found for {name_mid}:"
                    f" model_nwm_streams_ln index [{index}]:"
                    f" feature id is {nwm_reach.feature_id}"
                )
                continue
            else:
                geocurve_df = gpd.GeoDataFrame(pd.concat(geocurve_df_list, ignore_index=True))

            if len(geocurve_df) == 0:
                RLOG.critical("No geocurves records to be saved. Check for geocurve errors")
                sys.exit(1)

            geocurve_df.crs = model_crs  # not a reproject, just setting it as it did not know
            geocurve_df = geocurve_df.sort_values(by=['feature_id', 'discharge_cfs'])

            # reproject
            # geocurves_new_crs_df = geocurve_df.to_crs(sv.DEFAULT_RASTER_OUTPUT_CRS)

            # This list of features is not automatically same nwm_reach feature id
            # and there can be more than one now.
            for feature_id in geocurve_df.feature_id.unique():
                subset_geocurve_df = geocurve_df.loc[geocurve_df.feature_id == feature_id]

                # ras2inundation needs the first part of the geocurve to be the feature ID
                geocurve_file_name = f"{feature_id}_{name_mid}_geocurve.csv"
                path_geocurve = os.path.join(path_geocurve_folder, geocurve_file_name)
                RLOG.trace(f"Saving: {path_geocurve}")
                subset_geocurve_df.to_csv(path_geocurve, index=False)

        except Exception:
            RLOG.error(f"An error occurred while creating geocurves for {model.final_name_key}")
            RLOG.error(traceback.format_exc())

    # Test to see if any geocurves were created. Each can independenly fail, but if all fail
    # then we have a larger issue
    if len(os.listdir(path_geocurve_folder)) == 0:
        RLOG.critical("No geocurve files were created. Program terminated")
        sys.exit(1)


# -------------------------------------------------
def manage_geo_rating_curves_production(ras2fim_huc_dir, overwrite):
    """
    This function sets up the multiprocessed generation of geo version of feature_id-specific rating curves.

    Args:
        ras2fim_huc_dir (str): Path to HUC8-level directory storing RAS2FIM outputs for a given run.
        output_folder (str): The path to the output folder where geo rating curves will be written.
    """

    # get the version
    changelog_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, 'doc', 'CHANGELOG.md')
    )
    # Get the code version
    code_version = sf.get_changelog_version(changelog_path)

    print()
    RLOG.lprint("+=================================================================+")
    RLOG.notice("|                   Create GeoCurves                              |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    RLOG.lprint(f"  ---(p) ras2fim_huc_dir: {ras2fim_huc_dir}")
    RLOG.lprint(f"  ---(o) overwrite: {overwrite}")

    overall_start_time = datetime.utcnow()
    dt_string = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Started (UTC): {dt_string}")

    # Set up output folders. (final outputs folder now created early in the ras2fim.py lifecycle)
    if not os.path.exists(ras2fim_huc_dir):
        RLOG.error(f"{ras2fim_huc_dir} does not exist")
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), ras2fim_huc_dir)

    # Make geocurves_dir
    geocurves_dir = os.path.join(ras2fim_huc_dir, sv.R2F_OUTPUT_DIR_FINAL, sv.R2F_OUTPUT_DIR_GEOCURVES)

    if os.path.exists(geocurves_dir) and not overwrite:
        RLOG.lprint(
            "The output directory, "
            + geocurves_dir
            + ", already exists. Use the overwrite flag (-o) to overwrite."
        )
        quit()

    if os.path.exists(geocurves_dir):
        shutil.rmtree(geocurves_dir)

    # Either way.. we are making a new geocurve folder. e.g. If it is overwrite, we deleted
    # before replacing it so we don't have left over garbage
    os.makedirs(geocurves_dir)

    # Feed into main geocurve creation function
    create_geocurves(ras2fim_huc_dir, code_version)

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
    # Sample:
    # python create_geocurves.py -p 'c:\ras2fim_data\output_ras2fim\12090301_2277_ble_240216' -o

    parser = argparse.ArgumentParser(description="== Produce Geo Rating Curves for RAS2FIM ==")

    parser.add_argument(
        "-p",
        dest="ras2fim_huc_dir",
        help="REQUIRED: Directory containing RAS2FIM unit output (huc/crs)",
        required=True,
        metavar="STRING",
        type=str,
    )

    parser.add_argument("-o", dest="overwrite", help="Overwrite files", required=False, action="store_true")

    args = vars(parser.parse_args())

    overwrite = args["overwrite"]
    ras2fim_huc_dir = args["ras2fim_huc_dir"]

    log_file_folder = os.path.join(ras2fim_huc_dir, "logs")
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
        manage_geo_rating_curves_production(ras2fim_huc_dir, overwrite)

    except Exception:
        RLOG.critical(traceback.format_exc())
