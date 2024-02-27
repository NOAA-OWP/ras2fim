# ras2fim v2.0
# Uses the 'ras2fim' conda environment

import argparse
import datetime as dt
import multiprocessing as mp
import os
import sys
import traceback

# import warnings
from functools import partial
from multiprocessing import Pool
from time import sleep

import geopandas as gpd

# may need to pip install netcdf4 for xarray
import numpy as np
import pandas as pd
import tqdm
import xarray as xr
from fiona import collection
from geopandas.tools import sjoin
from shapely import wkt
from shapely.geometry import Point, mapping

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG
MP_LOG = ras2fim_logger.RAS2FIM_logger()  # the mp version


# -------------------------------------------------
def fn_wkt_loads(x):
    try:
        return wkt.loads(x)
    except Exception as ex:
        RLOG.error("fn_wkt_loads errored out")
        RLOG.error(x)
        RLOG.error(f"Details: {ex}")
        return None


# -------------------------------------------------
def mp_snap_point(shply_line, list_of_df_row):
    # int_index, int_feature_id, str_huc12, shp_point = list_of_df_row
    int_index, shp_point, int_feature_id, str_huc12 = list_of_df_row

    point_project_wkt = shply_line.interpolate(shply_line.project(shp_point)).wkt

    list_col_names = ["feature_id", "str_huc_12", "geometry_wkt"]
    df = pd.DataFrame([[int_feature_id, str_huc12, point_project_wkt]], columns=list_col_names)

    sleep(0.03)  # this allows the tqdm progress bar to update

    return df


# -------------------------------------------------
def mp_create_gdf_of_points(rlog_file_path, rlog_file_prefix, tpl_request):
    # function to create and return a geoDataframe from a list of shapely points

    # This function is included as part of a multiproc so each process needs to have
    # it's own instance of ras2fim logger.
    # WHY? this stops file open concurrency as each proc has its own.
    # We attempt to keep them somewhat sorted by using YYMMDD_HHMMSECMillecond)

    # global MP
    # MP_LOG = ras2fim_logger.RAS2FIM_logger()

    try:
        file_id = sf.get_date_with_milli()
        log_file_name = f"{rlog_file_prefix}-{file_id}.log"
        MP_LOG.setup(os.path.join(rlog_file_path, log_file_name))

        feature_id = tpl_request[0]
        str_huc_12 = tpl_request[1]
        list_of_points = tpl_request[2]

        MP_LOG.trace(f"feature_id is {feature_id} and huc_12 is {str_huc_12}")

        # Create an empty dataframe
        df_points_nwm = pd.DataFrame(list_of_points, columns=["geometry"])

        # convert dataframe to geodataframeclear
        gdf_points_nwm = gpd.GeoDataFrame(df_points_nwm, geometry="geometry")

        gdf_points_nwm["feature_id"] = feature_id
        gdf_points_nwm["huc_12"] = str_huc_12

    except Exception:
        if ras2fim_logger.LOG_SYSTEM_IS_SETUP is True:
            MP_LOG.critical(traceback.format_exc())
        else:
            print(traceback.format_exc())

        sys.exit(1)

    return gdf_points_nwm


# -------------------------------------------------
def fn_conflate_hecras_to_nwm(huc8, ras_shp_file_dir, conflated_shp_dir, dir_datasets, path_unit_folder):
    # -------------------------------------------------
    # INPUT
    start_dt = dt.datetime.utcnow()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.notice("|        CONFLATE HEC-RAS TO NATIONAL WATER MODEL STREAMS         |")
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("  ---(w) HUC-8: " + huc8)
    RLOG.lprint("  ---(i) HEC-RAS INPUT SHP DIRECTORY: " + ras_shp_file_dir)

    # note the files names are hardcoded in 1 of 2
    ras_stream_ln = os.path.join(ras_shp_file_dir, "stream_LN_from_ras.shp")
    ras_cross_section_ln = os.path.join(ras_shp_file_dir, "cross_section_LN_from_ras.shp")

    RLOG.lprint("  ---(o) OUTPUT DIRECTORY: " + conflated_shp_dir)
    RLOG.lprint("  ---(n) NATIONAL DATASET LOCATION: " + dir_datasets)

    # Input - projection of the base level engineering models
    # get this string from the input shapefiles of the stream
    gdf_stream = gpd.read_file(ras_stream_ln)
    ras_prj = str(gdf_stream.crs)

    # Note that this routine requires three (3) national datasets.
    # (1) the NHD Watershed Boundary dataset
    # (2) the National water model flowlines geopackage
    # (3) the National water model recurrance flows

    str_wbd_geopkg_path = os.path.join(dir_datasets, sv.INPUT_WBD_NATIONAL_FILE)

    # Input - National Water Model stream lines geopackage
    str_nwm_flowline_geopkg_path = os.path.join(dir_datasets, sv.INPUT_NWM_FLOWS_FILE)

    # Input - Recurrance Intervals netCDF
    str_netcdf_path = os.path.join(dir_datasets, sv.INPUT_NWM_WBD_LOOKUP_FILE)

    # Geospatial projections
    nwm_prj = "ESRI:102039"

    # Load the geopackage into geodataframe - 1 minute +/-
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Loading Watershed Boundary Dataset ~ 20 sec")

    INPUT_WBD_HUC8_DIR = "WBD_HUC8"  # Pattern for huc files are 'HUC8_{huc number}.gpkg'

    # Use the HUC8 small vector to mask the large full WBD_Nation.gpkg.
    # This is much faster
    wdb_huc8_file = os.path.join(dir_datasets, INPUT_WBD_HUC8_DIR, f"HUC8_{huc8}.gpkg")
    huc8_wbd_db = gpd.read_file(wdb_huc8_file)
    gdf_ndgplusv21_wbd = gpd.read_file(str_wbd_geopkg_path, mask=huc8_wbd_db)

    list_huc8 = []
    list_huc8.append(huc8)

    # get only the polygons in the given HUC_8
    gdf_huc8_only = gdf_ndgplusv21_wbd.query("HUC_8==@list_huc8")

    gdf_huc8_only_nwm_prj = gdf_huc8_only.to_crs(nwm_prj)
    gdf_huc8_only_ras_prj = gdf_huc8_only.to_crs(ras_prj)

    # path of the shapefile to write
    str_huc8_filepath = os.path.join(conflated_shp_dir, f"{huc8}_huc_12_ar.shp")

    # -------------------------------------------------
    # Overlay the BLE streams (from the HEC-RAS models) to the HUC_12 shapefile

    # read the ble streams
    gdf_ras_streams = gpd.read_file(ras_stream_ln)

    # clip the BLE streams to the watersheds (HUC-12)
    gdf_ras_streams_intersect = gpd.overlay(gdf_ras_streams, gdf_huc8_only_ras_prj, how="intersection")

    # path of the shapefile to write
    str_filepath_ras_stream = os.path.join(conflated_shp_dir, f"{huc8}_ras_streams_ln.shp")

    # write the shapefile
    gdf_ras_streams_intersect.to_file(str_filepath_ras_stream)

    # -------------------------------------------------
    # Shapefiles of HUC_12s that have streams (provides shape files of huc12)
    # create an array of HUC_12 watersheds that have streams within them
    arr_huc_12_only_with_stream = gdf_ras_streams_intersect.HUC_12.unique()

    # convert the array to a pandas dataframe
    df_huc_12_only_with_stream = pd.DataFrame(arr_huc_12_only_with_stream, columns=["HUC_12"])

    # merge the dataframe and geodataframe to get only polygons that have streams
    gdf_huc_12_only_with_stream = pd.merge(
        gdf_huc8_only_ras_prj, df_huc_12_only_with_stream, on="HUC_12", how="inner"
    )

    # write the area watershed shapefile
    gdf_huc_12_only_with_stream.to_file(str_huc8_filepath)

    # -------------------------------------------------
    # Get the NWM stream centerlines from the provided geopackage

    # Union of the HUC-12 geodataframe - creates shapely polygon
    shp_huc8_union_nwm_prj = gdf_huc8_only_nwm_prj.geometry.unary_union

    # Create dataframe of the bounding coordiantes
    tuple_watershed_extents = shp_huc8_union_nwm_prj.bounds

    # Read Geopackage with bounding box filter
    gdf_stream = gpd.read_file(str_nwm_flowline_geopkg_path, bbox=tuple_watershed_extents)

    # reanme ID to feature_id
    gdf_stream = gdf_stream.rename(columns={"ID": "feature_id"})

    # -------------------------------------------------
    # Load the netCDF file to pandas dataframe - 15 seconds
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Loading the National Water Model Recurrence Flows ~ 15 sec")

    ds = xr.open_dataset(str_netcdf_path)
    df_all_nwm_streams = ds.to_dataframe()

    # get netCDF (recurrance interval) list of streams in the given huc
    df_streams_huc_only = df_all_nwm_streams.query("huc8==@list_huc8")

    # left join the recurrance stream table (dataFrame) with streams in watershed
    # this will remove the streams not within the HUC-8 boundary
    df_streams_huc_only = df_streams_huc_only.merge(gdf_stream, on="feature_id", how="left")

    # Convert the left-joined dataframe to a geoDataFrame
    gdf_streams_huc_only = gpd.GeoDataFrame(df_streams_huc_only, geometry=df_streams_huc_only["geometry"])

    # Set the crs of the new geodataframe
    gdf_streams_huc_only.crs = gdf_stream.crs

    # project the nwm streams to the ble projecion
    gdf_streams_nwm_bleprj = gdf_streams_huc_only.to_crs(ras_prj)

    # -------------------------------------------------
    # Create points at desired interval along each
    # national water model stream
    # -------------------------------------------------
    # TODO - 2021.09.21 - this should be 50 if meters and 150 if feet
    # too small a value creates long buffering times
    int_distance_delta = 150  # distance between points in hec-ras projection units

    int_count = 0

    # Multi-Linestrings to Linestrings
    gdf_streams_nwm_explode = gdf_streams_nwm_bleprj.explode(index_parts=True)

    # TODO - 2021.08.03 - Quicker to buffer the ras_streams first
    # and get the nwm streams that are inside or touch the buffer?

    list_points_aggregate = []
    RLOG.lprint("+-----------------------------------------------------------------+")

    for index, row in gdf_streams_nwm_explode.iterrows():
        str_current_linestring = row["geometry"]
        distances = np.arange(0, str_current_linestring.length, int_distance_delta)
        inter_distances = [str_current_linestring.interpolate(distance) for distance in distances]
        boundary_point = Point(
            str_current_linestring.boundary.bounds[0], str_current_linestring.boundary.bounds[1]
        )
        inter_distances.append(boundary_point)

        tpl_request = (row["feature_id"], row["huc12"], inter_distances)
        list_points_aggregate.append(tpl_request)

    # Create a pool of processors
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("start of creating gdf of points")

    # -------------------------------------------------
    # A "partial" just extends the original function to add extra params on the fly
    # e.g. The original mp_create_gdf_of_points has only sorted_list_points_aggregate being
    # passed in. Now, but adding a partial, the args beign passed into mp_create_gdf_of_points
    # are RLOG.LOG_DEFAULT_FOLDER, log_file_prefix, sorted_list_points_aggregate
    # Notice.. the partial gets a temp name that gets passed into the function in the pool
    log_file_prefix = "mp_create_gdf_of_points"
    fn_create_gdf_of_points_partial = partial(
        mp_create_gdf_of_points, RLOG.LOG_DEFAULT_FOLDER, log_file_prefix
    )
    num_processors = mp.cpu_count() - 2
    with Pool(processes=num_processors) as executor:
        # TODO .. not how to sort as it is a list of tuples
        sorted_list_points_aggregate = sorted(list_points_aggregate)

        len_points_agg = len(sorted_list_points_aggregate)

        list_gdf_points_all_lines = list(
            tqdm.tqdm(
                executor.imap(fn_create_gdf_of_points_partial, sorted_list_points_aggregate),
                total=len_points_agg,
                desc="Points on lines",
                bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
                ncols=67,
            )
        )

    # Now that multi-proc is done, lets merge all of the independent log file from each
    RLOG.merge_log_files(RLOG.LOG_FILE_PATH, log_file_prefix)

    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Mapping points to nwm streams")
    gdf_points_nwm = gpd.GeoDataFrame(pd.concat(list_gdf_points_all_lines, ignore_index=True))
    gdf_points_nwm = gdf_points_nwm.set_crs(ras_prj)

    # -------------------------------------------------
    # Read in the model stream shapefile
    gdf_segments = gpd.read_file(ras_stream_ln)  # gdf_conflate_streams_ras_to_nwm_fid_line.copy()

    # Simplify geom by 4.5 tolerance and rewrite the
    # geom to eliminate streams with too may verticies
    flt_tolerance = 4.5  # tolerance for simplifcation of HEC-RAS stream centerlines

    for index, row in gdf_segments.iterrows():
        shp_geom = row["geometry"]
        shp_simplified_line = shp_geom.simplify(flt_tolerance, preserve_topology=False)
        gdf_segments.at[index, "geometry"] = shp_simplified_line

    # create merged geometry of all streams
    shply_line = gdf_segments.geometry.unary_union

    # read in the national water model points
    gdf_points = gdf_points_nwm

    # reproject the points
    gdf_points = gdf_points.to_crs(gdf_segments.crs)

    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Buffering stream centerlines ~ 60 sec")

    # -------------------------------------------------
    # buffer the merged stream ceterlines - distance to find valid conflation point
    # -------------------------------------------------
    # distance to buffer around modeled stream centerlines
    int_buffer_dist = 600
    shp_buff = shply_line.buffer(int_buffer_dist)

    # convert shapely to geoDataFrame
    gdf_buff = gpd.GeoDataFrame(geometry=[shp_buff])

    # set the CRS of buff
    gdf_buff = gdf_buff.set_crs(gdf_segments.crs)

    # spatial join - points in polygon
    gdf_points_in_poly = sjoin(gdf_points, gdf_buff, how="left")

    # drop all points that are not within polygon
    gdf_points_within_buffer = gdf_points_in_poly.dropna()

    # need to reindex the returned geoDataFrame
    gdf_points_within_buffer = gdf_points_within_buffer.reset_index()

    # delete the index_right field
    del gdf_points_within_buffer["index_right"]

    total_points = len(gdf_points_within_buffer)

    df_points_within_buffer = pd.DataFrame(gdf_points_within_buffer)

    list_dataframe_args_snap = df_points_within_buffer.values.tolist()

    RLOG.lprint("+-----------------------------------------------------------------+")

    with Pool(processes=(mp.cpu_count() - 2)) as executor:
        list_df_points_projected = list(
            tqdm.tqdm(
                executor.imap(partial(mp_snap_point, shply_line), list_dataframe_args_snap),
                total=total_points,
                desc="Snap Points",
                bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
                miniters=int(total_points/1000),
                ncols=67,
            )
        )
    print()

    gdf_points_snap_to_ble = gpd.GeoDataFrame(pd.concat(list_df_points_projected, ignore_index=True))

    gdf_points_snap_to_ble["geometry"] = gdf_points_snap_to_ble.geometry_wkt.apply(fn_wkt_loads)
    gdf_points_snap_to_ble = gdf_points_snap_to_ble.dropna(subset=["geometry"])
    gdf_points_snap_to_ble = gdf_points_snap_to_ble.set_crs(gdf_segments.crs)

    # write the shapefile
    # str_filepath_ras_points = os.path.join(shp_out_path, f"{str_huc8}_ras_snap_points_PT.shp")

    # TODO: Nov 1, 2023. Somewhere in here is a bug?
    # gdf_points_snap_to_ble.to_file(str_filepath_ras_points)

    # -------------------------------------------------
    # Buffer the Base Level Engineering streams 0.1 feet (line to polygon)
    gdf_segments_buffer = gdf_segments
    gdf_segments["geometry"] = gdf_segments_buffer.geometry.buffer(0.1)

    # Spatial join of the points and buffered stream
    gdf_ras_points_feature_id = gpd.sjoin(
        gdf_points_snap_to_ble,
        gdf_segments_buffer[["geometry", "ras_path"]],
        how="left",
        predicate="intersects",
    )

    # delete the wkt_geom field
    del gdf_ras_points_feature_id["index_right"]

    # Intialize the variable
    gdf_ras_points_feature_id["count"] = 1

    df_ras_guess = pd.pivot_table(
        gdf_ras_points_feature_id, index=["feature_id", "ras_path"], values=["count"], aggfunc=np.sum
    )

    df_test = df_ras_guess.sort_values("count")

    str_csv_file = os.path.join(conflated_shp_dir, f"{huc8}_interim_list_of_streams.csv")

    # Write out the table - read back in
    # this is to white wash the data type
    df_test.to_csv(str_csv_file)
    df_test = pd.read_csv(str_csv_file)

    # Remove the duplicates and determine the feature_id with the highest count
    df_test = df_test.drop_duplicates(subset="feature_id", keep="last")

    # Left join the nwm shapefile and the
    # feature_id/ras_path dataframe on the feature_id

    # When we merge df_test into gdf_streams_nwm_bleprj (which does not have a ras_path colum)
    # the new gdf_nwm_stream_raspath does have the column of ras_path but it is all NaN
    # So, we flipped it for gdf_streams_nwm_bleprj into df_test and it fixed it
    df_nwm_stream_raspath = df_test.merge(gdf_streams_nwm_bleprj, on="feature_id", how="left")

    # We need to convert it back to a geodataframe for next steps (and exporting)
    gdf_nwm_stream_raspath = gpd.GeoDataFrame(df_nwm_stream_raspath)

    # path of the shapefile to write
    str_filepath_nwm_stream = os.path.join(conflated_shp_dir, f"{huc8}_nwm_streams_ln.shp")

    # write the shapefile
    gdf_nwm_stream_raspath.to_file(str_filepath_nwm_stream)

    # -------------------------------------------------
    # Read in the ble cross section shapefile
    gdf_ras_cross_sections = gpd.read_file(ras_cross_section_ln)

    str_xs_on_feature_id_pt = os.path.join(conflated_shp_dir, f"{huc8}_nwm_points_on_xs_PT.shp")

    # Create new dataframe
    column_names = ["feature_id", "river", "reach", "us_xs", "ds_xs", "peak_flow", "ras_path"]

    df_summary_data = pd.DataFrame(columns=column_names)

    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Determining conflated stream centerlines")

    # Loop through all feature_ids in the provided
    # National Water Model stream shapefile
    int_count = len(gdf_nwm_stream_raspath)
    for i in range(int_count):
        str_feature_id = gdf_nwm_stream_raspath.loc[[i], "feature_id"].values[0]
        str_ras_path = gdf_nwm_stream_raspath.loc[[i], "ras_path"].values[0]

        # get the NWM stream geometry
        df_stream = gdf_nwm_stream_raspath.loc[[i], "geometry"]

        # Select all cross sections in new dataframe where WTR_NM = strras_Name
        df_xs = gdf_ras_cross_sections.loc[gdf_ras_cross_sections["ras_path"] == str_ras_path]

        # Creates a set of points where the streams intersect
        points = df_stream.unary_union.intersection(df_xs.unary_union)

        if points.geom_type == "MultiPoint":
            # Create a shapefile of the intersected points

            schema = {"geometry": "Point", "properties": {}}

            with collection(str_xs_on_feature_id_pt, "w", "ESRI Shapefile", schema, crs=ras_prj) as output:
                # Slice ras_prj to remove the "epsg:"
                for i in points.geoms:
                    output.write({"properties": {}, "geometry": mapping(Point(i.x, i.y))})

            df_points = gpd.read_file(str_xs_on_feature_id_pt)

            # SettingWithCopyWarning
            df_xs["geometry"] = df_xs.geometry.buffer(0.1).copy()

            df_point_feature_id = gpd.sjoin(
                df_points,
                df_xs[["geometry", "max_flow", "stream_stn", "river", "reach"]],
                how="left",
                predicate="intersects",
            )

            # determine Maximum and Minimum stream station
            flt_us_xs = df_point_feature_id["stream_stn"].max()
            flt_ds_xs = df_point_feature_id["stream_stn"].min()

            # determine the peak flow with this stream station limits
            flt_max_q = df_point_feature_id["max_flow"].max()

            str_river = df_point_feature_id["river"].values[0]
            str_reach = df_point_feature_id["reach"].values[0]

            new_rec = {
                "feature_id": str_feature_id,
                "river": str_river,
                "reach": str_reach,
                "us_xs": flt_us_xs,
                "ds_xs": flt_ds_xs,
                "peak_flow": flt_max_q,
                "ras_path": str_ras_path,
            }

            df_new_row = pd.DataFrame.from_records([new_rec])
            df_summary_data = pd.concat([df_summary_data, df_new_row], ignore_index=True)

    # Creates a summary documents
    # Check to see if matching model is found
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint("Creating Quality Control Output")

    str_qc_csv_File = os.path.join(conflated_shp_dir, f"{huc8}_stream_qc_fid_xs.csv")
    df_summary_data.to_csv(str_qc_csv_File, index=False)

    RLOG.lprint("Number of feature_id's matched: " + str(df_summary_data["feature_id"].nunique()))

    # -------------------------------------------------
    # Reads the model_ids from model catalog and
    # save the conflated ras to nwm streams paths
    # -------------------------------------------------
    df_conflated_ras_models = df_summary_data.sort_values('feature_id', ascending=False).drop_duplicates(
        subset=['ras_path']
    )["ras_path"]

    path_model_catalog = os.path.join(path_unit_folder, "OWP_ras_models_catalog_" + huc8 + ".csv")

    if os.path.exists(path_model_catalog) is False:
        raise ValueError(f"The unit model catalog of {path_model_catalog} does not exist and is required")

    model_catalog = pd.read_csv(path_model_catalog)

    models_name_id = pd.concat([model_catalog["final_name_key"], model_catalog["model_id"]], axis=1)
    final_name_key = list(models_name_id["final_name_key"])

    path_conflated_models_splt = [path.split("\\") for path in list(df_conflated_ras_models)]
    conflated_model_names = [names[-2] for names in path_conflated_models_splt]

    conflated_model_names_id = []
    for nms in conflated_model_names:
        if nms not in final_name_key:
            raise Exception(
                f"Model name of {nms} not found in the models catalog of" f" {path_model_catalog}."
            )

        indx = final_name_key.index(nms)

        name_id = list(models_name_id.iloc[indx])

        conflated_model_names_id.append(name_id)

    conflated_model_names_id_df = pd.DataFrame(
        conflated_model_names_id, columns=["final_name_key", "model_id"]
    )

    df_conflated_ras_models.index = range(len(conflated_model_names_id_df))
    df_conflated_ras_models_path = pd.concat([df_conflated_ras_models, conflated_model_names_id_df], axis=1)

    if len(df_conflated_ras_models_path) > 0:
        conflated_rasmodels_path = os.path.join(conflated_shp_dir, "conflated_ras_models.csv")
        df_conflated_ras_models_path.to_csv(conflated_rasmodels_path)

        RLOG.lprint("")
        RLOG.success("COMPLETE")

        dur_msg = sf.get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        RLOG.lprint("+=================================================================+")

    else:
        RLOG.lprint("")
        RLOG.critical("NO CONFLATED RAS MODELS FOUND")

        dur_msg = sf.get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        RLOG.lprint("+=================================================================+")

        sys.exit(1)


# -------------------------------------------------
if __name__ == "__main__":
    # Sample:
    # python conflate_hecras_to_nwm -w 12090301
    # -i 'c:\\ras2fim_data\\output_ras2fim\\12090301_2277_230821\\01_shapes_from_hecras'
    # -o 'c:\\ras2fim_data\\output_ras2fim\\12090301_2277_230821\\02_csv_from_conflation'
    # -n 'c:\\ras2fim_data\\inputs\\X-National_Datasets'
    # -mc 'C:\\ras2fim_data\\OWP_ras_models\\ras2fimv2.0\\ras2fim_v2_output_12090301'

    parser = argparse.ArgumentParser(
        description="===== CONFLATE HEC-RAS TO NATIONAL WATER MODEL STREAMS ====="
    )

    parser.add_argument(
        "-w",
        dest="str_huc8",
        help="REQUIRED: HUC-8 watershed that is being evaluated: Example: 12090301",
        required=True,
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-i",
        dest="str_shp_in_arg",
        help=r"REQUIRED: Directory containing stream and cross section shapefiles:  Example: D:\ras_shapes",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-p",
        dest="path_unit_folder",
        help=r"REQUIRED: Directory containing model catalog for HUC8:  Example: D:\12090301_2277_20231214",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="str_shp_out_arg",
        help=r"REQUIRED: path to write output files: Example: D:\conflation_output",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-n",
        dest="str_nation_arg",
        help=r"Optional: path to national datasets: Example: E:\X-NWS\X-National_Datasets",
        required=False,
        default=sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
        metavar="DIR",
        type=str,
    )

    args = vars(parser.parse_args())

    str_huc8 = args["str_huc8"]
    str_shp_in_arg = args["str_shp_in_arg"]
    path_unit_folder = args["path_unit_folder"]
    str_shp_out_arg = args["str_shp_out_arg"]
    str_nation_arg = args["str_nation_arg"]

    log_file_folder = os.path.join(path_unit_folder, "logs")
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
        fn_conflate_hecras_to_nwm(str_huc8, str_shp_in_arg, str_shp_out_arg, str_nation_arg, path_unit_folder)

    except Exception:
        RLOG.critical(traceback.format_exc())
