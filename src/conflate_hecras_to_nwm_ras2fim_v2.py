# This is the first pre-processing tool that takes the HEC-RAS models
# in a given directory and creates attributed shapefiles of the stream
# centerline and cross sections
#
# ras2fim v2.0
# Uses the 'ras2fim' conda environment

import argparse
import datetime
import os.path
import time
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import LineString, Point


# -------------------------------------------------
def fn_cut_streams_in_two(line, distance):
    # Cuts a line in two at a distance from its starting point
    # Cut end of the ble streams to avoid conflation with downstream nwm streams
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i2, pi2 in enumerate(coords):
        pdl = line.project(Point(pi2))
        if pdl == distance:
            return [LineString(coords[: i2 + 1]), LineString(coords[i2:])]
        if pdl > distance:
            cpl = line.interpolate(distance)
            return [LineString(coords[:i2] + [(cpl.x, cpl.y)]), LineString([(cpl.x, cpl.y)] + coords[i2:])]


# -------------------------------------------------
# str_ras_path_arg = "C:/ras2fim_data/OWP_ras_models/ras2fimv2.0/excluded_models"
# str_shp_out_arg = "C:/ras2fim_data/OWP_ras_models/ras2fimv2.0/v1_step1_output_excluded"
# str_crs_arg = "EPSG:2277"
# fn_create_shapes_from_hecras(str_ras_path_arg, str_shp_out_arg, str_crs_arg)

# -------------------------------------------------
# str_huc8_arg = "12090301"
# str_shp_in_arg = "C:/ras2fim_data/OWP_ras_models/ras2fimv2.0/step1_outputs_models"
# str_shp_out_arg = "C:/ras2fim_data/OWP_ras_models/ras2fimv2.0/step1_outputs_models"
# str_nation_arg = "C:/ras2fim_data/inputs/X-National_Datasets"

# -------------------------------------------------
def fn_conflate_hecras_to_nwm(str_huc8_arg, str_shp_in_arg, str_shp_out_arg, str_nation_arg):
    # supress all warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    flt_start_conflate_hecras_to_nwm = time.time()

    print(" ")
    print("+=================================================================+")
    print("|        CONFLATE HEC-RAS TO NATIONAL WATER MODEL STREAMS         |")
    print("+-----------------------------------------------------------------+")

    str_huc8 = str_huc8_arg
    print("  ---(w) HUC-8: " + str_huc8)

    str_ble_shp_dir = str_shp_in_arg
    print("  ---(i) HEC-RAS INPUT SHP DIRECTORY: " + str_ble_shp_dir)

    # note the files names are hardcoded
    str_ble_stream_ln = str_ble_shp_dir + "\\" + "stream_LN_from_ras.shp"

    STR_OUT_PATH = str_shp_out_arg
    print("  ---(o) OUTPUT DIRECTORY: " + STR_OUT_PATH)

    str_national_dataset_path = str_nation_arg
    print("  ---(n) NATIONAL DATASET LOCATION: " + str_national_dataset_path)

    # Input - projection of the base level engineering (BLE) models
    # Get this string from the input csv of the stream
    gdf_stream = gpd.read_file(str_ble_stream_ln)
    ble_prj = str(gdf_stream.crs)

    # Note that this routine requires three (3) national datasets.
    # (1) the NHD Watershed Boundary dataset
    # (2) the National water model flowlines geopackage
    # (3) the National water model recurrance flows

    INPUT_NWM_FLOWS_FILE = "nwm_flows.gpkg"
    INPUT_NWM_WBD_LOOKUP_FILE = "nwm_wbd_lookup.nc"
    INPUT_WBD_NATIONAL_FILE = "WBD_National.gpkg"
    # Input - Watershed boundary data geopackage
    str_wbd_geopkg_path = str_national_dataset_path + "\\" + INPUT_WBD_NATIONAL_FILE

    # Input - National Water Model stream lines geopackage
    str_nwm_flowline_geopkg_path = str_national_dataset_path + "\\" + INPUT_NWM_FLOWS_FILE

    # Input - Recurrance Intervals netCDF
    str_netcdf_path = str_national_dataset_path + "\\" + INPUT_NWM_WBD_LOOKUP_FILE

    # Geospatial projections
    nwm_prj = "ESRI:102039"

    # Load the geopackage into geodataframe - 1 minute +/-
    print("+-----------------------------------------------------------------+")
    print("Loading Watershed Boundary Dataset ~ 20 sec")

    INPUT_WBD_HUC8_DIR = "WBD_HUC8"  # Pattern for huc files are 'HUC8_{huc number}.gpkg'

    # Use the HUC8 small vector to mask the large full WBD_Nation.gpkg.
    wdb_huc8_file = os.path.join(str_nation_arg, INPUT_WBD_HUC8_DIR, f"HUC8_{str_huc8}.gpkg")
    huc8_wbd_db = gpd.read_file(wdb_huc8_file)
    gdf_ndgplusv21_wbd = gpd.read_file(str_wbd_geopkg_path, mask=huc8_wbd_db)

    list_huc8 = []
    list_huc8.append(str_huc8)

    # Get only the polygons in the given HUC_8
    gdf_huc8_only = gdf_ndgplusv21_wbd.query("HUC_8==@list_huc8")

    gdf_huc8_only_nwm_prj = gdf_huc8_only.to_crs(nwm_prj)

    gdf_huc8_only_ble_prj = gdf_huc8_only.to_crs(ble_prj)

    # Path of the shapefile to write
    str_huc8_filepath = os.path.join(STR_OUT_PATH, f"{str_huc8}_huc_12_ar.shp")

    # -------------------------------------------------
    # Overlay the BLE streams (from the HEC-RAS models) to the HUC_12 shapefile

    # read the ble streams
    gdf_ble_streams = gpd.read_file(str_ble_stream_ln)

    # clip the BLE streams to the watersheds (HUC-12)
    gdf_ble_streams_intersect = gpd.overlay(gdf_ble_streams, gdf_huc8_only_ble_prj, how="intersection")

    # path of the shapefile to write
    str_filepath_ble_stream = os.path.join(STR_OUT_PATH, f"{str_huc8}_ble_streams_ln.shp")

    # write the shapefile
    gdf_ble_streams_intersect.to_file(str_filepath_ble_stream)

    # -------------------------------------------------
    # Shapefiles of HUC_12s that have streams (provides shape files of huc12)
    # create an array of HUC_12 watersheds that have streams within them
    arr_huc_12_only_with_stream = gdf_ble_streams_intersect.HUC_12.unique()

    # convert the array to a pandas dataframe
    df_huc_12_only_with_stream = pd.DataFrame(arr_huc_12_only_with_stream, columns=["HUC_12"])

    # merge the dataframe and geodataframe to get only polygons that have streams
    gdf_huc_12_only_with_stream = pd.merge(
        gdf_huc8_only_ble_prj, df_huc_12_only_with_stream, on="HUC_12", how="inner"
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

    # Load the netCDF file to pandas dataframe - 15 seconds
    print("+-----------------------------------------------------------------+")
    print("Loading the National Water Model Recurrence Flows ~ 15 sec")
    ds = xr.open_dataset(str_netcdf_path)
    df_all_nwm_streams = ds.to_dataframe()

    # Get netCDF (recurrance interval) list of streams in the given huc
    df_streams_huc_only = df_all_nwm_streams.query("huc8==@list_huc8")

    # Left join the recurrance stream table (dataFrame) with streams in watershed
    # this will remove the streams not within the HUC-8 boundary
    df_streams_huc_only = df_streams_huc_only.merge(gdf_stream, on="feature_id", how="left")

    # Convert the left-joined dataframe to a geoDataFrame
    gdf_streams_huc_only = gpd.GeoDataFrame(df_streams_huc_only, geometry=df_streams_huc_only["geometry"])

    # Set the crs of the new geodataframe
    gdf_streams_huc_only.crs = gdf_stream.crs

    # project the nwm streams to the ble projecion
    gdf_streams_nwm_bleprj = gdf_streams_huc_only.to_crs(ble_prj)

    # -------------------------------------------------
    print("Buffering stream centerlines ~ 120 sec")
    # Make a buffer around streams_nwm (create a polygone)

    # too small a value creates long buffering times
    nwm_buffer = 50  # ft
    ras_buffer = 150  # ft

    streams_nwm_bleprj_buf = gdf_streams_nwm_bleprj.buffer(nwm_buffer)
    streams_nwm_bleprj_buf_geom = streams_nwm_bleprj_buf.copy()
    gdf_streams_nwm_bleprj_buf = gdf_streams_nwm_bleprj.copy()
    gdf_streams_nwm_bleprj_buf.geometry = streams_nwm_bleprj_buf_geom

    # gdf_streams_nwm_bleprj_buf.to_file(
    #     str_shp_out_arg + "//" + "gdf_streams_nwm_bleprj_buf.shp")

    # -------------------------------------------------
    # Make a buffer around ras_ble_streams (create a polygone)
    # Explode MultiLineString geometry to LineString geometry
    gdf_ble_streams_intersect_exp = gdf_ble_streams_intersect.explode(index_parts=True, ignore_index=True)

    # Excluding streams with length less that 500 ft
    distance_delta = 500  # ft

    ble_streams_intersect_len = gdf_ble_streams_intersect_exp.length

    sln_indx = []
    for sln in range(len(ble_streams_intersect_len)):
        if ble_streams_intersect_len[sln] > distance_delta:
            sln_indx.append(sln)

    # Ble streams with length latger than distance_delta (500 ft)
    gdf_ble_streams_intersect_ldd = gdf_ble_streams_intersect_exp.iloc[sln_indx]
    gdf_ble_streams_intersect_ldd.index = range(len(gdf_ble_streams_intersect_ldd))

    # Generate the equidistant points
    distances = [np.arange(0, length, distance_delta) for length in gdf_ble_streams_intersect_ldd.length]

    list_ble_streams_cut_geo = []
    for dc in range(len(distances)):
        ligne = gdf_ble_streams_intersect_ldd.iloc[dc].geometry

        # Cut end of the ble streams to avoid conflation with downstream nwm streams
        # geometry collection format
        if len(distances[dc]) >= 4:
            ligne_cut = fn_cut_streams_in_two(ligne, distances[dc][-3])

        if len(distances[dc]) == 3:
            ligne_cut = fn_cut_streams_in_two(ligne, distances[dc][-2])

        if len(distances[dc]) == 2:
            ligne_cut = fn_cut_streams_in_two(ligne, distances[dc][-1])

        list_ble_streams_cut_geo.append(ligne_cut[0])

    # Replacing gdf_ble_streams_intersect geometry with shorter streams geometry
    gdf_ble_streams_intersect_ldd_cut = gdf_ble_streams_intersect_ldd.copy()
    gdf_ble_streams_intersect_ldd_cut.geometry = list_ble_streams_cut_geo

    # Make a buffer around ble_streams
    ble_streams_intersect_buf = gdf_ble_streams_intersect_ldd_cut.buffer(ras_buffer)
    ble_streams_intersect_buf_geom = ble_streams_intersect_buf.copy()
    gdf_ble_streams_intersect_buf = gdf_ble_streams_intersect_ldd_cut.copy()
    gdf_ble_streams_intersect_buf.geometry = ble_streams_intersect_buf_geom

    # gdf_ble_streams_intersect_buf.to_file(
    #     str_shp_out_arg + "//" + "gdf_ble_streams_intersect_buf_cut.shp")

    # -------------------------------------------------
    # Conflate ble streams to nwm streams
    print("Determining conflated stream centerlines")
    gdf_conflate_streams_ble_to_nwm_dup = gpd.overlay(
        gdf_streams_nwm_bleprj_buf, gdf_ble_streams_intersect_buf, how='intersection'
    )

    # gdf_conflate_streams_ble_to_nwm_dup.to_file(
    #     str_shp_out_arg + "//" + "gdf_conflate_streams_ble_to_nwm_cut_dup.shp")
    # gdf_conflate_streams_ble_to_nwm_dup.to_csv(
    #     str_shp_out_arg + "//" + "gdf_conflate_streams_ble_to_nwm_cut_dup.csv")

    # -------------------------------------------------
    # Remove duplicate ras models
    gdf_conflate_streams_ble_to_nwm_filter1 = gdf_conflate_streams_ble_to_nwm_dup.sort_values(
        'Length', ascending=False
    ).drop_duplicates(subset=['ras_path'])
    gdf_conflate_streams_ble_to_nwm_filter1.index = range(len(gdf_conflate_streams_ble_to_nwm_filter1))

    # Removing duplicate ras models from rrassler
    # Finding duplicate based on subset= ['river','feature_id'] does not work.
    ras_path_split = gdf_conflate_streams_ble_to_nwm_filter1['ras_path'].str.split("\\")

    list_model_names_split = []
    for splt in range(len(ras_path_split)):
        model_name = [
            ras_path_split[splt],
            ras_path_split[splt][0],
            ras_path_split[splt][1][0:-11],
            ras_path_split[splt][2],
        ]
        list_model_names_split.append(model_name)

    df_ras_path_split_dup = pd.DataFrame(list_model_names_split)
    df_ras_path_split_dup.columns = ['A', 'B', 'C', 'D']

    gdf_conflate_streams_ble_to_nwm_filter2 = gdf_conflate_streams_ble_to_nwm_filter1.assign(
        duplic_finder=df_ras_path_split_dup['C']
    )

    gdf_conflate_streams_ble_to_nwm = gdf_conflate_streams_ble_to_nwm_filter2.drop_duplicates(
        subset='duplic_finder'
    )
    gdf_conflate_streams_ble_to_nwm.index = range(len(gdf_conflate_streams_ble_to_nwm))
    # gdf_conflate_streams_ble_to_nwm.to_csv(
    #     str_shp_out_arg + "//" + "gdf_conflate_streams_ble_to_nwm.csv")
    # gdf_conflate_streams_ble_to_nwm.to_file(
    #     str_shp_out_arg + "//" + "gdf_conflate_streams_ble_to_nwm.shp")

    # -------------------------------------------------
    # Exporting the final ras models to a csv file
    path_to_ras_models_4step5 = gdf_conflate_streams_ble_to_nwm['ras_path']
    path_to_ras_models_4step5.to_csv(str_shp_out_arg + "//" + "path_to_ras_models_4step5.csv")

    # -------------------------------------------------
    # Finding streams that work with these final ras models
    # Determining the length of the final streams dataframe
    streams_same_ras = []
    streams_counter = 0
    for p2rm in range(len(path_to_ras_models_4step5)):
        ble_stream_conflated = gdf_ble_streams_intersect_exp[
            gdf_ble_streams_intersect_exp['ras_path'] == path_to_ras_models_4step5[p2rm]
        ]

        streams_same_ras.append(len(ble_stream_conflated))
        streams_counter += len(ble_stream_conflated)

        len_gdf = streams_counter

    # Create empty GeoDataFrame
    gdf_ble_streams_conflated = gpd.GeoDataFrame(
        columns=gdf_ble_streams_intersect_exp.columns, index=range(len_gdf)
    )

    # Creating the final streams dataframe
    index_counter = 0
    for p2rm in range(len(path_to_ras_models_4step5)):
        ble_streams_conflated = gdf_ble_streams_intersect_exp[
            gdf_ble_streams_intersect_exp['ras_path'] == path_to_ras_models_4step5[p2rm]
        ]

        index_range = range(index_counter, index_counter + len(ble_streams_conflated))

        ble_streams_conflated.set_index(pd.Index(index_range), inplace=True)

        gdf_ble_streams_conflated.iloc[index_range] = ble_streams_conflated

        index_counter += len(ble_streams_conflated)

    # Set the projection
    gdf_ble_streams_conflated.crs = ble_prj
    gdf_ble_streams_conflated_bleprj = gdf_ble_streams_conflated.to_crs(ble_prj)

    gdf_ble_streams_conflated_bleprj.to_csv(str_shp_out_arg + "//" + "gdf_ble_streams_conflated_bleprj.csv")
    gdf_ble_streams_conflated_bleprj.to_file(str_shp_out_arg + "//" + "gdf_ble_streams_conflated_bleprj.shp")

    # TODO: Add a column to model_catelog to state the reason of excluding a ras model

    print()
    print("COMPLETE")

    flt_end_create_shapes_from_hecras = time.time()
    flt_time_pass_conflate_hecras_to_nwm = (
        flt_end_create_shapes_from_hecras - flt_start_conflate_hecras_to_nwm
    ) // 1
    time_pass_conflate_hecras_to_nwm = datetime.timedelta(seconds=flt_time_pass_conflate_hecras_to_nwm)

    print("Compute Time: " + str(time_pass_conflate_hecras_to_nwm))

    print("+=================================================================+")


if __name__ == "__main__":
    # Sample:
    # python conflate_hecras_to_nwm -w 12090301
    # -i 'c:\\ras2fim_data\\output_ras2fim\\12090301_2277_230821\\01_shapes_from_hecras'
    # -o 'c:\\ras2fim_data\\output_ras2fim\\12090301_2277_230821\\02_csv_from_conflation'
    # -n 'c:\\ras2fim_data\\inputs\\X-National_Datasets'

    parser = argparse.ArgumentParser(
        description="===== CONFLATE HEC-RAS TO NATIONAL WATER MODEL STREAMS ====="
    )

    parser.add_argument(
        "-w",
        dest="str_huc8_arg",
        help="REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204",
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
        help=r"REQUIRED: path to national datasets: Example: E:\X-NWS\X-National_Datasets",
        required=True,
        metavar="DIR",
        type=str,
    )

    args = vars(parser.parse_args())

    str_huc8_arg = args["str_huc8_arg"]
    str_shp_in_arg = args["str_shp_in_arg"]
    str_shp_out_arg = args["str_shp_out_arg"]
    str_nation_arg = args["str_nation_arg"]

    fn_conflate_hecras_to_nwm(str_huc8_arg, str_shp_in_arg, str_shp_out_arg, str_nation_arg)
