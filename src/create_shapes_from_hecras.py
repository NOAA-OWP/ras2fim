# This is the first pre-processing tool that turns HEC-RAS 1D modeling into
# flood inundation mapping products.  This routine takes the HEC-RAS models
# in a given directory and creates attributed shapefiles of the stream
# centerline and cross sections
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.31
#
# ras2fim - First pre-processing script
# Uses the 'ras2fim' conda environment

import argparse
import datetime
import multiprocessing as mp
import os.path
import re
import sys
import time
import traceback
from functools import partial
from multiprocessing import Pool
from os import path

import geopandas as gpd
import h5py
import numpy as np
import pandas as pd
import win32com.client
from shapely.geometry import LineString
from shapely.ops import linemerge, split

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# windows component object model for interaction with HEC-RAS API
# This routine uses RAS63.HECRASController (HEC-RAS v6.3.0 must be
# installed on this machine prior to execution)

# h5py for extracting data from the HEC-RAS g**.hdf files

# Global Variables
RLOG = sv.R2F_LOG
MP_LOG = ras2fim_logger.RAS2FIM_logger()  # the mp version


# -------------------------------------------------
def fn_open_hecras(rlog_file_path, rlog_file_prefix, str_ras_project_path):
    # Function - runs HEC-RAS (active plan) and closes the file

    # This function is included as part of a multiproc so each process needs to have
    # it's own instance of ras2fim logger.
    # WHY? this stops file open concurrency as each proc has its own.
    # We attempt to keep them somewhat sorted by using YYMMDD_HHMMSECMillecond)

    # This function is used as part of a multiproc in one place but not part of a
    # multi proc in another. When not MP, the rlog_file_path and rlog_file_prefix will be empty

    hec = None
    has_exception = False

    is_multi_proc = (rlog_file_prefix != "")

    try:
        if is_multi_proc:
            file_id = sf.get_date_with_milli()
            log_file_name = f"{rlog_file_prefix}-{file_id}.log"
            MP_LOG.setup(os.path.join(rlog_file_path, log_file_name))

            # opening HEC-RAS
            MP_LOG.trace(f"ras project path is {str_ras_project_path}")
        else:
            RLOG.trace(f"ras project path is {str_ras_project_path}")

        if os.path.exists(str_ras_project_path) is False:
            raise Exception(f"str_ras_project_path value of {str_ras_project_path} does not exist")

        # Make sure that the plan referenced files (in .g## form) are present
        # Poorly and agressivly addressing errors arising from
        # i.e. https://github.com/NOAA-OWP/ras2fim/issues/300;

        """
        with open(str_ras_project_path) as f:
            file_contents = f.read()

        # Look at extentions for state files
        file_matches = re.findall(r"File=(\w{3})", file_contents)
        if any(string[-1] != "1" for string in file_matches):
            MP_LOG.critical("model points at bad state file")
            raise Exception("plan file pointed to bad model state files")
        # Look at extentions for plan file
        file_matches = re.findall(r"Plan=(\w{3})", file_contents)
        if any(string[-1] != "1" for string in file_matches):
            MP_LOG.critical("model points at bad plan file")
            raise Exception("plan file pointed to plan higher than 1")
        """

        hec = win32com.client.Dispatch("RAS630.HECRASController")

        # hec.ShowRas()

        # opening HEC-RAS

        hec.Project_Open(str_ras_project_path)

        # to be populated: number and list of messages, blocking mode
        NMsg, TabMsg, block = None, None, True

        # computations of the current plan
        # We need to compute.  Opening RAS Mapper creates the Geom HDF
        v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

    except Exception:
        # re-raise it as error handling is farther up the chain
        # but I do need the finally to ensure the hec.QuitRas() is run
        print("")
        if is_multi_proc:
            MP_LOG.critical("An exception occurred with the HEC-RAS engine or its parameters.")
            MP_LOG.critical(f"str_ras_project_path is {str_ras_project_path}")
            MP_LOG.critical(traceback.format_exc())
        else:
            RLOG.critical("An exception occurred with the HEC-RAS engine or its parameters.")
            RLOG.critical(f"str_ras_project_path is {str_ras_project_path}")
            RLOG.critical(traceback.format_exc())

        print("")
        has_exception = True

    finally:
        # Especially with multi proc, if an error occurs with HEC-RAS (engine
        # or values submitted), HEC-RAS will not close itself just becuase of an python
        # exception. This leaves orphaned process threads (visible in task manager)
        # and sometimes visually as well.

        if hec is not None:
            try:
                hec.QuitRas()  # close HEC-RAS no matter watch
            except Exception as ex2:
                if is_multi_proc:  # meaning used by the non MP call
                    MP_LOG.warning("--- An error occured trying to close the HEC-RAS window process")
                    MP_LOG.warning(f"str_ras_project_path is {str_ras_project_path}")
                    MP_LOG.warning(f"--- Details: {ex2}")
                else:
                    RLOG.warning("--- An error occured trying to close the HEC-RAS window process")
                    RLOG.warning(f"str_ras_project_path is {str_ras_project_path}")
                    RLOG.warning(f"--- Details: {ex2}")

                # do nothing
        if has_exception and is_multi_proc:
            sys.exit(1)


# -------------------------------------------------
def fn_get_active_geom(str_path_hecras_project_fn2):
    # Fuction - gets the path of the active geometry HDF file

    try:
        if os.path.exists(str_path_hecras_project_fn2) is False:
            RLOG.warning(f"str_path_hecras_project_fn2 ({str_path_hecras_project_fn2}) does not exist")
            return ""

        # read the HEC-RAS project file
        with open(str_path_hecras_project_fn2) as f:
            file_contents = f.read()

        # Find the current plan
        pattern = re.compile(r"Current Plan=.*")
        matches = pattern.finditer(file_contents)

        if re.search("Current Plan=", file_contents) is None:
            RLOG.critical(" -- ALERT: Reconnect files for " + str_path_hecras_project_fn2)
            raise SystemExit(0)

        # close the HEC-RAS project file
        # f.close()

        for match in matches:
            str_current_plan = match.group(0)[-3:]

        str_path_to_current_plan = str_path_hecras_project_fn2[:-3] + str_current_plan

        # read the current plan
        with open(str_path_to_current_plan) as f:
            file_contents = f.read()

        # Find the current geometry
        pattern = re.compile(r"Geom File=.*")
        matches = pattern.finditer(file_contents)

        # close the HEC-RAS plan file
        # f.close()

        for match in matches:
            str_current_geom = match.group(0)[-3:]

        str_path_to_current_geom = str_path_hecras_project_fn2[:-3] + str_current_geom

        return str_path_to_current_geom # file name with the extension stripped off

    except Exception:
        RLOG.error(f"An error occurred while processing {str_path_hecras_project_fn2}")
        RLOG.error(traceback.format_exc())
        print("")
        return ""


# -------------------------------------------------
def fn_geodataframe_cross_sections(str_path_hecras_project_fn, STR_CRS_MODEL):
    # Fuction - Creates a GeoDataFrame of the cross sections for the
    # HEC-RAS geometry file in the active plan

    RLOG.trace(f"Creating gdf of cross sections for {str_path_hecras_project_fn}" f" and {STR_CRS_MODEL}")

    file_name = fn_get_active_geom(str_path_hecras_project_fn)
    if file_name == "":
        RLOG.warning("failure while getting active geometry")
        return gpd.GeoDataFrame()
    
    str_path_to_geom_hdf = file_name + ".hdf"

    if path.exists(str_path_to_geom_hdf):
        # open the geom hdf file
        hf = h5py.File(str_path_to_geom_hdf, "r")
    else:
        # run hec-ras and then open the geom file
        fn_open_hecras("", "", str_path_hecras_project_fn)
        hf = h5py.File(str_path_to_geom_hdf, "r")

    # get data from HEC-RAS hdf5 files

    # XY points of the cross section
    n1 = hf.get("Geometry/Cross Sections/Polyline Points")
    n1 = np.array(n1)

    # point maker where each stream points start
    n2 = hf.get("Geometry/Cross Sections/Polyline Parts")
    n2 = np.array(n2)

    # Attribute data of the streams (reach, river, etc...)
    n3 = hf.get("Geometry/Cross Sections/Attributes")
    n3 = np.array(n3)

    # Error handling: edge case, empty (bad) geo
    if n2.ndim == 0:
        RLOG.warning("Empty dataframe returned")
        return gpd.GeoDataFrame()

    # Create a list of  number of points per each stream line
    list_points_per_cross_section_line = []
    for row in n2:
        list_points_per_cross_section_line.append(row[1])

    # Get the name of the river, reach and station
    list_river_name = []
    list_reach_name = []
    list_station = []

    # Older geom hdf5 files do not have data in Geometry/Cross Sections/Attributes
    if n3.ndim > 0:
        # cross sections are in new hdf geom format
        for row in n3:
            list_river_name.append(row[0])
            list_reach_name.append(row[1])
            # Need to check for interpolated cross section
            # They end with a star
            str_xs_name = row[2]
            if str_xs_name[-1] == "*":
                # cross section is interpolated
                str_xs_name = str_xs_name[:-1]
            list_station.append(str_xs_name)
    else:
        # older hdf5 geom format
        n3_river_name = hf.get("Geometry/Cross Sections/River Names")
        n3_river_name = np.array(n3_river_name)
        for str_rivername in n3_river_name:
            list_river_name.append(str_rivername)

        n3_reach_name = hf.get("Geometry/Cross Sections/Reach Names")
        n3_reach_name = np.array(n3_reach_name)
        for str_reachname in n3_reach_name:
            list_reach_name.append(str_reachname)

        n3_stations = hf.get("Geometry/Cross Sections/River Stations")
        n3_stations = np.array(n3_stations)
        for str_station in n3_stations:
            str_xs_name = str_station
            if str_xs_name[-1] == "*":
                # cross section is interpolated
                str_xs_name = str_xs_name[:-1]
            list_station.append(str_xs_name)

    # Get a list of the points
    list_line_points_x = []
    list_line_points_y = []

    for row in n1:
        list_line_points_x.append(row[0])
        list_line_points_y.append(row[1])

    cross_section_points = [xy for xy in zip(list_line_points_x, list_line_points_y)]

    # Create an empty geopandas GeoDataFrame
    gdf_cross_sections = gpd.GeoDataFrame()
    gdf_cross_sections["geometry"] = None
    gdf_cross_sections["stream_stn"] = None
    gdf_cross_sections["river"] = None
    gdf_cross_sections["reach"] = None
    gdf_cross_sections["ras_path"] = None

    # set projection from input value
    gdf_cross_sections.crs = STR_CRS_MODEL

    # Loop through the cross section lines and create GeoDataFrame
    int_startPoint = 0
    i = 0

    for int_numPnts in list_points_per_cross_section_line:
        # Create linesting data with shapely
        gdf_cross_sections.loc[i, "geometry"] = LineString(
            cross_section_points[int_startPoint : (int_startPoint + int_numPnts)]
        )

        # River and Reach - these are numpy bytes and
        # need to be converted to strings
        # Note - HEC-RAS truncates values when loaded into the HDF

        gdf_cross_sections.loc[i, "stream_stn"] = list_station[i].decode("UTF-8")

        gdf_cross_sections.loc[i, "river"] = list_river_name[i].decode("UTF-8")
        gdf_cross_sections.loc[i, "reach"] = list_reach_name[i].decode("UTF-8")

        str_path_to_geom = str_path_to_geom_hdf[:-4]
        gdf_cross_sections.loc[i, "ras_path"] = str_path_to_geom

        i += 1
        int_startPoint = int_startPoint + int_numPnts

    return gdf_cross_sections


# -------------------------------------------------
def fn_geodataframe_stream_centerline(str_path_hecras_project_fn, STR_CRS_MODEL):
    # Function - Creates a GeodataFrame of the HEC-RAS stream centerline
    # for the geometry file in the active plan

    str_path_to_geom_hdf = (fn_get_active_geom(str_path_hecras_project_fn)) + ".hdf"

    if path.exists(str_path_to_geom_hdf):
        # open the geom hdf file
        hf = h5py.File(str_path_to_geom_hdf, "r")
    else:
        # run hec-ras and then open the geom file
        fn_open_hecras("", "", str_path_hecras_project_fn)
        hf = h5py.File(str_path_to_geom_hdf, "r")

    # XY points of the stream centerlines
    n1 = hf.get("Geometry/River Centerlines/Polyline Points")
    n1 = np.array(n1)

    # point maker where each stream points start
    n2 = hf.get("Geometry/River Centerlines/Polyline Parts")
    n2 = np.array(n2)

    # Error handling: edge case, empty (bad) geo
    if n2.ndim == 0:
        RLOG.warning(f"Polyline parts not found for model of '{STR_CRS_MODEL}'")
        return gpd.GeoDataFrame()

    # Get the name of the river and reach
    list_river_name = []
    list_reach_name = []

    # Attribute data of the streams (reach, river, etc...)
    n3 = hf.get("Geometry/River Centerlines/Attributes")
    n3 = np.array(n3)

    # TODO - MAC - 2021.10.31
    # Possible error with multiple rivers / reaches in older hdf5 geom

    if n3.ndim == 0:
        # some hdf files do not have Geometry/River Centerlines/Attributes
        # This is due to differences in the HEC-RAS versioning
        # Try an older hdf5 format for geom
        n3_reach = hf.get("Geometry/River Centerlines/Reach Names")
        n3_reach = np.array(n3_reach)

        n3_river = hf.get("Geometry/River Centerlines/River Names")
        n3_river = np.array(n3_river)

        # reach from older format
        if n3_reach.ndim == 0:
            list_reach_name.append("Unknown-not-found")
        else:
            list_reach_name.append(n3_reach[0])

        # river from older format
        if n3_river.ndim == 0:
            list_river_name.append("Unknown-not-found")
        else:
            list_river_name.append(n3_river[0])
    else:
        for row in n3:
            list_river_name.append(row[0])
            list_reach_name.append(row[1])

    # Create a list of  number of points per each stream line
    list_points_per_stream_line = []
    for row in n2:
        list_points_per_stream_line.append(row[1])

    # Get a list of the points
    list_line_points_x = []
    list_line_points_y = []

    for row in n1:
        list_line_points_x.append(row[0])
        list_line_points_y.append(row[1])

    stream_points = [xy for xy in zip(list_line_points_x, list_line_points_y)]

    # Create an empty geopandas GeoDataFrame
    gdf_streams = gpd.GeoDataFrame()
    gdf_streams["geometry"] = None
    gdf_streams["river"] = None
    gdf_streams["reach"] = None
    gdf_streams["ras_path"] = None

    # set projection from input value
    gdf_streams.crs = STR_CRS_MODEL

    # Loop through the stream centerlines and create GeoDataFrame
    int_startPoint = 0
    i = 0

    for int_numPnts in list_points_per_stream_line:
        # Create linesting data with shapely
        gdf_streams.loc[i, "geometry"] = LineString(
            stream_points[int_startPoint : (int_startPoint + int_numPnts)]
        )

        # Write the River and Reach - these are numpy bytes and need to be
        # converted to strings
        # Note - RAS truncates these values in the g01 and HDF files
        gdf_streams.loc[i, "river"] = list_river_name[i].decode("UTF-8")
        gdf_streams.loc[i, "reach"] = list_reach_name[i].decode("UTF-8")

        str_path_to_geom = str_path_to_geom_hdf[:-4]
        gdf_streams.loc[i, "ras_path"] = str_path_to_geom

        i += 1
        int_startPoint = int_startPoint + int_numPnts

    return gdf_streams


# -------------------------------------------------
def fn_get_active_flow(str_path_hecras_project_fn):
    # Fuction - gets the path of the active geometry HDF file

    # read the HEC-RAS project file
    with open(str_path_hecras_project_fn) as f:
        file_contents = f.read()

    # Find the current plan
    pattern = re.compile(r"Current Plan=.*")
    matches = pattern.finditer(file_contents)

    # close the HEC-RAS project file
    # f.close()

    for match in matches:
        str_current_plan = match.group(0)[-3:]

    str_path_to_current_plan = str_path_hecras_project_fn[:-3] + str_current_plan

    # read the current plan
    with open(str_path_to_current_plan) as f:
        file_contents = f.read()

    # Find the current geometry
    pattern = re.compile(r"Flow File=.*")
    matches = pattern.finditer(file_contents)

    # close the HEC-RAS plan file
    # f.close()

    # TODO 2021.03.08 - Error here if no flow file in the active plan
    # setting to a default of .f01 - This might not exist
    str_current_flow = "f01"

    for match in matches:
        str_current_flow = match.group(0)[-3:]

    str_path_to_current_flow = str_path_hecras_project_fn[:-3] + str_current_flow

    return str_path_to_current_flow


# -------------------------------------------------
def fn_get_flow_dataframe(str_path_hecras_flow_fn):
    # Get pandas dataframe of the flows in the active plan's flow file

    # initalize the dataframe
    df = pd.DataFrame()
    df["river"] = []
    df["reach"] = []
    df["start_xs"] = []
    df["max_flow"] = []

    with open(str_path_hecras_flow_fn, "r") as file1:
        lines = file1.readlines()
        i = 0  # number of the current row

        for line in lines:
            if line[:19] == "Number of Profiles=":
                # determine the number of profiles
                int_flow_profiles = int(line[19:])

                # determine the number of rows of flow - each row has maximum of 10
                int_flow_rows = int(int_flow_profiles // 10 + 1)

            if line[:15] == "River Rch & RM=":
                str_river_reach = line[15:]  # remove first 15 characters

                # split the data on the comma
                list_river_reach = str_river_reach.split(",")

                # Get from array - use strip to remove whitespace
                str_river = list_river_reach[0].strip()
                str_reach = list_river_reach[1].strip()
                str_start_xs = list_river_reach[2].strip()
                flt_start_xs = float(str_start_xs)

                # Read the flow values line(s)
                list_flow_values = []

                # for each line with flow data
                for j in range(i + 1, i + int_flow_rows + 1):
                    # get the current line
                    line_flows = lines[j]

                    # determine the number of values on this
                    # line from character count
                    int_val_in_row = int((len(lines[j]) - 1) / 8)

                    # for each value in the row
                    for k in range(0, int_val_in_row):
                        # get the flow value (Max of 8 characters)
                        str_flow = line_flows[k * 8 : k * 8 + 8].strip()
                        # convert the string to a float
                        flt_flow = float(str_flow)
                        # append data to list of flow values
                        list_flow_values.append(flt_flow)

                # Get the max value in list
                flt_max_flow = max(list_flow_values)

                # write to dataFrame
                df_new_row = pd.DataFrame.from_records(
                    [
                        {
                            "river": str_river,
                            "reach": str_reach,
                            "start_xs": flt_start_xs,
                            "max_flow": flt_max_flow,
                        }
                    ]
                )
                df = pd.concat([df, df_new_row], ignore_index=True)

            i += 1

    return df


# -------------------------------------------------
def fn_gdf_append_xs_with_max_flow(df_xs_fn, df_flows_fn):
    # Function - for a list of cross sections, determine the maximum flow
    # and return as a pandas dataframe

    list_max_flows_per_xs = []

    # for each row in cross section list
    for index, row in df_xs_fn.iterrows():
        max_flow_value = 0

        # for each row in flow break list
        for index2, row2 in df_flows_fn.iterrows():
            # if this is the same river/reach pair
            if row["river"] == row2["river"] and row["reach"] == row2["reach"]:
                # if xs station is less than (or equal to) flow break station
                if row["stream_stn"] <= row2["start_xs"]:
                    max_flow_value = row2["max_flow"]

        list_max_flows_per_xs.append(max_flow_value)

    df_xs_fn["max_flow"] = list_max_flows_per_xs

    return df_xs_fn


# -------------------------------------------------
def fn_cut_stream_downstream(gdf_return_stream_fn, df_xs_fn):
    # Function - split the stream line on the last cross section
    # This to remove the portion of the stream centerline that is
    # downstream of the last cross section; helps with stream conflation

    df_xs_fn["stream_stn"] = df_xs_fn["stream_stn"].astype(float)

    # Get minimum stationed cross section

    # TODO: Linting says with these following lines that flt_ds_xs
    # does not exist and shoudl be removed. But I am not sure that is true
    # flt_ds_xs = df_xs_fn["stream_stn"].min()
    # gdf_ds_xs = df_xs_fn.query("stream_stn==@flt_ds_xs")
    flt_ds_xs = df_xs_fn["stream_stn"].min()
    gdf_ds_xs = df_xs_fn[df_xs_fn["stream_stn"] == flt_ds_xs]

    # reset the index of the sampled cross section
    gdf_ds_xs = gdf_ds_xs.reset_index()

    # grab the first lines - assumes that the stream is the first stream
    stream_line = gdf_return_stream_fn["geometry"][0]
    ds_xs_line = gdf_ds_xs["geometry"][0]

    # first make sure the stream have been digitized from upstream to downstream.
    # To do that, split the stream at the last xsection and see if the
    # first splitted segment intersect the most upstream xsection or not

    flt_us_xs = df_xs_fn["stream_stn"].max()
    gdf_us_xs = df_xs_fn[df_xs_fn["stream_stn"] == flt_us_xs]

    result = split(stream_line, ds_xs_line)

    # if the first return of above split does not intersects most upstream xs, reverse the order
    if not result.geoms[0].intersects(gdf_us_xs["geometry"][0]):
        stream_line = LineString(list(stream_line.coords)[::-1])

    # now continue to shorten the streamline

    # split and return a GeoCollection
    result = split(stream_line, ds_xs_line)

    # the last cross section may be at the last stream point - 2021.10.27
    # get a list of items in the returned GeoCollection
    list_wkt_lines = [item for item in result.geoms]
    list_lines = []

    if len(list_wkt_lines) > 1:
        # merge all the lines except the last line
        for i in range(len(list_wkt_lines) - 1):
            list_lines.append(list_wkt_lines[i])

        # Now merge the line with the first segment of the downstream line

        # get first segment of the downstream (last) line
        new_line = LineString([list_wkt_lines[i + 1].coords[0], list_wkt_lines[i + 1].coords[1]])
        list_lines.append(new_line)

        # merge the lines
        shp_merged_lines = linemerge(list_lines)

        # Revise the geometry with the first line (assumed upstream)
        gdf_return_stream_fn["geometry"][0] = shp_merged_lines

    return gdf_return_stream_fn


# -------------------------------------------------
# Print iterations progress
def fn_print_progress_bar(
    iteration, total, prefix="", suffix="", decimals=1, length=100, fill="█", printEnd="\r"
):
    """
    from: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    Call in a loop to create terminal progress bar
    Keyword arguments:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)

    # Print New Line on Complete
    if iteration == total:
        print()


# -------------------------------------------------
def fn_create_shapes_from_hecras(input_models_path, output_shp_files_path, projection):
    # INPUT
    flt_start_create_shapes_from_hecras = time.time()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|    STREAM AND CROSS SECTION SHAPEFILES FROM HEC-RAS DIRECTORY   |")
    RLOG.lprint("+-----------------------------------------------------------------+")
    RLOG.lprint(f"  ---(i) INPUT PATH: {input_models_path}")
    RLOG.lprint(f"  ---(o) OUTPUT PATH: {output_shp_files_path}")
    RLOG.lprint(f"  ---(p) MODEL PROJECTION: {projection}")
    RLOG.lprint(f"  --- Module Started: {sf.get_stnd_date()}")

    str_path_to_output_streams = os.path.join(output_shp_files_path, "stream_LN_from_ras.shp")
    str_path_to_output_cross_sections = os.path.join(output_shp_files_path, "cross_section_LN_from_ras.shp")

    RLOG.lprint("+-----------------------------------------------------------------+")

    # *****MAIN******
    # get a list of all HEC-RAS prj files in a directory

    # we need to not walk but get the dirs and get the files on each. Why? some
    # dirs can be removed based on its dir name

    # Some models can not be processed and we are unable to trap them at this time, so we will
    # manually add an exception list to drop them

    bad_models_lst = ["1292972_BECK BRANCH_g01_1701646035", "1293152_DUCK CREEK_g01_1701646019"]

    list_prj_files = []
    for root, dirs, __ in os.walk(input_models_path):
        for folder_name in dirs:
            if folder_name in bad_models_lst:
                RLOG.warning(f"model folder name is on the 'bad model list' ({folder_name})")
                continue

            # check the model folder name and see if it's "g" number is anything but "g01"
            # If not.. drop the model.  aka.. 292972_BECK BRANCH_g01_1701646035 will continue
            # but 292972_BECK BRANCH_g02_1701646035 (just an example wil drop out here)
            if "_g01_" not in folder_name and "_G01_)" not in folder_name:
                RLOG.warning(f"model folder name is not a 'g01' folder ({folder_name})")
                continue

            # load its child files
            model_path = os.path.join(root, folder_name)
            files = os.listdir(model_path)
            for file_name in files:
                # -----------------------
                # test and cleanup all files in the directory
                str_file_path = os.path.join(model_path, file_name)

                # These tests are primarily about update the prj file
                # but removing the files helps too (less mess)
                file_ext_split = os.path.splitext(file_name)
                if len(file_ext_split) != 2:
                    RLOG.warning(f"model file of {str_file_path} skipped - invalid extension")
                    continue
                file_ext = file_ext_split[1].replace(".", "")

                # Matches all extensions starting with g, f, or p followed by a range of 02 to 99
                # we want to only keep g01, f01, p01
                # ie. Remove files like g03, or f11, or p26, etc
                # regex_first_char_pattern = "^[g|f|p][0-9][0-9]"
                # regex_last_chars_pattern = "[0-9][]"

                first_char = file_ext[0]
                remaining_str = file_ext[1:]
                if (first_char in ['f', 'g', 'p']) and (remaining_str.isnumeric()):
                    if remaining_str != "01":
                        # remove the file and continue
                        os.remove(str_file_path)
                        RLOG.warning(f"model file of {str_file_path} deleted - invalid extension")
                        continue
                # if it is an .hdf file, we want to delete so it can be regenerated.
                # if file_ext == "hdf":
                #    os.remove(str_file_path)
                #    continue

                # we don't want to keep files that are not prj or PRJ files.
                if (file_ext != "prj") and (file_ext != "PRJ"):
                    continue

                # -----------------------
                # Check for valid prj files which might have some lines removed
                # Don't rely on the fact that the file may or may not have existed
                is_a_valid_prj_file = True
                new_file_lines = []  # we will rewrite the file out with the dropped lines if applic
                has_lines_removed = False
                with open(str_file_path, 'r') as f:
                    file_lines = f.readlines()
                    for idx, line in enumerate(file_lines):
                        line = line.strip()  # removed new line characters
                        if any(x in line for x in ["PROJCS", "GEOGCS", "DATUM", "PROJECTION"]):
                            is_a_valid_prj_file = False
                            continue

                        if (
                            line.startswith("Geom File=")
                            or line.startswith("Flow File=")
                            or line.startswith("Plan File=")
                        ):
                            # strip off last three
                            last_three_chars = line[-3:]
                            if (
                                (last_three_chars != "g01")
                                and (last_three_chars != "p01")
                                and (last_three_chars != "f01")
                            ):
                                # aka.. can't be g02, g10, f21, etc.
                                has_lines_removed = True
                            else:
                                new_file_lines.append(line + "\n")
                                # new_file_lines.append(line)
                        else:
                            # new_file_lines.append(line + "\n")
                            new_file_lines.append(line + "\n")
                # end of the "with"

                if is_a_valid_prj_file:  # it is a valid prj file
                    list_prj_files.append(str_file_path)

                    if has_lines_removed:  # Then we want to rewrite the file with the lines removed
                        # Re-write the adjusted prj file.
                        with open(str_file_path, 'w') as f:
                            f.writelines(new_file_lines)

    # -----
    # checking to see if 'prj' files are not binary and
    # valid HEC-RAS prj files.  This should exclude all other
    # prj files
    # skip projection files
    textchars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7F})
    is_binary_string = lambda bytes: bool(bytes.translate(None, textchars))

    str_check = "Current Plan"
    list_files_valid_prj = []

    # for str_file_path in list_prj_files:
    for str_file_path in list_prj_files:
        if not is_binary_string(open(str_file_path, "rb").read(1024)):
            with open(str_file_path, "r") as file_prj:
                b_found_match = False

                for line in file_prj:
                    if str_check in line:
                        b_found_match = True
                        break
                if b_found_match:
                    list_files_valid_prj.append(str_file_path)

    RLOG.lprint(f"Number of valid prj files is {len(list_files_valid_prj)}")
    print()

    # Run all the HEC-RAS models that do not have the geom HDF files
    list_models_to_compute = []

    for str_prj in list_files_valid_prj:
        # print("processing:"+str_prj)
        file_name = fn_get_active_geom(str_prj)
        if file_name == "":
            continue
        str_path_to_geom_hdf = file_name + ".hdf"
        if not path.exists(str_path_to_geom_hdf):
            # the hdf file does not exist - add to list of models to compute
            list_models_to_compute.append(str_prj)

    if len(list_models_to_compute) > 0:
        # -------------------------------------------------
        # A "partial" just extends the original function to add extra params on the fly
        # e.g. The original fn_open_hecras has only list_models_to_compute being
        # passed in. Now, but adding a partial, the args beign passed into fn_open_hecras
        # are RLOG.LOG_DEFAULT_FOLDER, log_file_prefix, list_models_to_compute
        # Notice.. the partial gets a temp name that gets passed into the function in the pool
        log_file_prefix = "fn_open_hecras"
        fn_open_hecras_partial = partial(fn_open_hecras, RLOG.LOG_DEFAULT_FOLDER, log_file_prefix)
        # create a pool of processors
        num_processors = mp.cpu_count() - 2
        with Pool(processes=num_processors) as executor:
            # multi-process the HEC-RAS calculation of these models
            executor.map(fn_open_hecras_partial, list_models_to_compute)

        # TODO: We have a probem of ALL of the mp's fail in here
        # we also need to add teh "futures" system so can return some way to know if failed so we can update

        # Now that multi-proc is done, lets merge all of the independent log file
        # from each list_files_valid_prj
        # that one is easy as we can use "futures.result" to show us if it failed.
        # we have example code in this product.
        RLOG.merge_log_files(RLOG.LOG_FILE_PATH, log_file_prefix)

    # -----
    list_geodataframes_stream = []
    list_geodataframes_cross_sections = []
    len_valid_prj_files = len(list_files_valid_prj)

    fn_print_progress_bar(
        0, len_valid_prj_files, prefix="Reading HEC-RAS output", suffix="Complete", length=24
    )
    i = 0

    for ras_path in list_files_valid_prj:
        try:
            # print(ras_path)
            gdf_return_stream = fn_geodataframe_stream_centerline(ras_path, projection)

            df_flows = fn_get_flow_dataframe(fn_get_active_flow(ras_path))
            df_xs = fn_geodataframe_cross_sections(ras_path, projection)
            if df_xs.empty:
                RLOG.warning("Empty geometry in " + ras_path)
                continue

            # Fix interpolated cross section names (ends with *)
            for index, row in df_xs.iterrows():
                str_check = row["stream_stn"]
                if str_check[-1] == "*":
                    # Overwrite the value to remove '*'
                    df_xs.at[index, "stream_stn"] = str_check[:-1]

            df_xs["stream_stn"] = df_xs["stream_stn"].astype(float)
            gdf_xs_flows = fn_gdf_append_xs_with_max_flow(df_xs, df_flows)

            gdf_return_stream = fn_cut_stream_downstream(gdf_return_stream, df_xs)

            list_geodataframes_stream.append(gdf_return_stream)
            list_geodataframes_cross_sections.append(gdf_xs_flows)
        except Exception:
            RLOG.error(f"An error occurred while processing {ras_path}")
            RLOG.error(traceback.format_exc())
            print("")

        time.sleep(0.03)
        i += 1
        fn_print_progress_bar(
            i, len_valid_prj_files, prefix="Reading HEC-RAS output", suffix="Complete", length=24
        )

    # Create GeoDataframe of the streams and cross sections
    gdf_aggregate_streams = gpd.GeoDataFrame(pd.concat(list_geodataframes_stream, ignore_index=True))

    gdf_aggregate_cross_section = gpd.GeoDataFrame(
        pd.concat(list_geodataframes_cross_sections, ignore_index=True)
    )

    # Create shapefiles of the streams and cross sections
    gdf_aggregate_streams.to_file(str_path_to_output_streams)
    gdf_aggregate_cross_section.to_file(str_path_to_output_cross_sections)

    RLOG.lprint("")
    RLOG.success("SHAPEFILES CREATED")
    flt_end_create_shapes_from_hecras = time.time()
    flt_time_pass_create_shapes_from_hecras = (
        flt_end_create_shapes_from_hecras - flt_start_create_shapes_from_hecras
    ) // 1
    time_pass_create_shapes_from_hecras = datetime.timedelta(seconds=flt_time_pass_create_shapes_from_hecras)
    RLOG.lprint("Compute Time: " + str(time_pass_create_shapes_from_hecras))

    RLOG.lprint("====================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample:
    # python create_shapes_from_hecras.py -i C:\ras2fim_data\OWP_ras_models\models-12030105-small
    #  -o c:\ras2fim_data\output_ras2fim\12030105_2276_231024\01_shapes_from_hecras -p EPSG:2276

    parser = argparse.ArgumentParser(
        description="============ SHAPEFILES FROM HEC-RAS DIRECTORY ============"
    )

    parser.add_argument(
        "-i",
        dest="input_models_path",
        help=r"REQUIRED: path containing the HEC-RAS files: Example C:\ras2fim_data\OWP_ras_models\models",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="output_shp_files_path",
        help="REQUIRED: path to write shapefile:"
        r" Example c:\ras2fim_data\output_ras2fim\12030105_2276_231024\01_shapes_from_hecras",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-p",
        dest="projection",
        help="REQUIRED: projection of HEC-RAS models: Example EPSG:26915",
        required=True,
        metavar="STRING",
        type=str,
    )

    args = vars(parser.parse_args())

    referential_path = os.path.join(os.path.dirname(__file__), "..", args["output_shp_files_path"])
    config_file = os.path.abspath(referential_path)
    log_file_folder = os.path.join(config_file, "logs")
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
        fn_create_shapes_from_hecras(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
