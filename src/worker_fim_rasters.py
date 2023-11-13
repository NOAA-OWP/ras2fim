# Creates flow files, plan files and first run depth grids
#
# # Uses the 'ras2fim' conda environment
# ************************************************************
import errno
import os
import re
import shutil

# import matplotlib.pyplot as plt
# import matplotlib.ticker as tick
import numpy as np
import pandas as pd

import ras2fim_logger


# import win32com.client
# from scipy.interpolate import interp1d

# from datetime import date


# Global Variables
RLOG = ras2fim_logger.R2F_LOG  # the non mp version
MP_LOG = ras2fim_logger.RAS2FIM_logger()  # mp version

# This routine uses RAS630.HECRASController (HEC-RAS v6.0.0 must be
# installed on this machine prior to execution)


# -------------------------------------------------
def fn_create_profile_names(list_profiles, str_suffix):
    str_profile_names = 'Profile Names='

    for i in range(len(list_profiles)):
        str_profile_names += 'flow' + str(list_profiles[i]) + str_suffix  # flow was added to this line
        if i < (len(list_profiles) - 1):
            str_profile_names = str_profile_names + ','

    return str_profile_names


# -------------------------------------------------
def fn_get_flow_dataframe(str_path_hecras_flow_fn):
    # Get pandas dataframe of the flows in the active plan's flow file

    # initalize the dataframe
    df = pd.DataFrame()
    df['river'] = []
    df['reach'] = []
    df['start_xs'] = []
    df['max_flow'] = []

    file1 = open(str_path_hecras_flow_fn, 'r')
    lines = file1.readlines()
    i = 0  # number of the current row

    for line in lines:
        if line[:19] == 'Number of Profiles=':
            # determine the number of profiles
            int_flow_profiles = int(line[19:])

            # determine the number of rows of flow - each row has maximum of 10
            int_flow_rows = int(int_flow_profiles // 10 + 1)

        if line[:15] == 'River Rch & RM=':
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
                [{"river": str_river, "reach": str_reach, "start_xs": flt_start_xs, "max_flow": flt_max_flow}]
            )
            df = pd.concat([df, df_new_row], ignore_index=True)

        i += 1

    file1.close()

    return df


# -------------------------------------------------
def fn_create_firstpass_flowlist(int_fn_starting_flow, int_fn_max_flow, int_fn_number_of_steps):
    # create a list of flows for the first pass HEC-RAS

    list_first_pass_flows = []

    int_fn_deltaflowstep = int(int_fn_max_flow // (int_fn_number_of_steps - 2))

    for i in range(int_fn_number_of_steps):
        list_first_pass_flows.append(int_fn_starting_flow + (i * int_fn_deltaflowstep))

    return list_first_pass_flows


# -------------------------------------------------
def fn_format_flow_values(list_flow):
    int_number_of_profiles = len(list_flow)
    str_all_flows = ""

    int_number_of_new_rows = int((int_number_of_profiles // 10) + 1)
    int_items_in_new_last_row = int_number_of_profiles % 10

    # write out new rows of 10 grouped flows
    if int_number_of_new_rows > 1:
        # write out the complete row of 10
        for j in range(int_number_of_new_rows - 1):
            str_flow = ""
            for k in range(10):
                int_Indexvalue = j * 10 + k
                # format to 8 characters that are right alligned
                str_current_flow = "{:>8}".format(str(list_flow[int_Indexvalue]))
                str_flow = str_flow + str_current_flow
            str_all_flows += str_flow + "\n"

    # write out the last row
    str_flow_last_row = ""
    for j in range(int_items_in_new_last_row):
        int_indexvalue = (int_number_of_new_rows - 1) * 10 + j
        str_current_flow = "{:>8}".format(str(list_flow[int_indexvalue]))
        str_flow_last_row += str_current_flow
    str_all_flows += str_flow_last_row

    return str_all_flows


# -------------------------------------------------
# Reading old parent models flow and geometry files
# with WSE and normal depth (ND, slope) BCs
# and create a seperate list of flow and
# geometry files WSE and ND BCs
def create_list_of_paths_flow_geometry_files_4each_BCs(path_to_conflated_streams_csv):
    # Reads the name of all folders in the
    # parent ras models directory which are conflated

    # "02_csv_from_conflation":
    # Hard-coded as the name of output folder for step 2
    # "path_to_ras_models_4step5.csv":
    # Hard-coded as the name of output csv file for step 2
    str_path_to_csv = (
        path_to_conflated_streams_csv
        + "//"
        + "02_csv_from_conflation"
        + "//"
        + "path_to_ras_models_4step5.csv"
    )

    path_conflated_streams = pd.read_csv(str_path_to_csv)
    path_conflated_models = list(path_conflated_streams['ras_path'])

    ls_path_flowfiles = [paths[:-3] + "f01" for paths in path_conflated_models]

    # List of flow file paths
    # Water surface elevation BC
    str_path_to_flow_file_wse = []
    for fpath in ls_path_flowfiles:
        file_flow = open(fpath, 'r')
        lines_flow = file_flow.readlines()

        for flines in lines_flow:
            if flines[:11] == "Dn Known WS":
                str_path_to_flow_file_wse.append(fpath)
                break

        file_flow.close()

    # Normal depth BC
    str_path_to_flow_file_nd = []
    for fpath2 in ls_path_flowfiles:
        file_flow2 = open(fpath2, 'r')
        lines_flow2 = file_flow2.readlines()

        for flines2 in lines_flow2:
            if flines2[:8] == "Dn Slope":
                str_path_to_flow_file_nd.append(fpath2)
                break

        file_flow2.close()

    # List of geometry file paths
    str_path_to_geo_file_wse = []
    for fpath_wse in str_path_to_flow_file_wse:
        gpath = fpath_wse[:-3] + "g01"
        str_path_to_geo_file_wse.append(gpath)

    str_path_to_geo_file_nd = []
    for fpath_nd in str_path_to_flow_file_nd:
        gpath2 = fpath_nd[:-3] + "g01"
        str_path_to_geo_file_nd.append(gpath2)

    return (
        str_path_to_flow_file_wse,
        str_path_to_flow_file_nd,
        str_path_to_geo_file_wse,
        str_path_to_geo_file_nd,
    )


# path_to_conflated_streams_csv = "C:/ras2fim_data/OWP_ras_models/ras2fimv2.0/v2_outputs"

# [
#     str_path_to_flow_file_wse,
#     str_path_to_flow_file_nd,
#     str_path_to_geo_file_wse,
#     str_path_to_geo_file_nd,
# ] = create_list_of_paths_flow_geometry_files_4each_BCs(path_to_conflated_streams_csv)


# -------------------------------------------------
# Compute BC (75 flows/WSE) for the parent RAS models with WSE BC
def compute_boundray_condition_wse(str_path_to_flow_file_wse, str_path_to_geo_file_wse):
    list_bc_target_xs_huc8 = []
    for path_in in range(len(str_path_to_flow_file_wse)):
        # Get max flow for each xs in which flow changes in a dataframe format
        max_flow_df_wse = fn_get_flow_dataframe(str_path_to_flow_file_wse[path_in])

        # -------------------------------------------------
        # Create firstpass flow dataframe for each xs in which flow changes
        int_fn_starting_flow = 1  # cfs
        int_number_of_steps = 76

        # Water surface elevation BC
        first_pass_flows_xs_wse = []
        for num_xs in range(len(max_flow_df_wse)):
            int_fn_max_flow = int(max_flow_df_wse['max_flow'][num_xs])
            list_first_pass_flows = fn_create_firstpass_flowlist(
                int_fn_starting_flow, int_fn_max_flow, int_number_of_steps
            )
            first_pass_flows_xs_wse.append(list_first_pass_flows)

        first_pass_flows_xs_wse_df = pd.DataFrame(first_pass_flows_xs_wse).T
        first_pass_flows_xs_wse_df.columns = [int(j) for j in max_flow_df_wse['start_xs']]

        # -------------------------------------------------
        # Get all flow data from the current plan's (parent models) flow file
        # and save it in a pandas dataframe for
        # Water surface elevation BC
        str_path_hecras_flow_fn = str_path_to_flow_file_wse[path_in]

        file1 = open(str_path_hecras_flow_fn, 'r')
        lines = file1.readlines()
        i = 0  # number of the current row
        list_all_flow_values = []

        for line in lines:
            if line[:19] == 'Number of Profiles=':
                # determine the number of profiles
                int_flow_profiles = int(line[19:])

                # determine the number of rows of flow - each row has maximum of 10
                int_flow_rows = int(int_flow_profiles // 10 + 1)

            if line[:15] == 'River Rch & RM=':
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

                # print(list_flow_values)
                list_all_flow_values.append(list_flow_values)

            i += 1

        df_all_flow_values = pd.DataFrame(list_all_flow_values)
        column_names = ['flow' + str(j + 1) for j in range(int_flow_profiles)]
        df_all_flow_values.columns = column_names

        all_flow_info_df = pd.concat([max_flow_df_wse, df_all_flow_values], axis=1)
        target_xs = int(list(all_flow_info_df['start_xs'])[-1])  # last xs that flow changes in
        # str_target_xs = str(target_xs)

        # -------------------------------------------------
        # All flow data dataframe for the boundary condition of known WSE
        target_xs_flows = df_all_flow_values.iloc[-1]
        target_xs_flows_df = pd.DataFrame(target_xs_flows)
        target_xs_flows_df.index = [k for k in range(int_flow_profiles)]
        target_xs_flows_df.columns = ['discharge']

        # Get the WSE for the boundray condition (known WSE)
        target_xs_wse = []
        for line in lines:
            if line[:12] == 'Dn Known WS=':
                # determine the number of profiles
                WSE = float(line[12:])
                target_xs_wse.append(WSE)

        target_xs_wse_df = pd.DataFrame(target_xs_wse, columns=['wse'])

        # Dataframe of known WSE BC (flow and wse)
        bc_df = pd.concat([target_xs_flows_df, target_xs_wse_df], axis=1)
        bc_sort_df = bc_df.sort_values(by=['discharge'])

        file1.close()

        # -------------------------------------------------
        # Finding the last cross section (target XS for min elevation)
        # Water surface elevation BC
        str_path_hecras_geo_fn = str_path_to_geo_file_wse[path_in]  #

        file_geo = open(str_path_hecras_geo_fn, 'r')
        lines_geo = file_geo.readlines()
        for gline in lines_geo:
            if gline[:14] == 'Type RM Length':
                target_line = gline.split(",")
                counter_xs = int(target_line[1])

        # Last XS for min elevation
        str_target_xs_min_elev = counter_xs

        # -------------------------------------------------
        # Finding the geometry lines for the last XS
        # Water surface elevation BC
        j = 0
        for geoline in lines_geo:
            if geoline[:14] == 'Type RM Length':
                target_line = geoline.split(",")
                counter_xs = int(target_line[1])

                if counter_xs == int(str_target_xs_min_elev):  # str_target_xs
                    # read "XS GIS Cut Line" for the target xs

                    tls = j + 4  # "XS GIS Cut Line" line number
                    num_xs_cut_line = int(lines_geo[tls][16:])  # number of xs cut lines

                    if num_xs_cut_line % 2 != 0:  # if num_xs_cut_line is odd
                        num_xs_cut_line2 = num_xs_cut_line + 1
                        num_sta_elev_line = tls + 2 + (num_xs_cut_line2 / 2)
                        sta_elev_line = lines_geo[int(num_sta_elev_line)]
                    else:
                        num_sta_elev_line = tls + 2 + (num_xs_cut_line / 2)
                        sta_elev_line = lines_geo[int(num_sta_elev_line)]

                    num_stat_elev = int(sta_elev_line[10:])

                    if num_stat_elev % 5 == 0:  # 10 numbers in each row
                        len_stat_elev_ls = [
                            int(num_sta_elev_line + 1),
                            int(num_sta_elev_line + 1 + (num_stat_elev / 5)),
                        ]  # 5 station/elev sets per each row
                    else:
                        len_stat_elev_ls = [
                            int(num_sta_elev_line + 1),
                            int(num_sta_elev_line + 1 + 1 + int(num_stat_elev / 5)),
                        ]

            j += 1

        # Finding the min elevation from the target XS's station/elevation list
        stat_elev_ls = lines_geo[len_stat_elev_ls[0] : len_stat_elev_ls[1]]

        flt_stat_elev_ls = []
        for sel in range(len(stat_elev_ls)):
            sel_line = [float(sell) for sell in re.findall('.{1,8}', stat_elev_ls[sel])]
            flt_stat_elev_ls.append(sel_line)

        flt_stat_elev_nan_df = pd.DataFrame(flt_stat_elev_ls)
        num_stat_elev_nan = int(len(flt_stat_elev_nan_df) * 5)
        flt_stat_elev_ls_rsh = np.reshape(flt_stat_elev_nan_df, [num_stat_elev_nan, 2])
        flt_stat_elev_df = pd.DataFrame(flt_stat_elev_ls_rsh, columns=['sta', 'elev']).dropna()

        min_elev_target_xs = min(flt_stat_elev_df['elev'])

        file_geo.close()

        # -------------------------------------------------
        # WSE boundary condition (bc) dataframe plus min elevation point
        min_elev_flow_df = pd.DataFrame([0.01, min_elev_target_xs]).T
        min_elev_flow_df.columns = ['discharge', 'wse']

        bc_observ_flow_wse_df = pd.concat([min_elev_flow_df, bc_sort_df], ignore_index=True)

        # -------------------------------------------------
        # Computing WSE BC rating curve
        stage = bc_observ_flow_wse_df['wse']
        discharge = bc_observ_flow_wse_df['discharge']

        # curve_fit
        z = np.polyfit(discharge[:4], stage[:4], 2)
        poly_func = np.poly1d(z)
        # prediction and r-squared
        # pred1 = poly_func(discharge[:4])
        # r2_1 = round(r2_score(stage[:4], pred1), 3)

        knot_ind = 1
        # curve_fit
        z2 = np.polyfit(discharge[knot_ind:], stage[knot_ind:], 2)
        poly_func2 = np.poly1d(z2)
        # prediction and r-squared
        # pred2 = poly_func2(discharge[knot_ind:])
        # r2_2 = round(r2_score(stage[knot_ind:], pred2), 3)

        # -------------------------------------------------
        # Generating BC for the first pass flow
        flows1st_target_xs = first_pass_flows_xs_wse_df[target_xs]

        # Finding the knot point for the target xs and predicting WSE
        knot_point = discharge[knot_ind]
        pred_wse_flows1st_target_xs = []
        for flows_1st in flows1st_target_xs:
            if flows_1st <= knot_point:
                pred_wse_flow1st = poly_func(flows_1st)
            else:
                pred_wse_flow1st = poly_func2(flows_1st)
            pred_wse_flows1st_target_xs.append(pred_wse_flow1st)

        # -------------------------------------------------
        # Generating a monotonic wse for the first pass flow
        nm_in1 = []
        for wsei in range(len(pred_wse_flows1st_target_xs) - 1):
            if pred_wse_flows1st_target_xs[wsei + 1] < pred_wse_flows1st_target_xs[wsei]:
                nm_in1.append(wsei)
                break

        if len(nm_in1) > 0:
            pred_wse_nm = pred_wse_flows1st_target_xs[nm_in1[0]]
            nm_in = [nm_in1[0]]
            nm = 0
            for wsei2 in range(len(pred_wse_flows1st_target_xs) - nm_in1[0]):
                if pred_wse_flows1st_target_xs[nm + nm_in1[0]] < pred_wse_nm:
                    nm_in.append(nm + nm_in1[0])
                nm += 1

            nm_wse_1st = pred_wse_flows1st_target_xs[nm_in1[0]]
            wse_last = pred_wse_flows1st_target_xs[-1]

            delta_wse = (wse_last - nm_wse_1st) / (int_number_of_steps - nm_in1[0])
            delta_indx = int_number_of_steps - nm_in1[0]

            gen_mont_wse = [nm_wse_1st + (di) * delta_wse for di in range(delta_indx)]
            mont_wse = pred_wse_flows1st_target_xs[: nm_in1[0]]

            pred_wse_mont = pd.concat([pd.DataFrame(mont_wse), pd.DataFrame(gen_mont_wse)], ignore_index=True)

            bc_target_xs_col = pd.concat([flows1st_target_xs, pred_wse_mont], axis=1)
        else:
            bc_target_xs_col = pd.concat(
                [flows1st_target_xs, pd.DataFrame(pred_wse_flows1st_target_xs)], axis=1
            )

        bc_target_xs = bc_target_xs_col.set_axis(['discharge', 'wse'], axis=1)

        list_bc_target_xs_huc8.append(bc_target_xs)

    # -------------------------------------------------
    # Create profile names
    profile_names = fn_create_profile_names(first_pass_flows_xs_wse_df.index, '_ft')

    # TODO make all src monotonic
    # TODO optimize k-not point

    return list_bc_target_xs_huc8, profile_names


# -------------------------------------------------
# Compute BCs for the RAS parent models with normal depth (slope) BC
def compute_boundray_condition_nd(str_path_to_flow_file_nd):
    int_fn_starting_flow = 1  # cfs
    int_number_of_steps = 76

    list_str_slope_bc_nd = []
    list_first_pass_flows_xs_nd = []
    list_num_of_flow_change_xs_nd = []

    for path_in in range(len(str_path_to_flow_file_nd)):
        # Get max flow for each xs in which flow changes in a dataframe format
        # path_in = 1
        max_flow_df_nd = fn_get_flow_dataframe(str_path_to_flow_file_nd[path_in])

        # Number of XSs where flow changes for each ras model with normal depth BC
        list_num_of_flow_change_xs_nd.append(len(max_flow_df_nd['start_xs']))

        # -------------------------------------------------
        # Create firstpass flow dataframe for each xs in which flow changes
        # Normal depth BC
        first_pass_flows_xs_nd = []
        for num_xs2 in range(len(max_flow_df_nd)):
            int_fn_max_flow2 = int(max_flow_df_nd['max_flow'][num_xs2])
            list_first_pass_flows2 = fn_create_firstpass_flowlist(
                int_fn_starting_flow, int_fn_max_flow2, int_number_of_steps
            )
            first_pass_flows_xs_nd.append(list_first_pass_flows2)

        first_pass_flows_xs_nd_df = pd.DataFrame(first_pass_flows_xs_nd).T
        first_pass_flows_xs_nd_df.columns = [int(j2) for j2 in max_flow_df_nd['start_xs']]

        list_first_pass_flows_xs_nd.append(first_pass_flows_xs_nd)

        # read the slope from parent ras model
        file_flow2 = open(str_path_to_flow_file_nd[path_in], 'r')
        lines_flow2 = file_flow2.readlines()

        for flines2 in lines_flow2:
            if flines2[:8] == "Dn Slope":
                list_str_slope_bc_nd.append(flines2[9:])
                break

        file_flow2.close()

    return list_first_pass_flows_xs_nd, list_str_slope_bc_nd


# list_bc_target_xs_huc8, profile_names = compute_boundray_condition_wse(
#     str_path_to_flow_file_wse, str_path_to_geo_file_wse
# )
# list_first_pass_flows_xs_nd, list_str_slope_bc_nd = compute_boundray_condition_nd(str_path_to_flow_file_nd)


# -------------------------------------------------
# Create the HEC-RAS Flow file
# Normal Depth BC
# -------------------------------------------------
def create_ras_flow_file_nd(
    huc8_num,
    path_to_conflated_streams_csv,
    str_path_to_flow_file_nd,
    profile_names,
    list_str_slope_bc_nd,
    list_first_pass_flows_xs_nd,
    str_output_filepath,
):
    # Reads the name of all folders in the
    # parent ras models directory which are conflated
    # "02_csv_from_conflation":
    # Hard-coded as the name of output folder for step 2
    # "path_to_ras_models_4step5.csv":
    # Hard-coded as the name of output csv file for step 2
    str_path_to_csv = (
        path_to_conflated_streams_csv
        + "//"
        + "02_csv_from_conflation"
        + "//"
        + "path_to_ras_models_4step5.csv"
    )

    path_conflated_streams = pd.read_csv(str_path_to_csv)
    path_conflated_models = list(path_conflated_streams['ras_path'])

    folder_names_conflated = []
    for pcm_in in range(len(path_conflated_models)):
        path_to_conflated_models_splt = path_conflated_models[pcm_in].split("\\")

        folder_names_conflated.append(path_to_conflated_models_splt[-2])

    path_to_parent_ras = path_to_conflated_models_splt[0]

    # -------------------------------------------------
    int_number_of_steps = 76

    for path_in in range(len(str_path_to_flow_file_nd)):
        path_to_flow_file_nd_splt = str_path_to_flow_file_nd[path_in].split("\\")
        path_newras_nd = (
            str_output_filepath
            + "/"
            + "05_ras2fim_worker"
            + "/"
            + "ras2fim_v2_models_"
            + huc8_num
            + "/"
            + path_to_flow_file_nd_splt[1]
        )

        # Copy and paste parent ras models in the new ras model directory
        for folders in folder_names_conflated:
            if folders == path_to_flow_file_nd_splt[1]:
                source = path_to_parent_ras + "/" + folders
                destination = path_newras_nd

                try:
                    shutil.copytree(source, destination)
                except OSError as exc:  # python >2.5
                    if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                        shutil.copy(source, destination)
                    else:
                        raise

                # removing old flow file in the new directory
                old_flow_file_path_v2 = path_newras_nd + "/" + path_to_flow_file_nd_splt[2]
                os.remove(old_flow_file_path_v2)

                break

        # Get max flow for each xs in which flow changes in a dataframe format
        max_flow_df_nd = fn_get_flow_dataframe(str_path_to_flow_file_nd[path_in])

        # Number of XSs where flow changes for each ras model with normal depth BC
        int_num_of_flow_change_xs_nd = len(max_flow_df_nd['start_xs'])

        # All text up to the first cross section - Header of the Flow File
        with open(str_path_to_flow_file_nd[path_in]) as flow_file2:  # str_read_geom_file_path
            flowfile_contents2 = flow_file2.read()

        # Get River, reach and Upstream XS for flow file
        pattern_river = re.compile(r"River Rch & RM=.*")
        matches_river = pattern_river.finditer(flowfile_contents2)

        for match in matches_river:
            str_river_reach = flowfile_contents2[match.start() : match.end()]
            # split the data on the comma
            list_river_reach_s = str_river_reach.split(",")
            # Get from array - use strip to remove whitespace
            str_river = list_river_reach_s[0].strip()
            str_reach = list_river_reach_s[1].strip()

        # -------------------------------------------------
        # Write the flow file for normal depth BC
        str_flowfile2 = "Flow Title=BLE_"
        str_flowfile2 += str_river[15:] + "\n"  # str_feature_id
        str_flowfile2 += "Program Version=6.3" + "\n"
        str_flowfile2 += "BEGIN FILE DESCRIPTION:" + "\n"
        str_flowfile2 += "Flow File - Created from Base Level Engineering"
        str_flowfile2 += " data for Flood Inundation Library" + "\n"
        str_flowfile2 += "END FILE DESCRIPTION:" + "\n"
        str_flowfile2 += "Number of Profiles= " + str(int_number_of_steps) + "\n"
        str_flowfile2 += profile_names + "\n"

        for fc2 in range(int_num_of_flow_change_xs_nd):
            # list of the first pass flows
            list_firstflows2 = list_first_pass_flows_xs_nd[path_in][fc2]

            str_xs_upstream_nd = str(int(max_flow_df_nd['start_xs'][fc2]))
            str_flowfile2 += str_river + "," + str_reach + "," + str_xs_upstream_nd + "\n"

            str_flowfile2 += fn_format_flow_values(list_firstflows2) + "\n"

        for m2 in range(int_number_of_steps):
            str_flowfile2 += "Boundary for River Rch & Prof#="

            str_flowfile2 += str_river[15:] + "," + str_reach + ", " + str(m2 + 1) + "\n"

            str_flowfile2 += "Up Type= 0 " + "\n"
            str_flowfile2 += "Dn Type= 3 " + "\n"

            str_flowfile2 += "Dn Slope=" + list_str_slope_bc_nd[path_in]

        str_flowfile2 += "DSS Import StartDate=" + "\n"
        str_flowfile2 += "DSS Import StartTime=" + "\n"
        str_flowfile2 += "DSS Import EndDate=" + "\n"
        str_flowfile2 += "DSS Import EndTime=" + "\n"
        str_flowfile2 += "DSS Import GetInterval= 0 " + "\n"
        str_flowfile2 += "DSS Import Interval=" + "\n"
        str_flowfile2 += "DSS Import GetPeak= 0 " + "\n"
        str_flowfile2 += "DSS Import FillOption= 0 " + "\n"

        new_flow_file_path_v2 = old_flow_file_path_v2
        file2 = open(new_flow_file_path_v2, "w")  # str_feature_id
        file2.write(str_flowfile2)
        file2.close()


# str_output_filepath = "C:/ras2fim_data/OWP_ras_models/ras2fimv2.0/v2_outputs"
# huc8_num = "12090301"

# create_ras_flow_file_nd(
#     huc8_num,
#     path_to_conflated_streams_csv,
#     str_path_to_flow_file_nd,
#     profile_names,
#     list_str_slope_bc_nd,
#     list_first_pass_flows_xs_nd,
#     str_output_filepath,
# )


# -------------------------------------------------
# Create the HEC-RAS Flow file
# Water surface elevation BC
# -------------------------------------------------
def create_ras_flow_file_wse(
    huc8_num,
    path_to_conflated_streams_csv,
    str_path_to_flow_file_wse,
    profile_names,
    list_bc_target_xs_huc8,
    str_output_filepath,
):
    # Reads the name of all folders in the
    # parent ras models directory which are conflated
    # "02_csv_from_conflation":
    # Hard-coded as the name of output folder from step 2
    # "path_to_ras_models_4step5.csv":
    # Hard-coded as the name of output csv file from step 2
    str_path_to_csv = (
        path_to_conflated_streams_csv
        + "//"
        + "02_csv_from_conflation"
        + "//"
        + "path_to_ras_models_4step5.csv"
    )

    path_conflated_streams = pd.read_csv(str_path_to_csv)
    path_conflated_models = list(path_conflated_streams['ras_path'])

    folder_names_conflated = []
    for pcm_in in range(len(path_conflated_models)):
        path_to_conflated_models_splt = path_conflated_models[pcm_in].split("\\")

        folder_names_conflated.append(path_to_conflated_models_splt[-2])

    path_to_parent_ras = path_to_conflated_models_splt[0]

    # -------------------------------------------------
    int_fn_starting_flow = 1  # cfs
    int_number_of_steps = 76

    for path_in in range(len(str_path_to_flow_file_wse)):
        path_to_flow_file_wse_splt = str_path_to_flow_file_wse[path_in].split("\\")
        path_newras_wse = (
            str_output_filepath
            + "/"
            + "05_ras2fim_worker"
            + "/"
            + "ras2fim_v2_models_"
            + huc8_num
            + "/"
            + path_to_flow_file_wse_splt[1]
        )

        # Copy and paste parent ras models in the new ras model directory
        for folders in folder_names_conflated:
            if folders == path_to_flow_file_wse_splt[1]:
                source = path_to_parent_ras + "/" + folders
                destination = path_newras_wse

                # if os.path.isfile(source):
                try:
                    shutil.copytree(source, destination)
                except OSError as exc:  # python >2.5
                    if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                        shutil.copy(source, destination)
                    else:
                        raise

                # removing old flow file in the new directory
                old_flow_file_path_wse = path_newras_wse + "/" + path_to_flow_file_wse_splt[2]
                os.remove(old_flow_file_path_wse)

                break

        # -------------------------------------------------
        # Create firstpass flow dataframe for each xs in which flow changes
        # Water surface elevation BC

        # Get max flow for each xs in which flow changes in a dataframe format
        max_flow_df_wse = fn_get_flow_dataframe(str_path_to_flow_file_wse[path_in])

        first_pass_flows_xs_wse = []
        for num_xs in range(len(max_flow_df_wse)):
            int_fn_max_flow = int(max_flow_df_wse['max_flow'][num_xs])
            list_first_pass_flows = fn_create_firstpass_flowlist(
                int_fn_starting_flow, int_fn_max_flow, int_number_of_steps
            )
            first_pass_flows_xs_wse.append(list_first_pass_flows)

        # -------------------------------------------------
        # Writing the HEC-RAS Flow file for WSE BC

        # All text up to the first cross section - Header of the Flow File
        with open(str_path_to_flow_file_wse[path_in]) as flow_file:  # str_read_geom_file_path
            flowfile_contents = flow_file.read()

        # Get River, reach and Upstream XS for flow file
        pattern_river = re.compile(r"River Rch & RM=.*")
        matches_river = pattern_river.finditer(flowfile_contents)

        for match in matches_river:
            str_river_reach = flowfile_contents[match.start() : match.end()]
            # split the data on the comma
            list_river_reach_s = str_river_reach.split(",")
            # Get from array - use strip to remove whitespace
            str_river = list_river_reach_s[0].strip()
            str_reach = list_river_reach_s[1].strip()

        # -------------------------------------------------
        # Write the flow file
        str_flowfile = "Flow Title=BLE_"
        str_flowfile += str_river[15:] + "\n"  # str_feature_id
        str_flowfile += "Program Version=6.3" + "\n"
        str_flowfile += "BEGIN FILE DESCRIPTION:" + "\n"
        str_flowfile += "Flow File - Created from Base Level Engineering"
        str_flowfile += " data for Flood Inundation Library" + "\n"
        str_flowfile += "END FILE DESCRIPTION:" + "\n"
        str_flowfile += "Number of Profiles= " + str(int_number_of_steps) + "\n"
        str_flowfile += profile_names + "\n"

        # Number of XSs where flow changes for each ras model with normal depth BC
        int_num_of_flow_change_xs = len(max_flow_df_wse['start_xs'])

        for fc in range(int_num_of_flow_change_xs):
            # list of the first pass flows
            list_firstflows = first_pass_flows_xs_wse[fc]

            str_xs_upstream = str(int(max_flow_df_wse['start_xs'][fc]))
            str_flowfile += str_river + "," + str_reach + "," + str_xs_upstream + "\n"

            str_flowfile += fn_format_flow_values(list_firstflows) + "\n"

        bc_target_xs = list_bc_target_xs_huc8[path_in]

        for m in range(int_number_of_steps):
            str_flowfile += "Boundary for River Rch & Prof#="

            str_flowfile += str_river[15:] + "," + str_reach + ", " + str(m + 1) + "\n"

            str_flowfile += "Up Type= 0 " + "\n"
            str_flowfile += "Dn Type= 1 " + "\n"

            str_known_ws = str(round(bc_target_xs['wse'][m], 3))
            str_flowfile += "Dn Known WS=" + str_known_ws + "\n"  # Dn Slope=0.005

        str_flowfile += "DSS Import StartDate=" + "\n"
        str_flowfile += "DSS Import StartTime=" + "\n"
        str_flowfile += "DSS Import EndDate=" + "\n"
        str_flowfile += "DSS Import EndTime=" + "\n"
        str_flowfile += "DSS Import GetInterval= 0 " + "\n"
        str_flowfile += "DSS Import Interval=" + "\n"
        str_flowfile += "DSS Import GetPeak= 0 " + "\n"
        str_flowfile += "DSS Import FillOption= 0 " + "\n"

        new_flow_file_path_wse = old_flow_file_path_wse
        file = open(new_flow_file_path_wse, "w")
        file.write(str_flowfile)
        file.close()
