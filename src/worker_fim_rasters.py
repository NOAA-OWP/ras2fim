# Creates HEC-RAS files and first run depth grids
# Uses the 'ras2fim' conda environment

# -------------------------------------------------
# import argparse
import errno
import os
import pathlib
import re
import shutil
import traceback

import numpy as np
import pandas as pd
import win32com.client

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG  # the non mp version
MP_LOG = ras2fim_logger.RAS2FIM_logger()  # mp version

# This routine uses RAS630.HECRASController (HEC-RAS v6.0.0 must be
# installed on this machine prior to execution)


# -------------------------------------------------
def fn_create_firstpass_flowlist(int_fn_starting_flow, int_fn_max_flow, int_fn_number_of_steps):
    # create a list of flows for the first pass HEC-RAS

    list_first_pass_flows = []

    int_fn_deltaflowstep = int(int_fn_max_flow // (int_fn_number_of_steps - 2))

    for i in range(int_fn_number_of_steps):
        list_first_pass_flows.append(int_fn_starting_flow + (i * int_fn_deltaflowstep))

    return list_first_pass_flows


# -------------------------------------------------
def fn_create_profile_names(list_profiles, str_suffix):
    str_profile_names = 'Profile Names='

    for i in range(len(list_profiles)):
        str_profile_names += 'flow' + str(list_profiles[i]) + str_suffix
        if i < (len(list_profiles) - 1):
            str_profile_names = str_profile_names + ','

    return str_profile_names


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
def fn_get_flow_dataframe(str_path_hecras_flow_fn):
    # Get pandas dataframe of the flows in the active plan's flow file

    # initalize the dataframe
    df = pd.DataFrame()
    df['river'] = []
    df['reach'] = []
    df['start_xs'] = []
    df['max_flow'] = []

    with open(str_path_hecras_flow_fn, 'r') as hecras_flow_file:
        hecras_flow_lines = hecras_flow_file.readlines()
        i = 0  # number of the current row

        for line in hecras_flow_lines:
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
                    line_flows = hecras_flow_lines[j]

                    # determine the number of values on this
                    # line from character count
                    int_val_in_row = int((len(hecras_flow_lines[j]) - 1) / 8)

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
# Reading original parent models flow and geometry files
# with WSE and normal depth (ND, slope) BCs
# and create a seperate list of paths to
# flow and geometry files WSE and ND BCs
def create_list_of_paths_flow_geometry_files_4each_BCs(path_to_conflated_streams_csv):
    # Reads the name of all folders in the
    # parent ras models directory which are conflated

    # "02_csv_from_conflation":
    # Hard-coded as the name of output folder for step 2
    # "conflated_ras_models.csv":
    # Hard-coded as the name of output csv file for step 2
    str_path_to_csv = os.path.join(path_to_conflated_streams_csv, "conflated_ras_models.csv")

    path_conflated_streams = pd.read_csv(str_path_to_csv)
    path_conflated_models = list(path_conflated_streams['ras_path'])

    ls_path_flowfiles = [paths[:-3] + "f01" for paths in path_conflated_models]

    # List of flow file paths
    # Water surface elevation BC
    ls_path_to_flow_file_wse = []
    for fpath in ls_path_flowfiles:
        with open(fpath, 'r') as file_flow:
            lines_flow = file_flow.readlines()

            for flines in lines_flow:
                if flines[:11] == "Dn Known WS":
                    ls_path_to_flow_file_wse.append(fpath)
                    break
            file_flow.close()

    # Normal depth BC
    ls_path_to_flow_file_nd = []
    for fpath2 in ls_path_flowfiles:
        with open(fpath2, 'r') as file_flow2:
            lines_flow2 = file_flow2.readlines()

            for flines2 in lines_flow2:
                if flines2[:8] == "Dn Slope":
                    ls_path_to_flow_file_nd.append(fpath2)
                    break
            file_flow2.close()

    # List of geometry file paths
    ls_path_to_geo_file_wse = []
    for fpath_wse in ls_path_to_flow_file_wse:
        gpath = fpath_wse[:-3] + "g01"
        ls_path_to_geo_file_wse.append(gpath)

    ls_path_to_geo_file_nd = []
    for fpath_nd in ls_path_to_flow_file_nd:
        gpath2 = fpath_nd[:-3] + "g01"
        ls_path_to_geo_file_nd.append(gpath2)

    return (
        ls_path_to_flow_file_wse,
        ls_path_to_flow_file_nd,
        ls_path_to_geo_file_wse,
        ls_path_to_geo_file_nd,
    )


# -------------------------------------------------
# Compute BC (75 flows/WSE) for the parent RAS models with WSE BC
def compute_boundray_condition_wse(
    int_fn_starting_flow, int_number_of_steps, ls_path_to_flow_file_wse, ls_path_to_geo_file_wse
):
    list_bc_target_xs_huc8 = []
    for path_in in range(len(ls_path_to_flow_file_wse)):
        # Get max flow for each xs in which flow changes in a dataframe format
        max_flow_df_wse = fn_get_flow_dataframe(ls_path_to_flow_file_wse[path_in])

        # -------------------------------------------------
        # Create firstpass flow dataframe for each xs in which flow changes
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
        str_path_hecras_flow_fn = ls_path_to_flow_file_wse[path_in]

        with open(str_path_hecras_flow_fn, 'r') as hecras_flow_file:
            hecras_flow_lines = hecras_flow_file.readlines()
            i = 0  # number of the current row
            list_all_flow_values = []

            for line in hecras_flow_lines:
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
                        line_flows = hecras_flow_lines[j]

                        # determine the number of values on this
                        # line from character count
                        int_val_in_row = int((len(hecras_flow_lines[j]) - 1) / 8)

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
            for line in hecras_flow_lines:
                if line[:12] == 'Dn Known WS=':
                    # determine the number of profiles
                    WSE = float(line[12:])
                    target_xs_wse.append(WSE)

            target_xs_wse_df = pd.DataFrame(target_xs_wse, columns=['wse'])

            # Dataframe of known WSE BC (flow and wse)
            bc_df = pd.concat([target_xs_flows_df, target_xs_wse_df], axis=1)
            bc_sort_df = bc_df.sort_values(by=['discharge'])

            hecras_flow_file.close()

        # -------------------------------------------------
        # Finding the last cross section (target XS for min elevation)
        # Water surface elevation BC
        str_path_hecras_geo_fn = ls_path_to_geo_file_wse[path_in]  #

        with open(str_path_hecras_geo_fn, 'r') as file_geo:
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

    # TODO optimize k-not point

    return list_bc_target_xs_huc8


# -------------------------------------------------
# Compute BCs for the RAS parent models with normal depth (slope) BC
def compute_boundray_condition_nd(int_fn_starting_flow, int_number_of_steps, ls_path_to_flow_file_nd):
    list_str_slope_bc_nd = []
    list_first_pass_flows_xs_nd = []
    list_num_of_flow_change_xs_nd = []

    for path_in in range(len(ls_path_to_flow_file_nd)):
        # Get max flow for each xs in which flow changes in a dataframe format
        # path_in = 1
        max_flow_df_nd = fn_get_flow_dataframe(ls_path_to_flow_file_nd[path_in])

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
        with open(ls_path_to_flow_file_nd[path_in], 'r') as file_flow2:
            lines_flow2 = file_flow2.readlines()

            for flines2 in lines_flow2:
                if flines2[:8] == "Dn Slope":
                    list_str_slope_bc_nd.append(flines2[9:])
                    break

            file_flow2.close()

    return list_first_pass_flows_xs_nd, list_str_slope_bc_nd


# -------------------------------------------------
# Create the HEC-RAS Flow file
# Normal Depth BC ~ 40 s
# -------------------------------------------------
def create_ras_flow_file_nd(
    huc8_num,
    int_number_of_steps,
    path_to_conflated_streams_csv,
    ls_path_to_flow_file_nd,
    profile_names,
    list_str_slope_bc_nd,
    list_first_pass_flows_xs_nd,
    str_output_filepath,
):
    # Reads the name of all folders in the
    # parent ras models directory which are conflated
    # "02_csv_shapes_from_conflation":
    # Hard-coded as the name of output folder for step 2
    # "conflated_ras_models.csv":
    # Hard-coded as the name of output csv file for step 2
    str_path_to_csv = os.path.join(path_to_conflated_streams_csv, "conflated_ras_models.csv")

    path_conflated_streams = pd.read_csv(str_path_to_csv)
    path_conflated_models = list(path_conflated_streams['ras_path'])
    path_conflated_models_splt = [path.split("\\") for path in path_conflated_models]
    conflated_model_names = [names[-2] for names in path_conflated_models_splt]

    # TODO: path_model_catalog
    path_model_catalog = os.path.join(str_output_filepath, "OWP_ras_models_catalog_" + huc8_num + ".csv")

    model_catalog = pd.read_csv(path_model_catalog)
    models_name_id = pd.concat([model_catalog["final_name_key"], model_catalog["model_id"]], axis=1)

    final_name_key = list(models_name_id["final_name_key"])
    conflated_model_names_id = []
    for nms in conflated_model_names:
        indx = final_name_key.index(nms)

        name_id = list(models_name_id.iloc[indx])

        conflated_model_names_id.append(name_id)

    conflated_model_names_id_df = pd.DataFrame(
        conflated_model_names_id, columns=["final_name_key", "model_id"]
    )

    folder_names_conflated = list(conflated_model_names_id_df["final_name_key"])

    path_to_parent_ras = pathlib.PurePath(path_conflated_models[0]).parents[1]

    # -------------------------------------------------
    for path_in in range(len(ls_path_to_flow_file_nd)):
        path_to_flow_file_nd_splt = ls_path_to_flow_file_nd[path_in].split("\\")

        model_ids = str(
            list(
                conflated_model_names_id_df["model_id"][
                    path_to_flow_file_nd_splt[-2] == conflated_model_names_id_df["final_name_key"]
                ]
            )[0]
        )

        path_newras_nd = os.path.join(
            str_output_filepath,
            sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT,
            model_ids + "_" + path_to_flow_file_nd_splt[-2][8:-15],
        )

        # Copy and paste conflated parent ras models in the output ras model directory
        for folders in folder_names_conflated:
            if folders == path_to_flow_file_nd_splt[-2]:
                source = os.path.join(str(path_to_parent_ras), folders)
                destination = path_newras_nd

                try:
                    shutil.copytree(source, destination)
                except OSError as exc:  # python >2.5
                    if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                        shutil.copy(source, destination)
                    else:
                        raise

                break

        # Get max flow for each xs in which flow changes in a dataframe format
        max_flow_df_nd = fn_get_flow_dataframe(ls_path_to_flow_file_nd[path_in])

        # Number of XSs where flow changes for each ras model with normal depth BC
        int_num_of_flow_change_xs_nd = len(max_flow_df_nd['start_xs'])

        # All text up to the first cross section - Header of the Flow File
        with open(ls_path_to_flow_file_nd[path_in]) as flow_file2:  # str_read_geom_file_path
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
        str_flowfile2 = "Flow Title="
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

        new_flow_file_path_v2 = os.path.join(path_newras_nd, path_to_flow_file_nd_splt[-1])
        with open(new_flow_file_path_v2, "w") as file2:  # str_feature_id
            file2.write(str_flowfile2)
            file2.close()


# -------------------------------------------------
# Create the HEC-RAS Flow file
# Water surface elevation BC ~ 10 S
# -------------------------------------------------
def create_ras_flow_file_wse(
    huc8_num,
    int_fn_starting_flow,
    int_number_of_steps,
    path_to_conflated_streams_csv,
    ls_path_to_flow_file_wse,
    profile_names,
    list_bc_target_xs_huc8,
    str_output_filepath,
):
    # Reads the name of all folders in the
    # parent ras models directory which are conflated
    # "02_csv_shapes_from_conflation":
    # Hard-coded as the name of output folder for step 2
    # "conflated_ras_models.csv":
    # Hard-coded as the name of output csv file for step 2

    str_path_to_csv = os.path.join(path_to_conflated_streams_csv, "conflated_ras_models.csv")

    path_conflated_streams = pd.read_csv(str_path_to_csv)
    path_conflated_models = list(path_conflated_streams['ras_path'])
    path_conflated_models_splt = [path.split("\\") for path in path_conflated_models]
    conflated_model_names = [names[-2] for names in path_conflated_models_splt]


    path_model_catalog = os.path.join(str_output_filepath, "OWP_ras_models_catalog_" + huc8_num + ".csv")

    model_catalog = pd.read_csv(path_model_catalog)
    models_name_id = pd.concat([model_catalog["final_name_key"], model_catalog["model_id"]], axis=1)

    final_name_key = list(models_name_id["final_name_key"])
    conflated_model_names_id = []
    for nms in conflated_model_names:
        indx = final_name_key.index(nms)

        name_id = list(models_name_id.iloc[indx])

        conflated_model_names_id.append(name_id)

    conflated_model_names_id_df = pd.DataFrame(
        conflated_model_names_id, columns=["final_name_key", "model_id"]
    )

    folder_names_conflated = list(conflated_model_names_id_df["final_name_key"])

    path_to_parent_ras = pathlib.PurePath(path_conflated_models[0]).parents[1]

    for path_in in range(len(ls_path_to_flow_file_wse)):
        path_to_flow_file_wse_splt = ls_path_to_flow_file_wse[path_in].split("\\")

        model_ids = str(
            list(
                conflated_model_names_id_df["model_id"][
                    path_to_flow_file_wse_splt[-2] == conflated_model_names_id_df["final_name_key"]
                ]
            )[0]
        )

        path_newras_wse = os.path.join(
            str_output_filepath,
            sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT,
            model_ids + "_" + path_to_flow_file_wse_splt[-2][8:-15],
        )

        # Copy and paste conflated parent ras models in the new ras model directory
        for folders in folder_names_conflated:
            if folders == path_to_flow_file_wse_splt[-2]:
                source = os.path.join(str(path_to_parent_ras), folders)
                destination = path_newras_wse

                # if os.path.isfile(source):
                try:
                    shutil.copytree(source, destination, dirs_exist_ok=True)
                except OSError as exc:  # python >2.5
                    if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                        shutil.copy(source, destination, dirs_exist_ok=True)
                    else:
                        raise

                break

        # -------------------------------------------------
        # Create firstpass flow dataframe for each xs in which flow changes
        # Water surface elevation BC

        # Get max flow for each xs in which flow changes in a dataframe format
        max_flow_df_wse = fn_get_flow_dataframe(ls_path_to_flow_file_wse[path_in])

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
        with open(ls_path_to_flow_file_wse[path_in]) as flow_file:  # str_read_geom_file_path
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
        str_flowfile = "Flow Title="
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

        new_flow_file_path_wse = os.path.join(path_newras_wse, path_to_flow_file_wse_splt[-1])
        with open(new_flow_file_path_wse, "w") as file:
            file.write(str_flowfile)
            file.close()


# -------------------------------------------------
# Create the HEC-RAS Plan file
# All RAS Models BC ~ 5 S
# -------------------------------------------------
def create_ras_plan_file(str_output_filepath):
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    str_plan_middle_path = os.path.join(current_script_dir, "PlanStandardText01.txt")
    str_plan_footer_path = os.path.join(current_script_dir, "PlanStandardText02.txt")

    # The name of output folder is hard-coded
    path_v2ras = os.path.join(str_output_filepath, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)

    folder_names_conflated = os.listdir(path_v2ras)

    for folder in folder_names_conflated:
        str_planfile = "Plan Title="
        str_planfile += folder[6:] + "\n"
        str_planfile += "Program Version=5.07" + "\n"
        str_planfile += "Short Identifier=" + folder[6:] + "\n"

        # read a file and append to the str_planfile string
        # str_planFooterPath
        # To map the requested Depth Grids

        # read the plan middle input file
        with open(str_plan_middle_path) as f:
            file_contents = f.read()
        str_planfile += file_contents

        # TODO: To map the requested Depth Grids
        # Set to 'Run RASMapper=0 ' to not create requested DEMs
        # Set to 'Run RASMapper=-1 ' to create requested DEMs
        # if is_create_maps:
        #     str_planfile += "\n" + r"Run RASMapper=-1 " + "\n"
        # else:
        #     str_planfile += "\n" + r"Run RASMapper=0 " + "\n"

        str_planfile += "\n" + r"Run RASMapper=-1 " + "\n"

        # read the plan footer input file
        with open(str_plan_footer_path) as f:
            file_contents = f.read()
        str_planfile += file_contents

        path_plan = os.path.join(
            str_output_filepath, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT, folder, folder[6:] + ".p01"
        )
        with open(path_plan, "w") as file:
            file.write(str_planfile)
            file.close()


# -------------------------------------------------
# Create the HEC-RAS Project file
# All RAS Models BC ~ 5 S
# -------------------------------------------------
def create_ras_project_file(str_output_filepath, model_unit):
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    str_project_footer_path = os.path.join(current_script_dir, "ProjectStandardText01.txt")

    # The name of output folder is hard-coded
    path_v2ras = os.path.join(str_output_filepath, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)

    folder_names_conflated = os.listdir(path_v2ras)

    for folder in folder_names_conflated:
        str_projectfile = "Proj Title="
        str_projectfile += folder[6:] + "\n"

        str_projectfile += "Current Plan=p01" + "\n"
        str_projectfile += "Default Exp/Contr=0.3,0.1" + "\n"

        # Set up project as either SI of English Units
        if model_unit == "meter":
            str_projectfile += "SI Units" + "\n"
        else:
            # English Units
            str_projectfile += "English Units" + "\n"

        # read the project footer input file
        with open(str_project_footer_path) as f:
            file_contents = f.read()
        str_projectfile += file_contents

        path_project = os.path.join(
            str_output_filepath, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT, folder, folder[6:] + ".prj"
        )
        with open(path_project, "w") as file:
            file.write(str_projectfile)
            file.close()


# -------------------------------------------------
# Create the HEC-RAS mapper xml file
# All RAS Models BC ~ 10 S
# -------------------------------------------------
def create_ras_mapper_xml(huc8_num, int_number_of_steps, str_output_filepath, model_unit):
    # Function to create the RASMapper XML and add the requested DEM's to be created

    path_v2ras = os.path.join(str_output_filepath, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)

    folder_names_conflated = os.listdir(path_v2ras)

    str_river_id_fn = [folders[6:] for folders in folder_names_conflated]

    terrain_names = [folders[:5] for folders in folder_names_conflated]

    str_output_filepath_xml = str_output_filepath.replace("/", "\\")
    str_path_to_terrain = os.path.join(str_output_filepath_xml, sv.R2F_OUTPUT_DIR_HECRAS_TERRAIN)

    # -------------------------------------------------

    # TODO: profile_names for second path flow

    list_step_profiles_xml_fn = ["flow_" + str(nms) for nms in range(int_number_of_steps)]

    # -------------------------------------------------
    # Create .rasmap XML file for all conflated models (new ras models)

    str_path_to_projection = os.path.join(
        str_output_filepath_xml, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF, huc8_num + "_huc_12_ar.prj"
    )

    for xy in range(len(str_river_id_fn)):
        str_ras_mapper_file = ""

        str_ras_mapper_file = r"<RASMapper>" + "\n"
        str_ras_mapper_file += r"  <Version>2.0.0</Version>" + "\n"

        str_ras_mapper_file += (
            r'  <RASProjectionFilename Filename="' + str_path_to_projection + r'" />' + "\n"
        )

        str_ras_mapper_file += r'  <Geometries Checked="True" Expanded="True">' + "\n"

        str_ras_mapper_file += r'    <Layer Name="' + str_river_id_fn[xy] + '"'
        str_ras_mapper_file += r' Type="RASGeometry" Checked="True" '
        str_ras_mapper_file += r'Expanded="True" Filename="'

        str_ras_mapper_file += r"." + "\\" + str_river_id_fn[xy] + '.g01.hdf">' + "\n"

        str_ras_mapper_file += r'      <Layer Type="RAS'
        str_ras_mapper_file += r'River" Checked="True" />' + "\n"
        str_ras_mapper_file += r'      <Layer Type="RASXS" Checked'
        str_ras_mapper_file += r'="True" />' + "\n"
        str_ras_mapper_file += r"    </Layer>" + "\n"
        str_ras_mapper_file += r"  </Geometries>" + "\n"

        str_ras_mapper_file += r'  <Results Expanded="True">' + "\n"
        str_ras_mapper_file += r'    <Layer Name="'
        str_ras_mapper_file += str_river_id_fn[xy] + '" Type="RAS' + 'Results" Expanded="True" Filename=".'
        str_ras_mapper_file += "\\" + str_river_id_fn[xy] + r'.p01.hdf">' + "\n"
        str_ras_mapper_file += '      <Layer Type="RASGeometry" Filename=".'
        str_ras_mapper_file += "\\" + str_river_id_fn[xy] + r'.p01.hdf" />' + "\n"

        int_index = 0

        # Loop through all profiles and create an XML request to map each depth
        # grid in the list_step_profiles_xml_fn
        for i in list_step_profiles_xml_fn:
            str_ras_mapper_file += '      <Layer Name="depth" Type="RAS'
            str_ras_mapper_file += 'ResultsMap" Checked="True" Filename=".'

            if model_unit == "meter":
                str_ras_mapper_file += (
                    "\\" + str_river_id_fn[xy] + "\\" + "Depth_" + str(i) + 'm.vrt">' + "\n"
                )
            else:
                str_ras_mapper_file += (
                    "\\" + str_river_id_fn[xy] + "\\" + "Depth_" + str(i) + 'ft.vrt">' + "\n"
                )

            str_ras_mapper_file += "        <LabelFeatures "
            str_ras_mapper_file += 'Checked="True" Center="False" '
            str_ras_mapper_file += 'rows="1" cols="1" r0c0="FID" '
            str_ras_mapper_file += 'Position="5" Color="-16777216" />' + "\n"
            str_ras_mapper_file += '        <MapParameters MapType="depth" Layer'
            str_ras_mapper_file += 'Name="Depth" OutputMode="Stored Current '

            if model_unit == "meter":
                str_ras_mapper_file += (
                    'Terrain" StoredFilename=".\\' + str_river_id_fn[xy] + "\\Depth_" + str(i) + 'm.vrt"'
                )
            else:
                str_ras_mapper_file += (
                    'Terrain" StoredFilename=".\\' + str_river_id_fn[xy] + "\\Depth_" + str(i) + 'ft.vrt"'
                )

            str_ras_mapper_file += (
                ' Terrain="' + terrain_names[xy] + '" ProfileIndex="' + str(int_index) + '" '
            )
            str_ras_mapper_file += ' ProfileName="' + str(i) + 'm" ArrivalDepth="0" />' + "\n"
            str_ras_mapper_file += "      </Layer>" + "\n"

            int_index += 1

        # Get the highest (last profile) flow innundation polygon
        # --------------------
        str_ras_mapper_file += '      <Layer Name="depth" Type="RAS'
        str_ras_mapper_file += 'ResultsMap" Checked="True" Filename=".'

        str_ras_mapper_file += (
            "\\" + str_river_id_fn[xy] + "\\" + "Inundation Boundary_" + str(list_step_profiles_xml_fn[-1])
        )

        str_ras_mapper_file += 'ft Value_0.shp">' + "\n"
        str_ras_mapper_file += '        <MapParameters MapType="depth" '
        str_ras_mapper_file += 'LayerName="Inundation Boundary"'
        str_ras_mapper_file += ' OutputMode="Stored Polygon'
        str_ras_mapper_file += ' Specified Depth"  StoredFilename=".'
        str_ras_mapper_file += (
            "\\" + str_river_id_fn[xy] + "\\" + "Inundation Boundary_" + str(list_step_profiles_xml_fn[-1])
        )
        str_ras_mapper_file += (
            'm Value_0.shp"  Terrain="'
            + terrain_names[xy]
            + '" ProfileIndex="'
            + str(len(list_step_profiles_xml_fn) - 1)
        )
        str_ras_mapper_file += (
            '"  ProfileName="' + str(list_step_profiles_xml_fn[-1]) + 'm"  ArrivalDepth="0" />' + "\n"
        )
        str_ras_mapper_file += "      </Layer>" + "\n"
        # --------------------

        str_ras_mapper_file += r"    </Layer>" + "\n"
        str_ras_mapper_file += r"  </Results>" + "\n"

        str_ras_mapper_file += r'  <Terrains Checked="True" Expanded="True">' + "\n"

        str_ras_mapper_file += (
            r'    <Layer Name="' + terrain_names[xy] + r'" Type="TerrainLayer" Checked="True" Filename="'
        )

        str_ras_mapper_file += str_path_to_terrain + "\\" + terrain_names[xy] + r'.hdf">' + "\n"

        str_ras_mapper_file += r"    </Layer>" + "\n"
        str_ras_mapper_file += r"  </Terrains>" + "\n"

        str_ras_mapper_file += r"</RASMapper>"

        rasmap_path = os.path.join(
            str_output_filepath_xml,
            sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT,
            folder_names_conflated[xy],
            str_river_id_fn[xy] + ".rasmap",
        )
        with open(rasmap_path, "w") as file:
            file.write(str_ras_mapper_file)
            file.close()


# -------------------------------------------------
# Create All HEC-RAS files
# For All RAS Models BC ~ 3 min
# -------------------------------------------------
def create_hecras_files(huc8_num, int_fn_starting_flow, int_number_of_steps, unit_output_folder, model_unit):
    path_to_conflated_streams_csv = os.path.join(unit_output_folder, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF)

    # Reading original parent models flow and geometry files
    # with different WSE and normal depth (ND, slope) BCs
    # and create a seperate list of paths to
    # flow and geometry files for WSE and ND BCs
    [
        ls_path_to_flow_file_wse,
        ls_path_to_flow_file_nd,
        ls_path_to_geo_file_wse,
        ls_path_to_geo_file_nd,
    ] = create_list_of_paths_flow_geometry_files_4each_BCs(path_to_conflated_streams_csv)

    # Compute boundray condition for models with normal depth BCs
    list_first_pass_flows_xs_nd, list_str_slope_bc_nd = compute_boundray_condition_nd(
        int_fn_starting_flow, int_number_of_steps, ls_path_to_flow_file_nd
    )

    # Defining profile names
    # TODO: This needs to be fixed for second pass flow
    # if len(list_first_pass_flows_xs_nd)>0:
    list_profiles = [ns for ns in range(int_number_of_steps)]  # len(list_first_pass_flows_xs_nd[0])
    profile_names_str = fn_create_profile_names(list_profiles, '_ft')
    # else: profile_names = [f'flow{ns}_ft' for ns in range(len(list_bc_target_xs_huc8))]

    # Create the HEC-RAS Flow files Normal Depth BC ~ 40 s
    create_ras_flow_file_nd(
        huc8_num,
        int_number_of_steps,
        path_to_conflated_streams_csv,
        ls_path_to_flow_file_nd,
        profile_names_str,
        list_str_slope_bc_nd,
        list_first_pass_flows_xs_nd,
        unit_output_folder,
    )

    # Compute boundray condition for models with wse BCs and
    # Creating the flow files for wse BC
    if len(ls_path_to_flow_file_wse) > 0:
        list_bc_target_xs_huc8 = compute_boundray_condition_wse(
            int_fn_starting_flow, int_number_of_steps, ls_path_to_flow_file_wse, ls_path_to_geo_file_wse
        )

        # Create the HEC-RAS Flow files for Water Surface Elevation BC ~ 10 s
        create_ras_flow_file_wse(
            huc8_num,
            int_fn_starting_flow,
            int_number_of_steps,
            path_to_conflated_streams_csv,
            ls_path_to_flow_file_wse,
            profile_names_str,
            list_bc_target_xs_huc8,
            unit_output_folder,
        )

    else:
        RLOG.lprint(" ")
        RLOG.lprint("|        There Is No RAS Model With WSE Boundary Condition        |")

    # Create the HEC-RAS plan files ~ 5 s
    create_ras_plan_file(unit_output_folder)

    # Create the HEC-RAS project files ~ 5 s
    create_ras_project_file(unit_output_folder, model_unit)

    # Create the HEC-RAS mapper xml files ~ 5 s
    create_ras_mapper_xml(huc8_num, int_number_of_steps, unit_output_folder, model_unit)


# -------------------------------------------------
# Runs HEC-RAS
# For One RAS Model ~ 3 min
# -------------------------------------------------
def fn_run_hecras(str_ras_projectpath, int_number_of_steps):
    try:
        hec = None

        # Get the river name (instead of str_feature_id in V1)
        list_path = str_ras_projectpath.split("\\")  # is a string

        str_model_id = str(list_path[-2][0:5])

        hec = win32com.client.Dispatch("RAS630.HECRASController")
        # hec.ShowRas()

        hec.Project_Open(str_ras_projectpath)  # opening HEC-RAS

        # to be populated: number and list of messages, blocking mode
        NMsg, TabMsg, block = None, None, True

        # computations of the current plan
        v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

        # ID numbers of the river and the reach
        RivID, RchID = 1, 1

        # to be populated: number of nodes, list of RS and node types
        NNod, TabRS, TabNTyp = None, None, None

        # reading project nodes: cross-sections, bridges, culverts, etc.
        v1, v2, NNod, TabRS, TabNTyp = hec.Geometry_GetNodes(RivID, RchID, NNod, TabRS, TabNTyp)

        # HEC-RAS ID of output variables: Max channel depth, channel reach length,
        # and water surface elevation
        int_max_depth_id, int_node_chan_length, int_water_surface_elev, int_q_total = (4, 42, 2, 9)

        # -------------------------------------------------
        # Saving information for all profiles of all cross sections
        # Water depths, WSE and flows for each XS
        # -------------------------------------------------
        # make a list of unique ids using feature id and cross section name
        all_x_sections_info = pd.DataFrame()

        xsections_fids_xs = [str_model_id + "_" + value.strip() for value in TabRS]
        xsections_fids = [str_model_id for value in TabRS]
        xsections_xs = [value.strip() for value in TabRS]

        for int_prof in range(int_number_of_steps):
            this_profile_x_section_info = pd.DataFrame()
            this_profile_x_section_info["fid_xs"] = np.array(xsections_fids_xs)
            this_profile_x_section_info["modelid"] = np.array(xsections_fids)
            this_profile_x_section_info["Xsection_name"] = np.array(xsections_xs)

            # get a count of the cross sections in the HEC-RAS model
            int_xs_node_count = 0
            for i in range(0, NNod):
                if TabNTyp[i] == "":
                    int_xs_node_count += 1

            # initalize six numpy arrays
            arr_max_depth = np.empty([int_xs_node_count], dtype=float)
            arr_channel_length = np.empty([int_xs_node_count], dtype=float)
            arr_water_surface_elev = np.empty([int_xs_node_count], dtype=float)
            arr_q_total = np.empty([int_xs_node_count], dtype=float)

            int_count_nodes = 0

            for i in range(0, NNod):
                if TabNTyp[i] == "":  # this is a XS (not a bridge, culvert, inline, etc...)
                    # reading max depth in cross section
                    (arr_max_depth[int_count_nodes], v1, v2, v3, v4, v5, v6) = hec.Output_NodeOutput(
                        RivID, RchID, i + 1, 0, int_prof + 1, int_max_depth_id
                    )

                    # reading water surface elevation in cross section
                    (arr_water_surface_elev[int_count_nodes], v1, v2, v3, v4, v5, v6) = hec.Output_NodeOutput(
                        RivID, RchID, i + 1, 0, int_prof + 1, int_water_surface_elev
                    )

                    # reading the distance between cross sections (center of channel)
                    (arr_channel_length[int_count_nodes], v1, v2, v3, v4, v5, v6) = hec.Output_NodeOutput(
                        RivID, RchID, i + 1, 0, int_prof + 1, int_node_chan_length
                    )

                    # reading the Q total of the cross section
                    (arr_q_total[int_count_nodes], v1, v2, v3, v4, v5, v6) = hec.Output_NodeOutput(
                        RivID, RchID, i + 1, 0, int_prof + 1, int_q_total
                    )

                    int_count_nodes += 1

            # add wse and q_total for xsections
            this_profile_x_section_info["wse"] = arr_water_surface_elev
            this_profile_x_section_info["discharge"] = arr_q_total

            all_x_sections_info = pd.concat([all_x_sections_info, this_profile_x_section_info])

            # Revise the last channel length to zero
            arr_channel_length[len(arr_channel_length) - 1] = 0

        hec.QuitRas()  # close HEC-RAS

    except Exception as ex:
        # re-raise it as error handling is farther up the chain
        # but I do need the finally to ensure the hec.QuitRas() is run
        print("++++++++++++++++++++++++")
        MP_LOG.error("An exception occurred with the HEC-RAS engine or its parameters.")
        MP_LOG.error(f"details: {ex}")
        print()
        # re-raise it for logging (coming)
        raise ex

    finally:
        # Especially with multi proc, if an error occurs with HEC-RAS (engine
        # or values submitted), HEC-RAS will not close itself just becuase of an python
        # exception. This leaves orphaned process threads (visible in task manager)
        # and sometimes visually as well.

        if hec is not None:
            try:
                hec.QuitRas()  # close HEC-RAS no matter watch
            except Exception as ex2:
                MP_LOG.error("--- An error occured trying to close the HEC-RAS window process")
                MP_LOG.error(f"--- Details: {ex2}")
                print()
                # do nothng

    return all_x_sections_info


def fn_run_one_ras_model(
    str_ras_projectpath,
    int_number_of_steps,
    model_folder,
    unit_output_folder,
    log_default_folder,
    log_file_prefix,
    index_number,
    total_number_models,
):
    try:
        global MP_LOG

        file_id = sf.get_date_with_milli()
        log_file_name = f"{log_file_prefix}-{file_id}.log"
        MP_LOG.setup(os.path.join(log_default_folder, log_file_name))

        MP_LOG.lprint(f"Processing Model Number {index_number+1} Out Of {total_number_models}")
        MP_LOG.lprint(f"Starting Processing {model_folder} Model")
        MP_LOG.trace(str_ras_projectpath)

        all_x_sections_info = fn_run_hecras(str_ras_projectpath, int_number_of_steps)

        path_to_all_x_sections_info = os.path.join(
            unit_output_folder, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT, model_folder
        )
        path_all_x_sections_info = os.path.join(
            path_to_all_x_sections_info, "all_x_sections_info" + "_" + model_folder + ".csv"
        )
        all_x_sections_info.to_csv(path_all_x_sections_info)

        MP_LOG.lprint(f"Processing {model_folder} Model Completed")

    except Exception:
        if ras2fim_logger.LOG_SYSTEM_IS_SETUP is True:
            MP_LOG.error(traceback.format_exc())
        else:
            print(traceback.format_exc())

        # sys.exit(1)
