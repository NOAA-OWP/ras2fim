#!/usr/bin/env python
# coding: utf-8
#
# Purpose:
# Create a comparison plot for each rating curves from the BLE2FIM
# output relative to the current HAND rating curve calculations.
#
# Output generated:
# A PNG of the plotted rating curves with compted rating cuves for each
# HydroID that has a cooresponding BLE2FIM feature ID "reach averaged"
# synthetic rating curve
#
# Created by: Andy Carter, PE
# Last revised - 2021.04.29


import pandas as pd
import geopandas as gpd
import os

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

from scipy.interpolate import interp1d

from datetime import date

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Input

STR_NWM_HAND_RATING = r'path\to\the\HUC8\specific\hydroTable.csv'
STR_BLE2FIM_OUTPUT = r'path\to\ble2fim\output\to\walk'
STR_HYDRO_ID_PATH = r'path\to\HUC8\demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg'

FLT_M_TO_FT = 3.28083989
FLT_CMS_TO_CFS = 35.3146667
FLT_KM_TO_MILES = 0.62137119

STR_RATING_PATH = r'Path/to/write/out/pngs'
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

# load the NWM HydroID geopackage
gdf_hydro_id = gpd.read_file(STR_HYDRO_ID_PATH)

path = STR_BLE2FIM_OUTPUT

# os walk to get all the ble2fim rating curves
list_csv_files = [os.path.join(d, x)
                  for d, dirs, files in os.walk(path)
                  for x in files if x.endswith(".csv")]

df_nwm_rating = pd.read_csv(STR_NWM_HAND_RATING)

df_nwm_rating.assign(stage_ft="", discharge_cfs="")

df_nwm_rating['stage_ft'] = [row * FLT_M_TO_FT for row in df_nwm_rating['stage']]
df_nwm_rating['discharge_cfs'] = [row * FLT_CMS_TO_CFS for row in df_nwm_rating['discharge_cms']]


arr_unique_feature_id = df_nwm_rating['HydroID'].unique()
list_unique_feature_id = arr_unique_feature_id.tolist()


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def fn_create_rating_curve(list_int_step_flows_fn,
                           list_step_profiles_fn,
                           list_ble_flows_fn,
                           list_ble_profiles_fn,
                           str_hydro_id_fn,
                           str_feature_id_fn,
                           str_path_to_create_fn):

    str_file_name = str_hydro_id_fn + '_indirect_rating_curve.png'

    # Create a Rating Curve folder
    str_rating_path_to_create = str_path_to_create_fn
    os.makedirs(str_rating_path_to_create, exist_ok=True)

    fig = plt.figure()
    fig.patch.set_facecolor('gainsboro')
    fig.suptitle('HYDRO ID: '
                 + str_hydro_id_fn + '\nFEATURE ID: ' + str_feature_id_fn,
                 fontsize=10,
                 fontweight='bold')

    ax = plt.gca()
    today = date.today()

    try:
        # ``````````````````````````````````````````
        # Get paramaters
        # TODO - 2021.04.27 - MAC - messy coding - needs clean-up
        # get an interpolted list for each value in list_ble_flows_fn

        # f is the linear interpolator
        f = interp1d(list_flows, list_profiles)

        # get interpolated HAND depth values at each flow value of BLE
        arr_hand_depth = f(list_ble_flows)

        # convert the linear interpolation array to a list
        list_hand_depth = arr_hand_depth.tolist()

        # ****************************
        # mean absolute depth difference
        flt_sum_value = 0
        index = 0
        for i in list_hand_depth:
            flt_value = abs(list_ble_profiles[index] - i)
            flt_sum_value += flt_value
            index += 1

        flt_madd = flt_sum_value / len(list_ble_profiles)

        # ****************************
        # normalized root mean sqaure error
        flt_sum_value = 0
        index = 0
        for i in list_hand_depth:
            flt_value = (i - list_ble_profiles[index])**2
            flt_sum_value += flt_value
            index += 1

        flt_rmse = (flt_sum_value / len(list_ble_profiles))**0.5
        flt_min_ble_profile = min(list_ble_profiles)
        flt_max_ble_profile = max(list_ble_profiles)
        flt_norm_rmse = flt_rmse / (flt_max_ble_profile - flt_min_ble_profile) * 100

        # ****************************
        # percent bias
        flt_sum_value_dif = 0
        flt_sum_value_ble_depth = 0
        index = 0
        for i in list_hand_depth:
            flt_value_diff = (i - list_ble_profiles[index])
            flt_sum_value_dif += flt_value_diff
            flt_sum_value_ble_depth += list_ble_profiles[index]
            index += 1

        flt_percent_bias = (flt_sum_value_dif / flt_sum_value_ble_depth) * 100

        # ``````````````````````````````````````````
    except:
        flt_madd = 0
        flt_percent_bias = 0
        flt_norm_rmse = 0

    # -------------------------------------------
    # get the slope and length of the subject HydroID

    str_query = "HydroID == '" + str(str_hydro_id_fn) + "'"
    df_single_hydro_id = gdf_hydro_id.query(str_query)
    df_single_hydro_id = df_single_hydro_id.reset_index(drop=True)

    if len(df_single_hydro_id) > 0:
        hydro_id_slope = df_single_hydro_id['S0'][0] * 100
        hydro_id_length = df_single_hydro_id['LengthKm'][0] * FLT_KM_TO_MILES

        ax.text(0.98, 0.27, 'HydroID: slope=' + str("{:.2f}".format(hydro_id_slope)) 
                + "%" + " length=" + str("{:.2f}".format(hydro_id_length)) + ' mi',
                verticalalignment='bottom',
                horizontalalignment='right',
                backgroundcolor='w',
                transform=ax.transAxes, fontsize=3)

    # ``````````````````````````````````````````
    ax.text(0.98, 0.04, 'Created: ' + str(today),
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes,
            fontsize=6,
            style='italic')

    ax.text(0.98, 0.09, 'NOAA - Office of Water Prediction',
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=6, style='italic')

    ax.text(0.98, 0.16, 'Mean absolute depth difference: ' + str("{:.2f}".format(flt_madd)) +"ft" ,
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=4)

    ax.text(0.98, 0.19, 'Mean normalized RMSE: ' + str("{:.1f}".format(flt_norm_rmse)) +"%" ,
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=4)

    ax.text(0.98, 0.22, 'Percent bias: ' + str("{:.1f}".format(flt_percent_bias)) + "%",
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=4)

    plt.plot(list_int_step_flows_fn, list_step_profiles_fn, label="HAND FIM 3_0_15_2 (per Hydro_ID)")  # creates the line (HAND)
    plt.plot(list_ble_flows_fn, list_ble_profiles_fn, label="BLE2FIM (per Feature_ID)") # creates the line (ble2fim)
    plt.legend(loc="upper left", fontsize=6)

    # x-axis limits of the plot are from the ble2fim rating curve
    plt.xlim([max(list_ble_flows_fn) * -0.05 ,max(list_ble_flows_fn) * 1.2])

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     # determine the max limits of the depth to plot
    if max(list_int_step_flows_fn) > max(list_ble_flows_fn):
        # f is the linear interpolator
        f = interp1d(list_int_step_flows_fn, list_step_profiles_fn)
        flt_hand_max_in_range = f(max(list_ble_flows_fn))
    else:
        flt_hand_max_in_range = max(list_step_profiles_fn)

    if flt_hand_max_in_range > (max(list_ble_profiles_fn)):
        flt_max_ylim = flt_hand_max_in_range
    else:
        flt_max_ylim = (max(list_ble_profiles_fn))
    flt_max_ylim *= 1.2

    plt.ylim([-1, flt_max_ylim])
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    plt.plot(list_int_step_flows_fn, list_step_profiles_fn, 'bd', markersize=1) # adding blue diamond points on line
    plt.plot(list_ble_flows_fn, list_ble_profiles_fn, 'go', markersize=1)

    ax.get_xaxis().set_major_formatter(
        tick.FuncFormatter(lambda x, p: format(int(x), ',')))

    plt.xticks(rotation=90)

    plt.ylabel('Reach Average Depth (ft)')
    plt.xlabel('Discharge (ft^3/s)')

    plt.grid(True)

    str_rating_image_path = str_rating_path_to_create + '\\' + str_file_name
    plt.savefig(str_rating_image_path,
                dpi=300,
                bbox_inches="tight")
    plt.close

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


for i in list_unique_feature_id:
    list_profiles = []
    list_flows = []

    str_query = "HydroID == " + str(i)
    df_single_feature = df_nwm_rating.query(str_query)

    df_single_feature_reset = df_single_feature.reset_index(drop=True)
    str_feature_id = str(df_single_feature_reset['feature_id'][0])

    list_profiles = df_single_feature['stage_ft'].tolist()
    list_flows = df_single_feature['discharge_cfs'].tolist()

    # --------
    # get the path of the ble2fim rating path
    str_ble2fim_rating_path = ""

    for j in list_csv_files:
        if j[-24:-17] == str_feature_id:
            str_ble2fim_rating_path = j

    # --------

    if str_ble2fim_rating_path != "":
        # read the rating curve
        df_ble2fim_rating = pd.read_csv(str_ble2fim_rating_path)

        list_ble_profiles = df_ble2fim_rating['AvgDepth(ft)'].tolist()
        list_ble_flows = df_ble2fim_rating['Flow(cfs)'].tolist()

        fn_create_rating_curve(list_flows,
                               list_profiles,
                               list_ble_flows,
                               list_ble_profiles,
                               str(i),
                               str_feature_id,
                               STR_RATING_PATH)
