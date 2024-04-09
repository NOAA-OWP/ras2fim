#!/usr/bin/env python3

import datetime as dt
import os
import sys
import traceback

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv
# import shared_variables as sf


# Global Variables
RLOG = sv.R2F_LOG


'''
Note: Apr 5, 2024: This is a starting prototype tool. It has alot of hardcoding and
debug testing in it. This tool will evolve over time.

'''


#########################################################################
def barplot(
    dataframe,
    x_field,
    x_order,
    y_field,
    hue_field,
    ordered_hue,
    title_text,
    textbox_str=False,
    simplify_legend=False,
    display_values=False,
    dest_file=False,
):
    '''
    Create barplots.

    Parameters
    ----------
    dataframe : DataFrame
        Pandas dataframe data to be plotted.
    x_field : STR
        Field to use for x-axis
    x_order : List
        Order to arrange the x-axis.
    y_field : STR
        Field to use for the y-axis
    hue_field : STR
        Field to use for hue (typically FIM version)
    title_text : STR
        Text for plot title.
    simplify_legend : BOOL, optional
        If True, it will simplify legend to FIM 1, FIM 2, FIM 3.
        Default is False.
    display_values : BOOL, optional
        If True, Y values will be displayed above bars.
        Default is False.
    dest_file : STR or BOOL, optional
        If STR provide the full path to the figure to be saved. If False
        no plot is saved to disk. Default is False.

    Returns
    -------
    fig : MATPLOTLIB
        Plot.

    '''

    # initialize plot
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    # Use seaborn to plot the boxplot
    axes = sns.barplot(
        x=x_field,
        y=y_field,
        order=x_order,
        hue=hue_field,
        hue_order=ordered_hue,
        data=dataframe,
        palette='bright',
    )
    # set title of plot
    axes.set_title(f'{title_text}', fontsize=20, weight='bold')
    # Set yticks and background horizontal line.
    axes.set(ylim=(0.0, 1.0), yticks=np.arange(0, 1.1, 0.1))
    for index, ytick in enumerate(axes.get_yticks()):
        plt.axhline(y=ytick, color='black', linestyle='--', linewidth=1, alpha=0.1)
    # Define y axis label and x axis label.
    axes.set_ylabel(f'{y_field.upper()}', fontsize='xx-large', weight='bold')
    axes.set_xlabel('', fontsize=0, weight='bold')
    # Set sizes of ticks and legend.
    axes.tick_params(labelsize='xx-large')
    axes.legend(markerscale=2, fontsize=20, loc='upper right')
    # If simple legend desired
    if simplify_legend:
        # trim labels to FIM 1, FIM 2, FIM 3
        handles, org_labels = axes.get_legend_handles_labels()
        label_dict = {}
        for label in org_labels:
            if 'fim_1' in label:
                label_dict[label] = 'FIM 1'
            elif 'fim_2' in label:
                label_dict[label] = 'FIM 2' + ' '  # + fim_configuration.lower()
            elif 'fim_4' in label and len(label) < 20:
                label_dict[label] = label.replace('_', '.').replace('fim.', 'FIM ')
            else:
                label_dict[label] = label
        # Define simplified labels as a list.
        new_labels = [label_dict[label] for label in org_labels]
        # rename legend labels to the simplified labels.
        axes.legend(
            handles,
            new_labels,
            markerscale=2,
            fontsize=14,
            loc='upper right',
            ncol=int(np.ceil(len(new_labels) / 7)),
        )
    # Add Textbox
    if textbox_str:
        box_props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        axes.text(
            0.01,
            0.99,
            textbox_str,
            transform=axes.transAxes,
            fontsize=18,
            verticalalignment='top',
            bbox=box_props,
        )

    # Display Y values above bars
    if display_values:
        # Add values of bars directly above bar.
        for patch in axes.patches:
            value = round(patch.get_height(), 3)
            axes.text(
                patch.get_x() + patch.get_width() / 2.0,
                patch.get_height(),
                '{:1.3f}'.format(value),
                ha="center",
                fontsize=18,
            )

    # If figure to be saved to disk, then do so, otherwise return fig

    if dest_file:
        fig.savefig(dest_file)
        print(f"plot file saved to {dest_file}")
        plt.close(fig)
    else:
        return fig


#########################################################################
def filter_db(dframe, filters):
    """
    Processing:

    Inputs:
        - dataframe: pandas:
        - filters: dictionary with values which are lists
            Filters can be one of more of, and some can have more than one of a filter type
              (ie. unit_version of 230922 and 240319,  or code_version of v1.29.0 and v2.0.1
                - unit_name:  e.g 12040101_102739_ble
                - unit_version: e.g 230922
                - code_version: e.g v2.0.1
                - huc
                - benchmark
                - magnitude
                - ahps_lid
                - enviro
                - and a wide range of other columns such as critical_success_index,
                  f_score, matthews_correlation_coefficient, etc

                examples of filters  A dictionary of with valuse who are lists
                e.g - { "unit_name": ['12040101_102739_ble'], "unit_version": ["230922", "240321", "240602"] }
                    - { "code_version": ['v2.0.1'] }

    Return:
        - A filtered dataframe.

    """
    # -------------------
    # validation
    if dframe is None:
        raise Exception("dframe does not exists (is None)")
    if len(dframe) == 0:
        raise Exception("dframe is empty")

    # -------------------
    df_filtered = dframe.copy()
    df_column_names = list(dframe.columns)
    # ctr_filter_items = 0
    # num_filters = len(filters.items())
    df_query = ""
    for column_name, column_values in filters.items():
        # if not isinstance(column_values, list):
        #    column_values = list[column_values]

        if column_values == "":
            continue

        if column_name not in df_column_names:
            continue

        if df_query != "":
            df_query += " and "

        num_column_values = len(column_values)
        ctr_column_values = 0

        df_query += f"{column_name}"
        if num_column_values == 1:
            df_query += f" == '{column_values[0]}'"
        else:
            df_query += " in ["

            for column_val in column_values:
                if ctr_column_values > 0:
                    df_query += ","
                df_query += f"'{column_val}'"

                ctr_column_values += 1
            df_query += "]"

    print(f"df query is {df_query}")
    df_filtered.query(df_query, inplace=True)

    return df_filtered


#########################################################################
def eval_data():
    metrics_file = r"C:\ras2fim_data\gval\evaluations\PROD\eval_PROD_metrics.csv"
    metrics_df = pd.read_csv(metrics_file)

    """
    filter_dt = { "unit_name": ["12040101_102739_ble"] }
    file_name = "12040101_102739_ble_v1.png"
    db_filtered_metrics = filter_db(metrics_df, filter_dt)

    barplot(dataframe=db_filtered_metrics,
            x_field='magnitude',
            x_order=['100yr', '500yr'],
            y_field='critical_success_index',
            hue_field='code_version',
            ordered_hue=['v1.29.0','v2.0.1'],
            title_text='Unit 12090301 versions - ble',
            dest_file=f"C:\\ras2fim_data\\gval\\evaluations\\PROD\\eval_bench_results\\{file_name}")
        # good
    """

    # ----------------------------------
    """
    filter_dt = { "unit_name": ["12090301_2277_ble"],
                  "benchmark_source": ['ble'] }
    file_name = "12090301_2277_ble_all_versions_ble_240402_1435.png"
    db_filtered_metrics = filter_db(metrics_df, filter_dt)

    barplot(dataframe=db_filtered_metrics,
            x_field='magnitude',
            x_order=['100yr', '500yr'],
            y_field='critical_success_index',
            hue_field='code_version',
            ordered_hue=['v1.29.0','v2.0.1'],
            title_text='12090301_2277_ble - all versions - ble',
            dest_file=f"C:\\ras2fim_data\\gval\\evaluations\\PROD\\eval_bench_results\\{file_name}")
    """

    # ----------------------------------
    unit_name = "12040101_102739_ble"
    file_create_date = dt.datetime.now().strftime("%Y%m%d_%H%M")
    filter_dt = { "unit_name": [unit_name]}
    file_name = f"{unit_name}_all_unit_versions_all_bench_{file_create_date}.png"
    db_filtered_metrics = filter_db(metrics_df, filter_dt)

    # ----------------------------------    
    barplot(dataframe=db_filtered_metrics,
            x_field='magnitude',
            x_order=['100yr', '500yr','action', 'minor', 'moderate', 'major'],
            y_field='critical_success_index',
            hue_field='code_version',
            ordered_hue=['v1.29.0','v2.0.1'],
            title_text=f"{unit_name} - all unit versions - all bench sources",
            dest_file=f"C:\\ras2fim_data\\gval\\evaluations\\PROD\\{unit_name}\\eval_bench_results\\{file_name}")

    print("all done, yay")


#########################################################################
if __name__ == "__main__":
    # ---- Samples Inputs

    # referential_path = os.path.join(args["src_unit_dir_path"], "..", "ras_unit_to_s3_logs")
    # log_file_folder = os.path.abspath(referential_path)

    # input args coming

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # Creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        # log_file_name = f"{script_file_name}_{get_date_with_milli(False)}.log"
        # RLOG.setup(os.path.join(log_file_folder, log_file_name))

        # call main program
        eval_data()

    except Exception:
        RLOG.critical(traceback.format_exc())
