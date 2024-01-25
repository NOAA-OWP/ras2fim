import argparse
import datetime as dt
import os
import sys
import traceback

import colored as cl
import geopandas as gpd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_validators as val
import shared_variables as sv
from shared_functions import get_stnd_date, print_date_time_duration


# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def fn_extend_huc8_domain(target_huc8, path_wbd_huc12s_gpkg, output_path, run_by_cmd=True):
    '''
    TODO  (more notes, input, outputs    )

    Overview:
        Note: WBD_National is at crs EPSG:4269, but this script does not attempt to reproject
    '''

    print()
    start_dt = dt.datetime.utcnow()

    if run_by_cmd is True:
        RLOG.lprint("****************************************")
        RLOG.notice("==== Make a polygon for HUC8 extended domain ===")
        RLOG.lprint(f"    Started (UTC): {get_stnd_date()}")
        RLOG.lprint(f"  --- (-huc) Target HUC8 number: {target_huc8}")
        RLOG.lprint(f"  --- (-wbd) Path to WBD HUC12s gkpg: {path_wbd_huc12s_gpkg}")
        RLOG.lprint(f"  --- (-o) Path to output folder: {output_path}")
        RLOG.lprint("+-----------------------------------------------------------------+")
    else:
        RLOG.notice(f" -- Making extended domain file for {target_huc8}")

    try:
        # ------------
        # Validation code (more applicable if it came in via command line)
        huc_valid, err_msg = val.is_valid_huc(target_huc8)
        if huc_valid is False:
            raise ValueError(err_msg)

        if os.path.exists(path_wbd_huc12s_gpkg) is False:
            raise ValueError(f"File path to {path_wbd_huc12s_gpkg} does not exist")

        # ------------
        file_name = f"HUC8_{target_huc8}_domain.gpkg"
        output_file = os.path.join(output_path, file_name)

        # See if file exists and ask if they want to overwrite it.
        if os.path.exists(output_file):  # file, not folder
            print()
            msg = (
                f"{cl.fore.SPRING_GREEN_2B}"
                f"The domain file already exists at {output_file}. \n"
                "Do you want to overwrite it?\n\n"
                f"{cl.style.RESET}"
                f"   -- Type {cl.fore.SPRING_GREEN_2B}'overwrite'{cl.style.RESET}"
                " if you want to overwrite the current file.\n"
                f"   -- Type {cl.fore.SPRING_GREEN_2B}'skip'{cl.style.RESET}"
                " if you want to skip overwriting the file but continue running the program.\n"
                f"   -- Type {cl.fore.SPRING_GREEN_2B}'abort'{cl.style.RESET}"
                " to stop the program.\n"
                f"{cl.fore.LIGHT_YELLOW}  ?={cl.style.RESET}"
            )
            resp = input(msg).lower()

            if (resp) == "abort":
                RLOG.lprint(f"\n.. You have selected {resp}. Program stopped.\n")
                sys.exit(0)

            elif (resp) == "skip":
                return output_file
            else:
                if (resp) != "overwrite":
                    RLOG.lprint(f"\n.. You have entered an invalid response of {resp}. Program stopped.\n")
                    sys.exit(0)
                # else.. continue
        else:  # the file might not exist but the folder needs to, so we create it as necessary (full path)
            os.makedirs(output_path, exist_ok=True)

        # ------------
        print()
        print(" *** Stand by, this may take up to 10 mins depending on computer resources")
        # read wbd huc12s
        wbd_huc12s = gpd.read_file(path_wbd_huc12s_gpkg)[['geometry', 'HUC_8', 'HUC_12']]

        # make sure the huc_8 values are read as string
        wbd_huc12s['HUC_8'] = wbd_huc12s['HUC_8'].astype(str)

        # make domain of the target huc8
        huc8_domain = wbd_huc12s[wbd_huc12s['HUC_8'] == str(target_huc8)]
        huc8_domain = huc8_domain.dissolve(by="HUC_8").reset_index()

        # find all huc12s that intersect with target huc8 domain
        extended_huc12s = gpd.sjoin(wbd_huc12s, huc8_domain)
        extended_huc12s.loc[:, 'dissolve_index'] = 1
        extended_domain = extended_huc12s.dissolve(by="dissolve_index").reset_index()
        extended_domain.to_file(output_file, driver='GPKG')

        RLOG.lprint("--------------------------------------")
        RLOG.success(f" - HUC8 extended domain created: {get_stnd_date()}")
        dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        print()

        return output_file

    except ValueError as ve:
        RLOG.critical(ve)
        sys.exit(1)

    except Exception:
        msg = "An exception occurred while downloading file the USGS file."
        RLOG.critical(msg)
        RLOG.critical(traceback.format_exc())
        sys.exit(1)


# -------------------------------------------------
if __name__ == "__main__":
    # Sample usage showing min args.
    #     python extend_huc8_boundary.py
    #     -huc 12090301

    # Sample usage showing min args.
    #     python extend_huc8_boundary.py
    #     -huc 12090301
    #     -o 'C:\my_ras_folder\inputs\dems'  (folder only)
    #     -wbd 'C:\my_ras_folder\inputs\X-National_Datasets\WBD_National.gpkg'

    parser = argparse.ArgumentParser(description="==== Make a polygon for HUC8 extended domain ===")

    parser.add_argument(
        "-huc",
        dest="target_huc8",
        help="REQUIRED: HUC8 number. Can be entered as string or int. Example: 12090301",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-o",
        dest="output_path",
        help="OPTIONAL: path to the output folder only, which would be a gpkg file."
        " File name will be auto created as 'HUC8_{huc_number}_domain.gpkg'."
        f" Default = {sv.INPUT_3DEP_HUC8_10M_ROOT}",
        default=sv.INPUT_3DEP_HUC8_10M_ROOT,
        required=False,
        metavar="",
    )

    parser.add_argument(
        "-wbd",
        dest="path_wbd_huc12s_gpkg",
        help="Optional: path to WBD HUC12 polygon gpkg."
        f" Defaults to {sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH}",
        required=False,
        default=sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH,
        metavar="",
    )

    args = vars(parser.parse_args())

    log_file_folder = args["output_path"]
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
        fn_extend_huc8_domain(**args)

        print(f"log files saved to {RLOG.LOG_FILE_PATH}")

    except Exception:
        RLOG.critical(traceback.format_exc())
