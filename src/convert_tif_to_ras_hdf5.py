# Create HEC-RAS terrain files (HDF5) from GeoTIFFs
#
# Purpose:
# Note that this uses a CLI to the RasProcess.exe in HEC-RAS 6.3.
# This will need access to the cooresponding and support files
# (dll, exe, etc...).  As this is a CLI call it will run async with this
# script.
#
# Use the HEC-RAS Command line interface to convert the TIF to HEC-RAS
# hdf5 terrains per directions from Cam Ackerman - 2021.03.31
#
# Created by: Andy Carter, PE
# Created: 2021.08.05
# Last revised - 2021.10.24
#
# Sample:  RasProcess.exe CreateTerrain
# units=feet stitch=true
# prj="C:\Path\file.prj"
# out="C:\Path\Terrain.hdf"
# "C:\inputs\file1.tif" "C:\inputs\file2.tif" [...]
#
# ras2fim - Fourth pre-processing script
# Uses the 'ras2fim' conda environment

import argparse
import datetime
import os
import subprocess
import time
import traceback

import pyproj

import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def fn_get_filepaths(str_directory, str_file_suffix):
    # Fuction - walks a directory and determines a the path
    # to all the files with a given suffix

    list_file_paths = []
    int_file_suffix_len = len(str_file_suffix) * -1

    # Walk the tree.
    for root, directories, files in os.walk(str_directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filepath[int_file_suffix_len:] == str_file_suffix:
                list_file_paths.append(filepath)

    return list_file_paths


# -------------------------------------------------
# Print iterations progress
def fn_print_progress_bar(
    iteration, total, prefix="", suffix="", decimals=0, length=100, fill="█", printEnd="\r"
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
        print("")


# -------------------------------------------------
def fn_convert_tif_to_ras_hdf5(
    str_hec_path, str_geotiff_dir, str_dir_to_write_hdf5, str_projection, model_unit
):
    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # INPUT
    flt_start_convert_tif = time.time()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.lprint("|       CONVERT TERRAIN GEOTIFFS TO HEC-RAS TERRAINS (HDF5)       |")
    RLOG.lprint("+-----------------------------------------------------------------+")

    # path to the directory that contains RasProcess and associated dll

    STR_HEC_RAS_6_PATH = str_hec_path
    RLOG.lprint("  ---(r) HEC-RAS PATH: " + str(STR_HEC_RAS_6_PATH))

    # STR_HEC_RAS_6_PATH = r'C:\Program Files (x86)\HEC\HEC-RAS\6.3'
    STR_HEC_RAS_6_PATH += r"\RasProcess.exe"

    # path to walk to file geotiffs
    STR_CONVERT_FILEPATH = str_geotiff_dir
    RLOG.lprint("  ---(i) GEOTIFF INPUT PATH: " + str(STR_CONVERT_FILEPATH))

    # path to walk to file geotiffs
    STR_RAS_TERRAIN_OUT = str_dir_to_write_hdf5
    RLOG.lprint("  ---(o) DIRECTORY TO WRITE TERRAIN HDF5: " + str(STR_RAS_TERRAIN_OUT))

    # path to walk to file geotiffs
    STR_PRJ_FILE = str_projection
    RLOG.lprint("  ---(p) PROJECTION TO WRITE DEMS: " + str(STR_PRJ_FILE))

    RLOG.lprint("  --- The Ras Models unit (extracted from given GIS prj file): " + model_unit)

    RLOG.lprint("===================================================================")

    list_processed_dem = fn_get_filepaths(STR_CONVERT_FILEPATH, "tif")
    len_processed_dems = len(list_processed_dem)

    str_prefix = "Converting Terrains: "
    fn_print_progress_bar(0, len_processed_dems, prefix=str_prefix, suffix="Complete", length=29)

    int_count = 0
    int_valid_count = 0

    for i in list_processed_dem:
        int_count += 1

        fn_print_progress_bar(int_count, len_processed_dems, prefix=str_prefix, suffix="Complete", length=29)

        # Build a CLI call for RasProcess.exe CreateTerrain for each
        # terarin tile (HUC-12) in the list

        str_path_ras = '"' + STR_HEC_RAS_6_PATH + '"' + " CreateTerrain"
        str_path_ras += " units="

        if model_unit == "feet":
            str_path_ras += "Feet"
        else:
            str_path_ras += "Meter"

        str_path_ras += " stitch=true prj="
        str_path_ras += '"' + STR_PRJ_FILE + '"'
        str_path_ras += " out="
        str_path_ras += '"' + STR_RAS_TERRAIN_OUT + "\\"

        str_path_ras += i[-16:-4] + ".hdf" + '"'
        str_path_ras += " " + i

        # print(str_path_ras)

        int_return_code = subprocess.check_call(
            str_path_ras, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
        if int_return_code == 0:
            int_valid_count += 1
        else:
            RLOG.error(f"Error on: {i} ({STR_CONVERT_FILEPATH})")

        # A '0' error code will be given if the file already exists in the
        # output directory.  Terrain will not be over-written with this
        # routine.  It will be skipped.

    RLOG.lprint("+-----------------------------------------------------------------+")
    if int_valid_count == len(list_processed_dem):
        RLOG.success("All terrains processed successfully")
    else:
        RLOG.error(F"Errors when processing {STR_CONVERT_FILEPATH} - Check output or logs")

    flt_end_convert_tif = time.time()
    flt_time_convert_tif = (flt_end_convert_tif - flt_start_convert_tif) // 1
    time_pass_convert_tif = datetime.timedelta(seconds=flt_time_convert_tif)
    RLOG.lprint("Compute Time: " + str(time_pass_convert_tif))

    RLOG.lprint("===================================================================")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # Sample with mins
    # python convert_tif_to_ras_hdf5.py
    #  -i c:\ras2fim_data\output_ras2fim\12030105_2276_231024\03_terrain
    #  -o c:\ras2fim_data\output_ras2fim\12030105_2276_231024\04_hecras_terrain
    #  -p c:\.....\12030105_2276_231024\02_shapes_from_conflation\12030105_huc_12_ar.prj

    parser = argparse.ArgumentParser(
        description="==== CONVERT TERRAIN GeoTIFFS TO HEC-RAS TERRAINS (HDF5) ==="
    )

    parser.add_argument(
        "-i",
        dest="str_geotiff_dir",
        help=r"REQUIRED: directory containing the geotiffs to convert:  Example: D:\terrain",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="str_dir_to_write_hdf5",
        help=r"REQUIRED: path to write output files: Example: D:\hecras_terrain",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-p",
        dest="str_projection",
        help=r"REQUIRED: projection file of output coordinate zone: D:\conflation\10170204_huc_12_ar.prj",
        required=True,
        metavar="FILE PATH",
        type=str,
    )

    parser.add_argument(
        "-r",
        dest="str_hec_path",
        help="Optional: path to HEC-RAS 6.3 RasProcess.exe: "
        r'Example: "C:\Program Files (x86)\HEC\HEC-RAS\6.3" (wrap in quotes)',
        required=False,
        default=sv.DEFAULT_HECRAS_ENGINE_PATH,
        metavar="DIR",
        type=str,
    )

    args = vars(parser.parse_args())

    str_hec_path = args["str_hec_path"]
    str_geotiff_dir = args["str_geotiff_dir"]
    str_dir_to_write_hdf5 = args["str_dir_to_write_hdf5"]
    str_projection = args["str_projection"]

    # find model unit using the given GIS prj file
    with open(str_projection, "r") as prj_file:
        prj_text = prj_file.read()
    proj_crs = pyproj.CRS(prj_text)
    model_unit = sf.model_unit_from_crs(proj_crs)

    log_file_folder = args["str_dir_to_write_hdf5"]
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
        fn_convert_tif_to_ras_hdf5(
            str_hec_path, str_geotiff_dir, str_dir_to_write_hdf5, str_projection, model_unit
        )

    except Exception:
        RLOG.critical(traceback.format_exc())
