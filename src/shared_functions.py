#!/usr/bin/env python3

import fnmatch
import json
import os
import platform
import random
import re
import string
import sys
from concurrent.futures import as_completed
from datetime import datetime as dt
from pathlib import Path

import keepachangelog
import numpy as np
import pandas as pd
import rasterio
from dotenv import load_dotenv
from tqdm import tqdm

import ras2fim_logger
import shared_validators as val
import shared_variables as sv
from errors import ModelUnitError


# Global Variables
# RLOG = ras2fim_logger.RAS2FIM_logger()
RLOG = ras2fim_logger.R2F_LOG


# -------------------------------------------------
def confirm_models_unit(proj_crs, input_models_path):
    """
    - calls two other functions to infer units from ras models and -p projection.
    - raises an exception if units do not match.
    - return the unit if they match
    """

    unit = None
    try:
        if os.path.exists(input_models_path) is False:
            raise ValueError(f"The path of {input_models_path} can not be found.")

        unit_from_ras = model_unit_from_ras_prj(input_models_path)
        unit_from_crs = model_unit_from_crs(proj_crs)

        if unit_from_ras == unit_from_crs:  # if both are the same, return one of them
            unit = unit_from_crs

        elif unit_from_ras != unit_from_crs:
            raise ModelUnitError(
                "Specified projection (with -p) is in '%s' but the unit specified in RAS models"
                " prj files is in '%s'. Check your models or -p entry and try again."
                % (unit_from_crs, unit_from_ras)
            )

    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit


# -------------------------------------------------
def model_unit_from_crs(proj_crs):
    """
    get the unit( meter or feet) from -p EPSG input:
    return either 'meter' or 'feet'; Otherwise raises an error
    """

    try:
        unit = proj_crs.axis_info[0].unit_name
        if "foot" in unit:
            unit = "feet"
        elif "metre" in unit:
            unit = "meter"
        else:
            raise ModelUnitError(
                "Make sure you have entered a correct projection code.\
                                 The projection unit must be in feet or meter."
            )
    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit


# -------------------------------------------------
def model_unit_from_ras_prj(str_ras_path_arg):
    """
    -- return either 'meter' or 'feet'; Otherwise raises an error
    This function read prj file of HEC-RAS models in a dataset and records the unit (either Metric
    or US customary):
        - raises and exception if a mixed use of Metric and US customary units encounters
        - returns a unit (either Metric or US customary) when only one unit has been used for the
             entire dataset.
        - The function assumes always a unit has been specified in HEC-RAS prj file. If not, a unit
            must be added into prj file and then use ras2fim.
    """

    unit = None
    ras_prj_files = []
    if os.path.isdir(str_ras_path_arg):
        for root, dirnames, filenames in os.walk(str_ras_path_arg):
            for filename in fnmatch.filter(filenames, "*.[Pp][Rr][Jj]"):
                with open(os.path.join(root, filename)) as f:
                    first_file_line = f.readline()

                # skip projection files
                if any(x in first_file_line for x in ["PROJCS", "GEOGCS", "DATUM", "PROJECTION"]):
                    continue
                ras_prj_files.append(os.path.join(root, filename))
    elif os.path.isfile(str_ras_path_arg):
        ras_prj_files.append(str_ras_path_arg)

    units_found = []
    for ras_prj_file in ras_prj_files:
        with open(ras_prj_file) as f:
            file_contents = f.read()

        if re.search("SI Unit", file_contents, re.I):
            units_found.append("meter")
        elif re.search("English Unit", file_contents, re.I):
            units_found.append("feet")

    try:
        if len(set(units_found)) == 0:  # if no unit specified in any of the RAS models
            raise ModelUnitError(
                "At least one of the HEC-RAS models must have a unit specified in prj file."
                " Check your RAS models prj files and try again. "
            )

        elif len(set(units_found)) == 1:
            unit = units_found[0]

        elif len(set(units_found)) == 2:
            raise ModelUnitError(
                "Ras2fim only accepts HEC-RAS models with similar units (either U.S. Customary or "
                "International/Metric). The provided dataset uses a mix of these units. "
                "Verify your HEC-RAS models units and try again."
            )

    except ModelUnitError as e:
        print(e)
        sys.exit(1)
    return unit


# -------------------------------------------------
# Scrapes the top changelog version (most recent version listed in our repo)
def get_changelog_version(changelog_path):
    changelog = keepachangelog.to_dict(changelog_path)
    return list(changelog.keys())[0]


# -------------------------------------------------
def convert_rating_curve_to_metric(ras2rem_dir):
    src_path = os.path.join(ras2rem_dir, "rating_curve.csv")
    df = pd.read_csv(src_path)

    # convert to metric if needed
    if "stage_m" not in df.columns:  # if no meters, then only Imperial units are in the file
        df["stage_m"] = ["{:0.2f}".format(h * 0.3048) for h in df["stage_ft"]]
        df["discharge_cms"] = ["{:0.2f}".format(q * 0.0283168) for q in df["discharge_cfs"]]
        df.to_csv(os.path.join(src_path), index=False)

        # REM will also be in Imperial units if the incoming SRC was
        rem_path = os.path.join(ras2rem_dir, "rem.tif")
        with rasterio.open(rem_path) as src:
            raster = src.read()
            raster = np.multiply(raster, np.where(raster != 65535, 0.3048, 1))  # keep no-data value as-is
            output_meta = src.meta.copy()
        with rasterio.open(rem_path, "w", **output_meta, compress="LZW") as dest:
            dest.write(raster)

    return


# -------------------------------------------------
def load_config_enviro_path(config_file=sv.DEFAULT_CONFIG_FILE_PATH):
    ####################################################################
    # Load the enviroment file

    # The sv.DEFAULT_CONFIG_FILE_PATH comes in relative to the root and not to src/ras2fim
    # so we need to adjust it's path.

    if config_file == sv.DEFAULT_CONFIG_FILE_PATH:  # change to make relative to ras2fim.py
        referential_path = os.path.join(os.path.dirname(__file__), "..", sv.DEFAULT_CONFIG_FILE_PATH)
        config_file = os.path.abspath(referential_path)

    elif config_file == "":  # possible if not coming through __main__
        raise ValueError("The config file argument can not be empty")

    if os.path.exists(config_file) is False:
        raise ValueError(f"The config file of {config_file} can not found")

    load_dotenv(config_file)

    return config_file


# -------------------------------------------------
def is_windows():
    plt = platform.system()
    return "Windows" in plt


# -------------------------------------------------
def fix_proj_path_error():
    # Sep 7, 2023

    # But this has to be solved.

    # This code is on hold but not removed. With the latest upgrade rasterio (which we needed)
    # it creates problems with proj and gdal which not compatiable with the latest version.

    # https://github.com/rasterio/rasterio/blob/master/docs/
    #   faq.rst#why-cant-rasterio-find-projdb-rasterio-from-pypi-versions--120

    # Says for now. You have to point to older versions of proj and gdal but I tried a number of
    # combinations and no luck yet

    # If the PROJ_DB path (from pyrpo is incorrect, you will see bounding box issues such as
    # rioxarray.exceptions.NoDataInBounds: No data found in bounds.
    # and
    # ERROR 1: PROJ: proj_identify: C:\Users\rdp-user\anaconda3\envs\ras2fim\Library\share\proj\proj.db
    # lacks DATABASE.LAYOUT.VERSION.MAJOR / DATABASE.LAYOUT.VERSION.MINOR metadata. It comes from
    # another PROJ installation.
    # PROJ_LIB='C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Library\\site-packages\\pyproj\\proj_dir\\share\\proj'
    # PROJ_LIB="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Lib\\site-packages\\rasterio\\proj_data'
    # GDAL_DATA="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Lib\\site-packages\\rasterio\\gdal_data'
    # PROJ_LIB="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Lib\\site-packages\\rasterio\\proj_data'
    # GDAL_DATA="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Lib\\site-packages\\rasterio\\gdal_data'
    # PROJ_LIB="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Library\\share\\proj'
    # GDAL_DATA="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Library\\share\\gdal'
    # PROJ_LIB="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Library\\site-packages\\pyproj\\proj_dir\\share\\proj'
    # GDAL_DATA="C:\\Users\\rdp-user\\anaconda3\\envs\\ras2fim\\Library\\site-packages\\pyproj\\proj_dir\\share\\gdal'
    # PROJ_LIB="C:\\Program Files (x86)\\HEC\\HEC-RAS\\6.3\\GDAL\\common\\data'
    # GDAL_DATA="C:\\Program Files (x86)\\HEC\\HEC-RAS\\6.3\\GDAL\\common\\data'

    # File 'C:\Users\rdp-user\Projects\dev-linter\ras2fim\src\get_usgs_dem_from_shape.py', line 428,
    #    in fn_get_usgs_dem_from_shape usgs_wcs_local_proj_clipped = usgs_wcs_local_proj.rio.clip(str_geom)
    # File 'C:\Users\rdp-user\anaconda3\envs\ras2fim\lib\site-packages\rioxarray\raster_array.py',
    #   line 943, in clip
    #    raise NoDataInBounds(
    # rioxarray.exceptions.NoDataInBounds: No data found in bounds.

    # There is a known issue with rasterio and enviroment PROJ_LIB
    # an error of `PROJ: proj_create_from_database: Cannot find proj.db`
    # It does not stop anything it annoying. This is known hack for windows for it

    # This is not perfect. It will always show the error once at the start until this is loaded.
    # but it holds it off so we don't get the error over and over again.
    # Note: For win, the paths can be added to advanced system enviro variables in windows settings
    # but that can be a bit messy too.
    # This assumes that anaconda has been setup in the user path.

    # Depending on timing, this might load from the default config path but be updated later.

    try:
        # if (os.environ["PROJ_DB_FILE_PATH"] is None):
        #    raise EnvironmentError("PROJ_DB_FILE_PATH is not loaded into the os.enviro yet."\
        #                           " Check if the config file path exist")

        if is_windows():
            # first get the user and we have to build up a path
            # user_home_path = os.path.expanduser("~")

            # TODO: There could be other paths. ?? (depends how it was installed? future versions?)
            # anaconda3_env_path = os.path.join(user_home_path, r'anaconda3\envs\ras2fim\Library\share')

            # anaconda3_env_path_proj = os.path.join(anaconda3_env_path, "proj")

            # print(f"anaconda3_env_path_proj is {anaconda3_env_path_proj}")

            if os.getenv("PROJ_LIB") is not None:
                #               os.unsetenv("PROJ_LIB")
                #            else:
                print("os.getenv PROJ_LIB is")
                print(os.getenv("PROJ_LIB"))

            # if os.getenv("GDAL_DATA") is None and os.path.exists(anaconda3_env_path):
            #    os.environ["GDAL_DATA"] = anaconda3_env_path

        # remove the PROJ_LIB FILE and rasterio will set it

    except Exception as ex:
        print()
        print(
            "*** An internal error has occurred while attempting to load the proj and gdal"
            " environment variables. Details"
        )
        print(ex)
        print()
        pass


# -------------------------------------------------
def find_model_unit_from_rating_curves(r2f_hecras_outputs_dir):
    all_rating_files = list(Path(r2f_hecras_outputs_dir).rglob("*rating_curve.csv"))
    try:
        sample_rating_file = pd.read_csv(all_rating_files[0])
        SecondColumnTitle = sample_rating_file.columns[1]
        pattern = r"\((.*?)\)"  # Matches text between parentheses in column 2
        stage_unit = re.search(pattern, SecondColumnTitle).group(1).strip().lower()
        if stage_unit == "ft":
            model_unit = "feet"
        elif stage_unit == "m":
            model_unit = "meter"
        else:
            raise ValueError("Rating curve values should be either in feet or meter. Check the results")

        return model_unit
    except ValueError as e:
        print("Error:", e)
        sys.exit(1)
    except Exception:
        print(
            "Error: Make sure you have specified a correct input directory with has at least"
            " one '*.rating curve.csv' file."
        )
        sys.exit(1)


# -------------------------------------------------
def get_geometry_from_gdf(gdf, int_poly_index):
    """Function to parse features from GeoDataFrame in such
    a manner that rasterio wants them

    Keyword arguments:
        gdf -- pandas geoDataFrame
        int_poly_index -- index of the row in the geoDataFrame
    """

    if gdf is None:
        raise Exception("Internal error: gdf is none")

    if gdf.empty:
        raise Exception("Internal error: gdf can not be empty")

    geometry = [json.loads(gdf.to_json())["features"][int_poly_index]["geometry"]]

    return geometry


# -------------------------------------------------
def fn_get_random_string(int_letter_len_fn, int_num_len_fn):
    """Creates a random string of letters and numbers

    Keyword arguments:
    int_letter_len_fn -- length of string letters
    int_num_len_fn -- length of string numbers
    """
    letters = string.ascii_lowercase
    numbers = string.digits

    str_total = "".join(random.choice(letters) for i in range(int_letter_len_fn))
    str_total += "".join(random.choice(numbers) for i in range(int_num_len_fn))

    return str_total


# -------------------------------------------------
def get_stnd_date(inc_formating=True):
    # Standardizes date pattern

    if inc_formating is False:
        # Returns YYMMDD as in 230725  (UTC)
        str_date = dt.utcnow().strftime("%y%m%d")
    else:
        # Returns 2023-07-25 22:39:41 (UTC)
        str_date = dt.utcnow().strftime("%Y-%m-%d, %H:%M:%S (UTC)")
    return str_date


# -------------------------------------------------
def get_date_with_milli(add_random=True):
    # This returns a pattern of YYMMDD_HHMMSSf_{random 4 digit} (f meaning milliseconds to 6 decimals)
    # Some multi processing functions use this for file names.

    # We found that some processes can get stuck which can create collisions, so we added a 4 digit
    # random num on the end (1000 - 9999). Yes.. it happened.

    # If add_random is False, the the 4 digit suffix will be dropped

    str_date = dt.utcnow().strftime("%y%m%d_%H%M%S%f")
    if add_random is True:
        random_id = random.randrange(1000, 9999)
        str_date += "_" + str(random_id)

    return str_date


# -------------------------------------------------
def print_date_time_duration(start_dt, end_dt):
    # *********************
    # NOTE:  Ensure the date/tims coming in are UTC in all situations including
    #     just duration's, even though it really doesn't matter for durations.
    #     We are attempting to use UTC for ALL dates
    # *********************

    """
    Process:
    -------
    Calcuates the difference in time between the start and end time
    and prints is as:

        Duration: 4 hours 23 mins 15 secs

    -------
    Usage:
        from shared_functions as sf
        print(sf.print_current_date_time())

    -------
    Returns:
        Duration as a formatted string

    """
    time_delta = end_dt - start_dt
    total_seconds = int(time_delta.total_seconds())

    total_days, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

    duration_msg = "Duration: " + time_fmt
    # print(duration_msg)

    return duration_msg


# -------------------------------------------------
def get_stnd_r2f_output_folder_name(huc_number, crs):
    """
    Inputs:
        - huc (str)
        - crs (str):  ie) ESPG:2277 or ESRI:107239. Note, must start with ESRI or EPSG (non case-sensitive)

    """

    # returns pattern of {HUC}_{CRS_number}_{stnd date}. e.g 12090301_2277_230725

    # -------------------
    if len(str(huc_number)) != 8:
        raise ValueError("huc number is not eight characters in length")

    if huc_number.isnumeric() is False:
        raise ValueError("huc number is not a number")

    # -------------------
    # validate and split out the crs number.
    is_valid_crs, err_msg, crs_number = val.is_valid_crs(crs)

    if is_valid_crs is False:
        raise ValueError(err_msg)

    std_date = get_stnd_date(False)

    folder_name = f"{huc_number}_{crs_number}_{std_date}"

    return folder_name


# -------------------------------------------------
def progress_bar_handler(executor_dict, verbose, desc):
    for future in tqdm(
        as_completed(executor_dict),
        total=len(executor_dict),
        disable=(not verbose),
        desc=desc,
        bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%\n",
        ncols=100,
    ):
        try:
            future.result()
        except Exception as exc:
            print("{}, {}, {}".format(executor_dict[future], exc.__class__.__name__, exc))
