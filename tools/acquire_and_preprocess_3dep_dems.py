#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import subprocess
import sys
import traceback


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import extend_huc8_domain as hd
import s3_shared_functions as s3_sf

import ras2fim_logger
import shared_validators as val
import shared_variables as sv
from shared_functions import get_stnd_date, print_date_time_duration


# Global Variables
RLOG = ras2fim_logger.R2F_LOG

# local constants (until changed to input param)
# This URL is part of a series of vrt data available from USGS via an S3 Bucket.
# for more info see: "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/".
# The odd folder numbering is a translation of arc seconds with 13m  being 1/3 arc second or 10 meters.
# 10m = 13 (1/3 arc second)
__USGS_3DEP_10M_VRT_URL = (
    r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'
)


# -------------------------------------------------
def acquire_and_preprocess_3dep_dems(
    huc8s, path_wbd_huc12s_gpkg, target_output_folder_path, target_projection, upload_outputs_to_s3, s3_path
):
    '''
    Overview
    ----------
    This will download 3dep rasters from USGS using USGS vrts.

    Steps:
    - Using the single or set of HUC8s, submit each one at a time to the domain maker tool. It will
      use each HUC8 one at a time and look for all touching HUC12's via the extend_huc8_domain tool.
      Each HUC8 will now have a larger extent gpkg that can be submitted to USGS for their extent.

    - The new domain poly (per HUC8), is submitted as a extent area to USGS which will download the DEM
      using that spatial extent area. The new output DEMs will have the HUC8 name in it.
      If the newly created DEM already exists, it will ask the user if they want to overwrite it.

    - If the 'upload_outputs_to_s3' flag is TRUE, it will use the s3_path to upload it. If the DEM exists
      already in S3, it will also ask if the user wants to overwrite it.

    Notes:
       - As this is a relatively low use tool, and most values such as the USGS vrt path, output
         file names, etc are all hardcoded.

       - The output folder can be defined but not the output file names.

       - The output will create two files, one for the domains used for the cut to USGS
         and the other is the actual DEM. Folder input and outputs are overrideable but defaulted.
         The default root output folder will be c:/ras2fim_data/inputs/dems/3dep_HUC8_10m. If those file
         already exist, they will automatically be overwritten.

       - Each dem output file will follow the pattern of "HUC8_{huc_number}_dem.tif".
       - Each domain output file will follow a pattern "HUC8_{huc_number}_domain.gpkg".

    Parameters
    ----------
        - huc8s (str):
            A single or set of HUC8 as a string. This will be split based on the comma if applicable.
            Using a string prevents lost leading zeros.

        - path_wbd_huc12s_gpkg (str):
            The location and file name of the WBD_National (or equiv) (layer name must be known and is
            hardcoded in here). ie) C:\ras2fim_data\inputs\X-National_Datasets\WBD_National.gpkg.
            If it has more than one layer, the HUC8 layer must be called `WBD_National — WBDHU8`.

        - target_output_folder_path (str):
            The local output folder name where the DEMs will be saved. Defaults to:
            C:\ras2fim_data\inputs\3dep_dems\HUC8_10m

        - target_projection (str)
            Projection of the output DEMS and polygons (if included)
            Defaults to EPSG:5070

        - upload_outputs_to_s3 (True / False)
            If true, the newly created DEM(s) will be uploaded to S3 if the path is valid from
            the s3_path variable.

        - s3_path (str)
           URL path where the DEMs should be uploaded. Defaults to
           s3://ras2fim/inputs/inputs/dems/3dep_HUC8_10m

    '''
    print()
    start_dt = dt.datetime.utcnow()

    RLOG.lprint("****************************************")
    RLOG.lprint("==== Acquiring and Preprocess of HUC8 domains ===")
    RLOG.lprint(f" Started (UTC): {get_stnd_date()}")
    RLOG.lprint(f"  --- (-huc) HUC8(s): {huc8s}")
    RLOG.lprint(f"  --- (-wbd) Path to WBD HUC12s gkpg: {path_wbd_huc12s_gpkg}")
    RLOG.lprint(f"  --- (-t) Path to output folder: {target_output_folder_path}")
    RLOG.lprint(f"  --- (-proj) DEM downloaded target projection: {target_projection}")
    RLOG.lprint(f"  --- (-u) Upload to output to S3 ?: {upload_outputs_to_s3}")
    RLOG.lprint(f"  --- (-s3) Path to upload outputs to S3 (if applicable): {s3_path}")
    RLOG.lprint("+-----------------------------------------------------------------+")

    # ------------
    # Validation
    # TODO

    # ------------
    # split the incoming arg huc8s which might be a string or list
    huc8s = huc8s.replace(' ', '')
    if huc8s == "":
        raise ValueError("huc8 list is empty")
    if ',' in huc8s:
        huc_list = huc8s.split(',')
    else:
        huc_list = [huc8s]

    # ------------
    if os.path.isfile(path_wbd_huc12s_gpkg) is False:
        raise ValueError("File to wbd huc12 pgkg does not exist")

    # ---------------
    is_valid, err_msg, crs_number = val.is_valid_crs(target_projection)
    if is_valid is False:
        raise ValueError(err_msg)

    # ------------
    if upload_outputs_to_s3 is True:
        # check ras2fim output bucket exists
        if s3_sf.is_valid_s3_folder(s3_path) is False:
            raise ValueError(f"S3 output folder of {s3_path} ... does not exist")

    os.makedirs(target_output_folder_path, exist_ok=True)

    try:
        # ------------
        # Create domain files - list of dictionaries
        huc_domain_files = []
        for huc in huc_list:
            # Create the HUC8 extent area and save the poly
            # logging done inside function
            domain_file = hd.fn_extend_huc8_domain(
                huc, path_wbd_huc12s_gpkg, target_output_folder_path, False
            )
            item = {'huc': huc, 'domain_file': domain_file}
            # adds a dict to the list
            huc_domain_files.append(item)

        # ------------
        # Call USGS to create dem files
        # If it errors in here, it will stop execution
        huc_dem_files = []
        for item in huc_domain_files:
            huc = item['huc']
            file_name = f"HUC8_{huc}_dem.tif"
            extent_file = item['domain_file']
            output_dem_file_path = os.path.join(target_output_folder_path, file_name)
            # logging done inside function
            download_usgs_dem(extent_file, file_name, output_dem_file_path, target_projection)
            huc_dem_files.append(output_dem_file_path)

        # TODO:
        # if upload_outputs_to_s3 is True:
        #    for dem_file in huc_dem_files:
        # Check if file exists in S3
        # If file exists, ask user if they want to overwrite.
        # else, just upload it.

        RLOG.lprint("--------------------------------------")
        RLOG.success(f" Acquire and pre-proccess 3dep DEMs completed: {get_stnd_date()}")
        dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)

    except ValueError as ve:
        print(ve)

    except Exception:
        print("An exception occurred. Details:")
        print(traceback.format_exc())


# -------------------------------------------------
def download_usgs_dem(extent_file, file_name, output_dem_file_path, target_projection):
    '''
    Process:
    ----------
    download the actual raw (non reprojected files) from the USGS
    based on stated embedded arguments

    Notes
    ----------
        - pixel size set to 10 x 10 (m)
        - block size (256) (sometimes we use 512)
        - cblend 6 add's a small buffer when pulling down the tif (ensuring seamless
          overlap at the borders.)

    '''

    print(f"-- Downloading USGS DEM for {output_dem_file_path}")

    cmd = f'gdalwarp {__USGS_3DEP_10M_VRT_URL} {output_dem_file_path}'
    cmd += f' -cutline {extent_file} -crop_to_cutline -ot Float32 -r bilinear'
    cmd += ' -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"'
    cmd += ' -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -tr 10 10'
    cmd += f' -t_srs {target_projection} -cblend 6'

    """
    e.q. gdalwarp
       /vs/icurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt
       /data/inputs/usgs/3dep_dems/10m/HUC8_12090301_dem.tif
       -cutline /data/inputs/wbd/HUC8/HUC8_12090301.gpkg
       -co "BLOCKYSIZE=256"
       -crop_to_cutline -ot Float32 -r bilinear -of "GTiff" -overwrite -co "BLOCKXSIZE=256"
       -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -tr 10 10 -t_srs ESRI:102039 -cblend 6
    """

    try:
        process = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True,
        )

        RLOG.lprint(process.stdout)

        if process.stderr != "":
            if "ERROR" in process.stderr.upper():
                msg = f" - Downloading -- {file_name}" f"  ERROR -- details: ({process.stderr})"
                RLOG.critical(msg)
                sys.exit(1)
        else:
            msg = f" - Downloading -- {file_name} - Complete"
            RLOG.lprint(msg)

    except Exception:
        msg = "An exception occurred while downloading file the USGS file."
        RLOG.critical(msg)
        RLOG.critical(traceback.format_exc())
        sys.exit(1)


# -------------------------------------------------
def __setup_logs():
    # Auto saves to C:\ras2fim_data\tool_outputs\logs (not overrideable at this time)

    start_time = dt.datetime.utcnow()
    file_dt_string = start_time.strftime("%y%m%d-%H%M")

    script_file_name = os.path.basename(__file__).split('.')[0]
    file_name = f"{script_file_name}-{file_dt_string}.log"

    if os.path.exists(sv.DEFAULT_LOG_FOLDER_PATH) is False:
        os.makedirs(sv.DEFAULT_LOG_FOLDER_PATH, exist_ok=True)

    log_file_path = os.path.join(sv.DEFAULT_LOG_FOLDER_PATH, file_name)

    # ie) C:\ras2fim_data\tool_outputs\logs\acquire_and_pre_process_3dep_dems-{date}.log
    RLOG.setup(log_file_path)


# -------------------------------------------------
if __name__ == '__main__':
    '''

    TODO: sample (min args)
        python ./data/usgs/acquire_and_preprocess_3dep_dems.py -hucs 12090301

    Notes:
      - This is a very low use tool. So for now, this only can load 10m (1/3 arc second). For now
        there are some hardcode data such as the most of the args to call the USGS vrt, the URL
        for their VRT, and that this is a HUC8 only. If needed, we can add more flexibility.

      - We can also optionally push the outputs to up to S3. It requires adding the -u flag. The S3
        path is defaulted to "s3://ras2fim/inputs/dems/3dep_HUC8_10m/"

    '''

    parser = argparse.ArgumentParser(description='Acquires and pre-processes USGS 3Dep dems')

    parser.add_argument(
        "-hucs",
        dest="huc8s",
        help="REQUIRED: A single or muliple HUC8s. If using multiple HUC8s, ensure the set is"
        " wrapped with quotes and comma with no spaces between numbers.\n"
        " ie) -hucs '12090301,05040301' ",
        required=True,
        metavar="",
        type=str,
    )  # has to be string so it doesn't strip the leading zero

    parser.add_argument(
        '-wbd',
        '--path_wbd_huc12s_gpkg',
        help='OPTIONAL: location and name of the WBD_National (or similar)'
        f'defaults to {sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH}',
        default=sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH,
        required=False,
    )

    parser.add_argument(
        '-t',
        '--target_output_folder_path',
        help='OPTIONAL: location of where the 3dep files will be saved'
        f' Defaults to {sv.INPUT_3DEP_HUC8_10M_ROOT}',
        default=sv.INPUT_3DEP_HUC8_10M_ROOT,
        required=False,
    )

    parser.add_argument(
        '-proj',
        '--target_projection',
        help=f'OPTIONAL: Desired output CRS. Defaults to {sv.DEFAULT_RASTER_OUTPUT_CRS}',
        required=False,
        default=sv.DEFAULT_RASTER_OUTPUT_CRS,
    )

    parser.add_argument(
        '-u',
        '--upload_outputs_to_s3',
        help='OPTIONAL: If the flag is included, it will pload output files to S3.'
        ' Defaults to True (will upload files)',
        required=False,
        default=True,
        action="store_false",
    )

    parser.add_argument(
        '-s3',
        '--s3_path',
        help='OPTIONAL: The root s3 path to upload the new output files to S3. It will overwrite the'
        ' specific files in S3 if those files already there.'
        f' Defaults to {sv.S3_INPUTS_3DEP_DEMS}',
        required=False,
        default=sv.S3_INPUTS_3DEP_DEMS,
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        __setup_logs()

        # call main program
        acquire_and_preprocess_3dep_dems(**args)

    except Exception:
        # The logger does not get setup until after validation, so you may get
        # log system errors potentially when erroring in validation
        RLOG.critical(traceback.format_exc())
