#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import subprocess
import sys
import traceback

import colored as cl

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

__USGS_CRS = "EPSG:4269" # we will make all of our outputs in 4269

# ++++++++++++++++++++++++

# Note: This tool has an option to upload it to an S3 bucket and is defaulted to the NOAA, ras2fim bucket.
# That bucket is not publicly available, but you are welcome to upload  to your own 
# bucket if you like.

# ++++++++++++++++++++++++

# -------------------------------------------------
def acquire_and_preprocess_3dep_dems(
      huc8s, path_wbd_huc12s_gpkg, target_output_folder_path,
      inc_upload_outputs_to_s3, s3_path
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

    - If the 'inc_upload_outputs_to_s3' flag is TRUE, it will use the s3_path to upload it. If the DEM exists
      already in S3, it will also ask if the user wants to overwrite it.

    Notes:
       - As this is a relatively low use tool, and most values such as the USGS vrt path, output
         file names, etc are all hardcoded.

       - The output folder can be defined but not the output file names.

       - The output will create two files, one for the domains used for the cut to USGS
         and the other is the actual DEM. Folder input and outputs are overrideable but defaulted.
         The default root output folder will be c:/ras2fim_data/inputs/dems/ras_3dep_HUC8_10m. If those file
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

            

        - inc_upload_outputs_to_s3 (True / False)
            If true, the newly created DEM(s) will be uploaded to S3 if the path is valid from
            the s3_path variable.

        - s3_path (str)
           URL path where the DEMs should be uploaded. Defaults to
           s3://ras2fim/inputs/inputs/dems/ras_3dep_HUC8_10m

    '''
    print()
    start_dt = dt.datetime.utcnow()

    RLOG.lprint("****************************************")
    RLOG.notice("==== Acquiring and Preprocess of HUC8 domains ===")
    RLOG.lprint(f"      Started (UTC): {get_stnd_date()}")
    RLOG.lprint(f"  --- (-huc) HUC8(s): {huc8s}")
    RLOG.lprint(f"  --- (-wbd) Path to WBD HUC12s gkpg: {path_wbd_huc12s_gpkg}")
    RLOG.lprint(f"  --- (-t) Path to output folder: {target_output_folder_path}")



    #RLOG.lprint(f"  --- (-proj) DEM downloaded target projection: {target_projection}")
    RLOG.lprint(f"  --- (-u) Upload to output to S3 ?: {inc_upload_outputs_to_s3}")
    if inc_upload_outputs_to_s3 is True:
        RLOG.lprint(f"  --- (-s3) Path to upload outputs to S3: {s3_path}")
    RLOG.lprint("+-----------------------------------------------------------------+")

    # ------------
    # Validation

    if os.path.exists(path_wbd_huc12s_gpkg) is False:
        raise ValueError(f"File path to {path_wbd_huc12s_gpkg} does not exist")    
    
    # ------------
    # split the incoming arg huc8s which might be a string or list
    huc8s = huc8s.replace(' ', '')
    if huc8s == "":
        raise ValueError("huc8 list is empty")
    if ',' in huc8s:
        huc_list = huc8s.split(',')
    else:
        huc_list = [huc8s]

    for huc in huc_list:
        huc_valid, err_msg = val.is_valid_huc(huc)
        if huc_valid is False:
            raise ValueError(err_msg)

    # ------------
    if os.path.isfile(path_wbd_huc12s_gpkg) is False:
        raise ValueError("File to wbd huc12 gpkg does not exist")

    # ---------------
    #is_valid, err_msg, crs_number = val.is_valid_crs(target_projection)
    #if is_valid is False:
    #    raise ValueError(err_msg)

    # ------------
    if inc_upload_outputs_to_s3 is True:
        # check ras2fim output bucket exists

        adj_s3_path = s3_path.replace("s3://", "")
        path_segs = adj_s3_path.split("/")
        bucket_name = path_segs[0]

        if s3_sf.does_s3_bucket_exist(bucket_name) is False:
            raise ValueError(f"s3 bucket of {bucket_name} ... does not exist")

    os.makedirs(target_output_folder_path, exist_ok=True)

    try:
        # ------------
        # Create domain files - list of dictionaries
        huc_domain_files = []
        for huc in huc_list:
            # Create the HUC8 extent area and save the poly
            # logging done inside function
            domain_file_path = hd.fn_extend_huc8_domain(
                huc, path_wbd_huc12s_gpkg, target_output_folder_path, __USGS_CRS, False
            )
            domain_file_name = os.path.basename(domain_file_path)
            item = {'huc': huc,
                    'domain_file_name': domain_file_name,
                    'domain_file_path': domain_file_path
                    }
            # adds a dict to the list
            huc_domain_files.append(item)

        # ------------
        # Call USGS to create dem files
        # If it errors in here, it will stop execution
        huc_dem_files = []
        for item in huc_domain_files:
            huc = item['huc']
            dem_file_name = f"HUC8_{huc}_dem.tif"
            domain_file_path = item['domain_file_path']
            output_dem_file_path = os.path.join(target_output_folder_path, dem_file_name)
            # logging done inside function
            __download_usgs_dem(domain_file_path, output_dem_file_path)
            item = {'dem_file_name': dem_file_name,                    
                    'dem_file_path': output_dem_file_path}
            huc_dem_files.append(item)

        # ------------
        if inc_upload_outputs_to_s3 is True:
            print()
            RLOG.notice("We will be uploading both the domain file(s) and the dem file(s)."
                        " Domain files first.")
            # Upload Domain Files
            for item in huc_domain_files:
                domain_file_name = item['domain_file_name']
                domain_file_path = item['domain_file_path']
                __upload_file_to_s3(s3_path, domain_file_name, domain_file_path)

            # Upload DEM Files
            for item in huc_dem_files:
                dem_file_name = item['dem_file_name']
                dem_file_path = item['dem_file_path']
                __upload_file_to_s3(s3_path, dem_file_name, dem_file_path)                

        RLOG.lprint("--------------------------------------")
        RLOG.success(f" Acquire and pre-proccess 3dep DEMs completed: {get_stnd_date()}")
        dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        print(f"log files saved to {RLOG.LOG_FILE_PATH}")
        print()

    except ValueError as ve:
        print(ve)

    except Exception:
        print("An exception occurred. Details:")
        print(traceback.format_exc())

# -------------------------------------------------
def __upload_file_to_s3(s3_path, file_name, src_file_path):
    """
    Checks if the file is already uploaded.
    """

    start_dt = dt.datetime.utcnow()
    s3_file_path = s3_path + "/" + file_name

    print()
    RLOG.lprint(f"-- Start uploading {src_file_path} to {s3_file_path}")

    file_exists = s3_sf.does_s3_file_exist(s3_file_path)
    if file_exists is True:
        # Ask if they want to overwrite it
        print()
        msg = (
            f"{cl.fore.SPRING_GREEN_2B}"
            f"The file to be uploaded already exists at {s3_file_path}. \n"
            "Do you want to overwrite it?\n\n"
            f"{cl.style.RESET}"
            f"   -- Type {cl.fore.SPRING_GREEN_2B}'overwrite'{cl.style.RESET}"
            " if you want to overwrite the s3 file.\n"
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
            return
        else:
            if (resp) != "overwrite":
                RLOG.lprint(f"\n.. You have entered an invalid response of {resp}. Program stopped.\n")
                sys.exit(0)

    print(" *** Stand by, this may take 20 seconds to 10 minutes depending on computer resources")
    s3_sf.upload_file_to_s3(src_file_path, s3_file_path)
    print()
    RLOG.lprint(f"Upload to {s3_file_path} - Complete")
    dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


# -------------------------------------------------
def __download_usgs_dem(domain_file_path, output_dem_file_path):
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
        - uses the domain file which is EPSG:4269 and the USGS vrt is 4269, so our
          output will be 4269 (save problems discoverd with reprojecting in gdalwarp
          which has trouble inside conda for some strange reasons. It is not a problem
          running it in VSCode debugger but fails in anaconda powershell window unless
          all of the CRS's match)

    '''
    print()
    start_dt = dt.datetime.utcnow()

    cmd = f'gdalwarp {__USGS_3DEP_10M_VRT_URL} {output_dem_file_path}'
    cmd += f' -cutline {domain_file_path} -crop_to_cutline -ot Float32 -r bilinear'
    cmd += ' -of "GTiff" -overwrite -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256"'
    cmd += ' -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" -cblend 6'

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

        # See if file exists and ask if they want to overwrite it.
        if os.path.exists(output_dem_file_path):  # file, not folder
            msg = (
                f"{cl.fore.SPRING_GREEN_2B}"
                f"The dem file already exists at {output_dem_file_path}. \n"
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
                return
            else:
                if (resp) != "overwrite":
                    RLOG.lprint(f"\n.. You have entered an invalid response of {resp}. Program stopped.\n")
                    sys.exit(0)

        print(" *** Stand by, this may take up to 10 mins depending on computer resources.\n"
              " Note: sometimes the connection to USGS servers can be bumpy so retry if you"
              " have trouble.")
        print()
        RLOG.lprint(f"Downloading USGS DEM for {output_dem_file_path}")
        process = subprocess.run(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True,
        )
        print()
        RLOG.lprint(process)
        print()

        if process.stderr != "":
            if "ERROR" in process.stderr.upper():
                msg = f" Downloading -- {output_dem_file_path}" f"  ERROR -- details: ({process.stderr})"
                RLOG.critical(msg)
                sys.exit(1)
        else:
            msg = f" Downloading -- {output_dem_file_path} - Complete"
            RLOG.lprint(msg)

        dur_msg = print_date_time_duration(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        print()

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
        path is defaulted to "s3://ras2fim/inputs/dems/ras_3dep_HUC8_10m/"

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
        '-skips3',
        '--inc_upload_outputs_to_s3',
        help='OPTIONAL: If the flag is included, it will upload output files to S3.'
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