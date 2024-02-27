#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import sys
import traceback

import colored as cl
import fiona
import geopandas as gpd
import pyproj
import rasterio as rio
from rasterio.mask import mask


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import extend_huc8_domain as hd
import s3_shared_functions as s3_sf

import shared_functions as sf
import shared_validators as val
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG

# local constants (until changed to input param)
# This URL is part of a series of vrt data available from USGS via an S3 Bucket.
# for more info see: "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/".
# The odd folder numbering is a translation of arc seconds with 13m  being 1/3 arc second or 10 meters.
# 10m = 13 (1/3 arc second)
# __USGS_3DEP_10M_VRT_URL = (
#    r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'
# )

# ++++++++++++++++++++++++

# Note: This tool has an option to upload it to an S3 bucket and is defaulted to the NOAA, ras2fim bucket.
# That bucket is not publicly available, but you are welcome to upload  to your own
# bucket if you like.

# ++++++++++++++++++++++++


# -------------------------------------------------
def acquire_and_preprocess_3dep_dems(
    huc, path_wbd_huc12s_gpkg, target_output_folder_path, inc_upload_outputs_to_s3, s3_path, target_projection
):
    """
    Overview
    ----------
    This will download 3dep rasters from USGS using USGS vrts.

    Steps:
    - With the incoming HUC8 it will look for the HUC8 extent and all touching HUC12's via
      the extend_huc8_domain tool.  The HUC8 will now have a larger extent gpkg that can be submitted
      to USGS for their DEM.

    - The new domain poly (per HUC8), is submitted as a extent area to USGS which will download the DEM
      using that spatial extent area. The new output DEMs will have the HUC8 name in it.

    - If the 'inc_upload_outputs_to_s3' flag is TRUE, it will use the s3_path to upload it. If the DEM exists
      already in S3, it will also ask if the user wants to overwrite it.

    Notes:
       - As this is a relatively low use tool, and most values such as the USGS vrt path, output
         file names, etc are all hardcoded.

       - The output folder can be defined but not the output file names.

       - The output will create three files:
            1: for the domains used for the cut to USGS and
            2: the other is the actual DEM (now called a ras2fim DEM)
            3: using the DEM above, make out any levees that are in that extent.

        Folder input and outputs are overrideable but defaulted.
        The default root output folder will be c:/ras2fim_data/inputs/dems/ras_3dep_HUC8_10m. If those file
        already exist, they will automatically be overwritten.

       - Each ras2fim dem output file will follow the pattern of "HUC8_{huc_number}_dem.tif"
       - Each ras2fim dem output file with levees will follow the
         pattern of "HUC8_{huc_number}_w_levee_dem.tif"
       - Each domain output file will follow a pattern "HUC8_{huc_number}_domain.gpkg"

    Parameters
    ----------
        - huc (str):
            A single HUC8 as a string. Using a string prevents lost leading zeros.

        - path_wbd_huc12s_gpkg (str):
            The location and file name of the WBD_National (or equiv) (layer name must be known and is
            hardcoded in here). ie) C:\ras2fim_data\inputs\X-National_Datasets\WBD_National.gpkg.
            If it has more than one layer, the HUC8 layer must be called `WBD_National — WBDHU8`.

        - target_output_folder_path (str):
            The local output folder name where the DEMs will be saved. Defaults to:
            C:\ras2fim_data\inputs\3dep_dems\HUC8_10m

        - inc_upload_outputs_to_s3 (True / False)
            If true, the newly created DEM(s) will be uploaded to S3 if the path is valid from
            the s3_path variable.

        - s3_path (str)
            URL path where the DEMs should be uploaded. Defaults to
            s3://ras2fim/inputs/inputs/dems/ras_3dep_HUC8_10m

        - target_projection (str)
            Projection of the output DEMS and polygons (if included)
            Defaults to EPSG:5070
    """

    arg_values = locals().copy()

    print()
    start_dt = dt.datetime.utcnow()

    RLOG.lprint("*************************************************")
    RLOG.notice("==== Acquiring and Preprocess of HUC8 domains ===")
    RLOG.lprint(f"  --- (-huc) HUC8: {huc}")
    RLOG.lprint(f"  --- (-wbd) Path to WBD HUC12s gkpg: {path_wbd_huc12s_gpkg}")
    RLOG.lprint(f"  --- (-t) Path to output folder: {target_output_folder_path}")
    RLOG.lprint(f"  --- (-proj) DEM downloaded target projection: {target_projection}")
    RLOG.lprint(f"  --- (-u) Upload to output to S3 ?: {inc_upload_outputs_to_s3}")
    if inc_upload_outputs_to_s3 is True:
        RLOG.lprint(f"  --- (-s3) Path to upload outputs to S3: {s3_path}")
    RLOG.lprint(f"      Started (UTC): {sf.get_stnd_date()}")
    RLOG.lprint("+-----------------------------------------------------------------+")

    # ----------------
    # validate input variables
    # (at this point, there are no new derived variables) - some scripts to have a return dictionary
    __validate_input(**arg_values)

    os.makedirs(target_output_folder_path, exist_ok=True)

    try:
        # ------------
        # Create the HUC8 extent area and save the poly
        # logging done inside function
        domain_file_path = hd.fn_extend_huc8_domain(
            huc, path_wbd_huc12s_gpkg, target_output_folder_path, False
        )
        domain_file_name = os.path.basename(domain_file_path)

        # ------------
        # Call USGS to create dem files
        # If it errors in here, it will stop execution
        # create a temp file before masking in the Levees
        dem_file_raw_name = f"HUC8_{huc}_dem.tif"
        dem_file_raw_path = os.path.join(target_output_folder_path, dem_file_raw_name)
        # logging done inside function
        __download_usgs_dem(domain_file_path, dem_file_raw_path, target_projection)

        # mask levee protect areas into the raw DEM. It will also remove the temp file.
        dem_levee_file_name = f"HUC8_{huc}_w_levee_dem.tif"
        dem_levee_file_path = os.path.join(target_output_folder_path, dem_levee_file_name)
        __mask_dem_w_levee_protected_areas(dem_file_raw_path, dem_levee_file_path)

        # ------------
        if inc_upload_outputs_to_s3 is True:
            print()
            RLOG.notice(" -- We will be uploading the domain file first to S3, then the dem file.")
            # Upload Domain Files
            __upload_file_to_s3(s3_path, domain_file_name, domain_file_path)
            # Now the DEM
            __upload_file_to_s3(s3_path, dem_file_raw_name, dem_file_raw_path)
            # Now the masked DEM
            __upload_file_to_s3(s3_path, dem_levee_file_name, dem_levee_file_path)

            # now the levee version

        RLOG.lprint("--------------------------------------")
        RLOG.success(f" Acquire and pre-proccess 3dep DEMs completed: {sf.get_stnd_date()}")
        dur_msg = sf.get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
        RLOG.lprint(dur_msg)
        print(f"log files saved to {RLOG.LOG_FILE_PATH}")
        print()

    except ValueError as ve:
        print(ve)

    except Exception:
        RLOG.critical("An exception occurred. Details:")
        RLOG.critical(traceback.format_exc())
        sys.exit(1)


# -------------------------------------------------
def __download_usgs_dem(domain_file_path, output_dem_file_path, target_projection):
    """
    Processing:
        - Read in the domain file
        - Adjust it's crs to the usgs default crs
        - Get it's geometry
        - Call the USGS vrt and mask it right away with the geometry (which is the same crs)
        - Extracting the new masked np array (image) and its transform details, adjust the
          meta args.
        - Save it as a temp file with the name adjusted to add _pre_proj to it to the file system.
          Yes, still as 4269 (usgs proj)
        - Reopen the temp file (yes.. we have to save then reload).
        - Reproject it, and save it as the final dem name.
        - Delete the temp file.

        Note: At this time, we want resolution at 10, 10 and it is hardcoded in.

    """

    # the usgs vrt uses 4269, so we have to pull it down, then reproject.
    usgs_crs = "EPSG:4269"

    print()
    RLOG.notice(f" -- Downloading DEM file for {output_dem_file_path}")

    print(
        " *** Stand by, this may take up to 2 to 8 mins depending on computer resources.\n"
        " Note: sometimes the connection to USGS servers can be bumpy so retry if you"
        " have trouble."
    )
    print()

    try:
        gdf_aoi = gpd.read_file(domain_file_path)

        gdf_aoi_reproj = gdf_aoi.to_crs(pyproj.CRS.from_string(usgs_crs))
        new_proj_wkt = pyproj.CRS.from_string(target_projection).to_wkt()

        # get the coordinates of the domain extent
        geoms = sf.get_geometry_from_gdf(gdf_aoi_reproj, 0)

        url = r"/vsicurl/https://prd-tnm.s3.amazonaws.com/"
        url += r"StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt"

        with rio.open(url, 'r') as raster_vrt:
            out_image, out_transform = mask(raster_vrt, geoms, crop=True)

            out_meta = raster_vrt.meta.copy()
            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    'compress': 'lzw',
                    "transform": out_transform,
                }
            )

        # temp pre-reproj file name
        temp_dem_file_path = output_dem_file_path.replace(".tif", "_pre_proj.tif")
        # We need to save it as a temp file with the usgs crs. Shortly, we will reload,
        # reproject and resample.
        with rio.open(
            temp_dem_file_path, "w", **out_meta, tiled=True, blockxsize=1024, blockysize=1024, BIGTIFF='YES'
        ) as dest_file:
            dest_file.write(out_image)

        # Now we can reproject to the projection of our preference.
        sf.reproject_raster(temp_dem_file_path, output_dem_file_path, new_proj_wkt, 10)

        # remove the temp file
        os.remove(temp_dem_file_path)

        RLOG.success("USGS DEM downloaded")

    except Exception:
        RLOG.critical(" -- ALERT: Failure to download file")
        RLOG.critical(traceback.format_exc())
        sys.exit(1)


# -------------------------------------------------
def __mask_dem_w_levee_protected_areas(dem_file_raw_path, dem_levee_file_path):
    """
    Process:

        Inputs:
        (paths adjusted based on target folder arg)
        - dem_file_raw_path: ie) C:\ras2fim_data\inputs\dems\ras_3dep_HUC8_10m\HUC8_12010005_dem.tif
        - dem_file_path: ie) C:\ras2fim_data\inputs\dems\ras_3dep_HUC8_10m\HUC8_12010005_w_levee_dem.tif
    """
    print()
    RLOG.notice(" -- Masking levees into DEM")

    with rio.open(dem_file_raw_path) as dem_raw:
        dem_profile = dem_raw.profile.copy()

        with fiona.open(sv.INPUT_LEVEE_PROT_AREA_FILE_PATH) as leveed:
            geoms = [feature["geometry"] for feature in leveed]

            dem_masked, __ = mask(dem_raw, geoms, invert=True)

    with rio.open(dem_levee_file_path, "w", **dem_profile, BIGTIFF='YES') as dest_file:
        dest_file.write(dem_masked)


# -------------------------------------------------
def __upload_file_to_s3(s3_path, file_name, src_file_path):
    """
    Checks if the file is already uploaded to S3
    """

    start_dt = dt.datetime.utcnow()
    s3_file_path = s3_path + "/" + file_name

    print()
    RLOG.lprint(f"-- Start uploading {src_file_path} to {s3_file_path}")

    file_exists = s3_sf.is_valid_s3_file(s3_file_path)
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

    print(" *** Stand by, this may take 1 to 4 minutes depending on computer resources")
    s3_sf.upload_file_to_s3(src_file_path, s3_file_path)
    print()
    RLOG.success(f"Upload to {s3_file_path} - Complete")
    dur_msg = sf.get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()


# ------------
# Validation (at this point, there are no new derived variables)
def __validate_input(
    huc, path_wbd_huc12s_gpkg, target_output_folder_path, inc_upload_outputs_to_s3, s3_path, target_projection
):
    # target_output_folder_path (no validation needed)

    if os.path.exists(path_wbd_huc12s_gpkg) is False:
        raise ValueError(f"File path to {path_wbd_huc12s_gpkg} does not exist")

    huc_valid, err_msg = val.is_valid_huc(huc)
    if huc_valid is False:
        raise ValueError(err_msg)

    # ------------
    if os.path.isfile(path_wbd_huc12s_gpkg) is False:
        raise ValueError("File to wbd huc12 gpkg does not exist")

    # ---------------
    is_valid, err_msg, ___ = val.is_valid_crs(target_projection)
    if is_valid is False:
        raise ValueError(err_msg)

    # ------------
    if inc_upload_outputs_to_s3 is True:
        # check ras2fim output bucket exists

        adj_s3_path = s3_path.replace("s3://", "")
        path_segs = adj_s3_path.split("/")
        bucket_name = path_segs[0]

        if s3_sf.does_s3_bucket_exist(bucket_name) is False:
            raise ValueError(f"s3 bucket of {bucket_name} ... does not exist")

    # ------------
    # Check the levee file name and path.
    if os.path.exists(sv.INPUT_LEVEE_PROT_AREA_FILE_PATH) is False:
        raise ValueError(f"File path to {sv.INPUT_LEVEE_PROT_AREA_FILE_PATH} does not exist")


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

    sample (min args)
        python ./tools/acquire_and_preprocess_3dep_dems.py -huc 12090301

    Notes:
      - This is a very low use tool. So for now, this only can load 10m (1/3 arc second). For now
        there are some hardcode data such as the most of the args to call the USGS vrt, the URL
        for their VRT, and that this is a HUC8 only. If needed, we can add more flexibility.

      - We can also optionally push the outputs to up to S3. By default, it will upload to S3.
        To stop the S3 upload attempt, add the -skips3 flag. The S3
        path is defaulted to "s3://ras2fim/inputs/dems/ras_3dep_HUC8_10m/"

      - The S3 bucket being uploaded is for NOAA/OWP staff only at this time and requires
        implicit aws credentials on your file system.  You are welcome to use this tool but you will
        be required to have your own S3 bucket, credentials and pathing.

    '''

    parser = argparse.ArgumentParser(description='Acquires and pre-processes USGS 3Dep dems')

    parser.add_argument(
        "-huc", dest="huc", help="REQUIRED: A single HUC8", required=True, metavar="", type=str
    )  # has to be string so it doesn't strip the leading zero

    parser.add_argument(
        '-wbd',
        '--path_wbd_huc12s_gpkg',
        help='OPTIONAL: location and name of the WBD_National (or similar)'
        f'defaults to {sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH}',
        default=sv.INPUT_DEFAULT_WBD_NATIONAL_FILE_PATH,
        required=False,
        metavar="",
    )

    parser.add_argument(
        '-t',
        '--target_output_folder_path',
        help='OPTIONAL: location of where the 3dep files will be saved'
        f' Defaults to {sv.INPUT_3DEP_HUC8_10M_ROOT}',
        default=sv.INPUT_3DEP_HUC8_10M_ROOT,
        required=False,
        metavar="",
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
        metavar="",
    )

    parser.add_argument(
        '-proj',
        '--target_projection',
        help=f'OPTIONAL: Desired output CRS. Defaults to {sv.DEFAULT_RASTER_OUTPUT_CRS}',
        required=False,
        default=sv.DEFAULT_RASTER_OUTPUT_CRS,
        metavar="",
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
