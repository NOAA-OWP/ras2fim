#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import requests
import subprocess
import sys
import traceback
import urllib.request

import colored as cl
import geopandas as gpd
import pyproj

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
#__USGS_3DEP_10M_VRT_URL = (
#    r'/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/USGS_Seamless_DEM_13.vrt'
#)


# ++++++++++++++++++++++++

# Note: This tool has an option to upload it to an S3 bucket and is defaulted to the NOAA, ras2fim bucket.
# That bucket is not publicly available, but you are welcome to upload  to your own 
# bucket if you like.

# ++++++++++++++++++++++++

# -------------------------------------------------
def acquire_and_preprocess_3dep_dems(
      huc8s, path_wbd_huc12s_gpkg, target_output_folder_path,
      inc_upload_outputs_to_s3, s3_path, target_projection):
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

        - inc_upload_outputs_to_s3 (True / False)
            If true, the newly created DEM(s) will be uploaded to S3 if the path is valid from
            the s3_path variable.

        - s3_path (str)
            URL path where the DEMs should be uploaded. Defaults to
            s3://ras2fim/inputs/inputs/dems/ras_3dep_HUC8_10m

        - target_projection (str)
            Projection of the output DEMS and polygons (if included)
            Defaults to EPSG:5070
    '''
    print()
    start_dt = dt.datetime.utcnow()

    RLOG.lprint("****************************************")
    RLOG.notice("==== Acquiring and Preprocess of HUC8 domains ===")
    RLOG.lprint(f"      Started (UTC): {get_stnd_date()}")
    RLOG.lprint(f"  --- (-huc) HUC8(s): {huc8s}")
    RLOG.lprint(f"  --- (-wbd) Path to WBD HUC12s gkpg: {path_wbd_huc12s_gpkg}")
    RLOG.lprint(f"  --- (-t) Path to output folder: {target_output_folder_path}")
    RLOG.lprint(f"  --- (-proj) DEM downloaded target projection: {target_projection}")
    RLOG.lprint(f"  --- (-u) Upload to output to S3 ?: {inc_upload_outputs_to_s3}")
    if inc_upload_outputs_to_s3 is True:
        RLOG.lprint(f"  --- (-s3) Path to upload outputs to S3: {s3_path}")
    RLOG.lprint("+-----------------------------------------------------------------+")

    # ------------
    # Validation
    def _validation(huc8s, path_wbd_huc12s_gpkg, target_projection,
                     inc_upload_outputs_to_s3, s3_path):
            
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
        is_valid, err_msg, crs_number = val.is_valid_crs(target_projection)
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
        
        # Only param needs to be returned at this time so no multi return or return dictionary
        return huc_list  
        

    huc_list = _validation(huc8s, path_wbd_huc12s_gpkg, target_projection,
                     inc_upload_outputs_to_s3, s3_path)

    os.makedirs(target_output_folder_path, exist_ok=True)

    try:
        # ------------
        # Create domain files - list of dictionaries
        huc_domain_files = []
        for huc in huc_list:
            # Create the HUC8 extent area and save the poly
            # logging done inside function
            domain_file_path = hd.fn_extend_huc8_domain(
                huc, path_wbd_huc12s_gpkg, target_output_folder_path, False)
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
def __download_usgs_dem(domain_file_path, output_dem_file_path):
    '''
    Process:
    ----------
    download the actual raw (non reprojected files) from the USGS
    based on stated embedded arguments

    
    TODO  REDO NOTES  (what is an example fof the output_dem_file_path and domain_file_path)

    Notes
    ----------
        - pixel size set to 10,000 x 10,000 (m)
        
        - block size (256) (sometimes we use 512)
        - cblend 6 add's a small buffer when pulling down the tif (ensuring seamless
          overlap at the borders.)
        - uses the domain file which is EPSG:4269 and the USGS is 3857 (lambert), so our




          output will be 4269 (save problems discoverd with reprojecting in gdalwarp
          which has trouble inside conda for some strange reasons. It is not a problem
          running it in VSCode debugger but fails in anaconda powershell window unless
          all of the CRS's match)

    '''
    print()
    start_dt = dt.datetime.utcnow()

    # pull down in EPSG:3857  (the URL below likes 3857 - lambert)
    default_crs = "EPSG:3857" # and adjsut URL if necessary
    usgs_url_header = (
        r"https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/exportImage?"
    )
    
    # TODO: adjust dynamic crs in string
    usgs_url_query_1 = ("SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage&coverage=DEP3Elevation"
        "&CRS=EPSG:3857&FORMAT=GeoTiff")

    # overlap of the requested tiles in units (meters)
    # and other defaults. We will get extra sizes with overlap and buffers to mosaic and ensure no gaps
    default_int_overlap = 10
    #default_int_res = 10 # resolution of the downloaded terrain (meters)
    default_int_buffer = 50  # buffer distance for each watershed shp (meters)
    default_int_tile_size = 30000  # tile size requested from USGS WCS


    # See if file exists and ask if they want to overwrite it.
    """
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
    
    """

    RLOG.lprint(f"Downloading USGS DEM for {output_dem_file_path}")

    # read the "area of interest" file in to geopandas dataframe
    gdf_aoi = gpd.read_file(domain_file_path)

    # lets come back one dir to use as the temp dir
    temp_mosaic_dir = os.path.abspath(os.path.join(output_dem_file_path, "..", "dem_temp_downloads"))
    # create output directory
    os.makedirs(temp_mosaic_dir, exist_ok=True)


    aoi_prj_crs = gdf_aoi.crs.srs  # short form. ie) epsg:5070 (in lower)

    # see if we need to reproject to usgs default
    gdf_aoi_reproj = None
    if aoi_prj_crs.lower() != aoi_prj_crs.lower():
        gdf_aoi_reproj = gdf_aoi.to_crs(pyproj.CRS.from_string(default_crs))
    else:
        gdf_aoi_reproj = gdf_aoi

    # buffer the polygons in the input gkpg poly
    gdf_aoi_reproj["geometry"] = gdf_aoi_reproj.geometry.buffer(default_int_buffer)

    # What if it is an ESRI code coming in?
    #epsg_code = gdf_aoi_reproj.crs.to_epsg()   

    # We want ints and rounded up. We will clip again in a min (again the domain)
    # remember.. we are in negative numbers so min and max are reversed on the x axis

    #usgs_url_query_bb = f"&BBOX={minx},{miny},{maxx},{maxy}"


    default_int_tile_size = 20000
    cell_size = default_int_tile_size / 10

    parent_gdf_bounds = gdf_aoi_reproj.bounds
    parent_minx = round(parent_gdf_bounds.iloc[0]['minx'])
    #parent_maxx = round(parent_gdf_bounds.iloc[0]['maxx'])
    parent_maxx = parent_minx - default_int_tile_size
    parent_miny = round(parent_gdf_bounds.iloc[0]['miny'])
    #parent_maxy = round(parent_gdf_bounds.iloc[0]['maxy'])
    parent_maxy = parent_miny + default_int_tile_size


    #parent_bb_width = round(parent_maxx - parent_minx)  # ie) 147,078
    #parent_bb_height = round(parent_maxy - parent_miny) #  ie) 178,041

    # I had x mixed up with min and max
    # Bbox should have been BBOX=-10908400,3514100,-10911400,3517100  ??  (becuase of the negative x values)
    # usgs_url_query_bb = f"&BBOX={parent_minx},{parent_miny},{parent_maxx},{parent_maxy}"
    #usgs_url_query_bb = f"&BBOX={parent_minx},{parent_miny},{parent_maxx},{parent_maxy}"    

    #usgs_url_query_wh = f"&WIDTH={cell_size}&HEIGHT={cell_size}"
    #full_usgs_url = usgs_url_header + usgs_url_query_1 + usgs_url_query_bb + usgs_url_query_wh

    #download_tiles_list = make_tiles(gdf_aoi_reproj, default_int_overlap, default_int_res, default_int_tile_size, temp_mosaic_dir)

    # "bbox": f"{minx},{maxx},{maxy},{miny}",

    usgs_url_params = {"f": "image",
              "bbox": f"{parent_maxx},{parent_miny},{parent_minx},{parent_maxy}",
              "bboxSR": default_crs,
              "imageSR": default_crs,
              "format": "tiff",
              "pixelType": "F32",
              "size": f"{cell_size},{cell_size}",
              "noDataInterpretation": "esriNoDataMatchAny",
              "interpolation": "RSP_BilinearInterpolation"}

    #full_usgs_url = f'https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/exportImage?SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage&coverage=DEP3Elevation&CRS=EPSG:3857&FORMAT=GeoTiff&BBOX=-10827860.0,3512529.0,-10826360.0,3514029.0&WIDTH=500.0&HEIGHT=500.0'

    #__download_file(usgs_url_header, usgs_url_params,  output_dem_file_path)
    __download_file(usgs_url_params,  output_dem_file_path)

    print("all done for now)")

    # TODO: remove the temp dir

    sys.exit()


    # try:

    #except Exception:
    #    msg = "An exception occurred while downloading file the USGS file."
    #    RLOG.critical(msg)
    #    RLOG.critical(traceback.format_exc())
    #    sys.exit(1)



# -------------------------------------------------
def make_tiles(gdf_aoi_reproj, int_overlap_size, int_tile_size, temp_mosaic_dir):

    """
    Using the total bounding box size (which has a buffer built into the overall size),
    create a list of dictionary objects that are all of the details required as tiles to
    download from USGS.

    Output:
        A list of dictionaries that look like this
        tile_id: (0_0), (0_1)  (same pattern as Andy's)
        bbox_minx:
        bbox_miny:
        bbox_maxx:
        bbox_maxy:
        cell_size:  (a 10 of the total size
        output_size:)

    """
    

    # buffer the domain for downloading from USGS, but we will clip the buffer out later
    # gives us a safe download overlsap
    #gdf_aoi_reproj["geometry"] = gdf_aoi_reproj.geometry.buffer(default_int_buffer)
    parent_gdf_bounds = gdf_aoi_reproj.bounds
    parent_minx = parent_gdf_bounds.iloc[0]['minx']
    parent_maxx = parent_gdf_bounds.iloc[0]['maxx']
    parent_miny = parent_gdf_bounds.iloc[0]['miny']
    parent_maxy = parent_gdf_bounds.iloc[0]['maxy']
    parent_bb_width = round(parent_maxx - parent_minx)  # ie) 147,078
    parent_bb_height = round(parent_maxy - parent_miny) #  ie) 178,041

    # Break into sets of 10,000 (default_int_tile-size)  (remember, x and y might not be the same size).
    # We are going to figure out how many complete boxes of 10,000, then the reminder on 
    # the width (x) and height (y).
    # Don't worry about the overlap yet.

    mod_x = parent_bb_width % int_tile_size 
    # ie) 147,078 % 10,000 = 78 (left over) and we will have 147 x cells at 10,000    

    mod_y = parent_bb_height % int_tile_size
    # ie) 178,041 % 10,000 = 41 (left over) and we will have 178 cells at 10,000

    full_width_x_cells = (parent_bb_width - mod_x) / int_tile_size # ie) 147 
    full_width_y_cells = (parent_bb_height - mod_y) / int_tile_size # ie) 178
    num_full_cells = full_width_x_cells * full_width_y_cells  # 147 x 178 = 26,166 cells and downloads (use multi-thread)



# -------------------------------------------------
def __download_file(usgs_url_params, output_dem_file_path):
    """
    Downloads a requested URL to a requested file directory
    """

    #url_with_params = r'https://elevation.nationalmap.gov/arcgis/services/3DEPElevation/ImageServer/WCSServer?SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage&coverage=DEP3Elevation&CRS=EPSG:3857&BBOX=-10813982,3744776,-10812482,3746276&WIDTH=1500&HEIGHT=1500&FORMAT=GeoTiff'

    try:
        #response = urllib.request.urlretrieve(url_with_params, output_dem_file_path)

        #urllib.request.urlretrieve(url_with_params, output_dem_file_path)

        #if not os.path.exists(output_dem_file_path):
        #    urllib.request.urlretrieve(url_with_params, output_dem_file_path)


        #with requests.get(url=url_with_params, allow_redirects=True, stream=True) as response:
            #print(response)

        #    with open(output_dem_file_path, mode='wb') as localfile:
        #        for chunk in response.iter_content(chunk_size=10 * 1024):
        #            localfile.write(chunk)


        url = r"https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/exportImage?"


        """
        usgs_url_params = {"f": "image",
              "bbox": f"{-10813982},{3744776},{-10812482},{3746276}",
              "bboxSR": "EPSG:3857",
              "imageSR": "EPSG:3857",
              "format": "tiff",
              "pixelType": "F32",
              "size": f"{150},{150}",
              "noDataInterpretation": "esriNoDataMatchAny",
              "interpolation": "RSP_BilinearInterpolation"}
        """

        #"size": f"{parent_bb_width},{parent_bb_height}",

        """
        response = requests.get(url=url, params=usgs_url_params, allow_redirects=True)

        with open(output_dem_file_path, mode="wb") as image_file:
            image_file.write(response.content)
        """

        with requests.get(url=url, params=usgs_url_params, stream=True) as response:
            with open(output_dem_file_path, mode='wb') as localfile:
                for chunk in response.iter_content(chunk_size=10 * 1024):
                    localfile.write(chunk)

        print("hi")

    except:
        RLOG.critical(" -- ALERT: Failure to download file")
        #RLOG.critical(f"url_address: {url_address}")
        #RLOG.critical(f"url_params: {url_params}")
        RLOG.critical(f"writing to: {output_dem_file_path}")
        RLOG.critical(traceback.format_exc())    
        sys.exit(1)


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

    parser.add_argument(
        '-proj',
        '--target_projection',
        help=f'OPTIONAL: Desired output CRS. Defaults to {sv.DEFAULT_RASTER_OUTPUT_CRS}',
        required=False,
        default=sv.DEFAULT_RASTER_OUTPUT_CRS,
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
