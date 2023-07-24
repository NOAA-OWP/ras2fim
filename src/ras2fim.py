# This is the main orchestration script for "ras2fim".  It attempts to convert
# geospatial 1D HEC-RAS models into a set of flood inundation mapping (fim)
# library of rasters with a corresponding synthetic rating curve for a
# corresponding National Water Model (NWM) reach segment.
#
# This script needs other scripts to complete the process
# [create_shapes_from_hecras, conflate_hecras_to_nwm, get_usgs_dem_from_shape,
# clip_dem_from_shape, convert_tif_to_ras_hdf5, create_fim_rasters, 
# worker_fim_raster, simplify_fim_rasters, calculate_all_terrain_stats]
#
# This is built to run on a Windows machine and requires that HEC-RAS v6.0
# be installed prior to execution.
#
# Created by: Andy Carter, PE
# Last revised - 2021.10.21
#
# Main code for ras2fim
# Uses the 'ras2fim' conda environment


from create_shapes_from_hecras import fn_create_shapes_from_hecras
from conflate_hecras_to_nwm import fn_conflate_hecras_to_nwm
from get_usgs_dem_from_shape import fn_get_usgs_dem_from_shape
from clip_dem_from_shape import fn_cut_dems_from_shapes
from convert_tif_to_ras_hdf5 import fn_convert_tif_to_ras_hdf5
from create_fim_rasters import fn_create_fim_rasters
from simplify_fim_rasters import fn_simplify_fim_rasters
from calculate_all_terrain_stats import fn_calculate_all_terrain_stats
from run_ras2rem import fn_run_ras2rem
from ras2catchments import make_catchments

import argparse
import os
import shutil
import sys
import time
import pyproj
from datetime import datetime, timezone




import shared_variables as sv
import shared_functions as sf

# Global Variables
b_terrain_check_only = False

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_run_ras2fim(str_huc8_arg,
                   str_ras_path_arg,
                   str_out_arg,
                   str_crs_arg,
                   str_nation_arg,
                   str_hec_path,
                   str_terrain_override,
                   run_ras2rem,
                   model_huc_catalog_path,
                   int_step,
                   output_resolution,
                   model_unit
                   ):
    
    start_dt = datetime.now()
    
    print(" ")
    print("+=================================================================+")
    print("|          RUN RAS2FIM FOR A HEC-RAS 1-D DATASET (HUC8)           |")
    print("|     Created by Andy Carter, PE of the National Water Center     |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(r) HUC 8 WATERSHED: " + str(str_huc8_arg))
    print("  ---(i) PATH TO HEC-RAS: " + str(str_ras_path_arg))
    print("  ---(o) OUTPUT DIRECTORY: " + str(str_out_arg))
    print("  ---(p) PROJECTION OF HEC-RAS MODELS: " + str(str_crs_arg))
    print("  ---(n) PATH TO NATIONAL DATASETS: " + str(str_nation_arg))     
    print("  ---(r) PATH TO HEC-RAS v6.0: " + str(str_hec_path))
    print("  ---[t] Optional: Terrain to Utilize" + str(str_terrain_override))
    print("  ---[m] Optional: Run RAS2REM: " + str(run_ras2rem))
    print("  ---[-mc] Optional: path to models catalog - " + str(model_huc_catalog_path))    
    print("  ---[s] Optional: step to start at - " + str(int_step))
    print("  --- The Ras Models unit (extracted from RAS model prj file and given EPSG code): " + model_unit)
    print("===================================================================")
    print(" ")

    
    # ---- Step 1: create_shapes_from_hecras ----
    # create a folder for the shapefiles from hec-ras
    print()
    print ("+++++++ Processing for code STEP 1 +++++++" )

    str_shapes_from_hecras_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS) 
    if not os.path.exists(str_shapes_from_hecras_dir):
        os.mkdir(str_shapes_from_hecras_dir)
    
    # run the first script (create_shapes_from_hecras)
    if int_step <= 1:
        fn_create_shapes_from_hecras(str_ras_path_arg,
                                     str_shapes_from_hecras_dir,
                                     str_crs_arg)
    # -------------------------------------------

    # ------ Step 2: conflate_hecras_to_nwm -----    
    # do whatever is needed to create folders and determine variables
    print()
    print ("+++++++ Processing for code  STEP 2 +++++++" )

    str_shapes_from_conflation_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF)
    if not os.path.exists(str_shapes_from_conflation_dir):
        os.mkdir(str_shapes_from_conflation_dir)
    
    # run the second script (conflate_hecras_to_nwm)
    if int_step <= 2:
        fn_conflate_hecras_to_nwm(str_huc8_arg, 
                                  str_shapes_from_hecras_dir, 
                                  str_shapes_from_conflation_dir,
                                  str_nation_arg)
    # -------------------------------------------

    # ------ Step 3: get_usgs_dem_from_shape or clip_dem_from_shape ----    
    str_area_shp_name = str_huc8_arg + "_huc_12_ar.shp"
    str_input_path = os.path.join(str_shapes_from_conflation_dir, str_area_shp_name)
    print()
    print ("+++++++ Processing for code  STEP 3 +++++++" )

    # create output folder
    str_terrain_from_usgs_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_TERRAIN)
    if not os.path.exists(str_terrain_from_usgs_dir):
        os.mkdir(str_terrain_from_usgs_dir)
        
    # field name is from the National watershed boundary dataset (WBD)
    str_field_name = "HUC_12"
    
    # *** variables set - raster terrain harvesting ***
    # ==============================================
    int_res = 3 # resolution of the downloaded terrain (meters)
    int_buffer = 300 # buffer distance for each watershed shp
    int_tile = 1500 # tile size requested from USGS WCS
    # ==============================================
    
    # run the third script 
    if int_step <= 3:
        if str_terrain_override == "None Specified - using USGS WCS":
            # create terrain from the USGS WCS
            fn_get_usgs_dem_from_shape(str_input_path,
                                       str_terrain_from_usgs_dir,
                                       int_res,
                                       int_buffer,
                                       int_tile,
                                       model_unit,
                                       str_field_name)
        else:
            # user has supplied the terrain file
            fn_cut_dems_from_shapes(str_input_path,
                                    str_terrain_override,
                                    str_terrain_from_usgs_dir,
                                    int_buffer,
                                    model_unit,
                                    str_field_name)
    # -------------------------------------------

    # ------  Step 4: convert_tif_to_ras_hdf5 ----- 
     
    # folder of tifs created in third script (get_usgs_dem_from_shape)
    # str_terrain_from_usgs_dir
    
    # create a converted terrain folder
    str_hecras_terrain_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_HECRAS_TERRAIN)
    if not os.path.exists(str_hecras_terrain_dir):
        os.mkdir(str_hecras_terrain_dir)
    print()
    print ("+++++++  Processing for code STEP 4 +++++++" )
        
    str_area_prj_name = str_huc8_arg + "_huc_12_ar.prj"
    str_projection_path = os.path.join(str_shapes_from_conflation_dir, str_area_prj_name)
    
    if int_step <= 4:
        fn_convert_tif_to_ras_hdf5(str_hec_path,
                                   str_terrain_from_usgs_dir,
                                   str_hecras_terrain_dir,
                                   str_projection_path,
                                   model_unit)
    # -------------------------------------------
    
    # ------ Step 5: create_fim_rasters ----- 
    
    # folder of tifs created in third script (get_usgs_dem_from_shape)
    # str_terrain_from_usgs_dir
    
    # create a converted terrain folder
    str_hecras_out_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    if not os.path.exists(str_hecras_out_dir):
        os.mkdir(str_hecras_out_dir)
    print()
    print ("+++++++ Processing for code  STEP 5 +++++++" )
    
    # path to standard input (PlanStandardText01.txt, PlanStandardText02.txt, ProjectStandardText01.txt )
    str_std_input_path = os.getcwd() # assumed as in directory executing script
    
    # *** variables set - raster terrain harvesting ***
    # ==============================================
    if model_unit == 'feet':
        flt_interval = 0.5 # vertical step of average depth (0.5ft)
    else:
        flt_interval = 0.2 # vertical step of average depth (0.2m)
    # ==============================================
    
    if int_step <= 5:
        fn_create_fim_rasters(str_huc8_arg,
                              str_shapes_from_conflation_dir,
                              str_hecras_out_dir,
                              str_projection_path,
                              str_hecras_terrain_dir,
                              str_std_input_path,
                              flt_interval,
                              b_terrain_check_only)
    # -------------------------------------------
    
    # ------ Step 6: simplify fim rasters -----
    # ==============================================
    flt_resolution_depth_grid = int(output_resolution)

    #str_output_crs = "EPSG:3857"
    # ==============================================
    
    print()
    print ("+++++++  Processing for code STEP 6 +++++++" )
    if int_step <= 6:
        fn_simplify_fim_rasters(str_hecras_out_dir,
                                flt_resolution_depth_grid,
                                sv.DEFAULT_RASTER_OUTPUT_CRS,
                                model_unit)
    # ----------------------------------------
    

    # ------ Step 7: calculate terrain statistics -----
    print()
    print ("+++++++ Processing for code  STEP 7 +++++++" )

    if int_step <= 7:
        fn_calculate_all_terrain_stats(str_hecras_out_dir)
    # -------------------------------------------------


    # ------ 
    # Abort if ras2rem disabled.
    if (run_ras2rem == False):
        print ()
        print("*** The -m, process ras2rem flag, was set to false which stops the processing for ras2rem" \
              " as well as calculating catchments and finalization.")
        print()
        print("+=================================================================+")
        print("  RUN RAS2FIM - Completed                                         |")
        sf.print_date_time_duration(start_dt, datetime.now())
        print("+-----------------------------------------------------------------+")
        return


    # ------ Step 8: run ras2rem -----
    print()
    print ("+++++++ Processing for code  STEP 8 +++++++" )

    if int_step <= 8:
        fn_run_ras2rem(str_out_arg, model_unit)
    # -------------------------------------------------


    # ------ Step 9: run ras2catchments -----
    print()
    print ("+++++++ Processing for code  STEP 9 +++++++" )
    if int_step <= 9:
        make_catchments(str_huc8_arg, str_out_arg, str_nation_arg, model_huc_catalog_path)
    # -------------------------------------------------


    # ------ Final Step: cleanup files and move final files to release_files folder -----
    print()
    print ("+++++++ Finalizing processing +++++++" )
    r2f_ras2rem_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_RAS2REM) 
    r2f_catchments_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_CATCHMENTS)   
    r2f_final_dir = os.path.join(str_out_arg, sv.R2F_OUTPUT_DIR_FINAL)   

    # Copy all files from the 06_ras2rem and the 07_ras2catchemnts directories
    if (os.path.exists(r2f_final_dir) == True):
        shutil.rmtree(r2f_final_dir)
        # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
        # so sometimes mkdir can fail if rmtree isn't done
        time.sleep(2) # 2 seconds

    os.mkdir(r2f_final_dir)
    shutil.copy2(os.path.join(r2f_ras2rem_dir, "rem.tif"), r2f_final_dir)
    shutil.copy2(os.path.join(r2f_ras2rem_dir, "rating_curve.csv"), r2f_final_dir)
    shutil.copy2(os.path.join(r2f_catchments_dir, "nwm_catchments_subset.gpkg"), r2f_final_dir)
    shutil.copy2(os.path.join(r2f_catchments_dir, "r2f_features.tif"), r2f_final_dir)
    shutil.copy2(os.path.join(r2f_catchments_dir, "r2f_features_meta.gpkg"), r2f_final_dir)

    # TODO: use this models catalog to add columns for success/fail processing for each model and why it failed
    # if applicable.
    shutil.copy2(model_huc_catalog_path, r2f_final_dir)


    print("+=================================================================+")
    print("  RUN RAS2FIM - Completed                                         |")
    sf.print_date_time_duration(start_dt, datetime.now())
    print("+-----------------------------------------------------------------+")

    

def create_input_args_log (**kwargs):

    '''
    Overview:
        This method takes all incoming arguments, cycles through them and put them in a file
    Inputs:
        **kwargs is any dictionary of key / value pairs
    '''

    r2f_huc_output_dir = kwargs.get("r2f_huc_output_dir")

    arg_log_file = os.path.join(r2f_huc_output_dir, "run_arguments.txt")
    
    # Remove it if is aleady exists (relavent if we add an override system)
    if (os.path.exists(arg_log_file)):
        os.remove(arg_log_file)

    # start with the processing date
    utc_now = datetime.now(timezone.utc)
    str_date = utc_now.strftime("%Y-%m-%d")

    # The file can be parsed later by using the two colons and the line break if ever required
    # We are already talking about using data in this file for metadata files
    # especially as the DEM's becomed versions in the input files which meta data
    # will need to know what fim version of the DEM was used.
    with open(arg_log_file, "w") as arg_file:
        arg_file.write(f"process_date == {str_date}\n")
        arg_file.write(f"command_line_submitted == {(' '.join(sys.argv))}\n")

        for key, value in kwargs.items():
            arg_file.write("%s == %s\n" % (key, value))


def init_and_run_ras2fim(str_huc8_arg, 
                         str_crs_arg,
                         r2f_huc_output_dir,
                         str_hec_path  = sv.DEFAULT_HECRAS_ENGINE_PATH,
                         str_ras_path_arg = sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
                         str_nation_arg  = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                         str_terrain_override = 'None Specified - using USGS WCS',
                         rem_outputs = True,
                         model_huc_catalog_path = sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
                         str_step_override = 'None Specified - starting at the beginning',
                         output_resolution=10):


    ####################################################################
    ####  Some validation of input, but mostly setting up pathing ######

    # -------------------
    #read RAS models units from both prj file and given EPSG code through -p
    #below functions involve a series of excpetion checkings
    epsg_code=int(str_crs_arg.split(':')[1])
    proj_crs = pyproj.CRS.from_epsg(epsg_code)
    model_unit=sf.confirm_models_unit(proj_crs,str_ras_path_arg)

    # -w   (ie 12090301)
    if (len(str_huc8_arg) != 8):
        raise ValueError("the -w flag (HUC8) is not 8 characters long")
    if (str_huc8_arg.isnumeric() == False): # can handle leading zeros
        raise ValueError("the -w flag (HUC8) does not appear to be a HUC8")

    # -------------------
    # -i  (ie OWP_ras_models\models) (HECRAS models)
    if (os.path.exists(str_ras_path_arg) == False) and (str_ras_path_arg != sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH):
        raise ValueError("the -i arg (ras path arg) does not appear to be a valid folder.")

    # -------------------        
    # -o  (ie 12090301_meters_2277_test_1) or some full custom path
    # We need to remove the the last folder name and validate that the parent paths are valid
    is_invalid_path = False
    if ("\\" in r2f_huc_output_dir):  # submitted a full path
        parent_dir_name = os.path.dirname(r2f_huc_output_dir)
        if (os.path.exists(parent_dir_name) == False): # parent path must exist
            is_invalid_path = True
    else: # they provide just a child folder (base path name)

        r2f_huc_output_dir = os.path.join(sv.R2F_DEFAULT_OUTPUT_MODELS, r2f_huc_output_dir)

        # does the default parent folder exist?
        if (os.path.exists(sv.R2F_DEFAULT_OUTPUT_MODELS) == False):
            raise ValueError('The -o arg (huc ras model output path) was set to assume the default parent pathing' \
                             ' which needs to pre-exist. Please create the folder pathing for'\
                            f' {sv.R2F_DEFAULT_OUTPUT_MODELS} or change the -o value.')

        if (os.path.exists(r2f_huc_output_dir) == True): # child folder must not exist
            is_invalid_path = True

    if (is_invalid_path == True):
        raise ValueError('The -o child folder (huc ras model output path) can not pre-exist, as we will create it, but the' \
                         ' parent folders must exist. See code notes in the __main__ section for details and examples')


    # -------------------
    # -n  (ie: inputs\\X-National_Datasets)
    if (os.path.exists(str_nation_arg) == False) and (str_nation_arg != sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR):
        raise ValueError("the -n arg (inputs x national datasets path arg) does not appear to be a valid folder.")

    # -------------------
    # -r  (ie: C:\Program Files (x86)\HEC\HEC-RAS\6.0)
    if (os.path.exists(str_hec_path) == False):
        raise ValueError("the -r arg (HEC-RAS engine path) does not appear to be correct.")

    # -------------------
    # -t  (ie: blank or a path such as inputs\12090301_dem_meters_0_2277.tif)
    if (str_terrain_override != "None Specified - using USGS WCS"):
        if (os.path.exists(str_terrain_override) == False): # might be a full path 
            raise ValueError("the -t arg (terrain override) does not appear to be correct a valid path and file.")

    # -------------------
    if str_step_override == "None Specified - starting at the beginning":  
        int_step = 0
    else:
        if (not str_step_override.isnumeric()):
            raise ValueError("the -o step override is invalid.")
        else:
            int_step = int(str_step_override)

    # -------------------
    # adjust the model_catalog file name if applicable
    if ("[]" in model_huc_catalog_path):
        model_huc_catalog_path = model_huc_catalog_path.replace("[]", str_huc8_arg)
    if (os.path.exists(model_huc_catalog_path) == False):
         raise FileNotFoundError (f"The -mc models catalog ({model_huc_catalog_path}) does not exist. Please check your pathing.")


    # ********************************
    # -------------------
    # make the folder only if all other valudation tests pass.
    os.mkdir(r2f_huc_output_dir) # pathing has already been validated and ensure the child folder does not pre-exist

    # -------------------
    # Save incoming args and a few new derived variables created to this point into a log file
    # Careful... when **locals() is called, it will include ALL variables in this function to this point.
    create_input_args_log(**locals())


    fn_run_ras2fim(str_huc8_arg,
                   str_ras_path_arg,
                   r2f_huc_output_dir,
                   str_crs_arg,
                   str_nation_arg,
                   str_hec_path,
                   str_terrain_override,
                   rem_outputs,
                   model_huc_catalog_path,
                   int_step,
                   output_resolution,
                   model_unit)

    
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^    

if __name__ == '__main__':


    # Sample usage:
    # Using all defaults:
    #     python ras2fim.py -w 12090301 -o 12090301_meters_2277_test_2 -p EPSG:2277 -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif

    # There are a number of ways to use arguments:
    #
    # 1) You can use the defaults, which will use the folder structure as seen in the docs/INSTALL.md. It will require you to 
    #    to create folders in a specific pattern and specific names.

    # 2) You can also use the current code in legacy fashion with all of it's previous arguments.
    #    ie) python ras2fim.py -w 10170204 -i C:\HEC\input_folder -o C:\HEC\output_folder -p EPSG:26915
    #            -v False -n E:\X-NWS\X-National_Datasets -r "C:\Program Files (x86)\HEC\HEC-RAS\6.0"
    #        
    #    But any and all optional arguments can be overridden, so let's try this version:
    #    ie) python ras2fim.py -w 12090301 -i C:\HEC\input_folder -o c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2 -p EPSG:2277
    #                          -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif -n E:\X-NWS\X-National_Datasets
    #    
    #        - When the -n arg not being set, it defaults to c:/ras2fim_data/inputs/X-National_datasets.
    #
    #        - The -o arg is required, but can be either a full path (as shown above), or a simple folder name. 
    #             ie) -o c:/users/my_user/desktop/ras2fim_outputs/12090301_meters_2277_test_2  (the 6 folders of 01, 02... 06 will be under this folder can not be overriden).
    #                 OR
    #                 -o 12090301_meters_2277_test_3  (We will use the root default pathing and become c:/ras2fim_data/outputs_ras2fim_models/12090301_meters_2277_test_3)
    #             As before.. if the final folder of 12090301_meters_2277_test_2 can not pre-exist and we will create it.
    #             However, as before, it will error out if the parent path (ie c:/users/my_user/desktop/ras2fim_outputs) does not exist.
    #
    #         - The -i arg is optional and defaults to c:/ras2fim_data/OWP_ras_models/models. Each file or subfolder in this directory will be used as input into the ras2fim.py code.
    #              Again, you an override this to any location you like.
    #
    #  Note: Careful on copy / pasting commands directly from here as some have line breaks for display purposes.
    #        Python command line commands don't like line breaks and you will need to remove them.


    parser = argparse.ArgumentParser(description='========== RUN RAS2FIM FOR A HEC-RAS 1-D DATASET (HUC8) ==========')

    parser.add_argument('-w',
                        dest = "str_huc8_arg",
                        help = 'REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required = True,
                        type = str)  # has to be string so it doesn't strip the leading zero

    parser.add_argument('-p',
                        dest = "str_crs_arg",
                        help = r'REQUIRED: projection of HEC-RAS models: Example EPSG:2277',
                        required = True,
                        type = str)

    parser.add_argument('-o',
                        dest = "r2f_huc_output_dir",
                        help = r'REQUIRED: This can be used in one of two ways. You can submit either a full path' \
                               r' such as c:\users\my_user\Desktop\myoutput OR you can add a simple final folder name.' \
                                ' Please see the embedded notes in the __main__ section of the code for details and  examples.',
                        required = True,
                        type = str) 
    
    parser.add_argument('-r',
                        dest = "str_hec_path",
                        help = r'OPTIONAL: path to HEC-RAS 6.0: Defaults to C:\Program Files (x86)\HEC\HEC-RAS\6.0' \
                               r' but you can override it, Example: "C:\Program Files (x86)\HEC\HEC-RAS\6.3" (wrap in quotes)',
                        required = False,
                        default = sv.DEFAULT_HECRAS_ENGINE_PATH,
                        type = str)

    parser.add_argument('-i',
                        dest = "str_ras_path_arg",
                        help = r'OPTIONAL: path containing the HEC_RAS files: Example -i C:\HEC\input_folder\my_models.' \
                               r' Defaults to c:\ras2fim_datas\OWP_ras_models\models.',
                        default = sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
                        required = False,
                        type = str)

    parser.add_argument('-n',
                        dest = "str_nation_arg",
                        help = r'OPTIONAL: path to national datasets: Example: E:\X-NWS\X-National_Datasets.' \
                               r' Defaults to c:\ras2fim_data\inputs\X-National_Datasets.',
                        default = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                        required = False,
                        type = str)


    parser.add_argument('-t',
                        dest = "str_terrain_override",
                        help = r'OPTIONAL: full path to DEM Tif to use for mapping: Example: c:\ras2fim_data\inputs\some_dem.tif.' \
                               r'Defaults to calling USGS website, but note.. it can be unstable and throw 404 and 500 errors.',
                        required = False,
                        default = 'None Specified - using USGS WCS',
                        type = str)

    parser.add_argument('-m',
                        dest = "rem_outputs",
                        help = r'OPTIONAL: flag to dictate including RAS2REM execution: Enter false to skip, defaults to TRUE.',
                        required = False,
                        default = True,
                        metavar = 'T/F',
                        type = str2bool)

    parser.add_argument('-s',
                        dest = "str_step_override",
                        help = r'OPTIONAL: step of processing to start on. Note: This feature is temporarily not working.',
                        required = False,
                        default = 'None Specified - starting at the beginning',
                        type = str)

    parser.add_argument('-mc',
                        dest = "model_huc_catalog_path",
                        help = r'OPTIONAL: path to model catalog csv, filtered for the supplied HUC, file downloaded from S3.' \
                               r' Defaults to c:\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog_[].csv and will use subsitution'\
                               r' to replace the [] with the huc number.',
                        default = sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
                        required = False,
                        type = str)

    parser.add_argument('-res',
                    dest = "output_resolution",
                    help = r'OPTIONAL: Spatial resolution of flood depth rasters (Simplified Rasters). Defaults to 10.',
                    required = False,
                    default = 10,
                    type = int)
    
    args = vars(parser.parse_args())
    
    init_and_run_ras2fim(**args)
