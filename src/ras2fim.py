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
from create_geocurves import manage_geo_rating_curves_production

import argparse
import os
import pyproj
import shutil
import sys
import time

from datetime import datetime, timezone
from dotenv import load_dotenv

import shared_variables as sv
import shared_validators as svld
import shared_functions as sf

# Global Variables
b_terrain_check_only = False


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# If you are calling this function from an another python file, please just call this function
# as it validates inputs and sets up other key variables.
# Then will make the call to fn_run_ras2fim

def init_and_run_ras2fim(str_huc8_arg, 
                         str_crs_arg,
                         r2f_output_dir = sv.R2F_DEFAULT_OUTPUT_MODELS,
                         str_hec_path  = sv.DEFAULT_HECRAS_ENGINE_PATH,
                         str_ras_path_arg = sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
                         str_nation_arg  = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                         str_terrain_override = 'None Specified - using USGS WCS',
                         model_huc_catalog_path = sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
                         str_step_override = 'None Specified - starting at the beginning',
                         output_resolution = 10,
                         config_file = sv.DEFAULT_CONFIG_FILE_PATH):


    ####################################################################
    # Load the enviroment file

    # The sv.DEFAULT_CONFIG_FILE_PATH comes in relative to the root and not to src/ras2fim
    # so we need to adjust it's path.

    if (config_file == ""): # possible if not coming through __main__
        raise ValueError("The config file argument can not be empty")

    if (config_file == sv.DEFAULT_CONFIG_FILE_PATH): # change to make relative to ras2fim.py
        referential_path = os.path.join(os.path.dirname(__file__),'..', sv.DEFAULT_CONFIG_FILE_PATH)
        config_file = os.path.abspath(referential_path)

    if (not os.path.exists(config_file)):
        raise ValueError(f"The config file of {config_file} can not found")

    load_dotenv(config_file)

    ####################################################################
    ####  Some validation of input, but mostly setting up pathing ######

    # -------------------
    # Read RAS models units from both prj file and given EPSG code through -p
    # Functions below check for a series of exceptions 

    crs_number, is_valid, err_msg = svld.is_valid_crs(str_crs_arg) # I don't need the crs_number for now
    if (is_valid == False):
        raise ValueError(err_msg)

    proj_crs = pyproj.CRS.from_string(str_crs_arg) 
    model_unit = sf.confirm_models_unit(proj_crs, str_ras_path_arg)

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
    if (os.path.exists(r2f_output_dir) == False): # parent path must exist
        raise ValueError(f'The path of {r2f_output_dir} can not be found. Either the default path of '\
                         f'{sv.R2F_DEFAULT_OUTPUT_MODELS} or a path provided in the -o argument must exist.')

    # -------------------
    get_stnd_r2f_output_folder_name = sf.get_stnd_r2f_output_folder_name(str_huc8_arg, str_crs_arg)
    r2f_huc_output_dir = os.path.join(r2f_output_dir, get_stnd_r2f_output_folder_name)

    if (os.path.exists(r2f_huc_output_dir) == True): 
        raise ValueError(f'The path of {r2f_huc_output_dir} already exists. Please delete it and restart.')

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
    # for some reason, the argparser is sometimes making this an one element array (??)
    if ("[]" in model_huc_catalog_path):
        model_huc_catalog_path = model_huc_catalog_path.replace("[]", str_huc8_arg)
    if (os.path.exists(model_huc_catalog_path) == False):
         raise FileNotFoundError (f"The -mc models catalog ({model_huc_catalog_path}) does not exist. Please check your pathing.")


    # ********************************
    # -------------------
    # make the folder only if all other valudation tests pass.
    os.mkdir(r2f_huc_output_dir) # pathing has already been validated and ensure the child folder does not pre-exist

    # -------------------------------------------
    # ---- Make the "final" folder now as some modules will write to it through the steps
    #      why test if it exists with makedir for output above? We are going to be retarting
    #      the step system so it won't auto make the huc output dir
    r2f_final_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_FINAL)       
    if (not os.path.exists(r2f_final_dir)):
        os.mkdir(r2f_final_dir)

    # -------------------
    # Save incoming args and a few new derived variables created to this point into a log file
    # Careful... when **locals() is called, it will include ALL variables in this function to this point.
    create_input_args_log(**locals())

    # -------------------
    # copy the config env file to the final directory
    shutil.copy2(config_file, r2f_final_dir)

    # -------------------
    # validate dependencies of optional env flags
    # TODO: Create list for module dependencies for any given section (instead of the "if" tests)
    # NOTE: Order is critical here.
    if (os.getenv('RUN_RAS2CATCHMENTS') == 'True') and (os.getenv('RUN_RAS2REM') != 'True'):
        raise ValueError("For the catchments module to run, the env.RUN_RAS2REM must be True.")

    fn_run_ras2fim(str_huc8_arg,
                   str_ras_path_arg,
                   r2f_huc_output_dir,
                   str_crs_arg,
                   str_nation_arg,
                   str_hec_path,
                   str_terrain_override,
                   model_huc_catalog_path,
                   int_step,
                   output_resolution,
                   model_unit)


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

# If you are calling this python file from an another python file, DO NOT call this function first.
# Call the init_and_run_ras2fim function as it validates inputs and sets up other key variables.


def fn_run_ras2fim(str_huc8_arg,
                   str_ras_path_arg,
                   r2f_huc_output_dir,
                   str_crs_arg,
                   str_nation_arg,
                   str_hec_path,
                   str_terrain_override,
                   model_huc_catalog_path,
                   int_step,
                   output_resolution,
                   model_unit):
    
    start_dt = datetime.now()
    
    print(" ")
    print("+=================================================================+")
    print("|          RUN RAS2FIM FOR A HEC-RAS 1-D DATASET (HUC8)           |")
    print("|     Created by Andy Carter, PE of the National Water Center     |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(r) HUC 8 WATERSHED: " + str(str_huc8_arg))
    print("  ---(i) PATH TO HEC-RAS: " + str(str_ras_path_arg))
    print("  ---(o) OUTPUT DIRECTORY: " + r2f_huc_output_dir)
    print("  ---(p) PROJECTION OF HEC-RAS MODELS: " + str(str_crs_arg))
    print("  ---(n) PATH TO NATIONAL DATASETS: " + str(str_nation_arg))     
    print("  ---(r) PATH TO HEC-RAS v6.0: " + str(str_hec_path))
    print("  ---[t] Optional: Terrain to Utilize" + str(str_terrain_override))
    print("  ---[-mc] Optional: path to models catalog - " + str(model_huc_catalog_path))    
    print("  ---[s] Optional: step to start at - " + str(int_step))
    print("  --- The Ras Models unit (extracted from RAS model prj file and given EPSG code): " + model_unit)
    print("===================================================================")
    print(" ")

    # -------------------------------------------
    # ---- Make the "final" folder now as some modules will write to it through the steps
    #      why test if it exists with makedir for output above? We are going to be retarting
    #      the step system so it won't auto make the huc output dir
    r2f_final_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_FINAL)       

    # ---- Step 1: create_shapes_from_hecras ----
    # create a folder for the shapefiles from hec-ras
    print()
    print ("+++++++ Processing: STEP 1 (create_shapes_from_hecras) +++++++" )

    str_shapes_from_hecras_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS) 
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
    print ("+++++++ Processing: STEP 2 (conflate_hecras_to_nwm) +++++++" )

    str_shapes_from_conflation_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF)
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
    print ("+++++++ Processing: STEP 3 get / cut DEM +++++++" )

    # create output folder
    str_terrain_from_usgs_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_TERRAIN)
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
    str_hecras_terrain_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_HECRAS_TERRAIN)
    if not os.path.exists(str_hecras_terrain_dir):
        os.mkdir(str_hecras_terrain_dir)
    print()
    print ("+++++++  Processing: STEP 4 (convert tif to ras hdf5) +++++++" )
        
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
    str_hecras_out_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    if not os.path.exists(str_hecras_out_dir):
        os.mkdir(str_hecras_out_dir)

    print()
    print ("+++++++ Processing: STEP 5 (create fim rasters) +++++++" )
    
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
    flt_resolution_depth_grid = int(output_resolution)

    print()
    print ("+++++++ Processing:  STEP 5/6 (simplifying fim rasters and create metrics)  +++++++" )

    fn_simplify_fim_rasters(str_hecras_out_dir,
                            flt_resolution_depth_grid,
                            sv.DEFAULT_RASTER_OUTPUT_CRS,
                            model_unit)
        
    # ----------------------------------------
    print()
    print ("+++++++ Processing: STEP 5.c (calculate terrain statistics +++++++" )

    fn_calculate_all_terrain_stats(str_hecras_out_dir)

    # -------------------------------------------------
    if (os.getenv('RUN_RAS2REM') == 'True'):

        print()
        print ("+++++++ Processing: ras2rem +++++++" )

        fn_run_ras2rem(r2f_huc_output_dir, model_unit)

        r2f_ras2rem_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_METRIC, sv.R2F_OUTPUT_DIR_RAS2REM)  
        shutil.copy2(os.path.join(r2f_ras2rem_dir, "rem.tif"), r2f_final_dir)
        shutil.copy2(os.path.join(r2f_ras2rem_dir, "rating_curve.csv"), r2f_final_dir)

    # -------------------------------------------------
    if (os.getenv('RUN_RAS2CATCHMENTS') == 'True'):
        
        print()
        print ("+++++++ Processing: ras2catchments +++++++" )

        make_catchments(str_huc8_arg, r2f_huc_output_dir, str_nation_arg, model_huc_catalog_path)

        r2f_catchments_dir = os.path.join(r2f_huc_output_dir, sv.R2F_OUTPUT_DIR_CATCHMENTS)   
        shutil.copy2(os.path.join(r2f_catchments_dir, "nwm_catchments_subset.gpkg"), r2f_final_dir)
        shutil.copy2(os.path.join(r2f_catchments_dir, "r2f_features.tif"), r2f_final_dir)
        shutil.copy2(os.path.join(r2f_catchments_dir, "r2f_features_meta.gpkg"), r2f_final_dir)

    # -------------------------------------------------
    print()
    print ("+++++++ Finalizing processing +++++++" )

    # TODO: use the models catalog to add columns for success/fail processing for each model and why it failed
    # if applicable.  (maybe?)
    # Copy it here in case it gets updated along the way
    shutil.copy2(model_huc_catalog_path, r2f_final_dir)
    run_arguments_filepath = os.path.join(r2f_huc_output_dir, 'run_arguments.txt')
    shutil.copy2(run_arguments_filepath, r2f_final_dir)

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

    
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^    

if __name__ == '__main__':


    # Sample usage:
    # Using all defaults:
    #     python ras2fim.py -w 12090301 -p EPSG:2277 -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif

    # There are a number of ways to use arguments:
    #
    # 1) You can use the defaults, which will use the folder structure as seen in the docs/INSTALL.md. It will require you to 
    #    to create folders in a specific pattern and specific names.

    # 2) You can also use the current code in legacy fashion with all of it's previous arguments.
    #    ie) python ras2fim.py -w 10170204 -i C:\HEC\input_folder -o C:\HEC\output_folder -p EPSG:26915
    #            -n E:\X-NWS\X-National_Datasets -r "C:\Program Files (x86)\HEC\HEC-RAS\6.0"
    #        
    #    But any and all optional arguments can be overridden, so let's try this version:
    #    ie) python ras2fim.py -w 12090301 -i C:\HEC\input_folder -o c:/users/my_user/desktop/ras2fim_outputs-p EPSG:2277
    #                          -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif -n E:\X-NWS\X-National_Datasets
    #    
    #        - When the -n arg not being set, it defaults to c:/ras2fim_data/inputs/X-National_datasets.
    #
    #         - The -i arg is optional and defaults to c:/ras2fim_data/OWP_ras_models/models. Each file or subfolder in this directory will be used as input into the ras2fim.py code.
    #              Again, you an override this to any location you like.
    #
    # When ras2fim.py is run, it will automatically create an output folder name with the output files and some subfolders. 
    #     The folder name will be based on the pattern of {HUC number}_{CRS}_{DATE (YYMMDD)}. e.g.  12090301_2277_230725


    # ++++ Config file notes ++++++

    # By default, many of the section processing can be turned on/off by using a config file.
    # The default file is located at /config/r2f_config.env and it will be loaded. If you look at the file
    # it will look for some critical env variables to tell the system a number of things.
    # By using the -c argument, you can override to use your own config file in any location on your computer,
    # and make sure you fully path it in the argument.
    # Also ensure the file is a valid python environment file (see the current /config/) and has all of the most
    # current env variables. This app will fail if it is missing a variable from the config file.


    #  Note: Careful on copy / pasting commands directly from here as some have line breaks for display purposes.
    #        Python command line commands don't like line breaks and you will need to remove them.

    # There is a known problem with  proj_db error.
    # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db.
    # This will not stop all of the errors but some (in multi-proc).
    sf.fix_proj_path_error()

    parser = argparse.ArgumentParser(description='========== RUN RAS2FIM FOR A HEC-RAS 1-D DATASET (HUC8) ==========')

    parser.add_argument('-w',
                        dest = "str_huc8_arg",
                        help = 'REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required = True, metavar='',
                        type = str)  # has to be string so it doesn't strip the leading zero

    parser.add_argument('-p',
                        dest = "str_crs_arg",
                        help = 'REQUIRED: projection of HEC-RAS models: Example EPSG:2277',
                        required = True, metavar='', type = str)

    parser.add_argument('-o',
                        dest = "r2f_output_dir",
                        help = 'OPTIONAL: An ras2fim output folder will be created and automatically named. ' \
                               'It will default to ' + sv.R2F_DEFAULT_OUTPUT_MODELS + ', however by using this arg, '\
                               'you can override that path.',
                        required = False, metavar='', 
                        default = sv.R2F_DEFAULT_OUTPUT_MODELS,
                        type = str) 
    
    parser.add_argument('-r',
                        dest = "str_hec_path",
                        help = r'OPTIONAL: path to HEC-RAS 6.0: Defaults to C:\Program Files (x86)\HEC\HEC-RAS\6.0' \
                               r' but you can override it, Example: "C:\Program Files (x86)\HEC\HEC-RAS\6.3" (wrap in quotes)',
                        required = False, metavar='',
                        default = sv.DEFAULT_HECRAS_ENGINE_PATH,
                        type = str)

    parser.add_argument('-i',
                        dest = "str_ras_path_arg",
                        help = r'OPTIONAL: path containing the HEC_RAS files: Example -i C:\HEC\input_folder\my_models.' \
                               r' Defaults to c:\ras2fim_datas\OWP_ras_models\models.',
                        default = sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
                        required = False, metavar='',
                        type = str)

    parser.add_argument('-n',
                        dest = "str_nation_arg",
                        help = r'OPTIONAL: path to national datasets: Example: E:\X-NWS\X-National_Datasets.' \
                               r' Defaults to c:\ras2fim_data\inputs\X-National_Datasets.',
                        default = sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
                        required = False, metavar='',
                        type = str)

    parser.add_argument('-t',
                        dest = "str_terrain_override",
                        help = r'OPTIONAL: full path to DEM Tif to use for mapping: Example: c:\ras2fim_data\inputs\some_dem.tif.' \
                                'Defaults to calling USGS website, but note.. it can be unstable and throw 404 and 500 errors.',
                        required = False, metavar='',
                        default = 'None Specified - using USGS WCS',
                        type = str)

    parser.add_argument('-s',
                        dest = "str_step_override",
                        help = 'OPTIONAL: step of processing to start on. Note: This feature is temporarily not working.',
                        required = False, metavar='',
                        default = 'None Specified - starting at the beginning',
                        type = str)

    parser.add_argument('-mc',
                        dest = "model_huc_catalog_path",
                        help =  'OPTIONAL: path to model catalog csv, filtered for the supplied HUC, file downloaded from S3.' \
                               r' Defaults to c:\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog_[].csv and will use subsitution'\
                                ' to replace the [] with the huc number.',
                        default = sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
                        required = False, metavar='',
                        type = str)

    parser.add_argument('-res',
                    dest = "output_resolution",
                    help = 'OPTIONAL: Spatial resolution of flood depth rasters (Simplified Rasters). Defaults to 10.',
                    required = False, metavar='',
                    default = 10,
                    type = int)
    
    parser.add_argument('-c',
                        dest = "config_file",
                        help = "OPTIONAL: If you want to override the default config/r2f_config.env, enter" \
                               " a full path and name of your config path. Make sure it is a proper python" \
                               " formatted env file. Defaults to config/r2f_config.env",
                        required = False, metavar='',
                        default = sv.DEFAULT_CONFIG_FILE_PATH,
                        type = str)

    args = vars(parser.parse_args())
    
    init_and_run_ras2fim(**args)
