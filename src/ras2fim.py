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
# This is built to run on a Windows machine and requires that HEC-RAS v6.3
# be installed prior to execution.
#
# Created by: Andy Carter, PE

# Main code for ras2fim
# Uses the 'ras2fim' conda environment

import argparse
import datetime as dt
import os
import shutil
import sys
import traceback

import pyproj


import shared_functions as sf
import shared_validators as val
import shared_variables as sv
from calculate_all_terrain_stats import fn_calculate_all_terrain_stats
from clip_dem_from_shape import fn_cut_dems_from_shapes
from conflate_hecras_to_nwm import fn_conflate_hecras_to_nwm
from convert_tif_to_ras_hdf5 import fn_convert_tif_to_ras_hdf5
from create_fim_rasters import fn_create_fim_rasters
from create_geocurves import manage_geo_rating_curves_production
from create_model_domain_polygons import fn_make_domain_polygons
from create_shapes_from_hecras import fn_create_shapes_from_hecras
from create_src_fimrasters_4fids import fn_create_src_feature_ids
from get_usgs_dem_from_shape import fn_get_usgs_dem_from_shape
from reformat_ras_rating_curve import dir_reformat_ras_rc
from simplify_fim_rasters import fn_simplify_fim_rasters


# Global Variables
B_TERRAIN_CHECK_ONLY = False
ARG_LOG_FILE_NAME = "run_arguments.txt"
RLOG = sv.R2F_LOG


# -------------------------------------------------
# If you are calling this function from an another python file, please just call this function
# as it validates inputs and sets up other key variables.
# Then will make the call to fn_run_ras2fim
def init_and_run_ras2fim(
    str_huc8_arg,
    str_crs_arg,
    r2f_output_dir=sv.R2F_DEFAULT_OUTPUT_MODELS,
    str_hec_path=sv.DEFAULT_HECRAS_ENGINE_PATH,
    input_models_path=sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
    str_nation_arg=sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
    str_terrain_override="None Specified - using USGS WCS",
    model_huc_catalog_path=sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
    str_step_override="None Specified - starting at the beginning",
    output_resolution=10,
    config_file=sv.DEFAULT_CONFIG_FILE_PATH,
):
    config_file = sf.load_config_enviro_path(config_file)

    ####################################################################
    #  Some validation of input, but mostly setting up pathing ######

    # -------------------
    # Read RAS models units from both prj file and given EPSG code through -p
    # Functions below check for a series of exceptions

    # I don't need the crs_number for now
    is_valid, err_msg, crs_number = val.is_valid_crs(str_crs_arg)
    if is_valid is False:
        raise ValueError(err_msg)

    # -------------------
    # -w   (ie 12090301)
    huc_valid, err_msg = val.is_valid_huc(str_huc8_arg)
    if huc_valid is False:
        raise ValueError(err_msg)

    # -------------------
    # -i  (ie OWP_ras_models\models) (HECRAS models)
    if os.path.exists(input_models_path) is False:
        raise ValueError(f"the -i arg ({input_models_path}) does not appear to be a valid folder.")

    # -------------------
    proj_crs = pyproj.CRS.from_string(str_crs_arg)
    model_unit = sf.confirm_models_unit(proj_crs, input_models_path)

    # -------------------
    if os.path.exists(r2f_output_dir) is False:  # parent path must exist
        raise ValueError(
            f"The path of {r2f_output_dir} can not be found. Either the default path of "
            f"{sv.R2F_DEFAULT_OUTPUT_MODELS} or a path provided in the -o argument must exist."
        )

    # -------------------
    output_folder_name = sf.get_stnd_r2f_output_folder_name(str_huc8_arg, str_crs_arg)
    output_folder_path = os.path.join(r2f_output_dir, output_folder_name)

    if os.path.exists(output_folder_path) is True:
        raise ValueError(f"The path of {output_folder_path} already exists. Please delete it and restart.")

    # -------------------
    # -n  (ie: inputs\\X-National_Datasets)
    if (os.path.exists(str_nation_arg) is False) and (str_nation_arg != sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR):
        raise ValueError(
            "the -n arg (inputs x national datasets path arg) does not appear to be a valid folder."
        )

    # -------------------
    # -r  (ie: C:\Program Files (x86)\HEC\HEC-RAS\6.3)
    if os.path.exists(str_hec_path) is False:
        raise ValueError("the -r arg (HEC-RAS engine path) does not appear to be correct.")

    # -------------------
    # -t  (ie: blank or a path such as inputs\12090301_dem_meters_0_2277.tif)
    if str_terrain_override != "None Specified - using USGS WCS":
        if os.path.exists(str_terrain_override) is False:  # might be a full path
            raise ValueError(
                f"the -t arg (terrain override) path of {str_terrain_override} does not appear exist."
            )

    # -------------------
    if str_step_override == "None Specified - starting at the beginning":
        int_step = 0
    else:
        if not str_step_override.isnumeric():
            raise ValueError("the -o step override is invalid.")
        else:
            int_step = int(str_step_override)

    # ********************************
    # -------------------
    # make the folder only if all other valudation tests pass.
    # pathing has already been validated and ensure the child folder does not pre-exist
    os.mkdir(output_folder_path)

    # -------------------
    # adjust the model_catalog file name if applicable
    # for some reason, the argparser is sometimes making this an one element array (??)
    if "[]" in model_huc_catalog_path:
        model_huc_catalog_path = model_huc_catalog_path.replace("[]", str_huc8_arg)
    if os.path.exists(model_huc_catalog_path) is False:
        raise FileNotFoundError(
            f"The -mc models catalog ({model_huc_catalog_path}) does not exist. Please check your pathing."
        )
    # We need to copy the catalog to the root of the output unit so steps can use it
    # It may be adjsuted along the way and it wil be recopied to "final" folder at the end.
    shutil.copy2(model_huc_catalog_path, output_folder_path)
    # now change its path to be its new home in the output unit folder.

    model_file_name = os.path.basename(model_huc_catalog_path)
    model_huc_catalog_path = os.path.join(output_folder_path, model_file_name)

    # -------------------
    # setup the logging class (default unit folder path (HUC/CRS))
    # Log file must pre-exist
    log_folder = os.path.join(output_folder_path, "logs")
    os.mkdir(log_folder)
    RLOG.setup(os.path.join(log_folder, "ras2fim.log"))

    """
    print("............... Temp sample RLOG types")
    RLOG.trace("Sample trace (log file only)")
    print("Trace logs only to file so you won't see it on screen, this is a print line")

    RLOG.lprint("Sample lprint (console and log file)")
    RLOG.debug("Sample debug (console and log file)")
    RLOG.notice("Sample notice (console and log file but differnt color to stand out more))
    RLOG.success("Sample success (console and log file)")
    RLOG.warning("Sample warning (console, log file and warning file)")
    RLOG.error("Sample error (console, log file, and error file)")
    RLOG.critical("Sample critical (console log file, and error file)")
    print("...............")
    """

    # -------------------------------------------
    # ---- Make the "final" folder now as some modules will write to it through the steps
    #      why test if it exists with makedir for output above? We are going to be retarting
    #      the step system so it won't auto make the huc output dir
    r2f_final_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_FINAL)
    if not os.path.exists(r2f_final_dir):
        os.mkdir(r2f_final_dir)

    # -------------------
    # Save incoming args and a few new derived variables created to this point into a log file
    # Careful... when **locals() is called, it will include ALL variables in this function to this point.
    create_input_args_log(**locals())

    # -------------------
    # copy the config env file to the final directory
    shutil.copy2(config_file, r2f_final_dir)

    # -------------------
    # Now call the processing function
    fn_run_ras2fim(
        str_huc8_arg,
        input_models_path,
        output_folder_path,
        str_crs_arg,
        str_nation_arg,
        str_hec_path,
        str_terrain_override,
        model_huc_catalog_path,
        int_step,
        output_resolution,
        model_unit,
    )


# -------------------------------------------------
# If you are calling this python file from an another python file, DO NOT call this function first.
# Call the init_and_run_ras2fim function as it validates inputs and sets up other key variables.
def fn_run_ras2fim(
    str_huc8_arg,
    input_models_path,
    output_folder_path,
    str_crs_arg,
    str_nation_arg,
    str_hec_path,
    str_terrain_override,
    model_huc_catalog_path,
    int_step,
    output_resolution,
    model_unit,
):
    start_dt = dt.datetime.utcnow()

    RLOG.lprint("")
    RLOG.lprint("+=================================================================+")
    RLOG.notice("          RUN RAS2FIM FOR A HEC-RAS 1-D DATASET (HUC8)")
    RLOG.lprint("     Created by Andy Carter, PE of the National Water Center")
    RLOG.lprint("+-----------------------------------------------------------------+")

    RLOG.lprint(f"  ---(r) HUC 8 WATERSHED: {str_huc8_arg}")
    RLOG.lprint(f"  ---(i) PATH TO HEC-RAS: {input_models_path}")
    RLOG.lprint(f"  ---(o) OUTPUT DIRECTORY: {output_folder_path}")
    RLOG.lprint(f"  ---(p) PROJECTION OF HEC-RAS MODELS: {str_crs_arg}")
    RLOG.lprint(f"  ---(n) PATH TO NATIONAL DATASETS: {str_nation_arg}")
    RLOG.lprint(f"  ---(r) PATH TO HEC-RAS v6.3: {str_hec_path}")
    RLOG.lprint(f"  ---[t] Terrain to Utilize {str_terrain_override}")
    RLOG.lprint("  ---[s] Step to start at - " + str(int_step))
    RLOG.lprint(
        "  --- The Ras Models unit (extracted from RAS model prj file and given EPSG code): " + model_unit
    )
    RLOG.lprint(f"  --- ras2fim started: {sf.get_stnd_date()}")

    # -------------------------------------------
    # ---- Make the "final" folder now as some modules will write to it through the steps
    #      why test if it exists with makedir for output above? We are going to be retarting
    #      the step system so it won't auto make the huc output dir
    r2f_final_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_FINAL)

    # ---- Step 1: create_shapes_from_hecras ----
    # create a folder for the shapefiles from hec-ras
    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: STEP 1 (create_shapes_from_hecras) +++++++")

    str_shapes_from_hecras_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS)
    if not os.path.exists(str_shapes_from_hecras_dir):
        os.mkdir(str_shapes_from_hecras_dir)

    # run the first script (create_shapes_from_hecras)
    if int_step <= 1:
        fn_create_shapes_from_hecras(input_models_path, str_shapes_from_hecras_dir, str_crs_arg)
    # -------------------------------------------

    # ------ Step 2: conflate_hecras_to_nwm -----
    # do whatever is needed to create folders and determine variables
    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: STEP 2 (conflate_hecras_to_nwm) +++++++")

    str_shapes_from_conflation_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF)
    if not os.path.exists(str_shapes_from_conflation_dir):
        os.mkdir(str_shapes_from_conflation_dir)

    # run the second script (conflate_hecras_to_nwm)
    if int_step <= 2:
        fn_conflate_hecras_to_nwm(
            str_huc8_arg,
            str_shapes_from_hecras_dir,
            str_shapes_from_conflation_dir,
            str_nation_arg,
            output_folder_path,
        )
    # -------------------------------------------

    # ------ Step 3: get_usgs_dem_from_shape or clip_dem_from_shape ----
    str_area_shp_name = str_huc8_arg + "_huc_12_ar.shp"
    str_input_path = os.path.join(str_shapes_from_conflation_dir, str_area_shp_name)
    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: STEP 3 get / cut DEM +++++++")
    RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

    # create output folder
    str_terrain_from_usgs_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_TERRAIN)
    if not os.path.exists(str_terrain_from_usgs_dir):
        os.mkdir(str_terrain_from_usgs_dir)

    # field name is from the National watershed boundary dataset (WBD)
    str_field_name = "HUC_12"

    # *** variables set - raster terrain harvesting ***
    # ==============================================
    int_res = 3  # resolution of the downloaded terrain (meters)
    int_buffer = 300  # buffer distance for each watershed shp
    int_tile = 1500  # tile size requested from USGS WCS
    # ==============================================

    # run the third script
    if int_step <= 3:
        # provide conflation qc file to mark the parent models that conflated to NWM reaches
        conflation_qc_path = os.path.join(str_shapes_from_conflation_dir, "conflated_ras_models.csv")

        if str_terrain_override == "None Specified - using USGS WCS":
            # create terrain from the USGS WCS
            fn_get_usgs_dem_from_shape(
                str_input_path,
                str_terrain_from_usgs_dir,
                int_res,
                int_buffer,
                int_tile,
                model_unit,
                str_field_name,
            )
        else:
            # user has supplied the terrain file
            str_cross_sections_path = str_shapes_from_hecras_dir + "\\cross_section_LN_from_ras.shp"
            str_huc12_path = str_nation_arg + "\\" + sv.INPUT_WBD_NATIONAL_FILE

            fn_cut_dems_from_shapes(
                str_huc12_path,
                str_cross_sections_path,
                conflation_qc_path,
                str_terrain_override,
                str_terrain_from_usgs_dir,
                int_buffer,
                model_unit,
            )
    # -------------------------------------------

    # ------  Step 4: convert_tif_to_ras_hdf5 -----

    # folder of tifs created in third script (get_usgs_dem_from_shape)
    # str_terrain_from_usgs_dir

    # create a converted terrain folder
    str_hecras_terrain_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_HECRAS_TERRAIN)
    if not os.path.exists(str_hecras_terrain_dir):
        os.mkdir(str_hecras_terrain_dir)

    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: STEP 4 (convert tif to ras hdf5 +++++++")
    RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

    str_area_prj_name = str_huc8_arg + "_huc_12_ar.prj"
    str_projection_path = os.path.join(str_shapes_from_conflation_dir, str_area_prj_name)

    if int_step <= 4:
        fn_convert_tif_to_ras_hdf5(
            str_hec_path, str_terrain_from_usgs_dir, str_hecras_terrain_dir, str_projection_path, model_unit
        )

    # -------------------------------------------

    # ------ Step 5: create_fim_rasters -----

    # folder of tifs created in third script (get_usgs_dem_from_shape)
    # str_terrain_from_usgs_dir

    # create a converted terrain folder
    str_hecras_out_dir = os.path.join(output_folder_path, sv.R2F_OUTPUT_DIR_HECRAS_OUTPUT)
    if not os.path.exists(str_hecras_out_dir):
        os.mkdir(str_hecras_out_dir)

    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: STEP 5 (create fim rasters) +++++++")
    RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

    # *** variables set - raster terrain harvesting ***
    # ==============================================
    if model_unit == "feet":
        flt_interval = 0.5  # vertical step of average depth (0.5ft)
    else:
        flt_interval = 0.2  # vertical step of average depth (0.2m)
    # ==============================================

    if int_step <= 5:
        fn_create_fim_rasters(str_huc8_arg, output_folder_path, model_unit)

    # -------------------------------------------

    # --- Step 6: create_src_depthgrids_for_fids ---

    RLOG.lprint("")
    RLOG.notice("+++ Processing: STEP 6 (create src and fim rasters per fid) +++")
    RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

    if int_step <= 6:
        fn_create_src_feature_ids(str_huc8_arg, output_folder_path)

    # -------------------------------------------
    flt_resolution_depth_grid = int(output_resolution)

    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: 5.b (simplifying fim rasters and create metrics) +++++++")
    RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

    fn_simplify_fim_rasters(
        str_hecras_out_dir, flt_resolution_depth_grid, sv.DEFAULT_RASTER_OUTPUT_CRS, model_unit
    )

    # ----------------------------------------
    RLOG.lprint("")
    RLOG.notice("+++++++ Processing: STEP 5.c (calculate terrain statistics) +++++++")
    RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

    fn_calculate_all_terrain_stats(str_hecras_out_dir)

    # -------------------------------------------------
    if os.getenv("RUN_RAS2CALIBRATION") == "True":
        RLOG.lprint("")
        RLOG.notice("+++++++ Processing: STEP: Running ras2calibration +++++++")
        RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

        dir_reformat_ras_rc(
            output_folder_path,
            sv.R2F_OUTPUT_DIR_RAS2CALIBRATION,
            sv.R2F_OUTPUT_FILE_RAS2CAL_CSV,
            sv.R2F_OUTPUT_FILE_RAS2CAL_GPKG,
            sv.R2F_OUTPUT_FILE_RAS2CAL_LOG,
            "",
            "",
            False,
            sv.R2F_OUTPUT_DIR_SHAPES_FROM_CONF,
            sv.R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS,
            sv.R2F_OUTPUT_DIR_METRIC,
        )

        # Copy outputs into the ras2calibration subdirectory of the /final folder
        r2f_final_ras2cal_subdir = os.path.join(r2f_final_dir, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION)
        os.mkdir(r2f_final_ras2cal_subdir)

        shutil.copy2(
            os.path.join(
                output_folder_path, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION, sv.R2F_OUTPUT_FILE_RAS2CAL_CSV
            ),
            r2f_final_ras2cal_subdir,
        )
        shutil.copy2(
            os.path.join(
                output_folder_path, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION, sv.R2F_OUTPUT_FILE_RAS2CAL_GPKG
            ),
            r2f_final_ras2cal_subdir,
        )
        shutil.copy2(
            os.path.join(
                output_folder_path, sv.R2F_OUTPUT_DIR_RAS2CALIBRATION, "README_reformat_ras_rating_curve.txt"
            ),
            r2f_final_ras2cal_subdir,
        )

    # -------------------------------------------------
    if os.getenv("PRODUCE_GEOCURVES") == "True":
        RLOG.lprint("")
        RLOG.notice("+++++++ Processing: STEP: Producing Geocurves +++++++")

        RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

        # Produce geocurves
        job_number = os.cpu_count() - 2
        manage_geo_rating_curves_production(
            ras2fim_output_dir=output_folder_path,
            job_number=job_number,
            output_folder=r2f_final_dir,
            overwrite=False,
        )

    # -------------------------------------------------
    if os.getenv("CREATE_RAS_DOMAIN_POLYGONS") == "True":
        RLOG.lprint("")
        RLOG.notice("+++++++ Processing: STEP: Create polygons for HEC-RAS models domains +++++++")
        RLOG.lprint(f"Module Started: {sf.get_stnd_date()}")

        # get the path to the shapefile containing cross sections of the parent HEC-RAS models
        xsections_shp_file_path = os.path.join(str_shapes_from_hecras_dir, "cross_section_LN_from_ras.shp")

        # provide conflation qc file to mark the parent models that conflated to NWM reaches
        conflation_qc_path = os.path.join(str_shapes_from_conflation_dir, "%s_stream_qc.csv" % str_huc8_arg)

        # make output folder and build path to the output file
        # TODO: Nov 3, 2023: The creation of the output_polygon_dir and polygons_output_file_path
        # has to be done inside the fn_make_domain_polygons. Why? create_model_domain_polygons.py
        # fails when being run from command line as the folder doesn't exist
        # Also see note in __main__ of create_model_domain_polygons.py as a duplicate msg (more less)
        #  But even after just manually adding that folder it still fails when run from command line.
        output_polygon_dir = os.path.join(r2f_final_dir, sv.R2F_OUTPUT_DIR_DOMAIN_POLYGONS)
        polygons_output_file_path = os.path.join(output_polygon_dir, "models_domain.gpkg")
        os.mkdir(output_polygon_dir)

        fn_make_domain_polygons(
            xsections_shp_file_path,
            polygons_output_file_path,
            "ras_path",
            model_huc_catalog_path,
            conflation_qc_path,
        )

    # -------------------------------------------------
    RLOG.lprint("")
    RLOG.notice("+++++++ Finalizing processing +++++++")

    # Copy it here in case it gets updated along the way
    shutil.copy2(model_huc_catalog_path, r2f_final_dir)
    run_arguments_filepath = os.path.join(output_folder_path, "run_arguments.txt")
    shutil.copy2(run_arguments_filepath, r2f_final_dir)

    RLOG.lprint("+=================================================================+")
    RLOG.success("  RUN RAS2FIM - Completed                                         |")
    dur_msg = sf.print_date_time_duration(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    RLOG.lprint("+-----------------------------------------------------------------+")


# -------------------------------------------------
def create_input_args_log(**kwargs):
    """
    Overview:
        This method takes all incoming arguments, cycles through them and put them in a file
    Inputs:
        **kwargs is any dictionary of key / value pairs
    """

    r2f_huc_output_dir = kwargs.get("output_folder_path")
    arg_log_file = os.path.join(r2f_huc_output_dir, ARG_LOG_FILE_NAME)

    # Remove it if is aleady exists (relavent if we add an override system)
    if os.path.exists(arg_log_file):
        os.remove(arg_log_file)

    # start with the processing date in UTC (all date/times in UTC)
    utc_now = dt.datetime.utcnow()
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
if __name__ == "__main__":
    # Sample usage:
    # Using all defaults:
    #     python ras2fim.py -w 12090301 -p EPSG:2277
    #           -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif

    # There are a number of ways to use arguments:
    #
    # 1) You can use the defaults, which will use the folder structure as seen in the docs/INSTALL.md.
    #     It will require you to create folders in a specific pattern and specific names.

    # 2) You can also use the current code in legacy fashion with all of it's previous arguments.
    #    ie) python ras2fim.py -w 10170204 -i C:\HEC\input_folder -o C:\HEC\output_folder -p EPSG:26915
    #            -n E:\X-NWS\X-National_Datasets -r "C:\Program Files (x86)\HEC\HEC-RAS\6.3"
    #
    #    But any and all optional arguments can be overridden, so let's try this version:
    #    ie) python ras2fim.py -w 12090301 -i C:\HEC\input_folder
    #                          -o c:/users/my_user/desktop/ras2fim_outputs -p EPSG:2277
    #                          -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif
    #                          -n E:\X-NWS\X-National_Datasets
    #
    #        - When the -n arg not being set, it defaults to c:/ras2fim_data/inputs/X-National_datasets.
    #
    #         - The -i arg is optional and defaults to c:/ras2fim_data/OWP_ras_models/models. Each file
    #              or subfolder in this directory will be used as input into the ras2fim.py code.
    #              Again, you an override this to any location you like.
    #
    # When ras2fim.py is run, it will automatically create an output folder name with the output files
    #     and some subfolders. The folder name will be based on the pattern of
    #     {HUC number}_{CRS}_{DATE (YYMMDD)}. e.g.  12090301_2277_230725

    # ++++ Config file notes ++++++

    # By default, many of the section processing can be turned on/off by using a config file.
    # The default file is located at /config/r2f_config.env and it will be loaded. If you look at the file
    # it will look for some critical env variables to tell the system a number of things.

    # By using the -c argument, you can override to use your own config file in any location on
    # your computer, and make sure you fully path it in the argument.
    # Also ensure the file is a valid python environment file (see the current /config/) and has
    # all of the most current env variables.
    # This app will fail if it is missing a variable from the config file.

    parser = argparse.ArgumentParser(
        description="========== RUN RAS2FIM FOR A HEC-RAS 1-D DATASET (HUC8) =========="
    )

    parser.add_argument(
        "-w",
        dest="str_huc8_arg",
        help="REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204",
        required=True,
        metavar="",
        type=str,
    )  # has to be string so it doesn't strip the leading zero

    parser.add_argument(
        "-p",
        dest="str_crs_arg",
        help="REQUIRED: projection of HEC-RAS models: Example EPSG:2277",
        required=True,
        metavar="",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="r2f_output_dir",
        help="OPTIONAL: An ras2fim output folder will be created and automatically named. "
        "It will default to " + sv.R2F_DEFAULT_OUTPUT_MODELS + ", however by using this arg, "
        "you can override that path.",
        required=False,
        metavar="",
        default=sv.R2F_DEFAULT_OUTPUT_MODELS,
        type=str,
    )

    parser.add_argument(
        "-r",
        dest="str_hec_path",
        help=r"OPTIONAL: path to HEC-RAS 6.3: Defaults to C:\Program Files (x86)\HEC\HEC-RAS\6.3"
        r' but you can override it, Example: "C:\Program Files (x86)\HEC\HEC-RAS\6.4" (wrap in quotes)',
        required=False,
        metavar="",
        default=sv.DEFAULT_HECRAS_ENGINE_PATH,
        type=str,
    )

    parser.add_argument(
        "-i",
        dest="input_models_path",
        help=r"OPTIONAL: path containing the HEC_RAS files: Example -i C:\HEC\input_folder\my_models."
        r" Defaults to c:\ras2fim_datas\OWP_ras_models\models.",
        default=sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
        required=False,
        metavar="",
        type=str,
    )

    parser.add_argument(
        "-n",
        dest="str_nation_arg",
        help=r"OPTIONAL: path to national datasets: Example: E:\X-NWS\X-National_Datasets."
        r" Defaults to c:\ras2fim_data\inputs\X-National_Datasets.",
        default=sv.INPUT_DEFAULT_X_NATIONAL_DS_DIR,
        required=False,
        metavar="",
        type=str,
    )

    parser.add_argument(
        "-t",
        dest="str_terrain_override",
        help="OPTIONAL: full path to DEM Tif to use for mapping:"
        r"Example: c:\ras2fim_data\inputs\some_dem.tif."
        "Defaults to calling USGS website, but note.. it can be unstable and throw 404 and 500 errors.",
        required=False,
        metavar="",
        default="None Specified - using USGS WCS",
        type=str,
    )

    parser.add_argument(
        "-s",
        dest="str_step_override",
        help="OPTIONAL: step of processing to start on. Note: This feature is temporarily not working.",
        required=False,
        metavar="",
        default="None Specified - starting at the beginning",
        type=str,
    )

    parser.add_argument(
        "-mc",
        dest="model_huc_catalog_path",
        help="OPTIONAL: path to model catalog csv, filtered for the supplied HUC, file downloaded from S3."
        r" Defaults to c:\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog_[].csv"
        " and will use subsitution to replace the [] with the huc number.",
        default=sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
        required=False,
        metavar="",
        type=str,
    )

    parser.add_argument(
        "-res",
        dest="output_resolution",
        help="OPTIONAL: Spatial resolution of flood depth rasters (Simplified Rasters). Defaults to 10.",
        required=False,
        metavar="",
        default=10,
        type=int,
    )

    parser.add_argument(
        "-c",
        dest="config_file",
        help="OPTIONAL: If you want to override the default config/r2f_config.env, enter"
        " a full path and name of your config path. Make sure it is a proper python"
        " formatted env file. Defaults to config/r2f_config.env",
        required=False,
        metavar="",
        default=sv.DEFAULT_CONFIG_FILE_PATH,
        type=str,
    )

    args = vars(parser.parse_args())

    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # Note: don't do RLOG setup as the output path is calculated.
        # It is covered later in the steps.
        init_and_run_ras2fim(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
