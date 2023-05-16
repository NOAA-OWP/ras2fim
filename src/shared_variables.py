
import os

'''
# This is a collection of variables that help manage, centralize and standarize some values, such as pathing, or common valuse
# Common pathing, and mostly defaults. Most can be changed or overwritten at this point.
'''

# BASIC ROOT FOLDERS
DEFAULT_BASE_DIR = r"c:\ras2fim_data"
DEFAULT_HECRAS_ENGINE_PATH = r'C:\Program Files (x86)\HEC\HEC-RAS\6.0'

ROOT_DIR_INPUTS = "inputs"
ROOT_DIR_OWP_RAS_MODELS = "OWP_ras_models"
ROOT_DIR_R2F_OUTPUT_MODELS = "output_ras2fim_models"
ROOT_DIR_RAS2REM_RELEASES = "ras2rem_releases"
ROOT_DIR_SOURCE_HECRAS_DATA = "source_hecras_data"

# INPUT FOLDERS
INPUT_NWM_FLOWS_FILE = "nwm_flows.gpkg"
INPUT_NWM_WBD_LOOKUP_FILE = "nwm_wbd_lookup.nc"
INPUT_WBD_NATIONAL_FILE = "WBD_National.gpkg"
INPUT_DEFAULT_X_NATIONAL_DS_DIR ="X-National_Datasets"
# Yes... while a person can (for now) override the path for X-Nation... they can not for two new src datasets
INPUT_NWM_CATCHMENTS_FILE = os.path.join(ROOT_DIR_INPUTS, "nwm_hydrofabric", "nwm_catchments.gpkg")
INPUT_WBD_HUC8_DIR = os.path.join(ROOT_DIR_INPUTS, "wbd","HUC8_All" )

# outputs_ras2fim_models
R2F_OUTPUT_MODELS_DIR = r'OWP_ras_models\models'
R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS  = "01_shapes_from_hecras"
R2F_OUTPUT_DIR_SHAPES_FROM_CONF  = "02_shapes_from_conflation"
R2F_OUTPUT_DIR_TERRAIN  = "03_terrain"
R2F_OUTPUT_DIR_HECRAS_TERRAIN  = "04_hecras_terrain"
R2F_OUTPUT_DIR_HECRAS_OUTPUT  = "05_hecras_output"
R2F_OUTPUT_DIR_RAS2REM  = "06_ras2rem"



