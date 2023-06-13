
import os

'''
# This is a collection of variables that help manage, centralize and standarize some values, such as pathing, or common valuse
# Common pathing, and mostly defaults. Most can be changed or overwritten at this point.
'''

# BASIC ROOT FOLDERS
DEFAULT_BASE_DIR = r"c:\ras2fim_data"
DEFAULT_HECRAS_ENGINE_PATH = r'C:\Program Files (x86)\HEC\HEC-RAS\6.0'

# INPUT FOLDERS
ROOT_DIR_INPUTS = os.path.join(DEFAULT_BASE_DIR, "inputs")
INPUT_DEFAULT_X_NATIONAL_DS_DIR = os.path.join(ROOT_DIR_INPUTS, "X-National_Datasets")
INPUT_NWM_FLOWS_FILE = "nwm_flows.gpkg"
INPUT_NWM_WBD_LOOKUP_FILE = "nwm_wbd_lookup.nc"
INPUT_WBD_NATIONAL_FILE = "WBD_National.gpkg"
INPUT_DEFAULT_NWM_FLOWS_FILE_PATH = os.path.join(INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_NWM_FLOWS_FILE)
INPUT_DEFAULT_NWM_WBD_LOOKUP_FILE_PATH = os.path.join(INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_NWM_FLOWS_FILE)
INPUT_DEFAULT_INPUT_WBD_NATIONAL_FILE_PATH = os.path.join(INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_WBD_NATIONAL_FILE)

# Yes... while a person can (for now) override the path for X-Nation... they can not for two new src datasets
# TODO: full functionaly of these two paths are coming soon.
INPUT_NWM_CATCHMENTS_FILE = os.path.join(ROOT_DIR_INPUTS, "nwm_hydrofabric", "nwm_catchments.gpkg")
INPUT_WBD_HUC8_DIR = os.path.join(ROOT_DIR_INPUTS, "wbd","HUC8_All" )

# OWP RAS MODELS (pre-processed, ready to submit to ras2fim.py)
HECRAS_INPUT_DEFAULT_OWP_RAS_MODELS = os.path.join(DEFAULT_BASE_DIR, "OWP_ras_models", "models")

# RAS2FIM OUTPUT FOLDERS 
R2F_DEFAULT_OUTPUT_MODELS = os.path.join(DEFAULT_BASE_DIR, "output_ras2fim_models")
R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS  = "01_shapes_from_hecras"
R2F_OUTPUT_DIR_SHAPES_FROM_CONF  = "02_shapes_from_conflation"
R2F_OUTPUT_DIR_TERRAIN  = "03_terrain"
R2F_OUTPUT_DIR_HECRAS_TERRAIN  = "04_hecras_terrain"
R2F_OUTPUT_DIR_HECRAS_OUTPUT  = "05_hecras_output"
R2F_OUTPUT_DIR_RAS2REM  = "06_ras2rem"
R2F_OUTPUT_DIR_RELEASES = os.path.join(DEFAULT_BASE_DIR, "ras2fim_releases")

# NODATA VALUE
DEFAULT_NODATA_VAL = (0 - 9999)

# VERTICAL DATUM
OUTPUT_VERTICAL_DATUM = "NAVD88"