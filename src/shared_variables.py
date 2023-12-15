import os

import ras2fim_logger as rl


"""
This is a collection of variables that help manage, centralize and standarize some values,
   such as pathing, or common valuse
Common pathing, and mostly defaults. Most can be changed or overwritten at this point.
"""

# global RLOG
R2F_LOG = rl.RAS2FIM_logger()


# BASIC ROOT FOLDERS
DEFAULT_BASE_DIR = r"c:\ras2fim_data"
DEFAULT_HECRAS_ENGINE_PATH = r"C:\Program Files (x86)\HEC\HEC-RAS\6.3"

# INPUT FOLDERS
ROOT_DIR_INPUTS = os.path.join(DEFAULT_BASE_DIR, "inputs")
INPUT_DEFAULT_X_NATIONAL_DS_DIR = os.path.join(ROOT_DIR_INPUTS, "X-National_Datasets")
INPUT_NWM_FLOWS_FILE = "nwm_flows.gpkg"
INPUT_NWM_WBD_LOOKUP_FILE = "nwm_wbd_lookup.nc"
INPUT_WBD_NATIONAL_FILE = "WBD_National.gpkg"
INPUT_NWM_CATCHMENTS_FILE = "nwm_catchments.gpkg"
INPUT_WBD_HUC8_DIR = "WBD_HUC8"  # Pattern for huc files are 'HUC8_{huc number}.gpkg'
INPUT_DEFAULT_NWM_FLOWS_FILE_PATH = os.path.join(INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_NWM_FLOWS_FILE)
INPUT_DEFAULT_NWM_WBD_LOOKUP_FILE_PATH = os.path.join(INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_NWM_FLOWS_FILE)
INPUT_DEFAULT_INPUT_WBD_NATIONAL_FILE_PATH = os.path.join(
    INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_WBD_NATIONAL_FILE
)
INPUT_DEFAULT_NWM_FLOWS_FILE_PATH = os.path.join(INPUT_DEFAULT_X_NATIONAL_DS_DIR, INPUT_NWM_CATCHMENTS_FILE)

# OWP RAS MODELS (pre-processed, ready to submit to ras2fim.py)
DEFAULT_OWP_RAS_MODELS_MODEL_PATH = os.path.join(DEFAULT_BASE_DIR, "OWP_ras_models", "models")
DEFAULT_RSF_MODELS_CATALOG_FILE = os.path.join(
    DEFAULT_BASE_DIR, "OWP_ras_models", "OWP_ras_models_catalog_[].csv"
)

# RAS2FIM OUTPUT FOLDERS
R2F_DEFAULT_OUTPUT_MODELS = os.path.join(DEFAULT_BASE_DIR, "output_ras2fim")
R2F_OUTPUT_DIR_SHAPES_FROM_HECRAS = "01_shapes_from_hecras"
R2F_OUTPUT_DIR_SHAPES_FROM_CONF = "02_shapes_from_conflation"
R2F_OUTPUT_DIR_TERRAIN = "03_terrain"
R2F_OUTPUT_DIR_HECRAS_TERRAIN = "04_hecras_terrain"
R2F_OUTPUT_DIR_HECRAS_OUTPUT = "05_hecras_output"
R2F_OUTPUT_DIR_METRIC = "06_metric"
R2F_OUTPUT_DIR_SIMPLIFIED_GRIDS = "Depth_Grid"
R2F_OUTPUT_DIR_METRIC_RATING_CURVES = "Rating_Curve"
R2F_OUTPUT_DIR_METRIC_CROSS_SECTIONS = "Cross_Sections"
R2F_OUTPUT_DIR_RAS2REM = "ras2rem"
R2F_OUTPUT_DIR_CATCHMENTS = "ras2catchments"
R2F_OUTPUT_DIR_RAS2CALIBRATION = "ras2calibration"
R2F_OUTPUT_DIR_FINAL = "final"
R2F_OUTPUT_DIR_DOMAIN_POLYGONS = "models_domain"

R2F_OUTPUT_DIR_RELEASES = os.path.join(DEFAULT_BASE_DIR, "ras2fim_releases")

# OUTPUT FILES
R2F_OUTPUT_FILE_RAS2CAL_CSV = "ras2calibration_rating_curve_table.csv"
R2F_OUTPUT_FILE_RAS2CAL_GPKG = "ras2calibration_rating_curve_points.gpkg"
R2F_OUTPUT_FILE_RAS2CAL_LOG = "ras2calibration_log.txt"

# S3 PATHS
S3_OUTPUT_RAS2FIM_FOLDER = "output_ras2fim"
S3_DEFAULT_BUCKET_PATH = "s3://ras2fim"
S3_OUTPUT_MODELS_FOLDER = S3_DEFAULT_BUCKET_PATH + "/OWP_ras_models/models"
S3_DEFAULT_MODELS_CATALOG_PATH = S3_DEFAULT_BUCKET_PATH + "/OWP_ras_models/OWP_ras_models_catalog.csv"
S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER = "output_ras2fim_archive"
S3_OUTPUT_TRACKER_FILE = "ras_output_tracker.csv"

# TOOLS varibles
LOCAL_TOOLS_OUTPUT_PATH = os.path.join(DEFAULT_BASE_DIR, "tool_outputs")

# NODATA VALUE
DEFAULT_NODATA_VAL = 0 - 9999

# VERTICAL DATUM
OUTPUT_VERTICAL_DATUM = "NAVD88"

# DEFAULT MISC VALUES
DEFAULT_RASTER_OUTPUT_CRS = "EPSG:5070"
DEFAULT_CONFIG_FILE_PATH = "config\\r2f_config.env"
DEFAULT_LOG_FOLDER = "Logs"
COL_NAME_DOWNLOAD_SUCCESS = "download_success"
COL_NAME_ERROR_DETAILS = "error_details"
COL_NAME_FINAL_NAME_KEY = "final_name_key"
COL_NAME_MODEL_ID = "model_id"
