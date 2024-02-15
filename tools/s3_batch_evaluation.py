import argparse
import datetime as dt
import os
import sys
import traceback
from glob import glob


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf
from evaluate_ras2fim_unit import evaluate_unit_results

import shared_variables as sv
import shared_functions as sf


RLOG = sv.R2F_LOG
BUCKET_PROD = 'ras2fim'
BUCKET_DEV = "ras2fim-dev"
BUCKET = ""
RESOLUTION = 10
NWS_BENCHMARK_PREFIX = "gval/benchmark_data/nws/{0}/"  # format args: huc

# format args: bucket, unit name, benchmark source, stage
INUNDATION_URL = 's3://{0}/output_ras2fim/{1}/final/inundation_polys/{2}_{3}_inundation.gpkg'

# format args: bucket, unit name
MODEL_DOMAIN_URL = 's3://{0}/output_ras2fim/{1}/final/models_domain/models_domain.gpkg'

VALID_BENCHMARK_STAGES = {"ble": ["100yr", "500yr"], "nws": ["minor", "moderate", "major", "action"]}

# format args: bucket, benchmark source, huc, stage
BENCHMARK_URIS = {
    "ble": 's3://{0}/gval/benchmark_data/{1}/{2}/{3}/{1}_huc_{2}_extent_{3}.tif',
    # format args: benchmark source, huc, nws station, stage
    "nws": 's3://{0}/gval/benchmark_data/{1}/{2}/{3}/{4}/ahps_{3}_huc_{2}_extent_{4}.tif',
}


# -------------------------------------------------
def get_benchmark_uri(unit_name: str, benchmark_source: str, stage: str, nws_station: str) -> str:
    """
    Method to get the appropriate benchmark uri

    Parameters
    ----------
    unit_name: str
        The unit_name (folder name) from outputs_ras2fim
    benchmark_source: str
        What benchmark source to use in the URI
    stage: str
        What stage to use in the URI
    nws_station: str
        What National Weather Service station to use in the URI

    Returns
    -------
    str
        URI with the appropriate location of benchmark source

    """

    if benchmark_source == "ble":
        return BENCHMARK_URIS["ble"].format(BUCKET, benchmark_source, unit_name.split('_')[0], stage)

    elif benchmark_source == "nws":
        if nws_station is None:
            raise ValueError("nws_station cannot be none when nws is chosen as a benchmark source")

        return BENCHMARK_URIS["nws"].format(
            BUCKET, benchmark_source, unit_name.split('_')[0], nws_station, stage
        )

    else:
        raise ValueError("benchmark source is not available")


# -------------------------------------------------
def get_nws_stations(huc: str) -> list:
    """Get available NWS stations for a HUC

    Parameters
    ----------
    huc: str
        HUC code to retrieve available NWS stations

    Returns
    -------
    list
        NWS stations available for HUC

    """
    return [
        s_unit['key'] for s_unit in s3_sf.get_folder_list(BUCKET, NWS_BENCHMARK_PREFIX.format(huc), False)
    ]


# -------------------------------------------------
def check_necessary_files_exist(unit: str, benchmark_source: str, stage: str, nws_station: str) -> dict:
    """Checks whether the necessary inputs for evaluations exist in S3

    Parameters
    ----------
    unit: str
        The name of the unit folder created and saved to ras2fim output
    benchmark_source: str
        What benchmark source to use in the URI
    stage: str
        What stage to use in the URI
    nws_station: str
        What National Weather Service station to use in the URI

    Returns
    -------
    dict
        Dictionary with all existing files, or an empty dictionary if any of the three do not exist

    """

    files = {
        'inundation_polygons': INUNDATION_URL.format(BUCKET, unit, benchmark_source, stage),
        'model_domain_polygons': MODEL_DOMAIN_URL.format(BUCKET, unit),
        'benchmark_raster': get_benchmark_uri(unit, benchmark_source, stage, nws_station),
    }

    exists = [
        s3_sf.is_valid_s3_file(files['inundation_polygons']),
        s3_sf.is_valid_s3_file(files['model_domain_polygons']),
        s3_sf.is_valid_s3_file(files['benchmark_raster']),
    ]

    if sum(exists) == 3:
        return files
    else:
        return {}


# -------------------------------------------------
def add_input_arguments(
    eval_args: list, unit_name: str, benchmark_source: str, stage: str, nws_station: str, output_dir: str
) -> list:
    """Add input args if the files exists for use in evaluation function

    Parameters
    ----------
    eval_args: list
        Array of dictionaries representing input arguments for evaluations
    unit_name: str
        The name of ras2fim unit created and stored in ras2fim output
    benchmark_source: str
        What benchmark source to use in the URI
    stage: str
        What stage to use in the URI
    nws_station: str
        What National Weather Service station to use in the URI
    output_dir: str
        Directory to save output evaluation files

    Returns
    -------
    list
        Array of dictionaries representing input arguments for evaluations

    """

    input_files = check_necessary_files_exist(unit_name, benchmark_source, stage, nws_station)
    if input_files:
        input_files['output_dir'] = output_dir
        input_files['unit_name'] = f"{unit_name}_{stage}"
        eval_args.append(input_files)
    else:
        RLOG.trace(
            f"ras2fim Unit Name {unit_name}, benchmark_source {benchmark_source}, "
            f"stage {stage} nws_station {nws_station} inputs do not exist"
        )

    return eval_args


# -------------------------------------------------
def report_missing_ouput(
    units: list = None,
    benchmark_sources: list = None,
    stages: list = None,
    output_dir: str = 'c:\\ras2fim_data\\test_batch_eval',
):
    """
    Method to report missing output that was provided

    Parameters
    ----------
    units: list, default=None
        An array of the names of the units in the ras2fim output folder, (runs all if None)
    benchmark_sources: list, default=None
        Array of strings representing all benchmark sources to run, (runs all if None)
    stages: list, default=None
        Array of strings representing all stages to run, (runs all if None)
    output_dir: str
         Directory to save output evaluation files

    """

    report_missing = {"unit": [], "benchmark_sources": [], "stages": []}

    # Remove forward slash if exists as last character in output_dir
    if output_dir[-1] == '/':
        output_dir = output_dir[:-1]

    def __glob_check(search, search_type, report_missing):
        """Check if search term exists in output_dir or not"""
        if glob(f"{output_dir}/*{search}*") == []:
            report_missing[search_type].append(search)
        return report_missing

    if units is not None:
        for sp in units:
            report_missing = __glob_check(sp, "unit", report_missing)

    if benchmark_sources is not None:
        for be in benchmark_sources:
            report_missing = __glob_check(be, "benchmark_sources", report_missing)

    if stages is not None:
        for st in stages:
            report_missing = __glob_check(st, "stages", report_missing)

    # If any missing outputs exist report
    if (
        sum(
            [
                len(report_missing['unit']),
                len(report_missing['benchmark_sources']),
                len(report_missing['stages']),
            ]
        )
        > 0
    ):
        print()
        RLOG.warning(
            "The following provided args have no or incomplete inputs existing on s3: " f"\n {report_missing}"
        )
        print()        


# -------------------------------------------------
def run_batch_evaluations(
    environment: str,
    unit_names: list = None,
    benchmark_sources: list = None,
    stages: list = None,
    output_dir: str = 'c:\\ras2fim_data\\test_batch_eval',
):
    """
    Run batch evaluations on s3 objects for every valid combination of desired sources

    Parameters
    ----------
    environment: PROD or DEV
    unit_names: list, default=None
        Array of strings representing all spatial processing units to run, (runs all if None)
        ie) 12090301_2277_ble_240206
    benchmark_sources: list, default=None
        Array of strings representing all benchmark sources to run, (runs all if None)
    stages: list, default=None
        Array of strings representing all stages to run, (runs all if None)
    output_dir: str
         Directory to save output evaluation files

    """

    start_dt = dt.datetime.utcnow()

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.notice("      Begin s3 batch evaluation")
    RLOG.lprint(f"  (-u):  Source unit folder name(s): {unit_names} ")
    RLOG.lprint(f"  (-e):  Environment type: {environment}")
    if benchmark_sources is None or len(benchmark_sources) == 0:
        RLOG.lprint(f"  (-b):  Source benchmarks to run: All")
    else:
        RLOG.lprint(f"  (-b):  Source benchmarks: {benchmark_sources}")

    if stages is None or len(stages) == 0:
        RLOG.lprint(f"  (-st): Stages to run: All")
    else:
        RLOG.lprint(f"  (-st): Stages to run: {stages}")

    RLOG.lprint(f"  (-o):  Root output directory: {output_dir}")
    RLOG.lprint(f" Started (UTC): {sf.get_stnd_date()}")
    print()
    print("NOTE: All output inundation files will be overwritten")

    print()    
    eval_args = []

    if environment != "PROD" and environment != "DEV":
        raise ValueError(
            "The -e (environment) value must be either the word 'PROD' or 'DEV'."
            " The value is case-sensitive"
        )
    global BUCKET
    if environment == "PROD":
        BUCKET = BUCKET_PROD
    else:
        BUCKET = BUCKET_DEV

    for s_unit in s3_sf.get_folder_list(BUCKET, sv.S3_RAS_UNITS_OUTPUT_FOLDER, False):
        s3_unit_name = s_unit.get('key')

        rd = s3_sf.parse_unit_folder_name(s3_unit_name)
        # TODO: Upgrade this pathing.
        # figure out the output pathing.
        output_dir = os.path.join(
            sv.LOCAL_GVAL_ROOT, sv.LOCAL_GVAL_EVALS, environment, rd["key_unit_id"], rd["key_date_as_str"]
        )
        # e.g: C:\ras2fim_data\gval\evaluations\DEV\12090301_2277_ble\230923

        # Check if directory is in desired ras2fim output units list if provided
        if unit_names is None or s3_unit_name in unit_names:
            for key, val in VALID_BENCHMARK_STAGES.items():
                # Check if benchmark source is in desired benchmark_sources list if provided
                if benchmark_sources is None or key in benchmark_sources:
                    for stage in val:
                        # Check if stage is in desired stages list if provided
                        if stages is None or stage in stages:
                            if key == "nws":
                                # Add arguments for each valid nws_station
                                for nws_station in get_nws_stations(s3_unit_name.split('_')[0]):
                                    eval_args = add_input_arguments(
                                        eval_args, s3_unit_name, key, stage, nws_station, output_dir
                                    )

                            else:
                                eval_args = add_input_arguments(
                                    eval_args, s3_unit_name, key, stage, None, output_dir
                                )
                            print()

    # Run ras2fim model evaluation
    for kwargs in eval_args:
        # RLOG.lprint(f"Processing evaluation for ras2fim output unit {kwargs['unit_name']}")
        evaluate_unit_results(**kwargs)

    if not eval_args:
        RLOG.warning("No valid combinations found, check inputs and try again.")
    else:
        report_missing_ouput(unit_names, benchmark_sources, stages, output_dir)

    print()
    print("===================================================================")
    RLOG.success("Batch Processing complete")
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.success(f"Ended (UTC): {dt_string}")
    RLOG.success(f"log files saved to {RLOG.LOG_FILE_PATH}")
    dur_msg = sf.get_date_time_duration_msg(start_dt, dt.datetime.utcnow())
    RLOG.lprint(dur_msg)
    print()

# -------------------------------------------------
if __name__ == '__main__':
    # ***********************
    # This tool is intended for NOAA/OWP staff only as it requires access to an AWS S3 bucket with a
    # specific folder structure.
    # It has hardcoded output pathing.
    # ***********************

    """
    Example Usage:

    python s3_batch_evaluation.py
    -u "12030105_2276_ble_230923" "12040101_102739_ble_230922"
    -b "ble" "nws"
    -st "100yr" "500yr" "moderate"
    -o "c:\ras2fim_data\test_batch_eval"
    -e PROD or DEV
    """

    # TODO: While it can get files from S3, it can only save outputs locally  and can not push
    # them back to S3. Deliberately at this time, allows for review before going back to S3.
    # Maybe add feature later to push back to s3.

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Produce Inundation from RAS2FIM geocurves.\n"
        " You must use known benchmark sources and stages. e.g ble:100yr, nws:moderate"
    )

    parser.add_argument(
        "-o", "--output_dir", help='REQUIRED: Directory to save output evaluation files', required=True
    )

    parser.add_argument(
        "-e",
        "--environment",
        help="REQUIRED: Must be either the value of PROD meaning it is going to"
        " s3://ras2fim  or  DEV meaning pointing to s3://ras2fim-dev",
        required=True,
    )

    # TODO. Test pattern of cmd args and change e.g. here.
    parser.add_argument(
        "-u",
        "--unit_names",
        nargs='*',
        help="OPTIONAL: Argument/s representing unit names that refer to ras2fim output runs"
        " (if not provided run all existing).\n"
        " Note: The unit_names need to exist in either s3://ras2fim or s3://ras2fim-dev"
        " depending on selected (-e) environment.\n"
        " e.g. 12030105_2276_ble_230923",
        required=False,
    )

    # TODO. Test pattern of cmd args and change ie here, also needs an e.g.
    parser.add_argument(
        "-b",
        "--benchmark_sources",
        nargs='*',
        help="OPTIONAL: Argument/s representing list of benchmark sources to run"
        " (if not provided run all existing)\n"
        " Note: The unit_names need to exist in either s3://ras2fim or s3://ras2fim-dev"
        " depending on selected (-e) environment.",
        required=False,
    )

    # TODO. Test pattern of cmd args and change ie here, also needs an e.g.
    parser.add_argument(
        "-st",
        "--stages",
        nargs='*',
        help="OPTIONAL: Argument/s representing list of stages to run (if not provided run all existing)",
        required=False,
    )

    args = vars(parser.parse_args())

    log_file_folder = os.path.join(args["output_dir"], "logs")
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # Creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        log_file_name = f"{script_file_name}_{sf.get_date_with_milli(False)}.log"
        RLOG.setup(os.path.join(log_file_folder, log_file_name))

        run_batch_evaluations(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
