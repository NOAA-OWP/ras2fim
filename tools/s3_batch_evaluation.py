import os
import sys

from evaluate_ras2fim_model import evaluate_model_results
from s3_shared_functions import get_folder_list, is_valid_s3_file


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv


RLOG = sv.R2F_LOG
BUCKET = 'ras2fim'
BUCKET_DEV = "ras2fim-dev"
RESOLUTION = 10
PREFIX = 'output_ras2fim/'
NWS_BENCHMARK_PREFIX = "gval/benchmark_data/nws/{0}/"  # format args: huc

# format args: spatial processing unit, benchmark source, stage
INUNDATION_URL = 's3://ras2fim/output_ras2fim/{0}/final/inundation_polys/{1}_{2}_inundation.gpkg'

# format args: spatial processing unit
MODEL_DOMAIN_URL = 's3://ras2fim/output_ras2fim/{0}/final/models_domain/models_domain.gpkg'

VALID_BENCHMARK_STAGES = {"ble": ["100yr", "500yr"], "nws": ["minor", "moderate", "major", "action"]}

# format args: benchmark source, huc, stage
BENCHMARK_URIS = {
    "ble": 's3://ras2fim-dev/gval/benchmark_data/{0}/{1}/{2}/{0}_huc_{1}_extent_{2}.tif',
    # format args: benchmark source, huc, nws station, stage
    "nws": 's3://ras2fim-dev/gval/benchmark_data/{0}/{1}/{2}/{3}/ahps_{2}_huc_{1}_extent_{3}.tif'
}


def get_benchmark_uri(spatial_proc_unit: str, benchmark_source: str, stage: str, nws_station: str) -> str:
    """ Method to get the appropriate benchmark uri

    Parameters
    ----------
    spatial_proc_unit: str
        Spatial processing unit assigned to ras2fim output
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
        return BENCHMARK_URIS["ble"].format(
            benchmark_source, spatial_proc_unit.split('_')[0], stage
        )

    elif benchmark_source == "nws":

        if nws_station is None:
            raise ValueError("nws_station cannot be none when nws is chosen as a benchmark source")

        return BENCHMARK_URIS["nws"].format(
            benchmark_source, spatial_proc_unit.split('_')[0], nws_station, stage
        )

    else:
        raise ValueError("benchmark source is not available")


def get_nws_stations(huc: str) -> list:
    """ Get available NWS stations for a HUC

    Parameters
    ----------
    huc: str
        HUC code to retrieve available NWS stations

    Returns
    -------
    list
        NWS stations available for HUC

    """
    return [s_unit['key'] for s_unit in get_folder_list(BUCKET_DEV, NWS_BENCHMARK_PREFIX.format(huc), False)]


def check_necessary_files_exist(spatial_proc_unit: str,
                                benchmark_source: str,
                                stage: str,
                                nws_station: str) -> dict:
    """ Checks whether the necessary inputs for evaluations exist in S3

    Parameters
    ----------
    spatial_proc_unit: str
        Spatial processing unit assigned to ras2fim output
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

    files = {'inundation_polygons': INUNDATION_URL.format(spatial_proc_unit, benchmark_source, stage),
             'model_domain_polygons': MODEL_DOMAIN_URL.format(spatial_proc_unit),
             'benchmark_raster': get_benchmark_uri(spatial_proc_unit, benchmark_source, stage, nws_station)}

    exists = [is_valid_s3_file(files['inundation_polygons']),
              is_valid_s3_file(files['model_domain_polygons']),
              is_valid_s3_file(files['benchmark_raster'])]

    if sum(exists) == 3:
        return files
    else:
        return {}


def add_input_arguments(eval_args: list,
                        spatial_proc_unit: str,
                        benchmark_source: str,
                        stage: str,
                        nws_station: str,
                        output_dir: str) -> list:
    """ Add input args if the files exists for use in evaluation function

    Parameters
    ----------
    eval_args: list
        Array of dictionaries representing input arguments for evaluations
    spatial_proc_unit: str
        Spatial processing unit assigned to ras2fim output
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

    input_files = check_necessary_files_exist(spatial_proc_unit, benchmark_source, stage, nws_station)
    if input_files:
        input_files['output_dir'] = output_dir
        input_files['spatial_unit'] = f"{spatial_proc_unit}_{stage}"
        eval_args.append(input_files)

    return eval_args


def run_batch_evaluations(spatial_units: list = None,
                          benchmark_sources: list = None,
                          stages: list = None,
                          output_dir: str = './'):
    """ Run batch evaluations on s3 objects for every valid combination of desired sources

    Parameters
    ----------
    spatial_units: list, default=None
        Array of strings representing all spatial processing units to run, (runs all if None)
    benchmark_sources: list, default=None
        Array of strings representing all benchmark sources to run, (runs all if None)
    stages: list, default=None
        Array of strings representing all stages to run, (runs all if None)
    output_dir: str
         Directory to save output evaluation files

    """

    eval_args = []

    for s_unit in get_folder_list(BUCKET, PREFIX, False):
        spatial_proc_unit = s_unit.get('key')

        # Check if directory is in desired spatial_units list if provided
        if spatial_units is None or spatial_proc_unit in spatial_units:

            for key, val in VALID_BENCHMARK_STAGES.items():

                # Check if benchmark source is in desired benchmark_sources list if provided
                if benchmark_sources is None or key in benchmark_sources:

                    for stage in val:

                        # Check if stage is in desired stages list if provided
                        if stages is None or stage in stages:

                            if key == "nws":

                                # Add arguments for each valid nws_station
                                for nws_station in get_nws_stations(spatial_proc_unit.split('_')[0]):
                                    eval_args = add_input_arguments(
                                        eval_args, spatial_proc_unit, key, stage, nws_station, output_dir
                                    )

                            else:
                                eval_args = add_input_arguments(
                                    eval_args, spatial_proc_unit, key, stage, None, output_dir
                                )

    # Run ras2fim model evaluation
    for kwargs in eval_args:
        RLOG.lprint(f"Processing evaluation for spatial processing unit {kwargs['spatial_unit']}")
        evaluate_model_results(**kwargs)


if __name__ == '__main__':

    import argparse
    import os
    from datetime import datetime

    """
    Example Usage:

    python s3_batch_evaluation.py
    -su "12030105_2276_ble_230923,12040101_102739_ble_230922"
    -b "ble,nws"
    -st "100yr,500yr,moderate"
    -o "./test_batch_eval"
    """

    # Parse arguments
    parser = argparse.ArgumentParser(description="Produce Inundation from RAS2FIM geocurves.")
    parser.add_argument(
        "-su",
        "--spatial_units",
        help="Comma delimited str representing list of spatial units to run (if not provided run all)",
        required=False
    )
    parser.add_argument(
        "-b",
        "--benchmark_sources",
        help="Comma delimited str representing list of benchmark sources to run (if not provided run all)",
        required=False
    )
    parser.add_argument(
        "-st",
        "--stages",
        help="Comma delimited str representing list of stages to run (if not provided run all)",
        required=False
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help='Directory to save output evaluation files',
        required=True,
    )

    args = vars(parser.parse_args())

    if args['spatial_units']:
        args['spatial_units'] = args['spatial_units'].split(',')
    if args['benchmark_sources']:
        args['benchmark_sources'] = args['benchmark_sources'].split(',')
    if args['stages']:
        args['stages'] = args['stages']

    # creates the log file name as the script name
    script_file_name = os.path.basename(__file__).split('.')[0] + datetime.now().strftime('%Y-%m-%d_%H:%M')
    # assumes RLOG has been added as a global var.
    RLOG.setup(os.path.join(args['output_dir'], script_file_name + ".log"))

    run_batch_evaluations(**args)







