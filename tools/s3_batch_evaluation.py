import boto3
from evaluate_ras2fim_model import evaluate_model_results
from tqdm import tqdm


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


def s3_list_objects(bucket: str, path: str, max_keys=1) -> dict:
    """ Lists all s3 folders and files (to be replaced by existing functions)

    Parameters
    ----------
    bucket: str
        Name of s3 bucket
    path: str
        s3 directory to list objects
    max_keys: int
        Maximum amount of keys to list

    Returns
    -------
    dict
        A dictionary of objects within the s3 bucket folder

    """
    s3 = boto3.client('s3')
    if not path.endswith('/'):
        path = path+'/'
    return s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/', MaxKeys=max_keys)


def folder_exists_and_not_empty(bucket: str, path: str) -> bool:
    """ Checks whether a folder exists and is not empty

    Parameters
    ----------
    bucket: str
        Name of s3 bucket
    path: str
        s3 directory to list objects

    Returns
    -------
    bool
        Whether the folder exists and is not empty or not

    """
    resp = s3_list_objects(bucket=bucket, path=path, max_keys=1)
    return 'Contents' in resp or "CommonPrefixes" in resp


def file_exists(bucket: str, path: str) -> bool:
    """ Check if the file exists in the s3 bucket

    Parameters
    ----------
    bucket: str
        Name of s3 bucket
    path: str
        s3 directory to list objects

    Returns
    -------
    bool
        Whether the file exists in the s3 bucket or not

    """
    file = path.split('/')[-1]
    path = '/'.join(path.split('/')[:-1])

    resp = s3_list_objects(bucket=bucket, path=path, max_keys=9999)

    if "Contents" in resp:
        return file in [x["Key"].split('/')[-1] for x in resp["Contents"]]
    else:
        return False


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
    client = boto3.client('s3')
    result = client.list_objects(Bucket=BUCKET_DEV, Prefix=NWS_BENCHMARK_PREFIX.format(huc), Delimiter='/')
    return [s_unit['Prefix'].split('/')[-2] for s_unit in result.get('CommonPrefixes', [])]


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

    exists = [file_exists(BUCKET, '/'.join(files['inundation_polygons'].split('/')[3:])),
              file_exists(BUCKET, '/'.join(files['model_domain_polygons'].split('/')[3:])),
              file_exists(BUCKET_DEV, '/'.join(files['benchmark_raster'].split('/')[3:]))]

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
    client = boto3.client('s3')
    result = client.list_objects(Bucket=BUCKET, Prefix=PREFIX, Delimiter='/')
    eval_args = []

    for s_unit in result.get('CommonPrefixes'):
        spatial_proc_unit = s_unit.get('Prefix').split('/')[1]

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
    for args in tqdm(eval_args):
        evaluate_model_results(**args)


if __name__ == '__main__':

    import argparse

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

    run_batch_evaluations(**args)


