#!/usr/bin/env python3

import multiprocessing as mp
import os
import sys
import traceback
#from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
import botocore.exceptions
from botocore.client import ClientError
from tqdm import tqdm


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv
import shared_functions as sf


####################################################################
def upload_file_to_s3(src_path, bucket_name, s3_folder_path, file_name="", is_verbose=False):
    """
    Overview:
        This file upload will overwrite an existing file it if already exists. Use caution

    Input
        - src_path: e.g c:\ras2fim_data\output_ras2fim\my_file.csv
        - bucket_name: e.g mys3bucket_name
        - s3_folder_path: e.g.  output_ras2fim or temp\robh
        - file_name: If this is empty, then the file name from the source
            will be used. Otherwise, this becomes the new files name in S3

    """

    # yes.. outside the try/except
    if not os.path.exists(src_path):
        raise FileNotFoundError(src_path)

    try:
        if file_name == "":
            # strip it out the source name
            s3_file_name = os.path.basename(src_path)
        else:
            s3_file_name = file_name

        # safety feature in case we have more than one foreward slash as that can
        # be a mess in S3 (it will honor all slashs)
        s3_folder_path = s3_folder_path.replace("//", "/")

        s3_full_target_path = f"s3://{bucket_name}/{s3_folder_path}"
        if is_verbose is True:
            print(f".. s3 target path is {s3_full_target_path}")

        s3_key_path = f"{s3_folder_path}/{s3_file_name}"

        client = boto3.client("s3")

        with open(src_path, "rb"):
            # s3.Bucket(bucket_name).put_object(Key=s3_key_path, Body=data)
            client.upload_file(src_path, bucket_name, s3_key_path)
            if is_verbose is True:
                print(f".... File uploaded {src_path} as {s3_full_target_path}/{s3_file_name}")

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options."
        )
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print("** Error uploading file to S3:")
        raise ex


####################################################################
def upload_folder_to_s3(src_path, bucket_name, s3_folder_path, huc_crs_folder_name, is_verbose):
    """
    Input
        - src_path: e.g c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - bucket_name: e.g mys3bucket_name
        - s3_folder_path: e.g.  output_ras2fim or output_ras2fim_archive
        - target_huc_crs_folder_name:  12030105_2276_230810 (slash stripped off the end)
    """

    print("===================================================================")
    print("")

    try:
        # we are going to walk it twice, once to get a file count, the other for TQDM processing
        file_count = 0
        for subdir, dirs, files in os.walk(src_path):
            file_count += len(files)

        client = boto3.client("s3")

        with tqdm(
            total=file_count,
            desc=f"Uploading {file_count} files to S3",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
            ncols=80,
            disable=is_verbose,
        ) as pbar:
            for subdir, dirs, files in os.walk(src_path):
                for file in files:
                    # don't let it upload a local copy of the tracker file
                    # if it happens to exist in the source folder.
                    if sv.S3_OUTPUT_TRACKER_FILE in file:
                        pbar.update(1)
                        continue

                    # print(f".. src file = {file}")
                    src_file_path = os.path.join(src_path, subdir, file)
                    if is_verbose is True:
                        print("-----------------")
                        print(f".. src file_path = {src_file_path}")

                    src_ref_path = subdir.replace(src_path, "")

                    # switch the slash
                    src_ref_path = src_ref_path.replace("\\", "/")
                    # s3_key_path = s3_folder_path + src_ref_path + '/' + file
                    s3_key_path = f"{s3_folder_path}/{huc_crs_folder_name}/{src_ref_path}/{file}"
                    # safety feature in case we have more than one foreward slash as that can
                    # be a mess in S3 (it will honor all slashs)
                    s3_key_path = s3_key_path.replace("//", "/")

                    s3_full_target_path = f"s3://{bucket_name}/{s3_key_path}"
                    if is_verbose is True:
                        print(f".. s3 target path is {s3_full_target_path}")

                    try:
                        with open(src_file_path, "rb"):
                            # s3.Bucket(bucket_name).put_object(Key=s3_key_path, Body=data)
                            client.upload_file(src_file_path, bucket_name, s3_key_path)
                            if is_verbose is True:
                                print(".... File uploaded")

                    except FileNotFoundError:
                        print("-----------------")
                        print(f"** The file {src_file_path} was not found")
                        print(
                            "** This is considered a critical error as the file name was"
                            " found programatically."
                        )
                        sys.exit(1)

                    pbar.update(1)

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options."
        )
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print("** Error uploading files to S3:")
        raise ex


####################################################################
def delete_s3_folder(bucket_name, s3_folder_path, is_verbose):
    """
    Overview:
        Sometimes we want to delete s3 folder before loading a replacement.
        Why not just overwrite? It can leave old unwanted files.
        Technically, you can't delete a folder, just all of the objects in a prefix path.

    Input:
        - bucket_name: e.g mys3bucket_name
        - s3_folder_path: e.g.  temp/rob/12030105_2276_230810  (or output_ras2fim)
    """

    s3_full_target_path = f"s3://{bucket_name}/{s3_folder_path}"

    print("===================================================================")
    print("")
    print(f"Deleting the files and folder at {s3_full_target_path}")

    try:
        client = boto3.client("s3")

        # Files inside the folder (yes.. technically there are no folders)
        # but it is possible to a folder that is empty (prefix with no keys)
        s3_files = client.list_objects(Bucket=bucket_name, Prefix=s3_folder_path + "/")

        file_count = len(s3_files)

        if file_count == 0:  # no files to delete but it is possible to have an empty folder
            client.delete_object(Bucket=bucket_name, Key=s3_folder_path + "/")  # slash added

        with tqdm(
            total=file_count,
            desc=f"Deleting {file_count} files from S3",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
            ncols=80,
            disable=is_verbose,
        ) as pbar:
            # Delete all objects in the folder
            for s3_file in s3_files["Contents"]:
                client.delete_object(Bucket=bucket_name, Key=s3_file["Key"])

            pbar.update(1)

        # remove the folder that is left over
        client.delete_object(Bucket=bucket_name, Key=s3_folder_path + "/")  # slash added

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print("** Credentials not available. Try aws configure or review AWS permissions options.")
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print(f"** Error deleting files at S3: {ex}")
        raise ex


####################################################################
def copy_file_in_s3(s3_client, bucket_name, src_file_path, target_file_path):
    """
    Copy a single file from one folder to another. Assumes same bucket.
    This is generally used for multi-threading.
    """

    #print(f"Copying __{src_file_path}")
    copy_source = {'Bucket': bucket_name, 'Key': src_file_path}
    s3_client.copy_object(Bucket=bucket_name, CopySource = copy_source, Key = target_file_path)


####################################################################
def move_s3_folder_in_bucket(bucket_name, s3_src_folder_path, s3_target_folder_path):
    """
    Overview:
        To move an S3 folder, we have to copy all of it's objects recursively
        then delete the original folder objects.
        S3/boto3 has no "move" specific tools.

    Input:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. output_ras2fim/12030105_2276_230810
        - s3_target_folder_path: e.g.  output_ras2fim_archive/12030105_2276_230810
    """
    try:
        print("===================================================================")
        print("")
        print(f"Moving folder from {s3_src_folder_path}  to  {s3_target_folder_path}")

        client = boto3.client("s3")

        # use paginator as it can handle more than the 1000 file limit max from list_objects_v2
        paginator = client.get_paginator("list_objects")
        operation_parameters = {"Bucket": bucket_name, "Prefix": s3_src_folder_path}

        page_iterator = paginator.paginate(**operation_parameters)

        # Lets run quickly through it once to get a count and a list of src files names
        # (only take a min or two) but the copying is slower.
        s3_files = []  # a list of dictionaries (src file path, targ file path)
        print("Loading existing files... standby (~1 to 2 mins)")
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    if key['Size'] != 0:
                        src_file_path = key["Key"]
                        target_file_path = src_file_path.replace(s3_src_folder_path, s3_target_folder_path, 1)
                        item = {
                            's3_client': client,
                            'bucket_name': bucket_name,
                            'src_file_path': src_file_path,
                            'target_file_path': target_file_path
                        }
                        # adds a dict to the list
                        s3_files.append(item)
                    #pbar.update(1)
        print(f"Number of files to be copied is {len(s3_files)}")

        # If the bucket is incorrect, it will throw an exception that already makes sense
        #s3_src_objs = client.list_objects_v2(Bucket=bucket_name, Prefix=s3_src_folder_path)
        
        if len(s3_files) == 0:
            print(f"No files in source folder of {s3_src_folder_path}. Move invalid.")
            return
        
        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at 20
        num_workers = 20

        # copy the files first
        #session = boto3.client("s3")
        with ThreadPoolExecutor(max_workers=num_workers) as executor:

            executor_dict = {}

            for copy_file_args in s3_files:
                #print(f"__ copy_file_args src is {copy_file_args['src_file_path']}")

                try:
                    future = executor.submit(copy_file_in_s3, **copy_file_args)
                    executor_dict[future] = copy_file_args['src_file_path']
                except Exception as tp_ex:
                    print(f"*** {tp_ex}")
                    traceback.print_exc()
                    sys.exit(1)                    

            """
            for future in futures.as_completed(executor_dict):
                key = future_to_key[future]
                exception = future.exception()

                if not exception:
                    yield key, future.result()
                else:
                    yield key, exception
            """

            sf.progress_bar_handler(executor_dict, True, f"Copying files with {num_workers} workers")


    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options."
        )
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print("** Error moving folders in S3:")
        raise ex


####################################################################
def is_valid_s3_folder(s3_bucket_and_folder):
    # This will throw exceptions for all errors

    if s3_bucket_and_folder.endswith("/"):
        s3_bucket_and_folder = s3_bucket_and_folder[:-1]

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_bucket_and_folder.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]
    s3_folder_path = adj_s3_path.replace(bucket_name, "", 1)
    s3_folder_path = s3_folder_path.lstrip("/")

    client = boto3.client("s3")

    try:
        # If the bucket is incorrect, it will throw an exception that already makes sense
        s3_objs = client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder_path, MaxKeys=2, Delimiter="/")

        # assumes the folder exists and has values ??
        # print(s3_objs)
        if s3_objs["KeyCount"] == 0:
            raise ValueError(
                "S3 bucket exists but the folder path does not exist and is required. "
                "Path is case-sensitive"
            )

    except ValueError:
        # don't trap these types, just re-raise
        raise

    except botocore.exceptions.NoCredentialsError:
        print("** Credentials not available. Try aws configure.")
    except Exception as ex:
        print(f"An error has occurred with talking with S3; Details {ex}")

    return bucket_name, s3_folder_path


####################################################################
def does_s3_bucket_exist(bucket_name):
    client = boto3.client("s3")

    try:
        client.head_bucket(Bucket=bucket_name)
        # resp = client.head_bucket(Bucket=bucket_name)
        # print(resp)

        return True  # no exception?  means it exist

    except botocore.exceptions.NoCredentialsError:
        print("** Credentials not available for submitted bucket. Try aws configure.")
        sys.exit(1)

    except client.exceptions.NoSuchBucket:
        return False

    except ClientError:
        return False

    # other exceptions can be passed through


####################################################################
def parse_huc_crs_folder_name(huc_crs_folder_name):
    """
    Overview:
        While all uses of this function pass back errors if invalid the calling code can decide if it is
        an exception. Sometimes it doesn't, it just want to check to see if the key is a huc crs key.

    Input:
        huc_crs_folder_name: migth be a full s3 string, or a s3 key or just the folder name
           e.g.  s3://xzy/output_ras2fim/12090301_2277_230811
              or output_ras2fim/12090301_2277_230811
              or 12090301_2277_230811

    Output:
        A dictionary with records of:
                       key_huc,
                       key_crs_number,
                       key_date_as_str (date string eg: 230811),
                       key_date_as_dt  (date obj for 230811)
                       huc_crs_folder_name (12090301_2277_230811) (cleaned version)
        OR
        If in error, dictionary will have only one key of "error", saying why it the
           reason for the error. It lets the calling code to decide if it wants to raise
           and exception.
           Why? There are some incoming folders that will not match the pattern and the
           calling code will want to know that and may just continue on

        BUT: if the incoming param doesn't exist, that raises an exception
    """

    rtn_varibles_dict = {}

    if huc_crs_folder_name == "":
        raise ValueError("huc_crs_folder_name can not be empty")

    # cut off the s3 part if there is any.
    huc_crs_folder_name = huc_crs_folder_name.replace("s3://", "")

    # s3_folder_path and we want to strip the first one only. (can be deeper levels)
    if huc_crs_folder_name.endswith("/"):
        huc_crs_folder_name = huc_crs_folder_name[:-1]  # strip the ending slash

    # see if there / in it and split out based on the last one (migth not be one)
    huc_crs_folder_segs = huc_crs_folder_name.rsplit("/", 1)
    if len(huc_crs_folder_segs) > 1:
        huc_crs_folder_name = huc_crs_folder_segs[-1]

    # The best see if it has an underscore in it, split if based on that, then
    # see the first chars are an 8 digit number and that it has two underscores (3 segs)
    # and will split it to a list of tuples
    if "_" not in huc_crs_folder_name or len(huc_crs_folder_name) < 9:
        rtn_varibles_dict["error"] = "Does not contain any underscore or folder name to short."
        return rtn_varibles_dict

    segs = huc_crs_folder_name.split("_")
    if len(segs) != 3:
        rtn_varibles_dict["error"] = (
            "Expected three segments split by two underscores." "e.g . 12090301_2277_230811"
        )
        return rtn_varibles_dict

    key_huc = segs[0]
    key_crs = segs[1]
    key_date = segs[2]

    if (not key_huc.isnumeric()) or (not key_crs.isnumeric()) or (not key_date.isnumeric()):
        rtn_varibles_dict["error"] = "All three segments are expected to be numeric."
        return rtn_varibles_dict

    if len(key_huc) != 8:
        rtn_varibles_dict["error"] = "First part of the three segments (huc) is not 8 digits long."
        return rtn_varibles_dict

    if (len(key_crs) < 4) or (len(key_crs) > 6):
        rtn_varibles_dict["error"] = (
            "Second part of the three segments (crs) is not" " between 4 and 6 digits long."
        )
        return rtn_varibles_dict

    if len(key_date) != 6:
        rtn_varibles_dict["error"] = "Last part of the three segments (date) is not 6 digits long."
        return rtn_varibles_dict

    # test date format
    # format should come in as yymmdd  e.g. 230812
    # If successful, the actual date object be added
    dt_key_date = None
    try:
        dt_key_date = datetime.strptime(key_date, "%y%m%d")
    except Exception:
        rtn_varibles_dict["error"] = (
            "Last part of the three segments (date) does not appear"
            " to be in the pattern of yymmdd eg 230812"
        )
        return rtn_varibles_dict

    rtn_varibles_dict["key_huc"] = key_huc
    rtn_varibles_dict["key_crs_number"] = key_crs
    rtn_varibles_dict["key_date_as_str"] = key_date
    rtn_varibles_dict["key_date_as_dt"] = dt_key_date
    rtn_varibles_dict["huc_crs_folder_name"] = huc_crs_folder_name  # cleaned version

    return rtn_varibles_dict
