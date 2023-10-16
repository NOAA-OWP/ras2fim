#!/usr/bin/env python3

import os
import sys
import traceback

# from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
import botocore.exceptions
from botocore.client import ClientError


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_functions as sf
import shared_variables as sv


####################################################################
def upload_file_to_s3(bucket_name, src_path, s3_folder_path, file_name="", show_upload_msg=True):
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

        s3_key_path = f"{s3_folder_path}/{s3_file_name}"

        client = boto3.client("s3")

        with open(src_path, "rb"):
            client.upload_file(src_path, bucket_name, s3_key_path)

            if show_upload_msg is True:
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


###################################################################
def upload_folder_to_s3(src_path, bucket_name, s3_folder_path, unit_folder_name):
    """
    Input
        - src_path: e.g c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - bucket_name: e.g mys3bucket_name
        - s3_folder_path: e.g.  output_ras2fim or output_ras2fim_archive
        - unit_folder_name:  12030105_2276_230810 (slash stripped off the end)
    """

    s3_full_target_path = f"s3://{bucket_name}/{s3_folder_path}/{unit_folder_name}"

    print("===================================================================")
    print("")
    print(f"Uploading folder from {src_path}  to  {s3_full_target_path}")
    print()

    # nested function
    def __upload_file(s3_client, bucket_name, src_file_path, target_file_path):
        with open(src_file_path, "rb"):
            # s3.Bucket(bucket_name).put_object(Key=s3_key_path, Body=data)
            s3_client.upload_file(src_file_path, bucket_name, target_file_path)

    try:
        client = boto3.client("s3")

        s3_files = []  # a list of dictionaries (src file path, targ file path)
        for subdir, dirs, files in os.walk(src_path):
            for file in files:
                # don't let it upload a local copy of the tracker file
                # if it happens to exist in the source folder.
                if sv.S3_OUTPUT_TRACKER_FILE in file:
                    continue

                # print(f".. src file = {file}")
                src_file_path = os.path.join(src_path, subdir, file)
                src_ref_path = subdir.replace(src_path, "")

                # switch the slash
                src_ref_path = src_ref_path.replace("\\", "/")
                s3_key_path = f"{s3_folder_path}/{unit_folder_name}/{src_ref_path}/{file}"
                # safety feature in case we have more than one foreward slash as that can
                # be a mess in S3 (it will honor all slashs)
                s3_key_path = s3_key_path.replace("//", "/")

                item = {
                    's3_client': client,
                    'bucket_name': bucket_name,
                    'src_file_path': src_file_path,
                    'target_file_path': s3_key_path,
                }
                # adds a dict to the list
                s3_files.append(item)

        if len(s3_files) == 0:
            print(f"No files in source folder of {src_path}. Upload invalid.")
            return

        print(f"Number of files to be uploaded is {len(s3_files)}")

        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at 20 for now
        num_workers = 20

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor_dict = {}

            for upload_file_args in s3_files:
                try:
                    future = executor.submit(__upload_file, **upload_file_args)
                    executor_dict[future] = upload_file_args['src_file_path']

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

            sf.progress_bar_handler(executor_dict, True, f"Uploading files with {num_workers} workers")

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
def delete_s3_folder(bucket_name, s3_folder_path):
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
    print(f"Deleting the files and folders at {s3_full_target_path}")
    print()

    # nested function
    def __delete_file(s3_client, bucket_name, s3_file_path):
        s3_client.delete_object(Bucket=bucket_name, Key=s3_file_path)

    try:
        client = boto3.client("s3")

        # use paginator as it can handle more than the 1000 file limit max from list_objects_v2
        paginator = client.get_paginator("list_objects")
        operation_parameters = {"Bucket": bucket_name, "Prefix": s3_folder_path}

        page_iterator = paginator.paginate(**operation_parameters)

        # Lets run quickly through it once to get a count and a list of src files names
        # (only take a min or two) but the copying is slower.
        s3_files = []  # a list of dictionaries (src file path, targ file path)
        print("Calculating list of files to be deleted ... standby (~1 to 2 mins)")
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    item = {'s3_client': client, 'bucket_name': bucket_name, 's3_file_path': key["Key"]}
                    # adds a dict to the list
                    s3_files.append(item)

        if len(s3_files) == 0:
            print(f"No files in s3 folder of {s3_full_target_path} to be deleted.")
            return

        print(f"Number of files to be deleted in s3 is {len(s3_files)}")

        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at 20
        num_workers = 20

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor_dict = {}

            for del_file_args in s3_files:
                try:
                    future = executor.submit(__delete_file, **del_file_args)
                    executor_dict[future] = del_file_args['s3_file_path']
                except Exception as tp_ex:
                    print(f"*** {tp_ex}")
                    traceback.print_exc()
                    sys.exit(1)

            sf.progress_bar_handler(executor_dict, True, f"Deleting files with {num_workers} workers")

        # remove the folder that is left over (Did we check it if existed?)
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

    # nested function
    def __copy_file(s3_client, bucket_name, src_file_path, target_file_path):
        """
        Copy a single file from one folder to another.
        This is used for multi-threading.
        """

        # print(f"Copying __{src_file_path}")
        copy_source = {'Bucket': bucket_name, 'Key': src_file_path}
        s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=target_file_path)

    try:
        print("===================================================================")
        print("")
        print(f"Moving folder from {s3_src_folder_path}  to  {s3_target_folder_path}")
        print()
        print("***  NOTE: s3 can not move files, it can only copy then delete them.")
        print()

        client = boto3.client("s3")

        # use paginator as it can handle more than the 1000 file limit max from list_objects_v2
        paginator = client.get_paginator("list_objects")
        operation_parameters = {"Bucket": bucket_name, "Prefix": s3_src_folder_path}

        page_iterator = paginator.paginate(**operation_parameters)

        # Lets run quickly through it once to get a count and a list of src files names
        # (only take a min or two) but the copying is slower.
        s3_files = []  # a list of dictionaries (src file path, targ file path)
        print("Creating list of files to be copied ... standby (~1 to 2 mins)")
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
                            'target_file_path': target_file_path,
                        }
                        # adds a dict to the list
                        s3_files.append(item)

        # If the bucket is incorrect, it will throw an exception that already makes sense

        if len(s3_files) == 0:
            print(f"No files in source folder of {s3_src_folder_path}. Move invalid.")
            return

        print(f"Number of files to be copied is {len(s3_files)}")

        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at 20
        num_workers = 20

        # copy the files first
        # session = boto3.client("s3")
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor_dict = {}

            for copy_file_args in s3_files:
                try:
                    future = executor.submit(__copy_file, **copy_file_args)
                    executor_dict[future] = copy_file_args['src_file_path']
                except Exception as tp_ex:
                    print(f"*** {tp_ex}")
                    traceback.print_exc()
                    sys.exit(1)

            sf.progress_bar_handler(executor_dict, True, f"Copying files with {num_workers} workers")

        # now delete the original ones
        delete_s3_folder(bucket_name, s3_src_folder_path)

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

    """
    Process:  This will throw exceptions for all errors
    Input: 
        - s3_bucket_and_folder: eg. s3://ras2fim/OWP_ras_models
    """

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
def parse_unit_folder_name(unit_folder_name):
    """
    Overview:
        While all uses of this function pass back errors if invalid the calling code can decide if it is
        an exception. Sometimes it doesn't, it just want to check to see if the key is a huc crs key.

    Input:
        unit_folder_name: migth be a full s3 string, or a s3 key or just the folder name
           e.g.  s3://xzy/output_ras2fim/12090301_2277_230811
              or output_ras2fim/12090301_2277_230811
              or 12090301_2277_230811

    Output:
        A dictionary with records of:
                       key_huc,
                       key_crs_number,
                       key_date_as_str (date string eg: 230811),
                       key_date_as_dt  (date obj for 230811)
                       unit_folder_name (12090301_2277_230811) (cleaned version)
        OR
        If in error, dictionary will have only one key of "error", saying why it the
           reason for the error. It lets the calling code to decide if it wants to raise
           and exception.
           Why? There are some incoming folders that will not match the pattern and the
           calling code will want to know that and may just continue on

        BUT: if the incoming param doesn't exist, that raises an exception
    """

    rtn_varibles_dict = {}

    if unit_folder_name == "":
        raise ValueError("unit_folder_name can not be empty")

    # cut off the s3 part if there is any.
    unit_folder_name = unit_folder_name.replace("s3://", "")

    # s3_folder_path and we want to strip the first one only. (can be deeper levels)
    if unit_folder_name.endswith("/"):
        unit_folder_name = unit_folder_name[:-1]  # strip the ending slash

    # see if there / in it and split out based on the last one (migth not be one)
    unit_folder_segs = unit_folder_name.rsplit("/", 1)
    if len(unit_folder_segs) > 1:
        unit_folder_name = unit_folder_segs[-1]

    # The best see if it has an underscore in it, split if based on that, then
    # see the first chars are an 8 digit number and that it has two underscores (3 segs)
    # and will split it to a list of tuples
    if "_" not in unit_folder_name or len(unit_folder_name) < 9:
        rtn_varibles_dict["error"] = "Does not contain any underscore or folder name to short."
        return rtn_varibles_dict

    segs = unit_folder_name.split("_")
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
    rtn_varibles_dict["unit_folder_name"] = unit_folder_name  # cleaned version

    return rtn_varibles_dict
