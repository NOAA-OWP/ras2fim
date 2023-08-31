#!/usr/bin/env python3

import os
import sys
from datetime import datetime

sys.path.append("..")
import boto3
import botocore.exceptions
import ras2fim.src.shared_variables as sv
from botocore.client import ClientError
from tqdm import tqdm


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

        s3_key_path = (f"{s3_folder_path}/{s3_file_name}")

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
def upload_folder_to_s3(
    src_path, bucket_name, s3_folder_path, huc_crs_folder_name, is_verbose
):

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
                    s3_key_path = (
                        f"{s3_folder_path}/{huc_crs_folder_name}/{src_ref_path}/{file}"
                    )
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
            client.delete_object(
                Bucket=bucket_name, Key=s3_folder_path + "/"
            )  # slash added

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
        client.delete_object(
            Bucket=bucket_name, Key=s3_folder_path + "/"
        )  # slash added

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print(
            "** Credentials not available. Try aws configure or review AWS permissions options."
        )
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print(f"** Error deleting files at S3: {ex}")
    raise ex


####################################################################
def move_s3_folder_in_bucket(bucket_name, s3_src_folder_path, s3_target_folder_path, is_verbose):

    """
    Overview:
        To move an S3 folder, we have to copy all of it's objects recursively
        then delete the original folder objects

    Input:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. output_ras2fim/12030105_2276_230810
        - s3_target_folder_path: e.g.  output_ras2fim_archive/12030105_2276_230810
    """
    try:

        print("===================================================================")
        print("")
        print(f"Moving folder from {s3_src_folder_path} to {s3_target_folder_path}")

        client = boto3.client("s3")

        # use paginator as it can handle more than the 1000 file limit max from list_objects_v2
        paginator = client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name,
                                'Prefix': s3_src_folder_path}

        page_iterator = paginator.paginate(**operation_parameters)

        file_count = len(page_iterator)

        with tqdm(
            total=file_count,
            desc=f"Moving {file_count} files in S3",
            bar_format="{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
            ncols=80,
            disable=is_verbose,
        ) as pbar:

            for page in page_iterator:
                print(page['Contents'])
                if "Contents" in page:
                    for key in page[ "Contents" ]:
                        keyString = key[ "Key" ]
                        print(keyString)
                        pbar.update(1)

        """
        # If the bucket is incorrect, it will throw an exception that already makes sense
        s3_src_objs = client.list_objects_v2(Bucket=bucket_name, Prefix=s3_src_folder_path)
        
        if s3_src_objs["KeyCount"] == 0:
            print(f"No files in source folder of {s3_src_folder_path}. Move invalid.")
            return
        
        source_key = s3_src_objs["Contents"][0]["Key"]

        copy_source = {'Bucket': bucket_name, 'Key': s3_src_folder_path}

        client.copy_object(Bucket = bucket_name, CopySource = copy_source, Key = old_key_name + your_destination_file_name)

        # s3 doesn't really use folder names, it jsut makes a key with a long name with slashs
        # in it.
        for folder_name_key in s3_src_objs.get("CommonPrefixes"):
            # comes in like this: output_ras2fim/12090301_2277_230811/
            # strip of the prefix and last slash so we have straight folder names
            key = folder_name_key["Prefix"]

            if is_verbose is True:
                print("--------------------")
                print(f"key is {key}")

            # strip to the final folder names (but first occurance of the prefix only)
            key_child_folder = key.replace(sv.S3_OUTPUT_RAS2FIM_FOLDER, "", 1)

            # We easily could get extra folders that are not huc folders, but we will purge them
            # if it is valid key, add it to a list. Returns a dict.
            # If it does not match a pattern we want, the first element of the tuple will be
            # the word error, but we don't care. We only want valid huc_crs_date pattern folders.
            existing_dic = s3_sf.parse_huc_crs_folder_name(key_child_folder)
            if 'error' in existing_dic: # if error exists, just skip this one
                continue

            # see if the huc and crs it matches the incoming huc number and crs
            if (existing_dic['key_huc'] == src_name_dict['key_huc']) and (
                existing_dic['key_crs_number'] == src_name_dict['key_crs_number']
            ):
                s3_huc_crs_folder_names.append(key_child_folder)

        if is_verbose is True:
            print("huc_crs folders found are ...")
            print(s3_huc_crs_folder_names)
            """

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
        s3_objs = client.list_objects_v2(
            Bucket=bucket_name, Prefix=s3_folder_path, MaxKeys=2, Delimiter="/"
        )

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
        #resp = client.head_bucket(Bucket=bucket_name)        
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
        rtn_varibles_dict["error"] = "Expected three segments split by two underscores." \
                                     "e.g . 12090301_2277_230811"
        return rtn_varibles_dict

    key_huc = segs[0]
    key_crs = segs[1]
    key_date = segs[2]

    if ((not key_huc.isnumeric())
        or (not key_crs.isnumeric())
        or (not key_date.isnumeric())):

        rtn_varibles_dict["error"] = "All three segments are expected to be numeric."
        return rtn_varibles_dict

    if len(key_huc) != 8:
        rtn_varibles_dict["error"] = "First part of the three segments (huc) is not 8 digits long."
        return rtn_varibles_dict

    if (len(key_crs) < 4) or (len(key_crs) > 6):
        rtn_varibles_dict["error"] = "Second part of the three segments (crs) is not"\
                                     " between 4 and 6 digits long."
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
        rtn_varibles_dict['error'] = "Last part of the three segments (date) does not appear" \
                                     " to be in the pattern of yymmdd eg 230812"
        return rtn_varibles_dict

    rtn_varibles_dict['key_huc'] = key_huc
    rtn_varibles_dict['key_crs_number'] = key_crs
    rtn_varibles_dict['key_date_as_str'] = key_date
    rtn_varibles_dict['key_date_as_dt'] = dt_key_date
    rtn_varibles_dict['huc_crs_folder_name'] = huc_crs_folder_name # cleaned version

    return rtn_varibles_dict
