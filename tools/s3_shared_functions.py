﻿#!/usr/bin/env python3

import datetime as dt
import fnmatch
import os
import random
import shutil
import subprocess
import sys
import time
import traceback
from concurrent import futures
from datetime import datetime

import boto3
import botocore.exceptions
import colored as cl
from botocore.client import ClientError


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shared_variables as sv
import shared_functions as sf


# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def upload_file_to_s3(src_path, full_s3_path_and_file_name):
    """
    Overview:
        This file upload will overwrite an existing file it if already exists. Use caution

    Input
        - src_path: e.g c:\ras2fim_data\output_ras2fim\my_file.csv
        - full_s3_path_and_file_name: e.g s3://mys3bucket_name/output_ras2fim/some_file.txt
    """

    # yes.. outside the try/except
    if not os.path.exists(src_path):
        raise FileNotFoundError(src_path)

    try:
        if full_s3_path_and_file_name == "":
            raise Exception("full s3 path and file name is not defined")

        # we need the "s3 part stripped off for now" (if it is even there)
        adj_s3_path = full_s3_path_and_file_name.replace("s3://", "")
        path_segs = adj_s3_path.split("/")
        bucket_name = path_segs[0]
        # could have subfolders
        s3_key_path = adj_s3_path.replace(bucket_name, "", 1)
        s3_key_path = s3_key_path.lstrip("/")

        if len(s3_key_path) == 0:
            raise Exception(f"full s3 path and file name of {full_s3_path_and_file_name} is invalid")

        client = boto3.client("s3")

        with open(src_path, "rb"):
            client.upload_file(src_path, bucket_name, s3_key_path)

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error uploading file to S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
def upload_folder_to_s3(src_path, bucket_name, s3_folder_path, unit_folder_name, skip_files=[]):
    """
    Input
        - src_path: e.g c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - bucket_name: e.g mys3bucket_name
        - s3_folder_path: e.g.  output_ras2fim or output_ras2fim_archive
        - unit_folder_name:  12030105_2276_230810 (slash stripped off the end)
        - skip_files: files we don't want uploaded. (fully pathed)

    Notes:
        - if the file names starts with three underscores, it will not be uploaded to S3.
    """

    s3_full_target_path = f"s3://{bucket_name}/{s3_folder_path}/{unit_folder_name}"

    RLOG.lprint("===================================================================")
    print("")
    RLOG.notice(f"Uploading folder from {src_path}  to  {s3_full_target_path}")
    print()

    # nested function
    def __upload_file(s3_client, bucket_name, src_file_path, target_file_path):
        with open(src_file_path, "rb"):
            # s3.Bucket(bucket_name).put_object(Key=s3_key_path, Body=data)
            s3_client.upload_file(src_file_path, bucket_name, target_file_path)

    try:
        client = boto3.client("s3")

        s3_files = []  # a list of dictionaries (src file path, targ file path)

        for subdir, dirs, files in os.walk(src_path, followlinks=False):
            for file in files:
                src_file_path = os.path.join(src_path, subdir, file)

                RLOG.trace(f".. src_file_path = {src_file_path}")

                if src_file_path in skip_files:
                    RLOG.trace(f"skipped: {file}")
                    continue

                src_folder = os.path.dirname(src_file_path)
                src_file_name = os.path.basename(src_file_path)
                trg_folder_path = src_folder.replace(src_path, "")

                # switch the slash
                trg_folder_path = trg_folder_path.replace("\\", "/")
                s3_key_path = f"{s3_folder_path}/{unit_folder_name}/{trg_folder_path}/{src_file_name}"
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
            RLOG.error(f"No files in source folder of {src_path}. Upload invalid")
            return

        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at max of 20 for now.
        num_workers = 20
        total_cpus_available = os.cpu_count() - 2
        if total_cpus_available < num_workers:
            num_workers = total_cpus_available

        RLOG.lprint(f"Number of files to be uploaded is {len(s3_files)}")
        print(" ... This may take a few minutes, stand by")
        RLOG.lprint(f" ... Uploading with {num_workers} workers")

        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor_dict = {}

            for upload_file_args in s3_files:
                try:
                    future = executor.submit(__upload_file, **upload_file_args)
                    executor_dict[future] = upload_file_args['src_file_path']

                except Exception:
                    RLOG.critical(f"Critical error while uploading {upload_file_args['src_file_path']}")
                    RLOG.critical(traceback.format_exc())
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
        RLOG.lprint(" ... Uploading complete")
        print()

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        RLOG.critical("** Error uploading files to S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
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

    RLOG.lprint("===================================================================")
    print("")
    RLOG.notice(f"Deleting the files and folders at {s3_full_target_path}")
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
        RLOG.lprint("Calculating list of files to be deleted ... standby (~1 to 2 mins)")
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    item = {'s3_client': client, 'bucket_name': bucket_name, 's3_file_path': key["Key"]}
                    # adds a dict to the list
                    s3_files.append(item)

        if len(s3_files) == 0:
            RLOG.error(f"No files in s3 folder of {s3_full_target_path} to be deleted")
            return

        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at max of 20 for now.
        num_workers = 20
        total_cpus_available = os.cpu_count() - 2
        if total_cpus_available < num_workers:
            num_workers = total_cpus_available

        RLOG.lprint(f"Number of files to be deleted in s3 is {len(s3_files)}")
        print(" ... This may take a few minutes, stand by")
        RLOG.lprint(f" ... Deleting with {num_workers} workers")

        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor_dict = {}

            for del_file_args in s3_files:
                try:
                    future = executor.submit(__delete_file, **del_file_args)
                    executor_dict[future] = del_file_args['s3_file_path']
                except Exception:
                    RLOG.critical(f"Critical Error while deleting {del_file_args['s3_file_path']} ")
                    RLOG.critical(traceback.format_exc())
                    sys.exit(1)

        # remove the folder that is left over (Did we check it if existed?)
        client.delete_object(Bucket=bucket_name, Key=s3_folder_path + "/")  # slash added
        RLOG.lprint(" ... Deleting complete")
        print()

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("-----------------")
        RLOG.critical("** Credentials not available. Try aws configure or review AWS permissions options")
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error deleting files at S3")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
def move_s3_folder_in_bucket(bucket_name, s3_src_folder_path, s3_target_folder_path):
    """
    Overview:
        To move an S3 folder, we have to copy all of it's objects recursively
        then delete the original folder objects.
        S3/boto3 has no "move" specific tools.

    Input:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. output_ras2fim/12030105_2276_ble_230810
        - s3_target_folder_path: e.g.  output_ras2fim_archive/12030105_2276_ble_230810
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
        RLOG.lprint("===================================================================")
        print("")
        RLOG.notice(f"Moving folder from {s3_src_folder_path}  to  {s3_target_folder_path}")
        print()
        print(
            f"{cl.fg('dodger_blue_1')}"
            "***  NOTE: s3 can not move files, it can only copy the files then delete them"
            f"{cl.attr(0)}"
        )
        print()

        client = boto3.client("s3")

        # use paginator as it can handle more than the 1000 file limit max from list_objects_v2
        paginator = client.get_paginator("list_objects")
        operation_parameters = {"Bucket": bucket_name, "Prefix": s3_src_folder_path}

        page_iterator = paginator.paginate(**operation_parameters)

        # Lets run quickly through it once to get a count and a list of src files names
        # (only take a min or two) but the copying is slower.
        s3_files = []  # a list of dictionaries (src file path, targ file path)
        RLOG.lprint("Creating list of files to be copied ... standby (~1 to 2 mins)")
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
            RLOG.error(f"No files in source folder of {s3_src_folder_path}. Move invalid")
            return

        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at max of 20 for now.
        num_workers = 20
        total_cpus_available = os.cpu_count() - 2
        if total_cpus_available < num_workers:
            num_workers = total_cpus_available

        RLOG.lprint(f"Number of files to be copied is {len(s3_files)}")
        print(" ... This may take a few minutes, stand by")
        RLOG.lprint(f" ... Copying with {num_workers} workers")

        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor_dict = {}

            for copy_file_args in s3_files:
                try:
                    future = executor.submit(__copy_file, **copy_file_args)
                    executor_dict[future] = copy_file_args['src_file_path']
                except Exception:
                    RLOG.critical(f"Error while coping file for {copy_file_args['src_file_path']}")
                    RLOG.critical(traceback.format_exc())
                    sys.exit(1)

        RLOG.lprint(" ... Copying complete")

        # now delete the original ones
        delete_s3_folder(bucket_name, s3_src_folder_path)

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options."
        )
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error moving folders in S3:")
        RLOG.critical(traceback.format_exc())
        raise ex

# -------------------------------------------------
def download_folders(list_folders):
    """
    Process:
        - The s3 pathing values needs to be case-sensitive.
        - This method is multi-threaded (not multi-proc) for performance.
        - If the local_folders_already exist, it will not pre-clean the folders so it is
        encouraged to pre-delete the child folders if required.

    Inputs:
        - list_folders. List of dictionary objects
            - schema is:
                - "bucket_name":
                - "folder_id": folder_name or any unique value. e.g. 12030105_2276_ble_230923
                - "s3_src_folder": e.g. output_ras2fim/12030105_2276_ble_230923
                - "target_local_folder": e.g. C:\ras2fim_data\output_ras2fim\12030105_2276_ble_230923
                   all downloaded files and folders will be under this folder.        
      Output
        - The dictionary objects will have three keys.
            - "folder_id": folder_name or any unique value. e.g. 12030105_2276_ble_230923
            - "download_success" as either
                the string value of 'True' or 'False'
            - "error_details" - why did it fail

    """
    rtn_threads = []
    rtn_download_details = []

    try:
        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at max of 20 for now.
        num_workers = 10
        total_cpus_available = os.cpu_count()  # yes.. it is MThreading, so you don't need a -2 margin
        if total_cpus_available < num_workers:
            num_workers = total_cpus_available

        RLOG.notice(f"Number of folders to be downloaded is {len(list_folders)}")
        RLOG.lprint(f" ... downloading with {num_workers} workers")

        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures_dict = []

            for download_args in list_folders:
                futures_dict.append(executor.submit(download_folder, **download_args))

            for future_result in futures.as_completed(futures_dict):
                result = future_result.result()
                future_exception = future_result.exception()

                if not future_exception:
                    rtn_threads.append(result)
                else:
                    RLOG.critical(f"Error occurred with {futures_dict}")
                    raise future_exception

        for result in rtn_threads:
            # err_msg might be empty
            item = {"folder_id": result['folder_id'], 
                    "download_success": result['is_success'],
                    "error_details": result['err_msg']}

            rtn_download_details.append(item)

        return rtn_download_details

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        RLOG.critical("** Error downloading folders from S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
def download_folder(bucket_name, folder_id, s3_src_folder, target_local_folder):

    """
    Process:
        - Using the incoming s3 src folder, call get_records to get a list of child folders and files
        - Open a s3 client and iterate through the files to download
    Inputs:
        - bucket_name:  eg. ras2fim-dev
        - folder_id:  e.g 12090301_2277_ble_230923  (or really anything unique)
        - src_s3_folder: e.g. output_ras2fim\\12090301_2277_ble_230923
        - target_local_folder: e.g . C:\\ras2fim_data\\output_ras2fim\\12090301_2277_ble_230923
            or something like: C:\\ras2fim_data\\ras2fim_releases\\r102-test\\units\\12090301_2277_ble_230923
    Output:
        - A dictionary with three records
            - folder_id : the incoming folder id (for reference purposes)
            - is_success: True or False (did it download anything successfully)
            - err_msg: If it failed.. why did it fail, otherwise empty string

    """

    # TODO (Nov 22, 2023 - add arg validation)    

    # we are adding a small random delay timer so all of the threads don't start at the exact same time.
    delay_in_secs = round(random.uniform(0.1, 3.0), 2)  # 1/10 of a second seconds to 3 min  (to 2 decimal places)
    time.sleep(delay_in_secs)

    full_src_path = f"s3://{bucket_name}/{s3_src_folder}"

    # send in blank search key
    RLOG.lprint(f"Loading a list of files and folders from {full_src_path}")

    # add a duration system
    start_time = dt.datetime.utcnow()

    s3_items = get_records_list(bucket_name, s3_src_folder, "", False)

    if len(s3_items) == 0:
        RLOG.error(f"Download Skipped for {full_src_path}. no s3 files found.")
        result = {"folder_id": folder_id, "is_success": False, "err_msg": "no s3 files found"}
        return result

    try:
        #s3_client = boto3.client('s3')

        print()
        RLOG.lprint(f"Downloading files files and folders for {full_src_path}")

        if os.path.exists(target_local_folder):
            shutil.rmtree(target_local_folder)
        os.makedirs(target_local_folder)

        if not target_local_folder.endswith("\\"):
            target_local_folder += "\\"

        target_local_folder.replace("\\\\", "\\")

        if not full_src_path.endswith("/"):
            full_src_path += "/"

        #Launch a subprocess

        """
        for s3_item in s3_items:  # files and folders under the s3_src_folder
            # need to make the src file to be the full url minus the s:// and bucket name\
            # key is a file name (or child folder name) under s3_src_folder
            src_file = f"{s3_src_folder}/{s3_item['key']}"
            trg_file = os.path.join(target_local_folder, s3_item["key"])

            # Why extract the directory name? the key might have subfolder
            # names right in the key
            trg_path = os.path.dirname(trg_file)

            # folders may not yet exist
            if not os.path.exists(trg_path):
                os.makedirs(trg_path)
            #with open(trg_file, 'wb') as f:
            #    s3_client.download_fileobj(Bucket=bucket_name, Key=src_file, Fileobj=f)
            s3_client.download_file(Bucket=bucket_name, Key=src_file, Filename=trg_file)

        """

        cmd = f"aws s3 sync {full_src_path} {target_local_folder}"
        print(f"running : {cmd}")

        process = subprocess.Popen(
            cmd, shell=True, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, errors='replace'
        )

        num_files_downloaded = 0
        while True:
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            if realtime_output:
                # AWS spits out a tons of "completes"
                if not realtime_output.startswith("download") and not realtime_output.startswith("Completed"):
                    RLOG.error(realtime_output.strip())
                    #print(realtime_output.strip(), flush=False)
                else:
                #print(f"realtime_output is {realtime_output} ")
                    num_files_downloaded += 1

                sys.stdout.flush()


        if num_files_downloaded == 0:
            RLOG.warning(f"No files were found for downloaded from {full_src_path}.")
            result = {"folder_id": folder_id, "is_success": False, "err_msg": "No files found for download"}
        else:
            RLOG.notice(f"Download complete for {full_src_path}.\n"
                    f"Number of files / folders downloaded is {num_files_downloaded}")
            result = {"folder_id": folder_id, "is_success": True, "err_msg": ""}
            print()

        sf.print_date_time_duration(start_time, dt.datetime.utcnow())

    except Exception as ex:
        RLOG.error(f"Download Failed for {full_src_path}")
        RLOG.error(traceback.format_exc())      
        result = {"folder_id": folder_id, "is_success": False, "err_msg": ex}

    return result


# -------------------------------------------------
def get_records_list(bucket_name, s3_src_folder_path, search_key, is_verbose=False):
    """
    Process:
        - uses a S3 paginator to recursively look for matches (non case-sensitive)
    Inputs:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. OWP_ras_models/models (case-sensitive)
        - search_key: phrase (str) to be searched: e.g *Trinity River*
    Output
        - A list of dictionary items matching records.
            - first value is the match "key":
                    ie) 1262811_UNT 213 in Village Cr Washd_g01_1689773310/UNT 213 in Village Cr Washd.r01
            - The second value is the full "url" of it
                ie) s3://ras2fim-dev/OWP_ras_models/models-12030105-full/1262811_UNT...r01
    """

    try:
        if is_verbose is True:
            print("")
            RLOG.lprint(f"{cl.fg('light_yellow')}"
                        f"Searching files and folders in s3://{bucket_name}/{s3_src_folder_path}"
                        f" based on search key of '{search_key}'.\n"
                        " This may take a few minutes depending on size of the search folder"
                        f"{cl.attr(0)}")
            print("")

        if not s3_src_folder_path.endswith("/"):
            s3_src_folder_path += "/"

        s3_client = boto3.client("s3")
        s3_items = []  # a list of dictionaries

        default_kwargs = {"Bucket": bucket_name, "Prefix": s3_src_folder_path}

        # Examples:
        # search_key = "TRINITY*"  (none... only work if no chars in front of Trinity)
        # search_key = "*TRINITY*"
        # search_key = "*trinity river*"
        # search_key = "*caney*.prj"
        # search_key = "*caney*.g01"
        # search_key = "*caney*.g01*"
        # search_key = "*.g01*"
        # search_key = "*.g01"
        # search_key = "1262811*"

        next_token = ""

        while next_token is not None:
            updated_kwargs = default_kwargs.copy()
            if next_token != "":
                updated_kwargs["ContinuationToken"] = next_token

            # will limit to 1000 objects - hence tokens
            response = s3_client.list_objects_v2(**updated_kwargs)
            if response.get("KeyCount") == 0:
                return s3_items

            contents = response.get("Contents")
            if contents is None:
                raise Exception("s3 contents not did not load correctly")

            for result in contents:
                key = result.get("Key")
                key_adj = key.replace(s3_src_folder_path, "")
                if search_key == "":
                    item = {"key": key_adj, "url": f"s3://{bucket_name}/{s3_src_folder_path}{key_adj}"}
                    s3_items.append(item)
                elif fnmatch.fnmatch(key_adj, search_key):
                    item = {"key": key_adj, "url": f"s3://{bucket_name}/{s3_src_folder_path}{key_adj}"}
                    s3_items.append(item)
                # no else needed

            next_token = response.get("NextContinuationToken")

        return s3_items

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error finding files or folders in S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
def get_folder_list(bucket_name, s3_src_folder_path, is_verbose):
    """
    Process:
        - uses a S3 paginator to recursively look for matching folder names
    Inputs:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. OWP_ras_models/models (case-sensitive)
    Output
        - A list of dictionary items matching records.
            - first value is the match "key" (as in folder name)
                    ie) 1262811_UNT 213 in Village Cr Washd_g01_1689773310
            - The second value is the full "url" of it
                ie) s3://ras2fim-dev/OWP_ras_models/models-12030105-full/1262811_UNT...
    """

    try:
        if is_verbose is True:
            print("")
            RLOG.lprint(
                f"{cl.fg('light_yellow')}"
                f" Searching for folder names in s3://{bucket_name}/{s3_src_folder_path} (non-recursive)\n"
                " This may take a few minutes depending on number of folders in the search folder"
                f"{cl.attr(0)}"
            )
            print("")

        if not s3_src_folder_path.endswith("/"):    
            s3_src_folder_path += "/"

        s3_client = boto3.client("s3")
        s3_items = []  # a list of dictionaries

        default_kwargs = {"Bucket": bucket_name, "Prefix": s3_src_folder_path, "Delimiter": "/"}

        next_token = ""

        while next_token is not None:
            updated_kwargs = default_kwargs.copy()
            if next_token != "":
                updated_kwargs["ContinuationToken"] = next_token

            # will limit to 1000 objects - hence tokens
            response = s3_client.list_objects_v2(**updated_kwargs)
            # print(response)
            if response.get("KeyCount") == 0:
                return s3_items

            prefix_recs = response.get("CommonPrefixes")
            if prefix_recs is None:
                raise Exception("s3 not did not load folders names correctly")

            for result in prefix_recs:
                prefix = result.get("Prefix")
                prefix_adj = prefix.replace(s3_src_folder_path, "")
                if prefix_adj.endswith("/"):
                    prefix_adj = prefix_adj[:-1]
                item = {"key": prefix_adj, "url": f"s3://{bucket_name}/{s3_src_folder_path}{prefix_adj}"}
                s3_items.append(item)

            next_token = response.get("NextContinuationToken")

        return s3_items

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error finding files or folders in S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
def get_folder_size(bucket_name, s3_src_folder_path):
    """
    Granted.. there is no such thing as folders in S3, only keys, but we want the size of
    a folder and its recursive size

    Process:
        - uses a S3 paginator to recursively look for matching folder names
    Inputs:
        - bucket_name: e.g mys3bucket_name
        - s3_src_folder_path: e.g. OWP_ras_models/models (case-sensitive)
    Output
        - total size in MB to one decimal
    """

    try:
        if not s3_src_folder_path.endswith("/"):
            s3_src_folder_path += "/"

        s3_client = boto3.client("s3")
        total_size = 0  # in bytes

        default_kwargs = {"Bucket": bucket_name, "Prefix": s3_src_folder_path}

        next_token = ""

        while next_token is not None:
            updated_kwargs = default_kwargs.copy()
            if next_token != "":
                updated_kwargs["ContinuationToken"] = next_token

            # will limit to 1000 objects - hence tokens
            response = s3_client.list_objects_v2(**updated_kwargs)
            if response.get("KeyCount") > 0:
                contents = response.get("Contents")
                if contents is None:
                    raise Exception("s3 contents not did not load correctly")

                for result in contents:
                    total_size += result.get("Size")

            next_token = response.get("NextContinuationToken")

        if total_size > 0:
            # bytes to kb to mb rounded up nearest mb
            size_in_mg = total_size / 1028 / 1028
            total_size = round(size_in_mg, 1)

        return total_size

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("-----------------")
        RLOG.critical(
            "** Credentials not available for the submitted bucket. Try aws configure or review AWS "
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error finding files or folders in S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


# -------------------------------------------------
def is_valid_s3_folder(s3_full_folder_path):
    """
    Process: 
    Input:
        - s3_full_folder_path: eg. s3://ras2fim/OWP_ras_models
    """

    if s3_full_folder_path.endswith("/"):
        s3_full_folder_path = s3_full_folder_path[:-1]

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_full_folder_path.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]

    # will throw it's own exceptions if in error
    if does_s3_bucket_exist(bucket_name) is False:
        raise Exception(f"S3 bucket name of '{bucket_name}' does not exist")

    s3_folder_path = adj_s3_path.replace(bucket_name, "", 1)
    s3_folder_path = s3_folder_path.lstrip("/")

    client = boto3.client("s3")

    try:
        # If the bucket is incorrect, it will throw an exception that already makes sense
        # Don't need pagination as MaxKeys = 2 as prefix will likely won't trigger more than 1000 rec
        s3_objs = client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder_path, MaxKeys=2, Delimiter="/")

        # print(s3_objs)
        return (s3_objs["KeyCount"] > 0)

    except ValueError:
        # don't trap these types, just re-raise
        raise

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("** Credentials not available. Try aws configure")
    except Exception:
        RLOG.critical("An error has occurred with talking with S3")
        RLOG.critical(traceback.format_exc())

    return False


# -------------------------------------------------
def is_valid_s3_file(s3_full_file_path):
    """
    Process:  This will throw exceptions for all errors
    Input:
        - s3_full_file_path: eg. s3://ras2fim-dev/OWP_ras_models/my_models_catalog.csv
    Output:
        True/False (exists)
    """

    file_exists = False

    if s3_full_file_path.endswith("/"):
        raise Exception("s3 file path is invalid as it ends with as forward slash")

    RLOG.lprint(f"Validating s3 file of {s3_full_file_path}")

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_full_file_path.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]
    s3_file_path = adj_s3_path.replace(bucket_name + "/", "", 1)

    try:
        if does_s3_bucket_exist(bucket_name) is False:
            raise ValueError(f"s3 bucket of {bucket_name} does not appear to exist")

        client = boto3.client("s3")

        result = client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)

        if 'Contents' in result:
            file_exists = True

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("** Credentials not available. Try aws configure")
    except Exception:
        RLOG.critical("An error has occurred with talking with S3")
        RLOG.critical(traceback.format_exc())

    return file_exists


# -------------------------------------------------
def does_s3_bucket_exist(bucket_name):
    client = boto3.client("s3")

    try:
        client.head_bucket(Bucket=bucket_name)
        # resp = client.head_bucket(Bucket=bucket_name)
        # print(resp)

        return True  # no exception?  means it exist

    except botocore.exceptions.NoCredentialsError:
        RLOG.critical("** Credentials not available for submitted bucket. Try aws configure")
        sys.exit(1)

    except client.exceptions.NoSuchBucket:
        return False

    except ClientError as ce:
        RLOG.critical(f"** An error occurred while talking to S3. Details: {ce}")
        sys.exit(1)

    # other exceptions can be passed through

# -------------------------------------------------
def parse_unit_folder_name(unit_folder_name):
    """
    Overview:
        While all uses of this function pass back errors if invalid the calling code can decide if it is
        an exception. Sometimes it doesn't, it just want to check to see if the key is a huc crs key.

    Input:
        unit_folder_name: migth be a full s3 string, or full local the folder name
           e.g.  s3://xzy/output_ras2fim/12090301_2277_ble_230811
           or c://my_ras_data/output_ras2fim/12090301_2277_ble_230811

    Output:
        A dictionary with records of:
                       key_huc,
                       key_crs_number,
                       key_source_code,
                       key_date_as_str (date string eg: 230811),
                       key_date_as_dt  (date obj for 230811)
                       unit_folder_name (12090301_2277_ble_230811) (cleaned version)
        OR
        If in error, dictionary will have only one key of "error", saying why it the
           reason for the error. It lets the calling code to decide if it wants to raise
           and exception.
           Why? There are some incoming folders that will not match the pattern and the
           calling code will want to know that and may just continue on

        BUT: if the incoming param doesn't exist, that raises an exception
    """

    rtn_dict = {}

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
    # see the first chars are an 8 digit number and that it has three underscores (4 segs)
    # and will split it to a list of tuples
    segs = unit_folder_name.split("_")
    if len(segs) != 4:
        rtn_dict[
            "error"
        ] = "Expected four segments split by three underscores e.g. 12090301_2277_ble_230811"
        return rtn_dict

    key_huc = segs[0]
    key_crs = segs[1]
    key_source_code = segs[2]
    key_date = segs[3]

    if (not key_huc.isnumeric()) or (not key_crs.isnumeric()) or (not key_date.isnumeric()):
        rtn_dict["error"] = f"The unit folder name of {unit_folder_name} is invalid."
        " The pattern should look like '12090301_2277_ble_230811' for example."
        return rtn_dict

    if len(key_huc) != 8:
        rtn_dict["error"] = "The first part of the four segments (huc), is not 8 digits long"
        return rtn_dict

    if (len(key_crs) < 4) or (len(key_crs) > 6):
        rtn_dict["error"] = "Second part of the four segments (crs) is not"
        " between 4 and 6 digits long"
        return rtn_dict

    if len(key_date) != 6:
        rtn_dict["error"] = "Last part of the four segments (date) is not 6 digits long"
        return rtn_dict

    # test date format
    # format should come in as yymmdd  e.g. 230812
    # If successful, the actual date object be added
    dt_key_date = None
    try:
        dt_key_date = datetime.strptime(key_date, "%y%m%d")
    except Exception:
        # don't log it
        rtn_dict["error"] = (
            "Last part of the four segments (date) does not appear"
            " to be in the pattern of yymmdd eg 230812"
        )
        return rtn_dict

    rtn_dict["key_huc"] = key_huc
    rtn_dict["key_crs_number"] = key_crs
    rtn_dict["key_date_as_str"] = key_date
    rtn_dict["key_date_as_dt"] = dt_key_date
    rtn_dict["unit_folder_name"] = unit_folder_name  # cleaned version

    return rtn_dict

# -------------------------------------------------
def parse_bucket_and_folder_name(s3_full_folder_path):

    """
    Process: 
    Input:
        - s3_full_folder_path: eg. s3://ras2fim/OWP_ras_models/models
    Returns:
        A tuple:  bucket name, s3_folder_path
    """

    if s3_full_folder_path.endswith("/"):
        s3_full_folder_path = s3_full_folder_path[:-1]

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_full_folder_path.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]

    # will throw it's own exceptions if in error
    if does_s3_bucket_exist(bucket_name) is False:
        raise Exception(f"S3 bucket name of '{bucket_name}' does not exist")

    s3_folder_path = adj_s3_path.replace(bucket_name, "", 1)
    s3_folder_path = s3_folder_path.lstrip("/")

    return bucket_name, s3_folder_path

