#!/usr/bin/env python3

import fnmatch
import os
import sys
import traceback
from concurrent import futures
from datetime import datetime

import boto3
import botocore.exceptions
import colored as cl
from botocore.client import ClientError


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import ras2fim_logger
import shared_variables as sv


# Global Variables
RLOG = ras2fim_logger.R2F_LOG


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
                RLOG.lprint(f".... File uploaded {src_path} as {s3_full_target_path}/{s3_file_name}")

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


###################################################################
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
    RLOG.lprint(
        f"{cl.fg('light_yellow')}"
        f"Uploading folder from {src_path}  to  {s3_full_target_path}"
        f"{cl.attr(0)}"
    )
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
            RLOG.lprint(
                f"{cl.fg('red_1')}" f"No files in source folder of {src_path}. Upload invalid" f"{cl.attr(0)}"
            )
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

    RLOG.lprint("===================================================================")
    print("")
    RLOG.lprint(
        f"{cl.fg('light_yellow')}" f"Deleting the files and folders at {s3_full_target_path}" f"{cl.attr(0)}"
    )
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
            RLOG.lprint(
                f"{cl.fg('red_1')}"
                f"No files in s3 folder of {s3_full_target_path} to be deleted"
                f"{cl.attr(0)}"
            )
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
        RLOG.lprint("===================================================================")
        print("")
        RLOG.lprint(
            f"{cl.fg('light_yellow')}"
            f"Moving folder from {s3_src_folder_path}  to  {s3_target_folder_path}"
            f"{cl.attr(0)}"
        )
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
            RLOG.lprint(
                f"{cl.fg('red_1')}"
                f"No files in source folder of {s3_src_folder_path}. Move invalid"
                f"{cl.attr(0)}"
            )
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
            "permissions options"
        )
        sys.exit(1)

    except Exception as ex:
        RLOG.critical("-----------------")
        RLOG.critical("** Error moving folders in S3:")
        RLOG.critical(traceback.format_exc())
        raise ex


####################################################################
def get_records_list(bucket_name, s3_src_folder_path, search_key, is_verbose=True):
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
            RLOG.lprint(
                f"{cl.fg('light_yellow')}"
                f"Searching files and folders in s3://{bucket_name}/{s3_src_folder_path}"
                f" based on search key of '{search_key}'.\n"
                " This may take a few minutes depending on size of the search folder"
                f"{cl.attr(0)}"
            )
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


####################################################################
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
                f"Searching for folder names in s3://{bucket_name}/{s3_src_folder_path} (non-recursive)\n"
                "This may take a few minutes depending on number of folders in the search folder"
                f"{cl.attr(0)}"
            )
            print("")

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


####################################################################
def download_folders(
    s3_src_parent_path: str, local_parent_folder: str, df_folder_list, df_download_column_name: str
):
    """
    Process:
        - Use the incoming list of S3 folder names, just simple s3 non-pathed folder names and
        all of the files inside it will be downloaded.
        - The list needs to be case-sensitive.
        - This method is multi-threaded (not multi-proc) for performance.
        - If the local_folders_already exist, it will not pre-clean the folders so it is
        encouraged to pre-delete the child folders if required.
        - Because it only needs a s3 parent name and a list of child folders to download,
        you could in theory do this: src_parent_folder = "rob_folder"
        and list_folder_names = "all my models" (spaces allowed)
    Inputs:
        - s3_src_parent_path: the full s3 path to the parent folder name.
          eg. s3://mybucket/OWP_ras_models/models/, then all folders listed in the df child folder
          name will be downloaded recursively

        - df_folder_list: A dataframe which includes one column for folder names to be downloaded
          ie). 1293240_Old Channel EFT River_g01_1689773308
               1293222_Buffalo Creek_g01_1690618796

        - df_download_column_name: The column name in the dataframe to be used as the folder name
          value in S3 to download. ie) final_name_key

        - local_parent_folder: e.g. c:/ras2fim_data/OWP_ras_models/models_12090301_full
             All files / folders will be added under that defined parent.
             eg. c:/ras2fim_data/OWP_ras_models/models_12090301_full/file 1.txt
             and c:/ras2fim_data/OWP_ras_models/models_12090301_full/rob subfolder/file3.txt
    Output
        - The dataframe will be updated and returned.
             - Two columns will be added if they do not already exist
                - "download_success" (see sv.COL_NAME_DOWNLOAD_SUCCESS) as either
                   the string value of 'True' or 'False'
                - "error_details" (see sv.COL_NAME_ERROR_DETAILS) - why did it fail

    """

    # nested function
    def __download_folder(bucket_name, folder_id, s3_src_folder, target_local_folder):
        # send in blank search key
        s3_items = get_records_list(bucket_name, s3_src_folder, "", False)

        if len(s3_items) == 0:
            result = {"folder_id": folder_id, "is_success": False, "err_msg": "no s3 files found"}
            RLOG.warning(f"{folder_id} -- downloaded failure -- reason: no s3 files found")
            return result

        try:
            s3_client = boto3.client('s3')

            for s3_item in s3_items:
                # need to make the src file to be the full url minus the s:// and bucket name
                src_file = f"{s3_src_folder}/{s3_item['key']}"
                trg_file = os.path.join(target_local_folder, s3_item["key"])

                # Why extract the directory name? the key might have subfolder
                # names right in the key
                trg_path = os.path.dirname(trg_file)

                # folders may not yet exist
                if not os.path.exists(trg_path):
                    os.makedirs(trg_path)

                with open(trg_file, 'wb') as f:
                    s3_client.download_fileobj(Bucket=bucket_name, Key=src_file, Fileobj=f)

            result = {"folder_id": folder_id, "is_success": True, "err_msg": ""}
            RLOG.lprint(f"{folder_id} -- downloaded success")

        except Exception as ex:
            result = {"folder_id": folder_id, "is_success": False, "err_msg": ex}
            RLOG.error(f"{folder_id} -- downloaded failure -- reason: {ex}")

        return result

    # strip off last forwward slash if it is there
    if s3_src_parent_path.endswith("/"):
        s3_src_parent_path = s3_src_parent_path[:-1]

    # Checks both bucket and child path exist
    # Will raise it's own exceptions if needed
    # s3_folder_path is the URL with the "s3://" and bucket names stripped off
    bucket_name, s3_folder_path = is_valid_s3_folder(s3_src_parent_path)

    # See if the df already has the two download colums and add them if required.
    # We will update the values later.
    if sv.COL_NAME_DOWNLOAD_SUCCESS not in df_folder_list.columns:
        df_folder_list[sv.COL_NAME_DOWNLOAD_SUCCESS] = False  # default

    if sv.COL_NAME_ERROR_DETAILS not in df_folder_list.columns:
        df_folder_list[sv.COL_NAME_ERROR_DETAILS] = ""

    # With each valid record found, we will add a dictionary record that can be passed
    # as arguments to find files and download them for a folder
    list_folders = []

    for ind, row in df_folder_list.iterrows():
        folder_name = row[df_download_column_name]
        # ensure the value does not already exist in the list.
        if folder_name in list_folders:
            raise Exception(
                f"The value of {folder_name} exists at least twice"
                f" in the {df_download_column_name} column"
            )

        # Ensure the error column does not already have an error msg in it.
        # Unlikely but possible. (pre-tests of model records?)
        err_val = row[sv.COL_NAME_ERROR_DETAILS]
        if err_val is not None and err_val.strip() != "":
            msg = f"{folder_name} -- skipped download:  error already existed in the"
            f"{sv.COL_NAME_ERROR_DETAILS} column"
            RLOG.warning(msg)
            row[sv.COL_NAME_ERROR_DETAILS] = msg
            continue

        s3_src_child_full_path = f"{s3_folder_path}/{folder_name}"
        local_child_full_path = os.path.join(local_parent_folder, folder_name)

        # yes.. use the folder name as the ID
        item = {
            "bucket_name": bucket_name,
            "folder_id": folder_name,
            "s3_src_folder": s3_src_child_full_path,
            "target_local_folder": local_child_full_path,
        }

        list_folders.append(item)

    if len(list_folders) == 0:
        msg = f"No models in src folder of {s3_src_parent_path} were downloaded."
        " This might be that the folders did not exist, or they were ineligible."
        raise Exception(msg)

    rtn_threads = []

    try:
        # As we are threading, we can add more than one thread per proc, but for calc purposes
        # and to not overload the systems or internet pipe, so it is hardcoded at max of 20 for now.
        num_workers = 20
        total_cpus_available = os.cpu_count()  # yes.. it is MT, so you don't need a -2 margin
        if total_cpus_available < num_workers:
            num_workers = total_cpus_available

        RLOG.notice(f"Number of folders to be downloaded is {len(list_folders)}")
        print(" ... This may take a few minutes, stand by")
        RLOG.lprint(f" ... downloading with {num_workers} workers")

        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures_dict = []

            for download_args in list_folders:
                futures_dict.append(executor.submit(__download_folder, **download_args))

            for future_result in futures.as_completed(futures_dict):
                result = future_result.result()
                future_exception = future_result.exception()

                if not future_exception:
                    rtn_threads.append(result)
                else:
                    RLOG.critical(f"Error occurred with {futures_dict}")
                    raise future_exception

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

    for result in rtn_threads:
        folder_id = result['folder_id']
        # print(f"{result['folder_id']} - downloaded: {result['is_success']} - err: {result['err_msg']}")
        mask = df_folder_list.final_name_key == folder_id
        df_folder_list.loc[mask, sv.COL_NAME_DOWNLOAD_SUCCESS] = result['is_success']
        df_folder_list.loc[mask, sv.COL_NAME_ERROR_DETAILS] = result['err_msg']  # might be empty

    return df_folder_list


####################################################################
def download_folder(bucket_name, folder_id, s3_src_folder, target_local_folder):
    # TODO (Nov 22, 2023 - add arg validation)
    """
    Process:
        - Using the incoming s3 src folder, call get_records to get a list of child folders and files
        - Open a s3 client and iterate through the files to download
    Output:
        - A dictionary with three records
            - folder_id : the incoming folder id (for reference purposes)
            - is_success: True or False (did it download anything successfully)
            - err_msg: If it failed.. why did it fail, otherwise empty string
    """

    # send in blank search key
    s3_items = get_records_list(bucket_name, s3_src_folder, "", False)

    if len(s3_items) == 0:
        result = {"folder_id": folder_id, "is_success": False, "err_msg": "no s3 files found"}
        return result

    try:
        s3_client = boto3.client('s3')

        for s3_item in s3_items:
            # need to make the src file to be the full url minus the s:// and bucket name
            src_file = f"{s3_src_folder}/{s3_item['key']}"
            trg_file = os.path.join(target_local_folder, s3_item["key"])

            # Why extract the directory name? the key might have subfolder
            # names right in the key
            trg_path = os.path.dirname(trg_file)

            # folders may not yet exist
            if not os.path.exists(trg_path):
                os.makedirs(trg_path)

            with open(trg_file, 'wb') as f:
                s3_client.download_fileobj(Bucket=bucket_name, Key=src_file, Fileobj=f)

        result = {"folder_id": folder_id, "is_success": True, "err_msg": ""}

    except Exception as ex:
        result = {"folder_id": folder_id, "is_success": False, "err_msg": ex}

    return result


####################################################################
def is_valid_s3_folder(s3_full_folder_path):
    """
    Process:  This will throw exceptions for all errors
    Input:
        - s3_bucket_and_folder: eg. s3://ras2fim/OWP_ras_models
    Output:
        bucket_name, s3_folder_path
    """

    if s3_full_folder_path.endswith("/"):
        s3_full_folder_path = s3_full_folder_path[:-1]

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_full_folder_path.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]
    s3_folder_path = adj_s3_path.replace(bucket_name, "", 1)
    s3_folder_path = s3_folder_path.lstrip("/")

    client = boto3.client("s3")

    try:
        if does_s3_bucket_exist(bucket_name) is False:
            raise ValueError(f"s3 bucket of {bucket_name} does not appear to exist")

        # If the bucket is incorrect, it will throw an exception that already makes sense
        # Don't need pagination as MaxKeys = 2 as prefix will likely won't trigger more than 1000 rec
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
        RLOG.critical("** Credentials not available. Try aws configure")
    except Exception:
        RLOG.critical("An error has occurred with talking with S3")
        RLOG.critical(traceback.format_exc())

    return bucket_name, s3_folder_path


####################################################################
def is_valid_s3_file(s3_full_file_path):
    """
    Process:  This will throw exceptions for all errors
    Input:
        - s3_full_file_path: eg. s3://ras2fim-dev/OWP_ras_models/my_models_catalog.csv
    Output:
        True/False (exists)
    """

    file_exists = True

    # TODO: Finish this

    return file_exists


####################################################################
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
        rtn_varibles_dict["error"] = "Does not contain any underscore or folder name to short"
        return rtn_varibles_dict

    segs = unit_folder_name.split("_")
    if len(segs) != 3:
        rtn_varibles_dict[
            "error"
        ] = "Expected three segments split by two underscores e.g. 12090301_2277_230811"
        return rtn_varibles_dict

    key_huc = segs[0]
    key_crs = segs[1]
    key_date = segs[2]

    if (not key_huc.isnumeric()) or (not key_crs.isnumeric()) or (not key_date.isnumeric()):
        rtn_varibles_dict["error"] = "All three segments are expected to be numeric"
        return rtn_varibles_dict

    if len(key_huc) != 8:
        rtn_varibles_dict["error"] = "First part of the three segments (huc) is not 8 digits long"
        return rtn_varibles_dict

    if (len(key_crs) < 4) or (len(key_crs) > 6):
        rtn_varibles_dict["error"] = (
            "Second part of the three segments (crs) is not" " between 4 and 6 digits long"
        )
        return rtn_varibles_dict

    if len(key_date) != 6:
        rtn_varibles_dict["error"] = "Last part of the three segments (date) is not 6 digits long"
        return rtn_varibles_dict

    # test date format
    # format should come in as yymmdd  e.g. 230812
    # If successful, the actual date object be added
    dt_key_date = None
    try:
        dt_key_date = datetime.strptime(key_date, "%y%m%d")
    except Exception:
        # don't log it
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
