#!/usr/bin/env python3

import os
import sys

import boto3
import botocore.exceptions

from botocore.client import ClientError
from datetime import datetime
from tqdm import tqdm


####################################################################
def upload_output_folder_to_s3(src_path, 
                               bucket_name, 
                               s3_folder_path, 
                               huc_crs_folder_name, 
                               is_verbose):

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
        client = boto3.client('s3')

        # we are going to walk it twice, once to get a file count, the other for TQDM processing
        file_count = 0
        for subdir, dirs, files in os.walk(src_path):
            file_count += len(files)

        with tqdm(total = file_count,
                desc = f"Uploading {file_count} files to S3",
                bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                ncols = 80,
                disable = is_verbose) as pbar:

            for subdir, dirs, files in os.walk(src_path):
                for file in files:

                    #print(f".. src file = {file}")
                    src_file_path = os.path.join(src_path, subdir, file)     
                    if (is_verbose):
                        print("-----------------")
                        print(f".. src file_path = {src_file_path}")

                    src_ref_path = subdir.replace(src_path, '')

                    # switch the slash            
                    src_ref_path = src_ref_path.replace('\\', '/')
                    #s3_key_path = s3_folder_path + src_ref_path + '/' + file
                    s3_key_path = f"{s3_folder_path}/{huc_crs_folder_name}/{src_ref_path}/{file}"
                    # safety feature in case we have more than one foreward slash as that can
                    # be a mess in S3 (it will honor all slashs)
                    s3_key_path = s3_key_path.replace('//', '/')

                    s3_full_target_path = f"s3://{bucket_name}/{s3_key_path}"
                    if (is_verbose):
                        print(f".. s3 target path is {s3_full_target_path}")
                    
                    try:
                        with open(src_file_path, 'rb') as data:
                            #s3.Bucket(bucket_name).put_object(Key=s3_key_path, Body=data)
                            client.upload_file(src_file_path, bucket_name, s3_key_path)
                            if (is_verbose):
                                print(".... File uploaded")

                    except FileNotFoundError:
                        print("-----------------")
                        print(f"** The file {src_file_path} was not found")
                        print(f"** This is considered a critical error as the file name was" \
                            " found programatically.")
                        sys.exit(1)                

                    pbar.update(1)


    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print("** Credentials not available for the submitted bucket. Try aws configure or review AWS " \
              "permissions options.")
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print(f"** Error uploading files to S3:")
        raise ex


####################################################################
def delete_s3_folder(bucket_name, 
                     s3_folder_path, 
                     is_verbose):

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

        client = boto3.client('s3')

        # Files inside the folder (yes.. technically there are no folders)
        # but it is possible to a folder that is empty (prefix with no keys)
        s3_files = client.list_objects(Bucket=bucket_name, Prefix=s3_folder_path + '/')

        file_count = len(s3_files)

        if (file_count == 0): # no files to delete but it is possible to have an empty folder
            client.delete_object(Bucket=bucket_name, Key=s3_folder_path + '/')  # slash added

        with tqdm(total = file_count,
                  desc = f"Deleting {file_count} files from S3",
                  bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                  ncols = 80,
                  disable = is_verbose) as pbar:

            # Delete all objects in the folder
            for s3_file in s3_files['Contents']:
                client.delete_object(Bucket=bucket_name, Key=s3_file['Key'])

            pbar.update(1)

        # remove the folder that is left over
        client.delete_object(Bucket=bucket_name, Key=s3_folder_path + '/')  # slash added

    except botocore.exceptions.NoCredentialsError:
        print("-----------------")
        print("** Credentials not available. Try aws configure or review AWS permissions options.")
        sys.exit(1)

    except Exception as ex:
        print("-----------------")
        print(f"** Error deleting files at S3: {ex}")
    raise ex


####################################################################
def is_valid_s3_folder(s3_bucket_and_folder):

    # This will throw exceptions for all errors

    if s3_bucket_and_folder.endswith('/'):
        s3_bucket_and_folder = s3_bucket_and_folder[:-1]

    # we need the "s3 part stripped off for now" (if it is even there)
    adj_s3_path = s3_bucket_and_folder.replace("s3://", "")
    path_segs = adj_s3_path.split("/")
    bucket_name = path_segs[0]
    s3_folder_path = adj_s3_path.replace(bucket_name, '', 1)
    s3_folder_path = s3_folder_path.lstrip('/')

    client = boto3.client('s3')
    
    try:    
        # If the bucket is incorrect, it will throw an exception that already makes sense
        s3_objs = client.list_objects_v2(Bucket = bucket_name,
                                     Prefix = s3_folder_path,
                                     MaxKeys = 2,
                                     Delimiter = '/')
        
        # assumes the folder exists and has values ??
        #print(s3_objs)
        if (s3_objs["KeyCount"] == 0):
            raise ValueError("S3 bucket exists but the folder path does not exist and is required. "\
                            "Path is case-sensitive")

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

    client = boto3.client('s3')

    try:
        resp = client.head_bucket(Bucket=bucket_name)
        #print(resp)

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
        A five tuple: (huc, 
                       crs,
                       date string eg: 230811,
                       a date obj for 230811,
                       huc_crs_folder_name)
        OR
        If in error, the first part of the tuple will be the word "error", and the second part is the
           reason for the error.
    """

    if (huc_crs_folder_name == ""):
        raise ValueError("huc_crs_folder_name can not be empty")

    # cut off the s3 part if there is any.
    huc_crs_folder_name = huc_crs_folder_name.replace("s3://", "")

    # s3_folder_path and we want to strip the first one only. (can be deeper levels)
    if huc_crs_folder_name.endswith('/'):    
        huc_crs_folder_name = huc_crs_folder_name[:-1] # strip the ending slash

    # see if there / in it and split out based on the last one (migth not be one)
    huc_crs_folder_segs = huc_crs_folder_name.rsplit("/", 1)
    if (len(huc_crs_folder_segs) > 1):
        huc_crs_folder_name = huc_crs_folder_segs[-1]

    # The best see if it has an underscore in it, split if based on that, then
    # see the first chars are an 8 digit number and that it has two underscores (3 segs)
    # and will split it to a list of tuples
    if ("_" not in huc_crs_folder_name) or (len(huc_crs_folder_name) < 9):
        return ("error", "Does not contain any underscore or folder name to short.")

    segs = huc_crs_folder_name.split('_')
    if (len(segs) != 3):
        return ("error", "Expected three segments split by two underscores. e.g . 12090301_2277_230811")
    key_huc = segs[0]
    key_crs = segs[1]
    key_date = segs[2]
    
    if (not key_huc.isnumeric()) or (not key_crs.isnumeric()) or (not key_date.isnumeric()):
        return ("error", "All three segments are expected to be numeric.")
    
    if (len(key_huc) != 8):
        return ("error", "First part of the three segments (huc) is not 8 digits long")

    if (len(key_crs) < 4) or (len(key_crs) > 6):
        return ("error", "Second part of the three segments (crs) is not between 4 and 6 digits long")

    if (len(key_date) != 6):
        return ("error", "Last part of the three segments (date) is not 6 digits long")
    
    # test date format 
    # format should come in as yymmdd  e.g. 230812
    # If successful, the actual date object be added
    dt_key_date = None
    try:
        dt_key_date = datetime.strptime(key_date, '%y%m%d')
    except:
        return ("error", "Last part of the three segments (date) does not appear" \
                " to be in the pattern of yymmdd eg 230812")
    
    return (key_huc, key_crs, key_date, dt_key_date, huc_crs_folder_name)
