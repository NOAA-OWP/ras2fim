#!/usr/bin/env python3

import os
import sys

import argparse
import boto3
import botocore.exceptions

from datetime import datetime

sys.path.append('..')
import ras2fim.src.shared_variables as sv
import ras2fim.src.r2f_validators as val


####################################################################
def save_output_to_s3(src_path_to_huc_crs_output_dir, 
                      s3_output_ras2fim_path,
                      s3_output_ras2fim_archive_path):
    
    start_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    print("")
    print( "=================================================================")
    print( "|          RUN ras2fim_to_s3                                    |")
    print(f"|   Saving HUC/CRS output folder to S3 start: {dt_string}       |")
    print( "=================================================================")

    # --------------------
    # validate input variables and setup key variables
    varibles_dict = __validate_input(src_path_to_huc_crs_output_dir,
                                     s3_output_ras2fim_path,
                                     s3_output_ras2fim_archive_path)

    src_path =  varibles_dict["huc_crs_full_path"]
    bucket_name = varibles_dict["bucket_name"]
    s3_folder_path = varibles_dict["s3_folder_path"] # excluding "s3:// and bucket"
    arc_s3_folder_path = varibles_dict["arc_s3_folder_path"] # excluding "s3:// and bucket"
    huc_crs_folder_name = varibles_dict["huc_crs_dir"] # should have the \ stripped off

    print("")
    print(f" --- s3 folder path is {s3_output_ras2fim_path}")
    print(f" --- S3 archive folder path is {s3_output_ras2fim_archive_path}")        
    print(f" --- huc_crs_full_path is {src_path}")
    print(f" --- huc_crs_folder_name is {huc_crs_folder_name}")
    print("===================================================================")
    print("")

    if (s3_folder_path.endswith('/')):
        s3_folder_path = s3_folder_path[:-1]

    # --------------------
    # we need to see if the directory already exists and possibly move it
    __check_existing_s3_folder(bucket_name, s3_folder_path, huc_crs_folder_name)

    # --------------------
    # Upload the folder and contents
    __upload_output_folder(src_path, bucket_name, s3_folder_path, huc_crs_folder_name)

    # --------------------
    print()    
    print("===================================================================")
    print("Copy to S3 Complete")
    end_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"Ended: {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()


####################################################################
def __upload_output_folder(src_path, bucket_name, s3_folder_path, huc_crs_folder_name):

    src_path, s3_folder_path, huc_crs_folder_name
    s3 = boto3.client('s3')

    for subdir, dirs, files in os.walk(src_path):
        for file in files:
            print("-----------------")
            #print(f".. src file = {file}")
            src_file_path = os.path.join(src_path, subdir, file)     
            print(f".. src file_path = {src_file_path}")
            src_ref_path = subdir.replace(src_path, '')

            # switch the slash            
            src_ref_path = src_ref_path.replace('\\', '/')
            #s3_key_path = s3_folder_path + src_ref_path + '/' + file
            s3_key_path = f"{s3_folder_path}/{huc_crs_folder_name}/{src_ref_path}/{file}"
            # safety feature in case we have more than one foreward slash as that can
            # be a mess in S3 (it will honor all slashs)
            s3_key_path = s3_key_path.replace('//', '/')

            print(f".. s3 target path is s3://{bucket_name}/{s3_key_path}")
            
            try:
                with open(src_file_path, 'rb') as data:
                    #s3.Bucket(bucket_name).put_object(Key=s3_key_path, Body=data)
                    s3.upload_file(src_file_path, bucket_name, s3_key_path)
                    print(".... File uploaded")
            except FileNotFoundError:
                print("** The file was not found")
            except botocore.exceptions.NoCredentialsError:
                print("** Credentials not available. Try aws configure.")
            except Exception as ex:
                print(f"** Error uploading file to S3: {ex}")


####################################################################
def __check_existing_s3_folder(bucket_name, s3_folder_path, huc_crs_folder_name):
    """
    Processing Steps:
      - Strip off the final folder from the s3_folder_path.  ie) output_ras2fim/12090301_2277_230721 
          now: down to output_ras2fim
      - Load all first level folder names from that folder. ie) output_ras2fim
      - See if there are any folder names that exact match including date and error if true.
      - Strip off the date from the HUC/CRS folder name. ie) 12090301_2277
      - Create a dictionary of all existing folder names matching that pattern.
      - Strip off the dates from each of those files names.
      - If the incoming date is older than any pre-existing one, error out
      - If the incoming date is newer that all pre-existing ones, then move the pre-existing
        one (or ones) to the archive folder.

        Net results: Only one folder with the HUC/CRS combo an exist in the s3 output folder name
           and older ones are all in archives.
    
       - If we find some that need to be moved, ask the user to confirm before contining (or abort)
    """




####################################################################
####  Some validation of input, but also creating key variables ######
def __validate_input(path_to_huc_crs_output_dir, s3_output_ras2fim_path, s3_output_ras2fim_archive_path):

    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    rtn_varibles_dict = {}

    #---------------
    # why is this here? might not come in via __main__
    if (path_to_huc_crs_output_dir == ""):
        raise ValueError("Source huc_crs_output parameter value can not be empty")

    #---------------
    path_to_huc_crs_output_dir = path_to_huc_crs_output_dir.replace('/', '\\')
    path_segs = path_to_huc_crs_output_dir.split('\\')
    # We need the source huc_crs folder name for later and the full path
    if (len(path_segs) == 1):
        rtn_varibles_dict["huc_crs_dir"] = path_segs[0]
        huc_crs_parent_dir = sv.R2F_DEFAULT_OUTPUT_MODELS
        rtn_varibles_dict["huc_crs_full_path"] = os.path.join(huc_crs_parent_dir, rtn_varibles_dict["huc_crs_dir"])
    else:
        rtn_varibles_dict["huc_crs_dir"] = path_segs[-1]
        # strip of the parent path
        rtn_varibles_dict["huc_crs_full_path"] = path_to_huc_crs_output_dir

    if (not os.path.exists(rtn_varibles_dict["huc_crs_full_path"])):
        raise ValueError(f"Source HUC/CRS folder not found at {rtn_varibles_dict['huc_crs_full_path']}")

    # --------------------
    # make sure it has a "final" folder and has some contents
    final_dir = os.path.join(rtn_varibles_dict["huc_crs_full_path"], sv.R2F_OUTPUT_DIR_FINAL)
    if (not os.path.exists(final_dir)):
        raise ValueError(f"Source HUC/CRS 'final folder' not found at {final_dir}." \
                         " Ensure ras2fim has been run to completion.")

    # check to see that it isn't empty
    file_count = len(os.listdir(final_dir))
    if (file_count == 0):
        raise ValueError(f"Source HUC/CRS 'final folder' at {final_dir}" \
                         " does not appear to have any files or folders.")

    # --------------------
    # check ras2fim output bucket exists
    # why is this here? might not come in via __main__    
    if (s3_output_ras2fim_path == ""):
        raise ValueError("s3_output_ras2fim_path parameter value can not be empty")
  
    print()
    print(f"Validating S3 output folder of {s3_output_ras2fim_path}")
    bucket_name, s3_folder_path = val.is_valid_s3_folder(s3_output_ras2fim_path)
    print(".... found")
    rtn_varibles_dict["bucket_name"] = bucket_name
    rtn_varibles_dict["s3_folder_path"] = s3_folder_path

    # --------------------
    # check ras2fim archive bucket folder
    # why is this here? might not come in via __main__    
    if (s3_output_ras2fim_path == ""):
        raise ValueError("s3_output_ras2fim_path parameter value can not be empty")

    print()
    print(f"Validating S3 output archive folder of {s3_output_ras2fim_archive_path}")
    arc_bucket_name, arc_s3_folder_path = val.is_valid_s3_folder(s3_output_ras2fim_archive_path)
    print(".... found")    

    if (bucket_name.lower() != arc_bucket_name.lower()):
        raise ValueError("Both S3 paths must point to the same S3 bucket")

    rtn_varibles_dict["arc_s3_folder_path"] = arc_s3_folder_path

    return rtn_varibles_dict

if __name__ == '__main__':

    # TODO: Add samples

    parser = argparse.ArgumentParser(description='Pushing ras2fim HUC/CRS output folders back to S3',
                                     formatter_class=argparse.RawTextHelpFormatter)

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
    # Note: the actual models folders are assumed to be a folder named "models" beside the models_catalog.csv
    parser.add_argument('-s', '--src_path_to_huc_crs_output_dir', 
                        help='REQUIRED: Can be used in two ways:\n' \
                             '1) Add just the output huc_crs folder name (assumed default pathing)\n' \
                             '2) A full defined path including output huc_crs folder',
                        required=True, metavar='')
    
    parser.add_argument('-t', '--s3_output_ras2fim_path',
                        help='REQUIRED: S3 path for where output ras2fim folders are placed.',
                        required=True, metavar='')

    parser.add_argument('-a', '--s3_output_ras2fim_archive_path', 
                        help='REQUIRED: S3 path for where output ras2fim folders are placed.',
                        required=True, metavar='')    

    args = vars(parser.parse_args())

    save_output_to_s3(**args)