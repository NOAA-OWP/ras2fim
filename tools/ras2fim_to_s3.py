#!/usr/bin/env python3

import os
import sys

import argparse
import boto3

from datetime import datetime

sys.path.append('..')
import ras2fim.src.shared_variables as sv
import ras2fim.src.shared_functions as sf
import s3_shared_functions as s3_sf


####################################################################
def save_output_to_s3(src_path_to_huc_crs_output_dir, 
                      s3_bucket_name,
                      is_verbose):
    
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
                                     s3_bucket_name)


    # why have this come in from variables_dict? the src_path_to_huc_crs_output_dir can come in not 
    # fully pathed, just the huc/crs folder in the default folder path
    src_path =  varibles_dict["huc_crs_full_path"] # eg. c:\ras2fim_data\output_ras2fim\12030202_102739_230810
    s3_full_output_ras2fim_folder = varibles_dict["s3_full_output_ras2fim_folder"] # e.g. s3://xyz/output_ras2fim
    s3_full_archive_path = varibles_dict["s3_full_archive_path"] # e.g. s3://xyz/output_ras2fim_archive
    huc_crs_folder_name = varibles_dict["huc_crs_dir"] # eg. 12030202_102739_230810
    s3_full_huc_crs_path = f"{s3_full_output_ras2fim_folder}/{huc_crs_folder_name}"

    print("")
    print(f" --- s3 folder target path is {s3_full_huc_crs_path}")
    print(f" --- S3 archive folder path is {s3_full_archive_path}")        
    print(f" --- huc_crs_full_path is {src_path}")
    print(f" --- huc_crs_folder_name is {huc_crs_folder_name}")
    print("===================================================================")
    print("")

    # --------------------
    # We need to see if the directory already exists in s3.
    # Depending on what we find will tell us where to uploading the incoming folder
    # and what to do with pre-existing if there are any pre-existing folders
    # matching the huc/crs.
    __upload_to_ras2fim_s3(s3_bucket_name, 
                           src_path,
                           s3_full_output_ras2fim_folder,
                           s3_full_archive_path,
                           huc_crs_folder_name,
                           is_verbose)

    # --------------------
    # Upload the folder and contents
    #__upload_output_folder(src_path,
    #                       bucket_name, 
    #                       s3_folder_path, 
    #                       huc_crs_folder_name,
    #                       is_verbose)

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
def __upload_to_ras2fim_s3(bucket_name, 
                           src_path,
                           s3_full_output_ras2fim_folder, 
                           s3_full_archive_path, 
                           huc_crs_folder_name,
                           is_verbose):
    """
    Processing Steps:
      - Load all first level folder names from that folder. ie) output_ras2fim
      - Get a list of tuples for any S3 folder names that match the huc and crs value

      
    #TODO: REDO THIS NOTES

      - using the dates for each of the existing s3 huc_crs:
          - If the incoming date is older or equal to than any pre-existing one, error out
          - If the incoming date is newer that all pre-existing ones, then move the pre-existing
            one (or ones) to the archive folder.

        Net results: Only one folder with the HUC/CRS combo an exist in the s3 output folder name
           and older ones are all in archives.
    
       - If we find some that need to be moved, ask the user to confirm before contining (or abort)

    Input
        - bucket_name: e.g xyz
        - s3_full_output_ras2fim_folder:  e.g s3://xyz/output_ras2fim
        - s3_full_archive_path: eg s3://xyz/output_ras2fim_archive
        - huc_crs_folder_name:  12030105_2276_230810

        Note: s3_full_output_ras2fim_folder and s3_full_archive_path are generally for display 
        purposes only, as we can't draw logic on them. We will use the bucket_name variable
        and the shared_variable values.
    """

    print("===================================================================")
    print("Checking existing s3 folders for folders starting with same huc number" \
          " and crs value")
    print("")
    print("** NOTE: The intention is that only one HUC/CRS output folder, usually the most current," \
          " is kept in the offical output_ras2fim folder. All HUC/CRS folders in that s3 folder will be" \
          " included for ras2releases, and duplicate HUC/CRS foldes are likely undesirable.")
    print("")

    #---------------
    # splits it a three part tuple (huc, crs, date (as string), a date object of the key date)
    target_name_segs = sf.parse_stnd_r2f_output_folder_name(huc_crs_folder_name)
    if (target_name_segs[0] == "error"):
        raise Exception(target_name_segs[1])
    
    #---------------    
    #  only folders that match the huc and crs (don't worry about the date yet)
    s3_huc_crs_folder_names = __get_s3_huc_crs_folder_list(bucket_name,
                                                           target_name_segs,
                                                           is_verbose)

    if (len(s3_huc_crs_folder_names) > 1):
        print("*********************")
        print("*** NOTE  ")
        print("We have detected more than one s3 existing folders with exact or similar " \
              f"names as the new incoming folder name of {huc_crs_folder_name}) at " \
              f" {s3_full_output_ras2fim_folder})." \
              "Sets of questions will be asked comparing the incoming folder name with each " \
              "of the pre-existing folder names, each one at a time.")
        print("*********************")        

    #---------------
    # Using the date object in the tuple, see if any are the same date or newer
    # All of the s3_huc_crs_folder_names items already share the huc and crs value
    # but not necessarily the date
    for s3_existing_folder in s3_huc_crs_folder_names:

        # If the existing s3_folder has the same name, ask if abort, move to archive or overwrite
        # We will re-assemble the s3 existing folder (migth not match the data).
        # e.g. 12030105_2276_230303
        existing_folder_name = f"{s3_existing_folder[0]}_{s3_existing_folder[1]}_{s3_existing_folder[2]}"

        # Now we want the S3 path for the existing folder (without the bucket)
        existing_folder_path = f"{sv.S3_OUTPUT_RAS2FIM_FOLDER}/{existing_folder_name}"

        action = ""
        if (existing_folder_name == huc_crs_folder_name):
            action = __ask_user_about_dup_folder_name(huc_crs_folder_name,
                                                      s3_full_output_ras2fim_folder,
                                                      s3_full_archive_path)
            
        # An existing s3_folder has a newer date
        else:
            if (s3_existing_folder[3] >= target_name_segs[3]):
                is_existing_older = False
            elif (s3_existing_folder[3] < target_name_segs[3]):
                is_existing_older = True

            action = __ask_user_about_different_date_folder_name(huc_crs_folder_name,
                                                                s3_full_output_ras2fim_folder,
                                                                s3_full_archive_path,
                                                                existing_folder_name,
                                                                s3_existing_folder[2],
                                                                is_existing_older)


        if (action == "overwrite"): # overwrite the pre-existing same named folder with the incoming version
            # we need to delete the original folder so we don't leave junk it int.

            s3_sf.delete_s3_folder(bucket_name, existing_folder_path, is_verbose)

            # Yes.. if it deletes but fails to upload the new one, we have a problem.
            # TODO: make a temp copy somewhere (not in archives root folder), then delete it from
            # the original existing path, then load the new one, then delete the temp copy.
            # if upload fails, copy back from temp
            s3_sf.upload_output_folder_to_s3(src_path, 
                                             bucket_name, 
                                             sv.S3_OUTPUT_RAS2FIM_FOLDER, 
                                             huc_crs_folder_name, 
                                             is_verbose)

        elif (action == "archive") or (action == "incoming"): # move pre-existing to the archive

            # move existing to archive
            # delete it from archive first if it exists (keeps it clean otherwise it is a merge)


            # upload the new one output_ras2fim
            s3_sf.upload_output_folder_to_s3(src_path, 
                                             bucket_name, 
                                             sv.S3_OUTPUT_RAS2FIM_FOLDER, 
                                             huc_crs_folder_name, 
                                             is_verbose)

        elif (action == "existing"): # move incoming folder straight to the archive

            # delete it from archive first if it exists (keeps it clean otherwise it is a merge)

            # upload the new one but straight to archive
            s3_sf.upload_output_folder_to_s3(src_path, 
                                             bucket_name, 
                                             sv.S3_RAS2FIM_ARCHIVE_FOLDER, 
                                             huc_crs_folder_name, 
                                             is_verbose)
        else:
            raise Exception("Internal Error: Invalid action type of {action}")


####################################################################
# Note: There is enough differnce that I wanted a seperate function for this scenario
def __ask_user_about_dup_folder_name(target_huc_crs_folder_name,
                                     s3_full_output_ras2fim_folder,
                                     s3_full_archive_path):
    
    print()
    print("*********************")

    msg = f"You are wanting to upload a folder using the huc/crs of {target_huc_crs_folder_name}. " \
            f"However, a folder of the same name already exists at {s3_full_output_ras2fim_folder}. \n\n" \
            "   -- Type 'overwrite' if you want to overwrite the current folder.\n" \
            "           Note: if you overwrite an existing folder, the s3 version will be deleted first,\n" \
            "           then the new incoming will be loaded.\n\n" \
            "   -- Type 'archive' if you want to move the existing folder in to the archive folder.\n " \
            "           Note: if you archive (move) the existing archive folder and it already exists,\n" \
            "           it will be overwritten.\n\n" \
            "   -- Type 'abort' to stop the program.\n" \
            ">>"
    
    action = input(msg).lower()
    if (action) == 'abort':
        print()
        print(f".. You have selected {action}. Program stopped.")
        print()
        sys.exit(0)
    elif (action) == 'overwrite':
        print()
        print(f".. You have selected {action}. Folder will be overwritten.")
        print()
    elif (action) == 'archive':
        print()
        print(f".. You have selected {action}. Existing folder will be moved to {s3_full_archive_path}.")
        print()
    else:
        print()
        print(f".. You have entered an invalid value of '{action}'. Program stopped.")
        print()
        sys.exit(0)

    return action

####################################################################
# Note: There is enough differnce that I wanted a seperate function for this scenario
def __ask_user_about_different_date_folder_name(target_huc_crs_folder_name,
                                                s3_full_output_ras2fim_folder,
                                                s3_full_archive_path,
                                                existing_folder_name,
                                                existing_folder_date,
                                                is_existing_older):
    
    # is_existing_older = True means existing migth be 230814, but target is 230722)
    # is_existing_older = False means existing migth be 221103, but target is 230816)
    # if the dates are the same, we handle it in a different function (enough differences)

    print()
    print("*********************")

    msg = f"You are wanting to upload a folder using the huc/crs of {target_huc_crs_folder_name}. " \
          f"However, a folder of the starting with the same huc and crs already exists at {s3_full_output_ras2fim_folder} " 
    
    if (is_existing_older):
        msg += "but with an older date"
    else:
        msg += "but with a newer date"

    msg += f" of {existing_folder_date} ({existing_folder_name}).\n" \
            "Only one can remain in the target folder, the other can be moved to the archive folder.\n" \
            "... Which of the two do you want to move to archive?"
    msg += "   -- Type 'existing' to keep the existing and put the incoming folder straight to archive.\n " \
           "   -- Type 'incoming' to keep the new incoming folder and move the existing one to " \
           "      to the archive folder.\n\n" \
           "... Note: if you archive (move) the existing archive folder and it already exists," \
           " it will be overwritten.\n" \
           "   -- Type 'abort' to stop the program." \
           ">>"

    action = input(msg).lower()
    if (action) == 'abort':
        print()
        print(f".. You have selected {action}. Program stopped.")
        print()
        sys.exit(0)
    elif (action) == 'existing':
        print()
        print(f".. You have selected {action}. The new incoming folder will be moved to the archive folder at"\
              f" {s3_full_archive_path}.")              
        print()
    elif (action) == 'incoming':
        print()
        print(f".. You have selected {action}. The pre-existing folder will be moved to the archive folder at"\
              f" {s3_full_archive_path}.")
        print()
    else:
        print()
        print(f".. You have entered an invalid value of '{action}'. Program stopped.")
        print()
        sys.exit(0)

    return action

####################################################################
def __get_s3_huc_crs_folder_list(bucket_name, target_name_segs, is_verbose):
    
    """
    Overview
        This will search the first level path of the s3_folder_path (prefix)
        for folders that start with the same huc and crs of the incoming target huc_crs folder (target_name_segs)
    Inputs
        - bucket_name: eg. mys3bucket_name
        - target_name_segs: a tuple of the original target huc_crs folder name, split into
            four segments (huc, crs, date key text, date object of date key)
            eg. ('12030105', '2276', '230810', date object of 230810)
    Output
        - a list of s3 folders that match the starting huc and crs segments
    """

    s3_huc_crs_folder_names = []

    try:
        s3 = boto3.client('s3')   
            
        # If the bucket is incorrect, it will throw an exception that already makes sense
        s3_objs = s3.list_objects_v2(Bucket = bucket_name,
                                     Prefix = sv.S3_OUTPUT_RAS2FIM_FOLDER,
                                     Delimiter = '/')
        if (s3_objs["KeyCount"] == 0):
            return s3_huc_crs_folder_names # means folder was empty

        # s3 doesn't really use folder names, it jsut makes a key with a long name with slashs 
        # in it. 
        for folder_name_key in s3_objs.get('CommonPrefixes'):
            # comes in like this: output_ras2fim/12090301_2277_230811/
            # strip of the prefix and last slash so we have straight folder names
            key = folder_name_key["Prefix"]

            if (is_verbose):
                print("--------------------")
                print(f"key is {key}")

            # strip to the final folder names (but first occurance of the prefix only)
            key_child_folder = key.replace(sv.S3_OUTPUT_RAS2FIM_FOLDER, '', 1)

            # We easily could get extra folders that are not huc folders, but we will purge them
            # if it is valid key, add it to a list. Returns a tuple.
            # If it does not match a pattern we want, the first element of the tuple will be
            # the word error, but we don't care. We only want valid huc_crs_date pattern folders.
            key_segs = sf.parse_huc_crs_folder_name(key_child_folder)
            if (key_segs[0] != "error"):
                # see if the huc and crs it matches the incoming huc number and crs
                if (key_segs[0] == target_name_segs[0]) and (
                    key_segs[1] == target_name_segs[1]):
                    s3_huc_crs_folder_names.append(key_segs)

        if (is_verbose):
            print("huc_crs folders found are ...")
            print(s3_huc_crs_folder_names)

    except Exception as ex:
        print("===================")
        print(f"An critical error has occurred with talking with S3. Details: {ex}")
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print(f"Ended: {dt_string}")
        raise 

    return s3_huc_crs_folder_names

####################################################################
####  Some validation of input, but also creating key variables ######
def __validate_input(path_to_huc_crs_output_dir, 
                     s3_bucket_name):

    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    rtn_varibles_dict = {}

    #---------------
    # why is this here? might not come in via __main__
    if (path_to_huc_crs_output_dir == ""):
        raise ValueError("Source huc_crs_output parameter value can not be empty")

    if (s3_bucket_name == ""):
        raise ValueError("Bucket name parameter value can not be empty")

    #---------------
    # we need to split this to seperate variables.
    # e.g path_to_huc_crs_output_dir = c:\ras2fim_data\output_ras2fim\12030202_102739_230810
    #   or 12030202_102739_230810

    # "huc_crs_dir" becomes (if not already) 12030202_102739_230810
    # "huc_crs_full_path" becomes (if not already) c:\ras2fim_data\output_ras2fim\12030202_102739_230810
    # remembering that the path or folder name might be different.

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

    # check to see that the "final" directory isn't empty
    file_count = len(os.listdir(final_dir))
    if (file_count == 0):
        raise ValueError(f"Source HUC/CRS 'final folder' at {final_dir}" \
                         " does not appear to have any files or folders.")

    # --------------------
    # check ras2fim output bucket exists
  
    print()
    print(f"Validating that the s3 bucket of {s3_bucket_name} exists")
    if (s3_sf.does_s3_bucket_exist(s3_bucket_name) == False):
        raise ValueError(f"{s3_bucket_name} does not exist")
    else:
        print(".... found")

    # --------------------
    # check ras2fim archive bucket folder
    s3_full_output_ras2fim_folder = f"s3://{s3_bucket_name}/{sv.S3_OUTPUT_RAS2FIM_FOLDER}"
    print()
    print(f"Validating S3 output folder of {s3_full_output_ras2fim_folder}")

    # it will throw an error if it does not exist
    s3_sf.is_valid_s3_folder(s3_full_output_ras2fim_folder) # don't care about return values here
    print(".... found")
    rtn_varibles_dict["s3_full_output_ras2fim_folder"] = s3_full_output_ras2fim_folder

    # --------------------
    # check ras2fim archive bucket folder
    s3_full_archive_path = f"s3://{s3_bucket_name}/{sv.S3_RAS2FIM_ARCHIVE_FOLDER}"
    print()
    print(f"Validating S3 output archive folder of {s3_full_archive_path}")
    s3_sf.is_valid_s3_folder(s3_full_archive_path) # don't care about return values here
    print(".... found")
    rtn_varibles_dict["s3_full_archive_path"] = s3_full_archive_path

    return rtn_varibles_dict


if __name__ == '__main__':

    # TODO: Add samples

    # NOTE: pathing inside the bucket can not be changed.
    # The root folder (prefix) is hardcoded to output_ras2fim and the archive folder is
    # hardcoded to output_ras2fim_archive.
    # This will help preserve other tools that are relying on specific s3 pathing.
    # The folder name from the source folder (not path) will automatically becomes the s3 folder name.

    parser = argparse.ArgumentParser(description='Pushing ras2fim HUC/CRS output folders back to S3',
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-s', '--src_path_to_huc_crs_output_dir', 
                        help='REQUIRED: Can be used in two ways:\n' \
                             '1) Add just the output huc_crs folder name (assumed default pathing)\n' \
                             '2) A full defined path including output huc_crs folder\n' \
                             ' ie) c:\ras2fim_data\output_ras2fim\12030202_102739_230810\n' \
                             '  or just 12030202_102739_230810',
                        required=True, metavar='')

    # s3 bucket name. Can't default S3 paths due to security.  ie) xyz  of (s3://xyz)
    parser.add_argument('-b', '--s3_bucket_name',
                        help='REQUIRED: S3 bucket where output ras2fim folders are placed.\n' \
                             'eg) xyz from s3://xyz',
                        required=True, metavar='')

    parser.add_argument('-v','--is_verbose', 
                        help='OPTIONAL: Adding this flag will give additional tracing output. Default = False (no extra output)',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())

    save_output_to_s3(**args)