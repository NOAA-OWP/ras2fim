#!/usr/bin/env python3

import argparse
import datetime as dt
import os
import sys
import traceback

import boto3
import colored as cl
import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import s3_shared_functions as s3_sf

import ras2fim_logger
import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = ras2fim_logger.R2F_LOG
TRACKER_ACTIONS = ["uploaded", "moved_to_arch", "overwriting_prev", "straight_to_arch"]
TRACKER_SRC_LOCAL_PATH = ""

"""

NOTE: This script is primarily designed for NOAA/OWP use, but if you have access to your own
S3, output_ras2fim (unit) and output_ras2fim_archive folders, you are welcome to use it.
We can not grant access to our NOAA / OWP S3 bucket at this time.

This tool also had assumes some very specific folder structure of having the output_ras2fim and
output_ras2fim_archive at the root of your repo (deliberately not configurable at this time.)

To run this tool, you must have already ran 'aws configure' and added creds to the S3 bucket. You should
find that you only need to setup yoru machine once with 'aws configure' and not each time you use this tool.

Overall logic flow:

    - Selecting a single output (unit)(huc/crs) folder (ie. 12090301_2277_0821) to be selected to
      S3. The bucket is definable but not the pathing inside the bucket. It assumes
      the folders of output_ras2fim and output_ras2fim_archive exist at the root repo.

    - The incoming folder will check output_ras2fim folder.
        - If the exact unit folder name (inc huc_crs_date) exist, it will ask the user if they want to:
            - abort
            - overwrite the one in output_ras2fim (likely a mistake or they are redoing it quickly)
            - archive: move the existing folder to the archive folder, and upload the nw one .

        - If a folder matching the huc and crs exist (but not the date), it will tell the user if the
          existing folder is older or newer then the incoming folder and ask if they want to:
             - abort
             - existing: the existing folder to output_ras2fim_archive, the upload the new one to
               output_ras2fim
             - incoming: Re-direct the incoming folder directly to output_ras2fim_archive.

        - If there is no existing folder matching the huc and crs, just upload it to output_ras2fim

    - If there happens to be more than one folder in S3 that match that huc and crs, the user will
      be given the option to abort, or it will move existing ones to archive and upload the one one

    - A file named ras_output_tracker.csv in the S3:{bucket_name}/output_ras2fim folder will be updated
      to add a new record of what happened.  The ras_output_tracker.csv is the master and covers all
      transactions made in relation in uploading new unit folder.

"""


####################################################################
def unit_to_s3(src_unit_dir_path, s3_bucket_name, is_verbose):
    start_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")

    RLOG.lprint("")
    RLOG.lprint("=================================================================")
    RLOG.lprint("          RUN ras_unit_to_s3 ")
    RLOG.lprint(f"  (-s): Source unit folder {src_unit_dir_path} ")
    RLOG.lprint(f"  (-b): s3 bucket name {s3_bucket_name}")
    RLOG.lprint("")
    RLOG.lprint(f" --- Saving unit folder to S3 start: {dt_string} (UTC time) ")

    # --------------------
    # validate input variables and setup key variables
    varibles_dict = __validate_input(src_unit_dir_path, s3_bucket_name)

    # why have this come in from variables_dict? the src_unit_dir_path can come in not
    # fully pathed, just the unit folder in the default folder path
    src_path = varibles_dict["src_unit_full_path"]
    # eg. c:\ras2fim_data\output_ras2fim\12030202_102739_230810
    unit_folder_name = varibles_dict["src_unit_dir"]
    # eg. 12030202_102739_230810
    s3_full_output_path = varibles_dict["s3_full_output_path"]
    # e.g. s3://xyz/output_ras2fim
    s3_full_archive_path = varibles_dict["s3_full_archive_path"]
    # e.g. s3://xyz/output_ras2fim_archive
    s3_full_unit_path = f"{s3_full_output_path}/{unit_folder_name}"
    # Note: Generally speaking most values starting with with "s3://xyz" are used for display purposes only
    # as generaly logic for S3 needs to do it in parts (bucket, and folder). We usually change the
    # folder values to add in the phrase "output_ras2fim" or "output_ras2fim_archive"

    # We don't want to try upload current active log files (for this script)
    # and the temp local tracker file

    global TRACKER_SRC_LOCAL_PATH
    TRACKER_SRC_LOCAL_PATH = os.path.join(src_unit_dir_path, sv.S3_OUTPUT_TRACKER_FILE)

    # These are files that will not be uploaded to S3
    # Note: Will upload log files from original process such as ras2fim files
    # but not the logs files being created as part of ras_unit_to_s3 logs
    skip_files = [
        TRACKER_SRC_LOCAL_PATH,
        RLOG.LOG_FILE_PATH,
        RLOG.LOG_WARNING_FILE_PATH,
        RLOG.LOG_ERROR_FILE_PATH,
    ]

    RLOG.lprint(f" --- source unit full path is {src_path}")
    RLOG.lprint(f" --- s3 folder target path is {s3_full_unit_path}")
    RLOG.lprint(f" --- S3 archive folder path (if applicable) is {s3_full_archive_path}")
    RLOG.lprint("")
    RLOG.lprint("=================================================================")

    # --------------------
    # We need to see if the directory already exists in s3.
    # Depending on what we find will tell us where to uploading the incoming folder
    # and what to do with pre-existing if there are any pre-existing folders
    # matching the huc/crs.
    __process_upload(s3_bucket_name, src_path, unit_folder_name, skip_files, is_verbose)

    # --------------------
    RLOG.lprint("")
    RLOG.lprint("===================================================================")
    RLOG.lprint("Copy to S3 Complete")
    end_time = dt.datetime.utcnow()
    dt_string = dt.datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
    RLOG.lprint(f"Ended (UTC): {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    RLOG.lprint(f"Duration: {str(time_duration).split('.')[0]}")
    RLOG.lprint("")


####################################################################
def __process_upload(bucket_name, src_path, unit_folder_name, skip_files, is_verbose):
    """
    Processing Steps:
      - Load all first level folder names from that folder. ie) output_ras2fim

      - Get a list for any S3 folder names that match the huc and crs value

      - using the dates for each of the existing s3 unit:
          - If the incoming date is older or equal to than any pre-existing one, error out
          - If the incoming date is newer that all pre-existing ones, then move the pre-existing
            one (or ones) to the archive folder.

        Net results: Only one folder with the HUC/CRS combo an exist in the s3 output folder name
           and older ones (generally) are all in archives.

       - If we find some that need to be moved, ask the user to confirm before contining (or abort)

       - Any folder moved to archive, will have an addition text added to it, in the form of:
            _BK_yymmdd_hhmm  (24 hours time.)
              eg) s3://xyz/output_ras2fim_archive/12030105_2276_230810_BK_230825_1406

    Input
        - bucket_name: e.g xyz
        - src_path:  eg. c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - skip_files: files that will not be uploaded to S3 (such as ras_unit_to_s3 log files)
        - unit_folder_name:  12030105_2276_230810

    """

    RLOG.lprint("Checking existing s3 folders for folders starting with same huc number and crs value")
    RLOG.lprint("")

    # yes.. print
    print(
        f"{cl.fore.SPRING_GREEN_2B}"
        "-- The intention is that only one unit (per huc/crs) output folder, usually the most current,"
        " is kept in the offical output_ras2fim folder. All unit folders in the s3 output_ras2fim"
        f" folder will be included for ras2releases.{cl.style.RESET}"
    )

    # ---------------
    # splits it a five part dictionary
    src_name_dict = s3_sf.parse_unit_folder_name(unit_folder_name)
    if "error" in src_name_dict:
        raise Exception(src_name_dict["error"])

    # ---------------
    # We want only folders that match the huc and crs (don't worry about the date yet).
    # In theory, it shoudl only find 0 or 1 match as there should only ever be
    # the latest unit version folder in outputs_ras2fim. But if there are more
    # than one... something went wrong or someone loaded one directly.
    s3_unit_folder_names = __get_s3_unit_folder_list(bucket_name, src_name_dict, is_verbose)

    if len(s3_unit_folder_names) == 1:
        # ---------------
        # Figure out the action (by asking the user)

        # All of the s3_unit_folder_names items already share the huc and crs value
        # but not necessarily the date
        s3_existing_folder_name = s3_unit_folder_names[0]

        # Now we want the S3 path for the existing folder (without the bucket)
        # existing_folder_path = f"{sv.S3_OUTPUT_RAS2FIM_FOLDER}/{s3_existing_folder_name}"
        # eg. outputs_ras2fim/12030105_2276_230303

        existing_name_dict = s3_sf.parse_unit_folder_name(s3_existing_folder_name)

        action = ""

        if existing_name_dict["unit_folder_name"] == src_name_dict["unit_folder_name"]:
            # exact same folder name including date
            action = __ask_user_about_dup_folder_name(s3_existing_folder_name, bucket_name)
            # RLOG.debug(f"action is {action}")

        # An existing s3_folder has a newer or older date
        else:
            if existing_name_dict["key_date_as_dt"] < src_name_dict["key_date_as_dt"]:
                is_existing_older = False
            elif existing_name_dict["key_date_as_dt"] > src_name_dict["key_date_as_dt"]:
                is_existing_older = True

            action = __ask_user_about_different_date_folder_name(
                unit_folder_name, bucket_name, existing_name_dict["unit_folder_name"], is_existing_older
            )
            # RLOG.debug(f"action is {action}")

        # --------------------------
        # Process the action  (aka we start the uploads to S3 and changes in S3)
        if action == TRACKER_ACTIONS[1]:  # moved_to_arch
            # existing moved to archive, new one to output
            # RLOG.debug(f"action is {TRACKER_ACTIONS[1]}")

            # We will change the name to add on "_BK_yymmdd_hhmm".
            #  eg) s3://xyz/output_ras2fim_archive/12030105_2276_230810_BK_230825_1406
            # On the super rare that it was updated in the exact date hr and min, just overwrite it

            new_s3_folder_name = __adjust_folder_name_for_archive(s3_existing_folder_name)

            # move existing to archive
            __move_s3_folder_to_archive(
                bucket_name, src_path, s3_existing_folder_name, new_s3_folder_name, is_verbose
            )

            # move new one to output
            __upload_s3_folder(
                bucket_name,
                src_path,
                unit_folder_name,
                TRACKER_ACTIONS[0],
                sv.S3_OUTPUT_RAS2FIM_FOLDER,
                unit_folder_name,
                skip_files,
                is_verbose,
            )

        elif action == TRACKER_ACTIONS[2]:  # (overwriting_prev)
            # overwrite the pre-existing same named folder with the incoming
            #  version, we need to delete the original folder so we don't leave junk it int.
            # RLOG.debug(f"action is {TRACKER_ACTIONS[2]}")
            __overwrite_s3_existing_folder(
                src_path, bucket_name, src_name_dict["unit_folder_name"], skip_files, is_verbose
            )

        elif action == TRACKER_ACTIONS[3]:  # straight_to_arch
            # RLOG.debug(f"action is {TRACKER_ACTIONS[3]}")
            new_s3_folder_name = __adjust_folder_name_for_archive(unit_folder_name)

            # new incoming goes straight to archive
            __upload_s3_folder(
                bucket_name,
                src_path,
                unit_folder_name,
                TRACKER_ACTIONS[3],
                sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER,
                new_s3_folder_name,
                skip_files,
                is_verbose,
            )

        else:
            raise Exception(f"Internal Error: Invalid action type of {action}")

    elif len(s3_unit_folder_names) > 1:
        RLOG.lprint("+++++++++++++++++++++++++")
        RLOG.lprint("")
        msg = (
            f"{cl.fore.SPRING_GREEN_2B}"
            "We have detected multiple previously existing s3 folders with the same HUC and CRS "
            f"as the new incoming folder name of {unit_folder_name} at "
            f"s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_FOLDER}.\n\n"
            "All previously existing folders with this HUC and CRS will be moved to the archive"
            f" folder, then the new incoming unit will be uploaded to outputs.\n\n{cl.style.RESET}"
            f"{cl.fore.LIGHT_YELLOW}"
            " Do you want to continue?\n\n"
            f"{cl.style.RESET}"
            f"   -- Type {cl.fore.SPRING_GREEN_2B}'continue'{cl.style.RESET}"
            "  if you want to move existing s3 folder(s) and upload the new one.\n"
            f"   -- Type {cl.fore.SPRING_GREEN_2B}'abort'{cl.style.RESET}"
            " to stop the program.\n"
            f"{cl.fore.LIGHT_YELLOW}  ?={cl.style.RESET}"
        )

        resp = input(msg).lower()
        if (resp) == "abort":
            RLOG.lprint(f"\n.. You have selected {resp}. Program stopped.\n")
            sys.exit(0)
        elif (resp) == "continue":
            action = TRACKER_ACTIONS[2]  # overwriting_prev
            RLOG.lprint(f"\n.. You have selected {resp}.\n")

        for i in range(len(s3_unit_folder_names)):
            s3_existing_folder_name = s3_unit_folder_names[i]

            RLOG.lprint(f"--- The existing folder {s3_existing_folder_name} will be now be archived.\n\n")

            new_s3_folder_name = __adjust_folder_name_for_archive(s3_existing_folder_name)

            # move existing to archive
            __move_s3_folder_to_archive(
                bucket_name, src_path, s3_existing_folder_name, new_s3_folder_name, is_verbose
            )
            RLOG.lprint("")

        # Now we can upload the incoming
        __upload_s3_folder(
            bucket_name,
            src_path,
            unit_folder_name,
            TRACKER_ACTIONS[0],
            sv.S3_OUTPUT_RAS2FIM_FOLDER,
            unit_folder_name,
            skip_files,
            is_verbose,
        )

    else:
        # folder with the same unit doesn't exist and we can load to the output folder
        __upload_s3_folder(
            bucket_name,
            src_path,
            unit_folder_name,
            TRACKER_ACTIONS[0],
            sv.S3_OUTPUT_RAS2FIM_FOLDER,
            unit_folder_name,
            skip_files,
            is_verbose,
        )


####################################################################
# Unit folder already exists in outputs
# Note: There is enough differnce that I wanted a seperate function for this scenario
def __ask_user_about_dup_folder_name(unit_folder_name, bucket_name):
    print()
    print("*********************")

    msg = (
        f"{cl.fore.SPRING_GREEN_2B}"
        f"You have requested to upload a unit folder named {unit_folder_name}."
        " However, a unit folder of the exact same name already exists at"
        f" s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_FOLDER}.\n"
        "Ensure that you or other staff have not uploaded the same unit folder (huc_crs_date).\n\n"
        f"{cl.style.RESET}"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'overwrite'{cl.style.RESET}"
        " if you want to overwrite the current folder.\n"
        "           Note: If you overwrite an existing folder, the s3 version will be deleted first,\n"
        "           then the new incoming folder will be loaded.\n"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'archive'{cl.style.RESET}"
        " if you want to move the existing folder in to the archive folder.\n"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'abort'{cl.style.RESET}"
        " to stop the program.\n"
        f"{cl.fore.LIGHT_YELLOW}  ?={cl.style.RESET}"
    )

    resp = input(msg).lower()
    if (resp) == "abort":
        RLOG.lprint(f"\n.. You have selected {resp}. Program stopped.\n")
        sys.exit(0)
    elif (resp) == "overwrite":
        action = TRACKER_ACTIONS[2]  # overwriting_prev
        RLOG.lprint(f"\n.. You have selected {resp}\n")

    elif (resp) == "archive":
        action = TRACKER_ACTIONS[1]  # moved_to_arch
        RLOG.lprint(
            f"\n.. You have selected {resp}. Existing folder will be moved to "
            f"s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER}.\n"
        )
    else:
        RLOG.lprint(f"\n.. You have entered an invalid value of '{resp}'. Program stopped.\n")
        sys.exit(0)

    return action


####################################################################
# Note: There is enough differnce that I wanted a seperate function for this scenario
def __ask_user_about_different_date_folder_name(
    src_unit_folder_name, bucket_name, existing_folder_name, is_existing_older
):
    # is_existing_older = True means existing might be 230814, but target is 230722)
    # is_existing_older = False means existing migth be 221103, but target is 230816)
    # if the dates are the same, we handle it in a different function (enough differences)

    print()
    print("*********************")

    msg = (
        f"{cl.fore.SPRING_GREEN_2B}"
        f"You have requested to upload a unit folder named {src_unit_folder_name}."
        " However, a folder of the starting with the same huc and crs already exists as"
        f" s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_FOLDER}/{existing_folder_name}.\n"
    )

    if is_existing_older is True:
        msg += "The incoming folder has an older date than the existing folder.\n\n"
    else:
        msg += "The incoming folder has a newer date than the existing folder.\n\n"

    msg += (
        f"Only one can remain in the {sv.S3_OUTPUT_RAS2FIM_FOLDER} folder, the other can be moved"
        " to the archive folder.\n\n"
        f"{cl.style.RESET}"
        f"{cl.fore.LIGHT_YELLOW}"
        "... Which of the two do you want to move to archive?\n"
        f"{cl.style.RESET}"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'incoming'{cl.style.RESET}"
        "  to upload the incoming folder straight into archives and keep the existing in outputs.\n"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'existing'{cl.style.RESET}"
        " to move the existing one to the archive folder and upload the incoming folder to outputs.\n"
        f"   -- Type {cl.fore.SPRING_GREEN_2B}'abort'{cl.style.RESET}"
        " to stop the program.\n"
        f"{cl.fore.LIGHT_YELLOW}  ?={cl.style.RESET}"
    )

    print()
    resp = input(msg).lower()
    if resp == "abort":
        RLOG.lprint(f"\n.. You have selected {resp}. Program stopped.\n")
        sys.exit(0)

    elif resp == "incoming":
        action = TRACKER_ACTIONS[3]  # incoming straight_to_arch
        RLOG.lprint(
            f"\n.. You have selected {resp}. The new incoming folder will be uploaded directly to the"
            f" archive folder at s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER}.\n"
        )

    elif resp == "existing":
        action = TRACKER_ACTIONS[1]  # existing moved_to_arch
        RLOG.lprint(
            f"\n.. You have selected {resp}. The pre-existing folder will be moved to the archive folder at"
            f" s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER}.\n"
        )

    else:
        RLOG.lprint(f"\n.. You have entered an invalid value of '{resp}'. Program stopped.\n")
        sys.exit(0)

    return action


####################################################################
def __upload_s3_folder(
    bucket_name,
    src_path,
    orig_folder_name,
    action,
    s3_folder_path,
    new_s3_folder_name,
    skip_files,
    is_verbose,
):
    """
    Overview:
        Upload a folder from local into S3. Sometimes it might be loaded to output_ras2fim
        or it might be uploaded directly into output_ras2fim_archive
    Input:
        - bucket_name:  eg. my_bucket_name  (as  in s3://my_bucket_name)
        - src_path: serves as a temp place to adjust the s3 tracker csv before re-loading
            eg. c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - orig_folder_name: The original folder name as it started: generally just the
             huc_crs_date:  eg. 12030202_102739_230810
        - action: What is happening. Best to use TRACKER_ACTIONS dict (ugly but workable for now)
        - s3_folder_path: eg. output_ras2fim  or output_ras2fim_archive
        - new_s3_folder_name: eg. 12030202_102739_230810_BK_230825_1406 if going to archive.
             It may or may not in the process of having a new folder names as it is being loaded,
             especially if an incoming upload folder is going straight to archives (which is an option)
        - skip_files: Files that will not be uploaded. ie) the wip local tracker file,
             ras_unit_to_s3 log file
    """

    s3_sf.upload_folder_to_s3(src_path, bucket_name, s3_folder_path, new_s3_folder_name, skip_files)

    __add_record_to_tracker(
        bucket_name, src_path, orig_folder_name, action, new_s3_folder_name, s3_folder_path, is_verbose
    )


####################################################################
def __move_s3_folder_to_archive(bucket_name, src_path, orig_folder_name, target_folder_name, is_verbose):
    """
    Overview:
        Just moving one S3 folder to another place.
    Input:
        - bucket_name:  eg. my_bucket_name  (as  in s3://my_bucket_name)
        - src_path: serves as a temp place to adjust the s3 tracker csv before re-loading
            eg. c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - orig_folder_name:  12030202_102739_230810
        - target_folder_name: eg. 12030202_102739_230810_BK_230825_1406
    """

    s3_src_folder_path = f"{sv.S3_OUTPUT_RAS2FIM_FOLDER}/{orig_folder_name}"
    s3_target_folder_path = f"{sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER}/{target_folder_name}"

    # s3 actually can't move files or folders, so we copy them then delete them.
    # move_s3_folder_in_bucket will take care of both
    RLOG.debug(f"Moving {s3_src_folder_path} to {s3_target_folder_path}")
    s3_sf.move_s3_folder_in_bucket(bucket_name, s3_src_folder_path, s3_target_folder_path)

    # TRACKER_ACTIONS[1] = moved_to_arch
    __add_record_to_tracker(
        bucket_name,
        src_path,
        orig_folder_name,
        TRACKER_ACTIONS[1],
        target_folder_name,
        sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER,
        is_verbose,
    )


####################################################################
def __overwrite_s3_existing_folder(src_path, bucket_name, unit_folder_name, skip_files, is_verbose):
    """
    Overview:
        When the system sees the exact same S3 huc_crs_date combination in output_ras2fim,
        it probably means it was loaded to day by this person or someone else for the same,
        Maybe they loaded it yesterday, forgot they did it and are try to reload it again.
        We gave them the option to overwrite the current output_ras2fim folder just in case.

        So we need to delete the existing and reload the new one (same name of course)
    Inputs:
        - src_path: eg. c:\ras2fim_data\output_ras2fim\12030202_102739_230810
        - bucket_name: {your bucket name}
        - unit_folder_name: eg. 12030202_102739_230810
        - skip_files: Files not to be uploaded ie) the wip local tracker file, ras_unit_to_s3 log file.
    """
    s3_folder_path = f"{sv.S3_OUTPUT_RAS2FIM_FOLDER}/{unit_folder_name}"

    print("***  NOTE: we will delete the original directory, then upload the new unit")
    print()

    RLOG.debug(f"Deleting {s3_folder_path}")
    s3_sf.delete_s3_folder(bucket_name, s3_folder_path)

    # Yes.. if it deletes but fails to upload the new one, we have a problem.
    # TODO: maybe ?? - make a temp copy somewhere (not in archives root folder), then delete
    #  it from the original existing path, then load the new one, then delete the temp copy.
    # if upload fails, copy back from temp ??

    RLOG.debug(f"Uploading {src_path} to {sv.S3_OUTPUT_RAS2FIM_FOLDER}")

    s3_sf.upload_folder_to_s3(
        src_path, bucket_name, sv.S3_OUTPUT_RAS2FIM_FOLDER, unit_folder_name, skip_files
    )

    # TRACKER_ACTIONS[2] = overwriting_prev
    __add_record_to_tracker(
        bucket_name,
        src_path,
        unit_folder_name,
        TRACKER_ACTIONS[2],
        unit_folder_name,
        sv.S3_OUTPUT_RAS2FIM_FOLDER,
        is_verbose,
    )
    # eg, my_bucket_name c:\ras2fim_data\output_ras2fim\12030202_102739_230810,
    # 12030202_102739_230810, 'overwriting_prev', 12030202_102739_230810,
    # output_ras2fim, False  (yes. in this case the src and target folder names are the same)


####################################################################
def __adjust_folder_name_for_archive(folder_name):
    # formatted to S3 convention for archiving
    # Takes the original name changes it to:
    # {fold_name}_BK_{utc current date (yymmdd)}_{utc time (HHmm)}
    #   eg. 12030105_2276_230810_BK_230825_1406
    # all UTC
    # Why rename it for the archive folder? we can't have dup folder names

    if folder_name.endswith("/"):
        folder_name = folder_name[:-1]

    cur_date = sf.get_stnd_date(False)  # eg. 230825  (in UTC)
    cur_time = dt.datetime.utcnow().strftime("%H%M")  # eg  2315  (11:15 pm) (in UTC)
    new_s3_folder_name = f"{folder_name}_BK_{cur_date}_{cur_time}"

    return new_s3_folder_name


####################################################################
def __get_s3_unit_folder_list(bucket_name, src_name_dict, is_verbose):
    """
    Overview
        This will search the first level path of the s3_folder_path (prefix)
        for folders that start with the same huc and crs of the incoming target unit
        folder (target_name_segs).

        It will filter folders to only ones that match the huc and crs
    Inputs
        - bucket_name: eg. mys3bucket_name
        - target_name_segs: a dictionary of the original src unit folder name, split into
            five keys:
                key_huc,
                key_crs_number,
                key_date_as_str (date string eg: 230811),
                key_date_as_dt (date obj for 230811)
                unit_folder_name (12090301_2277_230811) (cleaned version)
    Output
        - a list of s3 folders (just folder names not path) that match
          the starting huc and crs segments
    """

    s3_unit_folder_names = []

    try:
        s3 = boto3.client("s3")

        # If the bucket is incorrect, it will throw an exception that already makes sense
        s3_objs = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=sv.S3_OUTPUT_RAS2FIM_FOLDER + "/", Delimiter="/"
        )
        if is_verbose is True:
            RLOG.debug("s3_objs is")
            RLOG.debug(s3_objs)

        if s3_objs["KeyCount"] == 0:
            return s3_unit_folder_names  # means folder was empty

        # s3 doesn't really use folder names, it jsut makes a key with a long name with slashs
        # in it.
        for folder_name_key in s3_objs["CommonPrefixes"]:
            # comes in like this: output_ras2fim/12090301_2277_230811/
            # strip of the prefix and last slash so we have straight folder names
            key = folder_name_key["Prefix"]

            if is_verbose is True:
                RLOG.debug("--------------------")
                RLOG.debug(f"key is {key}")

            # strip to the final folder names (but first occurance of the prefix only)
            key_child_folder = key.replace(sv.S3_OUTPUT_RAS2FIM_FOLDER, "", 1)

            # strip off last folder slash (might be more than one in middle)
            if key_child_folder.endswith("/"):
                key_child_folder = key_child_folder[:-1]

            # strip off last folder slash (might be more than one in middle)
            if key_child_folder.startswith("/"):
                key_child_folder = key_child_folder[1:]

            # We easily could get extra folders that are not huc folders, but we will purge them
            # if it is valid key, add it to a list. Returns a dict.
            # If it does not match a pattern we want, the first element of the tuple will be
            # the word error, but we don't care. We only want valid huc_crs_date pattern folders.
            existing_dic = s3_sf.parse_unit_folder_name(key_child_folder)
            if "error" in existing_dic:  # if error exists, just skip this one
                continue

            # see if the huc and crs it matches the incoming huc number and crs
            if (existing_dic["key_huc"] == src_name_dict["key_huc"]) and (
                existing_dic["key_crs_number"] == src_name_dict["key_crs_number"]
            ):
                s3_unit_folder_names.append(key_child_folder)

        if is_verbose is True:
            RLOG.debug("unit folders found are ...")
            RLOG.debug(s3_unit_folder_names)

    except Exception:
        RLOG.critical("===================")
        RLOG.critical("An critical error has occurred with talking with S3.")
        RLOG.critical(traceback.format_exc())
        dt_string = dt.datetime.ustnow().strftime("%m/%d/%Y %H:%M")
        RLOG.critical(f"Ended (UTC time): {dt_string}")
        sys.exit(1)

    return s3_unit_folder_names


####################################################################
def __add_record_to_tracker(
    bucket_name, src_unit_full_path, orig_folder_name, cur_action, cur_folder_name, s3_folder, is_verbose
):
    """
    Overview:
        The s3 tracker csv will add records to the ras_output_tracker.csv that is in
        S3 output_ras2fim folder. This will only ever add records and never update or
        delete records.

        Note: if something fails, it will not stop the program unless it is a AWS credentials
        error.  Anything else wil be displayed to the user and asked to fix it by hand.
        Why? so we don't try to load the folder / files again into S3.

    Inputs:
        - bucket_name
        - src_unit_full_path: local folder of the huc_crs dir being saved up to s3.
        - orig_folder_name:  eg. 12090301_2276_230815
        - cur_action: initial_load, moved_to_arch, overwriting_prev  (see TRACKER_ACTIONS)
        - cur_folder_name: When folders go to the archive directory, it gets a date and time stamp
          added to the name
        - s3_folder: the folder it is being moved into (output_ras2fim or output_ras2fim_archive)
            eg. output_ras2fim or output_ras2fim_archive

        eg: (bucket_name), c:\ras2fim_data\output_ras2fim\12030202_102739_230810, 12030202,
                102739, 12030202_102739_230810, 'overwriting_prev', 12030202_102739_230810,
                output_ras2fim, False

        or (bucket_name), c:\ras2fim_data\output_ras2fim\12030202_102739_230810, 12030202,
                102739, 12030202_102739_230810, 'moved_to_arch', 12030202_102739_230810_BK_230625_1423,
                output_ras2fim_archive, False

    Outputs:
        None
    """

    # it is in the s3 output_ras2fim
    s3_path_to_tracker_file = f"s3://{bucket_name}/{sv.S3_OUTPUT_RAS2FIM_FOLDER}/{sv.S3_OUTPUT_TRACKER_FILE}"

    try:
        print()
        RLOG.lprint(f"Updating the s3 tracker file at {s3_path_to_tracker_file}")

        huc_crs_folder_dict = s3_sf.parse_unit_folder_name(orig_folder_name)

        # ----------
        # calls over to S3 using the aws creds file even though it doesn't use it directly
        df_cur_tracker = pd.read_csv(s3_path_to_tracker_file, header=0, encoding="unicode_escape")

        # get last tracker ID
        if len(df_cur_tracker) == 0:
            next_tracker_id = 1000
        else:
            last_tracker_id = df_cur_tracker["tracker_id"].max()
            next_tracker_id = last_tracker_id + 1

        new_rec = {}  # dictionary
        new_rec["tracker_id"] = next_tracker_id
        new_rec["HUC"] = str(huc_crs_folder_dict["key_huc"])
        new_rec["CRS"] = str(huc_crs_folder_dict["key_crs_number"])
        new_rec["orig_folder_name"] = orig_folder_name

        dt_stamp = dt.datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S")
        new_rec["dt_action"] = dt_stamp
        new_rec["action"] = cur_action
        new_rec["cur_folder_name"] = cur_folder_name
        new_rec["cur_s3_folder"] = s3_folder

        df_new_rec = pd.DataFrame.from_records([new_rec])

        # save locally, then copy it up to S3. Yes.. we have a very small chance
        # of data conflict if both are updating at the same time (pull down versus push)

        # we need to save it temporarily so we will put it in the source unit folder
        # then delete it once it gets to s3.

        if is_verbose is True:
            RLOG.debug(f"Saving tracker file to {TRACKER_SRC_LOCAL_PATH}")

        # concat original with new record df
        df_tracker = pd.concat([df_cur_tracker, df_new_rec], ignore_index=True)

        # temp save to file system
        df_tracker.to_csv(TRACKER_SRC_LOCAL_PATH, index=False)

        if is_verbose is True:
            RLOG.debug("Starting tracker file upload to s3")

        s3_sf.upload_file_to_s3(
            bucket_name, TRACKER_SRC_LOCAL_PATH, sv.S3_OUTPUT_RAS2FIM_FOLDER, sv.S3_OUTPUT_TRACKER_FILE, False
        )

        if is_verbose is True:
            RLOG.debug("Done upload, removing temp copy")

        os.remove(TRACKER_SRC_LOCAL_PATH)

    except Exception:
        # If anything goes wrong just tell the user to look for it in their src path
        # and ask them to fix it by hand in S3.

        errMsg = (
            "** Error updating the ras output tracker file to S3. Details:\n"
            "\nAll applicable files and folder have been loaded to S3, but"
            f" there was a problem updating the {sv.S3_OUTPUT_TRACKER_FILE} file."
            " Depending where the error occurred for updating the tracker file,"
            f" there may be an updated copy in your {src_unit_full_path} folder.\n\n"
            " Please download the tracker file from S3, make any applicable edits,"
            " and save it back to S3 as quick as reasonably possible.\n\n"
            " Please do not simply re-run this script as it will make duplicate copies"
            " of files and folders in S3."
        )

        if ras2fim_logger.LOG_SYSTEM_IS_SETUP is True:
            print("-----------------")
            RLOG.critical(errMsg)
            RLOG.critical(traceback.format_exc())
            RLOG.critical("-----------------")
            RLOG.critical(traceback.format_exc())
        else:
            print(traceback.format_exc())
        sys.exit(1)


####################################################################
####  Some validation of input, but also creating key variables ######
def __validate_input(src_path_to_unit_output_dir, s3_bucket_name):
    # Some variables need to be adjusted and some new derived variables are created
    # dictionary (key / pair) will be returned

    rtn_varibles_dict = {}

    # ---------------
    # why is this here? might not come in via __main__
    if src_path_to_unit_output_dir == "":
        raise ValueError("Source src_path_to_unit_output_dir parameter value can not be empty")

    if not os.path.exists(src_path_to_unit_output_dir):
        raise ValueError(f"Source unit folder not found at {src_path_to_unit_output_dir}")

    if s3_bucket_name == "":
        raise ValueError("Bucket name parameter value can not be empty")

    if ("/" in s3_bucket_name) or ("\\" in s3_bucket_name) or ("s3:" in s3_bucket_name):
        raise ValueError(
            "Bucket name parameter value invalid. It needs to be a single word"
            " or phrase such as my_xyz or r2f-dev. It can not contain values such as"
            " s3: or forward or back slashes"
        )

    # ---------------
    # we need to split this to seperate variables.
    # e.g src_path_to_unit_output_dir = c:\ras2fim_data\output_ras2fim\12030202_102739_230810

    # "src_unit_dir" becomes (if not already) 12030202_102739_230810
    # "src_unit_full_path" becomes (if not already) c:\ras2fim_data\output_ras2fim\12030202_102739_230810
    # remembering that the path or folder name might be different.
    src_path_to_unit_output_dir = src_path_to_unit_output_dir.replace("/", "\\")
    src_path_segs = src_path_to_unit_output_dir.split("\\")

    # We need the source huc_crs folder name for later and the full path
    rtn_varibles_dict["src_unit_dir"] = src_path_segs[-1]
    # strip of the parent path
    rtn_varibles_dict["src_unit_full_path"] = src_path_to_unit_output_dir

    # --------------------
    # make sure it has a "final" folder and has some contents
    final_dir = os.path.join(rtn_varibles_dict["src_unit_full_path"], sv.R2F_OUTPUT_DIR_FINAL)
    if not os.path.exists(final_dir):
        raise ValueError(
            f"Source unit 'final' folder not found at {final_dir}."
            " Ensure ras2fim has been run to completion and that the full path is submitted in args."
        )

    # check to see that the "final" directory isn't empty
    file_count = len(os.listdir(final_dir))
    if file_count == 0:
        raise ValueError(
            f"Source unit 'final' folder at {final_dir} does not appear to have any files or folders."
        )

    # --------------------
    # check ras2fim output bucket exists

    print()
    msg = f"    Validating that the s3 bucket of {s3_bucket_name} exists"
    if s3_sf.does_s3_bucket_exist(s3_bucket_name) is False:
        raise ValueError(f"{msg} ... does not exist")
    else:
        RLOG.lprint(f"{msg} ... found")

    # --------------------
    # check ras2fim output folder exists
    s3_full_output_path = f"s3://{s3_bucket_name}/{sv.S3_OUTPUT_RAS2FIM_FOLDER}"
    msg = f"    Validating that the S3 output folder of {s3_full_output_path} exists"
    if s3_sf.is_valid_s3_folder(s3_full_output_path) is False:
        raise ValueError(f"{msg} ... does not exist")
    else:
        RLOG.lprint(f"{msg} ... found")
    rtn_varibles_dict["s3_full_output_path"] = s3_full_output_path

    # --------------------
    # check ras2fim archive folder exists
    s3_full_archive_path = f"s3://{s3_bucket_name}/{sv.S3_OUTPUT_RAS2FIM_ARCHIVE_FOLDER}"
    msg = f"    Validating that the S3 archive folder of {s3_full_archive_path} exists"
    if s3_sf.is_valid_s3_folder(s3_full_archive_path) is False:
        raise ValueError(f"{msg} ... does not exist")
    else:
        RLOG.lprint(f"{msg} ... found")
    rtn_varibles_dict["s3_full_archive_path"] = s3_full_archive_path

    return rtn_varibles_dict


####################################################################
if __name__ == "__main__":
    # ---- Samples Inputs
    #    python /tools/ras_unit_to_s3.py -s c:\my_ras\output\12030202_102739_230810 -b xyz_bucket

    # NOTE: pathing inside the bucket can not be changed.
    # The root folder (prefix) is hardcoded to output_ras2fim and the archive folder is
    # hardcoded to output_ras2fim_archive.
    # This will help preserve other tools that are relying on specific s3 pathing.
    # The folder name from the source folder (not path) will automatically becomes the s3 folder name.

    parser = argparse.ArgumentParser(
        description="Saving ras2fim unit output folders back to S3",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-s",
        "--src_unit_dir_path",
        help="REQUIRED: A full defined path including output unit folder\n"
        " ie) c:\my_ras\output\\12030202_102739_230810",
        required=True,
        metavar="",
    )

    # s3 bucket name. Can't default S3 paths due to security.  ie) xyz  of (s3://xyz)
    parser.add_argument(
        "-b",
        "--s3_bucket_name",
        help="REQUIRED: S3 bucket where output ras2fim folders are placed.\n"
        "eg) xyz_bucket from s3://xyz_bucket",
        required=True,
        metavar="",
    )

    parser.add_argument(
        "-v",
        "--is_verbose",
        help="OPTIONAL: Adding this flag will give additional tracing output."
        "Default = False (no extra output)",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    log_file_folder = args["src_unit_dir_path"]  # don't put in logs dir
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # Creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        unit_to_s3(**args)

    except Exception:
        RLOG.critical(traceback.format_exc())
