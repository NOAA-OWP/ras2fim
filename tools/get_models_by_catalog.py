#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import re
import shutil
import s3fs
import subprocess
import sys
import time
import traceback

from datetime import datetime

sys.path.append('..')
import ras2fim.src.shared_variables as sv
import ras2fim.src.r2f_validators as val

'''
This tool uses a HUC8 number to call over to an AWS S3 models_catalog.csv and will scan
all records in the models catalog, HUC8's column looking for matches.

NOTE: This script is primarily designed for NOAA/OWP use, but if you have access to your own
S3, models_catalog.csv and models folders, you are welcome to use it. We can not grant access to our
NOAA / OWP S3 bucket at this time. 

To run this tool, you must have already ran 'aws configure' and added creds to the S3 bucket. You should
find that you only need to setup yoru machine once with 'aws configure' and not each time you use this tool.


Features for this tool include:
    - Ability to create a list only (log file) without actual downloads (test download).

    - A log file for the output is created with unique date/time stamp.

    - A "verbose" flag can be optionally added as an argument for additional processing details (note: don't over
         use this as it can make errors harder to find). To find an error, simply search the log for the word "error"

    - Filters downloads from the src models catalog to look for status of "ready" only. Also filters out
         records where the final_name_key starts with either "1_" or "2_".

    - This script can find huc numbers in the models catalog "hucs" field regardless of string format in that column. 
         It does assume that models catalog has ensure that leading zero's exist.

    - This script further filters model records to the matching crs value. Why? ras2fim.py has an input param for its crs (-p)
         and only models matching that crs can be processed at one time. If we have one HUC that has some models in one projection
         and some models in another projection, they must be processed via ras2fim.py separately with separate output folders.

    - As the models folders are about to be downloaded, all subfolders in the local "models" folder will first be removed,
         as ras2fim.py processes all folders in that directory.

    - It creates a filtered copy of the S3 OWP_ras_models_catalog.csv showing all folders downloaded, if they were successful or not
         and if not, why. Local models catalog file name will be "OWP_ras_models_catalog_{HUC}.csv"
    
'''

MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS = "download_success"
MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON = "download_fail_reason"

class Get_Ras_Models_By_Catalog():

    
    # default values listed in "__main__"  but also here in case code calls direct.. aka. not through "__main__"
    def get_models(self, 
                   s3_path_to_catalog_file, 
                   huc_number, 
                   projection,
                   list_only = False, 
                   target_owp_ras_models_path = sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH,
                   target_owp_ras_models_csv_file = sv.DEFAULT_RSF_MODELS_CATALOG_FILE,
                   is_verbose = False):

        '''
        Overview  (and Processing Steps)
        -----------------------
        - Calls over to S3 using a save aws configure creds file (needs to be in place prior to the run)
        - Reads model catalog csv (from s3_path_to_catalog), looking through the HUC list for matches. e.g. OWP_ras_models\OWP_ras_models_catalog.csv
        - If downloading folders...  (ie.. not just the list only)
            - Empty the OWP_ras_models\models (or target models path) folder as we know ras2fim will automatically read all folders in that directory
            - Download all of the folders found using data extracted from columns in the filtered OWP_ras_models_catalog.csv file.
                The S3 "models" folder, MUST exist beside the model catalog file.
        - Uses the OWP_ras_models_catalog.csv (or equiv), only records with the status of 'ready'
        - Current filters when used against the OWP_ras_models_catalog.csv:
              - 'status' column = 'ready'
              - 'hucs' column includes the provided huc_number. Note.. more than one huc can be in that column.
              - 'crs' column matchs the incoming projection value (case-sensitive)
        - If the target_owp_ras_models_csv_file exists, it will be overwritten without warning.
            
        Inputs
        -----------------------
        - s3_path_to_catalog_file (str) : e.g. s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv (left as full pathing in case of pathing
            changes). 
        - huc_number (string)
        - projection (string): To calculate which model folders are to be downloaded, you need the HUC number and the case-senstive CRS number.
            The projection value must match the 'crs' column in the master copy in S3  (OWP_ras_models_catalog.csv)
        - list_only (True / False) : If you override to true, you can get just a csv list and not the downloads
        - target_owp_ras_models_path (str): Local path you want the model folders to be loaded to. Default: c\ras2fim_data\OWP_ras_models\models.
        - target_owp_ras_models_csv_file (str): The file name and path of where to save the filtered catalog. 
            Default: c\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog_[huc number].csv.
        
        Outputs
        -----------------------
        - All of the filtered actual folders found in the s3 OWP_ras_models based on the models catalog are
            placed in the local emptied (folders only) target_owp_ras_models_path (assuming not overwritten to list only).
        - A filtered copy of the "OWP_ras_models_catalog.csv" from S3 will be placed in as set by target_owp_ras_models_csv_file
            and includes list of all downloaded files and their attributes. If not overridden, the name will become OWP_ras_models_catalog_{HUC}.csv. This file
            will overwrite a pre-existing file of the same name. The new OWP_ras_models_catalog_{HUC}.csv is required for ras2fim.py processing.

        '''

        self.log_append_and_print("")
        start_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.log_append_and_print("****************************************")        
        self.log_append_and_print(f"Get ras models folders from s3 started: {dt_string}")


        # ----------
        # Validate inputs
        self.__validate_inputs(s3_path_to_catalog_file, 
                               huc_number, 
                               projection, 
                               target_owp_ras_models_path, 
                               target_owp_ras_models_csv_file,
                               list_only)
        
        self.list_only = list_only
        self.is_verbose = is_verbose
        # from here on, use the self. in front of variables as the variable might have been adjusted

        # setup an empty variable for scope reasons
        self.df_filtered = pd.DataFrame()
        try:

            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly
            df_all = pd.read_csv(self.s3_path_to_catalog_file, header= 0, encoding= 'unicode_escape')

            if (df_all.empty):
                self.log_append_and_print("The model catalog appears to be empty or did not load correctly" )
                return

            df_all["nhdplus_comid"] = df_all["nhdplus_comid"].astype(str)

            if (self.is_verbose == True):
                self.log_append_and_print(f"models catalog raw record count = {len(df_all)}")

            # ----------

            filter_msg = "Note: some may have been filtered out. Current filters are: status is ready;" \
                         " final_name_key does not start with 1_ or 2_; huc number exists in the huc column;" \
                         " and matching crs column values." 

            # look for records that are ready, contains the huc number and does not start with 1_ or 20_
            df_huc = df_all.loc[(df_all['status'] == 'ready') & 
                                (df_all['final_name_key'].str.startswith("1_") == False) & 
                                (df_all['final_name_key'].str.startswith("2_") == False) & 
                                (df_all['hucs'].str.contains(str(self.huc_number), na = False))]

            if (df_huc.empty):
                self.log_append_and_print(f"No valid records return for {self.huc_number}. {filter_msg}")
                return

            # ----------
            # Now filter based on CRS
            self.df_filtered = df_huc.loc[(df_huc['crs'] == self.projection)]
            if (self.df_filtered.empty):
                self.log_append_and_print(f"No valid records return for {huc_number} and crs {self.projection}. {filter_msg}")
                return

            self.df_filtered.reset_index(inplace=True)

            self.log_append_and_print(f"Number of model records after filtering is {len(self.df_filtered)} (pre-download).")

            if (self.is_verbose == True):

                # to see the huc list without column trucations (careful as this could be a huge output)
                with pd.option_context('display.max_columns', None):                    
                    print("df_huc list")
                    pd.set_option('display.max_colwidth', None)
                    print(self.df_filtered)
                    # don't log this
                
            # first add two columns, one to say if download succesful (T/F), the other to say download fail reason
            pd.options.mode.chained_assignment = None
            self.df_filtered.loc[:, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = ''
            self.df_filtered.loc[:, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = ''

            # we will save it initially but update it and save it again as it goes
            self.df_filtered.to_csv(self.target_filtered_csv_path, index=False)
            self.log_append_and_print(f"Filtered model catalog saved to : {self.target_filtered_csv_path}")
            if (self.list_only == False):                          
                self.log_append_and_print("Note: This csv represents all filtered models folders that are pending to be downloaded.\n" \
                                          "The csv will be updated with statuses after downloads are complete.")


            # make list from the models_catalog.final_name_key which should be the list of folder names to be downloaded
            folders_to_download = self.df_filtered["final_name_key"].tolist()
            folders_to_download.sort()

            # ----------
            # If inc_download_folders, otherwise we just stop.  Sometimes a list is wanted but not the downloads
            if (self.list_only == False):
                # loop through df_huc records and using name, pull down 
                # TODO: Add a progress bar and multi proc, BUT....
                # S3 does not like two many simulations downloads at a time. It has to do with the network speed of your machine.
                # Just keep an eye on it when multi threading.
                # For now, it is pretty fast anyways.
                self.download_files(folders_to_download)

            self.log_append_and_print("")
            if (self.list_only == True):
                self.log_append_and_print("List only as per (-f) flag - no downloads attempted")
                self.log_append_and_print(f"Number of model folders which would be attempted to downloaded is {len(self.df_filtered)}")
            else:
                self.log_append_and_print(f"Number of models folders successfully downloaded: {self.num_success_downloads}")
                num_skips = self.num_pending_downloads - self.num_success_downloads
                self.log_append_and_print(f"Number of models folders skipped / errored during download: {num_skips}")
                if (num_skips > 0):
                    self.log_append_and_print(f"Please review the output logs or the filtered csv for skip/error details.")

        except ValueError as ve:
           errMsg = "--------------------------------------" \
                     f"\n An error has occurred"
           errMsg = errMsg + traceback.format_exc()
           # don't log
           print(errMsg)
           sys.exit()

        except Exception as ex:
           errMsg = "--------------------------------------" \
                     f"\n An error has occurred"
           errMsg = errMsg + traceback.format_exc()
           self.log_append_and_print(errMsg)

        # resaved with the updates to the download columns
        if (self.df_filtered.empty == False) and (self.list_only == False):
            self.df_filtered.to_csv(self.target_filtered_csv_path, index=False)
            self.log_append_and_print(f"Filtered model catalog has been update.")

        end_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.log_append_and_print (f"ended: {dt_string}")

        # Calculate duration
        time_duration = end_time - start_time
        self.log_append_and_print(f"Duration: {str(time_duration).split('.')[0]}")

        self.save_logs()
        print()



    def __validate_inputs(self, s3_path_to_catalog_file, huc_number, projection, 
                          target_owp_ras_models_path, target_owp_ras_models_csv_file, list_only):

        '''
        Validates input but also sets up key variables

        If errors are found, an exception will be raised.
        '''

        #---------------
        if (huc_number is None):  # Possible if not coming via the __main__ (also possible)
            raise ValueError("huc number not set")

        if (len(str(huc_number)) != 8):
            raise ValueError("huc number is not eight characters in length")
        
        if (huc_number.isnumeric() == False):
            raise ValueError("huc number is not a number")
        
        self.huc_number = huc_number

        #---------------
        crs_number, is_valid, err_msg = val.is_valid_crs(projection) # I don't need the crs_number for now
        if (is_valid == False):
            raise ValueError(err_msg)
        
        self.projection = projection

        #---------------
        # why is this here? might not come in via __main__
        if (target_owp_ras_models_csv_file == ""):
            raise ValueError("target_owp_ras_models_csv_file can not be empty")
        
        #---------------
        target_owp_ras_models_path = target_owp_ras_models_path.replace('/', '\\')
        self.target_owp_ras_models_path = target_owp_ras_models_path

        if (list_only == False):
            if (os.path.exists(self.target_owp_ras_models_path)):
                shutil.rmtree(self.target_owp_ras_models_path)
                # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
                # so sometimes mkdir can fail if rmtree isn't done
                time.sleep(2) # 2 seconds

            os.mkdir(self.target_owp_ras_models_path)

        # the string may or may not have the [] and that is ok.
        self.target_filtered_csv_path = target_owp_ras_models_csv_file.replace("[]", huc_number)

        #---------------
        # Extract the base s3 bucket name from the catalog pathing.
        # temp remove the s3 tag
        # note: the path was already used and validated
        self.s3_path_to_catalog_file = s3_path_to_catalog_file

        # we need the "s3 part stipped off for now"
        adj_s3_path = self.s3_path_to_catalog_file.replace("s3://", "")
        path_segs = adj_s3_path.split("/")
        self.bucket_name = f"s3://{path_segs[0]}"  

        # lets join the path back together (pulling off the model catalog file as well)
        self.src_owp_model_folder_path = ""
        for index, segment in enumerate(path_segs):
            # drop the first and last (the last segment was the model catalog file name)
            if (index == 0) or (index == len(path_segs)-1):
                continue
            self.src_owp_model_folder_path += segment + "/"

        # The "models" must be beside the models catalog  
        # Note: This value DOES NOT include the bucket name but does add the models folder name
        # ie). OWP_ras_models/models/   or maybe OWP_ras_models_robtest/models
        self.src_owp_model_folder_path = self.bucket_name + "/" + self.src_owp_model_folder_path
        self.src_owp_model_folder_path += "models"        

        #---------------
        # The log files will go to a folder that is one level higher (a parent folder) for the models target.
        target_owp_ras_models_path_parent = os.path.dirname(target_owp_ras_models_path)
        self.log_folder_path = os.path.join(target_owp_ras_models_path_parent, "logs")

        self.log_append_and_print(f"Source download path for models is {self.src_owp_model_folder_path}")
        self.log_append_and_print(f"Target file name and path for the filtered csv is {self.target_filtered_csv_path}")        
        self.log_append_and_print(f"Target path for models is {self.target_owp_ras_models_path}")
        self.log_append_and_print("")


    def download_files(self, folder_list):

        self.log_append_and_print("")
        self.log_append_and_print("......................................")

        s3 = s3fs.S3FileSystem()

        #not all will be found for download. Print the ones now found, count the rest
        num_processed = 0

        self.num_pending_downloads = len(folder_list)
        self.num_success_downloads = 0

        root_cmd = "aws s3 cp --recursive "
        for folder_name in folder_list:

            num_processed += 1

            self.log_append_and_print("")

            src_path = f"{self.src_owp_model_folder_path}/{folder_name}/"
            print(src_path)

            # Get row so we can can update it
            rowIndexes = self.df_filtered.index[self.df_filtered['final_name_key']==folder_name].tolist()
            if (len(rowIndexes) != 1):
                msg = f"Sorry, something went wrong looking the specific record with a final_name_key of {folder_name}"
                self.log_append_and_print(f"== {msg}")
                break

            rowIndex = rowIndexes[0]

            if s3.exists(src_path) == False:
                msg = f"skipped - s3 folder of {src_path} doesn't exist"
                self.log_append_and_print(f"== {folder_name}")
                self.log_append_and_print(f".... {msg}")
                progress_msg = f">>> {num_processed} of {self.num_pending_downloads} processed"
                self.log_append_and_print(progress_msg)

                # update the df (csv) to show it failed and why
                self.df_filtered.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = "False"
                self.df_filtered.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = msg
                continue

            target_path = os.path.join(self.target_owp_ras_models_path, folder_name)
            self.log_append_and_print(f"== {folder_name} - Downloading to path = {target_path}")

            #cmd = root_cmd + f"{src_path} {target_path} --dryrun"
            # NOTE: we are using subprocesses for now as boto3 can not download folders, only files.
            # Granted you can get a list of files matching the prefix and iterate through them but
            # it is ugly. For now, we will use 

            # TODO: what if the target folder already exists. ie) duplicate entry in the final_name_key 
            # column of the master models catalog csv.
            # We need to pre-search the csv and record duplicates


            cmd = root_cmd + f"\"{src_path}\" \"{target_path}\""
            if (self.is_verbose):
                self.log_append_and_print(f"    {cmd}")

            process_s3 = subprocess.run(cmd, capture_output=True, text=True)
            if (process_s3.returncode != 0):
                msg = "*** an error occurred\n"
                msg += process_s3.stderr
                self.log_append_and_print(msg)
                progress_msg = f">>> {num_processed} of {self.num_pending_downloads} processed"
                self.log_append_and_print(progress_msg)

                # so it doesn't interfer with the delimiter
                msg = msg.replace(",", " ")

                # update the df (csv) to show it failed and why
                self.df_filtered.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = "False"
                self.df_filtered.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = msg
                continue

            self.log_append_and_print(f" ----- successful")
            self.df_filtered.at[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = "True"

            #self.df_row[rowIndex] = df_row
            self.num_success_downloads += 1

            progress_msg = f">>> {num_processed} of {self.num_pending_downloads} processed"
            self.log_append_and_print(progress_msg)

        return 
    

    def log_append_and_print(self, msg):

        '''
        Overview  (and Processing Steps)
        -----------------------
        This will start a new log object if required and it will stay with the object (self).
        It will keep appending to the log string until it is ready to be output as one 
        large chuck. It does not attempt to live output.

        AND.. will print to screen

        Note: for each new msg coming in, it will add \n to the end of it
        '''

        # if the attribute (class variable) has not been created yet, do it.
        if (hasattr(self, 'log_file_msg') == False):
            self.log_file_msg = ""

        self.log_file_msg += msg + "\n"
        print(msg)


    def save_logs(self):

        '''
        Overview: Saves built up log file data to a file with a unique name
        '''
        # 

        # We want to attempt to log some types of exceptions if possible. Depending on where it fails,
        # we may or may not yet have assigned self.log_folder_path
        if (hasattr(self, 'log_folder_path') == False):
            return

        if (os.path.exists(self.log_folder_path) == False):
            os.mkdir(self.log_folder_path)

        start_time = datetime.now()
        file_dt_string = start_time.strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(self.log_folder_path, f"get_ras_models_{self.huc_number}-{file_dt_string}.log")

        with open(log_file_path, 'w') as log_file:
            log_file.write(self.log_file_msg)
            print(f"log file created as {log_file_path}")


if __name__ == '__main__':


    # ***********************
    # This tool is intended for NOAA/OWP staff only as it requires access to an AWS S3 bucket with a specific folder structure.
    # If you create your own S3 bucket in your own AWS account, you are free to use this tool
    # ***********************

    # ----------------------------
    # Sample Usage (min required args) (also downloads related folders)
    # python3 ./tools/get_ras_models_by_catalog.py -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv -u 12090301 -p ESRI:102739

    # Sample Usage with most params
    # python3 ./tools/get_ras_models_by_catalog.py -u 12090301 -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
    #  -tm c:\\ras2fim\\ras_models -tcsv c:\\ras2fim\ras_models\12090301_models_catalog.csv -v -p ESRI:102739
    
    # Sample Usage to get just the list and not the model folders downloaded (and most defaults)
    # python3 ./tools/get_ras_models_by_catalog.py -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv -u 12090301 -p EPSG:2277 -f

    # ----------------------------
    # NOTE: You need to have aws credentials already run on this machine, at least once per server.
    # This is done usually using `aws configure`.  
    # If has been run on this machine before, you will find a config file and credentials file in your root /.aws directory
    # ie) /users/{your user name}/.aws

    # NOTE: This script assumes that each of the downloadable "model" folders are in subfolder named "models" and under where 
    # the -s / --sub_path_to_catalog file is at.
    # e.g.  s3://(some bucket)/OWP_ras_models/OWP_ras_models_catalog.csv with a "models" folder beside it with 
    # all of the "model" folders under that.
    #     ie)  s3://(some bucket)/OWP_ras_models/OWP_ras_models_catalog.csv
    #                                           /models/
    #                                                   /(model number 1 folder)
    #                                                   /(model number 2 folder)
    #                                                   /(model number 3 folder)
    #                                                   /etc
    # And of course, your path doesn't have to match, but the pattern of the csv, "models" folder, and subfolders is critical
            
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services to download OWP_ras_model folders by HUC')

    # False means you get a list only, not the folders downloaded (at \OWP_ras_models\huc_OWP_ras_models.csv)

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
    # Note: the actual models folders are assumed to be a folder named "models" beside the models_catalog.csv
    parser.add_argument('-s', '--s3_path_to_catalog_file', 
                        help='REQUIRED: S3 path and file name of models_catalog.',
                        required=True, metavar='')

    parser.add_argument('-u','--huc_number', 
                        help='REQUIRED: Download model records matching this provided number',
                        required=True, metavar='')

    parser.add_argument('-p','--projection', 
                        help='REQUIRED: All model folders have an applicable crs value, but one HUC might have some models in one'\
                            ' crs and another set of models in a different crs. This results in one HUC having' \
                            ' to be processed twice (one per crs). Please enter the crs value from the OWP_ras_models_catalog.csv' \
                            ' to help filter the correct models you need. Example: EPSG:2277 (case sensitive)',
                        required=True, metavar='', type=str)

    parser.add_argument('-f','--list_only', 
                        help='OPTIONAL: Adding this flag will result in a log file only with the list of potential downloadable folders, " \
                        "but not actually download the folders. Default = False (download files)',
                        required=False, default=False, action='store_true')

    parser.add_argument('-tcsv','--target_owp_ras_models_csv_file',
                        help='OPTIONAL: Path and file name of where to save the filtered models catalog csv.' \
                             ' Defaults = c:\\ras2fim_data\\OWP_ras_models\\OWP_ras_models_catalog_[huc number].csv.',
                        required=False, default=sv.DEFAULT_RSF_MODELS_CATALOG_FILE, metavar='')

    parser.add_argument('-tm','--target_owp_ras_models_path',
                        help='OPTIONAL: Where to download the model folders such the root ras2fim owp_ras_models folders.' \
                             ' Defaults = c:\\ras2fim_data\\OWP_ras_models\\models',
                        required=False, default=sv.DEFAULT_OWP_RAS_MODELS_MODEL_PATH, metavar='')

    parser.add_argument('-v','--is_verbose', 
                        help='OPTIONAL: Adding this flag will give additional tracing output. Default = False (no extra output)',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    obj = Get_Ras_Models_By_Catalog()
    obj.get_models(list_only = args['list_only'],
                   s3_path_to_catalog_file = args['s3_path_to_catalog_file'],
                   target_owp_ras_models_path = args['target_owp_ras_models_path'],
                   target_owp_ras_models_csv_file = args['target_owp_ras_models_csv_file'],
                   huc_number = args['huc_number'],
                   projection = args['projection'],
                   is_verbose = args['is_verbose']) 
        
