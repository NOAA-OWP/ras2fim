#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import shutil
import s3fs
import subprocess
import time
import traceback

#from aws_base import *
from datetime import datetime

'''
This tool uses a HUC8 number to call over to an AWS S3 models_catalog.csv and will scan
all records in the models catalog, HUC8's column looking for matches.

NOTE: This script is primarily designed for NOAA/OWP use, but if you have access to your own
S3, models_catalog.csv and models folders, you are welcome to use it. We can not grant access to our
NOAA / OWP S3 bucket at this time. 



NOTE: For now.. a limitation exists of needing both a credentials .env (config) file with aws creds AND 
for the user to a have run aws configure on their machine sometime in the past (aka.. the config and credentials
files exist in the root user home ".aws" directory). You don't need to run aws configure each time you use the program




Features for this tool include:
    - Ability to create a list only (log file) without actual downloads (test download).

        # TODO: What am I trying to say here?
    - For both pulling from S3 as a source as well as for the local target, the "models" will automatically be
         added at the end of those provided arguments if/as required.

    - A log file be created in the models/log folder with unique date stamps per run.

    - A "verbose" flag can be optionally added as an argument for additional processing details (note: don't over
         use this as it can make errors harder to find). To find an error, simply search the log for the word "error"

    - Filters downloads from the src models catalog to look for status of "ready" only. Also filters out
         records where the final_name_key starts with either "1_" or "2_".

    - This script can find huc numbers in the models catalog "hucs" field regardless of string format in that column. 
         It does assume that models catalog has ensure that leading zero's exist.

    - As the models folders are about to be downloaded, all subfolders in the local "models" folder will first be removed,
         as ras2fim.py processes all folders in that directory.

    - It creates a filtered copy of the S3 OWP_ras_models_catalog.csv showing all folders downloaded, if they were successful or not
         and if not, why. Local models catalog file name will be "OWP_ras_models_catalog_{HUC}.csv"
    
'''

MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS = "download_success"
MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON = "download_fail_reason"

#class Get_Ras_Models_By_HUC(AWS_Base):
class Get_Ras_Models_By_HUC():

    
    # default values listed in "__main__"  but also here in case code calls direct.. aka. not through "__main__"
    def get_models(self, 
                   s3_path_to_catalog_file, 
                   huc_number, 
                   list_only = False, 
                   target_owp_ras_models_path = "c:\\ras2fim_data\\OWP_ras_models",
                   is_verbose = False):

        '''
        Overview  (and Processing Steps)
        -----------------------
        - Calls over to S3 using a save aws configure creds file (needs to be in place prior to the run)
        - Reads model catalog csv (from s3_path_to_catalog), looking through the HUC list for matches. e.g. OWP_ras_models\OWP_ras_models_catalog.csv
        - If downloading folders...  (ie.. not just the list only)
            - Empty the OWP_ras_models\models folder as we know ras2fim will automatically read all folders in that directory
            - Download all of the folders found using data extracted from columns in the filtered models_catalog.csv file.
                In S3, the source download folder will be the "models" folder, but put into the "models" folder here, 
                beside the OWP_ras_models_catalog.csv or its overriden value.
                The S3 "models" folder, MUST exist beside the model catalog file.
        - Using the OWP_ras_models_catalog.csv (or equiv), only records with the status of 'ready'
        - Note: The target folder value will check to see if it ends with "models" and add it if needed
            
        Inputs
        -----------------------
        - s3_path_to_catalog_file (str) : e.g. s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv (left as full pathing in case of pathing
            changes). 
        - huc_number (string)
        - list_only (True / False) : If you override to true, you can get just a csv list and not the downloads
        - target_owp_ras_models_path (str): Local path you want the files to be loaded to. Default: c\ras2fim_data\OWP_ras_models.
            If the path does not end with a "models" folder, the program will add the word "models" path/folder.
        
        Outputs
        -----------------------
        - All of the filtered actual folders found in the s3 OWP_ras_models based on the models catalog are
            placed in the local emptied (folders only) target_owp_ras_models_path (assuming not overwritten to list only).
         - a copy of the "OWP_ras_models_catalog.csv" from S3 will be placed in the target_owp_ras_models_path directory and includes
            list of all downloaded files and their attributes. The name will become OWP_ras_models_catalog_{HUC}.csv. This file
            will overwrite a pre-existing file of the same name.

        '''


        self.log_append_and_print("")
        start_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.log_append_and_print("****************************************")        
        self.log_append_and_print(f"Get ras models folders from s3 started: {dt_string}")


        # ----------
        # Validate inputs
        self.validate_inputs(s3_path_to_catalog_file, huc_number, target_owp_ras_models_path)
        self.list_only = list_only
        self.is_verbose = is_verbose
        # from here on, use the self. in front of variables as the variable might have been adjusted

        # setup an empty variable for scope reasons
        self.df_huc = pd.DataFrame()
        try:

            #aws_session = super().get_aws_s3_session()

            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly
            #df_all = pd.read_csv(self.s3_path_to_catalog_file, sep=",", encoding= 'unicode_escape')
            df_all = pd.read_csv(self.s3_path_to_catalog_file, header= 0, encoding= 'unicode_escape')

            if (df_all.empty):
                self.log_append_and_print("The model catalog appears to be empty or did not load correctly" )
                return

            df_all["nhdplus_comid"] = df_all["nhdplus_comid"].astype(str)

            if (self.is_verbose == True):
                self.log_append_and_print(f"models catalog raw record count = {len(df_all)}")

            # ----------
            # look for records that are ready, contains the huc number and does not start with 1_ or 20_
            self.df_huc = df_all.loc[(df_all['status'] == 'ready') & 
                                     (df_all['final_name_key'].str.startswith("1_") == False) & 
                                     (df_all['final_name_key'].str.startswith("2_") == False) & 
                                     (df_all['hucs'].str.contains(str(huc_number), na = False))]

            if (self.df_huc.empty):
                self.log_append_and_print(f"No valid records return for {huc_number}. Note: some may have been filtered out. " \
                      "Current filter are: status is ready; final_name_key does not start with 1_ or 2_; " \
                       "and huc number exists in the huc column." )
                return

            self.df_huc.reset_index(inplace=True)

            self.log_append_and_print(f"Number of model records after filtering is {len(self.df_huc)} (pre-download).")

            if (self.is_verbose == True):

                # to see the huc list without column trucations (careful as this could be a huge output)
                with pd.option_context('display.max_columns', None):                    
                    print("df_huc list")
                    pd.set_option('display.max_colwidth', None)
                    print(self.df_huc)
                    # don't log this
                
            # first add two columns, one to say if download succesful (T/F), the other to say download fail reason
            pd.options.mode.chained_assignment = None
            self.df_huc.loc[:, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = ''
            self.df_huc.loc[:, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = ''

            # we will save it initially but update it and save it again as it goes
            self.df_huc.to_csv(self.target_filtered_csv_path, index=False)
            self.log_append_and_print(f"Filtered model catalog saved to : {self.target_filtered_csv_path}")
            if (self.list_only == False):                          
                self.log_append_and_print("Note: This csv represents all filtered models folders that are pending to be downloaded.\n" \
                                          "The csv will be updated with statuses after downloads are complete.")


            # make list from the models_catalog.final_name_key which should be the list of folder names to be downloaded
            folders_to_download = self.df_huc["final_name_key"].tolist()
            folders_to_download.sort()

            # ----------
            # If inc_download_folders, otherwise we just stop.  Sometimes a list is wanted but not the downloads
            if (self.list_only == False):
                # loop through df_huc records and using name, pull down 
                num_downloaded = self.download_files(folders_to_download)

            self.log_append_and_print("")
            if (self.list_only == True):
                self.log_append_and_print("List only as per (-d) flag - no downloads attempted")
                self.log_append_and_print(f"Number of folders which would be attempted to downloaded is {len(self.df_huc)}")
            else:
                self.log_append_and_print("Downloads completed")
                self.log_append_and_print(f"Number of folders downloaded {num_downloaded} (skips removed)")

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
        if (self.df_huc.empty == False) and (self.list_only == False):
            self.df_huc.to_csv(self.target_filtered_csv_path, index=False)
            self.log_append_and_print(f"Filtered model catalog has been update.")

        end_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.log_append_and_print (f"ended: {dt_string}")

        # Calculate duration
        time_duration = end_time - start_time
        self.log_append_and_print(f"Duration: {str(time_duration).split('.')[0]}")

        self.save_logs()
        print()



    def validate_inputs(self, s3_path_to_catalog_file, huc_number, target_owp_ras_models_path, ):

        '''
        If errors are found, an exception will be raised.
        '''

        # huc number is valid

        if (huc_number is None):  # Possible if not coming via the __main__ (also possible)
            raise ValueError("huc number not set")

        if (len(str(huc_number)) != 8):
            raise ValueError("huc number is not eight characters in length")
        
        if (huc_number.isnumeric() == False):
            raise ValueError("huc number is not a number")

        # target_owp_ras_models_path exists
        if (not os.path.exists(target_owp_ras_models_path)):
            raise ValueError(f"Target owp_ras_models folder of '{target_owp_ras_models_path}' does not exist")
        
        # this is the target path without the "models" subfolder added
        self.target_owp_ras_models_path = target_owp_ras_models_path
        
        # optionally ends with \\ or no \\ (adjusted for escaping chars)
        if (target_owp_ras_models_path.lower().endswith("\\models\\") == True):
            self.target_owp_models_folder = target_owp_ras_models_path
        elif (target_owp_ras_models_path.lower().endswith("\\models") == True):
            self.target_owp_models_folder = target_owp_ras_models_path + "\\"  # add \\
        else: 
            self.target_owp_models_folder = f"{target_owp_ras_models_path}\\models"

        self.target_filtered_csv_path = f"{self.target_owp_ras_models_path}\\OWP_ras_models_catalog_{huc_number}.csv"

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

        self.log_append_and_print(f"Adjusted source download path for models is {self.src_owp_model_folder_path}")
        self.log_append_and_print(f"Adjusted target path is {self.target_owp_models_folder}")
        self.log_append_and_print("")


    def download_files(self, folder_list):

        self.log_append_and_print("")
        self.log_append_and_print("......................................")

        # ----------
        # remove folders only from the local OWP_ras_models/models (or overwride), keep files
        if (os.path.exists(self.target_owp_models_folder)):
            shutil.rmtree(self.target_owp_models_folder)
            # shutil.rmtree is not instant, it sends a command to windows, so do a quick time out here
            # so sometimes mkdir can fail if rmtree isn't done
            time.sleep(2) # 2 seconds

        os.mkdir(self.target_owp_models_folder)
        if (self.is_verbose == True):
            print(f"{self.target_owp_models_folder} created")

        s3 = s3fs.S3FileSystem(anon=False, key = os.getenv('AWS_ACCESS_KEY'), secret = os.getenv('AWS_SECRET_ACCESS_KEY'))

        #not all will be found for download. Print the ones now found, count the rest
        num_downloaded = 0
        num_folders_pending = len(folder_list)
        num_processed = 0

        root_cmd = "aws s3 cp --recursive "
        for folder_name in folder_list:

            num_processed += 1

            self.log_append_and_print("")

            src_path = f"{self.src_owp_model_folder_path}/{folder_name}/"
            print(src_path)

            # Get row so we can can update it
            rowIndexes = self.df_huc.index[self.df_huc['final_name_key']==folder_name].tolist()
            if (len(rowIndexes) != 1):
                msg = f"Sorry, something went wrong looking the specific record with a final_name_key of {folder_name}"
                self.log_append_and_print(f"== {msg}")
                break

            rowIndex = rowIndexes[0]

            if s3.exists(src_path) == False:
                msg = f"skipped - s3 folder of {src_path} doesn't exist"
                self.log_append_and_print(f"== {folder_name} - {msg}")
                progress_msg = f">>> {num_processed} of {num_folders_pending} processed"
                self.log_append_and_print(progress_msg)

                # update the df (csv) to show it failed and why
                self.df_huc.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = "False"
                self.df_huc.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = msg
                continue

            target_path = os.path.join(self.target_owp_models_folder, folder_name)
            self.log_append_and_print(f"== {folder_name} - Downloading to path = {target_path}")

            #cmd = root_cmd + f"{src_path} {target_path} --dryrun"
            # NOTE: we are using subprocesses for now as boto3 can not download folders, only files.
            # Granted you can get a list of files matching the prefix and iterate through them but
            # it is ugly. For now, we will use 
            cmd = root_cmd + f"\"{src_path}\" \"{target_path}\""
            if (self.is_verbose):
                self.log_append_and_print(f"    {cmd}")

            process_s3 = subprocess.run(cmd, capture_output=True, text=True)
            if (process_s3.returncode != 0):
                msg = "*** an error occurred\n"
                msg += process_s3.stderr
                self.log_append_and_print(msg)
                progress_msg = f">>> {num_processed} of {num_folders_pending} processed"
                self.log_append_and_print(progress_msg)

                # so it doesn't interfer with the delimiter
                msg = msg.replace(",", " ")

                # update the df (csv) to show it failed and why
                self.df_huc.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = "False"
                self.df_huc.loc[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_FAIL_REASON] = msg
                continue

            self.log_append_and_print(f" ----- successful")
            self.df_huc.at[rowIndex, MODELS_CATALOG_COLUMN_DOWNLOAD_SUCCESS] = "True"

            #self.df_row[rowIndex] = df_row
            num_downloaded += 1

            progress_msg = f">>> {num_processed} of {num_folders_pending} processed"
            self.log_append_and_print(progress_msg)

        return num_downloaded
    

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
        # we may or may not yet have assigned self.target_owp_models_folder
        if (hasattr(self, 'target_owp_models_folder') == False):
            return

        # At this point, the target_owp_models_folder has a "models" ending subfolder, so we need to as the models folder to logs
        # May or may not have a slash on the end, but we have to be careful not to pull out the name model in the middle
        if (self.target_owp_models_folder.endswith("\\")):
            self.target_owp_models_folder = self.target_owp_models_folder[:len(self.target_owp_models_folder) - 1]

        root_model_path = self.target_owp_models_folder[:len(self.target_owp_models_folder) - len("\\models")]
        log_folder = os.path.join(root_model_path, "logs")
        if (os.path.exists(log_folder) == False):
            os.mkdir(log_folder)

        start_time = datetime.now()
        file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
        log_file_path = os.path.join(log_folder, f"get_ras_models__{file_dt_string}.log")

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
    # python3 ./tools/aws/get_ras_models_by_HUC.py -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv -u 12090301

    # Sample Usage with most params
    # python3 ./tools/aws/get_ras_models_by_HUC.py -c C:\\ras2fim\\aws_creds.env -u 12090301 -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv -t c:\\ras2fim\\ras_models -v
    
    # Sample Usage to get just the list and not the model folders downloaded (and most defaults)
    # python3 ./tools/aws/get_ras_models_by_HUC.py -s s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv -u 12090301 -d

    # ----------------------------
    # NOTE: There are some minor issues with s3 authentication. So.. even though we are passing in a aws creds file,
    # for now, you need to be in your user root home directory and run aws configure (if not already done at some point)
    # If has been run on this machine before, you will find a config file and credentials file in your root /.aws directory
    # ie) /users/{your user name}/.aws

    # Ensure you have made a copy of the aws_creds_template.env file to another location and edited for your actual aws assigned
    # creds. NOAA can not share our credentials to non NOAA/OWP personnel. See the "-c" param for defaults or you can 
    # override it using the -c arg.

    # NOTE: This script assumes that each of the downloadable "model" folders are in subfolder named "models" and under where 
    # the -s / --sub_path_to_catalog file is at.
    # e.g.  s3://(some bucket)/OWP_ras_models/OWP_ras_models_catalog.csv with a "models" folder beside it with 
    # all of teh "model" folders under that.
    #     ie)  s3://(some bucket)/OWP_ras_models/OWP_ras_models_catalog.csv
    #                                           /models/
    #                                                   /(single model subfolder number 1)
    #                                                   /(single model subfolder number 2)
    #                                                   /(single model subfolder number 3)    
    #                                                   /etc
    # And of course, your path doesn't have to match, but the pattern of the csv, "models" folder, and subfolders is critical
            
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services to download OWP_ras_model folders by HUC')
    
    #parser.add_argument('-c','--aws_cred_env_file', 
    #                    help='(Opt) Path to aws credentials env file. Defaults = C:\\ras2fim_data\config\\aws_hv_s3_creds.env',
    #                    required=False, default="C:\\ras2fim_data\\config\\aws_hv_s3_creds.env")
    
    # False means you get a list only, not the folders downloaded (at \OWP_ras_models\huc_OWP_ras_models.csv)
    parser.add_argument('-d','--list_only', 
                        help='(Opt) Adding this flag will result in a log file only with the list of potential downloadable folders, " \
                        "but not actually download the folders. Default = False (download files)',
                        required=False, default=False, action='store_true')

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/OWP_ras_models_catalog.csv
    # Note: the actual models folders are assumed to be a folder named "models" beside the models_catalog.csv
    parser.add_argument('-s','--s3_path_to_catalog_file', 
                        help='(Reqd) s3 path and file name of models_catalog.',
                        required=True)

    parser.add_argument('-t','--target_owp_ras_models_path',
                        help='(Opt) Where to download the folders such the root ras2fim owp_ras_models folders. Defaults = c:\\ras2fim_data\\OWP_ras_models',
                        required=False, default="c:\\ras2fim_data\\OWP_ras_models")
    
    parser.add_argument('-u','--huc_number', 
                        help='(Reqd) download model records matching this provided number',
                        required=True)

    parser.add_argument('-v','--is_verbose', 
                        help='(Opt) Adding this flag will give additional tracing output. Default = False (no extra output)',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    #obj = Get_Ras_Models_By_HUC(path_to_cred_env_file = args['aws_cred_env_file'], 
    #                            is_verbose = args['is_verbose'])

    obj = Get_Ras_Models_By_HUC()
    obj.get_models(list_only = args['list_only'],
                   s3_path_to_catalog_file = args['s3_path_to_catalog_file'],
                   target_owp_ras_models_path = args['target_owp_ras_models_path'],
                   huc_number = args['huc_number'],
                   is_verbose = args['is_verbose']) 
        

