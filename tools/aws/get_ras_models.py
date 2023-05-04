#!/usr/bin/env python3

import argparse
import pandas as pd
import shutil
import s3fs
import subprocess
import traceback

from aws_base import *
from datetime import datetime

'''
This tool uses a HUC8 number to call over to an AWS S3 models_catalog.csv and will scan
all records in the HUC8's column looking for matches.

For each matched row, it will bring back columns, which will tell which
OWP_ras_models/models/_unprocessed folders to download. 

NOTE: This script is primarily designed for NOAA/OWP use, but if you have access to your own
S3, models_catalog.csv and models folders, you are welcome to use it. We can not grant access to our
NOAA / OWP S3 bucket at this time. 

NOTE: For now.. a limitation exists of needing both a credentials .env (config) file with aws creds AND 
for the user to a have run aws configure on their machine sometime in the past (aka.. the config and credentials
files exist in the root user home ".aws" directory). You don't need to run aws configure each time you use the program

Features for this tool include:
    - Ability to create a list only (log file) without actual downloads (test download)
    - For both pulling from S3 as a source as well as for the local target, the "models" will automatically be
         added at the end of thos provided arguments if/as required.
    - A log file be created in the models/log folder with unique date stamps per run
    - A "verbose" flag can be optionally added as an argument for additional processing details (note: don't over
         use this as it can make errors harder to find). To find an error, simply search the log for the word "error"
    - Filters downloads from the src models catalog to look for status of "ready" only. Also filters out
         model catalog final_key_names starting with "1_" one underscore.
    - Can find huc numbers in the models catalog "hucs" field regardless of string format in that column. 
         It does assume that models catalog has ensure that leading zero's exist.
    - All folders not starting with "__" (two underscores) are as downloads start to ensure old data from previous
         runs does not bleed through. The "__" system is for the "__unprocessed" folder or your own folders if they exist.
    
'''

class Get_Ras_Models(AWS_Base):


    # default values listed in "__main__"  but also here in case code calls direct.. aka. not through "__main__"
    def get_models(self, s3_path_to_catalog, huc_number, list_only = False, 
                   target_owp_ras_models_path = "c:\\ras2fim_data\\OWP_ras_models"):

        '''
        Overview  (and Processing Steps)
        -----------------------
        - Calls over to S3 using the aws creds file.
        - Reads models_catalog.csv (from s3_path_to_catalog), looking through the HUC list for matches.
        - If downloading as well...  (might be a list only)
            - Empty the OWP_ras_models\models folder as we know ras2fim will automatically read all folders in that directory
            - Download all of the folders found using data extracted from columns in the filtered models_catalog.csv file.
                In S3, the source download folder will be the "models" folder, but put into the "models" folder here, 
                beside the models_catalog.csv or its override value.
                The S3 "models" folder, MUST exist beside the model catalog file.
        - Using the models_catalog.csv, only records with the status of 'ready'
        - Note: The target folder value will check to see if it ends with "models" and add it if needed
            
        Inputs
        -----------------------
        - s3_path_to_catalog (str) : ie) s3://xyz/OWP_ras_models/models_catalog.csv (left as full pathing in case of pathing
            changes). In testing, this might be models_catalog_robh.csv
        - huc_number (int)
        - list_only (True / False) : If you override to true, you can get just a list and not the downloads
        - target_owp_ras_models_path (str): Local path you want the files to be loaded too. Default: c\ras2fim_data\OWP_ras_models.
            If the path does not end with a "models" folder, the program will add the word "models" path/folder.
        
        Outputs
        -----------------------
        - All of the actual folders found in the s3 OWP_ras_models based on records from the models catalog
          placed in the local emptied (folders only) target_owp_ras_models_path (assuming not overwritten to list only).
          All folders starting with the phrase "__" (two underscore) will not be removed. ie) __unprocessed or __robh
        - A list of all downloaded files will be placed in the target models folder.

        '''


        self.log_append_and_print("")
        start_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.log_append_and_print("****************************************")        
        self.log_append_and_print(f"Get ras models folders from s3 started: {dt_string}")

        try:

            aws_session = super().get_aws_s3_session()

            # ----------
            # Validate inputs
            self.validate_inputs(s3_path_to_catalog, huc_number, target_owp_ras_models_path)

            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly
            df_all = pd.read_csv(s3_path_to_catalog)

            if (df_all.empty):
                self.log_append_and_print("The model catalog appears to be empty or did not load correctly" )
                return

            df_all["nhdplus_comid"] = df_all["nhdplus_comid"].astype(str)

            if (self.is_verbose == True):
                self.log_append_and_print(f"models catalog raw record count = {len(df_all)}")

            # ----------
            # look for records that are ready, contains the huc number and does not start with 1_
            df_huc = df_all.loc[(df_all['status'] == 'ready') & 
                                (~df_all['final_name_key'].str.startswith("1_")) & 
                                (df_all['hucs'].str.contains(str(huc_number), na = False))]

            if (df_huc.empty):
                self.log_append_and_print(f"No valid records return for {huc_number}. Note: some may have been filtered out. " \
                      "Current filter are: status is ready; final_name_key does not start with 1_; " \
                       "and huc number exists in the huc column." )
                return

            if (self.is_verbose == True):
                self.log_append_and_print(f"Number of model final_name_keys is {len(df_huc)}")

            if (self.is_verbose == True):

                # to see the huc list without column trucations (careful as this could be a huge output)
                with pd.option_context('display.max_columns', None):                    
                    print("df_huc list")
                    pd.set_option('display.max_colwidth', None)
                    print(df_huc)
                    # don't log this
                

            # Save a copy of the filtered models_catalog.csv (or whaver name inputted with date stamp)
            # to OWP_ras_models (or whatever target path).
            # version of it.
            file_dt_string = start_time.strftime("%Y%m%d")
            filtered_catalog_path = os.path.join(target_owp_ras_models_path, f"models_catalog_{file_dt_string}.csv")
            df_huc.to_csv(filtered_catalog_path, index=False)
            self.log_append_and_print(f"Filtered model catalog saved to : {filtered_catalog_path}.\n" \
                                      "Note: This csv represents all models folders that are pending to be downloaded.\n" \
                                      "If one model folder does not exist for download from s3, this list is not updated.")


            # make list from the models_catalog.final_name_key which should be the list of folder names to be downloaded
            folders_to_download = df_huc["final_name_key"].tolist()
            folders_to_download.sort()

            # ----------
            # If inc_download_folders, otherwise we just stop.  Sometimes a list is wanted but not the downloads
            if (list_only == True):
                # write out the log
                for folder_name in folders_to_download:
                    self.log_file_msg += folder_name + "\n"  # don't print, just log
                
                print("List created, skipping download as per list_only flag")
                self.save_logs()
                return

            # loop through df_huc records and using name, pull down 
            num_downloaded = self.download_files(folders_to_download)

            self.log_append_and_print("")
            self.log_append_and_print("Downloads completed")
            self.log_append_and_print(f"Number of folders downloaded {num_downloaded} (not counting skips)")

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



        end_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.log_append_and_print (f"ended: {dt_string}")

        # Calculate duration
        time_duration = end_time - start_time
        self.log_append_and_print(f"Duration: {str(time_duration).split('.')[0]}")
        self.log_append_and_print("")        

        self.save_logs()
        print()



    def validate_inputs(self, s3_path_to_catalog, huc_number, target_owp_ras_models_path):

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
        
        if (target_owp_ras_models_path.lower().endswith("\\models\\") == True):
            self.target_owp_models_folder = target_owp_ras_models_path
        elif (target_owp_ras_models_path.lower().endswith("\\models") == True):
            self.target_owp_models_folder = target_owp_ras_models_path + "\\"
        else: 
            self.target_owp_models_folder = os.path.join(target_owp_ras_models_path, "models")

        # Extract the base s3 bucket name from the catalog pathing.
        # temp remove the s3 tag
        # note: the path was already used and validated
        adj_s3_path = s3_path_to_catalog.replace("s3://", "")
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
        self.src_owp_model_folder_path += "models/"

        self.log_append_and_print(f"Adjusted source download path is {self.bucket_name}/{self.src_owp_model_folder_path}")
        self.log_append_and_print(f"Adjusted target path is {self.target_owp_models_folder}")
        self.log_append_and_print("")


    def download_files(self, folder_list):

        self.log_append_and_print("")
        self.log_append_and_print("......................................")

        # ----------
        # remove folders only from the local OWP_ras_models/models (or overwride), keep files
        # also keep folders starting with "__"
        if (os.path.exists(self.target_owp_models_folder)):
            models_dir_list = os.listdir(self.target_owp_models_folder)

            for model_dir in models_dir_list:
                model_dir_path = os.path.join(self.target_owp_models_folder, model_dir)
                if (model_dir.startswith("__") == False) and (os.path.isdir(model_dir_path)):  #two underscores and is dir
                    shutil.rmtree(model_dir_path)
                    if (self.is_verbose == True):
                        self.log_append_and_print(f"{model_dir_path} folder removed")
        else:
            os.mkdir(self.target_owp_models_folder)
            if (self.is_verbose == True):
                print(f"{self.target_owp_models_folder} created")

        s3 = s3fs.S3FileSystem(anon=False, key = os.getenv('AWS_ACCESS_KEY'), secret = os.getenv('AWS_SECRET_ACCESS_KEY'))

        #not all will be found for download. Print the ones now found, count the rest
        num_downloaded = 0
        num_folders_pending = len(folder_list)
        num_processed = 0
        #root_cmd = super().get_aws_cli_credentials()
        root_cmd = "aws s3 cp --recursive "
        for folder_name in folder_list:

            num_processed += 1

            self.log_append_and_print("")

            src_path = self.bucket_name + "/" + self.src_owp_model_folder_path + folder_name + "/"

            if s3.exists(src_path) == False:
                self.log_append_and_print(f"== {folder_name} - skipped - s3 folder of {src_path} doesn't exist")
                progress_msg = f">>> {num_processed} of {num_folders_pending} processed"
                self.log_append_and_print(progress_msg)
                continue

            target_path = os.path.join(self.target_owp_models_folder, folder_name)
            self.log_append_and_print(f"== {folder_name} - Downloading to path = {target_path}")

            #cmd = root_cmd + f"{src_path} {target_path} --dryrun"
            cmd = root_cmd + f"{src_path} {target_path}"
            if (self.is_verbose):
                self.log_append_and_print(f"    {cmd}")

            process_s3 = subprocess.run(cmd, capture_output=True, text=True)
            if (process_s3.returncode != 0):
                msg = "*** an error occurred\n"
                msg += process_s3.stderr
                self.log_append_and_print(msg)
                progress_msg = f">>> {num_processed} of {num_folders_pending} processed"
                self.log_append_and_print(progress_msg)
                continue

            self.log_append_and_print(f" ----- successful")
            num_downloaded += 1

            progress_msg = f">>> {num_processed} of {num_folders_pending} processed"
            self.log_append_and_print(progress_msg)

        return num_downloaded
    

    # TODO: this should be changed out of here and moved to a common py file
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


    # TODO: this should be changed out of here and moved to a common py file
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

    # Sample Usage (mins) (also downloads related folders)
    #python3 ./tools/aws/get_list_ras_models.py -s s3://xyz/OWP_ras_models/models_catalog.csv -u 12010401

    # NOTE: There are some minor issues with s3 authentication. So.. even though we are passing in a aws creds file,
    # for now, you need to be in your user root home directory and run aws configure (if not already done at some point)
    # If has been run on this machine before, you will find a config file and credentials file in your root /.aws directory
    # ie) /users/{your user name}/.aws

    # Ensure you have made a copy of the aws_creds_template.env file to another location and edited for your actual aws assigned
    # creds. NOAA can not share our credentials to non NOAA/OWP personnel. See the "-c" param for defaults or you can 
    # override it using the -c arg.
            
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services to download OWP_ras_model folders by HUC')
    
    parser.add_argument('-c','--aws_cred_env_file', 
                        help='(Opt) Path to aws credentials env file. Defaults = C:\\ras2fim_data\config\\aws_hv_s3_creds.env',
                        required=False, default="C:\\ras2fim_data\\config\\aws_hv_s3_creds.env")
    
    # False means you get a list only, not the folders downloaded (at \OWP_ras_models\huc_OWP_ras_models.csv)
    parser.add_argument('-d','--list_only', 
                        help='(Opt) True or False to including downloading the folders. Adding this flag will result in a log file " \
                        "only with the list of potential downloadable folders. Default = False (download files)',
                        required=False, default=False, action='store_true')

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/models_catalog.csv
    # Note: the actual models folders are assumed to be a folder named "models" beside the models_catalog.csv
    parser.add_argument('-s','--s3_path_to_catalog', 
                        help='(Reqd) s3 path and file name of models_catalog.', required=True)

    parser.add_argument('-t','--target_owp_ras_models_path',
                        help='(Opt) Location of the root ras2fim owp_ras_models folders. Defaults = c:\\ras2fim_data\\OWP_ras_models',
                        required=False, default="c:\\ras2fim_data\\OWP_ras_models")
    
    parser.add_argument('-u','--huc_number', 
                        help='(Reqd) download model records matching this provided number', required=True)

    parser.add_argument('-v','--is_verbose', 
                        help='(Opt) Adding this flag will give additional tracing output. Default = False (no extra output)',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    obj = Get_Ras_Models(path_to_cred_env_file = args['aws_cred_env_file'], 
                             is_verbose = args['is_verbose'])
    
    obj.get_models(list_only = args['list_only'],
                   s3_path_to_catalog = args['s3_path_to_catalog'],
                   target_owp_ras_models_path = args['target_owp_ras_models_path'],
                   huc_number = args['huc_number']) 
        

