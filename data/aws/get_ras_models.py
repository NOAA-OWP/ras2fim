#!/usr/bin/env python3


import argparse
#import io
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

'''

class Get_Ras_Models(AWS_Base):


    # default values listed in "__main__"  but also here in case code calls direct.. aka. not through "__main__"
    def get_models(self, s3_path_to_catalog, huc_number, inc_download_folders=True, 
                   target_owp_ras_models_path = "c\\ras2fim_data\\OWP_ras_models"):

        '''
        Overview  (and Processing Steps)
        -----------------------
        - calls over to S3 using the aws creds file
        - reads models_catalog.csv (from s3_path_to_catalog), looking through the HUC list for matches
        - if downloading as well...  (might be a list only)
            - empty the OWP_ras_models\models folder as we know ras2fim will automatically read all folders in that directory
            - download all of the folders found using data extracted from columns in the filtered models_catalog.csv file.
                In S3, the source download folder will be the "models" folder, but put into the "models" folder here, 
                beside the models_catalog.csv or its override value.
        - Using the models_catalog.csv, only records with the status of 'unprocessed'
        - Note: The target folder value will check to see if it ends with "models" and add it if needed
            
        Inputs
        -----------------------
        - s3_path_to_catalog (str) : ie) s3://xyz/OWP_ras_models/models_catalog.csv (left as full pathing in case of pathing
            changes). In testing, this might be models_catalog_robh.csv
        - huc_number (int)
        - inc_download_folders (True / False) : If you override to false, you can get just a list and not the downloads
        - target_owp_ras_models_path (str): Local path you want the files to be loaded too. Default: c\ras2fim_data\OWP_ras_models.
            if the word "models" does not already exist at the end of the path, the program will add the word "models".
        
        Outputs
        -----------------------
        - all of the actual folders found in the s3 OWP_ras_models based on records from the models catalog
          placed in the local emptied (folders only) target_owp_ras_models_path (assuming not overwritten to list only).
          All folders starting with the phrase "__" (two underscore) will not be removed. ie) __unprocessed or __robh
        - a list of all downloaded files will be placed in the target models folder.

        '''


        start_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        #file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")

        # build up a string of log messages (yes.. should be using the logger class). I want to build it up 
        # and not continuously write to the file as the logger does.
        self.log_file_msg = ""
        #self.log_file_path = os.path.join(self.target_owp_models_folder, f"get_ras_models__{file_dt_string}.log")

        msg = f"Get ras models folders from s3 started: {dt_string}"
        print (msg)
        self.log_file_msg += msg + "\n"

        try:

            aws_session = super().get_aws_s3_session()

            # ----------
            # Validate inputs
            self.validate_inputs(s3_path_to_catalog, huc_number, target_owp_ras_models_path)

            # TODO show / log params

            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly
            df_all = pd.read_csv(s3_path_to_catalog)

            # TODOD: what it if doesn't find it, or it is empty

            if (df_all.empty):
                print("The model catalog appears to be empty or did not load correctly" )
                return

            df_all["nhdplus_comid"] = df_all["nhdplus_comid"].astype(str)

            if (self.is_verbose == True):
                print(f"df_all list count = {len(df_all)}")
                # log

            # ----------
            # look for records that are unprocessed, contains the huc number and does not start with 1_
            df_huc = df_all.loc[(df_all['status'] == 'unprocessed') & 
                                (~df_all['final_name_key'].str.startswith("1_")) & 
                                (df_all['hucs'].str.contains(str(huc_number), na = False))]

            if (df_huc.empty):
                print(f"No valid records return for {huc_number}. Note: some may have been filtered out. " \
                      "Current filter are: status is unprocessed; final_name_key does not start with 1_; " \
                       "and huc number exists in the huc column." )
                return

            print(f"Number of model folders to download is {len(df_all)}")
            # TODO log

            if (self.is_verbose == True):
                with pd.option_context('display.max_columns', None):                    
                    print("df_huc list")
                    pd.set_option('display.max_colwidth', None)
                    print(df_huc)
                    self.log_file_msg += df_huc.to_string() + "\n"

            # ----------
            # remove folders only from the local OWP_ras_models/models (or overwride), keep files
            # also keep folders starting with "__"

            if (os.path.exists(self.target_owp_models_folder)):
                models_dir_list = os.listdir(self.target_owp_models_folder)

                for model_dir in models_dir_list:
                    if (not model_dir.startswith("__")):  #two underscores
                        model_dir_path = os.path.join(self.target_owp_models_folder, model_dir)
                        shutil.rmtree(model_dir_path)
                        if (self.is_verbose == True):
                            msg = f"{model_dir_path} folder removed"
                            print(msg)
                            self.log_file_msg += msg + "\n"
            else:
                os.mkdir(self.target_owp_models_folder)
                if (self.is_verbose == True):
                    print(f"{self.target_owp_models_folder} created")
                
            # ----------
            # If inc_download_folders, otherwise we just stop.  Sometimes a list is wanted but not the downloads

            # make list from the models_catalog.final_name_key which should be the list of folder names to be downloaded
            folders_to_download = df_huc["final_name_key"].tolist()
            folders_to_download.sort()

            if (inc_download_folders == False):
                # write out the log
                for folder_name in folders_to_download:
                    self.log_file_msg += folder_name + "\n"
                
                print("List created at ... ")
                return

            # loop through df_huc records and using name, pull down 
            num_downloaded = self.download_files(s3_path_to_catalog, folders_to_download)


        except Exception as ex:
           errMsg = "--------------------------------------" \
                     f"\n An error has occurred"
           errMsg = errMsg + traceback.format_exc()
           print(errMsg, flush=True)
           #log_error(self.fim_directory, usgs_elev_flag,
           #             hydro_table_flag, src_cross_flag, huc_id, errMsg)


        #print(df)
        print()
        print("Downloads completed")
        print(f"number of folders downloaded {num_downloaded}")

        end_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print (f"ended: {dt_string}")

        # Calculate duration
        time_duration = end_time - start_time
        print(f"Duration: {str(time_duration).split('.')[0]}")
        print()        


    def validate_inputs(self, s3_path_to_catalog, huc_number, target_owp_ras_models_path):

        '''
        If errors are found, an exception will be raised.
        '''

        # TODO

        # s3 bucket file exists (and creds by default)

        # huc number is valid

        # target_owp_ras_models_path exists
        if (not os.path.exists(target_owp_ras_models_path)):
            raise ValueError(f"Target owp_ras_models folder of '{target_owp_ras_models_path}' does not exist")
        
        self.target_owp_ras_models_path = target_owp_ras_models_path
        if (self.target_owp_ras_models_path.endswith("\\")):
            self.target_owp_ras_models_path = self.target_owp_ras_models_path[:-1]

        if (not self.target_owp_ras_models_path.endswith("models")):
            self.target_owp_models_folder = os.path.join(self.target_owp_ras_models_path, "models")



    def download_files(self, s3_path_to_catalog, folder_list):

        # Extract the base s3 bucket name from the catalog pathing.
        # temp remove the s3 tag
        # note: the path was already used and validated
        adj_s3_path = s3_path_to_catalog.replace("s3://", "")
        print(adj_s3_path)
        path_segs = adj_s3_path.split("/")
        print(path_segs)

        bucket_name = f"s3://{path_segs[0]}"
        print(bucket_name)

        # lets join the path back together
        src_owp_model_folder_path = ""        
        for index, segment in enumerate(path_segs):
            # drop the first and last
            if (index == 0) or (index == len(path_segs)-1):
                continue
            src_owp_model_folder_path += segment + "/"
            print(f"index is {index} - {src_owp_model_folder_path}")

        if (not src_owp_model_folder_path.endswith("models/")):
            src_owp_model_folder_path += "models/"

        msg = f"Download src path is {src_owp_model_folder_path}\n"
        self.log_file_msg += msg
        print(msg)

        s3 = s3fs.S3FileSystem(anon=False, key = os.getenv('AWS_ACCESS_KEY'), secret = os.getenv('AWS_SECRET_ACCESS_KEY'))

        #not all will be found for download. Print the ones now found, count the rest
        num_downloaded = 0
        #root_cmd = super().get_aws_cli_credentials()
        root_cmd = "aws s3 cp --recursive "
        for folder_name in folder_list:
            src_path = bucket_name + "/" + src_owp_model_folder_path + folder_name + "/"

            if not s3.exists(src_path):
                msg = f"s3 folder of {src_path} doesn't exist\n"
                self.log_file_msg += msg
                print(msg)
                continue

            target_path = os.path.join(self.target_owp_models_folder, folder_name)
            msg = f" -- {folder_name} - Downloading to path = {target_path}\n"
            self.log_file_msg += msg
            print(msg)
                

            #cmd = root_cmd + f"{src_path} {target_path} --dryrun"
            cmd = root_cmd + f"{src_path} {target_path}"
            #print(cmd)
            #print()

            process_s3 = subprocess.run(cmd, capture_output=True, text=True)
            if (process_s3.returncode != 0):
                msg = "*** an error occurred\n"
                msg += process_s3.stderr
                self.log_file_msg += msg
                print(msg)
                continue

            msg = f" ---- {folder_name} successful\n"
            self.log_file_msg += msg
            print(msg)
            num_downloaded += 1

        return num_downloaded


if __name__ == '__main__':

    # Sample Usage (mins) (also downloads related folders)
    #python3 get_list_ras_models.py -s s3://xyz/OWP_ras_models/models_catalog.csv -u 12010401
            
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services')
    
    parser.add_argument('-c','--aws_cred_env_file', 
                        help='path to aws credentials env file', required=False, default="C:\\ras2fim_data\\config\\aws_hv_s3_creds.env")
    
    # False means you get a list only, not the folders downloaded (at \OWP_ras_models\huc_OWP_ras_models.csv)
    parser.add_argument('-d','--inc_download_folders', 
                        help='True or False to including downloading the folders', required=False, default=True, action='store_true')

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/models_catalog.csv
    # Note: the actual models folders are assumed to be a folder named "models" beside the models_catalog.csv
    parser.add_argument('-s','--s3_path_to_catalog', 
                        help='s3 path and file name of models_catalog.', required=True)

    parser.add_argument('-t','--target_owp_ras_models_path',
                        help='location of the root ras2fim owp_ras_models folders.', required=False, default="c\\ras2fim_data\OWP_ras_models")
    
    parser.add_argument('-u','--huc_number', 
                        help='pull model records matching the huc number', required=True, type=int)

    parser.add_argument('-v','--is_verbose', 
                        help='Adding this flag will give additional tracing output',
                        required=False, default=False, action='store_true')

    args = vars(parser.parse_args())
    
    obj = Get_Ras_Models(path_to_cred_env_file = args['aws_cred_env_file'], 
                             is_verbose = args['is_verbose'])
    
    obj.get_models(inc_download_folders = args['inc_download_folders'],
                    s3_path_to_catalog = args['s3_path_to_catalog'],
                    target_owp_ras_models_path = args['target_owp_ras_models_path'],
                    huc_number = args['huc_number']) 
        

