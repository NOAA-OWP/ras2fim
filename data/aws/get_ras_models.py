#!/usr/bin/env python3


import argparse
import io
import pandas as pd
import shutil
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
                In S3, the source download folder will be the "models" folder, but put into the "models" folder here.
        - Using the models_catalog.csv, only records with the status of 'unprocessed'
            
        Inputs
        -----------------------
        - s3_path_to_catalog (str) : ie) s3://xyz/OWP_ras_models/models_catalog.csv (left as full pathing in case of pathing
            changes). In testing, this might be models_catalog_robh.csv
        - huc_number (int)
        - inc_download_folders (True / False) : If you override to false, you can get just a list and not the downloads
        - target_owp_ras_models_path (str): Local path you want the files to be loaded too. Default: c\ras2fim_data\OWP_ras_models

        
        Outputs
        -----------------------
        - all of the actual folders found in the s3 OWP_ras_models based on records from the models catalog
          placed in the local emptied (folders only) target_owp_ras_models_path (assuming not overwritten to list only)

        '''

        start_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print (f"started: {dt_string}")

        try:

            aws_session = super().load_aws_s3_session()

            # ----------
            # Validate inputs
            self.validate_inputs(s3_path_to_catalog, huc_number, inc_download_folders, target_owp_ras_models_path)



            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly
            df_all = pd.read_csv(s3_path_to_catalog)

            if (self.is_verbose == True):
                print("df_all list")
                print(df_all)

            # ----------
            # look for records that are unprocessed, contains the huc number and does not start with 1_
            df_huc = df_all.loc[(df_all['status'] == 'unprocessed') & 
                                (~df_all['final_name_key'].str.startswith("1_")) & 
                                (df_all['hucs'].str.contains(str(huc_number),na=False))]

            if (self.is_verbose == True):
                print("df_huc list")
                print(df_huc)


            # ----------
            # remove folders only from the local OWP_ras_models/models (or overwride), keep files
            # also keep folders starting with "__"
            owp_models_folder = os.path.join(target_owp_ras_models_path, "models")
            if (os.path.exists(owp_models_folder)):
                models_dir_list = os.listdir(owp_models_folder)

                for model_dir in models_dir_list:
                    if (not model_dir.startswith("__")):  #two underscores
                        model_dir_path = os.path.join(owp_models_folder, model_dir)
                        shutil.rmtree(model_dir_path)
                        if (self.is_verbose == True):
                            print(f"{model_dir_path} folder removed")
            else:
                os.mkdir(owp_models_folder)
                if (self.is_verbose == True):
                    print(f"{owp_models_folder} created")
                

            # ----------
            # create log file that can be saved

            # ----------
            # If inc_download_folders, otherwise we just stop.  Sometimes a list is wanted but not the downloads

            if (inc_download_folders == False):
                # write out the log
                print("List created at ... ")
                return

            # loop through df_huc records and using name, pull down 



        except Exception as ex:
           errMsg = "--------------------------------------" \
                     f"\n An error has occurred"
           errMsg = errMsg + traceback.format_exc()
           print(errMsg, flush=True)
           #log_error(self.fim_directory, usgs_elev_flag,
           #             hydro_table_flag, src_cross_flag, huc_id, errMsg)


        #print(df)
        print("Downloads completed")
        print("number of folders downloaded ??")

        end_time = datetime.now()
        dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        print (f"ended: {dt_string}")

        # Calculate duration
        time_duration = end_time - start_time
        print(f"Duration: {str(time_duration).split('.')[0]}")
        print()        


    def validate_inputs(self, s3_path_to_catalog, huc_number, inc_download_folders=True, 
                        target_owp_ras_models_path = "c\\ras2fim_data\\OWP_ras_models"):

        '''
        If errors are found, an exception will be raised.
        '''

        # s3 bucket file exists (and creds by default)

        # huc number is valid

        # target_owp_ras_models_path exists
        if (not os.path.exists(target_owp_ras_models_path)):
            raise ValueError(f"Target owp_ras_models folder of '{target_owp_ras_models_path}' does not exist")



if __name__ == '__main__':

    # Sample Usage (mins) (also downloads related folders)
    #python3 get_list_ras_models.py -s s3://xyz/OWP_ras_models/models_catalog.csv -u 12010401
            
    parser = argparse.ArgumentParser(description='Communication with aws s3 data services')
    
    parser.add_argument('-c','--aws_cred_env_file', 
                        help='path to aws credentials env file', required=False, default="C:\\ras2fim_data\\config\\aws_hv_s3_creds.env")
    
    # False means you get a list only, not the folders downloaded (at \OWP_ras_models\huc_OWP_ras_models.csv)
    parser.add_argument('-d','--inc_download_folders', 
                        help='True or False to including downloading the folders', required=False, default=True, action='store_false')

    # can't default due to security.  ie) s3://xyz/OWP_ras_models/models_catalog.csv
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
        

