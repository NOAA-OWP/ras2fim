#!/usr/bin/env python3


import argparse
import io
import pandas as pd

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
    def get_models(self, s3_path_to_catalog, huc_number, inc_download_folders=True, target_owp_ras_models_path = "c\\ras2fim_data\\OWP_ras_models"):

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

        # ----------
        # Validate inputs



        try:
            # ----------
            # calls over to S3 using the aws creds file even though it doesn't use it directly

            aws_session = super().load_aws_s3_session()

            #print(s3_client)
            #s3 = aws_session.resource("s3")
            df_all = pd.read_csv(s3_path_to_catalog)
            #print(df_all)

#            aws_so = {'key': os.getenv('AWS_ACCESS_KEY'),
#                      'secret': os.getenv('AWS_ACCESS_KEY') }
                  
#            df = dd.read_csv(s3_path_to_catalog, storage_options = aws_so).compute()

#            print(df)

            # the 'hucs' column has a list of hucs in semi-pythonic formatt, which is not 
            #df_huc = df_all.loc[(df_all['status'] == 'unprocessed')]        
            #df_huc = df_all.loc[(df_all['hucs'].str.contains(str(huc_number),na=False))]

            huc_number = '07010101'

            #df_huc = df_all.loc[(df_all['status'] == 'unprocessed') & 
            #                    (df_all['hucs'].str.contains(str(huc_number),na=False))]
                                        
            #print(df_huc)

            #aws_credentials = { "key": "***", "secret": "***", "token": "***" }
            #df = pd.read_csv("s3://...", storage_options=aws_credientials)

            #obj = s3_client.get_object(Bucket= "ras2fim" , Key = "OWP_ras_models/model_catalog_robh.csv")
            #df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

        except Exception as ex:
           print(ex)

        #print(df)
        print("Done")
        print("number of folders downloaded ??")


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
        

