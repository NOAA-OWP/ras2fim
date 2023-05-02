#!/usr/bin/env python3

# standard library imports (https://docs.python.org/3/library/
# (imports first, then "froms", in alpha order)
import os
import subprocess
import sys

import boto3

# third party imports
# (imports first, then "froms", in alpha order)
from dotenv import load_dotenv

'''
This implements all common variables related when communicating to AWS
'''
class AWS_Base(object):
    
    def __init__(self, path_to_cred_env_file = 'C:\ras2fim_data\config\aws_hv_s3_creds.env', *args, **kwargs):

        '''
        Overview
        ----------
        This will load the aws credentials enviroment file.
        For now, we will feed it via an env file. Eventuallly, it should be
        changed to ~/.aws/credentials (and maybe profiles)
        
        The aws_credentials_file will be loaded and an aws client
        will be created ready for use.

        Parameters
        ----------
        path_to_cred_env_file : str
            File path of the aws_credentials_file as an .env

        is_verbose : bool
            If True, then debugging output will be included

        '''
        
        if (not os.path.exists(path_to_cred_env_file)):
            raise FileNotFoundError("AWS credentials file not found")
            
        load_dotenv(path_to_cred_env_file)
        
        if kwargs:
            is_verbose = kwargs.pop("is_verbose",None)

        self.is_verbose = is_verbose
        
        # TODO: validate service name with AWS (which will help
        # validate the connection)
    
    
    def load_aws_s3_session(self):
        
        '''
        Overview
        ----------
        This will create an AWS session object which can be applied to different 
        code objects to make actual calls.

        Inputs
        ----------
        - self : needs to be included in most class methods, you do not need to submit it for a method call
        

        Returns
        -----------
        an AWS Session object
        
        '''

        # has not yet been loaded
        aws_session = boto3.Session( aws_access_key_id = os.getenv('AWS_ACCESS_KEY'), 
                                       aws_secret_access_key=os.getenv('AWS_ACCESS_KEY'), 
                                       region_name=os.getenv('AWS_ACCESS_KEY') )

       
        return aws_session