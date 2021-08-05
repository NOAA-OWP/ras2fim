# Multi-processing HEC-RAS
#
# Purpose:
# Walk a directory of HEC-RAS files and getting a list of all project (prj)
# file paths.  Using the win32com API for HEC-RAS (installed on the machine
# when HEC-RAS is installed) compute the active plan.
#
# Created by: Andy Carter, PE
# Last revised - 2021.06.01

import pythoncom 
import win32com.client
import os
from multiprocessing import Pool
from datetime import datetime 
from win32com.client import GetObject 

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT
NUM_PROCESSES = 10
str_filepath_to_walk = r'E:\X-NWS\BLE\Hydraulic_Models\1203010601'
# ~~~~~~~~~~~~~~~~~~~~~~~~

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
def fn_filelist(source):
    # walk a directory and get all the HEC-RAS projects (PRJ)
    # returns a list of file paths
    matches = []
    for root, dirnames, filenames in os.walk(source):
        for filename in filenames:
            if filename.endswith(('.prj', '.PRJ')):
                matches.append(os.path.join(root, filename))
    return matches
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


# ************************************
def fn_runras(str_prj_path):
    # Function - runs HEC-RAS (active plan) and closes the file
    
    # Initialize
    pythoncom.CoInitialize()
    
    hec = win32com.client.Dispatch("RAS507.HECRASController")
    
    # Create id
    hec_id = pythoncom.CoMarshalInterThreadInterfaceInStream(pythoncom.IID_IDispatch, hec)

    # opening HEC-RAS
    hec.Project_Open(str_prj_path)

    # to be populated: number and list of messages, blocking mode
    NMsg, TabMsg, block = None, None, True

    # computations of the current plan
    v1, NMsg, TabMsg, v2 = hec.Compute_CurrentPlan(NMsg, TabMsg, block)

    hec.QuitRas()   # close HEC-RAS
# ************************************

if __name__ == '__main__':
    
    time_start = datetime.now()
    list_ras_prj = fn_filelist(str_filepath_to_walk)
    
    with Pool(NUM_PROCESSES) as p:
        p.map(fn_runras, list_ras_prj)
    
    # Kill all the active Ras.exe started after the beginning of this script
    WMI = GetObject('winmgmts:')
    for process in WMI.ExecQuery('select * from Win32_Process where Name="Ras.exe"'):
        create_dt, *_ = process.CreationDate.split('.') 
        time_run = datetime.strptime(create_dt,'%Y%m%d%H%M%S')
        
        if time_run > time_start: 
           #print("Terminating PID:", process.ProcessId) 
           os.system("taskkill /pid "+str(process.ProcessId))