# Create HEC-RAS terrain files (HDF5) from GeoTIFFs
#
# Purpose:
# Note that this uses a CLI to the RasProcess.exe in HEC-RAS 6.0.
# This will need access to the cooresponding and support files
# (dll, exe, etc...).  As this is a CLI call it will run async with this
# script.
#
# Use the HEC-RAS Command line interface to convert the TIF to HEC-RAS
# hdf5 terrains per directions from Cam Ackerman - 2021.03.31
#
# Created by: Andy Carter, PE
# Created: 2021.08.05
# Last revised - 2021.10.24
#
# Sample:  RasProcess.exe CreateTerrain
# units=feet stitch=true
# prj="C:\Path\file.prj"
# out="C:\Path\Terrain.hdf"
# "C:\inputs\file1.tif" "C:\inputs\file2.tif" [...]
#
# ras2fim - Fourth pre-processing script
# Uses the 'ras2fim' conda environment

# ************************************************************
import os
import subprocess
import argparse

import time
import datetime
# ************************************************************

# -------------------------------------------------------
def fn_get_filepaths(str_directory, str_file_suffix):
    # Fuction - walks a directory and determines a the path
    # to all the files with a given suffix
    
    list_file_paths = []
    int_file_suffix_len = len(str_file_suffix) * -1

    # Walk the tree.
    for root, directories, files in os.walk(str_directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filepath[int_file_suffix_len:] == str_file_suffix:
                list_file_paths.append(filepath)

    return list_file_paths
# -------------------------------------------------------

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@    
# Print iterations progress
def fn_print_progress_bar (iteration,
                           total,
                           prefix = '', suffix = '',
                           decimals = 0,
                           length = 100, fill = '█',
                           printEnd = "\r"):
    """
    from: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    Call in a loop to create terminal progress bar
    Keyword arguments:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@   


def fn_convert_tif_to_ras_hdf5(str_hec_path,
                               str_geotiff_dir,
                               str_dir_to_write_hdf5,
                               str_projection,
                               b_in_feet):
    # ~~~~~~~~~~~~~~~~~~~~~~~~
    # INPUT
    flt_start_convert_tif = time.time()
    
    print(" ")
    print("+=================================================================+")
    print("|       CONVERT TERRAIN GEOTIFFS TO HEC-RAS TERRAINS (HDF5)       |")
    print("+-----------------------------------------------------------------+")
    
    # path to the directory that contains RasProcess and associated dll
    
    STR_HEC_RAS_6_PATH = str_hec_path
    print("  ---(r) HEC-RAS PATH: " + str(STR_HEC_RAS_6_PATH))
    
    #STR_HEC_RAS_6_PATH = r'C:\Program Files (x86)\HEC\HEC-RAS\6.0'
    STR_HEC_RAS_6_PATH += r'\RasProcess.exe'
    
    # path to walk to file geotiffs
    STR_CONVERT_FILEPATH = str_geotiff_dir
    print("  ---(i) GEOTIFF INPUT PATH: " + str(STR_CONVERT_FILEPATH))
    
    # path to walk to file geotiffs
    STR_RAS_TERRAIN_OUT = str_dir_to_write_hdf5
    print("  ---(o) DIRECTORY TO WRITE TERRAIN HDF5: " + str(STR_RAS_TERRAIN_OUT))
    
    # path to walk to file geotiffs
    STR_PRJ_FILE = str_projection
    print("  ---(p) PROJECTION TO WRITE DEMS: " + str(STR_PRJ_FILE))
    
    # 
    B_CONVERT_TO_VERT_FT = b_in_feet
    print("  ---[v]   Optional: VERTICAL IN FEET: " + str(B_CONVERT_TO_VERT_FT))
    
    print("===================================================================")
    
    list_processed_dem = fn_get_filepaths(STR_CONVERT_FILEPATH, "tif")
    l = len(list_processed_dem)

    str_prefix = "Converting Terrains: "
    fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 29)
    
    int_count = 0
    int_valid_count = 0
    
    for i in list_processed_dem:
        int_count += 1
        
        fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 29)
    
        # Build a CLI call for RasProcess.exe CreateTerrain for each
        # terarin tile (HUC-12) in the list
    
        str_path_ras = "\"" + STR_HEC_RAS_6_PATH + "\"" + " CreateTerrain"
        str_path_ras += " units="
        
        if B_CONVERT_TO_VERT_FT:
            str_path_ras += "Feet"
        else:
            str_path_ras += "Meter"
        
        str_path_ras += " stitch=true prj="
        str_path_ras += "\"" + STR_PRJ_FILE + "\""
        str_path_ras += " out="
        str_path_ras += "\"" + STR_RAS_TERRAIN_OUT + "\\"
    
        str_path_ras += i[-16:-4] + ".hdf" + "\""
        str_path_ras += " " + i
    
        #print(str_path_ras)
        
        int_return_code = subprocess.check_call(str_path_ras,
                                                stdout=subprocess.DEVNULL,
                                                stderr=subprocess.STDOUT)
        if int_return_code == 0:
            int_valid_count += 1
        else:
            print('Error on: ' + str(i))
            
        # A '0' error code will be given if the file already exists in the
        # output directory.  Terrain will not be over-written with this
        # routine.  It will be skipped.

    print("+-----------------------------------------------------------------+")
    if int_valid_count == len(list_processed_dem):
        print('All terrains processed successfully')
    else:
        print('Errors when processing - Check output')
    
    flt_end_convert_tif = time.time()
    flt_time_convert_tif = (flt_end_convert_tif - flt_start_convert_tif) // 1
    time_pass_convert_tif = datetime.timedelta(seconds=flt_time_convert_tif)
    print('Compute Time: ' + str(time_pass_convert_tif))
    
    print("===================================================================")
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='==== CONVERT TERRAIN GeoTIFFS TO HEC-RAS TERRAINS (HDF5) ===')
    
    parser.add_argument('-r',
                        dest = "str_hec_path",
                        help=r'REQUIRED: path to HEC-RAS 6.0 RasProcess.exe: Example: "C:\Program Files (x86)\HEC\HEC-RAS\6.0" (wrap in quotes)',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    
    parser.add_argument('-i',
                        dest = "str_geotiff_dir",
                        help=r'REQUIRED: directory containing the geotiffs to convert:  Example: D:\terrain',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-o',
                        dest = "str_dir_to_write_hdf5",
                        help=r'REQUIRED: path to write output files: Example: D:\hecras_terrain',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-p',
                        dest = "str_projection",
                        help=r'REQUIRED: projection file of output coordinate zone: D:\conflation\10170204_huc_12_ar.prj',
                        required=True,
                        metavar='FILE PATH',
                        type=str)
    
    parser.add_argument('-v',
                        dest = "b_in_feet",
                        help='OPTIONAL: create vertical data in feet: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    args = vars(parser.parse_args())
    
    str_hec_path = args['str_hec_path']
    str_geotiff_dir = args['str_geotiff_dir']
    str_dir_to_write_hdf5 = args['str_dir_to_write_hdf5']
    str_projection = args['str_projection']
    b_in_feet = args['b_in_feet']

    fn_convert_tif_to_ras_hdf5(str_hec_path,
                               str_geotiff_dir,
                               str_dir_to_write_hdf5,
                               str_projection,
                               b_in_feet)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
