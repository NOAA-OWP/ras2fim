#!/usr/bin/env python
# coding: utf-8

# Iowa Terrain (HUC-12) to HEC-RAS HDF Terrain
#
# Purpose:
# Terrain in Iowa can currently be downloaded on a 'per HUC_12' basis from
# https://acpfdata.gis.iastate.edu/ACPF/dem.html.  These data are provided
# compressed as a .7z file.  Once unzipped, these files include GeoTIFFs that
# are in UTM (horiz = m, vertical = m).  Vertical data in the GeoTIFF are
# encoded as Integer16 (int16) for storage size.  The vertical elevation is
# multiplied by 100. (Example: a elevation of 432.12m is encoded as 43212m).
# This script does the following - (1) unzip all the provided 7z -(2) revizes
# the  int16 to a corrected float32 and (3) - uses the RasProcess.exe of
# HEC-RAS version 6, beta 3 through a Command Line Interface to convert the
# terrain (GeoTIFF) to a HEC-RAS compliant HDF Terrain.
#
# Output generated:
# Ultimately, a HEC-RAS HDF terrain for each downloaded HUC-12
# watershed (exmaple 101702040604.hdf), all in the same folder
#
# Created by: Andy Carter, PE
# Last revised - 2021.04.06
#
# Unique terrain preprocessing pipeline - for Iowa 2m

# ***

import os
import py7zr
import rasterio as rio

# ~~~~~~~~~~~~~~~~~~~~~~~~
# INPUT

# Directory containing .7z terrain files that were downloaded
STR_FILE_PATH = r"E:\X-IowaFloodCenter\Terrain_Download"

# Path to store the unzipped files
STR_PATH_UNIZPPED = r"E:\X-IowaFloodCenter\Terrain_Output"

# Path to store the converted (float32) files
STR_CONVERT_FILEPATH = r"E:\X-IowaFloodCenter\Terrain_Output\Terrain_Processed"

# ESRI projection file of the HEC-RAS models
STR_PRJ_FILE = r"E:\X-IowaFloodCenter\RockValley_PreProcess\10170204_huc_12_ar.prj"

# Directory to place the HDF terrain files
STR_RAS_TERRAIN_OUT = r"E:\X-IowaFloodCenter\Terrain_Output\Terrain_Processed\HEC-RAS-HDF"

# Path to the HEC-RAS v6.0 beta 3 RasProcess.exe
STR_HEC_RAS_6_PATH = r"C:\Junk\Test_HEC_RAS\6.0 Beta 3\RasProcess.exe"
# ~~~~~~~~~~~~~~~~~~~~~~~~


# ++++++++++++++++++++++++++
def fn_list_files(filepath, filetype):

    # walks a directory and return a lits of all the
    # files with a given file type

    paths = []
    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.lower().endswith(filetype.lower()):
                paths.append(os.path.join(root, file))
    return(paths)
# ++++++++++++++++++++++++++


list_files = fn_list_files(STR_FILE_PATH, "7z")


# .........................
def fn_unzip_list(str_file_path_fn, str_path_out_fn):

    # Unzips all the files in a given list (.7z)
    # and place in desired folder

    archive = py7zr.SevenZipFile(str_file_path_fn, mode='r')
    archive.extractall(path=str_path_out_fn)
    archive.close()
# .........................


fn_unzip_list(list_files, STR_PATH_UNIZPPED)
print('Unzip complete')

# Create a list of all the GeoTIFFs (from the unzipped HUC-12's)
list_files = fn_list_files(STR_PATH_UNIZPPED, "tif")


# >>>>>>>>>>>>>>>>>>>>>>>>>
def fn_convert_terrain(str_input_dem_filepath, str_huc12):

    # Load the raster and convert

    # Read the overall terrain raster
    src = rio.open(str_input_dem_filepath)

    # Copy the metadata of the src terrain
    out_meta = src.meta.copy()

    with rio.open(str_input_dem_filepath) as src:
        elevation = src.read()

    # create numpy array 'a'
    a = elevation

    # convert numpy datatype to unsigned 16 bit integer
    b = a.astype('float32')

    # divde all cell values by 100
    b = b / 100

    str_convert_file_name = STR_CONVERT_FILEPATH + "\\" + str_huc12 + ".tif"

    # change the data type and the null value
    with rio.open(str_input_dem_filepath) as src2:
        profile = src2.profile
        profile.update(dtype='float32', nodata=-21474836.480, compress='lzw')

        with rio.open(str_convert_file_name, 'w', **profile) as dataset:
            dataset.write(b)
# >>>>>>>>>>>>>>>>>>>>>>>>>


# converting terrain to float32 for each GeoTIFF in list

for i in list_files:
    str_current_terrain_name = i[-16:-4]
    fn_convert_terrain(i, str_current_terrain_name)
    print(str_current_terrain_name)


# Convert the GeoTIFFs of HUC-12 terrain to HEC-RAS HDF5
#
# Note that this uses a CLI to the RasProcess.exe in HEC-RAS 6.0 beta 3.
# This will need access to the cooresponding and support files
# (dll, exe, etc...).  As this is a CLI call it will run async with this
# notebook.
#
# Use the HEC-RAS Command line interface to convert the TIF to HEC-RAS
# hdf5 terrains per email from Cam Ackerman - 2021.03.31 - HEC-RAS 6.0 beta 3

# Sample:  RasProcess.exe CreateTerrain
# units=feet stitch=true
# prj="C:\Path\file.prj"
# out="C:\Path\Terrain.hdf"
# "C:\inputs\file1.tif" "C:\inputs\file2.tif" [...]

list_processed_dem = fn_list_files(STR_CONVERT_FILEPATH, "tif")

int_count = 0

for i in list_processed_dem:
    int_count += 1

    # Build a CLI call for RasProcess.exe CreateTerrain for each
    # terarin tile (HUC-12) in the list

    str_path_ras = "\"" + STR_HEC_RAS_6_PATH + "\"" + " CreateTerrain"
    str_path_ras += " units=meter stitch=true prj="
    str_path_ras += "\"" + STR_PRJ_FILE + "\""
    str_path_ras += " out="
    str_path_ras += "\"" + STR_RAS_TERRAIN_OUT + "\\"

    str_path_ras += i[-16:-4] + ".hdf" + "\""
    str_path_ras += " " + i

    print(str(i[-16:-4]) + " : " + str(int_count)
          + " of " + str(len(list_processed_dem)))

    # Use the os object to execute the terrain converstion call
    os.system(str_path_ras)

    o = os.popen(str_path_ras)
    print(o)

print('Done')
