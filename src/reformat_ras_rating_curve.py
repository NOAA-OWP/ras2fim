#!/usr/bin/env python3

import os, argparse, rasterio, datetime, sys, shutil, gdal, osr, rasterio.crs
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas.tools import overlay
from shapely.geometry import LineString, Point
from concurrent.futures import ProcessPoolExecutor, as_completed, wait

# -----------------------------------------------------------------
# Reads, compiles, and reformats the rating curve info for each directory
# -----------------------------------------------------------------
def dir_reformat_ras_rc(dir, input_folder_path, verbose, intermediate_filename, 
                        output_save_folder, int_output_table_label, 
                        int_geospatial_label, int_log_label, source, 
                        location_type, active, input_vdatum, nodataval):

    """
    Reads, compiles, and reformats the rating curve info for the given directory (run in ___main___)

    """
    # Create empty output log
    output_log = []
    output_log.append(" ")
    output_log.append(f"Directory: {dir}")

    if verbose == True:
        print()
        print(f"Directory: {dir}")
    
    # Check for rating curve, get metadata and read it in if it exists
    rc_path = os.path.join(input_folder_path, dir, "06_ras2rem", "rating_curve.csv")
    
    if os.path.isfile(rc_path) == False:
        print(f"No rating curve file available for {dir}.")
        output_log.append("Rating curve NOT available.")
        dir_log_filename = str(dir) + int_log_label
        dir_log_filepath = os.path.join(output_save_folder, intermediate_filename, dir_log_filename)

        with open(dir_log_filepath, 'w') as f:
            for line in output_log:
                f.write(f"{line}\n")

    else:
        if verbose == True: 
            print(f"Rating curve CSV available.") 
        output_log.append("Rating curve CSV available.")

        # ------------------------------------------------------------------------------------------------
        # Get filepaths for the geospatial data

        root_dir = os.path.join(input_folder_path, dir)

        nwm_no_match_ext = '_no_match_nwm_lines.shp'
        nwm_all_lines_ext = '_nwm_streams_ln.shp'

        # Find filepaths for NWM streams and NWM streams - no match 
        for dirpath, dirnames, filenames in os.walk(root_dir):
            for filename in filenames:
                if filename.endswith(nwm_no_match_ext):
                    nwm_no_match_filepath = os.path.join(dirpath, filename)
                elif filename.endswith(nwm_all_lines_ext):
                    nwm_all_lines_filepath = os.path.join(dirpath, filename)

        # ------------------------------------------------------------------------------------------------
        # Read rating curve and extract the stage and discharge units from column headers

        rc_df = pd.read_csv(rc_path)    
        stage_units = rc_df.columns[rc_df.columns.str.contains('stage', case=False)][0].split('(')[1].strip(')')
        discharge_units = rc_df.columns[rc_df.columns.str.contains('Discharge', case=False)][0].split('(')[1].strip(')')

        # ------------------------------------------------------------------------------------------------
        # Read in the shapefile for the directory, project geodatabase, and get lat and lon of centroids

        # Read shapefiles
        nwm_no_match_shp = gpd.read_file(nwm_no_match_filepath)
        nwm_all_lines_shp = gpd.read_file(nwm_all_lines_filepath)

        # Subtract the nomatch lines from the full nwm set to reveal matched lines
        nwm_diff = overlay(nwm_all_lines_shp, nwm_no_match_shp, how="difference")

        # Create empty output lists
        feature_id_list = [] 
        midpoint_x_list = []
        midpoint_y_list = []

        # Project to CRS to get the coordinates in the correct format
        nwm_diff_prj = nwm_diff.to_crs('EPSG:4326') ##not sure if right, just debugging

        # Get middle segment of the flowlines
        for index, row in nwm_diff_prj.iterrows():
            feature_id = nwm_diff_prj['feature_id'][index] 

            # Create linestring      
            line = LineString(nwm_diff_prj['geometry'][index])

            # Calculate linestring midpoint and retrieve coordinates
            midpoint = line.interpolate(line.length / 2)
            midpoint_x = midpoint.x
            midpoint_y = midpoint.y

            # Append results to output lists
            feature_id_list.append(feature_id)
            midpoint_x_list.append(midpoint_x)
            midpoint_y_list.append(midpoint_y)

        # Make output dataframe
        midpoints_df = pd.DataFrame(
            {'feature_id': feature_id_list,
            'midpoint_lon': midpoint_x_list,
            'midpoint_lat': midpoint_y_list
            })

        # ------------------------------------------------------------------------------------------------
        # [Placeholder]: Crosswalk the sites to the correct FIM feature ID?

        # ------------------------------------------------------------------------------------------------
        # Read terrain file and get projection information

        terrain_folder_path = os.path.join(input_folder_path, dir, "03_terrain")

        # Get terrain file from path and select the first one if there's multiple files
        terrain_files = []
        for file in os.listdir(terrain_folder_path):
            if file.endswith('.tif'):
                terrain_files.append(file)
        terrain_file_first = terrain_files[0]

        # Print and log a warning if there is more than one terrain file
        if (len(terrain_files) > 1):
            newlog=f'Warning: More than one terrain file found in {dir}. Will use {terrain_file_first} for extracting elevation.'
            print(newlog)
            output_log.append(newlog)

        # Join filepath and read terrain file
        terrain_file_path = os.path.join(terrain_folder_path, terrain_file_first)

        # Read the terrain raster and copy the metadata of the src terrain data
        terrain = rasterio.open(terrain_file_path)
        out_meta = terrain.meta.copy()

        # Get the projection and units of the raster
        d = gdal.Open(terrain_file_path)
        proj = osr.SpatialReference(wkt=d.GetProjection())
        str_epsg_raster = proj.GetAttrValue('AUTHORITY', 1)
        str_unit_raster = proj.GetAttrValue('UNIT', 0)

        # ------------------------------------------------------------------------------------------------
        # [Placeholder]: Vertical datum conversion

        # # Set output datum (?)
        # datum_vcs = "NAVD88" #Probably navd88 

        # # If vertical datum isn't found, use the default (assumed) vertical datum
        # if vertical_datum == None:
        #     print(f"No vertical datum found for terrain file in {dir}.")
        #     print(f"Will use default vertical datum: {default_vdatum}")
        #     vertical_datum = default_vdatum
        # else:
        #     print(f"Vertical datum: {vertical_datum}")

        # # Check whether the datums need conversion
        # if vertical_datum == datum_vcs:
        #     print(f"Current vertical datum is correct. No need to convert the vertical datum for {dir}.")
        # else:
        #     print(f"Current vertical datum {vertical_datum} must be converted to {datum_vcs}.")

        # ## example snippet: ngvd_to_navd_ft() in NOAA-OWP/inundation-mapping/blob/dev/tools/tools_shared_functions.py (line 1099)
        # ## for now, the output datum is NAVD88 and assumed input datum is assumed to be NAVD88 at this time (Emily, 6/2/23)

        # ------------------------------------------------------------------------------------------------
        # Get elevation value from the converted terrain value using the midpoint lat and lon 

        # Convert midpoints to a geodataframe
        midpoints_df['geometry'] = midpoints_df.apply(lambda x: Point((float(x.midpoint_lon), float(x.midpoint_lat))), axis=1)
        midpoints_gdf = gpd.GeoDataFrame(midpoints_df, geometry='geometry')

        # Make sure the rasters and points are in the same projection so that the extract can work properly
        midpoints_gdf = midpoints_gdf.set_crs('EPSG:4326') # set the correct projection 
        midpoints_gdf = midpoints_gdf.to_crs(epsg=str_epsg_raster) # reproject to same as terrain

        # Extract elevation value from terrain raster
        raw_elev_list = []
        for point in midpoints_gdf['geometry']:

            # Format points
            x = point.xy[0][0]
            y = point.xy[1][0]
            row, col = terrain.index(x,y)

            # Get elevation from point and add to list
            raw_elev = terrain.read(1)[row,col]
            raw_elev_list.append(raw_elev)

        # Add elevation list to midpoints geodatabase
        midpoints_gdf['Raw_elevation'] = raw_elev_list

        # Print statements and add to output log
        if verbose == True: 
            print("Midpoints projection " + str(midpoints_gdf.crs))
            print("Terrain projection: " + str(terrain.crs))

        output_log.append("Midpoints projection " + str(midpoints_gdf.crs))
        output_log.append("Terrain projection: " + str(terrain.crs))

        # ------------------------------------------------------------------------------------------------
        # Compile elevation and datum information

        # Join raw elevation to rating curve using feature_id and drop the redundant feature_id column
        rc_elev_df = pd.merge(rc_df, midpoints_gdf[['feature_id', 'Raw_elevation', 'midpoint_lat', 'midpoint_lon']], 
                              left_on='feature-id', right_on='feature_id', how='left')
        rc_elev_df = rc_elev_df.drop('feature_id', axis=1)

        # If there is no datum conversion, the datum and the navd88_datum columns are both equal to Raw_elevation
        rc_elev_df['navd88_datum'] = rc_elev_df['Raw_elevation']
        rc_elev_df['datum'] = rc_elev_df['Raw_elevation']

        # Make column just called 'stage' with the stage values
        stage_columns = rc_elev_df.filter(like='stage')
        rc_elev_df['stage'] = stage_columns.iloc[:, 0]

        # Assimilate stage unit names and provide error if needed
        if stage_units in ['m', 'meters', 'meter', 'metre']:
            stage_units = 'm'
        elif stage_units in ['f', 'ft', 'feet']:
            stage_units = 'ft'
        else:
            log = "Warning: Stage units not recognized as feet or meters."
            output_log.append(log)
            print(log)
        
        # Assimilate raster unit names and provide error if needed
        if str_unit_raster in ['m', 'meters', 'meter', 'metre']:
            str_unit_raster = 'm'
        elif str_unit_raster in ['f', 'ft', 'feet']:
            str_unit_raster = 'ft'
        else:
            log = f"Warning: Raster units not recognized as feet or meters. ({dir})"
            output_log.append(log)
            print(log)

        # If the rating curve units aren't equal to the elevation units, convert them 
        if str_unit_raster == stage_units:
            rc_elev_df['stage_final'] = rc_elev_df['stage'] 
            log = 'Stage units and raster units are the same, no unit conversion needed.'
            # print('Stage units and raster units are the same, no unit conversion needed.')

        elif stage_units == 'ft' and str_unit_raster == 'meter':
            rc_elev_df['stage_final'] = rc_elev_df['stage'] / 3.281  ## meter = feet / 3.281
            log = 'Need to convert stage units to meter.'
            # print('Need to convert stage units to meter.')

        elif stage_units == 'm' and str_unit_raster == 'ft':
            rc_elev_df['stage_final'] = rc_elev_df['stage'] / 0.3048  ## feet = meters / 0.3048
            log = 'Need to convert stage units to feet.'
            # print('Need to convert stage units to feet.')

        else:
            log = f'Warning: No conversion available between {stage_units} and {str_unit_raster}. Stage set to nodata value ({nodataval}).'
            output_log.append(log)
            # print(log)
            rc_elev_df['stage_final'] = nodataval

        if verbose == True:
            print(log)

        ## Calculate elevation_navd88 by adding the rating curve value to the navd88_datum value
        #rc_elev_df['elevation_navd88'] = rc_elev_df['navd88_datum'] + rc_elev_df['stage']

        # If 'stage' does not equal a nodata value, calculate elevation_navd88 by adding 'navd88_datum' and 'stage'
        rc_elev_df.loc[rc_elev_df['stage_final'] != nodataval, 'elevation_navd88'] = rc_elev_df['stage_final'] + rc_elev_df['navd88_datum']

        # If stage DOES equal the nodata value, set the 'elevation_navd88' to the nodata value as well
        rc_elev_df.loc[rc_elev_df['stage_final'] == nodataval, 'elevation_navd88'] = nodataval

        # ------------------------------------------------------------------------------------------------
        # [Placeholder]: Determine whether an elevation adjustment is needed to supplement 
        # the cross walking. if so, further adjust the rating curve
        

        # ------------------------------------------------------------------------------------------------
        # Make output tables

        # Make column just called 'flow' with the discharge values
        flow_columns = rc_elev_df.filter(like='Discharge')
        rc_elev_df['flow'] =  flow_columns.iloc[:, 0]
        
        # Get timestamp
        wrds_timestamp = datetime.datetime.now() 

        dir_output_table = pd.DataFrame({'flow' : rc_elev_df['flow'],
                                         'stage' : rc_elev_df['stage_final'], # the stage value that was corrected for units
                                         'feature_id' : rc_elev_df['feature-id'],  
                                         'location_type': location_type, #str 
                                         'source': source, #str
                                         'flow_units': discharge_units, #str
                                         'stage_units': stage_units, #str
                                         'wrds_timestamp': wrds_timestamp, #str
                                         'active': active, #str
                                         'datum': rc_elev_df['datum'], #num
                                         'datum_vcs': input_vdatum, #str
                                         'navd88_datum': rc_elev_df['navd88_datum'], #num
                                         'elevation_navd88': rc_elev_df['elevation_navd88'], #num
                                         'lat': rc_elev_df['midpoint_lat'], #num
                                         'lon': rc_elev_df['midpoint_lon']}) #num
        
        dir_geospatial = dir_output_table.drop(columns=['stage', 'elevation_navd88', 'flow'])
        dir_geospatial = dir_geospatial.drop_duplicates(subset='feature_id', keep='first')

        # ------------------------------------------------------------------------------------------------
        # Export dir_output_table, dir_geospatial, and log to the intermediate save folder

        # Save output table for directory
        dir_output_table_filename = str(dir) + int_output_table_label 
        dir_output_table_filepath = os.path.join(output_save_folder, intermediate_filename, dir_output_table_filename)
        dir_output_table.to_csv(dir_output_table_filepath, index=False)        

        # Save geospatial table for directory
        dir_geospatial_filename = str(dir) + int_geospatial_label
        dir_geospatial_filepath = os.path.join(output_save_folder, intermediate_filename, dir_geospatial_filename)
        dir_geospatial.to_csv(dir_geospatial_filepath, index=False)  

        # Save log for directory
        dir_log_filename = str(dir) + int_log_label
        dir_log_filepath = os.path.join(output_save_folder, intermediate_filename, dir_log_filename)
        with open(dir_log_filepath, 'w') as f:
            for line in output_log:
                f.write(f"{line}\n")

        if verbose == True:
            print(f"Saved multiprocessor outputs for {dir}.")

# -----------------------------------------------------------------
# Compiles the rating curve and points from each directory 
# -----------------------------------------------------------------
def compile_ras_rating_curves(input_folder_path, output_save_folder, log, verbose, num_workers, 
                              keep_intermediates,  start_time_string, overwrite, source, 
                              location_type, active, input_vdatum, nodataval):

    """
    Creates directory list and feeds directories to dir_reformat_ras_rc() inside a multiprocessor.
    Compiles the rating curve and geopackage info from the intermediate data folder and saves a final 
    rating curve CSV and geospatial outputs geopackage.

    """

    # Get a list of the directories in the input folder path
    dirlist = os.listdir(input_folder_path)

    # Establish file naming conventions
    int_output_table_label = "_output_table.csv"
    int_geospatial_label = "_geospatial.csv"
    int_log_label = "_log.txt"

    # Create intermediate directory
    intermediate_filename = "intermediate_outputs"
    intermediate_filepath = os.path.join(output_save_folder, intermediate_filename)

    if overwrite == True:
        if os.path.exists(intermediate_filepath):

            try:
                shutil.rmtree(intermediate_filepath)
                print(f"Overwriting {intermediate_filepath}.")

            except OSError as e:
                print("Error: %s : %s" % (intermediate_filepath, e.strerror))

    else:
        if os.path.exists(intermediate_filepath):
            sys.exit(f"Error: File already exists at {intermediate_filepath}. "\
                     "Manually delete directory, use overwrite flag (-ov) or "\
                     "use a different output save folder.")

    os.mkdir(intermediate_filepath)

    # Create empty output log and give it a header
    output_log = []
    output_log.append(f"Processing for reformat_ras_rating_curves.py started at {str(start_time_string)}")
    output_log.append(f"Input directory: {input_folder_path}")

    # ------------------------------------------------------------------------------------------------
    # # Create a process pool and run dir_reformat_ras_rc() for each directory in the directory list
    # with ProcessPoolExecutor(max_workers=num_workers) as executor:

    if verbose == True:
        print()
        print("--------------------------------------------------------------")
        print("Begin iterating through directories with multiprocessor...")
        print()


    #     for dir in dirlist:
    #         executor.submit(dir_reformat_ras_rc, dir, input_folder_path, verbose, intermediate_filename, output_save_folder, int_output_table_label, int_geospatial_label, int_log_label, source, 
                              #location_type, active, input_vdatum, nodataval)

    ##debug: run without multiprocessor
    for dir in dirlist:
        dir_reformat_ras_rc(dir, input_folder_path, verbose, intermediate_filename, output_save_folder, 
                            int_output_table_label, int_geospatial_label, int_log_label, source, 
                            location_type, active, input_vdatum, nodataval)


    # ------------------------------------------------------------------------------------------------
    # Read in all intermedate files (+ output logs) and combine them 

    if verbose == True:
        print()
        print("--------------------------------------------------------------")
        print("Begin compiling multiprocessor outputs...")
        print()

    # Get list of intermediate files from path
    intermediate_filepath = os.path.join(output_save_folder, intermediate_filename)

    int_output_table_files = []
    int_geospatial_files = []
    int_logs = []

    for dirpath, dirnames, filenames in os.walk(intermediate_filepath):

        for filename in filenames:
            if filename.endswith(int_output_table_label): 
                path = os.path.join(dirpath, filename)
                int_output_table_files.append(path)

            elif filename.endswith(int_geospatial_label): 
                path = os.path.join(dirpath, filename)
                int_geospatial_files.append(path)   

            elif filename.endswith(int_log_label): 
                path = os.path.join(dirpath, filename)
                int_logs.append(path)                      

    # Read and compile the intermediate rating curve tables
    full_output_table = pd.DataFrame()

    for file_path in int_output_table_files:
        df = pd.read_csv(file_path)
        full_output_table = pd.concat([full_output_table, df])

    full_output_table.reset_index(drop=True, inplace=True)

    # Read and compile the intermediate geospatial tables
    full_geospatial = pd.DataFrame()

    for file_path in int_geospatial_files:
        df = pd.read_csv(file_path)
        full_geospatial = pd.concat([full_geospatial, df])

    full_geospatial.reset_index(drop=True, inplace=True)

    # Read and compile all logs
    for file_path in int_logs:
        with open(file_path) as f:
            lines = f.readlines()

            for line in lines:
                output_log.append(line)

    # Remove extra linebreaks from log
    output_log = [s.replace('\n', '') for s in output_log]
    output_log.append(" ")

    # If keep-intermediates option is not selected, clean out intermediates folder
    if keep_intermediates == False:
        shutil.rmtree(intermediate_filepath)
        if verbose == True:
            print("Cleaning intermediate files.")

    # ------------------------------------------------------------------------------------------------
    # Check for and remove duplicate values
    if full_geospatial['feature_id'].duplicated().any():
        print()
        print("Duplicate feature_ids were found and removed from geospatial data table.")
        
        if verbose == True:
            duplicated_fids = full_geospatial.loc[full_geospatial['feature_id'].duplicated() == True]['feature_id']
            print("Duplicate feature_id values: " + str(set(list(duplicated_fids))))

            output_log.append("Duplicate feature_id values: " + str(set(list(duplicated_fids))))
            output_log.append("Duplicate feature_ids were removed from geospatial data table.")
            output_log.append(" ")

        full_geospatial.drop_duplicates(subset='feature_id', inplace=True)

    # ------------------------------------------------------------------------------------------------
    # Combine lat and lon column to a shapely Point() object and convert to a geopackage format

    output_geopackage_temp = full_geospatial
    output_geopackage_temp['geometry'] = output_geopackage_temp.apply(lambda x: Point((float(x.lon), float(x.lat))), axis=1)
    output_geopackage = gpd.GeoDataFrame(output_geopackage_temp, geometry='geometry')

    if verbose == True:
        print()
        print("Midpoint geopackage created.")

    # ------------------------------------------------------------------------------------------------
    # Export the output points geopackage and the rating curve table to the save folder

    geopackage_name = 'reformat_ras_rating_curve_points.gpkg'
    geopackage_path = os.path.join(output_save_folder, geopackage_name)
    output_geopackage.to_file(geopackage_path, driver='GPKG')

    csv_name = 'reformat_ras_rating_curve_table.csv'
    csv_path = os.path.join(output_save_folder, csv_name)
    full_output_table.to_csv(csv_path, index=False)

    # ------------------------------------------------------------------------------------------------
    # Print filepaths and logs (if the verbose and log arguments are selected)

    output_log.append(f"Geopackage save location: {geopackage_path}")
    output_log.append(f"Compiled rating curve csv save location: {csv_path}") 

    if verbose == True:
        print()
        print(f"Geopackage save location: {geopackage_path}")
        print(f"Compiled rating curve csv save location: {csv_path}") 

    # Save output log if the log option was selected
    if log == True:
        log_name = 'reformat_ras_rating_curve_log.txt'
        log_path = os.path.join(output_save_folder, log_name)

        with open(log_path, 'w') as f:
            for line in output_log:
                f.write(f"{line}\n")
        print()
        print(f"Log saved to {log_path}.")
    else:
        print()
        print("No output log saved.")

if __name__ == '__main__':

    """
    Sample usage (no linebreak so they can be copied and pasted):
    
    # Recommended parameters:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -v -l -ov

    # Minimalist run (only required arguments):
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp'

    # Maximalist run (all possible arguments):
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -v -l -j 6 -k -ov -so "ras2fim" -lt "USGS" -ac "True"

    # Overwrite existing intermediate files:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -v -ov

    # Run with 6 workers:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -v -l -j 6

    # Keep intermediates, save output log, and run quietly (no verbose tag):
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -k -l

    # Input the data source, location type, and active information using the -so, -lt, and -ac flags:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -v -so "ras2fim" -lt "USGS" -ac "True"

    Notes:
       - Required arguments: -i "input path", -o "output path"
       - Optional arguments: use the -l tag to save the output log
                             use the -v tag to run in a verbose setting
                             use the -j flag followed by the number to specify number of workers
                             use the -k flag to keep intermediate files once the script has run
                             use the -ov flag to overwrite any existing intermediate files
                             use the -so flag to input a value for the "source" output column (i.e. "ras2fim", "ras2fim v2.1") 
                             use the -lt flag to input a value for the "location_type" output column (i.e. "USGS", "IFC")
                             use the -ac flag to input a value for the "active" column ("True" or "False")

    """
        
    # Parse arguments
    parser = argparse.ArgumentParser(description='Iterate through a directory containing ras2fim outputs and '\
                                     'compile a rating curve table and rating curve location point file.')
    parser.add_argument('-i', '--input-path', help='Input directory containing ras2fim outputs to process.', required=True)
    parser.add_argument('-o', '--output-path', help='Output save folder.', required=True)
    parser.add_argument('-l', '--log', help='Option to save output log to output save folder.', required=False, default=False, action='store_true')
    parser.add_argument('-v', '--verbose', help='Option to print more updates and descriptions.', required=False, default=False, action='store_true')
    parser.add_argument('-j', '--num-workers',help='Number of concurrent processes', required=False, default=1, type=int)
    parser.add_argument('-k', '--keep-intermediates', help='Option to save intermediates in temp directory after script is finished.', 
                        required=False, default=False, action='store_true')
    parser.add_argument('-ov', '--overwrite', help='Option to overwrite existing intermediate files in the output save folder.', 
                        required=False, default=False, action='store_true')
    parser.add_argument('-so', '--source', help='Input a value for the "source" output column (i.e. "ras2fim", "ras2fim v2.1").', required=False, default=" ")
    parser.add_argument('-lt', '--location-type', help='Input a value for the "location_type" output column (i.e. "USGS", "IFC").', required=False, default=" ")
    parser.add_argument('-ac', '--active', help='Input a value for the "active" column ("True" or "False")', required=False, default=" ")


    # Assign variables from arguments
    args = vars(parser.parse_args())
    input_folder_path = args['input_path']
    output_save_folder = args['output_path']
    log = bool(args['log'])
    verbose = bool(args['verbose'])
    num_workers = args['num_workers']
    keep_intermediates = bool(args['keep_intermediates'])
    overwrite = bool(args['overwrite'])
    source = str(args['source'])
    location_type = str(args['location_type'])
    active = str(args['active'])

    # Record and print start time
    start_time = datetime.datetime.now()
    start_time_string = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    print("-----------------------------------------------------------------------------------------------")
    print("Begin rating curve compilation process...")
    print()
    print(f"Start time: {start_time_string}.")
    print()
    print("Settings: ")
    print(f"    Verbose: {str(verbose)}")
    print(f"    Save output log to folder: {str(log)}")
    print(f"    Keep intermediates: {str(keep_intermediates)}")
    print(f"    Number of workers: {num_workers}")

    # Set default vertical datum and print warning about vertical datum conversion
    input_vdatum = "NAVD88"
    output_vdatum = "NAVD88"

    if input_vdatum == output_vdatum:
        print(f"    No datum conversion will take place.")

    # End of settings block
    print()

    # Set nodata value
    nodataval = (0 - 9999) # -9999

    # Check job numbers
    total_cpus_requested = num_workers
    total_cpus_available = os.cpu_count() - 1
    if total_cpus_requested > total_cpus_available:
        raise ValueError("Total CPUs requested exceeds your machine\'s available CPU count minus one. "\
                          "Please lower the quantity of requested workers accordingly.")

    # Run main function
    compile_ras_rating_curves(input_folder_path, output_save_folder, log, verbose, num_workers, keep_intermediates, 
                              start_time_string, overwrite, source, location_type, active, input_vdatum, nodataval)

    # Record end time, calculate runtime, and print runtime
    end_time = datetime.datetime.now()
    runtime = end_time - start_time

    print()
    print(f"Process finished. Total runtime: {runtime}") 
    print("-----------------------------------------------------------------------------------------------")



    