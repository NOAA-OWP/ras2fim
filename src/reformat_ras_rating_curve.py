#!/usr/bin/env python3

import os, re, argparse, rasterio, datetime
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas.tools import overlay
from shapely.geometry import LineString, Point
from concurrent.futures import ProcessPoolExecutor, as_completed, wait

def compile_ras_rating_curves(input_folder_path, output_save_folder, log, verbose):

    # Get a list of the directories in the input folder path
    dirlist = os.listdir(input_folder_path)

    # Create empty DataFrame with specific column names & types
    all_outputs_table = pd.DataFrame({'flow': pd.Series(dtype='float64'), 
                                'stage': pd.Series(dtype='float64'),
                                'feature_id': pd.Series(dtype='float64'), 
                                'location_type': pd.Series(dtype='object'),  
                                'source': pd.Series(dtype='object'),  
                                'flow_units': pd.Series(dtype='object'),  
                                'stage_units': pd.Series(dtype='object'),  
                                'wrds_timestamp': pd.Series(dtype='object'),  
                                'active': pd.Series(dtype='bool'),  
                                'datum': pd.Series(dtype='float64'),  
                                'datum_vcs': pd.Series(dtype='object'),  
                                'navd88_datum': pd.Series(dtype='float64'),  
                                'elevation_navd88': pd.Series(dtype='float64'), 
                                'lat': pd.Series(dtype='float64'),                      
                                'lon': pd.Series(dtype='float64')})

    #Create empty DataFrame with specific column names & types
    all_geospatial = pd.DataFrame({'feature_id': pd.Series(dtype='float64'), 
                                'location_type': pd.Series(dtype='object'),  
                                'source': pd.Series(dtype='object'),  
                                'flow_units': pd.Series(dtype='object'),  
                                'stage_units': pd.Series(dtype='object'),  
                                'wrds_timestamp': pd.Series(dtype='object'),  
                                'active': pd.Series(dtype='bool'),  
                                'datum': pd.Series(dtype='float64'),  
                                'datum_vcs': pd.Series(dtype='object'),  
                                'navd88_datum': pd.Series(dtype='float64'),  
                                'elevation_navd88': pd.Series(dtype='float64'), 
                                'lat': pd.Series(dtype='float64'),                      
                                'lon': pd.Series(dtype='float64')})

    # Create empty output log and give it a header
    output_log = []
    output_log.append("reformat_ras_rating_curves.py started at " + str(start_time_string))

    # Create a process pool and specify max number of workers
    max_workers = 10 ## update later

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for dir in dirlist:
            
            if verbose == True:
                print(" ")
                print(f"Directory: {dir}")
            
            # Check for rating curve, get metadata and read it in if it exists
            rc_path = os.path.join(input_folder_path, dir, "06_ras2rem", "rating_curve.csv")
            
            if os.path.isfile(rc_path) == False:
                print(f"No rating curve file available for directory: {dir}.")
                output_log.append("Rating curve NOT available for directory: " + str(dir))

            else:
                if verbose == True: 
                    print(f"Rating curve csv available for {dir}.") 

                output_log.append("Rating curve available for directory: " + str(dir))

                # ------------------------------------------------------------------------------------------------
                # Get filepaths for the geospatial data

                root_dir = os.path.join(input_folder_path, dir)

                nwm_no_match_ext = '_no_match_nwm_lines.shp'
                nwm_all_lines_ext = '_nwm_streams_ln.shp'

                # Iterate through all subdirectories looking for metadata and geodata filepaths
                for dirpath, dirnames, filenames in os.walk(root_dir):
                    for filename in filenames:

                        # Find filepaths for the NWM streams and NWM streams (no match shapefiles)
                        if filename.endswith(nwm_no_match_ext):
                            nwm_no_match_filepath = os.path.join(dirpath, filename)

                        elif filename.endswith(nwm_all_lines_ext):
                            nwm_all_lines_filepath = os.path.join(dirpath, filename)

                # ------------------------------------------------------------------------------------------------
                # Read rating curve and extract data

                rc_df = pd.read_csv(rc_path)    

                # Extract the stage and discharge units from column headers
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
                nwm_diff_prj = nwm_diff.to_crs("EPSG:4326")

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
                # Placeholder: Crosswalk the sites to the correct FIM feature ID? (I don't think it's needed because we already have feature IDs) 
                

                # ------------------------------------------------------------------------------------------------
                # Read terrain file and get elevation value using the lat lon

                terrain_folder_path = os.path.join(input_folder_path, dir, "03_terrain")

                # Get terrain file from path and select the first one if there's multiple files
                terrain_files = []
                for file in os.listdir(terrain_folder_path):
                    # check only text files
                    if file.endswith('.tif'):
                        terrain_files.append(file)
                terrain_file_first = terrain_files[0]

                # Print and log a warning if there is more than one terrain file
                if (len(terrain_files) > 1):
                    newlog=f'Warning: More than one terrain file found in {dir}. Will use {terrain_file_first} for extracting elevation.'
                    print(newlog)
                    output_log.append(newlog)

                # Convert midpoints to a geodataframe
                midpoints_df['geometry'] = midpoints_df.apply(lambda x: Point((float(x.midpoint_lon), float(x.midpoint_lat))), axis=1)
                midpoints_gdf = gpd.GeoDataFrame(midpoints_df, geometry='geometry')

                # Join filepath and read terrain file
                terrain_file_path = os.path.join(terrain_folder_path, terrain_file_first)
                terrain = rasterio.open(terrain_file_path)

                # Make sure the rasters and points are in the same projection so that the extract can work properly
                midpoints_gdf = midpoints_gdf.set_crs('EPSG:4326') # set the correct projection 
                midpoints_gdf = midpoints_gdf.to_crs('EPSG:26915') # so it matches terrain

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

                # Print statements
                if verbose == True: 
                    print("Midpoints projection " + str(midpoints_gdf.crs))
                    print("Terrain projection: " + str(terrain.crs))
                    print()
                    print("Midpoints geodataframe: ")
                    print(midpoints_gdf)

                # ------------------------------------------------------------------------------------------------
                # Placeholder: Pull datum information from the datum API

                # determine input datum

                # if input datum doesn't equal output datum, convert (navd88 (north american vertical datum 88))

                # based on ngvd_to_navd_ft() in NOAA-OWP/inundation-mapping/blob/dev/tools/tools_shared_functions.py (line 1099)
                # ngvd_to_navd_ft(datum_info, region = 'contiguous')
                ## ** need to confirm output datum 
                ## -> I think it is navd88 (north american vertical datum 88) -> https://geodesy.noaa.gov/datums/vertical/north-american-vertical-datum-1988.shtml

                datum = 999 # numerical 
                datum_vcs = 999 # output datum name?
                navd88_datum = 999 # numerical, same as datum?
                elevation_navd88 = 999 # stage + datum?

                # [Placeholder: Determine whether an elevation adjustment is needed to supplement the cross walking. if so, further adjust the rating curve]
                

                # ------------------------------------------------------------------------------------------------
                # Make output rating curve table for the directory (to append to larger table and then export as a csv)

                # Prep output variables
                location_type = "RAS2FIM" # type of location the data is coming from
                active = True # should be TRUE or FALSE
                source = "temp source id" # where the model came from (example: IFC)

                wrds_timestamp = datetime.datetime.now() # current timestamp (## double check?)

                dir_rating_curve = pd.DataFrame({'flow': rc_df.iloc[:,2], 
                                    'stage': rc_df.iloc[:,1], 
                                    'feature_id': rc_df.iloc[:,0]})

                dir_geospatial = pd.DataFrame({'feature_id' : midpoints_df['feature_id'],  
                                'location_type': location_type, #str 
                                'source': source, #str
                                'flow_units': discharge_units, #str
                                'stage_units': stage_units, #str
                                'wrds_timestamp': wrds_timestamp, #str
                                'active': active, #bool
                                'datum': datum, #num
                                'datum_vcs': datum_vcs, #num 
                                'navd88_datum': navd88_datum, #num
                                'elevation_navd88': elevation_navd88, #num
                                'lat': midpoints_df['midpoint_lat'], #num
                                'lon': midpoints_df['midpoint_lon']}) #num

                # Join rating curve to geospatial data by feature id
                dir_output_table = dir_rating_curve.merge(dir_geospatial, on='feature_id')

                # Join all outputs table (rating curve + geospatial) and geospatial output table to their respective output tables
                all_outputs_table = all_outputs_table.append(dir_output_table, ignore_index=True)
                all_geospatial = all_geospatial.append(dir_geospatial, ignore_index=True)

    # ------------------------------------------------------------------------------------------------
    # Outputs and log warnings

    # Check for and remove duplicate values
    if all_geospatial['feature_id'].duplicated().any():
        print()
        print("Duplicate feature_ids removed from geospatial data table.")
        
        if verbose == True:
            duplicated_fids = all_geospatial.loc[all_geospatial['feature_id'].duplicated() == True]['feature_id']
            
            print("Duplicate feature_id values:")
            print(duplicated_fids)

        output_log.append("Duplicate feature_id's removed from geospatial data table.")
        output_log.append(duplicated_fids)

        all_geospatial.drop_duplicates(subset='feature_id', inplace=True)

    # ------------------------------------------------------------------------------------------------
    # Combine lat and lon column to a shapely Point() object and convert to a geopackage format

    output_geopackage_temp = all_geospatial
    output_geopackage_temp['geometry'] = output_geopackage_temp.apply(lambda x: Point((float(x.lon), float(x.lat))), axis=1)
    output_geopackage = gpd.GeoDataFrame(output_geopackage_temp, geometry='geometry')

    # ------------------------------------------------------------------------------------------------
    # Export the output points geopackage and the rating curve table to the save folder

    geopackage_name = 'reformat_ras_rating_curve_points.gpkg'
    geopackage_path = os.path.join(output_save_folder, geopackage_name)
    output_geopackage.to_file(geopackage_path, driver='GPKG')

    csv_name = 'reformat_ras_rating_curve_table.csv'
    csv_path = os.path.join(output_save_folder, csv_name)
    all_outputs_table.to_csv(csv_path, index=False)

    # ------------------------------------------------------------------------------------------------
    # Print filepaths and logs (if the verbose and log arguments are selected)

    if verbose == True:

        print()
        print(f"Geopackage save location: {geopackage_path}")
        print(f"Compiled rating curve csv save location: {csv_path}") 

        print()
        print("Log:")
        print(*output_log, sep = "\n")

    output_log.append(f"Geopackage save location: {geopackage_path}")
    output_log.append(f"Compiled rating curve csv save location: {csv_path}") 

    # Save output log if the log option was selected
    if log == True:
        log_name = 'reformat_ras_rating_curve_log.txt'
        log_path = os.path.join(output_save_folder, log_name)

        with open(log_path, 'w') as f:
            for line in output_log:
                f.write(f"{line}\n")

        print(f"Log saved to {log_path}.")

    else:
        print("Output log not saved.")
                    
if __name__ == '__main__':

    # Sample usage:
    """
    <sample usage>

    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i '/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs' -o '/Users/rdp-user/projects/reformat-ras2fim/temp' -v -l

     Notes:
       - 
       - To save the output log, use the '-l' tag
       - To run in a verbose setting, use the '-v' tag

    """
        
    # Parse arguments
    parser = argparse.ArgumentParser(description='Iterate through a directory containing ras2fim outputs and compile a rating curve table and rating curve location point file.')
    parser.add_argument('-i', '--input_path', 
                        help='Input directory containing ras2fim outputs to process.', 
                        required=True)
    parser.add_argument('-o', '--output_path',
                        help='Output save folder.',
                        required=True)
    parser.add_argument('-l', '--log', help='Option to save output log to output save folder.', required=False, default=False, action='store_true')
    parser.add_argument('-v', '--verbose', help='Option to print more updates and descriptions.', required=False, default=False, action='store_true')
    # parser.add_argument('-j', '--num-workers',help='Number of concurrent processes', required=False, default=1, type=int)

    # Assign variables from arguments.
    args = vars(parser.parse_args())

    input_folder_path = args['input_path']
    output_save_folder = args['output_path']
    log = bool(args['log'])
    verbose = bool(args['verbose'])
    # num_workers = args['num_workers']

    ## potential table outputs that could be arguments: 
    # location_type = "RAS2FIM" # type of location the data is coming from
    # active = True # should be TRUE or FALSE
    # source = "temp source id" # where the model came from (example: IFC)

    # Record and print start time
    start_time = datetime.datetime.now()
    start_time_string = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    print("============================================")
    print(f"Started compiling ras2fim rating curves at {start_time_string}.")
    print(f"Verbose: {str(verbose)}")
    print(f"Save output log to folder: {str(log)}")
    print()

    # # Check job numbers
    # total_cpus_requested = job_number_huc * job_number_branch
    # total_cpus_available = os.cpu_count() - 1
    # if total_cpus_requested > total_cpus_available:
    #     raise ValueError('The HUC job number, {}, multiplied by the branch job number, {}, '\
    #                       'exceeds your machine\'s available CPU count minus one. '\
    #                       'Please lower the job_number_huc or job_number_branch'\
    #                       'values accordingly.'.format(job_number_huc,job_number_branch))

    # Run main function
    compile_ras_rating_curves(input_folder_path, output_save_folder, log, verbose) #+num_workers?

    # Record end time, calculate runtime, and print runtime
    end_time = datetime.datetime.now()
    runtime = end_time - start_time

    print()
    print("Process finished.") 
    print("Total runtime:", runtime)
    print("============================================")
