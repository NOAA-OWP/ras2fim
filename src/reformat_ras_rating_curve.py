#!/usr/bin/env python3

import os, re, datetime
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas.tools import overlay
from shapely.geometry import LineString, Point
import rasterio

# ------------------------------------------------------------------------------------------------
# Read inputs and arguments (## eventually, convert to args for convert_ras2fim_rating_curve() )

# Required: 
input_folder_path = "/Users/rdp-user/projects/reformat-ras2fim/ras2fim_test_outputs" ## eventually this path will be an arg
output_save_folder = "/Users/rdp-user/projects/reformat-ras2fim/temp"

# Optional:
# nwm_streamlines_path =  r"\ras2fim_data\inputs\X-National_Datasets\nwm_flows.gpkg"# default: [unsure what path should be default] 
log = True # true or false, default is false
verbose = True # true or false, default is false

# convert_ras2fim_rating_curve(input_folder_path, nwm_streamlines_path, output_save_folder, log, verbose)

# ------------------------------------------------------------------------------------------------
# Read in NWM geopackage ** sidelined for now, not sure if we need to do any of this crosswalking
# def read_crosswalking_dataset(crosswalk_dataset_path):
#     """
#     Placeholder function (as of 5/23/23)
#     This function reads in the baseline dataset(s) for crosswalking the ras2fim data to the NWM data.
    
#     """

#     print("Reading geopackage...")
#     startgpkg =  datetime.datetime.now()
#     nwm_geopkg = gpd.read_file(crosswalk_dataset_path)
#     endgpkg =  datetime.datetime.now()
#     print("Time to read geopackage: " + str(endgpkg - startgpkg))

#     # List columns in geopackage
#     for col in nwm_geopkg.columns.drop('geometry'):
#         print(col)
#         data = nwm_geopkg[col]

#     print(data)

# ------------------------------------------------------------------------------------------------
# Define  datum conversion tool
# def ngvd_to_navd_ft(datum_info, region = 'contiguous'):
#     '''
#     Given the lat/lon, retrieve the adjustment from NGVD29 to NAVD88 in feet. 
#     Uses NOAA tidal API to get conversion factor. Requires that lat/lon is
#     in NAD27 crs. If input lat/lon are not NAD27 then these coords are 
#     reprojected to NAD27 and the reproject coords are used to get adjustment.
#     There appears to be an issue when region is not in contiguous US.

#     Parameters
#     ----------
#     lat : FLOAT
#         Latitude.
#     lon : FLOAT
#         Longitude.

#     Returns
#     -------
#     datum_adj_ft : FLOAT
#         Vertical adjustment in feet, from NGVD29 to NAVD88, and rounded to nearest hundredth.

#     '''
#     #If crs is not NAD 27, convert crs to NAD27 and get adjusted lat lon
#     if datum_info['crs'] != 'NAD27':
#         lat, lon = convert_latlon_datum(datum_info['lat'],datum_info['lon'],datum_info['crs'],'NAD27')
#     else:
#         #Otherwise assume lat/lon is in NAD27.
#         lat = datum_info['lat']
#         lon = datum_info['lon']
    
#     #Define url for datum API
#     datum_url = 'https://vdatum.noaa.gov/vdatumweb/api/convert'     
    
#     #Define parameters. Hard code most parameters to convert NGVD to NAVD.    
#     params = {}
#     params['lat'] = lat
#     params['lon'] = lon
#     params['region'] = region
#     params['s_h_frame'] = 'NAD27'     #Source CRS
#     params['s_v_frame'] = 'NGVD29'    #Source vertical coord datum
#     params['s_vertical_unit'] = 'm'   #Source vertical units
#     params['src_height'] = 0.0        #Source vertical height
#     params['t_v_frame'] = 'NAVD88'    #Target vertical datum
#     params['tar_vertical_unit'] = 'm' #Target vertical height
    
#     #Call the API
#     response = requests.get(datum_url, params = params, verify=False)

#     #If successful get the navd adjustment
#     if response:
#         results = response.json()
#         #Get adjustment in meters (NGVD29 to NAVD88)
#         adjustment = results['t_z']
#         #convert meters to feet
#         adjustment_ft = round(float(adjustment) * 3.28084,2)                
#     else:
#         adjustment_ft = None
#     return adjustment_ft    

# ------------------------------------------------------------------------------------------------
# Scrape metadata and geodata filepaths from directory (unit, projection, datum) 

def get_directory_metadata(input_folder_path, dir):

    global grouped_proj_df, nwm_no_match_filepath, nwm_all_lines_filepath

    # Input target metadata (format that the output rating curve needs to be in)
    target_metadata = pd.DataFrame({'Type': ['DATUM', 'GEOGCS', 'PROJCS', 'PROJECTION'], 'Value': ['text1', 'text2', 'text3', 'text4']})

    root_dir = os.path.join(input_folder_path, dir)
    proj_ext = ".prj"
    proj_words = ['PROJCS', 'GEOGCS', 'DATUM', 'PROJECTION']

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

            # Find .prj files
            elif filename.endswith(proj_ext):
                filepath = os.path.join(dirpath, filename)

                # Open .prj files and pull out the metadata
                with open(filepath, 'r') as f:

                    for line_num, line in enumerate(f, start=1):

                        # Check units
                        if 'SI Units' in line:
                            unit = 'meter'
                            new_row = {'Directory': dir, 'File': filename, 'Type': 'Unit', 'Value': unit} 
                            proj_df.loc[len(proj_df)] = new_row
                        elif 'English Units' in line:
                            unit = 'foot'
                            new_row = {'Directory': dir, 'File': filename, 'Type': 'Unit', 'Value': unit} 
                            proj_df.loc[len(proj_df)] = new_row

                        # Search for the metadata keyword using a regular expression
                        for proj_word in proj_words:
                            match = re.search(f"{proj_word}\\[\"(.*?)\"", line)

                            # If metadata is found, add it to the table
                            if match:
                                new_row = {'Directory': dir, 'File': filename, 'Type': proj_word, 'Value': match.group(1)} 
                                proj_df.loc[len(proj_df)] = new_row

    # Check that each metadata type has only one value for each directory
    unique_check = proj_df.groupby(['Directory', 'Type'])['Value'].nunique()
    if (unique_check > 1).any():
        print("Number of unique values: ")
        print(unique_check)
        raise ValueError('Error: More than one value found for each metadata type.') ## after I test this on a larger set, we will see how common this is. 
                                                                                        ## if it's super common, figure out a contingency plan (maybe the 
                                                                                        ## projection from a certain file should be prioritized?)

    # Group the dataframe by Directory and Type, and get the unique Value for each group
    grouped_proj_df = proj_df.groupby(['Directory', 'Type'])['Value'].unique().reset_index()

    # Check that there is a value for all metadata categories?
    missing_values = set(['PROJCS', 'GEOGCS', 'DATUM', 'PROJECTION', 'Unit']) - set(grouped_proj_df['Type'].unique())
    if missing_values:
        print(f"Warning: The following values are missing {dir} metadata: {missing_values}")
        ## maybe eventually: if no value was found for one of the metadata categories, add in a default value here? 

    # Remove "[" and "]" from values in the "Value" column
    grouped_proj_df['Value'] = grouped_proj_df['Value'].str[0].str.replace(r'[\[\]]', '')

    # This function creates: grouped_proj_df, nwm_no_match_filepath, nwm_all_lines_filepath


# ------------------------------------------------------------------------------------------------
# Get subfolders of working directory and iterate through each subdirectory (use multiprocessor here)
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

# Create empty output log
output_log = []

for dir in dirlist: ## eventually this will be replaced with the multiprocessor 
    print(" ")
    print(f"Directory: {dir}")

    # Make blank df to hold projections
    proj_df = pd.DataFrame(columns = ['Directory', 'File', 'Type', 'Value'])

    # ------------------------------------------------------------------------------------------------
    # Check for rating curve, get metadata and read it in if it exists

    rc_path = os.path.join(input_folder_path, dir, "06_ras2rem", "rating_curve.csv")
    if os.path.isfile(rc_path) == False:
        newlog = f"No rating curve file available for directory: {dir}."
        print(newlog)
        output_log.append(newlog)
    else:
        print("Rating curve csv available.")

        # ------------------------------------------------------------------------------------------------
        # Scrape metadata from directory (unit, projection, datum) 

        get_directory_metadata(input_folder_path, dir)

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

        print("midpoints projection " + str(midpoints_gdf.crs))
        print("terrain projection: " + str(terrain.crs))

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

        # ------------------------------------------------------------------------------------------------
        # Placeholder: Determine whether an elevation adjustment is needed to supplement the cross walking. if so, further adjust the rating curve
        

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

## end of multiprocessor

# ------------------------------------------------------------------------------------------------
# Check outputs and log warnings

## warn if there's duplicate feature_ids in all_geospatial 

## remove duplicate feature_id's in all_geospatial

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

print(f"gpkg location: {geopackage_path}")
print(f"csv location: {csv_path}")


print("Output log:")
print(*output_log, sep = "\n")
