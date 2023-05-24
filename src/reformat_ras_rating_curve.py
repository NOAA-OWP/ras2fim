#!/usr/bin/env python3

import os, re, datetime
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas.tools import overlay


# ------------------------------------------------------------------------------------------------
# Read inputs and arguments (## eventually, convert to args for convert_ras2fim_rating_curve() )

# Required: 
input_folder_path = r"\Users\rdp-user\projects\reformat-ras2fim\ras2fim_test_outputs" ## eventually this path will be an arg
output_save_folder = "save_folder_path_temp"

# Optional:
# nwm_streamlines_path =  r"\ras2fim_data\inputs\X-National_Datasets\nwm_flows.gpkg"# default: [unsure what path should be default] 
log = True # true or false, default is false
verbose = True # true or false, default is false


# convert_ras2fim_rating_curve(input_folder_path, nwm_streamlines_path, output_save_folder, log, verbose)

# ------------------------------------------------------------------------------------------------
# Read in NWM geopackage ** sidelined for now, not sure if we need to do any of this crosswalking
def read_crosswalking_dataset(crosswalk_dataset_path):
    """
    Placeholder function (as of 5/23/23)
    This function reads in the baseline dataset(s) for crosswalking the ras2fim data to the NWM data.
    
    """

    print("Reading geopackage...")
    startgpkg =  datetime.datetime.now()
    nwm_geopkg = gpd.read_file(crosswalk_dataset_path)
    endgpkg =  datetime.datetime.now()
    print("Time to read geopackage: " + str(endgpkg - startgpkg))

    # List columns in geopackage
    for col in nwm_geopkg.columns.drop('geometry'):
        print(col)
        data = nwm_geopkg[col]

    print(data)

# ------------------------------------------------------------------------------------------------
# Define  datum conversion tool
def ngvd_to_navd_ft(datum_info, region = 'contiguous'):
    '''
    Given the lat/lon, retrieve the adjustment from NGVD29 to NAVD88 in feet. 
    Uses NOAA tidal API to get conversion factor. Requires that lat/lon is
    in NAD27 crs. If input lat/lon are not NAD27 then these coords are 
    reprojected to NAD27 and the reproject coords are used to get adjustment.
    There appears to be an issue when region is not in contiguous US.

    Parameters
    ----------
    lat : FLOAT
        Latitude.
    lon : FLOAT
        Longitude.

    Returns
    -------
    datum_adj_ft : FLOAT
        Vertical adjustment in feet, from NGVD29 to NAVD88, and rounded to nearest hundredth.

    '''
    #If crs is not NAD 27, convert crs to NAD27 and get adjusted lat lon
    if datum_info['crs'] != 'NAD27':
        lat, lon = convert_latlon_datum(datum_info['lat'],datum_info['lon'],datum_info['crs'],'NAD27')
    else:
        #Otherwise assume lat/lon is in NAD27.
        lat = datum_info['lat']
        lon = datum_info['lon']
    
    #Define url for datum API
    datum_url = 'https://vdatum.noaa.gov/vdatumweb/api/convert'     
    
    #Define parameters. Hard code most parameters to convert NGVD to NAVD.    
    params = {}
    params['lat'] = lat
    params['lon'] = lon
    params['region'] = region
    params['s_h_frame'] = 'NAD27'     #Source CRS
    params['s_v_frame'] = 'NGVD29'    #Source vertical coord datum
    params['s_vertical_unit'] = 'm'   #Source vertical units
    params['src_height'] = 0.0        #Source vertical height
    params['t_v_frame'] = 'NAVD88'    #Target vertical datum
    params['tar_vertical_unit'] = 'm' #Target vertical height
    
    #Call the API
    response = requests.get(datum_url, params = params, verify=False)

    #If successful get the navd adjustment
    if response:
        results = response.json()
        #Get adjustment in meters (NGVD29 to NAVD88)
        adjustment = results['t_z']
        #convert meters to feet
        adjustment_ft = round(float(adjustment) * 3.28084,2)                
    else:
        adjustment_ft = None
    return adjustment_ft    

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

    print("Metadata: ")
    print(grouped_proj_df)

    print(f"no match filepath: {nwm_no_match_filepath}")
    print(f"all lines filepath: {nwm_all_lines_filepath}")

    # This function creates: grouped_proj_df, nwm_no_match_filepath, nwm_all_lines_filepath


# ------------------------------------------------------------------------------------------------
# Get subfolders of working directory and iterate through each subdirectory (use multiprocessor here)
dirlist = os.listdir(input_folder_path)

for dir in dirlist: ## eventually this will be replaced with the multiprocessor 
    print(" ")
    print(f"Directory: {dir}")

    # Make blank df to hold projections
    proj_df = pd.DataFrame(columns = ['Directory', 'File', 'Type', 'Value'])

    # ------------------------------------------------------------------------------------------------
    # Check for rating curve, get metadata and read it in if it exists
    

    rc_path = os.path.join(input_folder_path, dir, "06_ras2rem", "rating_curve.csv")
    if os.path.isfile(rc_path) == False:
        newlog = f"No rating curve file available for {dir}."
        print(newlog)
        ## eventually instead of printing this will be logged (maybe if the log option is selected?)
    else:
        print("Rating curve csv available.")

        # ------------------------------------------------------------------------------------------------
        # Scrape metadata from directory (unit, projection, datum) 

        get_directory_metadata(input_folder_path, dir)

        # print("TEST OUTPUTS: ")
        # print(grouped_proj_df)
        # print(nwm_no_match_filepath)
        # print(nwm_all_lines_filepath)

        # ------------------------------------------------------------------------------------------------
        # Read rating curve and extract data

        rc_df = pd.read_csv(rc_path)    

        # Extract the stage and discharge units from column headers
        stage_units = rc_df.columns[rc_df.columns.str.contains('stage', case=False)][0].split('(')[1].strip(')')
        discharge_units = rc_df.columns[rc_df.columns.str.contains('Discharge', case=False)][0].split('(')[1].strip(')')

        # print("Rating curve: ")    
        # print(rc_df.head())

        # ------------------------------------------------------------------------------------------------
        # Read in the shapefile for the directory
        
        # Read shapefiles
        nwm_no_match_shp = gpd.read_file(nwm_no_match_filepath)
        nwm_all_lines_shp = gpd.read_file(nwm_all_lines_filepath)

        nwm_diff = overlay(nwm_all_lines_shp, nwm_no_match_shp, how="difference")

        ## Check data types 
        # print(type(nwm_no_match_shp))
        # print(type(nwm_all_lines_shp))
        # print(type(nwm_diff))
        # print(nwm_diff.head())


        # Save file to preview
        # print("saving file")
        # nwm_diff.to_file(r"\Users\rdp-user\projects\reformat-ras2fim\temp\nwm_diff.gpkg", driver="GPKG")



        # Clean workspace
        del nwm_no_match_shp, nwm_all_lines_shp

        # ------------------------------------------------------------------------------------------------
        # Placeholder: Crosswalk the sites to the correct FIM feature ID 
        
        # Connect the sites to the correct location ID's (or do this in the later for loop??)
        location_id = "temp location id"

    
        # ------------------------------------------------------------------------------------------------
        # Project geodatabase and get lat and lon of centroids

        nwm_diff_prj = nwm_diff.to_crs(4326)
        nwm_diff_prj['lon'] = nwm_diff_prj.centroid.x  
        nwm_diff_prj['lat'] = nwm_diff_prj.centroid.y

        # ***** UserWarning: Geometry is in a geographic CRS. Results from 'centroid' are likely incorrect. 
        # Use 'GeoSeries.to_crs()' to re-project geometries to a projected CRS before this operation.

        print("lat and lon (nwm_diff_prj): ")
        print(nwm_diff_prj['lon'])
        print(nwm_diff_prj['lat'])

        ## reproject to ESRI:102039


        # ------------------------------------------------------------------------------------------------
        # Pull datum information from the datum API

        # based on ngvd_to_navd_ft() in NOAA-OWP/inundation-mapping/blob/dev/tools/tools_shared_functions.py (line 1099)




        
        # ngvd_to_navd_ft(datum_info, region = 'contiguous')







        ## ** need to confirm output datum 
        ## -> I think it is navd88 (north american vertical datum 88) -> https://geodesy.noaa.gov/datums/vertical/north-american-vertical-datum-1988.shtml

        datum = "temp datum" # numerical 
        datum_vcs = "NAVD88" # output datum name?
        navd88_datum = "temp davd88 datum" # numerical, same as datum?
        elevation_navd88 = "temp elevation navd88" # stage + datum?



        # output vertical datum: navd88 (north american vertical datum 88) -> https://geodesy.noaa.gov/datums/vertical/north-american-vertical-datum-1988.shtml


        # ------------------------------------------------------------------------------------------------
        # Placeholder: Determine whether an elevation adjustment is needed to supplement the cross walking. if so, further adjust the raing curve
        



        # ------------------------------------------------------------------------------------------------
        # Make output rating curve table for the directory (to append to larger table and then export as a csv)

        # Prep last output variables
        wrds_timestamp = datetime.datetime.now() # current timestamp (## double check?)
        location_type = "RAS2FIM" # type of location the data is coming from
        active = "temp active" # should be TRUE or FALSE
        source = "temp source id" # where the model came from (example: IFC)

        output_data_dir = {'flow': rc_df.iloc[:,2], 
                            'stage': rc_df.iloc[:,1],
                            'location_id': location_id,
                           'location_type': location_type, 
                           'source': source, 
                           'flow_units': discharge_units, 
                           'stage_units': stage_units, 
                           'wrds_timestamp': wrds_timestamp, 
                           'active': active, 
                           'datum': datum, 
                           'datum_vcs': datum_vcs, 
                           'navd88_datum': navd88_datum, 
                           'elevation_navd88': elevation_navd88}

        output_df_dir = pd.DataFrame(output_data_dir)

        ##print(output_df_dir)
    ## end of multiprocessor
    ## join together all tables? (if they haven't already)

    # ------------------------------------------------------------------------------------------------
    # Use the lat lon table to create the output points geopackage



    # ------------------------------------------------------------------------------------------------
    # Export the output points geopackage and the rating curve table to the save folder

    # write_csv()






    


        























