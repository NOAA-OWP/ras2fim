#!/usr/bin/env python3

import os, re, sys
import argparse, datetime
import shutil
import gdal
import osr
import rasterio
import rasterio.crs
import pandas as pd
import numpy as np 
import geopandas as gpd
from tqdm import tqdm
from rasterio.merge import merge
from geopandas.tools import overlay
from shapely.geometry import LineString, Point
from concurrent.futures import ProcessPoolExecutor

import shared_variables as sv

# -----------------------------------------------------------------
# Writes a metadata file into the save directory
# -----------------------------------------------------------------
def write_metadata_file(output_save_folder, start_time_string, midpoints_crs):

    """
    Overview:

    Creates a metadata textfile and saves it to the output save folder.


    Parameters (will error if do not exist):
    
    - output_save_folder: str
        filepath for folder to put output files (optional arguments set in __main__ or defaults to value from ## )    

    - start_time_string: str
        system datetime when the code was run

    """

    metadata_content = []
    metadata_content.append(' ')
    metadata_content.append(f'Data was produced using reformat_ras_rating_curve.py on {start_time_string}.')
    metadata_content.append(' ')
    metadata_content.append('ras2fim file inputs:')
    metadata_content.append('  /02_shapes_from_conflation/<dir>_no_match_nwm_lines.shp')
    metadata_content.append('  /02_shapes_from_conflation/<dir>_nwm_streams_ln.shp')
    metadata_content.append('  /03_terrain/<numerical_id>.tif')
    metadata_content.append('  /06_ras2rem/rating_curve.csv')
    metadata_content.append(' ')
    metadata_content.append('Outputs: ')
    metadata_content.append('  reformat_ras_rating_curve_points.gpkg (point location geopackage)')
    metadata_content.append('  reformat_ras_rating_curve_table.csv (rating curve CSV)')
    metadata_content.append('  reformat_ras_rating_curve_log.txt (output log textfile, only saved if -l argument is used)')
    metadata_content.append(' ')
    metadata_content.append('Metadata:')
    metadata_content.append('Column name        Source                  Type            Description')
    metadata_content.append('flow               From rating curve       Number          Discharge value from rating curve in each directory')
    metadata_content.append('stage              From rating curve       Number          Stage value from the rating curve in each directory')
    metadata_content.append('feature_id         From geometry files     Number          NWM feature ID associated with the stream segment')
    metadata_content.append('location_type      User-provided           String          Type of site the data is coming from (example: IFC or USGS) (optional)')
    metadata_content.append('source             User-provided           String          Source that produced the data (example: RAS2FIM) (optional)')
    metadata_content.append('flow_units         From rating curve       String          Discharge units, from the directory rating curve column header')
    metadata_content.append('stage_units        From rating curve       String          Stage units, from the directory rating curve column header')
    metadata_content.append('wrds_timestamp     Calculated in script    Datetime        Describes when this table was compiled')
    metadata_content.append('active             User-provided           True/False      Whether a gage is active (optional)')
    metadata_content.append('datum              Calculated from terrain Number          Elevation at midpoint at input datum (currently assumed that input datum is NAVD88)')
    metadata_content.append('datum_vcs          User-provided           String          desired output datum (defaults to NAVD88)')
    metadata_content.append('navd88_datum       Calculated in script    Number          elevation at midpoint at output datum ')
    metadata_content.append('elevation_navd88   Calculated in script    Number          Stage elevation at midpoint at output datum (calculated as NAVD88_datum + stage)')
    metadata_content.append('lat                Calculated from streams Number          Latitude of midpoint associated with feature_id. CRS: '+ str(midpoints_crs))
    metadata_content.append('lon                Calculated from streams Number          Longitude of midpoint associated with feature_id. CRS:  '+ str(midpoints_crs))

    metadata_name = 'README_reformat_ras_rating_curve.txt'
    metadata_path = os.path.join(output_save_folder, metadata_name)

    with open(metadata_path, 'w') as f:
        for line in metadata_content:
            f.write(f"{line}\n")

# -----------------------------------------------------------------
# Reads, compiles, and reformats the rating curve info for each directory
# -----------------------------------------------------------------
def dir_reformat_ras_rc(dir, input_folder_path, verbose, intermediate_filename, 
                        # output_save_folder, 
                        int_output_table_label, 
                        int_geospatial_label, int_log_label, source, 
                        location_type, active, input_vdatum, nodataval, midpoints_crs):

    """
    Overview:
    
    Reads, compiles, and reformats the rating curve info for the given directory (runs in compile_ras_rating_curves). 

    Notes:
        - Currently has placeholders for crosswalking the sites to a feature_id if they are not already correct, 
          converting the vertical datum if needed, and adjusting the rating curve elevation to supplement the 
          cross walking.

        - Automatically overwrites the main outputs (the compiled CSV, geopackage, and log) if they already 
          exist in the output folder. If there is a need to keep the existing main outputs, use a different 
          output folder.

    
    Parameters (will error if do not exist):

    - dir: str
        name of file on which to run `dir_reformat_ras_rc` (ras2fim output file containing steps 01 through 06)
    
    - input_folder_path: str
        filepath for folder containing input ras2fim models (optional arguments set in __main__ or defaults to value from __)
    
    - output_save_folder: str
        filepath for folder to put output files (optional arguments set in __main__ or defaults to value from __)        
    
    - verbose: bool
        option to run verbose code with a lot of print statements (optional argument set in __main__)
    
    - source: str
        optional input value for the "source" output column (i.e. "", "ras2fim", "ras2fim v2.1") 
    
    - location_type: str
        optional input value for the "location_type" output column (i.e. "", "USGS", "IFC")
    
    - active: str
        optional input value for the "active" column (i.e. "", "True", "False")
    
    - intermediate_filename: str
        name of file to store intermediates (set in compile_ras_rating_curves, defaults to "intermediate_outputs")
    
    - int_output_table_label: str 
        suffix for intermediate output table (set in compile_ras_rating_curves, defaults to "_output_table.csv")
    
    - int_geospatial_label: str 
        suffix for intermediate output table (set in compile_ras_rating_curves, defaults to "_geospatial.csv")
    
    - int_log_label: str
        suffix for intermediate output table (set in compile_ras_rating_curves, defaults to "_log.txt")
    
    - input_vdatum: str
        vertical datum of the input ras2fim data (defaults to "NAVD88", only used to fill the datum column)
    
    - nodataval: int
        value to use for no data (from ___ ## defaults file??)
    

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

    # Create intermediate output file within directory (08_adjusted_src)
    intermediate_filepath = os.path.join(input_folder_path, dir, intermediate_filename)
    if not os.path.exists(intermediate_filepath):
        os.mkdir(intermediate_filepath)

    if os.path.isfile(rc_path) == False:
        print(f"No rating curve file available for {dir}.")
        output_log.append("Rating curve NOT available.")
        dir_log_filename = str(dir) + int_log_label
        dir_log_filepath = os.path.join(intermediate_filepath, dir_log_filename)

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

        # Find filepaths for NWM streams and NWM streams no match 
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
        huc8_list = []

        # Project to CRS to get the coordinates in the correct format
        nwm_diff_prj = nwm_diff.to_crs(midpoints_crs) 

        # Get middle segment of the flowlines
        for index, row in nwm_diff_prj.iterrows():
            feature_id = nwm_diff_prj['feature_id'][index] 

            # Get HUC8 
            huc8 = nwm_diff_prj['huc8'][index]

            # Create linestring      
            line = LineString(nwm_diff_prj['geometry'][index])

            # Calculate linestring midpoint and retrieve coordinates
            midpoint = line.interpolate(line.length / 2)
            midpoint_x = midpoint.x
            midpoint_y = midpoint.y

            # Append results to output lists
            huc8_list.append(huc8)
            feature_id_list.append(feature_id)
            midpoint_x_list.append(midpoint_x)
            midpoint_y_list.append(midpoint_y)

        # Make output dataframe
        midpoints_df = pd.DataFrame(
            {'huc8': huc8_list, 
             'feature_id': feature_id_list,
            'midpoint_lon': midpoint_x_list,
            'midpoint_lat': midpoint_y_list
            })

        # [Placeholder] Crosswalk the sites to the correct FIM feature ID if they are not already correct

        # ------------------------------------------------------------------------------------------------
        # Read terrain file and get projection information
        terrain_folder_path = os.path.join(input_folder_path, dir, "03_terrain")

        # Get a list of terrain filenames
        terrain_files = []
        for file in os.listdir(terrain_folder_path):
            if file.endswith('.tif'):
                terrain_files.append(file)

        if (len(terrain_files) == 1):
            terrain_file_path = os.path.join(terrain_folder_path, terrain_files[0])
            terrain = rasterio.open(terrain_file_path)

            # Get the projection and units of the raster
            d = gdal.Open(terrain_file_path)
            proj = osr.SpatialReference(wkt=d.GetProjection())
            str_epsg_raster = proj.GetAttrValue('AUTHORITY', 1)
            str_unit_raster = proj.GetAttrValue('UNIT', 0)

        else:
            if verbose == True:
                print("Multiple terrain rasters found.")

            # Iterate through terrain rasters and merge into a mosaic
            raster_to_mosiac = []

            for p in tqdm(terrain_files, desc='Mosaicking raster file', unit = 'files'):
            # for p in terrain_files:
                terrain_file_path = os.path.join(terrain_folder_path, p)
                raster = rasterio.open(terrain_file_path)
                raster_to_mosiac.append(raster)
            mosaic, output = merge(raster_to_mosiac)

            # Prepare metadata for mosaic terrain raster
            # (Assumes that all the rasters in the dir have the same CRS and units)
            output_meta = raster.meta.copy()
            output_meta.update(
                {"driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": output,
                "dtype": rasterio.float64,
                "compress":"LZW"
                }
            )

            # Write mosaic terrain raster to intermediate folder with metadata
            dir_mosaic_filepath = os.path.join(intermediate_filepath, "{}_mosaic.tif".format(dir))
            with rasterio.open(dir_mosaic_filepath, "w", **output_meta) as tiffile:
                tiffile.write(mosaic)

            # Read mosaic terrain raster
            terrain = rasterio.open(dir_mosaic_filepath)

            # Get CRS from terrain raster mosaic
            str_epsg_raster = str(terrain.crs).replace("EPSG:", "")
            str_unit_raster = terrain.crs.linear_units

            if verbose == True:
                print("Terrain mosaic saved.")

        # [Placeholder] Convert vertical datum if needed. 
        # Potential example snippet: ngvd_to_navd_ft() in NOAA-OWP/inundation-mapping/blob/dev/tools/tools_shared_functions.py (line 1099)
        # For now, the output datum is NAVD88 and assumed input datum is assumed to be NAVD88 at this time (Emily, 6/2/23)

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

        if verbose == True:
            print(f"Number of midpoints: {str(len(midpoints_gdf))}")

        for index, row in tqdm(midpoints_gdf.iterrows(), desc='Getting elevation for midpoints', unit = ' points'):

            point = row['geometry']

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
            print("Midpoints projection " + str(midpoints_gdf))
            print("Terrain projection: " + str(terrain.crs))

        output_log.append("Midpoints projection " + str(midpoints_gdf.crs))
        output_log.append("Terrain projection: " + str(terrain.crs))

        # ------------------------------------------------------------------------------------------------
        # Compile elevation and datum information

        # Join raw elevation to rating curve using feature_id and drop the redundant feature_id column
        rc_elev_df = pd.merge(rc_df, midpoints_gdf[['feature_id', 'huc8', 'Raw_elevation', 'midpoint_lat', 'midpoint_lon']], 
                              left_on='feature-id', right_on='feature_id', how='left')
        rc_elev_df = rc_elev_df.drop('feature_id', axis=1)

        # If there is no datum conversion, the datum and the navd88_datum columns are both equal to Raw_elevation
        rc_elev_df['navd88_datum'] = rc_elev_df['Raw_elevation']
        rc_elev_df['datum'] = rc_elev_df['Raw_elevation']

        # Make column just called 'stage' with the stage values
        stage_columns = rc_elev_df.filter(like='stage')
        rc_elev_df['stage'] = stage_columns.iloc[:, 0]

        # Standardize unit names and provide error if needed: stage
        if re.search(r'\b(foot|ft|feet)\b', stage_units):
            stage_units = 'ft'
        elif re.search(r'\b(meter|meters|m|M|metre)\b', stage_units):
            stage_units = 'm'
        else:
            log = "Warning: Stage units not recognized as feet or meters."
            output_log.append(log)
            print(log)
        
        # Standardize unit names and provide error if needed: terrain raster
        if re.search(r'\b(foot|ft|feet)\b', str_unit_raster):
            str_unit_raster = 'ft'
        elif re.search(r'\b(meter|meters|m|M|metre)\b', str_unit_raster):
            str_unit_raster = 'm'
        else:
            log = "Warning: Raster units not recognized as feet or meters."
            output_log.append(log)
            print(log)

        # If the rating curve units aren't equal to the elevation units, convert them 
        if str_unit_raster == stage_units:
            rc_elev_df['stage_final'] = rc_elev_df['stage'] 
            log = 'Stage units and raster units are the same, no unit conversion needed.'

        elif stage_units == 'ft' and str_unit_raster == 'm':
            rc_elev_df['stage_final'] = rc_elev_df['stage'] / 3.281  ## meter = feet / 3.281
            log = 'Converting stage units to meter to match elevation data.'

        elif stage_units == 'm' and str_unit_raster == 'ft':
            rc_elev_df['stage_final'] = rc_elev_df['stage'] / 0.3048  ## feet = meters / 0.3048
            log = 'Converting stage units to feet to match elevation data.'
        else:
            log = f'Warning: No conversion available between {stage_units} and {str_unit_raster}. Stage set to nodata value ({nodataval}).'
            output_log.append(log)
            rc_elev_df['stage_final'] = nodataval

        if verbose == True:
            print(log)

        # [Placeholder] If an elevation adjustment is needed to supplement the cross walking, adjust rating curve here

        # If 'stage' does not equal a nodata value, calculate elevation_navd88 by adding 'navd88_datum' and 'stage'
        rc_elev_df.loc[rc_elev_df['stage_final'] != nodataval, 'elevation_navd88'] = rc_elev_df['stage_final'] + rc_elev_df['navd88_datum']

        # If stage DOES equal the nodata value, set the 'elevation_navd88' to the nodata value as well
        rc_elev_df.loc[rc_elev_df['stage_final'] == nodataval, 'elevation_navd88'] = nodataval

        # ------------------------------------------------------------------------------------------------
        # Make output tables

        # Make column just called 'flow' with the discharge values (removes unit labels in discharge column header)
        flow_columns = rc_elev_df.filter(like='Discharge')
        rc_elev_df['flow'] =  flow_columns.iloc[:, 0]
        
        # Get a current timestamp
        wrds_timestamp = datetime.datetime.now() 

        dir_output_table = pd.DataFrame({'flow' : rc_elev_df['flow'],
                                         'stage' : rc_elev_df['stage_final'], # the stage value that was corrected for units
                                         'feature_id' : rc_elev_df['feature-id'], 
                                         'huc8' : rc_elev_df['huc8'],  #str
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
        
        dir_geospatial = dir_output_table.drop(columns=['stage', 'flow', 'elevation_navd88']) # If you want to keep `elevation_navd88` in the geopackage, 
                                                                                              # remove it from this command.
        dir_geospatial = dir_geospatial.drop_duplicates(subset='feature_id', keep='first')

        # ------------------------------------------------------------------------------------------------
        # Export dir_output_table, dir_geospatial, and log to the intermediate save folder

        # Save output table for directory
        dir_output_table_filename = str(dir) + int_output_table_label 
        dir_output_table_filepath = os.path.join(intermediate_filepath, dir_output_table_filename)
        dir_output_table.to_csv(dir_output_table_filepath, index=False)        

        # Save geospatial table for directory
        dir_geospatial_filename = str(dir) + int_geospatial_label
        dir_geospatial_filepath = os.path.join(intermediate_filepath, dir_geospatial_filename)
        dir_geospatial.to_csv(dir_geospatial_filepath, index=False)  

        # Save log for directory
        dir_log_filename = str(dir) + int_log_label
        dir_log_filepath = os.path.join(intermediate_filepath, dir_log_filename)
        with open(dir_log_filepath, 'w') as f:
            for line in output_log:
                f.write(f"{line}\n")

        if verbose == True:
            print(f"Saved multiprocessor outputs for {dir}.")

# -----------------------------------------------------------------
# Compiles the rating curve and points from each directory 
# -----------------------------------------------------------------
def compile_ras_rating_curves(input_folder_path, output_save_folder, log, verbose, num_workers, 
                              overwrite, source, location_type, active):

    """
    Overview:

    Creates directory list and feeds directories to dir_reformat_ras_rc() inside a multiprocessor.
    Compiles the rating curve and geopackage info from the intermediate data folder and saves a final 
    rating curve CSV and geospatial outputs geopackage. Runs from __main__.


    Parameters (will error if do not exist):

    - input_folder_path: str
        filepath for folder containing input ras2fim models (optional arguments set in __main__ or defaults to value from ## )
    
    - output_save_folder: str
        filepath for folder to put output files (optional arguments set in __main__ or defaults to value from ## )        
    
    - verbose: bool
        option to run verbose code with a lot of print statements (argument set in __main__)

    - log: bool
        option to save output logs as a textfile (optional argument set in __main__)

    - num_workers: int
        number of workers to use during parallel processing (optional argument set in __main__)

    - overwrite: bool
        option to overwrite the existing files in the intermediate outputs file (optional argument set in __main__)
    
    - source: str
        optional input value for the "source" output column (i.e. "", "ras2fim", "ras2fim v2.1") 
    
    - location_type: str
        optional input value for the "location_type" output column (i.e. "", "USGS", "IFC")
    
    - active: str
        optional input value for the "active" column (i.e. "", "True", "False")
    
    """

    # Set nodata value
    nodataval = sv.DEFAULT_NODATA_VAL # get nodata val from default variables
    
    # Establish file naming conventions
    int_output_table_label = "_output_table.csv"
    int_geospatial_label = "_geospatial.csv"
    int_log_label = "_log.txt"
    intermediate_filename = "08_adjusted_src"
    
    # Record and print start time
    start_time = datetime.datetime.now()
    start_time_string = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")

    # Set default vertical datum and print warning about vertical datum conversion
    input_vdatum = "NAVD88"
    output_vdatum = sv.OUTPUT_VERTICAL_DATUM

    # Set CRS for midpoints data
    midpoints_crs = 'EPSG:4326'

    # Settings block
    print("-----------------------------------------------------------------------------------------------")
    print("Begin rating curve compilation process.")
    print()
    print(f"Start time: {start_time_string}.")
    print()
    print("Settings: ")
    print(f"    Verbose: {str(verbose)}")
    print(f"    Save output log to folder: {str(log)}")
    print(f"    Number of workers: {num_workers}")
    print(f"    No data value: {nodataval}")
    if input_vdatum == output_vdatum:
        print(f"    No datum conversion will take place.")
    print()

    # Get default paths from shared variables if they aren't included
    if input_folder_path == "":
        input_folder_path = sv.R2F_DEFAULT_OUTPUT_MODELS # ras2fim output folder
        print(f"    Using default input folder path: {input_folder_path}")

        if not os.path.exists(input_folder_path):
            print(f"    No file exists at {input_folder_path}")

    # Error out if the input folder doesn't exist
    if not os.path.exists(input_folder_path):
        sys.exit(f"    No folder at input folder path: {input_folder_path}")

    # Check for output folders
    if output_save_folder == "":
        
        # Error if the parent directory is missing
        if not os.path.exists(sv.R2F_OUTPUT_DIR_RELEASES):
            print(f"Error: Attempted to use default output save folder but parent directory {str(sv.R2F_OUTPUT_DIR_RELEASES)} is missing.")
            print("Create parent directory or specify a different output folder using `-o` followed by the directory filepath.")
            sys.exit()

        # Check that default output folder exists
        output_save_folder = os.path.join(sv.R2F_OUTPUT_DIR_RELEASES, "compiled_rating_curves")
        print(f"    Using default output save folder: {output_save_folder}")

        # Attempt to make default output folder if it doesn't exist
        if not os.path.exists(output_save_folder): 
            print(f"    No folder found at {input_folder_path}")
            try:
                print("    Creating output folder.")
                os.mkdir(output_save_folder)
            except OSError:
                print(OSError)
                sys.exit(f"Unable to create folder at {input_folder_path}")

    else:

        # Check that user-inputted output folder exists
        if not os.path.exists(output_save_folder):
            sys.exit(f"No folder found at {output_save_folder}")

    # Check job numbers
    total_cpus_requested = num_workers
    total_cpus_available = os.cpu_count() - 2 
    if total_cpus_requested > total_cpus_available:
        raise ValueError("Total CPUs requested exceeds your machine\'s available CPU count minus one. "\
                          "Please lower the quantity of requested workers accordingly.")

    # Get a list of the directories in the input folder path
    dirlist = os.listdir(input_folder_path)

    # Create empty output log and give it a header
    output_log = []
    output_log.append(f"Processing for reformat_ras_rating_curves.py started at {str(start_time_string)}")
    output_log.append(f"Input directory: {input_folder_path}")

    # ------------------------------------------------------------------------------------------------
    # Create a process pool and run dir_reformat_ras_rc() for each directory in the directory list

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        if verbose == True:
            print()
            print("--------------------------------------------------------------")
            print("Begin iterating through directories with multiprocessor...")
            print()


        for dir in dirlist:
            executor.submit(dir_reformat_ras_rc, dir, input_folder_path, verbose, intermediate_filename, # output_save_folder, 
                            int_output_table_label, int_geospatial_label, int_log_label, 
                            source, location_type, active, input_vdatum, nodataval, midpoints_crs)

    # # Run without multiprocessor
    # for dir in dirlist:
    #     dir_reformat_ras_rc(dir, input_folder_path, verbose, intermediate_filename, # output_save_folder, 
    #                         int_output_table_label, int_geospatial_label, int_log_label, source, 
    #                         location_type, active, input_vdatum, nodataval, midpoints_crs)


    # ------------------------------------------------------------------------------------------------
    # Read in all intermedate files (+ output logs) and combine them 

    if verbose == True:
        print()
        print("--------------------------------------------------------------")
        print("Begin compiling multiprocessor outputs...")
        print()

    # Get list of intermediate files from path
    int_output_table_files = []
    int_geospatial_files = []
    int_logs = []

    
    for dir in dirlist:
        intermediate_filepath = os.path.join(input_folder_path, dir, intermediate_filename)

        # Get output table
        filename = dir + int_output_table_label
        path = os.path.join(intermediate_filepath, filename)
        int_output_table_files.append(path)

        # Get geospatial filename
        filename = dir + int_geospatial_label
        path = os.path.join(intermediate_filepath, filename)
        int_geospatial_files.append(path)  

        # Get log filename
        filename = dir + int_log_label
        path = os.path.join(intermediate_filepath, filename)
        int_logs.append(path)      

    # Read and compile the intermediate rating curve tables
    full_output_table = pd.DataFrame()
    for file_path in int_output_table_files:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            full_output_table = pd.concat([full_output_table, df])
    full_output_table.reset_index(drop=True, inplace=True)

    # Read and compile the intermediate geospatial tables
    full_geospatial = pd.DataFrame()
    for file_path in int_geospatial_files:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            full_geospatial = pd.concat([full_geospatial, df])
    full_geospatial.reset_index(drop=True, inplace=True)

    # Read and compile all logs
    for file_path in int_logs:
        if os.path.exists(file_path):
            with open(file_path) as f:
                lines = f.readlines()
                for line in lines:
                    output_log.append(line)

    # Remove extra linebreaks from log
    output_log = [s.replace('\n', '') for s in output_log]
    output_log.append(" ")

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
    output_geopackage = output_geopackage.set_crs(midpoints_crs)

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
    # Export metadata, print filepaths and save logs (if the verbose and log arguments are selected)

    write_metadata_file(output_save_folder, start_time_string, midpoints_crs)

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

    # Record end time, calculate runtime, and print runtime
    end_time = datetime.datetime.now()
    runtime = end_time - start_time

    print()
    print(f"Process finished. Total runtime: {runtime}") 
    print("-----------------------------------------------------------------------------------------------")

if __name__ == '__main__':

    """
    Sample usage (no linebreak so they can be copied and pasted):

    # Recommended parameters:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i 'C:/ras2fim_data/output_ras2fim_models' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves' -v -l -ov

    # Minimalist run (all defaults used):
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py

    # Maximalist run (all possible arguments):
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i 'C:/ras2fim_data/output_ras2fim_models' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves' -v -l -j 6 -ov -so "ras2fim" -lt "USGS" -ac "True"

    # Only input folders:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i 'C:/ras2fim_data/output_ras2fim_models/subset' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves'

    # Overwrite existing intermediate files:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i 'C:/ras2fim_data/output_ras2fim_models' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves' -v -ov

    # Run with 6 workers:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i 'C:/ras2fim_data/output_ras2fim_models' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves' -v -l -j 6

    # Input the data source, location type, and active information using the -so, -lt, and -ac flags:
    python ./Users/rdp-user/projects/reformat-ras2fim/ras2fim/src/reformat_ras_rating_curve.py -i 'C:/ras2fim_data/output_ras2fim_models' -o 'C:/ras2fim_data/ras2fim_releases/compiled_rating_curves' -v -so "ras2fim" -lt "USGS" -ac "True"

    Notes:
       - Required arguments: None
       - Optional arguments: use the -i tag to specify the input ras2fim filepath (defaults to c:\ras2fim_data\output_ras2fim_models if not specified)
                             use the -o tag to specify the output save folder (defaults to c:\ras2fim_data\ras2fim_releases\compiled_rating_curves if not specified)
                             use the -l tag to save the output log
                             use the -v tag to run in a verbose setting
                             use the -j flag followed by the number to specify number of workers
                             use the -ov flag to overwrite any existing intermediate files
                             use the -so flag to input a value for the "source" output column (i.e. "ras2fim", "ras2fim v2.1") 
                             use the -lt flag to input a value for the "location_type" output column (i.e. "USGS", "IFC")
                             use the -ac flag to input a value for the "active" column ("True" or "False")

    """
        
    # Parse arguments
    parser = argparse.ArgumentParser(description='Iterate through a directory containing ras2fim outputs and '\
                                     'compile a rating curve table and rating curve location point file.')
    parser.add_argument('-i', '--input-path', help='Input directory containing ras2fim outputs to process.', required=False, default='')
    parser.add_argument('-o', '--output-path', help='Output save folder.', required=False, default='')
    parser.add_argument('-l', '--log', help='Option to save output log to output save folder.', required=False, default=False, action='store_true')
    parser.add_argument('-v', '--verbose', help='Option to print more updates and descriptions.', required=False, default=False, action='store_true')
    parser.add_argument('-j', '--num-workers',help='Number of concurrent processes', required=False, default=1, type=int)
    parser.add_argument('-ov', '--overwrite', help='Option to overwrite existing intermediate files in the output save folder.', 
                        required=False, default=False, action='store_true')
    parser.add_argument('-so', '--source', help='Input a value for the "source" output column (i.e. "ras2fim", "ras2fim v2.1").', required=False, default="RAS2FIM")
    parser.add_argument('-lt', '--location-type', help='Input a value for the "location_type" output column (i.e. "USGS", "IFC").', required=False, default="")
    parser.add_argument('-ac', '--active', help='Input a value for the "active" column ("True" or "False")', required=False, default="")

    # Assign variables from arguments
    args = vars(parser.parse_args())
    input_folder_path = args['input_path']
    output_save_folder = args['output_path']
    log = bool(args['log'])
    verbose = bool(args['verbose'])
    num_workers = args['num_workers']
    overwrite = bool(args['overwrite'])
    source = str(args['source'])
    location_type = str(args['location_type'])
    active = str(args['active'])


    # Run main function
    compile_ras_rating_curves(input_folder_path, output_save_folder, log, verbose, num_workers, 
                               overwrite, source, location_type, active)
