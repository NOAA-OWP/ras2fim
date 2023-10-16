#!/usr/bin/env python3

import pandas as pd
import geopandas as gpd
from datetime import date

# If needed, download the most recent ras status CSV from the 'ras2fim cloud data migration' Google Sheet

# Add data filepaths
ras_status_filepath = 'ras_availability_map_database\_ras2fim cloud data migration - OWP HEC-RAS preprocessing.csv'
huc8_filepath = 'ras_availability_map_database\huc8_conus\HUC8_US.shp'
ras_availability_filepath = r'ras_availability_map_database\code_outputs\ras_availability.gpkg' # Save location for output geopackage

# --- 

print()
print('Generating ras availability geopackage...') ## debug

# Read in the availability CSV and HUC8 geopackage
ras_status_table = pd.read_csv(ras_status_filepath)
huc8_geom = gpd.read_file(huc8_filepath)

# Rename the 'Status' and 'HUC8' columns for easier merging
stage_columns = [col for col in ras_status_table.columns if 'Stage' in col]
ras_status_table['Status'] = ras_status_table[stage_columns] # Status labels: 'pushed' 'downloading' 'downloadable' nan 'S3' 'TOADD'

huc8_columns = [col for col in ras_status_table.columns if 'HUC8' in col]
ras_status_table['HUC8'] = ras_status_table[huc8_columns] 

# Get the 'HUC8' column in both dataframes to be the same data type for easier merging
ras_status_table['HUC8'] = pd.to_numeric(ras_status_table['HUC8'], errors='coerce', downcast='integer')
huc8_geom['HUC8'] = pd.to_numeric(huc8_geom['HUC8'], errors='coerce', downcast='integer')

# Join the 'Status' column to the HUC8 geometries using the 'HUC8' column
huc8_status_geom = huc8_geom.merge(ras_status_table[['HUC8', 'Status']], on='HUC8', how='left')

# Create a new gpkg that excludes HUCs that have no ras_status
huc8_status_cleaned_geom = huc8_status_geom.dropna(axis=0, subset=['Status'])

# Append export date column 
today = date.today()
huc8_status_cleaned_geom['Status_updated_as_of'] = str(today)

# Save new geopackage (maybe with the current date in the metadata?)
huc8_status_cleaned_geom.to_file(ras_availability_filepath, driver='GPKG')
print('Saved new geopackage.')
print()
