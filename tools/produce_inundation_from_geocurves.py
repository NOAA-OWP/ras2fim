import os
import argparse
import errno
import pandas as pd
import geopandas as gpd

VIZ_PROJECTION ='PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]'


def produce_inundation_from_geocurves(geocurves_dir, job_number, flow_file, output_inundation_dir, overwrite):
    """
    Produce inundation from RAS2FIM geocurves.

    Args:
        geocurves_dir (str): Path to directory containing RAS2FIM geocurve CSVs.
        job_number (int): Number of processes to use.
        flow_file (str): Discharges in CMS as a CSV file. "feature_id" and "discharge" columns MUST be supplied.
        output_inundation_dir (str): Where the output folder will be.
        overwrite (bool): Whether to overwrite files if they already exist.
    """
    
    
    # Check that output directory exists. Notify user that output directory will be created if not.
    if not os.path.exists(output_inundation_dir):
        print("Parent directory for " + os.path.split(output_inundation_dir)[1] + " does not exist. Directory will be created.")
        os.mkdir(output_inundation_dir)
    
    # Check that geocurves_dir exists
    if not os.path.exists(geocurves_dir):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), geocurves_dir)
        
    # Check that flow file exists
    if not os.path.exists(flow_file):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), flow_file)
    
    # Check job numbers and raise error if necessary
    total_cpus_available = os.cpu_count() - 1
    if job_number > total_cpus_available:
        raise ValueError('The number of workers (-w), {}, '\
                          'exceeds your machine\'s available CPU count minus one ({}). '\
                          'Please lower the job_number.'.format(job_number, total_cpus_available))
    
    # Create dictionary of available feature_id geocurve full paths.
    geocurves_list = os.listdir(geocurves_dir)
    geocurve_path_dictionary = {}
    available_feature_id_list = []
    for geocurve in geocurves_list:
        if '.csv' in geocurve:
            feature_id = geocurve.split('_')[0]
            available_feature_id_list.append(feature_id)
            geocurve_path_dictionary.update({feature_id:{'path': os.path.join(geocurves_dir, geocurve)}})
        
    # Open flow_file to detemine feature_ids to process
    flow_file_df = pd.read_csv(flow_file)
    
    # Loop through feature_ids and concatenate into a single dataframe
    iteration = 0
    series_list = []
    polygon_list = []
    df_list = []
    for feature_id in flow_file_df['feature_id']:
        if str(feature_id) not in available_feature_id_list:
            continue
        discharge_cms = flow_file_df.loc[flow_file_df['feature_id'] == feature_id, 'discharge'].values[0]
        try:
            geocurve_file_path = geocurve_path_dictionary[str(feature_id)]['path']
        except KeyError:
            continue
        geocurve_df = pd.read_csv(geocurve_file_path)


        # Use interpolation to find the row in geocurve_df that corresponds to the discharge_value
        row_idx = geocurve_df['discharge_cms'].sub(discharge_cms).abs().idxmin()
        subset_geocurve = geocurve_df.iloc[row_idx]
        polygon_path = subset_geocurve['path']
        if os.path.exists(polygon_path):
            polygon_list.append(polygon_path)
            
#        print(type(subset_geocurve))
#        if iteration == 0:
#            geocurve_df_to_save = subset_geocurve
#        else:
#            geocurve_df_to_save = pd.DataFrame(geocurve_df_to_save).join(subset_geocurve)
            
        series_list.append(subset_geocurve)
        df_list.append(subset_geocurve.to_frame())
        iteration += 1 
        
    series_df = pd.concat(series_list, axis=1).T
    
#    for index, row in series_df.iterrows():
#        print(row)
    
#    print(series_df)
    
#    geocurve_df_to_save = pd.merge(series_list)
#    print(geocurve_df_to_save)
    series_df.to_csv(os.path.join(output_inundation_dir, "inundation_polygons_final_from_csv4.csv"))
    
    print(type(series_df))
        
    geocurve_gdf = gpd.GeoDataFrame(series_df, crs='EPSG:5070').set_geometry(col='geometry', inplace=True)
    output_csv_gpkg_path = os.path.join(output_inundation_dir, "inundation_polygons_final_from_csv.gpkg")
    
    geocurve_gdf['valid'] = geocurve_gdf.is_valid  # Add geometry validity column
    
    for index, row in geocurve_gdf.iterrows():
        print(row)    
        
    geocurve_gdf = geocurve_gdf[geocurve_gdf['valid'] == True]
    
    geocurve_gdf.to_file(output_csv_gpkg_path,driver='GPKG')
        
    gdf = gpd.read_file(polygon_list[0])
    for polygon_file in polygon_list:
        new_gdf = gpd.read_file(polygon_file)
        gdf = gpd.pd.concat([gdf, new_gdf])
    
    # Now you have the GeoDataFrame `gdf` with polygons, and you can write it to a GeoPackage
    output_gpkg_path = os.path.join(output_inundation_dir, "inundation_polygons_final.gpkg")
    print(output_gpkg_path)
    gdf.to_file(output_gpkg_path, driver='GPKG')
    

if __name__ == '__main__':
    
    # Parse arguments
    parser = argparse.ArgumentParser(description = 'Produce Inundation from RAS2FIM geocurves.')
    parser.add_argument('-g', '--geocurves_dir', help='Path to directory containing RAS2FIM geocurve CSVs.',required=True)
    parser.add_argument('-j','--job_number',help='Number of processes to use', required=False, default=1, type=int)
    parser.add_argument('-f','--flow_file',help='Discharges in CMS as CSV file. "feature_id" and "discharge" columns MUST be supplied.',required=True)
    parser.add_argument('-t', '--output_inundation_dir', help = 'Target: Where the output folder will be', required = False)
    parser.add_argument('-o','--overwrite', help='Overwrite files', required=False, action="store_true")
        
    args = vars(parser.parse_args())
    
    produce_inundation_from_geocurves(**args)