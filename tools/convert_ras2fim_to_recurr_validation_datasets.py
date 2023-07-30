#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio as rio
import argparse
from os import listdir, remove
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.warp import reproject
from shutil import rmtree
from concurrent.futures import ProcessPoolExecutor

import src.shared_variables as sv

PREP_PROJECTION = 'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.2572221010042,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'

def generate_inundation_raster(depth_grid,extent_gridname):
        
    # Read in depth grid and convert to boolean
    depth_grid = rio.open(depth_grid)
    depth_data = depth_grid.read(1)
    no_data = depth_grid.nodata 

    # Convert depths to boolean 
    extent_grid = np.where(depth_data == int(no_data), -9999.0, depth_data.astype(int))
    extent_grid = np.where(extent_grid >= 0, 1, 0) 
    
    depth_profile = depth_grid.profile.copy()
    depth_profile.update({'nodata': -9999.0, 'dtype': 'int32'})
    
    with rio.open(extent_gridname, "w", **depth_profile) as dest:
        dest.write(extent_grid, indexes=1)    


def reproject_raster(extent_gridname,reproj_gridname,proj):

    # Read in raster and reproject
    extent_grid = rio.open(extent_gridname)
    reproj_data, reproj_trans = reproject(source=rio.band(extent_grid, 1),dst_crs=proj)
    grid_profile = extent_grid.profile.copy()
    
    grid_profile.update(driver="GTiff",
                        height=reproj_data.shape[1],
                        width=reproj_data.shape[2],
                        transform= reproj_trans,
                        tiled=True,
                        nodata=-9999.0,
                        blockxsize=512, 
                        blockysize=512,
                        dtype='int32',
                        crs=proj,
                        compress='lzw',
                        interleave='band')
    
    with rio.open(reproj_gridname, "w", **grid_profile) as dest:
            dest.write(reproj_data)
    

def write_rating_curves(ras_rc_rec_dir,aggregate_rc,reccur_flow_rc):

    ras_rc_rec_dir.mkdir(parents=True,exist_ok=True)
    
    if not aggregate_rc.is_file():
        reccur_flow_rc.to_csv(aggregate_rc,index=False)
    else:
        reccur_flow_rc.to_csv(aggregate_rc,index=False, mode='a',header=False)


def merge_rasters(ras_out,ras_list,proj):

    raster_to_mosiac = []
    for p in ras_list:
        raster = rio.open(p)
        raster_to_mosiac.append(raster)
    
    profile = raster.profile
    del raster
    
    # Merge rasters in HUC taking the max value where grids overlap
    mosaic, comp_trans = merge(raster_to_mosiac, method='max',res=10,precision=50,nodata=0)

    profile.update(driver="GTiff",
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=comp_trans,
            tiled=True,
            nodata=-9999.0,
            blockxsize=512, 
            blockysize=512,
            dtype='int32',
            crs=proj,
            compress='lzw')

    with rio.open(ras_out, "w", **profile) as m:
        m.write(mosaic) 


def mask_rasters(ras_out,wbd_geom,proj):
    
    # Read in merged raster
    raster = rio.open(ras_out)
    profile = raster.profile
    
    # Create mask using huc8 boundary and set values outside bounds to nodata
    out_image, out_transform = mask(raster,wbd_geom,nodata=2)
    
    profile.update(driver="GTiff",
            height=out_image.shape[1],
            width=out_image.shape[2],
            transform=out_transform,
            tiled=True,
            nodata=2,
            blockxsize=512, 
            blockysize=512,
            dtype='int32',
            crs=proj,
            compress='lzw')
    
    with rio.open(ras_out, "w", **profile) as dest:
        dest.write(out_image)


def extract_ras(args, huc):
    
    recurrence_dir           = args[0]
    ras_reorg_dir            = args[1]
    output_dir               = args[2]
    missing_flows_dir        = args[3]
    wbd_layer                = args[4]
    
    # Set paths
    missing_flows_logfile = missing_flows_dir / f"missing_flows_{huc}.csv"

    # Create table to log missing datasets
    missing_flows = pd.DataFrame({'huc': pd.Series(dtype='str'),
                            'feature_id': pd.Series(dtype='str'),
                            'recurr_interval': pd.Series(dtype='str'),
                            'ras_rc_max_depth': pd.Series(dtype='int'),
                            'interp_depth': pd.Series(dtype='float'),
                            'ras_rc_min_flow': pd.Series(dtype='float'),
                            'ras_rc_max_flow': pd.Series(dtype='float'),
                            'NWM_recurr_flow': pd.Series(dtype='float'),
                            'category': pd.Series(dtype='str')})

    
    # Run for each NWM recurrence flow
    for flow_file in listdir(recurrence_dir):
        
        # Read NWM recurrence flows for all feature ids at interval
        nwm_rc_flows = pd.read_csv(recurrence_dir / flow_file)
        
        # Path to HEC-RAS rating curves
        ras_rc_dir = ras_reorg_dir / huc / 'Rating_Curves'
        
        # Get list of HEC-RAS feature id rating curves in each HUC
        feature_ids_rc = listdir(ras_rc_dir)
        
        # Retrieve recurrence interval from file path 
        interval = flow_file.replace('nwm21_17C_recurr_','')
        interval = interval.replace('_0_cms.csv','yr')
        
        # Create output dir for extent grids
        out_grid_dir = output_dir /'extent_grids' / huc /  interval
        out_grid_dir.mkdir(parents=True,exist_ok=True)
        
        # Create output dir for reprojected extent grids
        reproj_out_grid_dir = output_dir/ 'extent_grids_reproj' / huc  / interval
        reproj_out_grid_dir.mkdir(parents=True,exist_ok=True)
        
        # Get NWM recurrence flows for feature ids in each huc
        for rc_file in feature_ids_rc:
            
            # Convert file names to list of feature ids
            feature_id = rc_file.replace("_rating_curve.csv","")
            
            # Get recurrence flows to interpolate depth
            val_discharge = nwm_rc_flows.loc[nwm_rc_flows.feature_id==int(feature_id)]
            ras_rc = pd.read_csv(ras_rc_dir / rc_file)
            
            # Get max depth for selecting depth grid
            rc_max_depth = int(ras_rc['stage_m'].max() * 10)
            
            # Interpolate flow at each interval flow
            interp_depth = np.interp(val_discharge['discharge'], ras_rc['discharge_cms'], ras_rc['stage_m'], left = np.nan, right = np.nan) 
            interp_depth = interp_depth.item()
            
            if not pd.isna(interp_depth):
                        
                # Query rating curve: must be >= interpolated depth within 1 ft
                # Generate extent grids
                benchmark = min(ras_rc.loc[ras_rc['stage_m']>=interp_depth]['stage_m'])
                
                if (not benchmark == None) and (benchmark - interp_depth <= 0.3048): # see https://github.com/NOAA-OWP/cahaba/wiki/Evaluating-HAND-Performance, Fig A.5
                
                    closest_rc_max_depth = int(benchmark*10)
        
                    closest_depth_gridname = f'{feature_id}-{closest_rc_max_depth}.tif'
                    depth_grid = ras_reorg_dir / huc / 'Depth_Grid' / closest_depth_gridname
                    
                    if depth_grid.exists():
                        # Create extent grid
                        extent_gridname = out_grid_dir / f'{feature_id}-{closest_rc_max_depth}.tif'
                        generate_inundation_raster(depth_grid,extent_gridname)
                        
                        # Reproject grid
                        reproj_gridname = reproj_out_grid_dir / f'{feature_id}-{closest_rc_max_depth}.tif'
                        reproject_raster(extent_gridname,reproj_gridname,PREP_PROJECTION)
                    else:
                        print(f"Missing depth grid {depth_grid} for feature id {feature_id} in HUC {huc}. Interpolated depth: {interp_depth}.")
                                        
                    # Append interpolated point to validation rating curve
                    reccur_flow_rc = pd.DataFrame()
                    interp_df = {'feature_id': feature_id, 'discharge': val_discharge['discharge'].item(),'avg_depth_m': np.round(interp_depth,1)}
                    reccur_flow_rc = reccur_flow_rc.append(interp_df, ignore_index = True)
                    
                    # Write to validation dataset
                    ras_rc_rec_dir = output_dir / huc / interval
                    aggregate_rc = ras_rc_rec_dir / f'ras2fim_huc_{huc}_flows_{interval}.csv'
                    write_rating_curves(ras_rc_rec_dir,aggregate_rc,reccur_flow_rc)
                
                else:
                    print(f"No {interval} depth grid exists within search window for feature id {feature_id} in HUC {huc}. Interpolated depth: {interp_depth}.")
                    
                    missing_flows = missing_flows.append({'huc': huc,
                                                'feature_id': feature_id,
                                                'recurr_interval': interval,
                                                'ras_rc_max_depth': rc_max_depth,
                                                'interp_depth': interp_depth,
                                                'ras_rc_min_flow': ras_rc['discharge_cms'].min(),
                                                'ras_rc_max_flow': ras_rc['discharge_cms'].max(),
                                                'NWM_recurr_flow': val_discharge['discharge'].item(),
                                                'category': 'missing in search window'}
                                            , ignore_index = True)
                                
            else:
                print(f"{interval} recurrence flow for feature id {feature_id} in HUC {huc} outside of ras2fim rating curve bounds")
                
                missing_flows = missing_flows.append({'huc': huc,
                                                'feature_id': feature_id,
                                                'recurr_interval': interval,
                                                'ras_rc_max_depth': rc_max_depth,
                                                'interp_depth': None,
                                                'ras_rc_min_flow': ras_rc['discharge_cms'].min(),
                                                'ras_rc_max_flow': ras_rc['discharge_cms'].max(),
                                                'NWM_recurr_flow': val_discharge['discharge'].item(),
                                                'category': 'out of range'}
                                            , ignore_index = True)
                continue
            
            del ras_rc
        
        del nwm_rc_flows
    
    # Merge extent rasters
    reproj_grid_dir = output_dir /'extent_grids_reproj' / huc
    
    # Get feature ids with missing grids
    exclude_feature_ids = list(set(missing_flows.feature_id))
    
    # Read in huc8 boundary geometry
    wbd = gpd.read_file(wbd_layer,layer='WBDHU8')
    wbd = wbd.to_crs(PREP_PROJECTION)
    wbd = wbd.loc[wbd.HUC8 == huc,'geometry'].to_list()
    
    # For each recurrence interval
    for interval in listdir(reproj_grid_dir):
        
        ras_rc_rec_dir = output_dir / huc / interval
        ras_out = ras_rc_rec_dir /f"ras2fim_huc_{huc}_extent_{interval}.tif"
        
        # Get list of extent grid paths
        reproj_out_grid_dir = reproj_grid_dir  / interval
        ras_list = [str(reproj_out_grid_dir / r) for r in listdir(reproj_out_grid_dir) if ".aux.xml" not in r]
        
        # Remove any grids that do not have extents for every recurrence interval
        for id in exclude_feature_ids:
            ras_list = [r for r in ras_list if id not in r]
    
        if len(ras_list) > 0:
            
            # Merge all grids within huc
            merge_rasters(ras_out,ras_list,PREP_PROJECTION) 
            
            # Convert cells outside of HUC8 boundaries to nodata
            mask_rasters(ras_out,wbd,PREP_PROJECTION)
                   
    if missing_flows_logfile.exists():
        remove(missing_flows_logfile)
    else:
        missing_flows.to_csv(missing_flows_logfile,index=False)
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Convert depth grids to inundation extents')
    parser.add_argument('-val_dir','--validation-dir', help='Validation data directory', required=True,type=str)
    parser.add_argument('-ras_dir','--ras-dir', help='HEC-RAS model directory', required=True,type=str)
    parser.add_argument('-j','--jobs', help='number of jobs', required=False,type=int,default=1)

    args = vars(parser.parse_args())
    recurrence_dir = Path(args['validation_dir'])
    ras_model_dir = Path(args['ras_dir'])
    num_workers= args['jobs']
    
    # Set paths
    ras_reorg_dir = ras_model_dir / 'ras_reorg'
    huc_list = listdir(ras_reorg_dir)
    output_dir = ras_model_dir / 'validation_data_ras2fim'
    missing_flows_dir = ras_model_dir / 'missing_flows'
    missing_flows_dir.mkdir(parents=True,exist_ok=True)
    wbd_layer = os.path.join(ras_model_dir, sv.INPUT_WBD_NATIONAL_FILE)

    print (f"Creating extent rasters for {len(huc_list)} HUCs")
    extent_grid_args = (recurrence_dir,ras_reorg_dir,output_dir,missing_flows_dir,wbd_layer)

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Find nearest depth grid and convert to inundation extent for each recurrence interval
        convert_depth_grids = [executor.submit(extract_ras, extent_grid_args, str(huc)) for huc in huc_list]
        
    # Collect tables for missing grids and append to single table
    all_missing_flows_logfile = ras_model_dir / 'missing_flows.csv'
    if all_missing_flows_logfile.is_file():
            remove(all_missing_flows_logfile)
    
    for csv in listdir(missing_flows_dir):
        
        missing_flows = pd.read_csv(missing_flows_dir / csv,dtype=str)
        
        if not all_missing_flows_logfile.is_file():
            missing_flows.to_csv(all_missing_flows_logfile,index=False)
        else:
            missing_flows.to_csv(all_missing_flows_logfile,index=False,mode='a',header=False)
    
    # Read final missing flows file and remove feature_ids from rating curves
    missing_flows = pd.read_csv(all_missing_flows_logfile,dtype=str)
    for huc in missing_flows.huc.unique():
        
        missing_flows_sub = missing_flows.loc[missing_flows.huc == huc]
        huc_dir = output_dir / str(huc)
        
        for interval in listdir(huc_dir):
            
            rc_path = huc_dir / interval / f"ras2fim_huc_{huc}_flows_{interval}.csv"
            rc = pd.read_csv(rc_path,dtype=str)
            
            # Get missing feature_ids and remove from current rating curve
            missing_feature_ids = missing_flows_sub.feature_id.drop_duplicates().to_list()
            rc = rc[~rc.feature_id.isin(missing_feature_ids)]
            
            # Remove old flow file
            remove(rc_path)
            
            if not rc.empty:
                # Replace flow file with remaining feature_ids
                rc.to_csv(rc_path,index=False)
            else:
                # Remove huc with incomplete flows for every recurrence interval
                rmtree(huc_dir)
                break
            
    # Remove dir with partial tables
    if all_missing_flows_logfile.is_file():
            rmtree(missing_flows_dir)
    
    # Remove intermediate grids
    extent_grids = output_dir / 'extent_grids'
    if extent_grids.is_dir():
            rmtree(extent_grids)
            
    extent_grids_reproj = output_dir / 'extent_grids_reproj'
    if extent_grids_reproj.is_dir():
            rmtree(extent_grids_reproj)
