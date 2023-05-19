import shutil
import os
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
from rasterio.merge import merge
import rasterio
import rasterio.mask
import re
import fiona
import fiona.transform
from shapely.geometry import Polygon
from rasterio import features
import glob
import time
import datetime as dt
from osgeo import gdal
import keepachangelog

def get_changelog_version():
    CHANGELOG_PATH = r"C:\Users\laura.keys\Documents\git\ras2fim\doc\CHANGELOG.md"
    changelog = keepachangelog.to_dict(CHANGELOG_PATH)
    return list(changelog.keys())[0]

# This function creates a geopackage of Feature IDs from the raster that
# matches the Depth Grid processing extents from ras2fim Step 5.
def vectorize(path):
    with rasterio.open(path) as src:
        rast = src.read()

    shapes = features.shapes(rast,transform=src.transform)

    geoms = []
    vals = []
    for shp,val in shapes:
      geoms.append(Polygon(shp["coordinates"][0]))
      vals.append(val)

    # Dataframe of Feature ID values to complement the geometries of the polygons derived from raster
    df = pd.DataFrame(vals)
    df.columns=["comid"]
    df["HydroID"] = vals
    
    # We pass in (potentially) many different depths for each Feature ID, so dissolve them together
    gdf = gpd.GeoDataFrame(df,crs=src.crs,geometry=geoms)
    gdf = gdf.dissolve('comid')    

    # XXX
    CATALOG_PATH = r"c:/users/rdp-user/documents/model_catalog.csv"
    CATALOG_PATH = r"c:/users/laura.keys/downloads/model_catalog.csv"
    model_df = pd.read_csv(CATALOG_PATH)
    
    # Join GeoDataFrame to model catalog that has some relevant metadata; keep only relevant fields
    gdf = gdf.merge(model_df,how="left",left_on="comid",right_on="nhdplus_comid")
    columns_to_keep = ["geometry","HydroID","source","last_modified","model_name"]
    gdf = gdf[columns_to_keep]
    gdf['last_modified'] = gdf['last_modified'].fillna(-1) # maybe should be time.time() for current
    try:
        gdf['last_modified'] = [dt.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S") for t in gdf['last_modified']]
    except Exception as e:
        print("Model catalog thing. Moving on.")
        print(e)

    #'''    
    #CATALOG_PATH = r"C:\Users\laura.keys\Downloads\model_footprints.fgb"
    #with fiona.open(CATALOG_PATH) as model_catalog_fgb:
    #    catalog_gdf = gpd.GeoDataFrame(model_catalog_fgb)
    #    catalog_gdf = catalog_gdf.set_crs('epsg:4326')
    #    gdf = gdf.sjoin(catalog_gdf.to_crs(gdf.crs),predicate="intersects",how="left")
    #'''

    #XXX
    SRC_PATH = r"C:\Users\rdp-user\Projects\output_HUC101702040606\rating_curve.csv"# r"c:/users/rdp-user/documents/rating_curve.csv"
    SRC_PATH = r"C:\Users\laura.keys\Documents\starter_rem_data\other_tests\HUC_070600010602\rating_curve.csv"
    src_df = pd.read_csv(SRC_PATH)
    
    # Get min and max stage and discharge for each Feature ID, then create a combined field for stage and flow
    agg_df = src_df.groupby(["feature_id"]).agg({'stage': ['min', 'max'], 'discharge_cms': ['min','max']})
    agg_df['stage_range'] = agg_df['stage']['min'].astype(str) + ' - ' + agg_df['stage']['max'].astype(str)
    agg_df['flow_range'] = agg_df['discharge_cms']['min'].astype(str) + ' - ' \
        + agg_df['discharge_cms']['max'].astype(str)

    # Flatten from MultiIndex and keep relevant columns
    agg_df.columns = ["_".join(c).rstrip('_') for c in agg_df.columns.to_flat_index()]
    agg_df = agg_df[['stage_range','flow_range']]

    # Join SRC data to polygon GeoDataFrame
    gdf = gdf.merge(agg_df,how="left",left_on="HydroID",right_on="feature_id")
    
    # Placeholders for other metadata
    gdf['ras_model_type'] = ['1D'] * len(gdf)
    gdf['model_date'] = ["Unknown"] * len(gdf) # where would we find this in the model data?
    gdf['flow_match_score'] = [0] * len(gdf)
    gdf['terrain_match_score'] = [0] * len(gdf)
    gdf['other_notes'] = [""] * len(gdf)
    gdf['ras2fim_version'] = [get_changelog_version()] * len(gdf)

    # ... then we should have all the metadata we need on a per-catchment basis 
    
    # should we update the src file with some of the fields we left blank?? (or modify inundation code to not read those columns since they're unused..)

    print("** Writing out updated catchments geopackage **")
    gdf.to_file(path.replace(".tif","_meta.gpkg"),driver="GPKG",layer="catchments") #,crs=gdf.crs)
    return

# This function creates a raster and geopackage of Feature IDs that correspond to the extent
# of the Depth Grids (and subsequent REMs that also match the Depth Grids)
def make_catchments(Input_dir,Output_dir):
    start_time = time.time()    
    all_tif_files=list(Path(Input_dir).rglob('*/Depth_Grid/*.tif'))
    rem_values = list(map(lambda var:str(var).split(".tif")[0].split("-")[-1], all_tif_files))
    rem_values=np.unique(rem_values)

    # setting up the clipped shapefile for the catchments
    all_comids = []
    for rem_value in rem_values:
        this_rem_tif_files = [file  for file in all_tif_files if file.name.endswith("-%s.tif"%rem_value)]
        for p in this_rem_tif_files:
            comid = str(p).split('\\')[-1].split('-')[0] # (or match what's between Depth_Grid and -, for
            all_comids.append(int(comid))

    all_comids = list(set(all_comids))
    print(f"Looking through {all_comids}")

    # The following file needs to point to a NWM catchments shapefile that is pulled from S3
    nwm_comid_polygons = r"C:\Users\rdp-user\Documents\nwm_catchments_clip\nwm_catchments_clip.shp" # full or full-ish set

    # The following file should be rewired to point to a subset polygon shape in a temp directory
    temp_sub_polygons = r"c:\users\rdp-user\documents\subset_polys.shp" # clipped to relevant

    with fiona.open(nwm_comid_polygons) as shapefile:
        print(".. getting all relevant shapes")
        with fiona.open(temp_sub_polygons,"w",driver=shapefile.driver, \
                        schema=shapefile.schema,crs=shapefile.crs) as dest:
            for x in shapefile:
                if x["properties"]["FEATUREID"] in all_comids:
                    print(f"...... writing {x['properties']['FEATUREID']}")
                    dest.write(x)

    print("TIME to create subset polygons: " +str(time.time()-start_time))
    for rem_value in rem_values:
        raster_to_mosiac = []
        this_rem_tif_files = [file  for file in all_tif_files if file.name.endswith("-%s.tif"%rem_value)]
        for p in this_rem_tif_files:
            comid = str(p).split('\\')[-1].split('-')[0] # (or match what's between Depth_Grid and -, for an all-system solution)
            print(f"** Addressing {comid}")
            raster = rasterio.open(p)

            temp_proj_polygons = r"c:\users\rdp-user\documents\temp_sub_proj_polys.shp" # reproj
            if not os.path.exists(temp_proj_polygons):
                print("Projecting COMID polygons from NWM Catchments")
                print(raster.crs)
                df = gpd.read_file(temp_sub_polygons)
                print(df.crs)
                df = df.to_crs(raster.crs)
                print(".. Subset shapefile read in and transformed")
                df.to_file(temp_proj_polygons)
                print(".. and written back out")

            with fiona.open(temp_proj_polygons) as subset_shapefile:
                shapes = [feature["geometry"] for feature in subset_shapefile if feature['properties']['FEATUREID']==int(comid)]
            
            # ... apply polygon as mask to initial raster
            masked_raster, transform = rasterio.mask.mask(raster, shapes, nodata=raster.nodata, filled=True, invert=False)

            long_raster_vals = np.array(masked_raster,dtype=np.int32)
            long_raster_vals[long_raster_vals != raster.nodata] = np.int32(comid)
                
            comid_rem_path = os.path.join(Output_dir,"temp_{}_{}.tif".format(rem_value,comid))
            output_meta = raster.meta.copy()
            output_meta.update({"dtype":np.int32})
            with rasterio.open(comid_rem_path, "w", **output_meta) as m:
                m.write(long_raster_vals)
            # append particular comid + depth value path to list
            raster_to_mosiac.append(comid_rem_path)
        # merge all comids at this depth value
        mosaic, output = merge(list(map(rasterio.open,raster_to_mosiac)))

        for p in raster_to_mosiac:
            os.remove(p)
            #pass

        mosaic = mosaic.astype(np.int32)
        output_meta = raster.meta.copy()
        output_meta.update(
            {"driver": "GTiff",
             "height": mosaic.shape[1],
             "width": mosaic.shape[2],
             "transform": output,
             "dtype": rasterio.int32
             }
        )
        # write all comids for this depth value
        comid_rem_path = os.path.join(Output_dir,"{}_comid.tif".format(rem_value))
        with rasterio.open(comid_rem_path, "w", **output_meta) as m:
            m.write(mosaic)
            print(f"++ Writing rem-based comids to {comid_rem_path}")

    # combine all comid files into one single raster and write out
    all_comid_files = list(Path(Output_dir).rglob('*_comid.tif'))
    raster_to_mosiac = []

    for p in all_comid_files:
        raster = rasterio.open(p)
        raster_to_mosiac.append(raster)
    mosaic, output = merge(raster_to_mosiac, method="min")

    output_meta = raster.meta.copy()
    output_meta.update(
        {"driver": "GTiff",
         "height": mosaic.shape[1],
         "width": mosaic.shape[2],
         "transform": output,
         }
    )
    comid_path = os.path.join(Output_dir,"comid.tif")
    with rasterio.open(comid_path, "w", **output_meta) as m:
        m.write(mosaic)
        print(f"** Writing final comid mosaic to {comid_path}")

    # finally delete unnecessary files to clean up
    for raster in raster_to_mosiac:
        raster.close()
    for p in all_comid_files:
        os.remove(p)
        #pass

    print(f"Removing {temp_sub_polygons} and {temp_proj_polygons}")
    temp_sub_poly_files = glob.glob(temp_sub_polygons.replace('.shp','.*'))
    temp_proj_poly_files = glob.glob(temp_proj_polygons.replace('.shp','.*'))
    print(temp_sub_poly_files)
    print(temp_proj_poly_files)
    for f in temp_sub_poly_files:
        os.remove(f)
    for f in temp_proj_poly_files:
        os.remove(f)

    print("TIME to finish raster creation: " +str(time.time()-start_time))
    print("*** Vectorizing COMIDs")
    vectorize(comid_path)
    print("TIME to finish vector creation: " +str(time.time()-start_time))

if __name__=="__main__":
    # delete this environment variable because the updated library we need
    # is included in the rasterio wheel
    try:
        del os.environ["PROJ_LIB"]
    except Exception as e:
        print(e)

    # These paths should be rewired to point to the following:
    # Input directory - results from ras2fim Step 5 that contain depth grids
    # Output directory - desired output directory for feature ID files (currently points to ras2rem outputs)
    Input_dir=r"C:\Users\rdp-user\Projects\HUC101702040606"
    Output_dir=r"C:\Users\rdp-user\Projects\metadata_output_HUC101702040606"

    '''
    if os.path.exists(Output_dir):
        shutil.rmtree(Output_dir)
    os.mkdir(Output_dir)
    
    make_catchments(Input_dir,Output_dir)
    '''
    path = r"C:\Users\rdp-user\Projects\metadata_output_HUC101702040606\comid.tif"
    path = r"C:\Users\laura.keys\Documents\starter_rem_data\other_tests\HUC_070600010602\comid.tif"
    vectorize(path)
