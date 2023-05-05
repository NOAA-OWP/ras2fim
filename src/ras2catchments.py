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
from osgeo import gdal

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

    df = pd.DataFrame(vals)
    df.columns=["comid"]
    df["HydroID"] = vals
    
    gdf = gpd.GeoDataFrame(df,crs=src.crs,geometry=geoms)
    gdf = gdf.dissolve('comid')    
    gdf.to_file(path.replace(".tif",".gpkg"),driver="GPKG",layer="catchments") #,crs=gdf.crs)
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
    nwm_comid_polygons = r"L:\laura_temp\nwm_catchments_clip.shp" # full or full-ish set

    # The following file should be rewired to point to a subset polygon shape in a temp directory
    temp_sub_polygons = r"c:\users\laura.keys\documents\subset_polys.shp" # clipped to relevant

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

            temp_proj_polygons = r"c:\users\laura.keys\documents\temp_sub_proj_polys.shp" # reproj
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
    Input_dir=r"C:\Users\laura.keys\Documents\starter_rem_data\ras2fim_for_ESIP_AWS\sample-dataset\output_iowa\05_hecras_output"
    Output_dir=r"C:\Users\laura.keys\Documents\starter_rem_data\ras2fim_for_ESIP_AWS\sample-dataset\output_iowa\06_ras2rem_output"

    make_catchments(Input_dir,Output_dir)
