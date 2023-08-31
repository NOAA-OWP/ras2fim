import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import LineString, Polygon
from shapely.validation import make_valid
import datetime
import time
import argparse
import pandas as pd
import os
import numpy as np


def fn_make_domain_polygons(xsections_shp_file_path, polygons_output_file_path, model_name_field,model_huc_catalog_path,
                            conflation_qc_path):
    '''
    The function produces polygons representing the domain of each HEC-RAS model using its cross section

    Args:
        - xsections_shp_file_path: Path to the shapefile containing HEC-RAS models cross sections
        - polygons_output_file_path: path to the output GPKG file containing models domain polygons
        - model_name_field: column/field name of the input shapefile showing each HEC-RAS model name
        - model_huc_catalog_path : path to the model catalog
        - conflation_qc_path :pth to conflation qc file with info for the RAS models which were successfully conflated to NWM reaches

    Returns:
        a polygon GPKG for domain of each HEC-RAS model considering cross sections

    Algorithm:
        for each HEC-RAS model (shown in model_name_field of the input shapefile) , the cross sections are selected.
        Then, we need 4 lines to build a polygon for model domain using the cross sections.
        The four lines are: 1)First cross section 2) Upper edge (by connecting the end points of all cross section)
        3) Last Cross section 4) lower edge (by connecting the first points of all cross sections )

        To make a valid polygon, need to reverse the order of points of either of cross sections and either of edges.
        Here, we reverse order of points of first cross section and lower edge. # Alternatively, we could reverse
        order of points of last cross section and upper edge

    '''

    print("  --- (-i) Path to the shapefile containing HEC-RAS models cross sections: " + str(xsections_shp_file_path))
    print("  --- (-o) path to the GPKG output file: " + str(polygons_output_file_path))
    print("  --- (-name) column/field name of the input shapefile having unique names for HEC-RAS models: " + str(model_name_field))
    print("  --- (-catalog) path to the model catalog: " + str(model_huc_catalog_path))
    print("  --- (-conflate) path to the conflation qc file: " + str(conflation_qc_path))
    print("+-----------------------------------------------------------------+")


    flt_start_domain = time.time()

    Xsections=gpd.read_file(xsections_shp_file_path)

    models_polygons=[]
    ras_paths=[]
    polygon_status=[]
    for ras_path in Xsections[model_name_field].unique():
        this_river_xsections=Xsections[Xsections[model_name_field]== ras_path]
        this_river_xsections.reset_index(inplace=True)

        first_xsection_line=this_river_xsections.iloc[0]['geometry']
        last_xsection_line=this_river_xsections.iloc[-1]['geometry']

        upper_edges_points= this_river_xsections['geometry'].apply(lambda line: Point(line.coords[0]))
        lower_edges_points= this_river_xsections['geometry'].apply(lambda line: Point(line.coords[-1]))

        upper_edges_line = LineString(upper_edges_points)

        #to make a valid polygon, need to reverse the order of points of either of cross sections and either of edges
        # below we reverse points of first cross section and lower edge.
        first_xsection_line = LineString(first_xsection_line.coords[::-1])
        lower_edges_line = LineString(reversed(lower_edges_points))

        this_polygon = Polygon(first_xsection_line.coords[:] + upper_edges_line.coords[1:] + last_xsection_line.coords[1:] + lower_edges_line.coords[1:])
        models_polygons.append(this_polygon)

        ras_paths.append(ras_path)
        
        if this_polygon.is_valid:
            polygon_status.append("Valid")
        else:
            validated_polygon = make_valid(this_polygon)
            if validated_polygon.is_valid:
                polygon_status.append("Validated")
            else:
                polygon_status.append("Invalid")

    models_polygons_gdf=gpd.GeoDataFrame()
    models_polygons_gdf[model_name_field]=ras_paths

    #get folder name and geometry file name used to create polygons
    models_polygons_gdf['ras_model_dir']=models_polygons_gdf.apply(lambda row: os.path.basename(os.path.dirname(row[model_name_field])), axis=1)
    models_polygons_gdf['ras_geo_file']=models_polygons_gdf.apply(lambda row: os.path.basename(row[model_name_field]), axis=1)

    models_polygons_gdf["geometry"]=models_polygons
    models_polygons_gdf["poly_status"]=polygon_status

    if model_huc_catalog_path.lower() != 'no_catalog':
        #get RRASSLER processing date...only get the first record date, since RASSLER usually takes<24h to process
        catalog_df=pd.read_csv(model_huc_catalog_path)
        rrassler_process_date=catalog_df.loc[0,'date']
        models_polygons_gdf['rrassler_date']=rrassler_process_date

    if conflation_qc_path.lower() != 'no_qc':
        conflate_qc_df=pd.read_csv(conflation_qc_path)
        models_polygons_gdf['conflated']=np.where(models_polygons_gdf[model_name_field].isin(conflate_qc_df[model_name_field]).values==True,"yes","no")

        #also add HUC8 number
        models_polygons_gdf["HUC8"]=os.path.basename(conflation_qc_path).split("_stream_qc.csv")[0]

    models_polygons_gdf.crs=Xsections.crs
    models_polygons_gdf.to_file(polygons_output_file_path,driver="GPKG")

    flt_end_domain = time.time()
    flt_time_pass_domain = (flt_end_domain - flt_start_domain) // 1
    time_pass_domain = datetime.timedelta(seconds=flt_time_pass_domain)
    print('Compute Time: ' + str(time_pass_domain))


if __name__=="__main__":

    parser = argparse.ArgumentParser(description='==== Make polygons for HEC-RAS models domains ===')

    parser.add_argument('-i',
                        dest = "xsections_shp_file_path",
                        help=r'REQUIRED: Path to shapefile containing HEC-RAS models cross sections: Example: C:\ras2fim_12090301\01_shapes_from_hecras\cross_section_LN_from_ras.shp',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "polygons_output_file_path",
                        help=r'REQUIRED: path to the output GPKG file',
                        required=True,
                        metavar='DIR',
                        type=str)


    parser.add_argument('-name',
                        dest = "model_name_field",
                        help=r'Optional: column/field name of the input shapefile having unique names for HEC-RAS models. Default:"ras_path"',
                        required=False,
                        default='ras_path',
                        metavar='STRING',
                        type=str)

    parser.add_argument('-catalog',
                    dest = "model_catalog_path",
                    help=r'Optional: path to the model catalog. Default=no_catalog',
                    required=False,
                    default='no_catalog',
                    metavar='STRING',
                    type=str)

    parser.add_argument('-conflate',
                dest = "conflation_qc_path",
                help=r'Optional: path to the conflation qc file in "02_shapes_from_conflation" folder. Default=no_qc',
                required=False,
                default='no_qc',
                metavar='STRING',
                type=str)

    args = vars(parser.parse_args())

    xsections_shp_file_path = args['xsections_shp_file_path']
    polygons_output_file_path = args['polygons_output_file_path']
    model_name_field=args['model_name_field']
    model_catalog_path=args['model_catalog_path']
    conflation_qc_path=args['conflation_qc_path']

    print()
    print ("+++++++ Create polygons for HEC-RAS models domains +++++++" )

    fn_make_domain_polygons(xsections_shp_file_path, polygons_output_file_path,model_name_field,model_catalog_path,conflation_qc_path)


