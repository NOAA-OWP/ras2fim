import argparse
import datetime
import os
import time
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point, Polygon
from shapely.validation import make_valid

import shared_functions as sf
import shared_variables as sv


# Global Variables
RLOG = sv.R2F_LOG


# -------------------------------------------------
def fn_make_domain_polygons(
    xsections_shp_file_path, output_polygon_dir, model_name_field, model_huc_catalog_path, conflation_qc_path
):
    """
    The function produces polygons representing the domain of each HEC-RAS model using its cross section

    Args:
        - xsections_shp_file_path: Path to the shapefile containing HEC-RAS models cross sections
        - output_polygon_dir: directory to the output GPKG files that include:
            1) all RAS models domain polygons, 2 )a dissolved polygon only for conflated models
        - conflation_qc_path :pth to conflation qc file with info for the RAS models which were
          successfully conflated to NWM reaches
        - model_name_field: column/field name of the input shapefile showing each HEC-RAS model name
        - model_huc_catalog_path : path to the model catalog


    Returns:
        a polygon GPKG for domain of each HEC-RAS model considering cross sections

    Algorithm:
        for each HEC-RAS model (shown in model_name_field of the input shapefile) , the cross sections
        are selected.
        Then, we need 4 lines to build a polygon for model domain using the cross sections.
        The four lines are:
        1) First cross section
        2) Upper edge (by connecting the end points of all cross section)
        3) Last Cross section
        4) lower edge (by connecting the first points of all cross sections )

        To make a valid polygon, need to reverse the order of points of either of cross sections
        and either of edges. Here, we reverse order of points of first cross section and lower edge.
        # Alternatively, we could reverse order of points of last cross section and upper edge.

    """

    # get the version
    changelog_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, 'doc', 'CHANGELOG.md')
    )
    version = sf.get_changelog_version(changelog_path)
    # RLOG.lprint("Version found: " + version)

    RLOG.lprint("")
    RLOG.notice("+++++++ Create polygons for HEC-RAS models domains +++++++")

    RLOG.lprint(
        "  --- (-i) Path to the shapefile containing HEC-RAS models cross sections: "
        + str(xsections_shp_file_path)
    )
    RLOG.lprint("  --- (-o) Directory to write output GPKG files: " + str(output_polygon_dir))
    RLOG.lprint(
        "  --- (-name) column/field name of the input shapefile having unique names for HEC-RAS models: "
        + str(model_name_field)
    )
    RLOG.lprint("  --- (-catalog) path to the model catalog: " + str(model_huc_catalog_path))
    RLOG.lprint("  --- (-conflate) path to the conflation qc file: " + str(conflation_qc_path))
    RLOG.lprint("  --- ras2fim version: " + str(version))
    RLOG.lprint("+-----------------------------------------------------------------+")

    flt_start_domain = time.time()

    Xsections = gpd.read_file(xsections_shp_file_path)

    models_polygons = []
    ras_paths = []
    polygon_status = []
    for ras_path in Xsections[model_name_field].unique():
        RLOG.trace(f"Processing ras_path: {ras_path}")
        this_river_xsections = Xsections[Xsections[model_name_field] == ras_path]
        this_river_xsections.reset_index(inplace=True)

        first_xsection_line = this_river_xsections.iloc[0]["geometry"]
        last_xsection_line = this_river_xsections.iloc[-1]["geometry"]

        upper_edges_points = this_river_xsections["geometry"].apply(lambda line: Point(line.coords[0]))
        lower_edges_points = this_river_xsections["geometry"].apply(lambda line: Point(line.coords[-1]))

        upper_edges_line = LineString(upper_edges_points)

        # To make a valid polygon, need to reverse the order of points of either of cross sections and
        # either of edges below we reverse points of first cross section and lower edge.
        first_xsection_line = LineString(first_xsection_line.coords[::-1])
        lower_edges_line = LineString(reversed(lower_edges_points))

        this_polygon = Polygon(
            first_xsection_line.coords[:]
            + upper_edges_line.coords[1:]
            + last_xsection_line.coords[1:]
            + lower_edges_line.coords[1:]
        )
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

    models_polygons_gdf = gpd.GeoDataFrame()
    models_polygons_gdf[model_name_field] = ras_paths

    # get folder name and geometry file name used to create polygons
    models_polygons_gdf["ras_model_dir"] = models_polygons_gdf.apply(
        lambda row: os.path.basename(os.path.dirname(row[model_name_field])), axis=1
    )
    models_polygons_gdf["ras_geo_file"] = models_polygons_gdf.apply(
        lambda row: os.path.basename(row[model_name_field]), axis=1
    )

    models_polygons_gdf["geometry"] = models_polygons
    models_polygons_gdf["poly_status"] = polygon_status

    # add conflation info
    conflate_qc_df = pd.read_csv(conflation_qc_path)
    models_polygons_gdf["conflated"] = np.where(
        models_polygons_gdf[model_name_field].isin(conflate_qc_df[model_name_field]).values, "yes", "no"
    )

    # also get HUC8 number from qc file name
    models_polygons_gdf["HUC8"] = os.path.basename(conflation_qc_path).split("_stream_qc_fid_xs.csv")[0]

    if model_huc_catalog_path.lower() != "no_catalog":
        # get RRASSLER processing date...only get the first record date, since
        # RASSLER usually takes<24h to process
        catalog_df = pd.read_csv(model_huc_catalog_path)
        rrassler_process_date = catalog_df.loc[0, "date"]
        models_polygons_gdf["rrassler_date"] = rrassler_process_date

    models_polygons_gdf["version"] = version
    models_polygons_gdf.crs = Xsections.crs
    models_polygons_gdf.to_file(os.path.join(output_polygon_dir, 'models_domain.gpkg'), driver="GPKG")

    # also make a single dissolved polygon only for conflated models
    conflated_models = models_polygons_gdf[models_polygons_gdf['conflated'] == 'yes']
    dissolved_conflated_models = conflated_models.dissolve(by='conflated').reset_index()
    columns_to_keep = ['geometry', 'HUC8', 'version']
    if model_huc_catalog_path.lower() != "no_catalog":
        columns_to_keep.append('rrassler_date')
    dissolved_conflated_models[columns_to_keep].to_file(
        os.path.join(output_polygon_dir, 'dissolved_conflated_models.gpkg'), driver="GPKG"
    )

    flt_end_domain = time.time()
    flt_time_pass_domain = (flt_end_domain - flt_start_domain) // 1
    time_pass_domain = datetime.timedelta(seconds=flt_time_pass_domain)
    RLOG.success("Compute Time: " + str(time_pass_domain))


# -------------------------------------------------
if __name__ == "__main__":
    # Sample:
    # python create_model_domain_polygons.py
    #  -i "C:\ras2fim_data\output_ras2fim\12090301_2277_240201\...
    #          01_shapes_from_hecras\cross_section_LN_from_ras.shp"
    #  -o "C:\ras2fim_data\output_ras2fim\12090301_2277_240201\final\models_domain\ cool"
    #  -conflate "C:\ras2fim_data\output_ras2fim\12090301_2277_240201\...
    #        02_csv_shapes_from_conflation\12090301_stream_qc_fid_xs.csv"
    #  -name ras_path
    #  -catalog "C:\ras2fim_data\output_ras2fim\12090301_2277_240201\OWP_ras_models_catalog_12090301.csv"

    parser = argparse.ArgumentParser(description="==== Make polygons for HEC-RAS models domains ===")

    parser.add_argument(
        "-i",
        dest="xsections_shp_file_path",
        help="REQUIRED: path to shapefile containing HEC-RAS models cross sections:"
        r" Example: C:\ras2fim_12090301\01_shapes_from_hecras\cross_section_LN_from_ras.shp",
        required=True,
        metavar="FILE",
        type=str,
    )

    parser.add_argument(
        "-o",
        dest="output_polygon_dir",
        help="REQUIRED: directory to save output GPKG files."
        r" Example: C:\ras2fim_12090301\final\models_domain",
        required=True,
        metavar="DIR",
        type=str,
    )

    parser.add_argument(
        "-conflate",
        dest="conflation_qc_path",
        help='REQUIRED: path to the conflation qc file.'
        r" Example: C:\ras2fim_12090301\02_shapes_from_conflation\***_stream_qc_fid_xs.csv",
        required=True,
        metavar="FILE",
        type=str,
    )

    parser.add_argument(
        "-name",
        dest="model_name_field",
        help="Optional: column/field name of the input shapefile having unique names"
        ' for HEC-RAS models. Default:"ras_path"',
        required=False,
        default="ras_path",
        metavar="STRING",
        type=str,
    )

    parser.add_argument(
        "-catalog",
        dest="model_catalog_path",
        help="Optional: path to the models catalog file."
        r" Example: C:\ras2fim_12090301\OWP_ras_models_catalog_12090301.csv. Default=no_catalog",
        required=False,
        default="no_catalog",
        metavar="FILE",
        type=str,
    )

    args = vars(parser.parse_args())

    xsections_shp_file_path = args["xsections_shp_file_path"]
    output_polygon_dir = args["output_polygon_dir"]
    model_name_field = args["model_name_field"]
    model_catalog_path = args["model_catalog_path"]
    conflation_qc_path = args["conflation_qc_path"]

    log_file_folder = output_polygon_dir
    try:
        # Catch all exceptions through the script if it came
        # from command line.
        # Note.. this code block is only needed here if you are calling from command line.
        # Otherwise, the script calling one of the functions in here is assumed
        # to have setup the logger.

        # creates the log file name as the script name
        script_file_name = os.path.basename(__file__).split('.')[0]
        # Assumes RLOG has been added as a global var.
        RLOG.setup(os.path.join(log_file_folder, script_file_name + ".log"))

        # call main program
        fn_make_domain_polygons(
            xsections_shp_file_path,
            output_polygon_dir,
            model_name_field,
            model_catalog_path,
            conflation_qc_path,
        )

    except Exception:
        RLOG.critical(traceback.format_exc())
