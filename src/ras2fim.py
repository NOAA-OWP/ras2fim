# Place holder - revise!!!!!
#
# Created by: Andy Carter, PE
# Last revised - 2021.09.07
#
# Main code for ras2fim


from create_shapes_from_hecras import fn_create_shapes_from_hecras
from conflate_hecras_to_nwm import fn_conflate_hecras_to_nwm
from get_usgs_dem_from_shape import fn_get_usgs_dem_from_shape
from convert_tif_to_ras_hdf5 import fn_convert_tif_to_ras_hdf5

import argparse
import os

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_run_ras2fim(str_huc8_arg,
                   str_ras_path_arg,
                   str_out_arg,
                   str_crs_arg,
                   b_is_feet,
                   str_nation_arg,
                   str_hec_path):
    
    # print out the user inputed variables
    # TODO - 2021.09.07
    
    # create an output folder
    # TODO - 2021.09.07
    # check if this folder exists and has HEC-RAS files
    
    # Do you want to overwrite the previous output if exists?
    
    if not os.path.exists(str_out_arg):
        os.mkdir(str_out_arg)
    
    # ---- Step 1: create_shapes_from_hecras ----
    # create a folder for the shapefiles from hec-ras
    str_shapes_from_hecras_dir = os.path.join(str_out_arg, "shapes_from_hecras") 
    if not os.path.exists(str_shapes_from_hecras_dir):
        os.mkdir(str_shapes_from_hecras_dir)
    
    # run the first script (create_shapes_from_hecras)
    fn_create_shapes_from_hecras(str_ras_path_arg,
                                 str_shapes_from_hecras_dir,
                                 str_crs_arg)
    # -------------------------------------------

    # ------ Step 2: conflate_hecras_to_nwm -----    
    # do whatever is needed to create folders and determine variables
    str_shapes_from_conflation_dir = os.path.join(str_out_arg, "shapes_from_conflation")
    if not os.path.exists(str_shapes_from_conflation_dir):
        os.mkdir(str_shapes_from_conflation_dir)
    
    # run the second script (conflate_hecras_to_nwm)
    fn_conflate_hecras_to_nwm(str_huc8_arg, 
                              str_shapes_from_hecras_dir, 
                              str_shapes_from_conflation_dir,
                              str_nation_arg)
    # -------------------------------------------

    # ------ Step 3: get_usgs_dem_from_shape ----    
    # create folder

    str_area_shp_name = str_huc8_arg + "_huc_12_ar.shp"
    str_input_path = os.path.join(str_shapes_from_conflation_dir, str_area_shp_name)
    
    # create output folder
    str_terrain_from_usgs_dir = os.path.join(str_out_arg, "terrain_from_usgs")
    if not os.path.exists(str_terrain_from_usgs_dir):
        os.mkdir(str_terrain_from_usgs_dir)
    
    # *** variables set - raster terrain harvesting ***
    int_res = 3
    int_buffer = 300
    int_tile = 1500
    # ***
    
    # field name is from the National watershed boundary dataset (WBD)
    str_field_name = "HUC_12"
    
    # run the third script (conflate_hecras_to_nwm)
    fn_get_usgs_dem_from_shape(str_input_path,
                               str_terrain_from_usgs_dir,
                               int_res,
                               int_buffer,
                               int_tile,
                               b_is_feet,
                               str_field_name)
    # -------------------------------------------

    # ------ Step 4: convert_tif_to_ras_hdf5 ----- 
    # 
    
    # folder of tifs created in third script (get_usgs_dem_from_shape)
    # str_terrain_from_usgs_dir
    
    # create a converted terrain folder
    str_hecras_terrain_dir = os.path.join(str_out_arg, "hecras_terrain")
    if not os.path.exists(str_hecras_terrain_dir):
        os.mkdir(str_hecras_terrain_dir)
        
    str_area_prj_name = str_huc8_arg + "_huc_12_ar.prj"
    str_projection_path = os.path.join(str_shapes_from_conflation_dir, str_area_prj_name)
    

    fn_convert_tif_to_ras_hdf5(str_hec_path,
                               str_terrain_from_usgs_dir,
                               str_hecras_terrain_dir,
                               str_projection_path,
                               b_is_feet)
    # -------------------------------------------
    
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='============ RUN RAS2FIM FOR A HEC-RAS DATASET (HUC8) ============')
    
    parser.add_argument('-w',
                        dest = "str_huc8_arg",
                        help='REQUIRED: HUC-8 watershed that is being evaluated: Example: 10170204',
                        required=True,
                        metavar='STRING',
                        type=str)
    
    parser.add_argument('-i',
                        dest = "str_ras_path_arg",
                        help=r'REQUIRED: path containing the HEC-RAS files: Example C:\HEC\ras_folder',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_out_arg",
                        help=r'REQUIRED: path to write all the outputs: Example C:\HEC\output_folder',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-p',
                        dest = "str_crs_arg",
                        help=r'REQUIRED: projection of HEC-RAS models: Example EPSG:26915',
                        required=True,
                        metavar='STRING',
                        type=str)
    
    parser.add_argument('-v',
                        dest = "b_is_feet",
                        help='REQUIRED: create vertical data in feet: Default=True',
                        required=True,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    parser.add_argument('-n',
                        dest = "str_nation_arg",
                        help=r'REQUIRED: path to national datasets: Example: E:\X-NWS\X-National_Datasets',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-r',
                        dest = "str_hec_path",
                        help=r'REQUIRED: path to HEC-RAS 6.0: Example: "C:\Program Files (x86)\HEC\HEC-RAS\6.0" (wrap in quotes)',
                        required=True,
                        metavar='DIR',
                        type=str)

    args = vars(parser.parse_args())
    
    str_huc8_arg = args['str_huc8_arg']
    str_ras_path_arg = args['str_ras_path_arg']
    str_out_arg = args['str_out_arg']
    str_crs_arg = args['str_crs_arg']
    b_is_feet = args['b_is_feet']
    str_nation_arg = args['str_nation_arg']
    sstr_hec_path = args['str_hec_path']
    
    fn_run_ras2fim(str_huc8_arg, str_ras_path_arg, str_out_arg, str_crs_arg, b_is_feet, str_nation_arg, str_hec_path)