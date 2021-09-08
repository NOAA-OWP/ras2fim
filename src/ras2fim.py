# Place holder - revise!!!!!
#
# Created by: Andy Carter, PE
# Last revised - 2021.09.07
#
# Main code for ras2fim


from create_shapes_from_hecras import fn_create_shapes_from_hecras
from conflate_hecras_to_nwm import fn_conflate_hecras_to_nwm

import argparse
import os

def fn_run_ras2fim(str_ras_path_arg, str_out_arg, str_crs_arg):
    # print out the user inputed variables
    # TODO - 2021.09.07
    
    # create an output folder
    # TODO - 2021.09.07
    # check if this folder exists and has HEC-RAS files
    
    # Do you want to overwrite the previous output if exists?
    
    if not os.path.exists(str_out_arg):
        os.mkdir(str_out_arg)
    
    # create a folder for the shapefiles from hec-ras
    
    str_shapes_from_hecras_dir = os.path.join(str_out_arg, "shapes_from_hecras") 
    if not os.path.exists(str_shapes_from_hecras_dir):
        os.mkdir(str_shapes_from_hecras_dir)
    
    # run the first script (create_shapes_from_hecras)
    fn_create_shapes_from_hecras(str_ras_path_arg,
                                 str_shapes_from_hecras_dir,
                                 str_crs_arg)
    
    
    # do whatever needed to create folders and determine variables
    str_shapes_from_conflation_dir = os.path.join(str_out_arg, "shapes_from_conflation")
    if not os.path.exists(str_shapes_from_conflation_dir):
        os.mkdir(str_shapes_from_conflation_dir)
    
    # run the second script (conflate_hecras_to_nwm)
    fn_conflate_hecras_to_nwm(str_huc8_arg, 
                              str_shapes_from_hecras_dir, 
                              str_shapes_from_conflation_dir,
                              str_nation_arg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='============ RUN RAS2FIM FOR A HEC_RAS DATASET (HUC8) ============')
    
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
    
    parser.add_argument('-n',
                        dest = "str_nation_arg",
                        help=r'REQUIRED: path to national datasets: Example: E:\X-NWS\X-National_Datasets',
                        required=True,
                        metavar='DIR',
                        type=str)

    args = vars(parser.parse_args())
    
    str_huc8_arg = args['str_huc8_arg']
    str_ras_path_arg = args['str_ras_path_arg']
    str_out_arg = args['str_out_arg']
    str_crs_arg = args['str_crs_arg']
    str_nation_arg = args['str_nation_arg']
    
    fn_run_ras2fim(str_ras_path_arg, str_out_arg, str_crs_arg)