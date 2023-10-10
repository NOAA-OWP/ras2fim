#!/usr/bin/env python3

import src.reformat_ras_rating_curve as reformat_ras

input_folder_path = 'C:\Users\rdp-user\projects\compile_ras_rating_curves_wrapper\output_ras2fim'
output_save_folder = 'C:\Users\rdp-user\projects\compile_ras_rating_curves_wrapper\compiled_outputs'


save_logs = True
verbose = True
num_workers = 1
source = 'ras2fim'
location_type = ''
active = ''

# Compiles the rating curve and points from each directory
reformat_ras.compile_ras_rating_curves(
    input_folder_path, output_save_folder, save_logs, verbose, num_workers, source, location_type, active
)



