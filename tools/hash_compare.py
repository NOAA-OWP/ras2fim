#!/usr/bin/env python3
import argparse
import hashlib
import os
import pathlib
import re

# -------------------------------------------------
def compare_sources(src1, src2, images_only, is_verbose):

    """
    This tool can compare either recursive directories or single files. It will use hashing to 
    validate that the files are exactly identical in contents. 
    
    Note: Watch for files that have embedded or hidden dates like gkpgs. Gpkgs are rarely successful.
    
    When src1 and src2 are single files, the files names need not match but the extensions must match.

    """

    print()
    print("+=================================================================+")
    print("|       Compare single files or all files in given directories    |")
    print()
    print(f"|  - src1 is {src1}")
    print(f"|  - src2 is {src2}")
    print(f"|  - image_only is {images_only}")
    print("+-----------------------------------------------------------------+")        

    is_directories = __validate_inputs(src1, src2)

    if is_directories:
        errors_found = compare_dirs(src1, src2, images_only, is_verbose)

        if (errors_found):
            print()
            print("*** Differences were found, review output above.")
            print()
        else:
            print()
            print("*** No differences were found in any files")
            print()            
    else:
        compare_files(src1, src2, True)

    print("  Comparing complete")
    print()

# -------------------------------------------------
def compare_dirs(src1, src2, image_only, is_verbose):

    if not src1.endswith("\\"):
        src1 += "\\"
    if not src2.endswith("\\"):
        src2 += "\\"

    errors_found = False

    file_ctr = 0

    for (root, dirs, files) in os.walk(src1):

        for file_1_name in files:

            # see if the file exists in the src2 directory
            #file_1_dir = os.path.dirname(file_1)
            #file_1_dir_less_root = file_1_dir.replace(src1, '')
            file_1 = os.path.join(root, file_1_name)

            # we are now building up the path with a replaced root
            file_2 = file_1.replace(src1, src2)

            if image_only is True:
                file_1_ext = pathlib.Path(file_1_name).suffix.lower()
                if file_1_ext != ".tif":
                    # then we can skip the compare
                    continue

            file_ctr += 1

            if os.path.exists(file_2) is False:
                # then we show the file 1, and 2 info either way
                print()
                print("------")
                print(f"  File 1: {file_1}")
                print(f"  File 2: {file_2}")
                print("  -- File 2 does not exist")
                continue

            comp_success = compare_files(file_1, file_2, is_verbose)
            if (comp_success is False):
                # don't change it from False unless one is in error
                errors_found = False

    print()
    print("=============================")
    print(f"  {file_ctr} files reviewed")

    return errors_found

# -------------------------------------------------
def compare_files(src1, src2, is_verbose):

    """
    Takes care of its own output
    """

    # Calling hashfile() function to obtain hashes
    # of the files, and saving the result
    # in a variable
    f1_hash = hashfile(src1)
    f2_hash = hashfile(src2)
    
    # Doing primitive string comparison to
    # check whether the two hashes match or not
    if f1_hash == f2_hash:
        if is_verbose:
            print()
            print("------")
            print(f"  File 1: {src1}")
            print(f"  File 2: {src2}")
            print("       Both files are same")

    else:
        # show details no matter regardless of verbosity
        print()
        print("------")
        print("***  Files are different!")        
        print(f"  File 1: {src1}")
        print(f"  File 2: {src2}")
        print(f"    Hash of File 1: {f1_hash}")
        print(f"    Hash of File 2: {f2_hash}")


# -------------------------------------------------
def hashfile(file):
  
    # A arbitrary (but fixed) buffer
    # size (change accordingly)
    # 65536 = 65536 bytes = 64 kilobytes
    BUF_SIZE = 65536
  
    # Initializing the sha256() method
    sha256 = hashlib.sha256()
  
    # Opening the file provided as
    # the first commandline argument
    with open(file, 'rb') as f:
         
        while True:
             
            # reading data = BUF_SIZE from
            # the file and saving it in a
            # variable
            data = f.read(BUF_SIZE)
  
            # True if eof = 1
            if not data:
                break
      
            # Passing that data to that sh256 hash
            # function (updating the function with
            # that data)
            sha256.update(data)
  
      
    # sha256.hexdigest() hashes all the input
    # data passed to the sha256() via sha256.update()
    # Acts as a finalize method, after which
    # all the input data gets hashed hexdigest()
    # hashes the data, and returns the output
    # in hexadecimal format
    return sha256.hexdigest()
 

# -------------------------------------------------
def __validate_inputs(src1, src2):

    """
    Output: True means they are directorys, False means are files
    """

    if os.path.isdir(src1) is True and os.path.isdir(src2) is False:
        raise ValueError("src1 is a directory but src2 is not. Check to see if src2 exists and is a dir"\
                        " and not a file")

    if os.path.isdir(src1) is False and os.path.isdir(src2) is True:
        raise ValueError("src2 is a directory but src1 is not. Check to see if src1 exists and is a dir"\
                        " and not a file")

    if os.path.isdir(src1) is True and os.path.isdir(src2) is True:
        return True

    # At this point, it is possible that both dir's don't exist, but let's check to see if they are files
    # first

    if os.path.isfile(src1) is True and os.path.isfile(src2) is False:
        raise ValueError("src1 is a file but src2 is either not a file or does not exist.")
    
    if os.path.isfile(src1) is False and os.path.isfile(src2) is True:
        raise ValueError("src2 is a file but src1 is either not a file or does not exist.")
    
    if os.path.isfile(src1) is True and os.path.isfile(src2) is True:
        # now check that the extensions match
        src1_ext = pathlib.Path(src1).suffix
        src2_ext = pathlib.Path(src2).suffix
        if src1_ext.lower() != src2_ext.lower():
            raise ValueError("The two files submitted need the extensions to match or they will obviously fail")
        
        return False
        
    # if we get here something is wrong
    msg = "Oh oh.. something is amiss. It appears that both src1 and src2 are missing or one is directory"
    " and one is a file or something. Please check both input parameters."
    raise ValueError(msg)


# -------------------------------------------------
if __name__ == '__main__':

    # Sample usage for directories
    # python ./tools/hash_compare.py 
    # -src1 C:\ras2fim_data\output_ras2fim\12030105_2276_230913_dev\05_hecras_output 
    # -src2 C:\ras2fim_data\output_ras2fim\12030105_2276_230913\05_hecras_output

    # Sample usage for files (file names need not match, but extensions do)
    # python ./tools/hash_compare.py
    # -src1 C:\ras2fim_data\output_ras2fim\12030105_2276_230913_dev\06_metric\all_rating_curves.csv
    # -src2 C:\ras2fim_data\output_ras2fim\12030105_2276_230913\06_metric\rating_curves_test.csv

    # NOTE: it will show you if a file exists in src1 and is not in src2, but the opposite is not true
    # eg. No warning if extra files in src2.

    parser = argparse.ArgumentParser(description='This tool can be used to compare exact contents of all'
        ' files in a dir recursively or a single file, except gkpgs, base on hash checksums. Use the -i'
        ' flag if only checking images.\nNote. If directories, it will only compare files with exact file'
        ' names. By default verbose output is turned on. To drop to minimum output, add the -v flag'
        '\n\nNote: If using directories, it will assume identical folder structures.')
    parser.add_argument('-src1', help='Directory or file 1 to be compared')
    parser.add_argument('-src2', help='Directory or file 2 to be compared')
    parser.add_argument(
        '-i',
        '--images_only', 
        help='Add this flag if only comparing tif files',
        default=False, action='store_true')

    parser.add_argument(
        "-v",
        "--is_verbose",
        help="OPTIONAL: Adding this flag will give additional tracing output."
        "Default = False (extra output not included)",
        required=False,
        default=False,
        action="store_false",
    )

    args = vars(parser.parse_args())

    compare_sources(**args)
