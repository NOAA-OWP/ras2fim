All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.


## v1.11.0 - 2023-07-04 - [PR#92](https://github.com/NOAA-OWP/ras2fim/pull/92)

This PR removes the V flag and added a new flag to control the spatial resolution of 'simplified' rasters. The 'model unit', which is meter or feet, is now identified by the program either using the -p projection entry (using EPSG code or providing a shp/prj file) or by processing the HEC-RAS prj files. The z unit of the given DEM must always be in meter. 

### Additions
- A new `src/errors.py` to hold the customized exceptions definitions for the entire program.
### Changes

- `src/clip_dem_from_shape.py`  : Changed the default buffer value from 700 to 300 to be consistent with the hard-coded value used in ras2fim.py
- `src/convert_tif_to_ras_hdf5.py` : Determined 'model_unit' using 'model_unit_from_crs()' function of shared_functions.py
- `src/create_fim_rasters.py`: Removed 'flt_out_resolution' argument which were not being used anywhere in the code.
- `src/get_usgs_dem_from_shape.py`: Determined 'model_unit' using 'model_unit_from_crs()' function of shared_functions.py
- `src/ras2fim.py` : Removed 'vert_unit,' argument and moved all codes to check the vertical unit of the code into shared_functions.py. Also, determined the model unit using 1) -p entry and 2) reading all HEC-RAS models prj files-- Used 'confirm_models_unit() function of shared_functions.py
- `src/shared_functions.py` : Added three functions to determine model units throughout the entire program 1) confirm_models_unit(), 2) model_unit_from_crs(), 3) model_unit_from_ras_prj()
- `src/simplify_fim_rasters.py` :  Changed the default value of output resolution from 3m to 10m
- `src/worker_fim_rasters.py` : Determined model_unit using 'model_unit_from_ras_prj()' function of shared_functions.py

<br/><br/>

## v1.10.0 - 2023-06-26 - [PR#84](https://github.com/NOAA-OWP/ras2fim/pull/84)

This PR covers a couple of minor fixes:
1) [Issue 72](https://github.com/NOAA-OWP/ras2fim/issues/72) - Change output ras2fim folder name: It was named `output_ras2fim_models`, now being named `output_ras2fim`.
2) [Issue 83](https://github.com/NOAA-OWP/ras2fim/issues/83) - A change to correct a re-occurring rasterio package issue.
3) There was also a minor bug fix in column names in the ras2catchments.py, now fixed.

### Changes  
- `src`
    - `ras2catchments.py` - Fix to match a column name for our inputs folder nwm_catchments.gkpg. Also small text adjustments for the adjusted `output_ras2fim` folder name.  Also removed some code which loaded a depth grid TIF to figure out it's CRS and apply that to output files from catchments. Now, it just loads the CRS value from the shared_variables.py file.
    - `ras2fim.py` - It was hardcoding `EPSG:3857` which is passed to other files and resulted in the CRS of the output depth grid TIFs. This is value is now moved up to `shared_variables.py` as a constant so other code can use it and be consistent (such as catchments).  Also changed the timing of when the new HUC output folder is created. Note: The "step" system is WIP and being removed.
    - `reformat_ras_rating_curve.py`: Text changes for the new output folder path.
    - `shared_variables.py`: Changed output folder name default, plus added the new default CRS constant.


<br/><br/>

## v1.9.0 - 2023-06-15 - [PR#64](https://github.com/NOAA-OWP/ras2fim/pull/64)

In a recent release, the ras2catchment product feature was included but was not completed and now is.

The ras2catchment product feature looks at depth grid files names created by `ras2fim`, the extract the river features ID that are relevant.  Using that list of relevant feature IDs,  extract the relevant catchment polygons from `nwm_catchments.gkpg`, creating its own catchments gkpg.

New input files are required for this release to be tested and used. One file and one folder need to be downloaded in the `X-National_Datasets` folder, or your equivalent. The new file and folder are:
- `nwm_catchments.gpkg`:  It needs to be sitting beside the original three files of `nwm_flows.gpkg`, `nwm_wbd_lookup.nc`, and `WBD_National.gpkg`.  
- A folder named `WBD_HUC8` needs to  be downloaded and place inside the `X-National_Datasets` or equiv.  The `WBD_HUC8` folder includes a gkpg for each HUC8 from the a full WBD national gkpg. They were split to separate HUC8 files for system performance.

The new file and folder can be found in `inputs` directories on both rasfim S3 or ESIP S3.

There are some other fixes that were rolled into this, some by discovery during debugging, some based on partially loaded functionality.  They include:
- [Issue 71](https://github.com/NOAA-OWP/ras2fim/issues/71): Fix proj.db error 
- [Issue 46](https://github.com/NOAA-OWP/ras2fim/issues/46): Develop metadata reporting system. Note: Laura had most of this written and it was rolled into this branch / PR.
- [Issue 56](https://github.com/NOAA-OWP/ras2fim/issues/56) ras2fim - fix ras2catchments for new input data sources.  This was the o Develop Python script to convert ras2fim rating curves to Inundation Mapping calibration formatriginal issue that started this branch and PR.

Note: Currently, this edition does not have multi processing which calculating `maxments`. A `maxment` is the catchment for any particular feature based on its maximum depth value. Multi processing will be added later and was deferred due to the fact that based on current code the child process needed to pass information back to the parent process. This is very difficult and unstable. A separate issue card, [#75](https://github.com/NOAA-OWP/ras2fim/issues/75), has been added to fix this later. 

### Environment Upgrade Notes
This version requires an update to the conda environment for ras2fim.  To upgrade, follow these steps (using an Anaconda / Mini Conda command terminal):
1) If not already, type `conda deactivate ras2fim`
2) Ensure you are pathed into the correct branch to where the new `environment.yml` file is at, then type `conda env update -f environment.yml`. This might take a few minutes
3) `conda activate ras2fim`

Coupled with downloading the new catchment file and WBD folder into inputs, you should be ready to go. 

### Additions 

- `src`
    - `shared_functions.py`: Includes a common duration calculating function, now used by both ras2fim and ras2catchments.py. This file can be shared for common functionality across all files in the code. Common shared functions help with consistency, avoid duplication and ease of implementation. Other code blocks have already been detected as being duplicated and may be moved into this file at a later time.  The first function is for calculating and printing date / time duration.

### Changes  

- `README.md`: Has been updated to talk about downloading all `X-National_Datasets` files and folder now in the `Inputs` directory. Previous versions of this page talked about getting three files from a folder in ESIP called `National Datasets`. Now both ESIP and ras2fim S3 have an identical `inputs` folder.  This md file now also talks the new file and WBD_HUC8 folder in its required downloads.
- `INSTALL.md`: Updated to talk about the new inputs instead of the original three files.
- `environment.yml`: Changes to upgrade the rasterio package upgrade. Also added a new package called `keepachangelog` which has to be loaded via PIP as it is not available in any of the conda repos.
- `src`
    - `clip_dem_from_shape.py`: Added a simple time duration calculator. Needed for timing tests.
    - `ras2catchments.py`: Changes include:
        - This has been rebuilt in recognition that it had a fair bit of temporary duplication code from `run_ras2rem`. 
        - Added code making a metadata gpkg, pulling the version number from the CHANGELOG.md to put into the meta data
        - Upgraded input validation. 
        - This file expects a valid model_catalog.csv, usually found in OWP_ras_models folder, which provides more meta data. A new optional param has been added to provide a specific model catalog file. By default, it will look for a file called `models_catalog_{huc}.csv`. It should match the "ras model" folders being submitted to `ras2fim.py`. 
    -  `ras2fim.py`: Changes include:
        - Updated to change the duration output system to come from the new `shared_functions.py` file replacing it's original duration calc code. 
        - The `step` feature has been partially repaired as it had an error in it, but has not been fully fixed or tested. A separate issue card and PR will be coming. 
        -  In the meantime, the code now has re-instated the code feature of not allowing a user to re-execute ras2fim.py against a folder that already exists.  
        - Updates to add ras2catchments.py into the overall process flow.
        - A new section to copy "final" files from the ras2rem and catchments folder. The new `final` subfolder has the files required for release, as in final product files.
        - A few styling updates
    -  `run_ras2rem.py`: Some minor style changes (mostly spacing), but also added a "with" wrapper around the multi processing "pool" code, to be more PEP-8 compliant. 
    - `shared_variables.py`: to add new pathing and constants for changes above.
    - `worker_fim_raster.py`: During debugging, it was noticed that the pre-existing system for logging errors in this file, logged some info such as model name, but failed to log the reason for the exception.

<br/><br/>



## v1.8.0 - 2023-06-14 - [PR#73](https://github.com/NOAA-OWP/ras2fim/pull/73)

Introduces a new tool for the ras2fim that cycles through the ras2fim outputs and compiles the stream segment midpoints and rating curve tables. The purpose of this tool is to facilitate the use of ras2fim outputs in the FIM calibration workflow. This tool is intended to be added to the (not yet completed) ras2release workflow rather than the ras2fim workflow.

### Changes

- `src/compile_ras_rating_curves.py`: New file. Processes ras2fim outputs into correct format for FIM calibration.
- `src/shared_variables.py`: Added nodata value, output vertical datum, and `c:\ras2fim_data\ras2fim_releases\` filepath to shared variables file.

<br/><br/>


## v1.7.0 - 2023-06-07 - [PR#69](https://github.com/NOAA-OWP/ras2fim/pull/69)

Each time ras2fim is run, it creates an output folder for a HUC. Inside this folder are the six output folders (01..06). A new file is now being included that records all incoming arguments that were submitted to ras2fim plus a few derived values.

### Changes  
- `src\ras2fim.py`: changes include:
          - moving the int_step calculations down into the init_and_run_ras2fim function. Reason: It is also logic for validating and setting up basic variables.
          - moved the creation of the new HUC folder into init_and_run_ras2fim function. The folder needs to be created earlier, so the new run_arguments.txt file can be saved.

<br/><br/>

## v1.6.0 - 2023-06-02 - [PR#65](https://github.com/NOAA-OWP/ras2fim/pull/65)
Updated `src/run_ras2rem.py` with following changes:
- Fixed a bug regarding the units used for rating_curve.csv output. Now, ras2rem will infer the vertical unit from ras2fim results in 05_hecras_output folder. 
- Added multi-processing capability for making tif files for rem values.
- Added a progress bar to show the progress during above step (making tif files for rem values). 
- Made the help notes for -p flag more clear, so the user will understand where the ras2rem outputs are created. 
- Added some extra columns needed for hydroviz into rating_curve.csv output. This involved moving the required changes from `src/ras2rem.py` file into `src/run_ras2rem.py`, which allowed removing the `src/ras2rem.py` altogether.
Removed `src/ras2rem.py`.

<br/><br/>

## v1.5.0 - 2023-05-12 - [PR#55](https://github.com/NOAA-OWP/ras2fim/pull/55)

When a recent version of ras2catchment.py was checked in, it had hardcoding paths that were not testable. Some new input files were also required to get the feature working.  Considering the new data flow model and folder structure, the team agreed to attempt to standardize the pathing from one folder to another.  A new system was added to help manage paths inside the `C:\\ras2fim_data` directory, or whatever name you like.  Most of the original arguments and full pathing continues to work, but is no longer needed and it is encourage to now only use the pathing defaults.

New usage pattern with minimum args is now:
`python ras2fim.py -w 12090301 -o 12090301_meters_2277_test_3 -p EPSG:2277 -r "C:\Program Files (x86)\HEC\HEC-RAS\6.0" -t C:\ras2fim_data\inputs\12090301_dem_meters_0_2277.tif`.  Min parameters are:
- `-w` is the huc number as before.
- `-o` just the basic folder name that will be created in the "outputs_ras_models" folder.  ie) if this value is 12090301_meters_2277_test_3, the full defaulted path becomes "c:\ras2fim_data\outputs_ras_models\12090301_meters_2277_test_3 and all of the 6 child (01 to 06) will be created under it. It can be overridden to a full different path.
- `-p` is the CRS/EPSG value as before.
- `-r` is the path to the HECRAS engine software as before.
- `-t` is the path to the underlying DEM to be used. It is technically optional. When not supplied, the system will call the USGS -  website to get the DEM, but calling USGS is unstable and will be removed shortly (as it was before).

**For more details on sample usage and arguments, see the "__main__" folders in source code.

A new system was added to `ras2fim.py`, `run_ras2rem.py` and `ras2catchments.py` by moving logic out of the __main__ functions. Moving that code into a separate method, allows for input validation, and defaults to be accurately be set even if the .py file is being used without going through command line.  

For example, with run_ras2rem.py, a person can use the script from command line, however, ras2fim.py could also call directly over to a method and not come through command line. By moving validation and defaults into a separate function inside run_ras2rem.py, they can be enforced or adjusted.

Some changes were started against ras2catchments.py for new data required for it to be run, but at this point, the ras2catchment system is not working. A separate card will be created to continue to adjust it and get it fully operational. Styling upgrades were made to be more compliant with PEP-8 and fim development standards.

While there is a little scope creep here, there was no much. Most was required due to testing or new requirements.

PR related to issue [51](https://github.com/NOAA-OWP/ras2fim/issues/51). 

### Additions  
- `src`
    - `shared_variables.py`: holding all of the default pathing, folder and file names to ensure consistency throughout the code.

### Changes  
- `src`
    - `calculate_all_terrain_stats.py`: cleanup for redundant info and screen output.
    - `clip_dem_from_shape.py`: cleanup for redundant info and screen output.
    - `conflate_hecras_to_nwm.py`: cleanup for redundant info and screen output and changed some file names to come from shared_variables.
    - `convert_tif_to_ras_hdf5.py`: cleanup for redundant info and screen output.
    - `create_fim_rasters.py`: cleanup for redundant info and screen output and added a "with" wrapper around the processing pooling for stability reasons.
    - `conflate_shapes_from_hecras.py`: cleanup for redundant info and screen output.
    - `get_usgs_dem_from_shape.py`: cleanup for redundant info and screen output.
    - `ras2catchments.py`: Changes described above (pathing, styling, moving code out of the "main" function, adding in pathing for new data sources, etc). Note: the script is not operational but has taken a step forward to getting it fully working. The new data inputs and pathing were required first.
    - `ras2fim.py`:  Many of the changes listed above such as some small styling fixes, moving code out of "main", cleaning pathing to defaults, change some hardcoded paths and file names to now come from the `shared_variables.py` file for code system consistency, and added a bit of better "code step" tracking. 
    - `run_ras2rem.py`: Change described above (pathing, using common values from `shared_variables.py`, minor styling fixes, moving logic from "main" to another method, and fixes to be more PEP-8 compliant. Note: This file does not work and did not work in the current dev branch prior to this PR. Assumed that a fix is coming from another PR.
    - `simplify_fim_rasters.py`: cleanup for redundant info and screen output.
- `tools`
    - `convert_ras2fim_to_recurr_validation_datasets.py`:  Changes to support the new common values from `shared_variables.py`
    - `nws_ras2fim_calculate_terrain_stats.py`:  cleanup for redundant info and screen output.
    - `nws_ras2fim_check_processs_stats.py`:  cleanup for redundant info and screen output.
    - `nws_ras2fim_clip_dem_from_shape.py`:  cleanup for redundant info and screen output.
    - `nws_ras2fim_entwine_dem_from_shp.py`: cleanup for redundant info and screen output.
  
<br/><br/>

## v1.4.0 - 2023-05-03 - [PR#43](https://github.com/NOAA-OWP/ras2fim/pull/43)

This program allows for a user to provide an S3 path and a HUC, have it query the remote models catalog csv and download "unprocessed" folders to their local machine for processing with ras2fim.py.

Key Notes
- This tool is designed for working with OWP S3 and will likely not have value to non NOAA staff, unless they have their own AWS account with a similar structure.  However, ras2fim.py will continue to work without the use of this tool
- Review the arguments being passed into `get_ras_mdoels.py` file which include defining your own `models catalog csv` file but it will default to `models_catalog.csv`.  ie). testing, it could be `models_catalog_rob.csv`.
- New dependency python packages were added. 

Associated to issue [39](https://github.com/NOAA-OWP/ras2fim/issues/39). 

### Additions  
- `tools/aws`
    - `aws_base.py`: a basic parent class used to provide common functionality to any aws type code calls. Currently, it is used by `get_ras_models.py`, but others are expected.
    - `get_ras_models.py` : The tool mentioned above. See key features below.
    - `aws_creds_templates.env`: At this point, some functionality requires a valid aws credentials. However, a user will still need to setup their local user .aws folder with a config and credentials file in it, for this program to run. These are hoped to be amalgamated later. One of the args into `get_ras_models.py` is an override for the default location and adjusted file name.

### Changes  
- `doc\INSTALL.md`: minor corrections

### Features for this tool include:
-  Ability to create a list only (log file) without actual downloads (test download).
- For both pulling from S3 as a source as well as for the local target, the "models" will automatically be added at the end of thos provided arguments if/as required.
- A log file be created in the models/log folder with unique date stamps per run.
- A "verbose" flag can be optionally added as an argument for additional processing details (note: don't over  use this as it can make errors harder to find). To find an error, simply search the log for the word "error".
- Filters downloads from the src models catalog to look for status of "ready" only. Also filters out model catalog final_key_names starting with "1_" one underscore.
- Can find huc numbers in the models catalog "hucs" field regardless of string format in that column.  It does assume that models catalog has ensure that leading zero's exist.
- All folders in the target models folder will be removed at the start of execution unless the folder starts with two underscores. This ensures old data from previous runs does not bleed through. 
- After the source model catalog has been filtered, it will save a copy called models_catalog_{YYYYMMDD}.csv into the target output folder.  Later this csv can be feed into ras2fim.py for meta data for each model.


### Steps to apply new features including AWS credentials and conda enviroment.yml changes.

While this is a number of steps, it also setups for a fair bit of new and future functionality.

Use your anaconda terminal window for all "type" commands below.

1) Path to your new code dir. ie) cd c:\users\(your profile name}\projects\dev\ras2fim.
2) Type `conda activate ras2fim`
3) Type `conda env update -f environment.yml`.  You are ready to do testing with `ras2fim.py` and other files, but not `get_ras_models.py`

**If you are an NOAA/OWP staff member, do the next steps. Only NOAA/OWP members can use the new `get_ras_models.py`.**
1) See if you already have aws credentials file in place. Path to c:\users\{profile name|\.aws with a file named config and another named credentials in it. You might have to turn on with file explorer, the ability to see hidden files / folders. Once this is setup for your machine, it will not need to be done again.
2) If that folder and files are not there, type `aws configure`. It will ask for some AWS keys which you should already have.
3) In the c:\ras2fim_data folder, create a new subfolder called "config"
4) Inside the code folder "\tools\aws" is a file called aws_cred_template.env. Copy that file to your new "config" folder.
5) Rename that file to "aws_hv_s3_creds.env"
6) Open the file and fill in the fields with the aws credentials values you should have. Make sure to save it.
7) You are ready to test the new tool.


<br/><br/>


## v1.3.0 - 2023-05-01 - [PR#42](https://github.com/NOAA-OWP/ras2fim/pull/42)

This pull request includes additions and modifications to the existing ras2fim / ras2rem workflow that creates output files that can be fed directly into inundation.py.

### Additions  
- `ras2catchments.py`: creates COMID catchments as a raster (tif) and polygons (gpkg) for use with discharge / stage lookup. The inputs to the main function are input and output directories (input: location of step 5 ras2fim files where all the depth grid files live, output: location of ras2rem files [or anywhere else of user's choosing, if they don't want the resulting COMID raster and polygon files to be with the ras2rem outputs])
-  `ras2inundation.py`: copy of inundation.py script that can be modified for ras2fim as necessary if separate needs arise from HAND-based modeling

### Changes  
- `ras2rem.py`: Modifications to synthetic rating curve (SRC) generation to ensure we have the necessary fields for inundation.py to work, consistent with HAND-based hydro tables. Specifically adds or modifies the following columns:
    - HydroID (added as copy of feature_id)
    - HUC (added)
    - LakeID (added as -999)
    - last_updated (added as empty string)
    - submitter (added as empty string)
    - obs_source (added as empty string)
    - AvgDepth (m) renamed to stage
    - Flow (cms) renamed to discharge_cms
    

<br/><br/>

## v1.2.0 - 2023-04-21 - [PR#33](https://github.com/NOAA-OWP/ras2fim/pull/33)
This PR adds `ras2rem` functionality into `ras2fim`. For each NWM stream within the domain of HEC RAS models, `ras2fim` produces flood depth grids for a common set of reach averaged-depths. `ras2rem` merges all these depth grids and create a single raster file delineating the areas inundated with different values of reach averaged-depth.

### Additions  

- Added the new `src/run_ras2rem.py` containing two functions used for ras2rem

### Changes  

- Updated `src/ras2fim.py` with the changes required for addition of ras2rem

<br/><br/>

## v1.1.0 - 2023-04-11 - [PR#31](https://github.com/NOAA-OWP/ras2fim/pull/31)

This merge adds a new Pull Request template that was copied from the `inundation-mapping` project PR template.

### Changes
- `/.github/PULL_REQUEST_TEMPLATE.md`: updates this file to match the one used by `inundation-mapping` repository.

<br/><br/>

## 1.0.0 - 2021.11.10 - Initial Release
