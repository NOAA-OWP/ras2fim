All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.

## v2.0.beta.X - 2024-01-03 - [PR#233](https://github.com/NOAA-OWP/ras2fim/pull/233)

This PR merges create_src_depthgrids_4fids.py python script that creates synthetic rating curves (SRC) for each nwm feature id in a HUC8 domain. To create the SRCs, it needs information on water depths in each cross-section per flow value (step 5 output) and upstream and downstream cross-sections of each feature-id (step 2 output). Therefore, this PR also requests changes in step 2 Python script (conflate_hecras_to_nwm.py).

Note: At this point in the current V2 rebuild, ras2fim.py should work up to the end of Step 4 and break on Step 5. However, Step 5 
and Step 6 work independently.

### Additions  

- `src`

  - `conflate_hecras_to_nwm.py`: Some changes were added to this script to provide upstream and downstream cross-sections of each feature-id in a CSV file. 

### Changes 

- `src`

  - `create_src_depthgrids_4fids.py`: This script creates synthetic rating curves (SRC) for each nwm feature-id in a HUC8 domain

### Testing

- This PR was tested on all RAS models in 12090301 HUC8.  


<br/><br/>


## v2.0.beta.11 - 2023-12-18 - [PR#227](https://github.com/NOAA-OWP/ras2fim/pull/227)

In today's, Dec 15, 2023, merge from [PR 225](https://github.com/NOAA-OWP/ras2fim/pull/225) into Dev, there was some merge conflicts which were fixed on the fly.  During post merge testing, it appears some of the merging was not 100% successful and will be fixed as part of this card.

Also noticed a few other minor pathing required fixes based on other modules in today's merge.

Note: At this point in the current V2 rebuild, ras2fim.py should work up to the end of Step 4 and break on Step 5.

### Changes  

- `src`
    - `clip_dem_from_shape.py`: a few minor text changes.
    - `conflate_hecras_to_nwm.py`: Correct merge issues. Some of this was due to merge conflicts that needed to be fixed by hand such as some functions disappearing. Also added a few text fixes. Also fixed a small output pathing issue for folders for the 04_hecras_terrain folder.
    - `ras2fim.py`: Minor linting fixes, plus changing a variable path value from Step 2 to Step 3.
    - `worker_fim_rasters.py`: Minor linting fixes.

<br/><br/>

## v2.0.beta.10 - 2023-12-15 - [PR#225](https://github.com/NOAA-OWP/ras2fim/pull/225)

This PR is to complete the step 5 `worker_fim_raster.py` of ras2fim v2 and apply a couple of fixes to step 2 `conflate_hecras_to_nwm.py` results. They include:

1) Issue [210](https://github.com/NOAA-OWP/ras2fim/issues/210):  Developing ras2fim V2 depth grids

Note: Running steps 1 to 4 appear to work correctly but fail on step 5. More changes are coming soon.

### Changes  
- `src`
    - `worker_fim_raster.py` Multiple functions were added to former step 5 to complete it including: create_ras_plan_file, create_ras_project_file, create_ras_mapper_xml, create_hecras_files, fn_run_hecras, create_run_hecras_models.
    - `conflate_hecras_to_nwm.py`: Also, in step 2 the model-id column was added to the results.
    - `ras2fim.py`: One small change in ras2fim.py related to conflate_hecras_nwm.py. Also moved the copy of the models catalog file to earlier in processing as some steps need it. At finalization, the model catalog will be again copied to the "final" folder. Why?  Steps might modify the catalog during processing.
    - `shared_variables.py`: Changed folder location for Step 2 so steps 1 to 4 can work. 

<br/><br/>


## v2.0.beta.9 - 2023-12-15 - [PR#222](https://github.com/NOAA-OWP/ras2fim/pull/222)

`create_geocurves.py` was failing to merged up MP_logs (multi-proc logs) into the master log file. Upon review, MP_log was not setup correctly in that file and is now fixed. However, it exposed some required minor changes to how the logging system works as a whole. This triggered minor changes in imports for all file.

Also fixed:
- Closes Issue [# 70](https://github.com/NOAA-OWP/ras2fim/issues/70): Let ras2fim kick off from root ras2fim and not src directory: An super easy to fix, annoying enhancement. The  user is no longer forced to have to be in the "src" directory to run any scripts. They can not be in the "ras2fim" root. Now we can use commands like "python ./src/ras2fim.py" and "python ./tools/s3_search.py".  Easier to keep the focal point of the app at the root directory of ras2fim
- `conflate_hecras_to_nwm.py`: had an input arg that was listed a Required but is now Optional.

### Changes  
- `src`
    - `calculate_all_terrain_stats.py`: changed logging imports.
    - `clip_dem_from_shape.py`: changed logging imports.
    - `conflate_hecras_to_nwm.py`: changed logging imports, plus changed input arg for location of `X_National_datasets` to be optional and defaulted
    - `convert_tif_to_ras_hdf5.py`: changed logging imports.
    - `create_fim_rasters.py`: : changed logging imports, plus changed file location where to find the "PlanStandard". This now allows for command pathing to no longer be forced to start from the `src` directory. See note above (Issue 70). It also means the input arg for that path is no longer required.
    - `create_geocurves.py`: changed logging imports, fixed MP_log issue, changed input arg for producing polygons to default to "true" and only require the `-p` argument if you DO NOT want the producing polygons. This is a follow-up to a different PR that changed the default to produce polygons but we didn't notice the missed change in the input args.
    - `create_model_domain_polygons.py`: changed logging imports.
    - `create_shapes_from_hecras.py`: changed logging imports.
    - `get_usgs_dem_from_shape.py`: changed logging imports.
    - `errors.py`: renamed to `r2f_errors.py` and changed logging imports. 
    - `ras2fim.py`: changed logging imports, plus remove need to pass in input path to `create_fim_rasters.py`
    - `ras2fim_logger.py`: Many changes to fix MP_log and enhance the logging system (stopping circular reference issue). Added an new `MP_log_setup` method for MP_logs and not regular RLogs. Also a bit of code cleanup.
    - `ras2inundation.py`:  changed logging imports.
    - `reformat_ras_rating_curve.py`: changed logging imports.
    - `shared_functions.py`: changed logging imports and a little code cleanup.
    - `shared_variables.py`: Added R2F_LOG (the instantiation of the logging system to here instead of at the bottom of `ras2fim_logger.py`: This solves some problems that started to occur with circular references.
    - `simplify_fim_rasters.py`:  changed logging imports.
    - `worker_fim_rasters.py`:  changed logging imports.
- `tools`
    - `get_models_by_catalog.py`:  changed logging imports and a bit of code cleanup.
    - `nws_ras2fim_clip_dem_from_shape.py`:  changed logging imports (despite being largely deprecated)
    - `nws_ras2fim_entwine.py`:  changed logging imports (despite being largely deprecated)
    - `nws_ras2fim_terrain_AWS_tiles.py`:  changed logging imports (despite being largely deprecated)
    - `nws_ras2fim_terrain_Texas.py`:  changed logging imports (despite being largely deprecated)
    - `nws_ras2fim_terrain_USGS.py`:  changed logging imports (despite being largely deprecated)
    - `ras_unit_to_s3.py`:  changed logging imports.
    - `s3_search_tool.py`:  changed logging imports and a couple console output color code. Was correct to screen but was creating invalid values in the log file.
    - `s3_shared_functions.py`:  changed logging imports and a couple console output color code. Was correct to screen but was creating invalid values in the log file.

<br/><br/>


## v2.0.beta.8 - 2023-12-15 - [PR#218](https://github.com/NOAA-OWP/ras2fim/pull/218)

Created a new tool that can compare the S3 version of the `OWP_ras_models_catalog.csv` to the S3 models folder. This is to ensure that the master catalog and the model folders stay in sync. There are rules and tests that are applied and recorded in a new report csv showing errors. See PR for those rules.

### Additions  
- `tools`
    - `s3_model_mgmt.py`:  As described above.

### Changes  

- `pyproject.toml`: added a new exception for the new file.
- `src`
    - `ras2fim.py`: Changed section headers to the new logging level of "notice" for better readability on screen.
    - `ras2fim_logger.py`: Code layout changes.
    - `reformat_ras_rating_curve.py`: Removed a debugging print line.
    - `shared_variables.py`: Took a slash off the end of "S3_DEFAULT_BUCKET_PATH" variable.
- `tools`
    - `get_models_by_catalog.py` : Changed starting model id to be 10001 and not 10000 plus fix a few code layout issues.
    - `ras_unit_to_s3.py`: Minor text changes and added a date to the log file being created.
    - `s3_search_tool.py`: Changed a function name in s3_shared_functions which needed updating here; fixed a few code layout issues and added a date to the log file being created.
    - `s3_shared_functions`:
        - Small text and layout changes.,
        - Moved a few functions to different places (on page).
        - Renamed one function.
        - Added a new function for "get_folder_list" (all folder names (well key names) at one S3 folder level only, recursively.
        - Added a new function for "get_folder_size". 
        - A few renamed variables.

<br/><br/>

## v2.0.beta.7 - 2023-12-15 - [PR#215](https://github.com/NOAA-OWP/ras2fim/pull/215)

The DEM clipping script has been updated to use full WBD gpkg file and find all the HUC12s (even in other HUC8s) that intersects with an RAS model domain. The relevant HUC12s are then dissolved together and used for clipping the DEM for the RAS model. One DEM is created for each RAS model and the tif file is saved under the name <model_id>.tif. 
Note that the new functionality also needs preparing DEMs that covers bigger domain than the studied HUC8 (probably by applying bigger buffers). Two additional inputs are now required for `src/clip_dem_from_shape.py`: 
1. The cross sections shapefile (from Step 1 ) to select the HUC12s that intersect with each RAS model
2. The csv file containing list of conflated models (from step2), so DEM clipping applies only for the conflated models.

This PR closes #190.

### Changes  

- `src/clip_dem_from_shape.py`
   - The input HUC12 shapefile argument can now be the full WBD shapefile (instead of the HUC12 shapefile from Steps 1&2 which are specific to the studied HUC8).  
   - Two more input arguments are needed as described above. 
   - Because the clipped DEM names must follow model_id from model catalog, there is no need to have the "str_field_name" argument anymore, and this argument has been removed. 
   - The intersection / spatial join algorithm to find the relevant HUC12s has been modified to incorporate cross sections. These last 2 changes were necessary because there could be additional HUC8s that overlap the model, particularly at its headwaters and outlet.
- `src/ras2fim.py`: the function call to clip DEMs has been modified to reflect the additional cross section shapefile, and csv file of conflated models, as well as removing the "str_field_name" argument, which is not needed anymore (because we must only use model_id derived from model catalog for tif file names). 

<br/><br/>

## v2.0.beta.6 - 2023-12-04 - [PR#212](https://github.com/NOAA-OWP/ras2fim/pull/212)

This PR covers a couple of fixes all based around the `get_models_by_catalog.py`. They include:

1) Issue [201](https://github.com/NOAA-OWP/ras2fim/issues/201):  Add ID number column to filtered unit models catalog
2) Issue [114](https://github.com/NOAA-OWP/ras2fim/issues/114): get model catalog tool - check if dup final_name_key
3) Issue [174](https://github.com/NOAA-OWP/ras2fim/issues/174): Add multi-threading to get_models_by_catalog.py
4) Logger file system exception: Some py files do not setup the RLOG system until after input args have been validated. This was throwing an exception saying that it could not write to the file system. Updated the logger so it just prints the log message to console but skips attempting to write to the log file and adds a print console message saying log to file system not set up.
5) Logger add new `notice` type: Found a need to log and display a new type which is more of an "info" type message that didn't fit in other types, but needed it's own color.

### Changes  
- `src`
    - `ras2fim_logger.py`: Fixed the logger file system exception issue and added new level type of `notice`.
    - `reformat_ras_rating_curve.py`: Added critical comment to help keep it in sync with FIM and small cleanup.
    - `shared_variables.py`: New variables to manage new named columns in the filtered models catalog saved locally.
- `tools`
    - `get_models_by_catalog.py`: 
         - Added new `model_id` catalog which starts at the number 10,000.
         - Removed actual downloading of s3 folders from this file and into `s3_shared_functions.py`.
         - Updated some output and log lines.
         - Updated how the existing `downloaded (successful` and `download error` columns are named and are populated.
    - `ras_unit_to_s3.py`: Change a log output line from `lprint` to `notice` for easier readability.
    - `s3_search_tools.py`: Change a log output line from `lprint` to `notice` for easier readability.
    - `s3_shared_functions.py: 
        - Various small comment and output text changes.
        - Added new function to allow for `download_folders` (from S3) which previously in `get_models_by_catalog.py`. This is expected to be used by other tools in the near future such as ras2release.  It also includes multi-threading (notice.. not multi proc). Multi proc would not use system resources very well for this type of serialization. Also notice is it folders plural.
        - Add new function to allow for `download_folder` (from S3) for a single folder. Also expected to be used in the near future.

<br/><br/>

## v2.0.beta.5 - 2023-12-04 - [PR#209](https://github.com/NOAA-OWP/ras2fim/pull/209)

Added a new tool that can so wildcard searching s3 for files and folders including recursively. It uses a simple * (asterisks) to represent zero to many characters. It is not case-sensitive.

Upon finding file and folder matches, an output csv is created.

Searching is not done on each individual file or folder name but the full path of both:
e.g. 1260391_EAST FORK TRINITY RIVER_g01_1690618796/EAST FORK TRINITY RIVER.f01 :  note the forward slash. Why the full path instead of both segments individual? e.g ['1260391_EAST FORK TRINITY RIVER_g01_1690618796', 'EAST FORK TRINITY RIVER.f01'] ?  Because a user might want to use different combinations such as '1260391*.f01`

While this is designed to work against model folders, it can be used against any s3 bucket anywhere (assuming valid credentials)

### Additions  

- `tools`
    - `s3_search_tools.py`: New tool as described above.

### Changes  

- `src`
   - `shared_functions.py`: Added option to include random number suffice to "get_date_with_milli".
   - `shared_validators.py`: text fix.
   - `shared_variables.py`: added new default pathing used by new search tool.

- `tools`
    - `get_models_by_catalog.py`: Simply and adjust some import statements, text adjustments.
    - `ras_unit_by_s3.py`: Simply and adjust some import statements, text adjustments, update a few variable names, add a bit more color to outputs.
    - `s3_shared_functions.py`: Adjusted how color tags are used, updated a few variable names, fixed job count error for multi-threading, added new `get_records` for getting a list of file/folder names matching the wildcard search as per s3_search_tool.py.


<br/><br/>

## v2.0.beta.4 - 2023-12-01 - [PR#208](https://github.com/NOAA-OWP/ras2fim/pull/208)

During a recent other merge, ras_unit_to_s3 began failing to upload. Now fixed.

### Changes  
- `src`/`shared_validators.py`: corrected text.
- `tools`
    - `ras_unit_to_s3.py`: Fix upload bug which was based in the `skip_files` system which exempts some files from  uploading to S3. Also added color to console as questions are asked of the user (live input). 
    - `s3_shared_functions.py`:  Removed progress bars which don't work well now with RLOG. . Fixed the `skip_files` system. Added color in key screen outputs for readability. Added throttling on max number of CPU's for multi-thread.

<br/><br/>

## v2.0.beta.3 - 2023-12-01 - [PR#213](https://github.com/NOAA-OWP/ras2fim/pull/213)

This PR addresses issue #180 and adds ras2fim version number, which is now automatically derived from `doc/CHANGELOG.md` file,  into the outputs of `src/create_model_domain_polygons.py` and `src/create_geocurves.py` scripts. 

In the earlier version of `src/create_geocurves.py` file, -v has been an argument that required asking the user to provide the `doc/CHANGELOG.md` file path. This argument has been removed and the `doc/CHANGELOG.md` path is now inferred by the code. Now, by default, ras2fim version number is always added to outputs of above two scripts. 

<br/><br/>

## v2.0.beta.2 - 2023-11-17 - [PR#205](https://github.com/NOAA-OWP/ras2fim/pull/205)

This PR updates `reformat_ras_rating_curve.py` to assign the ras2fim version to the `source` column using the get_changelog_version shared function. It also changes the output filenames to be named ras2calibration_rating_curve_points.gpkg and ras2calibration_rating_curve_table.csv, which are more descriptive than the previous names.


### Changes  
- `src/ras2fim.py`: Removes the 'ras2fim' argument from the line that runs `dir_reformat_ras_rc`. 
- `src/reformat_ras_rating_curve.py`: Added functinoality to automatically get the ras2fim version from `CHANGELOG.md`. Removed hardcoded `source` variables and vestigial references to the previously-removed `-so` flag.
- `src/shared_variables.py`: Changed the `R2F_OUTPUT_FILE_RAS2CAL_CSV` and `R2F_OUTPUT_FILE_RAS2CAL_GPKG` variable names to be more descriptive. 

<br/><br/>

## v2.0.beta.1 - 2023-11-16 - [PR#207](https://github.com/NOAA-OWP/ras2fim/pull/207)

The goal of this PR is to merge the first ras2fim V2.01 to the main branch. Step 2, `conflate_hecras_to_nwm.py` and Step 5, `worker_fim_rasters.py` of ras2fim V1 were significantly changed.  `conflate_hecras_to_nwm.py` V2.01 now conflates HECRAS model streams to the NWM streams and finds the matched streams. `worker_fim_rasters.py` V2.01 computes boundary conditions for conflated streams, creates RAS flow and plan files and generates the inundation depth grids using the HECRAS Controller. 

**Conflate_hecras_to_nwm.py works from the command line. ras2fim.py does not work. Neither worker_fim_rasters.py**

### Changes
- `src`
   -  `conflate_hecras_to_nwm.py`: 
      - `cut_streams_in_two` function was added. 
      -  `conflate_hecras_to_nwm` function had a major upgrade for ras2fim V2.
   - `worker_fim_rasters.py`: a major upgrade for ras2fim V2.

<br/><br/>



## v1.30.1 - 2023-11-2 - [PR#198](https://github.com/NOAA-OWP/ras2fim/pull/198)

This PR fixes a small bug for making polygons for model domains that results in reporting all models to be not-conflated to NWM reaches. This PR closes #195.

Changes include:
Updated src/create_model_domain_polygons.py by removing an extra "is True" from a single line. Also, the file tools/ras_unit_to_s3.py changed slightly after performing linting.

<br/><br/>

## v1.30.0 - 2023-11-08 - [PR#183](https://github.com/NOAA-OWP/ras2fim/pull/183)

A custom logging system was added. Testing against native python logging as well as some independent packages showed none them were reliable for multi-processed logs.

The solution here is to let each multi-process (MP) have it's own logging file, which avoids file collisions, then at the end of the MP, let the logger merge them back into the parent log files.  Sorry, it isn't the prettiest of solutions but solves the problem.

Almost all files were changed to add in the system.  When ras2fim.py is running, it will setup logging for all child scripts, however, each independent script has the ability to setup its own logging system as required.  There we no updates to logic of any core files and most files were changed to form the base of the new system. It is expected usage of the new logger functions will grow quicky.

A conda update is required again (conda remove --name ras2fim --all -y, then conda env create. See previous builds for full examples.

There is a wide amount of details on implemenation, usage, background, etc which can be read in the [PR 183](https://github.com/NOAA-OWP/ras2fim/pull/183). We encourage you to read the PR notes to become familiar with the system.

### Additions  
We will not list all files affected as most are. However, I will list files that have any additional fixes or changes other than adding of logging.

Many files had extra `LPRINT` logging with tracing and/or more context data for `DEBUG`, `WARNING`, `ERROR` and `CRITICAL`.

### Additions
- `ras2fim_logger.py`: The parent script that runs the entire logging system.

### Removals
- `src`
     - `ras2catchments.py`: No longer applicable for V2.

### Changes  
- ` config`
    - `r2f_config.env`: Change PRODUCE_GEOCURVE_POLYGONS to True.  Removed flags for RUN_RAS2REM, and RUN_RAS2CATCHMENTS. 
 
- `environment.yml`: added new package for colorma and colored.
- `pyproject.toml`:  adding of an linting exception for convert_ras2fim_to_recurr_valiation_datasets.py.
- `src`
   - `conflate_hecras_to_nwm.py`:  Changed a few variable names. Upgraded time stamp and duration system. Renamed a few functions using multi-processing for easier identification.
   - `convert_tif_to_ras_hdf5.py`: Added new partial "verbosity" system to allow for optional additional output.
   - `create_fim_rasters.py`: Upgraded time stamp and duration system. Added better error handling for multi-processing pools. Added the start of a verbosity system which can be used to show certain messages only if "is_verbose". More is needed later in this script.
   - `create_geocurves.py`: Added some screen output for incoming params to match other script patterns. Also added new logging for when some logging when depth grid files are skipped.
   - `create_shapes_from_hecras.py`: Upgraded the multi-proc pool for better error handling. A few variable name changes.
   - `ras2fim.py`: removed all references to `run_ras2rem` and `ras2catchments`. 
   - `ras2inundation.py`: Upgraded some error handling.    
   - `reformat_ras_rating_curve.py`: Upgrade datetime.now to datetime.utcnow. Updated some of the "sample usage" notes.
   - `run_ras2rem.py`: Added some of the logging but stopped. Moved it to the "tools" directory and marked as deprecated.
   - `shared_functions.py`: Added a new function as well as added new argument to get_stnd_date().
   - `shared_variables.py`: Added some new variables for the logging system.
   - `simplify_fim_rasters.py`: Fixed a bug allow float values to be passed in as command line arg parse values. Need to be int and not a float for the resolution size variable.
   - `worker_fim_rasters.py`: Keep the original "errors.csv" but renamed that output file. Updated a few variable names. Added a system to slightly stagger start of processes in the multi-process system to avoid rare collisions of a bunch of procs starting at once. A couple of minor variable name changes.
- `tools`
   - `get_models_by_catalog.py`: Added another delimiter when reading the S3 parent models catalog file, which was dropping records where the final key starts with `3_`.  Small try except updates. Removed its own previously existing logging system in favor of the new logging system.
- `ras_unit_to_s3.py`: fix some validation checking and updated a bit of error handling. Note: It has a bug in it that appears to be unrelated to logging. A new issue has been issued to be fixed later.
- `run_ras2rem.py`:  Moved from src directory and added deprecation message.
- `Note`: None of the files in the tools directory starting with the name of `nws_` were updated as they may be no longer in use.

<br/><br/>

## v1.29.0 - 2023-09-29 - [PR#166](https://github.com/NOAA-OWP/ras2fim/pull/166)

This PR includes a new tool that can take a ras2fim unit output folder and upload it to S3. During that upload processes, it checks the s3 `output_ras2fim` folder to look for folders already share the same huc and crs values. A folder may/may not pre-exist that matches the huc and crs but may/may not share a date.  A new master file called `ras_output_tracker.csv` exists now in the s3 `output_ras2fim` folder which tracks all folders uploaded, moved to archive, and overwritten. All activities done by the new `ras_unit_to_s3.py` update this new master copy in S3.

The s3 `output_ras2fim` folder is intended to have only folder with a given huc and crs and would almost always be the latest version.  All other versions would be renamed to the phrase of "_BK_" and a date and moved into the `output_ras2fim_archive` folder. 

There are some combinations where a user can overwrite older folders or simply move incoming folder straight to the archive folder.   Lots of output including input question to the user with choices of aborting, moving existing folders and other options.

Other small fixes include:
- [issue 152](https://github.com/NOAA-OWP/ras2fim/issues/152), which changes all ras2fim code to use UTC times.
- [issue 111](https://github.com/NOAA-OWP/ras2fim/issues/111) to fix is to make use of a previously added method which can validate a CRS when it comes in as an input arg to a file. This has now been applied to both `ras2fim.py` and `get_models_by_catalog.py`.
- `get_models_by_catalog.py` was renamed from its previous name of `get_ras_models_by_catalog.py`. It was also adjusted for an additional filter as per change in pre-processing (RRASSLER team).
- change all files with used multi-proc or multi-threading to be cpu proc count - 2 (used to be -1 and it was leaving machines without not enough cpu's to handle other tasks. It was especially noticed when debugging and needed to use Task Manager and it was bogging down the system badly.
- some small misc cleanup on a few files such as a bit of variable and function renaming, output and formatting.

### Additions  
- `tools`
   - `ras_unit_to_s3.py`: As described above.
   - `s3_shared_functions.py`:  A file to manage all communication with S3, upload, deleted, move, etc. It does not yet have a download function but it will likely be added and plugged into `get_models_by_catalog.py` at a later date. However, other tools that are coming soon can use these same functions.

### Changes  
- `pyproject.toml`: Added a few adjustments to the pre-file-ignores section.
- `src`
    - `calculate_all_terrain_stats.py`: Change multi-proc / number of workers count as mentioned above.
    - `conflate_hecras_to_nwm.py`: Change multi-proc / number of workers count as mentioned above.
    - `create_fim_rasters.py`: Change multi-proc / number of workers count as mentioned above.
    - `create_geocurves.py`: Change multi-proc / number of works and changed date time to UTC.
    - `create_shapes_from_hecras.py` Change multi-proc / number of works and comments on non standard linting issue.
    - `ras2catchment.py`: Change multi-proc / number of works and changed date time to UTC.
    - `ras2fim.py`: Changes include: 
         - Renamed a few variables to be more descriptive.
         - Small changes the argument log file.
         - Small change to track each section being processed by ensuring they have the word `step` in the header (not necessarily followed by a number). This allows for easier searching of the screen output for section headers.
         - Change multi-proc / number of works and changed date time to UTC.
     - `reformat_ras_rating_curve.py`: Change multi-proc / number of workers count as mentioned above.
     - `run_ras2rem.py`: Change multi-proc / number of workers count as mentioned above.
     - `shared_functions.py`: Moved the print_date_time_duration location in the file.  Added a test that a models folder existed when it was looking to get the projection from ras_proj. Why? It was possible to have invalid values folders or pathing in ras2fim.py and was creating issues on rare occasions (but it happened). Also changed date time to UTC.
     - `shared_validators.py`: text adjustment
     - `shared_variables.py`: add three new variables for S3.
     - `simplify_fim_rasters.py`: Change multi-proc / number of workers count as mentioned above.
     - `worker_fim_rasters.py`: Linting fix.
 - `tools`
     - `get_ras_models_by_catalog.py` renamed to `get_models_by_catalog.py`
     - `get_models_by_catalog.py:  renamed a page level function name. Changed to UTC time. Added "3_" as a new skip filter for downloading model folders. Removed code to validate incoming CRS value to use the `shared_validators.py` version.

<br/><br/>


## v1.28.0 - 2023-09-29 - [PR#168](https://github.com/NOAA-OWP/ras2fim/pull/168)

Errors were being thrown by `conflate_hecras_to_nwm` as it iterated through a shape layer, with some records being linestrings and others being multilinestrings.  Upon closer examination, it was doing some unnecessary "simplifying".  Cleaning that up took care of it, as it did not need to attempt to split a multi line string to independent segments to run simplify.

Also took care of a couple of other tidbits that I ran into:
1) During Step 2 (`create_shapes_from_hecras`), the hecras engine would throw errors saying if it could not find some key input files. This has now been fixed. Note: Later, this should be logged.
eg. 
![image](https://github.com/NOAA-OWP/ras2fim/assets/90854818/63e12a0f-78bd-4a87-afab-9c79e450bd3b)

2) Added a duration timer to calibration `reformat_ras_rating_curves.py`

### Changes  
- `src`
    - `conflate_hecras_to_nwm.py`:  simplified the code which uses the shapely "simplify" calls.
    - `create_shapes_from_hecras.py`:  added a test to ensure the ras_project_path (file) exists. 
    - `ras2fim.py`:  small text change.
    - `reformat_ras_rating_curve.py`:  added a duration output system.

<br/><br/>


## v1.27.1 - 2023-09-28 - [PR#176](https://github.com/NOAA-OWP/ras2fim/pull/176)

Upgrade our HEC-RAS software from 6.0 to 6.3.0.  Even though there is a 6.4x versions, we can not use it at this time.

Note: The actual change was a couple small fixes changing the value of `win32com.client.Dispatch("RAS60.HECRASController")` to `win32com.client.Dispatch("RAS630.HECRASController")`  (60 to 630).  Also changed the default pathing on the file system from 6.0 to 6.3.

The rest of the changes are documentation either in the README.md, output or inline comments.

To apply this update, you must fully un-install HECRAS, then re-install the new version.
- If you are a NOAA employee, please use software center for this upgrade.
- If you are not a NOAA employee, please use the link in the README.md file.


### Changes  

- `README.md`:  Text updates
- `src`
    - `convert_tif_to_ras_hdf5.py`: Comment changes.
    - `create_shapes_from_hecras.py`: HECRAS controller changed from 60 to 630, plus comment changes.
    - `ras2fim.py`: Comment and output note changes.
    - `shared_functions.py`: Comment changes.
    - `shared_variables.py`: Changed default pathing to the HECRAS software.
    - `worker_fim_rasters.py`: HECRAS controller changed from 60 to 630, plus comment changes.
 - `tools`
     - `nws_ras2fim_terrain_iowa.py`: HECRAS controller changed from 60 to 630, plus comment changes. Note: not tested, not believed to be in use anymore.

<br/><br/>


## v1.27.0 - 2023-09-21 - [PR#165](https://github.com/NOAA-OWP/ras2fim/pull/165)

Added pre-commit hooks.

At this point, we leave it to the honor system for you to install pre-commit and use it. Soon, we will be adding it as a mandatory push to git.  Please start getting use to using pre-commits now.

Notes:
- Most files were changed minorly for adjusted linting rules.
- For each repo downloaded, you need to install `pre-commit`. This is not the same as environment pack setup. This is an installation of the tool itself.  From your ras2fim directory, run `pre-commit install`.
- When you run `git commit ...`, it will automatically run isort, black, and pflake8 with exceptions already in place in the `pyproject.toml`.  `isort` and `black` will auto change your files. `pflake8` will block the commit if it fails a rule test. 
- You can also pre-test the three linter steps before the commit in a couple of ways.
    - you can run `pre-commit run` which will scan all files / folders and run all three commands on them.
    - you can run `pre-commit run ras2fim.py` ... aka a single file.
    - you can run each of the three command independently on a file or folder.  eg. `isort ras2fim.py` or `pflake8 ras2fim.py`    - 

For more details and image, please see [PR#165](https://github.com/NOAA-OWP/ras2fim/pull/165). Details include things like editing the pyproject.toml file, working with VSCode, and examples of using linting and pre-commit commands.

Note: Most files were changed minorly for adjusted linting rules.

### Additions  

- `.pre-commit-config.yaml` : setups rule for doing pre-commits

### Changes  

- `pyproject.toml`:  misc updates plus added one file exception with two excluded test rules.

<br/><br/>


## v1.26.1 - 2023-09-20 - [PR#163](https://github.com/NOAA-OWP/ras2fim/pull/163)

A bug was discovered with errors being thrown when a incoming depth grid tif used to create a geocurve had only zero pixel values. This means there are no cells above the water surface and a geocurve should not be created.

Also.. a simple progress bar was added to match convention of other modules so that the modules does not appear to be frozen. A new function was added to `shared_functions.py` so that other modules can begin to use it.

### Changes  

- `src`
    - `create_geocurves.py`:  added logic to skip creating a geocurve, as described above. Also added some upgraded error handling. Also added a simple progress bar system.
    - `shared_functions.py`: added a new function for easily implementing progress bars.

<br/><br/>


## v1.26.0 - 2023-09-07 - [PR#157](https://github.com/NOAA-OWP/ras2fim/pull/157)

We have upgraded all files to add a linting system using packages of `isort`, `black`, and `flake8 (pflake8)`.  All files were run through those three tools in that order, each one at a time with minor cleanup being done along the way.

Some functions existed in multiple files and some of them were moved to shared_functions for commonality / re-usability. 

Other minor changes were  most triggered by packages updates.

A new tool was also added called `hash_compare.py` which can take two files or two directories and using hashing to look for differences. This was used to validate changes via the addition of linting.

**Note**: A previously detected warning saying `PROJ: proj_create_from_database: Cannot find proj.db` continues to show up quite a bit. It does not hurt logic or code. A issue card already exists for it. [# 145](https://github.com/NOAA-OWP/ras2fim/issues/145).

In order to add the three new packages, it forced other package upgrades and some downgrades which triggered even more changes, so a full enviro reload is required. Here are the steps to reload the enviro.

### Conda Environment Upgrade
**Note** There is a critical upgrade to the ras2fim conda and it is not backwards compatible. To upgrade this particular version, 

1) We need to full uninstall the ras2fim environment, not upgraded it:
   - run: _conda deactivate_  (if you are already activated in ras2fim)
   - run:  _conda remove --name ras2fim --all_
   - Make sure you have downloaded (or merged) this PR (or dev branch).  Ensure you are in that folder in the `ras2fim` folder where the environment.yml is at.
   - run: _conda env create -f environment.yml_  . It might be slow, but 5 to 10 mins is reasonable.

### Code Merge Compatibility
There are literally hundreds of changes and it will be extremely hard to merge older code bases into this. It is strongly recommended to restart your changes by loading this new full branch, then manually add your changes back in to it.  Then run the three linting tools again before checking it it. Details lower.

### Linting Tools Usage
It is now expected that when you change a file, before you check it in, run the three linting tools using the following pattern. 
1) in cmd, run `isort {your file name}`.  e.g.  isort ras2fim.py.
2) in cmd, run `black {your file name}`.  e.g. black create_fim_rasters.py
3) Review the changes from black and make minor changes if you see fit. There are a good handful of anomalies and things that black will do that are not necessarily intuitive. Some of the major ones will be listed lower.
4) in cmd, run `pflake8 {your file name}`. e.g. pflake8 clip_dem_from_shape.py

We have added a configuration file that helps manage the rules we want in place for the three tools. The file is named pyproject.toml and is automatically used when using the three linting tools.  There are some tidbits, configuration and byproducts listed below.

### Linting Tidbits, Configuration and By-Products
- we set out line length at 110
- black does not attempt to line split when comments are in a line. You will need to split these yourselves to get it under 110. Note: If you have a line with some code and a comment after it, it will not split it correctly. Maybe go above and add extra blank line above that comment you like.  e.g.
    - was:
    ```
         some_var = "some string"
         print("do something but print cmd itself not over 100 …..") # and pretend this puts is over 110
    ```
    - now: consider (but optional as long as you don't break 110 in total)
    ```
        some_var = "some string"
		
	# and pretend this used to put it over 110 chars
        print("do something but print cmd itself not over 100 …..")
    ```
- Most of the time, argument values into a function will leave an extra comma on the end of the list and that is ok. e.g.
    ```
	some_var = ["some value",
                            "another value",]
    ```
- You may not be familiar with PEP-8 standards for `import`s and `from`s, but it is correct. See [isort](https://pycqa.github.io/isort/). 

### Additions  
- `pyproject.toml`: helps manage ras2fim rules for linting and it auto applied when using the linting commands in the same directory as current ras2fim code (your branch).
- `tools\hash_compare.py`: a tool to help compare two files or two directories using hashing to look for differences.

### Changes  
Almost all py files were changed for linting as well as some other additions listed above.

<br/><br/>


## v1.25.0 - 2023-08-29 - [PR#151](https://github.com/NOAA-OWP/ras2fim/pull/151)

Addressing the issue #120, this PR creates polygons for HEC-RAS models domains.

The required input is the shapefile of the RAS models cross sections created in output folder "01_shapes_from_hecras", 
The optional input is the conflation qc file available in output folder "02_shapes_from_conflation" to mark the HEC-RAS models that have been conflated to NWM reaches. 

The output is a gpkg file. 


### Additions  
Added the new `src/create_model_domain_polygons.py` file to create polygons for RAS models domains. 

### Changes  
- `config/r2f_config.env`
Added "CREATE_RAS_DOMAIN_POLYGONS = True" into the config file

- `src/ras2fim.py`
Added a section to manage the process to create domain polygons if the above config variable "CREATE_RAS_DOMAIN_POLYGONS" is True.  

- `src/shared_variables.py`
Defined the output folder name holding polygons gpkg file

<br/><br/>

## v1.24.0 - 2023-08-17 - [PR#139](https://github.com/NOAA-OWP/ras2fim/pull/139)

In previous versions of the code, `reformat_ras_rating_curve.py` would have to be run separately in order to compile the ras2fim results needed for FIM calibration. With this update, the directory-level processing required to prepare the data for the calibration workflow is run as one of the final steps in the `ras2fim.py` script. This will decrease the amount of work required to prepare `ras2fim.py` output runs in the future.

### Changes  

- `src/ras2fim.py`
  - Code now imports `dir_reformat_ras_rc()` function from `reformat_ras_rating_curve`
  - updated coded to integrate optional usage of the system via the config arg of `RUN_RAS2CALIBRATION`
  - Added `dir_reformat_ras_rc()`
  - Code now saves the output csv and geopackage to the `final` folder as well as the `ras2calibration` folder

- `src/shared_variables.py`
  - Added `R2F_OUTPUT_DIR_RAS2CALIBRATION` folder name.
  - Added `R2F_OUTPUT_FILE_RAS2CAL_CSV`, `R2F_OUTPUT_FILE_RAS2CAL_GPKG`, and `R2F_OUTPUT_FILE_RAS2CAL_LOG` file names. 

- `src/reformat_ras_rating_curve.py`
  - pulls filenames from `src/shared_variables.py`
  - Replaced `dir` and `input_folder_path` inputs to `dir_reformat_ras_rc()` function with a combined path + directory object (`dir_input_folder_path`)

- `config/r2f_config.env`: added a new config arg to turn ras2calibration on/off

<br/><br/>

## v1.23.0 - 2023-08-17 - [PR#140](https://github.com/NOAA-OWP/ras2fim/pull/140)

Adds the capabilities to produce geocurves and produce inundation from canned geocurves & precomputed polygons. Geocurves are simply default RAS2FIM rating curves, but with the geometry of inundation extents appended as a WKT geometry column.

All Hydrovis needs are the geocurves, but for FIM Team users, we will need to run `create_geocurves.py` with the `-p` option that generates polygons in addition to geocurves.


### Additions  
- `/src/create_geocurves.py`: Script to produce geocurves from hecras output folder. 


### Changes 
- `/src/ras2fim.py`: Added code (currently commented out with `TODO` block) that can be uncommented after steps 8 and 9 (REM code) have been removed from `ras2fim.py`. The geocurves code replaces those steps.
- `/src/ras2inundation.py`: Rewrite. New version produces inundation from the outputs of `create_geocurves.py`.
- `/config/r2f_config.env`: Added two entries for turning on/off of the geocurves system.

<br/><br/>


## v1.22.0 - 2023-08-22 - [PR#142](https://github.com/NOAA-OWP/ras2fim/pull/142)

This PR covers three tasks:
1) [Issue 141](https://github.com/NOAA-OWP/ras2fim/issues/141) - Add config system. Portions of logic in the system, such as ras2rem and ras2catchments are managed by "run" flags in a new config file.  This now gives us an env file that can be upgraded for many purposes. At this time, it it used to skip processing of ras2rem and ras2catchments.

2) We wanted to upgrade a number of key packages such as pandas, shapely, and geopandas. This triggered a number of other package updates. However, attempts to upgrade all packages were unsuccessful.  Major updates are:
     - geopandas  from 0.9.0  to 0.12.2
     - pandas  from 1.3.1  to  2.0.3
     - h5py   from 3.2.1  to 3.7.0
     - numpy  from 1.20.3  to  1.24.3
     - shapely  from  1.7.1  to  2.0.1
     - pyproj    from  2.6.1  to  3.4.1  (this one had to be done via pip embedded in the environment.yml file)

One major change is how dataframes add records. In the past, it used append but that is no longer available and  must you concat now. After research, it was uncovered that it is best to make a temp dataframe for new data coming in then, concat that the original df. 

3) [Issue 144](https://github.com/NOAA-OWP/ras2fim/issues/144): geopandas read_file not being masked and can be much faster.  This one was included as there were extensive line-by-line testing required for this PR and adding this fix speed up the overall performance. 

**Note**: A previously detected warning saying `PROJ: proj_create_from_database: Cannot find proj.db` was showing up in just a few places. Now it is showing up alot. It does not hurt logic or code. A new issue card has been created for it. [# 145](https://github.com/NOAA-OWP/ras2fim/issues/145).

### Conda Environment Upgrade
**Note** There is a critical upgrade to the ras2fim conda and it is not backwards compatible. To upgrade this particular version, `please following the next steps careful (including step 1)`. 

1) While having ras2fim activated, run _conda list_ . In the right column will tell you where your local packages were loaded from. If you see any that say `pypi` with the exception of "rasterio" and "keepachangelog", `you MUST uninstall that package before continuing`.  e.g. You find an entry already in that list for shapely  that came from pypi.  You need to run _pip uninstall shapely_. Do this for all you see.  Failure to do this will mess up your environment.

2) We need to full uninstall the ras2fim environment, not upgraded it:
   - run: _conda deactivate_  (if you are already activated in ras2fim)
   - run:  _conda remove --name ras2fim --all_
   - Make sure you have downloaded (or merged) this PR (or dev branch).  Ensure you are in that folder in the `ras2fim` folder where the environment.yml is at.
   - run: _conda env create -f environment.yml_  . It might be slow, but 5 to 10 mins is reasonable.
3) Reboot your computer

### Additions  

- `.gitignore`:  Allows us to control which files get uploaded into the repo, but we wanted to make an exception for our new config file.
- `config`
   - `r2f_config.env`:  As mentioned above for Task # 1 (config).

### Changes  

- `src`
    - `calculate_all_terrain_stats.py`:  Changes to match the new panda 2.x series.
    - `conflate_hecras_to_nwm.py`:  Changes including:
        - Changes to match the new panda 2.x series and geopandas
        - The additional of loading the smaller huc8 wbd file which is used as a mask against the large, slow to load WBD_National (watershed boundary dataset). 
        - Fix some variables used for pathing to use os.path.join for ease of readability.
        - replaced dataframe.appends to concat
   - `create_shapes_from_hecras.py`:  Replaced dataframe.appends to concat, plus misc changes due to package upgrades.
   - `ras2fim.py` : Changes including:
       - Added the config system.
       - moved the creation of the "final" directory earlier and copy the config file used into the it for tracking purposes.
       - Reorganized how the "final" folder is populated so that each section can add its own files to "final" as/when needed.
       - Removed the "m" flag to optional run ras2rem as it is now covered by the config file.
       - Add an optional "c" arg to point to an overridden config file.
   - `run_ras2rem.py`:  Changes to match the new panda 2.x series and geopandas.
   - `shared_functions.py`: Fixed some mixed case variable names to lower to match PEP-8 standards.
   - `shared_variables.py`: Fixed some mixed case variable names to lower to match PEP-8 standards.
   - `simplify_fim_rasters.py`: Changes include:
       - Fixed some mixed case variable names to lower to match PEP-8 standards.
       - Changes to match the new panda 2.x series and geopandas.
   - `worker_fim_rasters.py`: Changes include:
       - Fixed some mixed case variable names to lower to match PEP-8 standards.
       - Changes to match the new panda 2.x series and geopandas.
- 'tools'
    - `convert_ras2fim_to_recurr_validation_datasets.py`: Changes to match the new panda 2.x series and geopandas.
    - `nws_ras2fim_post_frequency_grids.py`: Changes to match the new panda 2.x series and geopandas.
      
<br/><br/>

## v1.21.0 - 2023-08-14 - [PR#137](https://github.com/NOAA-OWP/ras2fim/pull/137)

Recently, [PR 135](https://github.com/NOAA-OWP/ras2fim/pull/135) adjusted the ras2fim processing to export HECRAS cross-sections with water surface elevation for the calibration workflow. This PR updates  `compile_ras_rating_curves.py` accommodate the necessary updates in geospatial processing and new column names. It also removes unnecessary raster processing, NWM midpoints calculations, and unused flags to simplify the code. 

### Changes  

- `src/compile_ras_rating_curves.py`:
  - adds code to calculate the intersection points between the NWM lines and the HECRAS cross-sections
  - updated metadata function to import folder names and to have updated csv column descriptions
  - removed unused Python package imports
  - removed redundant or no longer used functionality (function for compiling rating curves (`fn_make_rating_curve`), functionality to extract units from column headers and to deal with mismatched units, NWM feature midpoints calculation, functions for the extraction of elevation from raster at midpoints, raster mosaicking, and the overwrite (`-ov`) and model unit (`-u`) flags)

<br/><br/>

## v1.20.0 - 2023-08-08 - [PR#134](https://github.com/NOAA-OWP/ras2fim/pull/134)

There are a couple of places in the code (Steps 2?? and Step 5) where it calls the hec ras engine. ie) hec = win32com.client.Dispatch("RAS60.HECRASController").  The problem is that if an exception is thrown, hec-ras does not automatically close its windows and windows process threads. This results in a growing collection of orphaned hec-ras processes tasks, that can be seen in task manager. It has also been seen where it can lock up a machine depending on the number of exceptions and number of proc's available on the machine.

Note: Now that some processes are no longer being orphaned, the entire process should run quicker.

Note:  A pre-existing bug, still in place, is when all models fail in Step 5 and no remaining "good" models exist by the time it gets to Step 7. The error is seen as "ValueError: Length mismatch: Expected axis has 0 elements, new values have 11 elements" and will be fixed as part of a separate issue.

### Changes  

- `src`
    - `create_shapes_from_hecras.py`:  Added try, except, finally and ensured the hec-ras ap win calls always called hec.QuitRas(). This triggered a chunk of code indentation.
    - `worker_fim_rasters.py`:  Added try, except, finally and ensured the hec-ras ap win calls always called hec.QuitRas(). This triggered a very chunk of code indentation.

<br/><br/>

## v1.19.0 - 2023-08-09 - [PR#135](https://github.com/NOAA-OWP/ras2fim/pull/135)

By this PR: 
1. water surface elevation (wse) and flow results are compiled for all cross sections 

2. standardization is implemented for rating curves and cross section results. This involves: 
    - A copy of rating curve files (both metric and U.S. units) for individual feature ids is saved in folder `06_metric/Rating_Curve`. 
    - A copy of cross sections files (both metric and U.S. units) for individual feature ids is saved in folder `06_metric/Cross_Section`. 
    - Also, the two files "all_cross_sections.csv" and "all_rating_curves.csv" stacking all individual files are created in `06_metric`

### Changes  
- `src/worker_fim_rasters.py`
1- for each modeled discharge, read wse and flow for all cross sections
2- report the above results in both metric and U.S. unit in a csv file for each feature id. 

- `src/simplify_fim_rasters.py`
1- make rating curve files in folder 06_metric
2- make cross sections files (wse and flow results) in folder 06_metric

- `src/shared_variables.py` 
Added variables to hold the name of new folders "Rating_Curve" and "Cross_Sections" within `06_metric` folder


<br/><br/>

## v1.18.0 - 2023-08-03 - [PR#128](https://github.com/NOAA-OWP/ras2fim/pull/128)

Rating curves were erroring out as a mismatch of number of items in a column with the wse (Water Surface Elevation) data list having one extra element in some scenarios.

Removed some unnecessary debug print lines. 

Plus renamed a few columns in the rating_curves.csv

### Changes  
- `src`
    - `reformat_ras_rating_curve.py`: reflect column names in rating_curve.csv
    - `simplify_fim_rasters.py`:  comment out some unneeded debugging lines.
    - `worker_fim_rasters.py`:  Redo out the formula for calculating the wse data list

<br/><br/>

## v1.17.0 - 2023-08-02 - [PR#126](https://github.com/NOAA-OWP/ras2fim/pull/126)

Adjusts  `worker_fim_rasters.py` to export water surface elevation alongside the stage and discharge in the rating curve. It also changes `reformat_rating_curve.py` so it can accept the water surface elevation and makes the script function properly without ras2rem being run (i.e. it compiles the rating curve, a functionality that used to be run with ras2rem). 

It also fixes some rating curve column values to matching depth grid file names. Most of the challenges were based in rounding at critical points of values.

This PR includes the code updates that were previously described in [PR#85](https://github.com/NOAA-OWP/ras2fim/pull/85), so that PR no longer needs to be approved and merged into dev.

Resolves [Issue #81 ](https://github.com/NOAA-OWP/ras2fim/issues/81), [Issue #95 ](https://github.com/NOAA-OWP/ras2fim/issues/95) and [Issue #119 ](https://github.com/NOAA-OWP/ras2fim/issues/119).


### Changes  
- `src/worker_fim_rasters.py`:
  - Reads in water surface elevation from HEC-RAS cross sections
  - Creates a list of water surface elevation values at the desired increments
  - Added WaterSurfaceElevation to output rating curve dataframe
  - Made some adjustments for rounding issues for the meters and millimeters columns.

- `src/reformat_ras_rating_curve.py`: 
  - Added progress bar to the raster mosaicking and elevation extraction processes.
  - Added explicit CRS to geopackage output 
  - Added function for normalizing unit names  (`get_unit_from_string`) and removes duplicate functionality in other parts of the code
  - Added function from ras2rem for compiling the rating curves within a directory, which had previously been done in ras2rem (`fn_make_rating_curve`)
  - Added update to pull unit, crs, and huc8 name from `run_arguments.txt` file
  - Added WaterSurfaceElevation column to output csv and geopackage
  - Fixed order of inputs given to the executor to run `dir_reformat_ras_rc`
  - Removed hardcoded CRS's, instead using CRS from `run_arguments.txt`  - 

- `src/simplify_fim_rasters.py`:
    - Adjusted code for rounding and how the depth grid file name is created. It now matches the exact logic pattern as the rating_curve.csv cals.

<br/><br/>

## v1.16.1 - 2023-07-19 - [PR#101](https://github.com/NOAA-OWP/ras2fim/pull/101)

This bug fix will check the result of conflation step (step 2) and stop the code if no conflated model exists.

Ras2fim needs at least one conflated model (one HEC-RAS model that its cross sections intersect with NWM reaches). If none of the user provided HEC-RAS models conflates to NWM reaches in the given HUC8, then the code should inform the user and terminate. The file "***_stream_qc.csv" in folder '02_shapes_from_conflation' is the best place to check for this situation.

### Changes
- `src/conflate_hecras_to_nwm.py` 
	called the "errors.check_conflated_models_count()" function to check the number of records in the "***_stream_qc.csv" file.

- `src/errors.py` 
	Added the definition of "errors.check_conflated_models_count()" function

<br/><br/>

## v1.16.0 - 2023-07-31 - [PR#115](https://github.com/NOAA-OWP/ras2fim/pull/115)

This PR covers two items, both are pretty small, and a couple bug fixes.

1) [Issue 108](https://github.com/NOAA-OWP/ras2fim/issues/108) - Standardizing the final huc output folder name
2) [Issue 116](https://github.com/NOAA-OWP/ras2fim/issues/116) - Change default for ras2rem to be false (aka. not run)

Issue 108:
**It will ensure that a user can no longer change the  `output_ras2fim`/huc folder name as before.  In past versions you could change the path and huc folder name, but now the huc folder name is fully calculated, but you can still change the parent path.** 

Issue 116:
The default now will stop just before ras2rem and catchments, but there continues to be a "finalization" step. At the default behavior, the temporarily only current output is a copy of the input argument of the filtered model catalog csv.

During the merge from dev-standardize-outputs, some additional bugs fixes were discovered as well as some required critical enhancements for very near future PRs around `canned polys`. The new `canned poly's` is a significant change in the final outputs from ras2fim.py and will be explained in more details as it is integrated.

3) The pending canned poly's system required rating curves to always have metric columns (discharge_m, stage_m). This existed already in the ras2rem model but needed to be moved to when the rating curve as first created. Why? With the new canned poly system, it will no longer use the ras2rem functionally. Another columned named "stage_mm" was also added which is the same value as "stage_m" but in millimeters instead of meters.

4) Depth grid tif's names were changed to be millimeter based for better precision, they were previously meters (as int) and lots some precision.  e.g. old file name of 1465666-1.tif  is now 1465666-1066.tif.

5) During tracing of a development bug, it was discovered that a bug in a pre-existing partial error logging system. Some run-time errors were recorded the error but no stack trace so we were unable to see where it failed. I added stack tracing info to the error logging as well as printing to screen. Without this fix, it was very difficult to find the error.  Also.. the partial pre-existing logging system, suppresses errors which were not on the screen and were not obvious in its output error csv. When all of my models failed later in the code stack, without this fix, I was super hard to find the suppressed farther up the code stream. Over time, we will continue to upgrade logging. 

6) There were a number of duplicate copies of a function called fn_str2bool, which many were not in use. The one that were in use, became obsolete with a better pattern added when using input arguments for incoming values that are "true / false" in nature.

7) a new validator was created for ensuring valid crs's to added to ras2fim input args. Other places can use this feature, but this can be plugged in later.

### Additions  
- `src`
    -`shared_validators`: A new script which initially includes code to validate crs input.

### Changes  
- `doc`
    - `INSTALL.md`:  Updated notes based on ras2fim.py input change for output folder name
- `src`
   - `clip_dem_from_shape.py`:  remove unused function and small style fixes.
   - `convert_tif_to_ras_hdf5.py`: remove unused function and small style fixes. 
   - `create_fim_rasters.py`: Changed one input to better convention which triggered an unnecessary function.
   - `get_usgs_dem_from_shape.py`: remove unused function and small style fixes. 
   - `ras2fim.py`:  A few changes
        - Change the input for the output path from including the final hec out folder name and path, to now just the path. The output folder name is now calculated using the  pattern of {huc_number}_{crs}_{yymmdd}.  ie) 12090301_2277_230729. This final folder name can no longer be overridden but the folder pathing can (as before).
        - Added code to validate that the incoming crs parameter was valid.
        - Moved the `init_and_run_ras2fim` function to the top of the file versus lower in the code. Why? It is already expected to have other new code call directly to ras2fim.py functions and not through command line arguments. Moving the function higher increases visibility to future coders.
        - Changed the `-m` (run_ras2rem) flag to now be defaulted to `false`, ie). the default behavior is to no longer run ras2rem or catchments code. This also triggers some changes to the "finalization" processes.
    - `run_ras2rem.py`:  remove some code to add metric columns as it was moved up in the code stream.
    - `shared_functions.py`: Added two new functions to help with standardization of code (not output standardization but code standardization)
    - `simplified_fim_rasters.py`:  Changed the name of the depth grid tif output file name.
    - `worker_fim_rasters.py`: A couple of changes:
        - Changed rating curve column name from `Flow(cfs)` and `AvgDepth(ft)` to `discharge_cfs` and `stage_ft`. Note: check code product wide and made changes to match the new column names.
        - Added two new metric columns if the columns don't already exist based on unit. This included foot to metric conversion. 
        - A new column was added named `stage_mm` but which is the `stage_m` column changed to millimeters.
- `tools`
    - `convert_ras2fim_to_recurr_validation_datasets.py`:  Replaced values of `AvgDepth(m)` with `stage_m` and `flow(cms)` to `discharge_cms`.
    - `get_ras_models_by_catalog.py`.  Added a TODO note.

<br/><br/>

## v1.15.0 - 2023-07-24 - [PR#113](https://github.com/NOAA-OWP/ras2fim/pull/113)

By this PR:
1. The results of `simplify_fim_rasters.py` is now saved in output folder `06_metric/Depth_Grid`. Also, if HEC-RAS models units are in feet:
    - Pixel values of the simplified depth grids (z unit) are converted into meter. 
    - File names of resulting depth grids (like "featureid-remvalue.tif") are updated with rem values in meter. 

2. The results of `run_ras2rem.py` is now saved in output folder `06_metric/ras2rem` in which:
    - Rating curve values are in meter and m^3/s
    - `rem.tif` file has projection of EPSG:5070 and pixel values (z unit) are in meter. 

Note that this PR is not addressing standardization for the results of step 9 ( `make_catchments` function), and @RobHanna-NOAA will work on that section, if needed. 

### Changes  
- `src/ras2fim.py` : Passed "model_unit" argument into `fn_simplify_fim_rasters` and `fn_run_ras2rem` functions, so these functions can convert the results into metric when HEC-RAS models unit are in feet.

- `src/simplify_fim_rasters.py` 
  1. The results of this step is now saved in output folder `06_metric/Depth_Grid` 
  2. If model unit is in feet, pixel values of the simplified depth grids (z unit) are converted into meter and file names of resulting depth grids (like "featureid-remvalue.tif") are updated with rem values in meter. 

- `src/run_ras2rem.py` :
  1. The results of ras2rem is now saved in output folder `06_metric/ras2rem`.
  2. If model unit is in feet, rating curve values are converted into meter and rounded into 3 digits. 

- `src/shared_variables.py` 
  1. Changed the CRS of the standardized outputs to be EPSG:5070. 

- `src/shared_functions.py` 
  1. Added a new function called `find_model_unit_from_rating_curves` to get 'model_unit' of HEC-RAS models, if we already have rating curve results from a successful run of ras2fim for step 5 (`fn_create_fim_rasters` function). This is applicable only when a user wants to directly run  `simplify_fim_rasters.py` or `run_ras2rem.py`. 

- `src/ras2catchments.py` : Changed pathing for location of the 
<br/><br/>


## v1.14.0 - 2023-07-20 - [PR#105](https://github.com/NOAA-OWP/ras2fim/pull/105)

One HUC may have models with multiple CRS's, but as this is an input param to ras2fim.py, where ras2fim.py needs to know the incoming CRS, then we need to have get_models_by_catalog have an additional filter to pull records to find records in the master S3 OWP_ras_models_catalog.csv by csv. It already has huc and status as filters.

Also noticed an issue with some of the -h help outputs for some files. So I added the attribute of `metavar-`''` to some files and most argparser objects.
Images of before and after the fix can be seen in the PR Notes.

### Changes  
- `src`
     - `ras2catchments.py`: Added some of the metavar args
     - `ras2fim.py`: Added some of the metavar args and did a small bit of style adj.
     - `run_ras2rem.py`: Added some of the metavar args

- `tools`
    - `get_ras_models_by_catalog.py`: Changes include:
        - adding the new csv required input arg
        - re-ordered the argparser parser objects and added the new metavar arg.
        - Change the input arg for `list-only` from `-d` to `-f`

<br/><br/>

## v1.13.1 - 2023-07-24 - [PR#103](https://github.com/NOAA-OWP/ras2fim/pull/103)

Previously, ras2fim could only accommodate projections in the EPSG format. This PR is a quick fix to how the projection is processed to accommodate projection codes in the ESRI format.

### Changes  

- `src/ras2fim.py`: Replaced `CRS.from_epsg` with `CRS.from_string` in order to accommodate ESRI projections.
- `src/shared_functions.py`: Edited error message for clarity.

<br/><br/>


## v1.13.0 - 2023-07-06 - [PR#93](https://github.com/NOAA-OWP/ras2fim/pull/93)

Add multi processing when calculating `maxments` for each feature ID. 

A `maxment` is the maximum depth for any feature within a catchments.

Also a bug in rasterio displays an error occasionally of "PROJ: proj_create_from_database: Cannot find proj.db".  This is a known issue due to each users computer environment (not ras2fim environment).  When adding multi-proc, this error was seen for each instance of multi proc. Code was added to minimize the error. It will now display once and only once when running ras2catchments.py (not always seen as part of ras2fim.py).

### Changes  

- `README.md`  - small updates for the new ESIP links and a bit of text adjustments.
- `doc`
    - `CHANGELOG.md` - corrected a couple of critical notes in the 1.9.0 release. (plus added new notes for this release of course).
    - `INSTALL.md` - Updated to show that we need to use `Anaconda Powershell Terminal Window` and not the non powershell version of Anaconda.
- `src`
    - `shared_functions.py`:  A few changes where made:
       - ` def convert_to_metric` method moved from ras2catchments to shared_functions.
       - Added to functions to help with the "cannot fine proj.db" error mentioned above.
     - `ras2catchments.py`: Changes include:
         - a little cleanup of the imports section
         - moved the "convert_to_metric" function to `shared_functions.py` for reusability.
         - added multi proc and a TQDM progress bar when getting maxments.
         - added a "is_verbose" flag arg and code for additional output for debugging. This is a common feature in the fim dev (inundation mapping) code and will hopefully can be helpful here as well.

<br/><br/>

## v1.12.1 - 2023-06-29 - [PR#90](https://github.com/NOAA-OWP/ras2fim/pull/90)

When the -m flag ( skip ras2rem), is set to False, then it should abort processing just before ras2rem and not continue on to catchments and finalization.

Also found bug when the default `output_ras2fim` does not exist. This is only discovered if the user has not overridden the pathing for output_ras2fim. Remember, the -o flag can be used in two ways. 
1. use just the new desired output folder name without pathing, which will default pathing to c:\ras2fim_data\outputs_ras2fim'
2. full override the -o flag to any fully pathed location. When using this option, the parent path must exist, but the child folder can not exist. 

Note: There is a bug in other parts of this code base that are being fixed as part of a different PR. Currently, it will fail as it attempts to process catchments. But to validate that this PR passes will be based on stopping processing before ras2rem or continuing on to ras2rem (and forward) and it will fail in catchments.

### Changes  

- `src`
    - `ras2fim.py`:  Fix to skip catchments and finalization as describe above plus fixed discovered and mentioned above.

<br/><br/>

## v1.12.0 - 2023-07-04 - [PR#92](https://github.com/NOAA-OWP/ras2fim/pull/92)

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

## v1.11.0 - 2023-06-29 - [PR#87](https://github.com/NOAA-OWP/ras2fim/pull/87)

New feature: This is a new tool which is for OWP staff only as it accesses our ras2fim S3 bucket. We can give the tool a HUC number, it will go to the models catalog csv in s3, look for valid matching models for that HUC and downloaded them to your local computer.

Some criteria for downloading are:
- The submitted HUC number exists in the OWP_ras_models_catalog.csv (previously named model_catalog.csv), in the "hucs" column
- The models catalog `status` column must say "ready" and the `final_name_key` must not start with "1_" or "2_"

When the code calls s3, it will load up the full s3 OWP_ras_models_catalog.csv (or arg override s3 csv file), and filter out all non-qualifying records, then save a copy of the new filtered version as `OWP_ras_models_catalog_{huc number}.csv`.  This csv file can now become one of the input args going into ras2fim.py (-mc flag).

Features of this tool are:
- You can add an argument of -d which gives you the downloaded filtered csv but does not actually download the model folders. It is a form of a "preview" system.
- If you choose to download model folders as well, the newly created `OWP_ras_models_catalog_{huc number}.csv` will add two columns to show if the model was successfully downloaded or why it failed.  As we are using the `final_name_key` to tell us which S3 model folder to download, it is possible that they don't match or the model folder does not exist. This will expose that problem.
- An processing log file is created which at this point, is nearly identical to screen output but allows for historical tracking as well as ease of automation later.
- At the end of ras2fim.py finalization, it will make a copy of the new `OWP_ras_models_catalog_{huc number}.csv` and put it in the huc ras2fim output folder, "final" folder e.g. output folder.
- As always, you can optionally override most arguments and most things are defaulted.

See `tools\get_ras_models_by_catalog.py`, in the "main" section to see usage examples.

**Note**: To use this tool, you must have a valid set of aws s3 ras2fim credentials which have been set up, generally by using `aws configure` command.  As mentioned, this tool is for OWP staff.

### Additions  

- `tools`
    - `get_ras_models_by_catalog.py`: as described above.

### Changes  
- `src`
   - `ras2catchments.py`: renamed a constants variable name.
   - `shared_variables.py`: renamed a couple of constants variable names.
   - `ras2fim.py`: renamed a couple of constants variable names.
>>>>>>> 85334574faf0ca5c742523d93a7754b2a4b1f522

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
This version requires an update to the conda environment for ras2fim.  To upgrade, follow these steps (using an Anaconda / Mini Conda Powershell command terminal):
1) If you are already in a conda activateed ras2fim environment, type `conda deactivate`
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
