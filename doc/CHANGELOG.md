All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.


## v1.4.0 - 2023-05-03 - [PR#43](https://github.com/NOAA-OWP/ras2fim/pull/43)

This program allows for a user to provide an S3 path and a HUC, have it query the remote models catalog csv and download "unprocessed" folders to their local machine for processing with ras2fim.py.

Key Notes
- This tool is designed for working with OWP S3 and will likely not have value to non NOAA staff, unless they have their own AWS account with a similar structure.  However, ras2fim.py will continue to work without the use of this tool
- Review the arguments being passed into `get_ras_mdoels.py` file which include defining your own `models catalog csv` file but it will default to `models_catalog.csv`.  ie). testing, it could be `models_catalog_rob.csv`.
- New dependency python packages were added. 

Associated to issue [39](https://github.com/NOAA-OWP/ras2fim/issues/39). 

### Additions  
- `data/aws`
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
- Filters downloads from the src models catalog to look for status of "unprocessed" only. Also filters out model catalog final_key_names starting with "1_" one underscore.
- Can find huc numbers in the models catalog "hucs" field regardless of string format in that column.  It does assume that models catalog has ensure that leading zero's exist.
- All folders in the target models folder will be removed at the start of execution unless the folder starts with two underscores. This ensures old data from previous runs does not bleed through. 
- After the source model catalog has been filtered, it will save a copy called models_catalog_{YYYYMMDD}.csv into the target output folder.  Later this csv can be feed into ras2fim.py for meta data for each model.

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
