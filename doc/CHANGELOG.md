All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.

## v1.1.pending - 2023-05-01 - [PR#29](https://github.com/NOAA-OWP/ras2fim/pull/29)

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

## v1.1.0 - 2023-04-11 - [PR#31](https://github.com/NOAA-OWP/ras2fim/pull/31)

This merge adds a new Pull Request template that was copied from the `inundation-mapping` project PR template.

### Changes
- `/.github/PULL_REQUEST_TEMPLATE.md`: updates this file to match the one used by `inundation-mapping` repository.

<br/><br/>

## 1.0.0 - 2021.11.10 - Initial Release
