All notable changes to this project will be documented in this file.
We follow the [Semantic Versioning 2.0.0](http://semver.org/) format.

## v1.2.0 - 2023-04-21 - [PR#33](https://github.com/NOAA-OWP/ras2fim/pull/33)
This PR adds `ras2rem` functioanlity into `ras2fim`. For each NWM stream within the domain of HEC RAS models, `ras2fim` produces flood depth grids for a common set of reach averaged-depths. `ras2rem` merges all these depth grids and create a single raster file delineating the areas inundated with different values of reach averaged-depth.

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
