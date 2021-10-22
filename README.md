# RAS2FIM <img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/ras2fim_agency_20211018.png" align="right" alt="ras2fim agency" height="80"> <br> <br>
## <i>Creation of flood inundation raster libraries and rating curves from HEC-RAS models </i>

<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/ras2fim_logo_20211018.png" align="right"
     alt="ras2fim logo" width="160" height="160">

**Description**:  Starting with geospatially attributed one-dimensional HEC-RAS floodplain models, these scripts are used to create a library of flood depth inundation rasters for a range of storm water discharges (flow).  HEC-RAS models are cut to roughly match the limits of the [National Water Model's {NWM}](https://water.noaa.gov/about/nwm) stream designations (hydrofabric).  For each matching NWM stream, a synthetic rating curve is created based on 'reach averaged' flood depths as determined from the HEC-RAS simulations.  The intent it to create a library of flood depth inundation grids with a ccorresponding rating curve that can be paired with the National Water Model's discharges determination and forecasting to create real-time and predictive floodplain mapping from a detailed HEC-RAS 1-D model.

  - **Technology stack**: Scripts were all developed in Python 3.8.11.  Use is intended within a custom 'ras2fim' [Anaconda environment](https://www.anaconda.com/products/individual) running on a Windows OS.  Prior to running these scripts, the user is required to install and run [HEC-RAS v 6.0](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).<br><br>
  - **Status**:  Version 1.0 - Inital release.  Refer to to the [CHANGELOG](CHANGELOG.md).<br><br>
  - **Overview Video**: [Link to overview video of ras2fim]()<br><br>
  - **Related Project**:  Inspiration for this repository was to develop flood inundation map libraries to replace Height Above Nearest Drainage (HAND) as calculated with the [Cahaba](https://github.com/NOAA-OWP/cahaba) repository where detailed HEC-RAS models exist.


**Overview**:
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/ras2fim_overview.png)
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/ras2fim_sample_output.png)

## Prior to Running the Code
### Input Data
<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/esip-logo.png" align="right" alt="esip logo" height="50">There are three (3) **"National Datasets"** that will need to be downloaded locally prior to running the RAS2FIM code.  These input data can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS Command Line Interface CLI tools.  This S3 Bucket (`s3://noaa-nws-owp-fim`) is set up as a "Requester Pays" bucket. Read more about what that means [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).<br>
### Configuring the AWS CLI
1. [Install AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. [Configure AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### Get AWS Folder - National Datasets
<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/AWS_logo.png" align="right" alt="aws logo" height="50"> List folder prior to download:  
```
aws s3 ls s3://noaa-nws-owp-fim/ras2fim/national-datasets --request-payer requester
```
Download National Datasets: (3.82 Gb)
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/ras2fim/national-datasets X-National_Datasets --request-payer requester
```
This download will include the following files:
1.  Watershed Boundary Dataset (WBD): WBD_National.gpkg (1.65 Gb)
2.  National Water Model (NWM) Flowline Hydrofabric: nwm_flows.gpkg (1.80 Gb)
3.  National Water Model to Watershed Boundary Lookup: nwm_wbd_lookup.nc (372 Mb)
<br><br>

### Install HEC-RAS verion 6.0
<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/RAS_logo.png" align="right" alt="hec-ras logo" height="80">These RAS2FIM scripts are written to utilize the computational engine and supporting APIs from the U.S Army Corp of Engineers' [Hydrologic Engineering Center's River Analysis System {HEC-RAS}](https://www.hec.usace.army.mil/software/hec-ras/).  Download and install **HEC-RAS version 6.0** to your local machine.  Note: **the version (6.0) matters!**<br><br>The install package can be downloaded [here](https://github.com/HydrologicEngineeringCenter/hec-downloads/releases/download/1.0.20/HEC-RAS_60_Setup.exe) or [from the USACE website](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).Once installed, **open HEC-RAS on that machine** to accept the terrms and conditions and ensure that it will function on that machine prior to running any RAS2FIM scripts.  Close HEC-RAS.

----
## Dependencies

* [HEC-RAS Version 6.0](https://www.hec.usace.army.mil/software/hec-ras/download.aspx)
* [Anaconda](https://www.anaconda.com/products/individual) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) for Windows
* National datasets - from AWS - See "Prior to Running the Code" section
* Runs on a Windows OS only -Tested on Windows 10

## Installation

Detailed instructions on cloning the git-hub repository, setting up an anaconda environment and running the main ras2fim script is in a separate [INSTALL](INSTALL.md) document

## Usage

With the (1) ras2fim anaconda environment created and (2) the ras2fim git-hub folder cloned, `ras2fim` python scripts are within the `src` folder.  The main scripts is titled `ras2fim.py`.  **All scripts have a helper flag of `-h`**.  It is recommended that you run the script with the helper flag first to determine the required input. <br><br>
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_python_run.png)
<br><br>
Note: For this script there are **seven (7) required** inputs and one (1) optional input.  While this input string will greatly vary on your machine, provided is a sample input string to execute the `ras2fim.py` script.
```
python ras2fim.py -w 10170204 -i C:\HEC\output_folder -p EPSG:26915 -p False -n E:\X-NWS\X-National_Datasets -r "C:\Program Files (x86)\HEC\HEC-RAS\6.0"
```

## How to test the software

If the software includes automated tests, detail how to run those tests.

## Known issues

Document any known significant shortcomings with the software.

## Getting help

Instruct users how to get help with this software; this might include links to an issue tracker, wiki, mailing list, etc.

**Example**

If you have questions, concerns, bug reports, etc, please file an issue in this repository's Issue Tracker.

## Getting involved

NOAA's National Water Center welcomes anyone to contribute to the RAS2FIM repository to improve flood inundation mapping capabilities. Please contact Brad Bates (bradford.bates@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)


----

## Credits and references

1. Projects that inspired you
2. Related projects
3. Books, papers, talks, or other sources that have meaningful impact or influence on this project
