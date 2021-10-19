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

### Get National Datasets
<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/AWS_logo.png" align="right" alt="aws logo" height="50"> Sample - List file prior to download:  `aws s3 ls s3://noaa-nws-owp-fim/hand_fim/fim_3_0_21_0/inputs/wbd/WBD_National.gpkg --request-payer requester`

1.  Watershed Boundary Dataset (WBD) `aws s3 cp s3://noaa-nws-owp-fim/hand_fim/fim_3_0_21_0/inputs/wbd/WBD_National.gpkg WBD_National.gpkg --request-payer requester`<br><br>
2.  National Water Model (NWM) Flowline Hydrofabric: `aws s3 cp s3://noaa-nws-owp-fim/hand_fim/fim_3_0_21_0/inputs/nwm_hydrofabric/nwm_flows.gpkg nwm_flows.gpkg --request-payer requester`<br><br>
3.  National Water Model to Watershed Boundary Lookup `aws s3 cp s3://noaa-nws-owp-fim/ras2fim/national_inputs/nwm_wbd_lookup.nc nwm_wbd_lookup.nc --request-payer requester`<br><br>

### Install HEC-RAS verion 6.0
<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/RAS_logo.png" align="right" alt="hec-ras logo" height="80">These RAS2FIM scripts are written to utilize the computational engine and supporting APIs from the U.S Army Corp of Engineers' [Hydrologic Engineering Center's River Analysis System {HEC-RAS}](https://www.hec.usace.army.mil/software/hec-ras/).  Download and install **HEC-RAS version 6.0** to your local machine.  <br><br>The install package can be downloaded [here](https://github.com/HydrologicEngineeringCenter/hec-downloads/releases/download/1.0.20/HEC-RAS_60_Setup.exe) or [from the USACE website](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).Once installed, **open HEC-RAS on that machine** to ensure that it will function on that machine and will be usable by the RAS2FIM scripts.  Close HEC-RAS.

----
## Dependencies

* [HEC-RAS Version 6.0](https://www.hec.usace.army.mil/software/hec-ras/download.aspx)
* [Anaconda](https://www.anaconda.com/products/individual)
* National datasets - See "Prior to Running the Code" section
* Runs on a Windows OS only -Tested on Windows 10

## Installation

Detailed instructions on how to install, configure, and get the project running.
This should be frequently tested to ensure reliability. Alternatively, link to
a separate [INSTALL](INSTALL.md) document.

## Configuration

If the software is configurable, describe it in detail, either here or in other documentation to which you link.

## Usage

Show users how to use the software.
Be specific.
Use appropriate formatting when showing code snippets.

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

General instructions on _how_ to contribute should be stated with a link to [CONTRIBUTING](CONTRIBUTING.md).


----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)


----

## Credits and references

1. Projects that inspired you
2. Related projects
3. Books, papers, talks, or other sources that have meaningful impact or influence on this project
