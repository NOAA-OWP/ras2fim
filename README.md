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
There are three (3) **"National Datasets"** that will need to be downloaded locally prior to running the RAS2FIM code.  These input data can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS Command Line Interface CLI tools.  This S3 Bucket (`s3://noaa-nws-owp-fim`) is set up as a "Requester Pays" bucket. Read more about what that means [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).<br>
### Configuring the AWS CLI
1. [Install AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. [Configure AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### Get National Datasets
1.  Watershed Boundary Dataset (WBD)
     - List file on AWS: `aws s3 ls s3://noaa-nws-owp-fim/hand_fim/fim_3_0_21_0/inputs/wbd/WBD_National.gpkg --request-payer requester`
3.
4.
## Dependencies

Describe any dependencies that must be installed for this software to work.
This includes programming languages, databases or other storage mechanisms, build tools, frameworks, and so forth.
If specific versions of other software are required, or known not to work, call that out.

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

This section should detail why people should get involved and describe key areas you are
currently focusing on; e.g., trying to get feedback on features, fixing certain bugs, building
important pieces, etc.

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
