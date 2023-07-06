# RAS2FIM <img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/ras2fim_agency_20211018.png" align="right" alt="ras2fim agency" height="80"> <br> <br>
## <i>Creation of flood inundation raster libraries and rating curves from HEC-RAS models </i>

<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/ras2fim_logo_20211018.png" align="right"
     alt="ras2fim logo" width="160" height="160">

**Description**:  Starting with geospatially attributed one-dimensional HEC-RAS floodplain models, these scripts are used to create a library of flood depth inundation rasters for a range of storm water discharges (flow).  HEC-RAS models are cut to roughly match the limits of the [National Water Model's {NWM}](https://water.noaa.gov/about/nwm) stream designations (hydrofabric).  For each matching NWM stream, a synthetic rating curve is created based on 'reach averaged' flood depths as determined from the HEC-RAS simulations.  The intent it to create a library of flood depth inundation grids with a ccorresponding rating curve that can be paired with the National Water Model's discharges determination and forecasting to create real-time and predictive floodplain mapping from a detailed HEC-RAS 1-D model.

  - **Technology stack**: Scripts were all developed in Python 3.8.11.  Use is intended within a custom 'ras2fim' [Anaconda environment](https://www.anaconda.com/products/individual) running on a Windows OS.  Prior to running these scripts, the user is required to install and run [HEC-RAS v 6.0](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).<br><br>
  - **Status**:  Version 1.0 - Inital release.  Refer to to the [CHANGELOG](CHANGELOG.md).<br><br>
  - **Overview Video**: [Link to overview video of ras2fim](https://www.youtube.com/watch?v=TDDTRSUplVA)<br><br>
  - **Related Project**:  Inspiration for this repository was to develop flood inundation map libraries to replace Height Above Nearest Drainage (HAND) as calculated with the [Cahaba](https://github.com/NOAA-OWP/cahaba) repository where detailed HEC-RAS models exist.

**RAS2FIM Wiki**:
More detail regarding RAS2FIM is located on the project's Wiki page.
<p align="center">
<a href="https://github.com/NOAA-OWP/ras2fim/wiki">
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/button_go-to-rasfim-wiki.png" alt="Go To Wiki" width="342">
</a>
</p>

**Overview**:
![](https://github.com/NOAA-OWP/ras2fim/blob/main/doc/ras2fim_overview.png)
![](https://github.com/NOAA-OWP/ras2fim/blob/main/doc/ras2fim_sample_output.png)

## Prior to Running the Code
### Input Data
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/esip-logo.png" align="right" alt="esip logo" height="50">There are a folder named **"Inputs"** that will need to be downloaded locally prior to running the RAS2FIM code.  These input data can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS Command Line Interface CLI tools.  This S3 Bucket (`s3://noaa-nws-owp-fim`) is set up as a "Requester Pays" bucket. Read more about what that means [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html).<br>
### Configuring the AWS CLI
1. [Install AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. [Configure AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### (1) Get AWS Folder - National Datasets
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/AWS_logo.png" align="right" alt="aws logo" height="50"> List folder prior to download:  
```
aws s3 ls s3://noaa-nws-owp-fim/ras2fim/inputs --no-sign-request
```
Download National Datasets: (3.82 Gb)
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/ras2fim/inputs inputs --no-sign-request
```
This download will include the following files / folder:
1.  Watershed Boundary Dataset (WBD): WBD_National.gpkg (1.65 Gb)
2.  The WBD_National.gkpg split into different gkpg files by HUC8: /WBD_HUC8/*
3.  National Water Model (NWM) Flowline Hydrofabric: nwm_flows.gpkg (1.80 Gb)
4.  National Water Model to Watershed Boundary Lookup: nwm_wbd_lookup.nc (372 Mb)
5.  National Water Model (NWM) Catchments file: nwm_catchments.gpkg (9.9 GB)
<br><br>

### (2) Install HEC-RAS verion 6.0
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/RAS_logo.png" align="right" alt="hec-ras logo" height="80">These RAS2FIM scripts are written to utilize the computational engine and supporting APIs from the U.S Army Corp of Engineers' [Hydrologic Engineering Center's River Analysis System {HEC-RAS}](https://www.hec.usace.army.mil/software/hec-ras/).  Download and install **HEC-RAS version 6.0** to your local machine.  Note: **the version (6.0) matters!**<br><br>The install package can be downloaded [here](https://github.com/HydrologicEngineeringCenter/hec-downloads/releases/download/1.0.20/HEC-RAS_60_Setup.exe) or [from the USACE website](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).Once installed, **open HEC-RAS on that machine** to accept the terrms and conditions and ensure that it will function on that machine prior to running any RAS2FIM scripts.  Close HEC-RAS.

### (3) Clone the Git-hub repository
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/Git_logo.png" align="right" alt="git logo" height="80"> Install `git` onto your Windows machine.  Clone this ras2fim reporitory on to your Windows machine.
```
git clone https://github.com/NOAA-OWP/ras2fim.git
```

----
## Dependencies

* [HEC-RAS Version 6.0](https://www.hec.usace.army.mil/software/hec-ras/download.aspx)
* [Anaconda](https://www.anaconda.com/products/individual) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) for Windows
* National datasets - from AWS - See "Prior to Running the Code" section
* Runs on a Windows OS only - Tested on Windows 10
* Tested on HEC-RAS 6.0 and default pathing is also set against v6.0.

## Installation

### (4) Create a new anaconda environment
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/Conda-Logo.jpg" align="right" alt="conda logo" height="80"><br>
Detailed instructions on setting up an anaconda environment and running the main ras2fim script is in a this separate [INSTALL](doc/INSTALL.md) document

## Usage

With the (1) ras2fim anaconda environment created and (2) the ras2fim git-hub folder cloned, `ras2fim` python scripts are within the `src` folder.  The main scripts is titled `ras2fim.py`.  **All scripts have a helper flag of `-h`**.  It is recommended that you run the script with the helper flag first to determine the required input. <br><br>
![](https://github.com/NOAA-OWP/ras2fim/blob/main/doc/conda_python_run.png)
** Image may be out of date slightly as parameters are being adjusted currently. Use the `-h` flag and also read the sample usage notes near the bottom of `ras2fim.py`.
<br><br>
Note: For this script there are **three (3) required** arguments and a number of optional arguments.  While this input string will greatly vary on your machine, below is a sample input string to execute the `ras2fim.py` script (with most arguments defaulted).
```
python ras2fim.py -w 12090301 -p EPSG:2277 -o 12090301_meters_2277
```

## How to test the software

<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/AWS_logo.png" align="right" alt="aws logo" height="50"> A sample input and output folder is provided for testing the application. From an AWS S3 bucket, a `sample-dataset` folder is provided.  It includes both sample input and sample output data.

List folder prior to download:  
```
aws s3 ls s3://noaa-nws-owp-fim/ras2fim/sample-datasets --no-sign-request
```
Download Sample Input and Output datasets: (212 Mb)
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/ras2fim/sample-dataset sample-datasets --no-sign-request
```
A video, showing the use of these sample data with the `ras2fim` scripts is provided.

[![Starting with ras2fim](https://img.youtube.com/vi/TDDTRSUplVA/0.jpg)](https://www.youtube.com/watch?v=TDDTRSUplVA)

## Known Issues & Getting Help

Please see the issue tracker on GitHub and the [Ras2Fim Wiki](https://github.com/NOAA-OWP/ras2fim/wiki) for known issues and getting help.

## Getting involved

NOAA's National Water Center welcomes anyone to contribute to the RAS2FIM repository to improve flood inundation mapping capabilities. Please contact Brad Bates (bradford.bates@noaa.gov) or Fernando Salas (fernando.salas@noaa.gov) to get started.

----

## Open source licensing info
1. [TERMS](doc/TERMS.md)
2. [LICENSE](LICENSE)


----

## Credits and references

1. [Office of Water Prediction (OWP)](https://water.noaa.gov/)
2. [Goodell, C. R. (2014). Breaking the Hec-Ras Code: A User’s Guide to Automating Hec-Ras. H2ls.](https://www.kleinschmidtgroup.com/breaking-the-hec-ras-code-2/)
3. [Executive Summary, & Guidance, S. (n.d.). InFRM Flood Decision Support Toolbox. Usgs.Gov. Retrieved October 22, 2021](https://webapps.usgs.gov/infrm/pubs/FDST%20Map%20Submission%20Guidelines%20_vDec20.pdf)
4. [Collete, A. (2013). Python and HDF5: Unlocking Scientific Data. O’Reilly Media.](https://www.oreilly.com/library/view/python-and-hdf5/9781491944981/)
5. [Board on Earth Sciences and Resources/Mapping Science Committee, Committee on FEMA Flood Maps, Mapping Science Committee, Board on Earth Sciences & Resources, Water Science and Technology Board, Division on Earth and Life Studies, & National Research Council. (2009). Mapping the zone: Improving flood map accuracy. National Academies Press.](https://www.amazon.com/Mapping-Zone-Improving-Resilience-Preparedness/dp/0309130573)
6. [Dysarz, Tomasz. (2018). Application of Python Scripting Techniques for Control and Automation of HEC-RAS Simulations. Water. 10. 1382. 10.3390/w10101382. ](https://www.mdpi.com/2073-4441/10/10/1382)
7. [User’s Manual. (n.d.). River Analysis System. Army.Mil.](https://www.hec.usace.army.mil/software/hec-ras/documentation/HEC-RAS_6.0_Users_Manual.pdf)

**Special Thanks to:** Cam Ackerman (US Army Corp of Engineers), Kristine Blickenstaff (US Geological Survey), Chris Goodell (Kleinschmidt Associates), Witold Krajewski (Iowa Flood Center), RoseMarie Klee (Texas Department of Transportation), David Maidment (University of Texas), Saul Nuccitelli (Texas Water Development Board), Paola Passalacqua (University of Texas), Jason Stocker (US Geological Survey), Justin Terry (Harris County Flood Control District)




