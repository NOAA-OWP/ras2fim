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

## Default Folder Structure

While ras2fim.py and other tools have optional parameters allowing pathing to any folder(s), we do have a recommended folder structure based on c: drive.

![ras2fim default folder structure image](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/default_folder_structure.png)

All documentation in this repo are based on the default folder structure.

## Downloading Data from ESIP

<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/esip-logo.png" align="right" alt="esip logo" height="50">There are folders and files that will need to be downloaded locally prior to running the RAS2FIM code or viewing samples.  Thsse data can be found in an Amazon S3 Bucket hosted by [Earth Science Information Partners (ESIP)](https://www.esipfed.org/). These data can be accessed using the AWS Command Line Interface (CLI) tools. AWS CLI install information described below.  The ESIP / NOAA NWS OWP FIM S3 Bucket (`s3://noaa-nws-owp-fim`) is set up as allow for anonymous free downloads.<br>

### Configuring the AWS CLI

To download any folders or files from 
1. [Install AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. [Configure AWS CLI tools](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### To test AWS CLI and access to ESIP

To list folders prior to download
```
aws s3 ls s3://noaa-nws-owp-fim --no-sign-request
```


## Output Samples
If you want to review what a sample output for a ras2fim.py processing run looks like, you can dowload a folder that was generated using five models in the HUC8 of 12090301.
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/ras2fim/output_ras2fim C:\ras2fim_data\output_ras2fim --no-sign-request
```

## Prior to Running the Code (if you choose to do some test processing)

To do some test processing, you will download additional ESIP folders which include the `inputs`, and the `OWP_ras_models` folders. We have provided a small sample set of five `models` based on the HUC8 of 12090301.  The `model` folders are folders, one per model, that has gone through preprocessing steps to convert the raw HEC-RAS data from providers such as BLE, and adjusted to be ready to processed via `ras2fim.py`. ras2fim.py will create output rating curves, REMs and other output files.

While not yet determined, we may publish more `models` later, however, you are also welcome to create our own `models` and use the `ras2fim.py` tools.

The OWP tools to preprocess HECRAS data to OWP_ras_models is not yet available.


### (1) Get AWS Folder - Inputs
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/AWS_logo.png" align="right" alt="aws logo" height="50">
It will download files and folders into a subfolder named `X-National_datasets` (14.3 Gb)
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/ras2fim/inputs c:\ras2fim_data\inputs --no-sign-request
```
This download will include the following files / folder:
1.  Watershed Boundary Dataset (WBD): WBD_National.gpkg
2.  The WBD_National.gkpg split into different gkpg files by HUC8: /WBD_HUC8/*
3.  National Water Model (NWM) Flowline Hydrofabric: nwm_flows.gpkg
4.  National Water Model to Watershed Boundary Lookup: nwm_wbd_lookup.nc
5.  National Water Model (NWM) Catchments file: nwm_catchments.gpkg

** Note:** A simple polygon for most HUC8s exist in the /WBD_HUC8 folder. You can download only the HUC8_{huc8 number}.gkpg of your choice(s) if you like. We have load most for your convenience.

### (2) Get AWS Folder - OWP_ras_models folder and OWP_ras_models_catalog.csv

At this point, ras2fim.py needs a file named OWP_ras_models_catalog.csv and we have loaded a sample for you. It has some meta data that is used in the final output folders. While the file must exist with the correct schema, it will not fail if record in it do not match.  This file may become optional at a later point, but for now, please include it and also add the `-mc` argument to `ras2fim.py`. eg. `-mc c:\ras2fim_data\OWP_ras_models\OWP_ras_models_catalog.csv` (or pathing of your choice of course, as is with most arguments.)

To download the `OWP_ras_models` folder, you AWS CLI command will be (adjusting for path overrides if you like):
```
aws s3 cp --recursive s3://noaa-nws-owp-fim/ras2fim/OWP_ras_models c:\ras2fim_data\OWP_ras_models --no-sign-request
```

<br><br>

### (3) Install HEC-RAS verion 6.0
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/RAS_logo.png" align="right" alt="hec-ras logo" height="80">These RAS2FIM scripts are written to utilize the computational engine and supporting APIs from the U.S Army Corp of Engineers' [Hydrologic Engineering Center's River Analysis System {HEC-RAS}](https://www.hec.usace.army.mil/software/hec-ras/).  Download and install **HEC-RAS version 6.0** to your local machine.  Note: **the version (6.0) matters!**<br><br>The install package can be downloaded [here](https://github.com/HydrologicEngineeringCenter/hec-downloads/releases/download/1.0.20/HEC-RAS_60_Setup.exe) or [from the USACE website](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).Once installed, **open HEC-RAS on that machine** to accept the terrms and conditions and ensure that it will function on that machine prior to running any RAS2FIM scripts.  Close HEC-RAS.


### (4) Clone the Git-hub repository
<img src="https://github.com/NOAA-OWP/ras2fim/blob/main/doc/Git_logo.png" align="right" alt="git logo" height="80"> Install [git](https://git-scm.com/downloads) onto your Windows machine. Next, clone this ras2fim reporitory on to your Windows machine.
```
git clone https://github.com/NOAA-OWP/ras2fim.git
```

### (5) Building and Testing ras2fim

Detailed instructions on setting up an anaconda environment and running the main ras2fim script is in a this separate [INSTALL](doc/INSTALL.md) document


----
## Dependency Sources

* [HEC-RAS Version 6.0](https://www.hec.usace.army.mil/software/hec-ras/download.aspx).
* [Anaconda](https://www.anaconda.com/products/individual) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) for Windows.
* National datasets - from AWS - See "Get AWS Folder - Inputs" section above.
* Runs on a Windows OS only - Tested on Windows 10.
* Tested on HEC-RAS 6.0 and default pathing is also set against v6.0.








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


## Limitations and Assumptions

Details coming soon.

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




