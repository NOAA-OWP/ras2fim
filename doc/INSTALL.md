## Creating a RAS2FIM environment

<img src="https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/ras2fim_logo_20211018.png" align="right"
     alt="ras2fim logo" width="120" height="120">
     
You will need to set up an Anaconda/Python environment for developing `ras2fim` that is separate from the "default" environment that you use in your own work/research.  This will allow you to utilize `ras2fim` without worrying about corrupting the Anaconda/Python environment on which your other work depends.

Below are instructions for building a separate development environment for using the `ras2fim` package using the [Conda](http://conda.pydata.org/docs/index.html) package management system.

### Step 1:
Download and Install [Anaconda](https://www.anaconda.com/products/individual) to your machine.<br>
<br>

### Step 2:
If you have not already done so, ensure `git` has been installed and clone a copy of the `ras2fim` repository on to your local machine. Path to your windows folder of choice, then run:<br>
```
    git clone https://github.com/NOAA-OWP/ras2fim.git
```
### Step 3:
Open an **Anaconda Powershell Prompt** and navigate to the cloned directory.  Your path may vary.<br>
![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_prompt.png)

### Step 4:
Next create the `ras2fim` conda environment from the cloned `environment.yml`.<br>
```
conda env create -f environment.yml
```
![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_create_env.png)

### Step 5:
You will now need some data.

If you have not already done so, you will need to downloaded some files/folders from ESIP. You now need the `inputs` and `OWP_ras_models` folders as mentioned in the [README](../README.md). Downloading these two folders will give you some default data that is required. Of course, you are welcome to override or experiment with some of this data as you see fit.
<br>

Here are some notes describing the two folders.

- `inputs`:  This folder is for non HECRAS data that is required for processing HECRAS models. 
  
- `OWP_ras_models\models`: While also optional, you are encouraged to move each of your models that have been pre-processed into the `OWP_ras_models\models`. Note: When you run `ras2fim.py`, each of your HEC-RAS models likely have there own folder, and you can put as many model folders as you like in the `models` folder. When you run `ras2fim.py` it will load in all data across all files and folders inside the `models` folder (location overrideable). Remember, you can put your model folders anywhere you like, this is just the default pattern.<br>
ie)<br>
![ras2fim default models folder structure image](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/default_models_folder_structure.png)
<br><br>

### --- You are now ready to start processing ras2fim models ---
<br>

## Usage

### Step 6
Each time you want to run ras2fim.py or other tools, you need to activate your ras2fim conda environment.
```
conda activate ras2fim
```
![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_activate.png)

### Step 7:

To begin processing, change directories into the source git code `src` subfolder.<br>

![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_src.png)


The main script is titled `ras2fim.py`.  **All scripts have a helper flag of `-h`**.  It is recommended that you run the script with the helper flag first to determine the required input. Also read the sample usage notes near the bottom of `ras2fim.py` code file.<br><br>
![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_python_run.png)
<br>
***--- Image may be out of date slightly as parameters are being adjusted currently ---***
<br><br>
**Note**: Currently, there are **three (3) required** arguments and a number of optional arguments. Below is a sample input string to execute the `ras2fim.py` script with most arguments defaulted. The defaults arguments are based on the default folder structure shown in the [README](../README.md)
```
python ras2fim.py -w 12090301 -p EPSG:2277 -o 12090301_meters_2277
```

**Note:** The `-p` argument is the incoming projection of the models that are about to be processed. Any HEC-RAS model folders, such as folders from OWP_ras_models\models, must match that projection. The `-res` argument defaults to `10` but it must match the resolution of the incoming HEC-RAS model files. You will need the `-t` DEM resolution to also match the the stated resolution.  At this point, only 10 meter has been tested, and more details and options coming soon.
<br><br>

### Editors

You can use any editor you like, but if you are a fan of VSCode and have it installed, you can type `code` in your Anaconda Powershell Prompt window and it will launch VSCode as your editor. With some VSCode additional setup, you will be able to do line-by-line (step) debugging if you like.  [Click here](https://code.visualstudio.com/docs/python/debugging) for more details.

<br>
<br>
