## Creating a RAS2FIM environment

<img src="https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/ras2fim_logo_20211018.png" align="right"
     alt="ras2fim logo" width="120" height="120">
     
You will need to set up an Anaconda/Python environment for developing `ras2fim` that is separate from the "default" environment that you use in your own work/research.  This will allow you to utilize `ras2fim` without worrying about corrupting the Anaconda/Python environment on which your other work depends.

Below are instructions for building a separate development environment for using the `ras2fim` package using the [Conda](http://conda.pydata.org/docs/index.html) package management system.

### Step 1:
Download and Install [Anaconda](https://www.anaconda.com/products/individual) to your machine.<br>
<br>

### Step 2:
If you have not already done so, ensure `git` has been installed and clone a copy of the `ras2fim` repository on to your local machine (see note below). Path to your windows folder of choice, then run:<br>
```
git clone https://github.com/NOAA-OWP/ras2fim.git (see note below)
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

If you have not already done so, you will need to downloaded some files/folders from ESIP. See the [README](../README.md) file for more details on ESIP access. 

You now need the `inputs` and `OWP_ras_models` folders as mentioned in the [README](../README.md). Downloading these two folders will give you some default data that is required. Of course, you are welcome to override or experiment with some of this data as you see fit.

More details are coming soon.
<br>

Here are some notes describing the two folders.

- `inputs`:  This folder is for non HECRAS data that is required for processing HECRAS models. 
  
- `OWP_ras_models\models`: While also optional, you are encouraged to move each of your models that have been pre-processed into the `OWP_ras_models\models`. Note: When you run `ras2fim.py`, each of your HEC-RAS models likely have there own folder, and you can put as many model folders as you like in the `models` folder. When you run `ras2fim.py` it will load in all data across all files and folders inside the `models` folder (location overrideable). Remember, you can put your model folders anywhere you like, this is just the default pattern.<br>
ie)<br>
![ras2fim default models folder structure image](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/default_models_folder_structure.png)

- `OWP_ras_models_catalog_{HUC HUMBER}.csv`: ras2fim relies on loading this file to get input data about the incoming models. See the ras2fim.py -h for details about overriding the location and file name. At a minumum, currently the only columns in use are the:
    - `final_name_key`:  Which must match the folder name, case-sensitive, from which ever folder you are loading your models. e.g. 1259243_STREAM 5B34_g01_1701459932. The folder names are also parced and need to follow an convention of "{model name}_ g01 _{time or really any value}" as in `underscore(g01)underscore`(no spaces).  The {model name} will become the name of many output folder. "_g01_", without this value, the system will reject the model name folder.
    - `model_id`: Any numeric as long as each row is unique. This value will also become part of folder and file output names.
A sample will be availabe in ESIP (ras2fim) subfolder.
  
<br><br>

### ------------------------------------------------------------
### --- You are now ready to start processing ras2fim models ---
<br>

### BAD_MODELS_LST
There is a wide number of ways that a model can fail in ras2fim.py. Many scenarios are being programatically caught and logged throughout the code. However, if all else fails, there is a new system called the `bad_models_lst` system. Bad models, based on the model folder name, can be added to this list and will be dropped if found. The bad models list line items are the model folder name minus the last part of the folder name which is a time stamp. For now, they are hardcoded into `create_shapes_from_hecras.py` where you can search for the phrase `bad_models_lst`. If you are using default OWP_ras_models, you may want to check in this file. Later, this system will be changed to a config file and hardcoded into the script.
<br/><br/>

## Usage


### Step 6
Each time you want to run ras2fim.py or other tools, you need to activate your ras2fim conda environment.
```
conda activate ras2fim
```
![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_activate.png)

### Step 7:

The main script is titled `ras2fim.py`.  **All scripts have a helper flag of `-h`**.  It is recommended that you run the script with the helper flag first to determine the required input. Also read the sample usage notes near the bottom of `ras2fim.py` code file.<br><br>
![](https://github.com/NOAA-OWP/ras2fim/blob/dev/doc/conda_python_run.png)
<br>
***--- Image may be out of date slightly as parameters are being adjusted currently ---***
<br><br>
**Note**: Currently, there are **two required** arguments and a number of optional arguments. Below is a sample input string to execute the `ras2fim.py` script with most arguments defaulted. The defaults arguments are based on the default folder structure shown in the [README](../README.md)
```
python ras2fim.py -w 12090301 -p EPSG:2277 
```

**Note:** The `-p` argument is the incoming projection of the models that are about to be processed. Any HEC-RAS model folders, such as folders from OWP_ras_models\models, must match that projection. The `-res` argument defaults to `10` but it must match the resolution of the incoming HEC-RAS model files. You will need the `-t` DEM resolution to also match the the stated resolution.  At this point, only 10 meter has been tested, and more details and options coming soon.
<br><br>

### Editors

You can use any editor you like, but if you are a fan of VSCode and have it installed, you can type `code` in your Anaconda Powershell Prompt window and it will launch VSCode as your editor. With some VSCode additional setup, you will be able to do line-by-line (step) debugging if you like.  [Click here](https://code.visualstudio.com/docs/python/debugging) for more details.
<br>
<br>

### Additional Tools

There are a wide range of other tools, in the `tools` directory, you may use that can help with processing and data gathering. Most of them are generally for NOAA/OWP use only as it relies on having access to our S3 bucket. However, if you have your own S3 and set it up with a matching default folder structure, you are welcome to use the scripts. Please read informaton in those scripts carefully, notes at the top of the script and bottom especially. You will also need some form of permissiosn to reach your bucket such as an aws config profile.


