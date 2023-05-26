## Creating a RAS2FIM Conda environment

<img src="https://github.com/NOAA-OWP/ras2fim/blob/master/doc/ras2fim_logo_20211018.png" align="right"
     alt="ras2fim logo" width="120" height="120">
     
We recommend that you set up a Anaconda/Python environment for developing `ras2fim` that is separate from the "default" environment that you use in your own work/research.  This will allow you to utilize `ras2fim` without worrying about corrupting the Anaconda/Python environment on which your other work depends.

Below are instructions for building a separate development environment for using the `ras2fim` package using the [Conda](http://conda.pydata.org/docs/index.html) package management system.

### Step 1:
Download and Install [git](https://git-scm.com/downloads) to your Windows machine.<br>

### Step 2:
Download and Install [Anaconda](https://www.anaconda.com/products/individual) to your machine.<br>

### Step 3:
Clone a copy of the `ras2fim` repository on to your local machine.<br>
```
    git clone https://github.com/NOAA-OWP/ras2fim.git
```
### Step 4:
Open an **Anaconda Prompt** and navigate to the cloned directory.  Your path may vary.<br>
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_prompt.png)

### Step 5:
In the Anaconda Promt, if this is a first time install, you can create the `ras2fim` conda environment from the cloned `environment.yml`.<br>
```
conda env create -f environment.yml
```
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_create_env.png)

### Step 6:
Activate the newly created `ras2fim` conda environment.<br>
```
conda activate ras2fim
```
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_activate.png)

**Note:**
You can use any editor you like, but if you are a fan of VSCode and have it installed, you can type<br>
```
code
```
and it will launch VSCode as your editor. You will be able to debug and test within VSCode if you like.


### Step 7:
Change directories into the source `src` folder.<br>
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_src.png)

### Step 8:
Run the `ras2fim.py` within the `ras2fim` conda environment with a help flag `-h` to verify that everything was installed.<br>
```
python ras2fim.py -h
```
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_python_run.png)

### Step 9:
Now we need to setup the **default folder structure** for your inputs and outputs. All code will use this default structure to look for data. Howver, most code will let you override any folder to your own pathing, and you can build your own folder structure. 

To setup your enviroment **default** folders:
```
a) Make a directory at your c: called ras2fim_data
b) Make the following subfolder with these names and case `inputs`, `OWP_ras_models`, `outputs_ras2fim_models`. Inside the `OWP_ras_models` folder, make another subfolder called `models`.
```
The default folder structure will look like this:<br>
![ras2fim default folder structure image](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/default_folder_structure.png)
<br>

### Step 10:
You will now need some data.
- `inputs`:  In the README.md, you may have already downloaded the X-National_datasets folder. You are encouraged, but not mandiatory, to move the X-National_datasets folder with it's four files and one folder, inside the `inputs` folder. 
- `OWP_ras_models\models`: While also optional, you are encouraged to move each of your models that have been pre-processed into the `OWP_ras_models\models`. Note: When you run `ras2fim.py`, each of your models likely have there own folder, and you can put as many model folders as you like in this `models` folder. When you run `ras2fim.py` it will load in all data across all files and folders inside the `OWP_ras_models\models` folder. Remember, you can put your model folders anywhere you like, this is just the default pattern.<br>
ie)<br>
![ras2fim default models folder structure image](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/default_models_folder_structure.png)
<br><br>

#### You are now ready to start processing ras2fim models
<br>
<br>
