## Creating a RAS2FIM Conda environment

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
In the Anaconda Promt, create the `ras2fim` conda environment from the cloned `environment.yml`.<br>
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

### Step 7:
Change directory to the source `src` folder.<br>
```
conda activate ras2fim
```
![](https://github.com/NOAA-OWP/ras2fim/blob/master/doc/conda_src.png)
