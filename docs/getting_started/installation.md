# Installation

The following sections describe how to install the `cgatcore` framework.

## Conda installation

The preferred method of installation is using Conda. If you do not have Conda installed, you can install it using [Miniconda](https://conda.io/miniconda.html) or [Anaconda](https://www.anaconda.com/download/#macos).

`cgatcore` is installed via the Bioconda channel, and the recipe can be found on [GitHub](https://github.com/bioconda/bioconda-recipes/tree/b1a943da5a73b4c3fad93fdf281915b397401908/recipes/cgat-core). To install `cgatcore`, run the following command:

```bash
conda install -c conda-forge -c bioconda cgatcore
```

### Prerequisites

Before installing `cgatcore`, ensure that you have the following prerequisites:

- **Operating System**: Linux or macOS
- **Python**: Version 3.6 or higher
- **Conda**: Recommended for dependency management

### Troubleshooting

- **Conda Issues**: If you encounter issues with Conda, ensure that the Bioconda and Conda-Forge channels are added and prioritized correctly.
- **Pip Dependencies**: When using pip, manually install any missing dependencies listed in the error messages.
- **Script Errors**: If the installation script fails, check the script's output for error messages and ensure all prerequisites are met.

### Verification

After installation, verify the installation by running:

```bash
python
```

```python
import cgatcore
print(cgatcore.__version__)
```

This should display the installed version of `cgatcore`.

## Pip installation

We recommend installation through Conda because it manages dependencies automatically. However, `cgatcore` is generally lightweight and can also be installed using the `pip` package manager. Note that you may need to manually install other dependencies as needed:

```bash
pip install cgatcore
```

## Automated installation

The preferred method to install `cgatcore` is using Conda. However, we have also created a Bash installation script, which uses [Conda](https://conda.io/docs/) under the hood.

Here are the steps:

```bash
# Download the installation script:
curl -O https://raw.githubusercontent.com/cgat-developers/cgat-core/master/install.sh

# See help:
bash install.sh

# Install the development version (recommended, as there is no production version yet):
bash install.sh --devel [--location </full/path/to/folder/without/trailing/slash>]

# To download the code in Git format instead of the default zip format, use:
--git # for an HTTPS clone
--git-ssh # for an SSH clone (you need to be a cgat-developer contributor on GitHub to do this)

# Enable the Conda environment as instructed by the installation script
# Note: you might want to automate this by adding the following instructions to your .bashrc
source </full/path/to/folder/without/trailing/slash>/conda-install/etc/profile.d/conda.sh
conda activate base
conda activate cgat-c
```

The installation script will place everything under the specified location. The aim of the script is to provide a portable installation that does not interfere with existing software environments. As a result, you will have a dedicated Conda environment that can be activated as needed to work with `cgatcore`.

## Manual installation

To obtain the latest code, check it out from the public Git repository and activate it:

```bash
git clone https://github.com/cgat-developers/cgat-core.git
cd cgat-core
python setup.py develop
```

To update to the latest version, simply pull the latest changes:

```bash
git pull
```

## Installing additional software

When building your own workflows, we recommend using Conda to install software into your environment where possible. This ensures compatibility and ease of installation.

To search for and install a package using Conda:

```bash
conda search <package>
conda install <package>
```

## Accessing the libdrmaa shared library

You may also need access to the `libdrmaa.so.1.0` C library, which can often be installed as part of the `libdrmaa-dev` package on most Unix systems. Once installed, you may need to specify the location of the DRMAA library if it is not in a default library path. Set the `DRMAA_LIBRARY_PATH` environment variable to point to the library location.

To set this variable permanently, add the following line to your `.bashrc` file (adjusting the path as necessary):

```bash
export DRMAA_LIBRARY_PATH=/usr/lib/libdrmaa.so.1.0
```

[Conda documentation](https://conda.io)