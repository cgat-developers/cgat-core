.. _getting_started-Installation:


============
Installation
============

The following sections describe how to install the cgatcore framework. 

.. _getting_started-Conda:

Conda Installation
------------------

The our preffered method of installation is using conda. If you dont have conda installed then
please install conda using `miniconda <https://conda.io/miniconda.html>`_ or `anaconda <https://www.anaconda.com/download/#macos>`_.

cgatcore is currently installed using the bioconda channel and the recipe can be found on `github <https://github.com/bioconda/bioconda-recipes/tree/b1a943da5a73b4c3fad93fdf281915b397401908/recipes/cgat-core>`_. To install cgatcore::

    conda install -c conda-forge -c bioconda cgatcore

.. _getting_started-Automated:


Pip installation
----------------
We recommend installation through conda because it manages the dependancies. However, cgatcore is 
generally lightweight and can be installed easily using pip package manager. However, you may also have to
install other dependancies manually::

	pip install cgatcore

.. _getting_started-pip:

Automated installation
----------------------

The following sections describe how to install the cgatcore framework. 

The preferred method to install the cgatcore is using conda but we have also created a bash installation script,
which uses `conda <https://conda.io/docs/>`_ under the hood.

Here are the steps::

   # download installation script:
   curl -O https://raw.githubusercontent.com/cgat-developers/cgat-core/master/install.sh

   # see help:
   bash install.sh

   # install the development version (recommended, no production version yet):
   bash install.sh --devel [--location </full/path/to/folder/without/trailing/slash>]

   # the code is downloaded in zip format by default. If you want to get a git clone, use:
   --git # for an HTTPS clone
   --git-ssh # for a SSH clone (you need to be a cgat-developer contributor on GitHub to do this)

   # enable the conda environment as requested by the installation script
   # NB: you probably want to automate this by adding the instructions below to your .bashrc
   source </full/path/to/folder/without/trailing/slash>/conda-install/etc/profile.d/conda.sh
   conda activate base
   conda activate cgat-c

The installation script will put everything under the specified location.
The aim of the script is to provide a portable installation that does not interfere with the existing
software. As a result, you will have a conda environment working with the cgat-core which can be enabled
on demand according to your needs.

.. _getting_started-Manual:

Manual installation
-------------------

To obtain the latest code, check it out from the public git_ repository and activate it::

   git clone https://github.com/cgat-developers/cgat-core.git
   cd cgat-core
   python setup.py develop

Once checked-out, you can get the latest changes via pulling::

   git pull 


.. _getting_started-Additional:

Installing additonal software
-----------------------------

When building your own workflows we recomend using conda to install software into your environment where possible.

This can easily be performed by::

   conda search <package>
   conda install <package>



.. _conda: https://conda.io
