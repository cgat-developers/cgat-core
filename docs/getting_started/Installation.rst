.. _getting_started-Installation:


============
Installation
============

The following sections describe how to install the CGATCore framework. For instructions on how to install
the CGAT-apps (scripts) and CGAT-flow (workflows/pipelines) please follow these instructions `here <https://www.cgat.org/downloads/public/cgatpipelines/documentation/InstallingPipelines.html>`_ .
Please note that installing the apps and flow will also install CGATCore as part of this process.

We distinguish between two different installation types: production and development. The former refers to a well tested subset of pipelines, and is the recommended installation.
The latter refers to the whole collection of pipelines developed at CGAT, which may contain code under active development.

.. _getting_started-Automated:

Automated installation
----------------------

The following sections describe how to install the CGATCore framework. For instructions on how to install
the CGAT-apps (scripts) and CGAT-flow (workflows/pipelines) please follow these instructions `here <https://www.cgat.org/downloads/public/cgatpipelines/documentation/InstallingPipelines.html>`_ .

The preferred method to install the CGATCore is using the installation script,
which uses `conda <https://conda.io/docs/>`_.

Here are the steps::

   # download installation script:
   curl -O https://raw.githubusercontent.com/cgat-developers/cgat-core/master/install-CGAT-tools.sh

   # see help:
   bash install-CGAT-tools.sh

   # install the development version (recommended, no production version yet):
   bash install-CGAT-tools.sh --devel [--location </full/path/to/folder/without/trailing/slash>]

   # the code is downloaded in zip format by default. If you want to get a git clone, use:
   --git # for an HTTPS clone
   --git-ssh # for a SSH clone (you need to be a cgat-developer contributor on GitHub to do this)

   # enable the conda environment as requested by the installation script
   # NB: you probably want to automate this by adding the instructions below to your .bashrc
   source </full/path/to/folder/without/trailing/slash>/conda-install/etc/profile.d/conda.sh cgat-c
   conda activate base
   conda activate cgat-c

The installation script will put everything under the specified location. It needs 1.2 GB of disk space.
The aim of the script is to provide a portable installation that does not interfere with the existing
software. As a result, you will have a conda environment working with the CGAT-core which can be enabled
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
