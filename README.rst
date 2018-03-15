=========
CGAT-core
=========

.. image:: https://readthedocs.org/projects/cgat-core/badge/?version=latest
    :target: http://cgat-core.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/travis/cgat-developers/cgat-core.svg
    :alt: Travis

.. image:: https://img.shields.io/twitter/follow/CGAT_Oxford.svg?style=social&logo=twitter&label=Follow
    :target: https://twitter.com/cgat_oxford?lang=en
    :alt: Twitter Followers

.. image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social&logo=twitter
    :target: https://twitter.com/cgat_oxford?lang=en
    :alt: Twitter URL

CGAT-core is a workflow management system that allows users to quickly and reproducibly build scalable
data analysis pipelines. CGAT-core is  a set of libraries and helper functions used to enable researchers
to design and build computational workflows for the analysis of large-scale data-analysis. 

Documentation for CGAT-core can be accessed at `read the docs <http://cgat-core.readthedocs.io/en/latest/>`_ 

Used in combination with CGAT-apps, we have deomonstrated the functionality of our
flexible implementation using a set of well documented, easy to install and easy to use workflows, 
called `CGAT-flow <https://github.com/cgat-developers/cgat-flow>`_ (`Documentation <https://www.cgat.org/downloads/public/cgatpipelines/documentation/>`_).

CGAT-core is open-sourced, powerful and user-friendly, and has been continually developed 
as a Next Generation Sequencing (NGS) workflow management system over the past 10 years.

============
Installation
============

The following sections describe how to install the CGATCore framework. For instructions on how to install
the CGAT-apps (scripts) and CGAT-flow (workflows/pipelines) please follow these instructions `here <https://www.cgat.org/downloads/public/cgatpipelines/documentation/InstallingPipelines.html>`_ .

The preferred method to install the CGATCore is using the installation script,
which uses `conda <https://conda.io/docs/>`_.

Here are the steps::

   # download installation script:
   curl -O https://raw.githubusercontent.com/PATH/TO/LOCATION/install-CGAT-tools.sh

   # see help:
   bash install-CGAT-tools.sh

   # install the development version (recommended, no production version yet):
   bash install-CGAT-tools.sh --devel --no-dashboard [--location </full/path/to/folder/without/trailing/slash>]

   # the code is downloaded in zip format by default. If you want to get a git clone, use:
   --git # for an HTTPS clone
   --git-ssh # for a SSH clone (you need to be a CGATOXford contributor on GitHub to do this)

   # the pipelines are intended to run on a cluster using the DRMAA API. If that's not your case, please use:
   --no-cluster

   # if you want to download and install IDEs like Spyder or RStudio with this installation, please use:
   --ide

   # once the installation is finished, enable the conda environment as requested by the installation script:
   source </full/path/to/folder/without/trailing/slash>/conda-install/bin/activate cgat-c


The installation script will put everything under the specified location. It needs
? GB of disk space and it takes about 7 minutes to complete. The aim of the
script is to provide a portable installation that does not interfere with the existing
software. As a result, you will have a conda environment working with the CGAT-core
which can be enabled on demand according to your needs.


Linux vs OS X
=============

* ulimit works as expected in Linux but it does not have an effect on OS X. `Disabled <https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065>`_ ulimit tests for OS X.

* ssh.connect times out in OSX. Exception `caught <https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065>`_

* Linux uses /proc/meminfo and OS X uses `vm_stat <https://github.com/cgat-developers/cgat-core/compare/bb1c75df8f42...575f0699b326>`_
