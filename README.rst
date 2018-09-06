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
data analysis pipelines. CGAT-core is a set of libraries and helper functions used to enable researchers
to design and build computational workflows for the analysis of large-scale data-analysis. 

Documentation for CGAT-core can be accessed at `read the docs <http://cgat-core.readthedocs.io/en/latest/>`_ 

Used in combination with CGAT-apps, we have demonstrated the functionality of our
flexible implementation using a set of well documented, easy to install and easy to use workflows, 
called `CGAT-flow <https://github.com/cgat-developers/cgat-flow>`_ (`Documentation <https://www.cgat.org/downloads/public/cgatpipelines/documentation/>`_).

CGAT-core is open-sourced, powerful and user-friendly, and has been continually developed 
as a Next Generation Sequencing (NGS) workflow management system over the past 10 years.

============
Installation
============

The following sections describe how to install the cgatcore framework. For instructions on how to install
the CGAT-apps (scripts) and CGAT-flow (workflows/pipelines) please follow these instructions `here <https://www.cgat.org/downloads/public/cgatpipelines/documentation/InstallingPipelines.html>`_ .

The preferred method to install the cgatcore is using the installation script,
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
   source </full/path/to/folder/without/trailing/slash>/conda-install/etc/profile.d/conda.sh
   conda activate base
   conda activate cgat-c

The installation script will put everything under the specified location. It needs 1.2 GB of disk space.
The aim of the script is to provide a portable installation that does not interfere with the existing
software. As a result, you will have a conda environment working with the CGAT-core which can be enabled
on demand according to your needs.

Linux vs OS X
=============

* ulimit works as expected in Linux but it does not have an effect on OS X. `Disabled <https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065>`_ ulimit tests for OS X.

* ssh.connect times out in OSX. Exception `caught <https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065>`_

* Linux uses /proc/meminfo and OS X uses `vm_stat <https://github.com/cgat-developers/cgat-core/compare/bb1c75df8f42...575f0699b326>`_
