.. _getting_started-Examples:


==================
Running a pipeline
==================


This section provides a tutorial-like introduction of how to run CGAT pipelines. As an example of how we build simple
computational pipelines please refer to `cgat-showcase <https://github.com/cgat-developers/cgat-showcase>`_. As an example of how we use cgatcore to 
build more complex computational pipelines, please refer to the code detailed in our `cgat-flow repository <https://github.com/cgat-developers/cgat-flow>`_.

.. _getting_started-Intro:

Introduction
=============

A pipeline takes input data and performs a series of automated steps on it to produce some output data.

Each pipeline is usually coupled with a report (usually MultiQC or Rmarkdown) document to
summarize and visualize the results.

It really helps if you are familiar with:

   * the unix command line to run and debug the pipeline
   * python_ in order to understand what happens in the pipeline
   * ruffus_ in order to understand the pipeline code
   * sge_ (or any other workflow manager) in order to monitor your jobs
   * git_ in order to up-to-date code

.. _getting_started-setting-up-pipeline:

Setting up a pipeline
======================

**Step 1**: Install cgat-showcase (our toy example of a cgatcore pipeline):

Check that your computing environment is appropriate and follow cgat-showcase installation instructions (see `Installation instructions <https://cgat-showcase.readthedocs.io/en/latest/getting_started/Installation.html>`_).

**Step2**: Clone the repository

To inspect the code and the layout clone the repository::

   git clone https://github.com/cgat-developers/cgat-showcase.git

When inspecting the respoitory:
The source directory will contain the pipeline master script named
:file:`cgatshowcase/pipeline_<name>.py`

The default configuration files will be contained in the folder
:file:`cgatshowcase/pipeline<Name>/`

All our pipelines are written to be lightweight. Therefore, a module file
assoaiated with the pipeline master script, typically named
:file:`cgatshowcase/Module<Name>.py`, is usually where code required to run the tasks
of the pipleine is located. 

**Step 3**: To run a pipeline you will need to create a working directory
and enter it. For example::

   mkdir version1
   cd version1/

This is where the pipeline will be executed and files will be generated in this
directory.

However, the cgat-showcase example comes with test data and this can be downloaded by running::

	wget https://www.cgat.org/downloads/public/showcase/showcase_test_data.tar.gz
	tar -zxvf showcase_test_data.tar.gz
	cd showcase_test_data

**Step 4**: Configure the cluster

Running pipelines on a cluster required the drmaa API settings to be configures and passed
to cgatcore. The default cluster engine is SGE, however we also support SLURM and Torque/PBSpro.
In order to execute using a non SGE cluster you will need to setup a `.cgat.yml` file in your
home directory and specify parameters according to the `cluster configuration documentation <https://cgat-core.readthedocs.io/en/latest/getting_started/Cluster_config.html>`_.

**Step 5**: Our pipelines are written with minimal hard coded options. Therefore,
to run a pipeline an initial configuration file needs to be
generated. A configuration file with all the default values can be obtained by
running::

      cgatshowcase <name> config

For example, if you wanted to run the transdiffexprs pipeline you would run::

      cgatshowcase transdiffexprs config


This will create a new :file:`pipeline.yml` file. **YOU MUST EDIT THIS
FILE**. The default values are unlikely to be configured correctly for your data. The
configuration file should be well documented and the format is
simple. The documenation for the `ConfigParser
<http://docs.python.org/library/configparser.html>`_ python module
contains the full specification.

**Step 6**: Add the input files. The required input is specific for each
pipeline in the documentation string at the; read the pipeline documentation to find out exactly which
files are needed and where they should be put. Commonly, a pipeline
works from input files linked into the working directory and
named following pipeline specific conventions.

**Step 7**: You can check if all the external dependencies to tools and
R packages are satisfied by running::

      cgatshowcase <name> check

.. _getting_started-pipelineRunning:

Running a pipeline
===================

pipelines are controlled by a single python script called
:file:`pipeline_<name>.py` that lives in the source directory. Command line usage information is available by running::

   cgatshowcase <name> --help
   
Alternatively, you can call the python script directly::

	python /path/to/code/cgatshowcase/pipeline_<name>.py --help

The basic syntax for ``pipeline_<name>.py`` is::

   cgatshowcase <name> [workflow options] [workflow arguments]

For example, to run the readqc pipeline you would run the following::

   cgatshowcase readqc make full

``workflow options`` can be one of the following:

make <task>

   run all tasks required to build task

show <task>

   show tasks required to build task without executing them

plot <task>

   plot image of workflow (requires `inkscape <http://inkscape.org/>`_) of
   pipeline state for task

touch <task>

   touch files without running task or its pre-requisites. This sets the 
   timestamps for files in task and its pre-requisites such that they will 
   seem up-to-date to the pipeline.

config

   write a new configuration file :file:`pipeline.ini` with
   default values. An existing configuration file will not be
   overwritten.

clone <srcdir>

   clone a pipeline from :file:`srcdir` into the current
   directory. Cloning attempts to conserve disk space by linking.

In case you are running a long pipeline, make sure you start it
appropriately, for example::

   nice -19 nohup cgatshowcase <name> make full -v5 -c1

This will keep the pipeline running if you close the terminal.

Fastq naming convention
-----------------------

Most of our pipelines assume that input fastq files follows the following
naming convention (with the read inserted between the fastq and the gz. The reason
for this is so that regular expressions do not have to acount for the read within the name.
It is also more explicit::

   sample1-condition-R1.fastq.1.gz
   sample1-condition-R2.fastq.2.gz


Additional pipeline options
---------------------------

In addition to running the pipeline with default command line options, running a
pipeline with --help will allow you to see additional options for ``workflow arguments``
when running the pipelines. These will modify the way the pipeline in ran.

`- -local`

    This option allows the pipeline to run locally.

`- -input-validation`

    This option will check the pipeline.ini file for missing values before the
    pipeline starts.

`- -debug`

    Add debugging information to the console and not the logfile

`- -dry-run`

    Perform a dry run of the pipeline (do not execute shell commands)

`- -exceptions`

    Echo exceptions immidietly as they occur.

`-c - -checksums`

    Set the level of ruffus checksums.

.. _getting_started-Building-reports:

Building pipeline reports
================================

We always associate some for of reporting with our pipelines to display summary information as a set of nicely formatted
html pages. 

Currently in CGAT we have 3 preferred types of report generation.

   * MultiQC report (for general alignment and tool reporting)
   * R markdown (for bespoke reporting)
   * IPython notebook (for bespoke reporting)

To determine which type of reporting is implimented for each pipeline, refer to
the specific pipeline documentation at the beginning of the script.

Reports are generated using the following command once a workflow has completed::

    cgatshowcase <name> make build_report

MultiQC report
--------------

MultiQC is a python framework for automating reporting and we have imliemnted it in the
majority of our workflows to generate QC stats for frequently used tools (mostly in our
generic workflows). 


R markdown
----------
R markdown report generation is very useful for generating bespoke reports that require user
defined reporting. We have implimented this in our bamstats workflow.

Jupyter notebook
----------------
Jupyter notebook is a second approach that we use to produce bespoke reports. An example is
also implimented in our bamstats workflow.

.. _getting_started-Troubleshooting:

Troubleshooting
===============

Many things can go wrong while running the pipeline. Look out for

   * bad input format. The pipeline does not perform sanity checks on the input format.  If the input is bad, you might see wrong or missing results or an error message.
   * pipeline disruptions. Problems with the cluster, the file system or the controlling terminal might all cause the pipeline to abort.
   * bugs. The pipeline makes many implicit assumptions about the input files and the programs it runs. If program versions change or inputs change, the pipeline might not be able to deal with it.  The result will be wrong or missing results or an error message.

If the pipeline aborts, locate the step that caused the error by
reading the logfiles and the error messages on stderr
(:file:`nohup.out`). See if you can understand the error and guess the
likely problem (new program versions, badly formatted input, ...). If
you are able to fix the error, remove the output files of the step in
which the error occured and restart the pipeline. Processing should
resume at the appropriate point.

.. note:: 

   Look out for upstream errors. For example, the pipeline might build
   a geneset filtering by a certain set of contigs. If the contig
   names do not match, the geneset will be empty, but the geneset
   building step might conclude successfully. However, you might get
   an error in any of the downstream steps complaining that the gene
   set is empty. To fix this, fix the error and delete the files
   created by the geneset building step and not just the step that
   threw the error.

Common pipeline errors
----------------------

One of the most common errors when runnig the pipeline is::

    GLOBAL_SESSION = drmaa.Session()
    NameError: name 'drmaa' is not defined

This error occurrs because you are not connected to the cluster. Alternatively
you can run the pipleine in local mode by adding `- -no-cluster` as a command line option.

Updating to the latest code version
-----------------------------------

To get the latest bugfixes, go into the source directory and type::

   git pull

The first command retrieves the latest changes from the master
repository and the second command updates your local version with
these changes.

.. _pipelineReporting:
