.. _getting_started-Tutorial:


=============================
Running a pipeline - Tutorial
=============================


Before beginning this tutorial make sure you have the CGAT-core installed correctly,
please see here (see :ref:`getting_started-Installation`) for installation instructions.

As a tutorial example of how to run a CGAT workflow we will run the cgat-showcase pipeline. Therefore,
you will also need to install the cgat-showcase (see `instructions <https://cgat-showcase.readthedocs.io/en/latest/getting_started/Tutorial.html>`_)

The aim of this pipeline is to perform pseaudoalignment using kallisto. The pipeline can be ran locally or
dirtributed accross a cluster. This tutorial will explain the steps required to run this pipeline. Further documentation
on cgat-showcase can be found `here <https://cgat-showcase.readthedocs.io/en/latest/>`_.

The cgat-showcase highlights some of the functionality of cgat-core. However, we also have our utility
pipelines contained in the cgat-flow repository which demonstrate our advanced pipelines for next-generation
sequencing analysis (see `cgat-flow <https://github.com/cgat-developers/cgat-flow>`_).

Tutorial start
--------------


**1.** First download the tutorial data::

   mkdir showcase
   cd showcase
   wget https://www.cgat.org/downloads/public/showcase/showcase_test_data.tar.gz
   tar -zxvf showcase_test_data.tar.gz

**2.** Next we will generate a configuration yml file so the pipeline output can be modified::

   cd showcase_test_data
   cgatshowcase transdiffexpres config

or you can alternatively call the workflow file directly::

   python /path/to/file/pipeline_transdiffexpres.py config

This will generate a **pipeline.yml** file containing the configuration parameters than can be used to modify
the output of the pipleine. However, for this tutorial you do not need to modify the parameters to run the 
pipeline. In the :ref:`modify_config` section below I have detailed how you can modify the config file to
change the output of the pipeline.

**3.** Next we will run the pipleine::

   cgatshowcase transdiffexpres make full -v5 --no-cluster

This ``--no-cluster`` will run the pipeline locally if you do not have access to a cluster. Alternatively if you have a
cluster remove the ``--no-cluster`` option and the pipleine will distribute your jobs accross the cluster.

.. note::

   There are many commandline options available to run the pipeline. To see available options please run :code:`cgatshowcase --help`.

**4.** Generate a report

The final step is to generate a report to display the output of the pipeline. We have a preference for using MultiQC
for generate bioinformatics tools (such as mappers and pseudoaligners) and Rmarkdown for generating custom reports.
In order to generate these run the command::

    cgatshowcase transdiffexprs make build_report -v 5 --no-cluster

This will generate a MultiQC report in the folder `MultiQC_report.dir/` and an Rmarkdown report in `R_report.dir/`. 



This completes the tutorial for running the transdiffexprs pipeline for cgat-showcase, hope you find it as useful as
we do for writing workflows within python. 
