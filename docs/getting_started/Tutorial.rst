.. _getting_started-Tutorial:


=============================
Running a pipeline - Tutorial
=============================


Before beginning this tutorial make sure you have the CGAT-core installed correctly,
please see here (see :ref:`getting_started-Installation`) for installation instructions.

As a tutorial example of how to run a CGAT workflow we will run the cgat-showcase piepline. Therefore,
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

   mkdir pipeline_showcase
   cd pipeline_showcase/
   wget 
   tar -zxvf tutorial_showcase.tar.gz

**2.** The next thing is to generate the pipeline configuration file by::

   cgatshowcase transdiffexprs config

**3.** This will generate a **pipeline.yml** configuration file. This is the file that can be modified to
alter the workflow of the pipline. However, for this tutorial you do not need to modify the parameters to run the pipeline. In the modify_config section below I have detailed how you can modify the config file to change the output of the pipeline. To run the pipeline do the following::

   cgatshowcase transdiffexprs make full -v 5 --local

This ``--local`` will run the pipeline locally if you do not have access to a cluster. Alternatively, if you have access to a
cluster then you can remove the ``--local`` option. The pipeline will then  distribute your jobs accross the cluster.

.. note:: 

   Depending upon the installation procedure you used, you may have to install software to
   run the pipeline. We recomend using `conda` to install these.

**4.** The **final step** is to generate the report. We have a preference for multiqc to generate reports
for general statistics and Rmarkdown for custom report generation. To generate MultiQC and Rmarkdown reports run the command::

   cgatshowcase transdiffexprs make build_report -v 5 --local

This will generate a multiqc report in the folder MultiQC_report.dir and a Rmarkdown report in ??.

**5.** Modify the pipeline.yml file to change the output of the pipeline.

The ability to modify the output of the pipeline is an important feature of our pipelines. We aim
to remove all hardcoded references within the pipeline code wherever possible. For example, for the
transdiffexprs pipeline can be ran using the wald test or the LRT test. This can be modified by altering the
pipeline.yml, without the need for modifying the pipeline code.

The following is the contents of the pipeline.yml file ::

   Insert the yml file here

**6.** Rerun the pipeline with the new configurations::

   cgatshowcase transdiffexprs make full -v5 --local

this completes the tutorial for running the transdiffexprs pipeline. 
