.. _getting_started-Tutorial:


=============================
Running a pipeline - Tutorial
=============================


Before beginning this tutorial make sure you have the CGAT-core installed correctly,
please see here (see :ref:`getting_started-Installation`) for installation instructions.

As a tutorial example of how to run a CGAT workflow we will run the readqc piepline. Therefore,
you will also need to install the CGAT-flow conda environment, instructions for this are as follows::

    Insert instructions here

The aim of this pipeline is to perform quality assessment of the reads following sequencing. The
pipeline also performs read pre-processing such as quality trimming or adaptor removal through the
implimentation of a number of third party tools such as trimmgalore, timmomatic, sickle, cutadapt,
pandaseq and flash.


Initally this piepline is ran one to assertain the read quality then, if it is oberved that
trimming or downstream adapter removal is needed then the pipeline is ran for a second time
after specifying these in the configuration files pipeline.ini

Tutorial start
--------------


**1.** First download the tutorial data::

   mkdir pipeline_readqc
   cd pipeline_readqc/
   wget https://www.cgat.org/downloads/public/cgatpipelines/Tutorial_data/tutorial_fastq.tar.gz
   tar -zxvf tutorial_fastq.tar.gz

**2.** The next thing is to generate the pipeline configuration file by::

   cgatflow readqc config

**3.** This will generate a **pipeline.ini** configuration file. This is the file that can be modified to
alter the workflow of the pipline. However, for the first run of the readqc pipeline, the .ini file
does not need to be modified. To run the pipeline do the following::

   cgatflow readqc make full -v 5 --local

This ``--local`` will run the pipeline locally if you do not have access to a cluster. Alternatively if you have a
cluster remove the ``--local`` option and the pipleine will distribute your jobs accross the cluster.

.. note:: 

   If you get the following sqlite error while running the pipeline please continue running the pipeline with a
   single process as follows ``cgatflow readqc make full -v5 -p1 --local`` and the pipeline should complete without
   errors. This occurrs because sqlite cannot handle multi-threaded access. Although we have added a fix for most
   cases, this can sometimes not always work. 



**4.** The **final step** is to generate the report. We have a preference for multiqc, however we also have our own
CGAT report implimented in this pipeline. To generate the reports run the command::

   cgatflow readqc make build_report -v 5 --local

This will generate a multiqc report in the folder MultiQC_report.dir and CGAT report in the folder /report/html/.

Depending on the report, you may now wish to run a trimming tool. In order to do this you will need
to modify the `pipeline.ini` file and specify in the preprocessing options which trimmer you would
like to run and then set the parameters of that tool. For the example of this tutorial, you will
need to modify the trimmomatic section in the .ini file. 
In the following example we will run trimmomatic. 

**5.** Set the preprocessing options in the pipeline.ini file to specify trimmomatic
::

    ################################################################
    ################################################################
    # preprocessing options
    ################################################################
    # these options will only work if you have run the 
    # readqc_pipeline previously
      
    # specify a comma seperated list of preprocessing tools to run
    # current options are:
    # trimmomatic
    # trimgalore
    # fastx_trimmer
    # sickle
    # flash
    # reversecomplement
    # pandaseq
    preprocessors=trimmomatic


**6.** Set the options for trimmomatic
::

    ################################################################
    ################################################################
    ##### trimmomatic options
    ################################################################
    [trimmomatic]
    # http://www.usadellab.org/cms/?page=trimmomatic
    #options
    options=LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:15  

    # if adapter removal is required, specify the location of a fasta file
    # containing adapters and define the adapter parameters
    # this variable will be overriden if auto_remove != 0
    # For the tutorial this is downloaded with the fastq files in the tar ball
    adapter=TruSeq2-PE.fa

    # max mismatches in adapter sequence  
    mismatches=5

    # accuracy thresholds. these values are (very roughly speaking) log-10
    # probabilities of getting a match at random
    # A threshold of 10 requires a perfect match of ~17bp (see above website)
    c_thresh=10

    # only used in paired end mode  
    p_thresh=30


**7.** Then run the pipeline to with the task ``full`` the regenerate the report::

   cgatflow readqc make full -v 5 --local
   cgatflow readqc make build_report -v 5


this completes the tutorial for running the readqc pipeline. 
