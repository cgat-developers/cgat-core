.. _defining_workflow-Writing_workflow:

==================
Writing a workflow
==================


.. _defining_workflow-philosophy:

Our workflow philosophy
-----------------------

The explicit aim of CGAT-core is to allow users to quickly and easily build their own computational pipelines that will speed up your analysis workflow.

When building pipelines it is often useful to keep in mind the following philosophy:

Flexibility
    There are always new tools and insights that could be incorporated into a pipeline. Ultimately, a pipeline should be flexible and the code should not constrain you when implimenting new features.
Scriptability
    The pipeline should be scriptable, i.e, the whole pipeline can be run within another pipeline. Similarly, parts of a pipeline can be duplicated to process several data streams in parallel. This is a crucial feature in genome studies as a single analysis will not permit making inferences by itself. When we write a pipeline we usually attempt to write a command line script (and include it in the CGAT-apps repository) and then run this script as a command line statement in the pipeline.
Reproducibility
    The pipeline is fully automated. The same inputs and configuration will produce the same outputs.
Reusability
    The pipeline should be able to be re-used on similar data, preferably only requiring changes to a configuration file (pipeline.yml).
Archivability
    Once finished, the whole project should be able to be archived without too many major dependencies on external data. This should be a simple process and hence all project data should be self-contained. It should not involve going through various directories or databases to figure out which files and tables belong to a project or a project depends on.

.. _defining_workflow-building:

Building a pipeline
-------------------

The best way to build a pipeline is to start from an example. In `cgat-showcase <https://cgat-showcase.readthedocs.io/en/latest/index.html>`_ we have a toy example of an RNA-seq 
analysis pipeline that aims to show users how simple workflows can be generated with minimal code. `cgat-flow <https://github.com/cgat-developers/cgat-flow>`_ demonstrates a set of complex workflows. 

For a step by step tutorial on how to run the pipelines please refer to our :ref:`getting_started-Tutorial`.

For help on how to construct pipelines from scratch please continue reading for more information.

In an empty directory you will need to make a new directory and then a python file
with the same name. For example::

   mkdir test && touch pipeline_test.py

All pipelines require a yml configuration file that will allow you to add configurable values to modify the behaviour of your code.
This is placed within the test/ directory, which should have the same name as the name of your pipeline_test.py file::

   touch test/pipeline.yml

In order to help with debugging and reading our code, our pipelines are written so that
a pipeline task file contains Ruffus tasks and calls functions in an associated module file,
which contains all of the code to transform and analyse the data.

Therefore, if you wish to create a module file, we usually save this file in the following convention,
``ModuleTest.py`` and it can be imported into the main pipeline task file (``pipeline_test.py``)as:

.. code-block:: python

   import ModuleTest

This section describes how pipelines can be constructed using the
:mod:`pipeline` module in cgat-core. The `pipeline <https://github.com/cgat-developers/cgat-core/tree/master/cgatcore/pipeline>`_ module contains a variety of
useful functions for pipeline construction.

.. _defining_workflow-p-input:

pipeline input
--------------

Pipelines are executed within a dedicated working
directory. They usually require the following files within this
directory:

   * a pipeline configuration file :file:`pipeline.yml`
   * input data files, usually listed in the documentatuion of each pipeline

Other files that might be used in a pipeline are:

   * external data files such as genomes that a referred to by they their full path name.

The pipelines will work from the input files in the working
directory, usually identified by their suffix. For example, a
mapping pipeline might look for any ``*.fastq.gz`` files in the
directory, run QC on these and map the reads to a genome sequence etc.

.. _defining_workflow-p-output:

pipeline output 
----------------

The pipeline will create files and database tables in the
working directory.  When building a pipeline, you can choose
any file/directory layout that suits your needs. Some prefer flat
hierarchies with many files, while others prefer deep directories.

.. _defining_workflow-guidelines:

Guidelines
----------

To preserve disk space, we always use compressed files as
much as possible.  Most data files compress very well, for example
fastq files often compress by a factor of 80% or more: a 10Gb file
will use just 2Gb.

Working with compressed files is straight-forward using unix pipes and
the commands ``gzip``, ``gunzip`` or ``zcat``.

If you require random access to a file, load the file into the
database and index it appropriately. Genomic interval files can be
indexed with tabix to allow random access.

.. _pipelineCommands:


Import statements
-----------------

In order to run our pipelines you will need to import the cgatcore python
modules into your pipeline. For every CGAT pipeline we recommend importing the
basic modules as follows. Then any additional modules can be imported as required.

.. code-block:: python

   from ruffus import *
   import cgatcore.experiment as E
   from cgatcore import pipeline as P
   import cgatcore.iotools as iotools

Selecting the appropriate Ruffus decorator
------------------------------------------

Before starting to write a pipeline it is always best to map out
on a whiteboard the the steps and flow of your potential pipeline. This will allow you
to identify the input and outputs of each task. Once you have assessed this then the next step is
to identify which Ruffus decorator you require. Documentation on each decorator can be found in the
`ruffus documentation <http://www.ruffus.org.uk/decorators/decorators.html>`_



Running commands within tasks
-----------------------------

To run a command line program within a pipeline task, build a
statement and call the :meth:`pipeline.run` method::

   @transform( '*.unsorted', suffix('.unsorted'), '.sorted')
   def sortFile( infile, outfile ):

       statement = '''sort %(infile)s > %(outfile)s'''
       P.run(statement)

On calling the :meth:`pipeline.run` method, the environment of the
caller is examined for a variable called ``statement``. The variable
is subjected to string substitution from other variables in the local
namespace. In the example above, ``%(infile)s`` and ``%(outfile)s``
are substituted with the values of the variables ``infile`` and
``outfile``, respectively.

The same mechanism also permits setting configuration parameters, for example::

   @transform( '*.unsorted', suffix('.unsorted'), '.sorted')
   def sortFile( infile, outfile ):

       statement = '''sort -t %(tmpdir)s %(infile)s > %(outfile)s'''
       P.run(statement)

will automatically substitute the configuration parameter ``tmpdir``
into the command. See ConfigurationValues_ for more on using configuration
parameters.

The pipeline will stop and return an error if the command exits with an error code.

If you chain multiple commands, only the return value of the last
command is used to check for an error. Thus, if an upstream command
fails, it will go unnoticed.  To detect these errors, insert
``&&`` between commands. For example::

   @transform( '*.unsorted.gz', suffix('.unsorted.gz'), '.sorted)
   def sortFile( infile, outfile ):

       statement = '''gunzip %(infile)s %(infile)s.tmp &&
		      sort -t %(tmpdir)s %(infile)s.tmp > %(outfile)s &&
		      rm -f %(infile)s.tmp
       P.run(statement)

Of course, the statement aboved could be executed more efficiently
using pipes::

   @transform( '*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
   def sortFile( infile, outfile ):

       statement = '''gunzip < %(infile)s 
		      | sort -t %(tmpdir)s 
		      | gzip > %(outfile)s'''
       P.run(statement)

The pipeline inserts code automatically to check for error return
codes if multiple commands are combined in a pipe.

Running commands on the cluster
-------------------------------

In order to run commands on cluster, use ``to_cluster=True``.

To run the command from the previous section on the cluster::

   @files( '*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
   def sortFile( infile, outfile ):

       to_cluster = True
       statement = '''gunzip < %(infile)s 
		      | sort -t %(tmpdir)s 
		      | gzip > %(outfile)s'''
       P.run(statement)

The pipeline will automatically create the job submission files,
submit the job to the cluster and wait for its return.

pipelines will use the command line options ``--cluster-queue``,
``--cluster-priority``, etc. for global job control. For example, to
change the priority when starting the pipeline, use::

   python <pipeline_script.py> --cluster-priority=-20

To set job options specific to a task, you can define additional
variables::

   @files( '*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
   def sortFile( infile, outfile ):

       to_cluster = True
       job_queue = 'longjobs.q'
       job_priority = -10
       job_options= "-pe dedicated 4 -R y" 
 
       statement = '''gunzip < %(infile)s 
		      | sort -t %(tmpdir)s 
		      | gzip > %(outfile)s'''
       P.run(statement)

The above statement will be run in the queue ``longjobs.q`` at a
priority of ``-10``.  Additionally, it will be executed in the
parallel environment ``dedicated`` with at least 4 cores.

Array jobs can be controlled through the ``job_array`` variable::

   @files( '*.in', suffix('.in'), '.out')
   def myGridTask( infile, outfile ):

       job_array=(0, nsnps, stepsize)
   
       statement = '''grid_task.bash %(infile)s %(outfile)s
          > %(outfile)s.$SGE_TASK_ID 2> %(outfile)s.err.$SGE_TASK_ID
       '''
       P.run(statement)


Note that the :file:`grid_task.bash` file must be grid engine
aware. This means it makes use of the :envvar:`SGE_TASK_ID`,
:envvar:`SGE_TASK_FIRST`, :envvar:`SGE_TASK_LAST` and
:envvar:`SGE_TASK_STEPSIZE` environment variables to select the chunk
of data it wants to work on.

The job submission files are files called `tmp*` in the :term:`working
directory`.  These files will be deleted automatically. However, the
files will remain after aborted runs to be cleaned up manually.

.. _defining_workflow-databases:


Useful information regarding decorators
---------------------------------------

To see a full list of ruffus decorators that control the flow of the pipeline please
see the `ruffus documentation <http://www.ruffus.org.uk/decorators/decorators.html>`_.

However, during peer review it was pointed out that it would be helpful to include a few examples of
how you can modify the infile name and transform it to the output filename. There are a few ways of doing this:

The first way is to capture the suffix so the outfile is placed into the same folder as the infile::

  # pairs are a tuple of read pairs (read1, read2) 
  @transform(pairs,
             suffix(.fastq.gz),
	     ("_trimmed.fastq.gz", "_trimmed.fastq.gz"))

This will transform an input <name_of_file>.fastq.gz and result in an output
with a new siffix <name_of_file>_trimmed.fastq.gz.

Another way to add a output file into aother filer is to use a regex::

   @follows(mkdir("new_folder.dir"))
   @transform(pairs,
             regex((\S+).fastq.gz),
	     (r"new_folder.dir/\1_trimmed.fastq.gz", r"new_folder.dir/\1_trimmed.fastq.gz"))

This can also be achieved using the formatter function::

  @follows(mkdir("new_folder.dir"))
   @transform(pairs,
             formatter((\S+).fastq.gz),
	     ("new_folder.dir/{SAMPLE[0]}_trimmed.fastq.gz", r"new_folder.dir/{SAMPLE[0]}_trimmed.fastq.gz"))


Combining commands together
---------------------------

In order to combine commands together you will need to use `&&`
to make sure your commands are chained correctly. For example::

  statement = """
              module load cutadapt &&
	      cutadapt ....
              """

  P.run(statement)
  
If you didnt have the `&&` then the command will fail because the cutadapt command will be
executed as part of the module load statement.
	     
Databases
---------

Loading data into the database
==============================

:mod:`pipeline.py` offers various tools for working with databases. By
default, it is configured to use an sqlite3 database in the
:term:`working directory` called :file:`csvdb`.

Tab-separated output files can be loaded into a table using the
:meth:`pipeline.load` function. For example::

   @jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
   @transform('data_*.tsv.gz', suffix('.tsv.gz'), '.load')
   def loadTables(infile, outfile):
      P.load(infile, outfile)

The task above will load all tables ending with ``tsv.gz`` into the
database Table names are given by the filenames, i.e, the data in
:file:`data_1.tsv.gz` will be loaded into the table :file:`data_1`.

The load mechanism uses the script :file:`csv2db.py` and can be
configured using the configuration options in the ``database`` section
of :file:`pipeline.ini`. Additional options can be given via the
optional *options* argument::

   @jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
   @transform('data_*.tsv.gz', suffix('.tsv.gz'), '.load')
   def loadTables( infile, outfile ):
      P.load(infile, outfile, "--add-index=gene_id")

In order for the load mechanism to be transparent, it is best avoided
to call the :file:`csv2db.py` script directly. Instead, use the
:meth:`pipeline.load` function. If the :file:`csv2db.py` needs to
called at the end of a succession of statements, use the
:meth:`pipeline.build_load_statement` method, for example::

   def loadTranscript2Gene(infile, outfile):
       '''build and load a map of transcript to gene from gtf file
       '''
       load_statement = P.build_load_statement(
	   P.toTable(outfile),
	   options="--add-index=gene_id "
	   "--add-index=transcript_id ")

       statement = '''
       gunzip < %(infile)s
       | python %(scriptsdir)s/gtf2tsv.py --output-map=transcript2gene -v 0
       | %(load_statement)s
       > %(outfile)s'''
       P.run()

See also the variants :meth:`pipeline.mergeAndLoad` and
`:meth:`pipeline.concatenateAndLoad` to combine multiple tables and
upload to the database in one go.

Connecting to a database
========================

To use data in the database in your tasks, you need to first connect
to the database. The best way to do this is via the connect() method
in pipeline.py.

The following example illustrates how to use the connection::

    @transform( ... )
    def buildCodingTranscriptSet( infile, outfile ):

	dbh = connect()

	statement = '''SELECT DISTINCT transcript_id FROM transcript_info WHERE transcript_biotype = 'protein_coding' '''
	cc = dbh.cursor()
	transcript_ids = set( [x[0] for x in cc.execute(statement)] )
	...

.. _pipelineReports:

Reports
-------

MultiQC
=======

When using cgat-core to build pipelines we recomend using `MultiQC <http://multiqc.info/>`_ 
as the default reporting tool for generic thrid party computational biology software.

To run multiQC in our pipelines you only need to run a statement as a commanline
task. For example we impliment this in our pipelines as::

    @follows(mkdir("MultiQC_report.dir"))
    @originate("MultiQC_report.dir/multiqc_report.html")
    def renderMultiqc(infile):
    '''build mulitqc report'''

    statement = '''LANG=en_GB.UTF-8 multiqc . -f;
                   mv multiqc_report.html MultiQC_report.dir/'''

    P.run(statement)


Rmarkdown
=========

MultiQC is very useful for running third generation computational biology tools. However, currently
it is very difficult to use it as a bespoke reporting tool. Therefore, one was of running
bespoke reports is using the Rmarkdown framework and using the render functionality of knitr.

Rendering an Rmarkdown document is very easy if you place the .Rmd file in the same test/ directory as the pipeline.yml.
Then the file can easily run using::

    @follows(mkdir("Rmarkdown.dir"))
    @originate("Rmarkdown.dir/report.html")
    def render_rmarkdown(outfile):

    NOTEBOOK_ROOT = os.path.join(os.path.dirname(__file__), "test")

    statement = '''cp %(NOTEBOOK_ROOT)s/report.Rmd Rmarkdown.dir &&
                   cd Rmarkdown.dir/ && R -e "rmarkdown::render('report.Rmd',encoding = 'UTF-8')" '''

    P.run(statement)

This should generate an html output of whatever report your wrote for your particular task.


Jupyter notebook
================

Another bespoke reporting that we also perform for our pipelines is to use a Jupyter notebook
implimentation and execture it in using the commandline. All that is required is that you
place your jupyter notebook into the same test/ directory as the pipeline.yml and call the following::

    @follows(mkdir("jupyter_report.dir"))
    @originate("jupyter_report.dir/report.html")
    def render_jupyter(outfile):
    
    NOTEBOOK_ROOT = os.path.join(os.path.dirname(__file__), "test")

    statement = '''cp %(NOTEBOOK_ROOT)s/report.ipynb jupyter_report.dir/ && cd jupyter_report.dir/ &&
                    jupyter nbconvert --ExecutePreprocessor.timeout=None --to html --execute *.ipynb --allow-errors;

    P.run(statement)


.. _ConfigurationValues:

Configuration values
--------------------

Setting up configuration values
===============================

There are different ways to pass on configuration values to pipelines.
Here we explain the priority for all the possible options so you can
choose the best one for your requirements.

The pipeline goes *in order* through different configuration options
to load configuration values and stores them in the :py:data:`PARAMS`
dictionary. This order determines a priority so values read in the first
place can be overwritten by values read in subsequent steps; i.e. values
read lastly have higher priority.

Here is the order in which the configuration values are read:

1. Hard-coded values in :file:`cgatcore/pipeline/parameters.py`.
2. Parameters stored in :file:`pipeline.yml` files in different locations.
3. Variables declared in the ruffus tasks calling ``P.run()``;
   e.g. ``job_memory=32G``
4. :file:`.cgat.yml` file in the home directory
5. ``cluster_*`` options specified in the command line;
   e.g. ``python pipeline_example.py --cluster-parallel=dedicated make full``


This means that configuration values for the cluster provided as
command-line options will have the highest priority. Therefore::

   python pipeline_example.py --cluster-parallel=dedicated make full

will overwrite any ``cluster_parallel`` configuration values given
in :file:`pipeline.yml` files. Type::

   python pipeline_example.py --help

to check the full list of available command-line options.

You are encouraged to include the following snippet at the beginning
of your pipeline script to setup proper configuration values for
your analyses::

   # load options from the config file
   from cgatcore import pipeline as P
   # load options from the config file
   P.get_parameters(
    ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
     "../pipeline.yml",
     "pipeline.yml"])

The method :meth:`pipeline.getParameters` reads parameters from
the :file:`pipeline.yml` located in the current :term:`working directory`
and updates :py:data:`PARAMS`, a global dictionary of parameter values.
It automatically guesses the type of parameters in the order of ``int()``,
``float()`` or ``str()``. If a configuration variable is empty (``var=``),
it will be set to ``None``.

However, as explained above, there are other :file:`pipeline.yml`
files that are read by the pipeline at start up. In order to get the
priority of them all, you can run::

   python pipeline_example.py printconfig

to see a complete list of :file:`pipeline.yml` files and their priorities.


Using configuration values
==========================

Configuration values are accessible via the :py:data:`PARAMS`
variable. The :py:data:`PARAMS` variable is a dictionary mapping
configuration parameters to values. Keys are in the format
``section_parameter``. For example, the key ``bowtie_threads`` will
provide the configuration value of::

   bowtie:
       threads: 4

In a script, the value can be accessed via
``PARAMS["bowtie_threads"]``.

Undefined configuration values will throw a :class:`ValueError`. To
test if a configuration variable exists, use::

   if 'bowtie_threads' in PARAMS: pass
      
To test, if it is unset, use::

   if 'bowie_threads' in PARAMS and not PARAMS['botwie_threads']:
      pass

Task specific parameters
------------------------

Task specific parameters can be set by creating a task specific section in
the :file:`pipeline.yml`. The task is identified by the output filename.
For example, given the following task::

   @files( '*.fastq', suffix('.fastq'), '.bam')
   def mapWithBowtie( infile, outfile ):
      ...

and the files :file:`data1.fastq` and :file:`data2.fastq` in the
:term:`working directory`, two output files :file:`data.bam` and
:file:`data2.bam` will be created on executing ``mapWithBowtie``. Both
will use the same parameters. To set parameters specific to the
execution of :file:`data1.fastq`, add the following to
:file:`pipeline.yml`::

   data1.fastq:
       bowtie_threads: 16

This will set the configuration value ``bowtie_threads`` to 16 when
using the command line substitution method in :meth:`pipeline.run`. To
get an task-specific parameter values in a python task, use::

   @files( '*.fastq', suffix('.fastq'), '.bam')
   def mytask( infile, outfile ):
       MY_PARAMS = P.substitute_parameters( locals() )
       
Thus, task specific are implemented generically using the
:meth:`pipeline.run` mechanism, but pipeline authors need to
explicitely code for track specific parameters.

Using different conda environments
----------------------------------

In addition to running a pipeline using your default conda environment, specifying `job_condaenv="<name of conda environment>"` to the
P.run() function allows you run the statement using a different conda environment. For example::

    @follows(mkdir("MultiQC_report.dir"))
    @originate("MultiQC_report.dir/multiqc_report.html")
    def renderMultiqc(infile):
    '''build mulitqc report'''

    statement = '''LANG=en_GB.UTF-8 multiqc . -f;
                   mv multiqc_report.html MultiQC_report.dir/'''

    P.run(statement, job_condaenv="multiqc")

This can be extremely useful when you have python 2 only code but are running in a python 3 environment. Or
more importantly, when you have conflicting dependancies in software and you need to seperate them out into
two different environments.xs
