'''pipeline.py - Tools for ruffus pipelines
===========================================

The :mod:`pipeline` module contains various utility functions for
interfacing CGAT ruffus pipelines with an HPC cluster, uploading data
to databases, providing parameterization, and more.

It is a collection of utility functions covering the topics:

* `pipeline control`_
* `Logging`_
* `Parameterisation`_
* `Running tasks`_
* `database upload`_
* `Report building`_

See :doc:`pipelines/pipeline_template` for a pipeline illustrating the
use of this module. See :ref:`pipelineSettingUp` on how to set up a
pipeline.

pipeline control
----------------

:mod:`pipeline` provides a :func:`main` function that provides command
line control to a pipeline. To use it, add::

    import cgatcore.pipeline as P
    # ...

    def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)


    if __name__ == "__main__":
        sys.exit(P.main(sys.argv))

to your pipeline script. Typing::

    python my_pipeline.py --help

will provide the following output:

    Usage:
    usage: <pipeline> [OPTIONS] [CMD] [target]

    Execute pipeline mapping.

    Commands can be any of the following

    make <target>
        run all tasks required to build *target*

    show <target>
        show tasks required to build *target* without executing them

    plot <target>
        plot image (using inkscape) of pipeline state for *target*

    debug <target> [args]
        debug a method using the supplied arguments. The method <target>
        in the pipeline is run without checking any dependencies.

    config
        write new configuration files pipeline.yml with default values

    dump
        write pipeline configuration to stdout

    touch
        touch files only, do not run

    regenerate
        regenerate the ruffus checkpoint file

    check
        check if requirements (external tool dependencies) are satisfied.

    clone <source>
        create a clone of a pipeline in <source> in the current
        directory. The cloning process aims to use soft linking to files
        (not directories) as much as possible.  Time stamps are
        preserved. Cloning is useful if a pipeline needs to be re-run from
        a certain point but the original pipeline should be preserved.



    Options:
    --version             show program's version number and exit
    -h, --help            show this help message and exit
    --pipeline-action=PIPELINE_ACTION
                        action to take [default=none].
    --pipeline-format=PIPELINE_FORMAT
                        pipeline format [default=svg].
    -n, --dry-run         perform a dry run (do not execute any shell commands)
                        [default=False].
    -c CONFIG_FILE, --config-file=CONFIG_FILE
                        benchmark configuration file [default=pipeline.yml].
    -f FORCE_RUN, --force-run=FORCE_RUN
                        force running the pipeline even if there are up-to-
                        date tasks. If option is 'all', all tasks will be
                        rerun. Otherwise, only the tasks given as arguments
                        will be rerun. [default=False].
    -p MULTIPROCESS, --multiprocess=MULTIPROCESS
                        number of parallel processes to use on submit host
                        (different from number of jobs to use for cluster
                        jobs) [default=none].
    -e, --exceptions      echo exceptions immediately as they occur
                        [default=True].
    -i, --terminate       terminate immediately at the first exception
                        [default=none].
    -d, --debug           output debugging information on console, and not the
                        logfile [default=False].
    -s VARIABLES_TO_SET, --set=VARIABLES_TO_SET
                        explicitely set paramater values [default=[]].
    --input-glob=INPUT_GLOBS, --input-glob=INPUT_GLOBS
                        glob expression for input filenames. The exact format
                        is pipeline specific. If the pipeline expects only a
                        single input, `--input-glob=*.bam` will be sufficient.
                        If the pipeline expects multiple types of input, a
                        qualifier might need to be added, for example
                        `--input-glob=bam=*.bam` --input-glob=bed=*.bed.gz`.
                        Giving this option overrides the default of a pipeline
                        looking for input in the current directory or
                        specified the config file. [default=[]].
    --checksums=RUFFUS_CHECKSUMS_LEVEL
                        set the level of ruffus checksums[default=0].
    -t, --is-test         this is a test run[default=False].
    --engine=ENGINE       engine to use.[default=local].
    --always-mount        force mounting of arvados keep [False]
    --only-info           only update meta information, do not run
                        [default=False].
    --work-dir=WORK_DIR   working directory. Will be created if it does not
                        exist [default=none].
    --input-validation    perform input validation before starting
                        [default=False].

    pipeline logging configuration:
    --pipeline-logfile=PIPELINE_LOGFILE
                        primary logging destination.[default=pipeline.log].
    --shell-logfile=SHELL_LOGFILE
                        filename for shell debugging information. If it is not
                        an absolute path, the output will be written into the
                        current working directory. If unset, no logging will
                        be output. [default=none].

     Script timing options:
     --timeit=TIMEIT_FILE
                        store timeing information in file [none].
     --timeit-name=TIMEIT_NAME
                        name in timing file for this class of jobs [all].
     --timeit-header     add header for timing information [none].

     Common options:
     --random-seed=RANDOM_SEED
                        random seed to initialize number generator with
                        [none].
     -v LOGLEVEL, --verbose=LOGLEVEL
                        loglevel [1]. The higher, the more output.
     --log-config-filename=LOG_CONFIG_FILENAME
                        Configuration file for logger [logging.yml].
     --tracing=TRACING   enable function tracing [none].
     -?                  output short help (command line options only.

     cluster options:
      --no-cluster, --local
                        do no use cluster - run locally [False].
      --cluster-priority=CLUSTER_PRIORITY
                        set job priority on cluster [none].
      --cluster-queue=CLUSTER_QUEUE
                        set cluster queue [none].
      --cluster-num-jobs=CLUSTER_NUM_JOBS
                        number of jobs to submit to the queue execute in
                        parallel [none].
      --cluster-parallel=CLUSTER_PARALLEL_ENVIRONMENT
                        name of the parallel environment to use [none].
      --cluster-options=CLUSTER_OPTIONS
                        additional options for cluster jobs, passed on to
                        queuing system [none].
      --cluster-queue-manager=CLUSTER_QUEUE_MANAGER
                        cluster queuing system [sge].
      --cluster-memory-resource=CLUSTER_MEMORY_RESOURCE
                        resource name to allocate memory with [none].
      --cluster-memory-default=CLUSTER_MEMORY_DEFAULT
                        default amount of memory to allocate [unlimited].

    Input/output options:
     -I FILE, --stdin=FILE
                        file to read stdin from [default = stdin].
     -L FILE, --log=FILE
                        file with logging information [default = stdout].
     -E FILE, --error=FILE
                        file with error information [default = stderr].
     -S FILE, --stdout=FILE
                        file where output is to go [default = stdout].


Documentation on using pipelines is at :ref:`getting_started-Examples`.

Logging
-------

Logging is set up by :func:`main`. Logging messages will be sent to
the file :file:`pipeline.log` in the current directory.  Additionally,
messages are sent to an RabbitMQ_ message exchange to permit
monitoring of pipeline progress.

Running tasks
-------------

:mod:`pipeline` provides a :func:`pipeline.run` method to control
running commandline tools. The :func:`pipeline.run` method takes care
of distributing these tasks to the cluster. It takes into
consideration command line options such as ``--cluster-queue``. The
command line option ``--local`` will run jobs locally for testing
purposes.

For running Python code that is inside a module in a distributed
function, use the :func:`submit` function. The :func:`execute` method
runs a command locally.

Functions such as :func:`shellquote`, :func:`getCallerLocals`,
:func:`getCaller`, :func:`buildStatement`, :func:`expandStatement`,
:func:`joinStatements` support the parameter interpolation mechanism
used in :mod:`pipeline`.

Parameterisation
----------------

:mod:`pipeline` provides hooks for reading pipeline configuration
values from :file:`.ini` files and making them available inside ruffus_
tasks. The fundamental usage is a call to :func:`getParamaters` with
a list of configuration files, typically::

    # load options from the config file
    P.get_parameters(
        ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
        "../pipeline.yml",
        "pipeline.yml"])

The :mod:`pipeline` module defines a global variable :data:`PARAMS`
that provides access the configuration values. To get a handle to
this variable outside a pipeline script, call :func:`getParams`::

    my_cmd = "%(scripts_dir)s/bam2bam.py" % P.getParams()

Functions such as :func:`configToDictionary`, :func:`loadParameters`
:func:`matchParameter`, :func:`substituteParameters` support this
functionality.

Functions such as :func:`asList` and :func:`isTrue` are useful to work
with parameters.

The method :func:`peekParameters` allows one to programmatically read the
parameters of another pipeline.

Temporary files
---------------

Tasks containg multiple steps often require temporary memory storage
locations.  The functions :func:`getTempFilename`, :func:`getTempFile`
and :func:`getTempDir` provide these. These functions are aware of the
temporary storage locations either specified in configuration files or
on the command line and distinguish between the ``private`` locations
that are visible only within a particular compute node, and ``shared``
locations that are visible between compute nodes and typically on a
network mounted location.

Requirements
------------

The methods :func:`checkExecutables`, :func:`checkScripts` and
:func:`checkParameter` check for the presence of executables, scripts
or parameters. These methods are useful to perform pre-run checks
inside a pipeline if a particular requirement is met. But see also the
``check`` commandline command.

database upload
---------------

To assist with uploading data into a database, :mod:`pipeline` provides
several utility functions for conveniently uploading data. The :func:`load`
method uploads data in a tab-separated file::

    @transform("*.tsv.gz", suffix(".tsv.gz"), ".load")
    def loadData(infile, outfile):
        P.load(infile, outfile)

The methods :func:`mergeAndLoad` and :func:`concatenateAndLoad` upload
multiple files into same database by combining them first. The method
:func:`createView` creates a table or view derived from other tables
in the database. The function :func:`importFromIterator` uploads
data from a python list or other iterable directly.

The functions :func:`tablequote` and :func:`toTable` translate track
names derived from filenames into names that are suitable for tables.

The method :func:`build_load_statement` can be used to create an
upload command that can be added to command line statements to
directly upload data without storing an intermediate file.

The method :func:`connect` returns a database handle for querying the
database.

Package layout
--------------

The module is arranged as a python package with several submodules. Functions
within a submodule to be exported are all imported to the namespace of
:mod:`pipeline`.

.. toctree::

   cgatcore.pipeline.control
   cgatcore.pipeline.database
   cgatcore.pipeline.execution
   cgatcore.pipeline.files
   cgatcore.pipeline.parameters
   cgatcore.pipeline.utils


'''
import os

# import submodules into namespace
from cgatcore.pipeline.control import *
from cgatcore.pipeline.database import *
from cgatcore.pipeline.files import *
from cgatcore.pipeline.cluster import *
from cgatcore.pipeline.execution import *
from cgatcore.pipeline.utils import *
from cgatcore.pipeline.parameters import *


# __all__ = [
#     # backwards incompatibility
#     "clone",
#     "touch",
#     "snip",
#     # execution.py
#     "run",
#     "execute",
#     "shellquote",
#     "buildStatement",
#     "submit",
#     "joinStatements",
#     "cluster_runnable",
#     "run_pickled",
#     # database.py
#     "tablequote",
#     "toTable",
#     "build_load_statement",
#     "load",
#     "concatenateAndLoad",
#     "mergeAndLoad",
#     "connect",
#     "createView",
#     "getdatabaseName",
#     "importFromIterator",
#     # Utils.py
#     "add_doc",
#     "isTest",
#     "getCallerLocals",
#     "getCaller",
#     # Control.py
#     "main",
#     "peekParameters",
#     # Files.py
#     "getTempFile",
#     "getTempDir",
#     "getTempFilename",
#     "checkScripts",
#     "checkExecutables",
#     # Local.py
#     "run_report",
#     "publish_report",
#     "publish_notebooks",
#     "publish_tracks",
#     "getProjectDirectories",
#     "getpipelineName",
#     "getProjectId",
#     "getProjectName",
#     "isCGAT",
#     # Parameters.py
#     "getParameters",
#     "loadParameters",
#     "matchParameter",
#     "substituteParameters",
#     "asList",
#     "checkParameter",
#     "isTrue",
#     "configToDictionary",
# ]
