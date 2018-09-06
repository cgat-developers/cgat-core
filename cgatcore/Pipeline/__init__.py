'''Pipeline.py - Tools for ruffus pipelines
===========================================

The :mod:`Pipeline` module contains various utility functions for
interfacing CGAT ruffus pipelines with an HPC cluster, uploading data
to databases, providing parameterization, and more.

It is a collection of utility functions covering the topics:

* `Pipeline control`_
* `Logging`_
* `Parameterisation`_
* `Running tasks`_
* `Database upload`_
* `Report building`_

See :doc:`pipelines/pipeline_template` for a pipeline illustrating the
use of this module. See :ref:`PipelineSettingUp` on how to set up a
pipeline.

Pipeline control
----------------

:mod:`Pipeline` provides a :func:`main` function that provides command
line control to a pipeline. To use it, add::

    import CGAT.Pipeline as P
    # ...

    if __name__ == "__main__":
        sys.exit(P.main(sys.argv))

to your pipeline script. Typing::

    python my_pipeline.py --help

will provide the following output:

.. program-output:: python ../CGATPipelines/pipeline_template.py --help

Documentation on using pipelines is at :ref:`PipelineRunning`.

Logging
-------

Logging is set up by :func:`main`. Logging messages will be sent to
the file :file:`pipeline.log` in the current directory.  Additionally,
messages are sent to an RabbitMQ_ message exchange to permit
monitoring of pipeline progress.

Running tasks
-------------

:mod:`Pipeline` provides a :func:`Pipeline.run` method to control
running commandline tools. The :func:`Pipeline.run` method takes care
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
used in :mod:`Pipeline`.

Parameterisation
----------------

:mod:`Pipeline` provides hooks for reading pipeline configuration
values from :file:`.ini` files and making them available inside ruffus_
tasks. The fundamental usage is a call to :func:`getParamaters` with
a list of configuration files, typically::

    PARAMS = P.getParameters(
        ["%s/pipeline.ini" % os.path.splitext(__file__)[0],
         "../pipeline.ini",
         "pipeline.ini"])

The :mod:`Pipeline` module defines a global variable :data:`PARAMS`
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

Database upload
---------------

To assist with uploading data into a database, :mod:`Pipeline` provides
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

Report building
---------------

Once built, a report can be published by copying it to the publicly
visible directories on the CGAT systems. At the same time, references
to files on CGAT systems need to be replaced with links through the
public web interface. The function :func:`publish_report` implements
this functionality.

The function :meth:`publish_tracks` builds a UCSC track hub and moves
it into the appropriate CGAT download directories. The method
:func:`publish_notebooks` builds and exports the ipython_ notebooks
related to project.

For these methods to work, the code assumes a certain directory
layout. The method :func:`isCGAT` checks if the code is executed within the
CGAT systems. The functions :func:`getProjectDirectories`,
:func:`getPipelineName`, :func:`getProjectId`, :func:`getProjectName`
provide information about the pipeline executed and the project context.

.. note::

   The methods above are CGAT specific and require a particalur layout.

Package layout
--------------

The module is arranged as a python package with several submodules. Functions
within a submodule to be exported are all imported to the namespace of
:mod:`Pipeline`.

.. toctree::

   Pipeline/Control
   Pipeline/Database
   Pipeline/Execution
   Pipeline/Files
   Pipeline/Local
   Pipeline/Parameters
   Pipeline/Utils

Reference
---------

'''
import os

# import submodules into namespace
from cgatcore.Pipeline.Control import *
from cgatcore.Pipeline.Database import *
from cgatcore.Pipeline.Files import *
from cgatcore.Pipeline.Cluster import *
from cgatcore.Pipeline.Execution import *
from cgatcore.Pipeline.Utils import *
from cgatcore.Pipeline.Parameters import *

# PARAMS is defined in Parameters.py
# PARAMS = get_params()

# set working directory at process launch to prevent repeated calls to
# os.getcwd failing if network is busy
PARAMS["workingdir"] = os.getcwd()


# __all__ = [
#     # backwards incompatibility
#     "clone",
#     "touch",
#     "snip",
#     # Execution.py
#     "run",
#     "execute",
#     "shellquote",
#     "buildStatement",
#     "submit",
#     "joinStatements",
#     "cluster_runnable",
#     "run_pickled",
#     # Database.py
#     "tablequote",
#     "toTable",
#     "build_load_statement",
#     "load",
#     "concatenateAndLoad",
#     "mergeAndLoad",
#     "connect",
#     "createView",
#     "getDatabaseName",
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
#     "getPipelineName",
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
