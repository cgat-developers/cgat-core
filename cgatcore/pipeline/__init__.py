'''
pipeline.py - Tools for CGAT Ruffus Pipelines
=============================================

This module provides a comprehensive set of tools to facilitate the creation and management
of data processing pipelines using CGAT Ruffus. It includes functionalities for pipeline control,
logging, parameterization, task execution, database uploads, temporary file management, and
integration with AWS S3.

**Features:**

- **Pipeline Control:** Command-line interface for executing, showing, and managing pipeline tasks.
- **Logging:** Configures logging to files and RabbitMQ for real-time monitoring.
- **Parameterization:** Loads and manages configuration parameters from various files.
- **Task Execution:** Manages the execution of tasks, supporting both local and cluster environments.
- **Database Upload:** Utilities for uploading processed data to databases.
- **Temporary File Management:** Functions to handle temporary files and directories.
- **AWS S3 Integration:** Support for processing files stored in AWS S3.

**Example Usage:**

```python
from cgatcore import pipeline as P

@P.transform("input.txt", suffix(".txt"), ".processed.txt")
def process_data(infile, outfile):
    # Processing logic here
    pass

if __name__ == "__main__":
    P.main()

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

    @P.transform("*.tsv.gz", suffix(".tsv.gz"), ".load")
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
# cgatcore/pipeline/__init__.py



# Import existing pipeline functionality
from cgatcore.pipeline.control import *
from cgatcore.pipeline.database import *
from cgatcore.pipeline.files import *
from cgatcore.pipeline.cluster import *
from cgatcore.pipeline.execution import *
from cgatcore.pipeline.utils import *
from cgatcore.pipeline.parameters import *

# Import original Ruffus decorators
from ruffus import (
    transform,
    merge,
    split,
    originate,
    follows
)

# Import S3-related classes and functions
from cgatcore.remote.file_handler import S3Pipeline, S3Mapper, s3_path_to_local, suffix

# Create a global instance of S3Pipeline
s3_pipeline = S3Pipeline()

# Expose S3-aware decorators via the S3Pipeline instance
s3_transform = s3_pipeline.s3_transform
s3_merge = s3_pipeline.s3_merge
s3_split = s3_pipeline.s3_split
s3_originate = s3_pipeline.s3_originate
s3_follows = s3_pipeline.s3_follows

# Expose S3Mapper instance if needed elsewhere
s3_mapper = s3_pipeline.s3

# Expose S3 configuration function
configure_s3 = s3_pipeline.configure_s3

# Update __all__ to include both standard and S3-aware decorators and functions
__all__ = [
    'transform', 'merge', 'split', 'originate', 'follows',
    's3_transform', 's3_merge', 's3_split', 's3_originate', 's3_follows',
    'S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix',
    's3_mapper', 'configure_s3'
]
# Add a docstring for the module
__doc__ = """
This module provides pipeline functionality for cgat-core, including support for AWS S3.

It includes both standard Ruffus decorators and S3-aware decorators. The S3-aware decorators
can be used to seamlessly work with both local and S3-based files in your pipelines.

Example usage:

from cgatcore import pipeline as P

# Using standard Ruffus decorator (works as before)
@P.transform("input.txt", suffix(".txt"), ".processed")
def process_local_file(infile, outfile):
    # Your processing logic here
    pass

# Using S3-aware decorator
@P.s3_transform("s3://my-bucket/input.txt", suffix(".txt"), ".processed")
def process_s3_file(infile, outfile):
    # Your processing logic here
    pass

# Configure S3 credentials if needed
P.configure_s3(aws_access_key_id="YOUR_KEY", aws_secret_access_key="YOUR_SECRET")
"""

