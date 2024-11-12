# Execution Module

`execution.py` - Job control for cgatcore pipelines

=========================================================

This module manages the job execution for cgatcore pipelines, particularly using DRMAA sessions for cluster job management. It provides functionality to start and close DRMAA sessions, execute shell commands, manage job submission, and handle different cluster environments. Supported cluster types include SGE, SLURM, Torque, and Kubernetes.

## Reference

The following documentation details the execution management utilities provided by the `execution.py` module.

## Import Statements

```python
import collections
import importlib
import os
import pickle
import re
import json
import stat
import socket
import logging
import subprocess
import sys
import time
import math
import shutil
import gevent
import signal
import cgatcore.experiment as E
import cgatcore.iotools as iotools
from cgatcore.pipeline.utils import get_caller_locals, get_caller, get_calling_function
from cgatcore.pipeline.files import get_temp_filename, get_temp_dir
from cgatcore.pipeline.parameters import substitute_parameters, get_params
from cgatcore.pipeline.cluster import get_queue_manager, JobInfo
from cgatcore.pipeline.executors import SGEExecutor, SlurmExecutor, TorqueExecutor, LocalExecutor
try:
    from cgatcore.pipeline.kubernetes import KubernetesExecutor
except ImportError:
    KubernetesExecutor = None  # Fallback if Kubernetes is not available
```

## Key Functions

### `start_session()`

Starts and initializes the global DRMAA session.

```python
def start_session():
    """Start and initialize the global DRMAA session."""
    global GLOBAL_SESSION

    if HAS_DRMAA and GLOBAL_SESSION is None:
        GLOBAL_SESSION = drmaa.Session()
        try:
            GLOBAL_SESSION.initialize()
        except drmaa.errors.InternalException as ex:
            get_logger().warn("could not initialize global drmaa session: {}".format(ex))
            GLOBAL_SESSION = None
        return GLOBAL_SESSION
```

### `close_session()`

Closes the global DRMAA session.

```python
def close_session():
    """Close the global DRMAA session."""
    global GLOBAL_SESSION

    if GLOBAL_SESSION is not None:
        GLOBAL_SESSION.exit()
        GLOBAL_SESSION = None
```

### `get_executor(options=None)`

Returns an executor instance based on the specified queue manager in the options.

Arguments:
- `options` (dict): Dictionary containing execution options, including `"cluster_queue_manager"`.

Returns:
- Executor instance appropriate for the specified queue manager.

This function decides which executor to use depending on the queue manager specified in the options, defaulting to the local executor if no cluster is specified or the cluster is not supported.

```python
def get_executor(options=None):
    if options is None:
        options = get_params()

    if options.get("testing", False):
        return LocalExecutor(**options)

    if not options.get("to_cluster", True):
        return LocalExecutor(**options)

    queue_manager = options.get("cluster_queue_manager", None)

    if queue_manager == "kubernetes" and KubernetesExecutor is not None:
        return KubernetesExecutor(**options)
    elif queue_manager == "sge" and shutil.which("qsub") is not None:
        return SGEExecutor(**options)
    elif queue_manager == "slurm" and shutil.which("sbatch") is not None:
        return SlurmExecutor(**options)
    elif queue_manager == "torque" and shutil.which("qsub") is not None:
        return TorqueExecutor(**options)
    else:
        return LocalExecutor(**options)
```

### `execute(statement, **kwargs)`

Executes a command line statement locally.

Arguments:
- `statement` (string): Command line statement to be run.

Returns:
- `stdout` (string): Data sent to standard output by the command.
- `stderr` (string): Data sent to standard error by the command.

```python
def execute(statement, **kwargs):
    if not kwargs:
        kwargs = get_caller_locals()

    kwargs = dict(list(get_params().items()) + list(kwargs.items()))

    logger = get_logger()
    logger.info("running %s" % (statement % kwargs))

    if "cwd" not in kwargs:
        cwd = get_params()["work_dir"]
    else:
        cwd = kwargs["cwd"]

    statement = " ".join(re.sub("\t+", " ", statement).split("\n")).strip()
    if statement.endswith(";"):
        statement = statement[:-1]

    os.environ.update({'BASH_ENV': os.path.join(os.environ['HOME'], '.bashrc')})
    process = subprocess.Popen(statement % kwargs,
                               cwd=cwd,
                               shell=True,
                               stdin=sys.stdin,
                               stdout=sys.stdout,
                               stderr=sys.stderr,
                               env=os.environ.copy(),
                               executable="/bin/bash")

    stdout, stderr = process.communicate()

    if process.returncode != 0:
        raise OSError(
            "Child was terminated by signal %i: \n"
            "The stderr was: \n%s\n%s\n" %
            (-process.returncode, stderr, statement))

    return stdout, stderr
```

### `run(statement, **kwargs)`

Runs a command line statement, either locally or on a cluster using DRMAA.

Arguments:
- `statement` (string or list of strings): Command line statement or list of statements to execute.
- `kwargs` (dict): Additional options for job execution.

This function runs the given statement(s) by selecting the appropriate executor. It handles different types of job submission (local or cluster-based) based on the provided arguments and global configuration.

```python
def run(statement, **kwargs):
    """
    Run a command line statement.
    """
    logger = get_logger()

    options = dict(list(get_params().items()))
    caller_options = get_caller_locals()
    options.update(list(caller_options.items()))

    if "self" in options:
        del options["self"]
    options.update(list(kwargs.items()))

    if "params" in options:
        try:
            options.update(options["params"]._asdict())
        except AttributeError:
            pass

    options['cluster']['options'] = options.get(
        'job_options', options['cluster']['options'])
    options['cluster']['queue'] = options.get(
        'job_queue', options['cluster']['queue'])
    options['without_cluster'] = options.get('without_cluster')

    name_substrate = str(options.get("outfile", "cgatcore"))
    if os.path.basename(name_substrate).startswith("result"):
        name_substrate = os.path.basename(os.path.dirname(name_substrate))
    else:
        name_substrate = os.path.basename(name_substrate)

    options["job_name"] = re.sub("[:"]", "_", name_substrate)
    try:
        calling_module = get_caller().__name__
    except AttributeError:
        calling_module = "unknown"

    options["task_name"] = calling_module + "." + get_calling_function()

    if isinstance(statement, list):
        statement_list = [interpolate_statement(stmt, options) for stmt in statement]
    else:
        statement_list = [interpolate_statement(statement, options)]

    if options.get("dryrun", False):
        for statement in statement_list:
            logger.info("Dry-run: {}".format(statement))
        return []

    executor = get_executor(options)

    with executor as e:
        benchmark_data = e.run(statement_list)

    for data in benchmark_data:
        logger.info(json.dumps(data))

    BenchmarkData = collections.namedtuple('BenchmarkData', sorted(benchmark_data[0]))
    return [BenchmarkData(**d) for d in benchmark_data]
```

## Notes

- This module is responsible for the orchestration and execution of jobs either locally or on different cluster environments (e.g., SGE, Slurm, Torque).
- The global DRMAA session is managed to interface with cluster schedulers, and the `GLOBAL_SESSION` variable maintains the session state.
- Executors are used to control where and how the jobs are executed, allowing for local and cluster-based execution. Depending on the queue manager specified, different executor classes such as `LocalExecutor`, `SGEExecutor`, `SlurmExecutor`, `TorqueExecutor`, or `KubernetesExecutor` are instantiated.

### Supported Cluster Types

- **SGE** (Sun Grid Engine)
- **SLURM** (Simple Linux Utility for Resource Management)
- **Torque**
- **Kubernetes**

## Usage Example

To use the `run()` function to execute a command either locally or on a cluster:

```python
from execution import run

statement = "echo 'Hello, world!'"
run(statement)
```

The execution environment is determined by the configuration parameters, and the job is run on the
