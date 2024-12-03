# Cluster Module

`cluster.py` - Cluster utility functions for cgatcore pipelines

==============================================================

This module abstracts the DRMAA native specification and provides convenience functions for running DRMAA jobs. It currently supports SGE, SLURM, Torque, and PBSPro cluster environments, enabling users to submit and manage cluster jobs easily within cgatcore pipelines.

## Reference

The following documentation details the cluster management utilities provided by the `cluster.py` module.

## Import Statements

```python
import re
import math
import collections
import os
import stat
import time
import datetime
import logging
import gevent
import cgatcore.experiment as E
try:
    import drmaa
except (ImportError, RuntimeError, OSError):
    pass
```

## Key Classes and Functions

### `get_logger()`

Returns the logger for the CGAT-core pipeline, which is used to handle logging within the cluster management utilities.

```python
def get_logger():
    return logging.getLogger("cgatcore.pipeline")
```

### `DRMAACluster` Class

This class provides core functionality for managing DRMAA cluster jobs, abstracting cluster specifications for SGE, SLURM, Torque, and PBSPro.

#### `__init__(self, session, ignore_errors=False)`

Initialises a DRMAA cluster instance.

Arguments:
- `session` (drmaa.Session): DRMAA session for interacting with the cluster.
- `ignore_errors` (bool, optional): If `True`, job errors are ignored, allowing the pipeline to continue.

#### `setup_drmaa_job_template(...)`

Sets up a DRMAA job template. Supported environments include SGE, SLURM, Torque, and PBSPro.

Arguments:
- `drmaa_session`: The DRMAA session object.
- `job_name` (string): Name of the job.
- `job_memory` (string): Memory requirements for the job.
- `job_threads` (int): Number of threads to allocate for the job.
- `working_directory` (string): Working directory for the job.

Raises:
- `ValueError`: If job memory is not specified.

#### `collect_single_job_from_cluster(...)`

Collects a single job running on the cluster, waiting for its completion and returning stdout, stderr, and resource usage.

Arguments:
- `job_id` (string): The job ID.
- `statement` (string): Command executed by the job.
- `stdout_path` (string): Path to the stdout file.
- `stderr_path` (string): Path to the stderr file.
- `job_path` (string): Path to the job file.

#### `get_drmaa_job_stdout_stderr(...)`

Fetches stdout and stderr for a DRMAA job, allowing for some lag.

Arguments:
- `stdout_path` (string): Path to the stdout file.
- `stderr_path` (string): Path to the stderr file.
- `tries` (int, optional): Number of attempts to retrieve the files.
- `encoding` (string, optional): Encoding for reading files.

Returns:
- `tuple`: stdout and stderr as lists of strings.

#### `set_drmaa_job_paths(job_template, job_path)`

Adds the job path, stdout path, and stderr path to the job template.

Arguments:
- `job_template`: DRMAA job template object.
- `job_path` (string): Path to the job script.

### Cluster-Specific Classes

The following classes inherit from `DRMAACluster` and implement cluster-specific logic for each cluster type.

#### `SGECluster`

Handles SGE-specific cluster job setup.

- **`get_native_specification(...)`**: Returns native specification parameters for SGE jobs.

#### `SlurmCluster`

Handles SLURM-specific cluster job setup.

- **`get_native_specification(...)`**: Returns native specification parameters for SLURM jobs.
- **`parse_accounting_data(...)`**: Parses SLURM accounting data to retrieve resource usage information.

#### `TorqueCluster`

Handles Torque-specific cluster job setup.

- **`get_native_specification(...)`**: Returns native specification parameters for Torque jobs.

#### `PBSProCluster`

Handles PBSPro-specific cluster job setup.

- **`get_native_specification(...)`**: Returns native specification parameters for PBSPro jobs.
- **`update_template(jt)`**: Updates the DRMAA job template environment.

### `get_queue_manager(queue_manager, *args, **kwargs)`

Returns a cluster instance based on the specified queue manager type.

Arguments:
- `queue_manager` (string): Type of queue manager (`sge`, `slurm`, `torque`, `pbspro`).
- `*args, **kwargs`: Additional arguments passed to the cluster class initialiser.

Raises:
- `ValueError`: If the queue manager type is not supported.

```python
def get_queue_manager(queue_manager, *args, **kwargs):
    qm = queue_manager.lower()
    if qm == "sge":
        return SGECluster(*args, **kwargs)
    elif qm == "slurm":
        return SlurmCluster(*args, **kwargs)
    elif qm == "torque":
        return TorqueCluster(*args, **kwargs)
    elif qm == "pbspro":
        return PBSProCluster(*args, **kwargs)
    else:
        raise ValueError("Queue manager {} not supported".format(queue_manager))
```

## Notes

- This module provides a unified interface for running cluster jobs across different cluster managers, allowing the user to switch between cluster types without rewriting job submission scripts.
- The module includes timeout settings for managing gevent event loops (`GEVENT_TIMEOUT_SACCT` and `GEVENT_TIMEOUT_WAIT`) to ensure that jobs are properly monitored without excessive waiting.
- The `JobInfo` named tuple is used to encapsulate job information, including job ID and resource usage.

### Supported Clusters

- **SGE** (Sun Grid Engine)
- **SLURM** (Simple Linux Utility for Resource Management)
- **Torque**
- **PBSPro**

Each cluster type requires specific configurations and resource definitions, which are managed through the appropriate cluster class.

## Usage Example

To use a specific cluster type, you would first initialise the relevant cluster class or use the `get_queue_manager()` function to automatically return an instance:

```python
from cluster import get_queue_manager

queue_manager = "slurm"
cluster = get_queue_manager(queue_manager, session=drmaa.Session(), ignore_errors=True)
```

Once the cluster is initialised, you can use its methods to create job templates, submit jobs, and manage their execution.

As you continue to expand the functionality of CGAT-core, ensure that this module is updated with new cluster types, resource mappings, and relevant updates for managing cluster jobs effectively.
