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

## Cluster Configuration

CGAT-core provides robust support for executing pipelines on various cluster platforms, including SLURM, SGE, and PBS/Torque.

### Supported Platforms

1. **SLURM Workload Manager**
   - Modern, scalable cluster manager
   - Extensive resource control
   - Fair-share scheduling

2. **Sun Grid Engine (SGE)**
   - Traditional cluster system
   - Wide deployment base
   - Flexible job control

3. **PBS/Torque**
   - Professional batch system
   - Advanced scheduling
   - Resource management

### Configuration

#### Basic Setup

Create `.cgat.yml` in your home directory:

```yaml
cluster:
    # Queue manager type (slurm, sge, pbspro, torque)
    queue_manager: slurm
    
    # Default queue
    queue: main
    
    # Memory resource identifier
    memory_resource: mem
    
    # Default memory per job
    memory_default: 4G
    
    # Parallel environment
    parallel_environment: dedicated
    
    # Maximum concurrent jobs
    max_jobs: 100
    
    # Job priority
    priority: 0
```

#### Platform-Specific Configuration

##### SLURM Configuration
```yaml
cluster:
    queue_manager: slurm
    options: --time=00:10:00 --cpus-per-task=8 --mem=1G
    queue: main
    memory_resource: mem
    parallel_environment: dedicated
```

##### SGE Configuration
```yaml
cluster:
    queue_manager: sge
    options: -l h_rt=00:10:00
    queue: all.q
    memory_resource: h_vmem
    parallel_environment: smp
```

##### PBS/Torque Configuration
```yaml
cluster:
    queue_manager: torque
    options: -l walltime=00:10:00 -l nodes=1:ppn=8
    queue: batch
    memory_resource: mem
    parallel_environment: dedicated
```

## Resource Management

### Memory Allocation

```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    """Sort BAM file with specific memory requirements."""
    job_memory = "8G"
    job_threads = 4
    
    statement = """
    samtools sort 
        -@ %(job_threads)s 
        -m %(job_memory)s 
        %(infile)s > %(outfile)s
    """
    P.run(statement)
```

### CPU Allocation

```python
@transform("*.fa", suffix(".fa"), ".indexed")
def index_genome(infile, outfile):
    """Index genome using multiple cores."""
    job_threads = 8
    
    statement = """
    bwa index 
        -t %(job_threads)s 
        %(infile)s
    """
    P.run(statement)
```

### Temporary Directory

```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_with_temp(infile, outfile):
    """Sort using specific temporary directory."""
    tmpdir = P.get_temp_dir()
    
    statement = """
    samtools sort 
        -T %(tmpdir)s/sort 
        %(infile)s > %(outfile)s
    """
    P.run(statement)
```

## Advanced Configuration

### Job Dependencies

```python
@follows(previous_task)
@transform("*.txt", suffix(".txt"), ".processed")
def dependent_task(infile, outfile):
    """Task that depends on previous_task completion."""
    P.run("process_file %(infile)s > %(outfile)s")
```

### Resource Scaling

```python
@transform("*.bam", suffix(".bam"), ".stats")
def calculate_stats(infile, outfile):
    """Scale resources based on input size."""
    infile_size = os.path.getsize(infile)
    job_memory = "%dG" % max(4, infile_size // (1024**3) + 2)
    job_threads = min(4, os.cpu_count())
    
    statement = """
    samtools stats 
        -@ %(job_threads)s 
        %(infile)s > %(outfile)s
    """
    P.run(statement)
```

### Queue Selection

```python
@transform("*.big", suffix(".big"), ".processed")
def process_big_file(infile, outfile):
    """Use specific queue for large jobs."""
    job_queue = "bigmem"
    job_memory = "64G"
    
    statement = """
    process_large_file %(infile)s > %(outfile)s
    """
    P.run(statement)
```

## Best Practices

### 1. Resource Specification

- Always specify memory requirements
- Set appropriate number of threads
- Use queue-specific options wisely
- Consider input file sizes

### 2. Error Handling

```python
try:
    P.run(statement)
except P.PipelineError as e:
    L.error("Cluster job failed: %s" % e)
    # Cleanup and resubmit if needed
    cleanup_and_resubmit()
```

### 3. Performance Optimization

- Group small tasks
- Use appropriate chunk sizes
- Monitor resource usage
- Clean up temporary files

### 4. Monitoring

```python
# Log resource usage
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def monitored_sort(infile, outfile):
    """Monitor resource usage during sort."""
    job_memory = "8G"
    job_threads = 4
    
    statement = """
    { time samtools sort %(infile)s > %(outfile)s ; } 2> %(outfile)s.metrics
    """
    P.run(statement)
```

## Troubleshooting

### Common Issues

1. **Job Failures**
   - Check error logs
   - Verify resource requirements
   - Monitor cluster status

2. **Resource Exhaustion**
   - Adjust memory limits
   - Check disk space
   - Monitor CPU usage

3. **Queue Issues**
   - Verify queue availability
   - Check user limits
   - Monitor queue status

### Debugging Tips

1. **Enable Detailed Logging**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Test Jobs Locally**
   ```bash
   python pipeline.py make task --local
   ```

3. **Monitor Resource Usage**
   ```bash
   python pipeline.py make task --cluster-queue=main --debug
   ```

## Security Considerations

1. **Access Control**
   - Use appropriate permissions
   - Implement job quotas
   - Monitor user activity

2. **Data Protection**
   - Secure temporary files
   - Clean up job artifacts
   - Protect sensitive data

For more information, see:
- [SLURM Documentation](https://slurm.schedmd.com/)
- [SGE Documentation](http://gridscheduler.sourceforge.net/)
- [PBS Documentation](https://www.pbsworks.com/)

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
