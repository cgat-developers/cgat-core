# Execution Module

`execution.py` manages job submission and monitoring for cgatcore pipelines.
It is the bridge between pipeline task functions and the compute backend
(cluster or local machine).

## Executor selection

When `P.run(statement)` is called, an executor is chosen automatically:

1. **`GridExecutor`** (DRMAA) — used when a DRMAA library is installed
   (e.g. `slurm-drmaa`, `drmaa-python`) and a session is active.
   This is the preferred path for HPC clusters.
2. **CLI-based executor** (`SlurmExecutor`, `SGEExecutor`, `TorqueExecutor`,
   `KubernetesExecutor`) — used when DRMAA is unavailable.
3. **`LocalExecutor`** — used when `to_cluster=False` or `without_cluster=True`.

See [Executors](executors.md) for a full description of each executor class.

## DRMAA session management

```python
start_session()   # initialise global DRMAA session
close_session()   # shut down the session
```

`start_session()` is called automatically by `run_workflow()`.  If the DRMAA
library is not installed, `HAS_DRMAA` is `False` and no session is started;
jobs fall back to the CLI-based executors.

## `run(statement, **kwargs)`

The primary function for submitting jobs from pipeline task functions:

```python
P.run(statement)
P.run(statement, job_memory="8G", job_threads=4)
P.run(statement, to_cluster=False)   # force local execution
```

### Context resolution

Options are merged in this priority order (lowest to highest):

1. Global `PARAMS` defaults
2. Local variables in the calling function's scope
3. Explicit `kwargs` passed to `run()`
4. A `params` namedtuple in the caller's scope (from TaskLibrary)

### Key options

| Option | Description |
|--------|-------------|
| `job_memory` | Memory per thread (e.g. `"4G"`) |
| `job_total_memory` | Total memory for the job (divided by `job_threads`) |
| `job_threads` | Number of threads/CPUs to request |
| `job_options` | Additional scheduler options string |
| `job_queue` | Queue/partition to submit to |
| `job_condaenv` | Conda environment to activate in the job |
| `to_cluster` | `True` to use cluster, `False` to run locally (default: `True`) |
| `without_cluster` | Alias for `to_cluster=False` |
| `dryrun` | If `True`, log the statement without executing it |

### Return value

Returns a list of `BenchmarkData` namedtuples with fields including
`task`, `job_id`, `start_time`, `end_time`, `exit_status`, `max_rss`,
`wall_t`, etc.

## `GridExecutor` and `GridArrayExecutor`

These classes (in `execution.py`) handle DRMAA-based cluster submission:

- **`GridExecutor`**: Submits a single job via DRMAA, polls status using
  `drmaa.Session`, collects resource usage from `sacct`/`qacct` on completion.
- **`GridArrayExecutor`**: Submits array jobs via DRMAA for parallel task execution.

Both inherit from `Executor`, which provides job tracking, signal handling,
and the context manager interface.

## `Executor` base class (DRMAA path)

`Executor` (in `execution.py`) is the base for `GridExecutor`.  It is
**distinct** from `BaseExecutor` (in `base_executor.py`) which is the base
for the CLI executors.

Key responsibilities:

- Tracks `active_jobs` for cleanup on pipeline interruption
- Installs SIGTERM/SIGINT handlers via `setup_signal_handlers()`
- Provides `cleanup_all_jobs()` and `cleanup_failed_job()`

## `will_run_on_cluster(options)`

Helper that returns `True` when DRMAA is available, a session is active,
`to_cluster` is truthy, and `without_cluster` is not set.  Returns `False`
otherwise (no exception is raised if DRMAA is absent).

## `get_executor(options)`

Factory function returning the appropriate CLI-based executor when DRMAA is
unavailable.  Checks `options["cluster_queue_manager"]` and the presence of
scheduler binaries:

```
"slurm"      + sbatch found  →  SlurmExecutor
"sge"        + qsub found    →  SGEExecutor
"torque"     + qsub found    →  TorqueExecutor
"kubernetes"                 →  KubernetesExecutor
otherwise                    →  LocalExecutor
```

## `LocalExecutor` (execution.py)

A second `LocalExecutor` in `execution.py` (distinct from the one in
`executors.py`) handles local job execution using gevent.  It is used when
DRMAA is unavailable and no cluster scheduler is configured.
