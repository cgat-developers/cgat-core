# Executors for job scheduling

## Overview

Executors are responsible for submitting pipeline statements to a compute
backend and waiting for them to finish.  cgatcore selects the executor
automatically based on availability:

1. **DRMAA / `GridExecutor`** — preferred when `slurm-drmaa`, `drmaa-python`,
   or any compatible DRMAA library is installed and a DRMAA session is active.
   This is the stable, battle-tested path used by most HPC clusters.
2. **CLI-based executors** (`SlurmExecutor`, `SGEExecutor`, `TorqueExecutor`)
   — fallback when DRMAA is unavailable.  They use standard command-line tools
   (`sbatch`, `qsub`, `squeue`, `sacct`, etc.) to submit and monitor jobs.
3. **`LocalExecutor`** — used when `to_cluster=False` or `without_cluster=True`,
   or when no cluster scheduler is detected.

The CLI executors all inherit from `BaseExecutor` (in
`cgatcore/pipeline/base_executor.py`) and are defined in
`cgatcore/pipeline/executors.py`.  The DRMAA-based `GridExecutor` lives in
`cgatcore/pipeline/execution.py`.

---

## `BaseExecutor`

`BaseExecutor` defines the interface that all CLI-based executors must
implement.  It is not intended to be used directly.

### Constructor

```python
BaseExecutor(**kwargs)
```

All keyword arguments are stored in `self.config` and are available to
subclasses.  Key configuration keys:

| Key | Description |
|-----|-------------|
| `job_script_dir` | Directory where temporary job scripts are written (defaults to the system temp dir) |
| `job_name` | Name passed to the scheduler for the job |

### Methods

#### `run(self, statement_list)`
Abstract — must be implemented by subclasses.  Receives a list of shell
statements, submits them to the backend, blocks until they complete, and
returns benchmark data.

#### `build_job_script(self, statement)`
Writes `statement` to a temporary bash script under `job_script_dir` and
returns `(statement, script_path)`.  Subclasses call this to obtain the
script path for submission.

#### `collect_benchmark_data(self, statements, resource_usage=None)`
Returns a dict with basic timing and resource information.  Subclasses may
override this to populate richer data.

#### `collect_metric_data(self, *args, **kwargs)`
Abstract hook for collecting scheduler-specific metrics.

#### `__enter__` / `__exit__`
Supports use as a context manager (`with executor as e: ...`).

---

## `SlurmExecutor`

CLI-based executor for Slurm clusters.  Used automatically when DRMAA is
unavailable and `cluster_queue_manager = slurm` is configured.

### Job submission

Jobs are submitted with `sbatch`:

```
sbatch --job-name=<name> --output=<path>.o --error=<path>.e <script>
```

`sbatch` prints `Submitted batch job <id>` on success.  The executor
extracts the numeric job ID from this output and validates it before
proceeding.

### Job monitoring

Monitoring uses a two-phase approach that avoids querying the accounting
database before a job is recorded:

1. **`squeue -j <id> -h -o %T`** — polled every 10 seconds while the job
   is in the queue (PENDING, RUNNING, etc.).  Terminal states detected here
   (`FAILED`, `TIMEOUT`, `CANCELLED`, `NODE_FAIL`, `OUT_OF_MEMORY`) cause an
   immediate `RuntimeError`.
2. **`sacct -j <id> --format=State,ExitCode --noheader --parsable2`** —
   queried once the job leaves the queue, with up to 10 retries (5-second
   intervals) to allow for accounting lag.  The state from the first output
   line (the batch step) is used; `CANCELLED by ...` is handled correctly.

### Error handling

| Condition | Behaviour |
|-----------|-----------|
| `sbatch` non-zero exit | `RuntimeError: Slurm job submission failed` |
| `sbatch` output not parseable as integer | `RuntimeError: Could not parse job ID` |
| Job enters terminal failure state in `squeue` | `RuntimeError: Job <id> failed with status: <state>` |
| `sacct` non-zero exit | `RuntimeError: sacct query failed` |
| Job does not reach terminal state after retries | `RuntimeError: did not reach a terminal state after sacct retries` |

---

## `SGEExecutor`

CLI-based executor for Sun Grid Engine (and compatible) clusters.

Jobs are submitted with `qsub`.  Status is checked with `qstat -j <id>`;
when that returns non-zero (job no longer queued), final status is obtained
from `qacct -j <id>`.

---

## `TorqueExecutor`

CLI-based executor for Torque/PBS clusters.

Jobs are submitted with `qsub`.  Status is checked with `qstat -f <id>`;
when that returns non-zero, final status is obtained from `tracejob <id>`.

---

## `LocalExecutor`

Runs statements directly on the local machine using `subprocess.Popen`.
Useful for development, testing, or single-node workloads.

No scheduler interaction takes place; stdout/stderr are captured and any
non-zero return code raises a `RuntimeError`.

---

## `KubernetesExecutor`

Submits jobs as Kubernetes `Job` objects via the Python Kubernetes client.

### Constructor

Loads the Kubernetes configuration (`~/.kube/config` or in-cluster config)
and sets up Core and Batch API clients.

### Methods

| Method | Description |
|--------|-------------|
| `run(statement, job_path, job_condaenv)` | Submit a job, wait for completion, collect logs and benchmark data, then clean up |
| `_wait_for_job_completion(job_name)` | Poll `read_namespaced_job_status()` until succeeded or failed |
| `_get_pod_logs(job_name)` | Retrieve stdout/stderr from the job's pod |
| `_cleanup_job(job_name)` | Delete the job and associated pods |
| `collect_benchmark_data(job_name, resource_usage_file)` | Gather CPU/memory metrics from the pod |
| `collect_metric_data(process, start_time, end_time, time_data_file)` | Save timing data |

---

## Logging

All CLI executors use `logging.getLogger(__name__)` (i.e. the
`cgatcore.pipeline.executors` logger).  Set verbosity >= 2 (`-v 2`) to see
`INFO`-level messages including job IDs, state transitions, and final status.

The older `E.info()` / `E.warn()` helpers (from `cgatcore.experiment`) are
used in the DRMAA-based `GridExecutor` and `Executor` classes.

---

## Executor selection logic

```
P.run(statement)
    |
    +- DRMAA available AND session active AND to_cluster=True?
    |       +- YES -> GridExecutor  (DRMAA, stable path)
    |
    +- NO -> get_executor(options)
                |
                +- testing=True or to_cluster=False -> LocalExecutor
                +- cluster_queue_manager=slurm + sbatch found -> SlurmExecutor
                +- cluster_queue_manager=sge  + qsub found   -> SGEExecutor
                +- cluster_queue_manager=torque + qsub found -> TorqueExecutor
                +- cluster_queue_manager=kubernetes          -> KubernetesExecutor
                +- fallback                                  -> LocalExecutor
```

---

## Summary

| Executor | Backend | Submission | Monitoring |
|----------|---------|------------|------------|
| `GridExecutor` | DRMAA | DRMAA session | DRMAA session |
| `SlurmExecutor` | Slurm CLI | `sbatch` | `squeue` then `sacct` |
| `SGEExecutor` | SGE CLI | `qsub` | `qstat` then `qacct` |
| `TorqueExecutor` | Torque CLI | `qsub` | `qstat` then `tracejob` |
| `KubernetesExecutor` | Kubernetes API | `create_namespaced_job` | `read_namespaced_job_status` |
| `LocalExecutor` | Local | `subprocess.Popen` | return code |
