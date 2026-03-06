# Run parameters

## Command-line options

All cgatcore pipelines accept the following options:

```
python my_pipeline.py <action> [options]
```

### Actions

| Action | Description |
|--------|-------------|
| `make <task>` | Run `<task>` and all prerequisites |
| `show <task>` | Print tasks that would run (dry-run) |
| `touch <task>` | Mark outputs as up-to-date without running |
| `config` | Write a default `pipeline.yml` to the working directory |
| `svg` | Write a dependency graph SVG |
| `state` | Print up-to-date / out-of-date status of all tasks |
| `printconfig` | Print all active parameter values |

### General options

| Option | Description |
|--------|-------------|
| `-v N, --loglevel N` | Verbosity: 0=errors, 1=info, 2+=debug (default: 1) |
| `-p N, --multiprocess N` | Parallel ruffus workers (default: half CPU count, or 40 on cluster) |
| `--local` | Run all jobs locally (no cluster submission) |
| `--without-cluster` | Alias for `--local` |
| `--log FILE` | Write log to FILE (default: `pipeline.log`) |
| `--checksums N` | Ruffus checksum level (0=timestamp, 1=file-content, etc.) |
| `--exceptions-terminate-immediately` | Stop on first failure rather than waiting for parallel tasks |
| `--force-run TASK` | Force re-run of TASK even if outputs are up-to-date |
| `--dry-run` | Print statements without executing them |

## Per-task options

Options can be passed per `P.run()` call:

```python
P.run(statement, job_memory="8G", job_threads=4, job_queue="highmem")
```

Or set as local variables in the task function (they are picked up automatically):

```python
@transform(...)
def my_task(infile, outfile):
    job_memory = "16G"
    job_threads = 8
    statement = "..."
    P.run(statement)
```

### Per-task options reference

| Option | Description |
|--------|-------------|
| `job_memory` | Memory per thread, e.g. `"4G"` |
| `job_total_memory` | Total memory for the job (divided by `job_threads`) |
| `job_threads` | Number of threads/CPUs |
| `job_options` | Extra scheduler options, e.g. `"--time=12:00:00"` |
| `job_queue` | Queue/partition override |
| `job_condaenv` | Conda environment to activate inside the job |
| `to_cluster` | `False` to force local execution for this task |

## Configuration files

Pipeline parameters are read from YAML files in this order (later entries override earlier):

1. cgatcore built-in defaults
2. `/etc/cgat/pipeline.yml`
3. `~/.cgat.yml`
4. `pipeline.yml` in the working directory
5. Command-line arguments

Generate a template `pipeline.yml` for the current pipeline:

```bash
python my_pipeline.py config
```

See [Cluster configuration](../pipeline_modules/cluster.md) for cluster-specific
YAML settings.
