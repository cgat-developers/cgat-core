# Cluster configuration

cgatcore supports SGE, SLURM, Torque, and Kubernetes workload managers.

Cluster settings are configured in a YAML file.  cgatcore reads configuration
from (in priority order):

1. `/etc/cgat/pipeline.yml` — site-wide defaults
2. `~/.cgat.yml` — per-user defaults
3. `pipeline.yml` in the working directory — per-run settings

All cluster settings live under a `cluster:` key.

## DRMAA vs CLI execution

By default, cgatcore uses **DRMAA** for cluster job submission when a DRMAA
library (e.g. `slurm-drmaa`, `drmaa-python`) is installed and the
`DRMAA_LIBRARY_PATH` environment variable points to the shared library.  This
is the recommended and most stable path.

When DRMAA is unavailable, cgatcore falls back to direct CLI tools (`sbatch`,
`qsub`, etc.).  In that case, set `cluster_queue_manager` in your config so
the correct CLI executor is selected.

### Setting DRMAA_LIBRARY_PATH

```bash
export DRMAA_LIBRARY_PATH=/usr/lib/libdrmaa.so.1.0
```

Add this to your `~/.bashrc` or activate it in your conda environment.

## Configuration reference

All keys below sit under a `cluster:` section in your YAML file:

| Key | Default | Description |
|-----|---------|-------------|
| `queue_manager` | `sge` | Scheduler type: `sge`, `slurm`, `torque`, `kubernetes` |
| `queue` | `all.q` | Default queue / partition to submit jobs to |
| `priority` | `-10` | Job priority |
| `num_jobs` | `100` | Maximum jobs in the queue at once |
| `memory_resource` | `mem_free` | Scheduler resource name for memory requests |
| `memory_default` | `4G` | Default memory per job |
| `memory_ulimit` | `False` | Enforce memory limit via ulimit |
| `options` | `""` | Extra options appended to every submission command |
| `parallel_environment` | `dedicated` | SGE parallel environment for multi-threaded jobs |
| `tmpdir` | `False` | Temp directory on cluster nodes (falls back to global `tmpdir`) |

## Example configurations

### SLURM (with DRMAA)

```yaml
cluster:
  queue_manager: slurm
  queue: shared
  memory_resource: mem
  memory_default: 4G
  options: "--time=24:00:00"
```

Install `slurm-drmaa` and set `DRMAA_LIBRARY_PATH` to use the DRMAA path.

### SLURM (without DRMAA, CLI fallback)

```yaml
cluster:
  queue_manager: slurm
  queue: shared
  memory_resource: mem
  memory_default: 4G
  options: "--time=24:00:00 --cpus-per-task=1"
```

Without DRMAA, cgatcore uses `sbatch`/`squeue`/`sacct` directly.

### SGE

```yaml
cluster:
  queue_manager: sge
  queue: all.q
  memory_resource: mem_free
  memory_default: 4G
  parallel_environment: dedicated
```

### Torque

```yaml
cluster:
  queue_manager: torque
  queue: batch
  memory_resource: mem
  options: "-l walltime=24:00:00"
```

## Running locally

To bypass cluster submission and run all jobs on the local machine, either:

- Pass `--local` on the command line
- Set `without_cluster: true` in your `pipeline.yml`
- Call `P.run(statement, to_cluster=False)` for individual tasks
