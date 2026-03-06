# Installation

## Requirements

- Python 3.8 or higher
- Linux or macOS
- Conda (recommended) or pip

## Conda (recommended)

cgatcore is available on the Bioconda channel:

```bash
conda install -c conda-forge -c bioconda cgatcore
```

## pip

```bash
pip install cgatcore
```

## Development installation

To install from source for development:

```bash
git clone https://github.com/cgat-developers/cgat-core.git
cd cgat-core
pip install -e .
```

To update:

```bash
git pull
pip install -e .
```

## Verify installation

```python
import cgatcore
print(cgatcore.__version__)
```

## DRMAA support (for HPC clusters)

To submit jobs to a cluster via DRMAA, install the Python DRMAA bindings and
point the environment variable at your cluster's DRMAA shared library:

```bash
pip install drmaa
# or, for Slurm specifically:
conda install -c conda-forge slurm-drmaa
```

```bash
export DRMAA_LIBRARY_PATH=/usr/lib/libdrmaa.so.1.0
```

Add the `export` line to your `~/.bashrc` (or your conda environment's
activation script) so it is set every session.

If DRMAA is not available, cgatcore falls back to direct CLI job submission
(`sbatch`, `qsub`, etc.) — see [Cluster configuration](../pipeline_modules/cluster.md).

## Troubleshooting

- **Conda channel priority**: ensure `conda-forge` and `bioconda` are listed in
  your `.condarc` and that `conda-forge` is first.
- **Missing dependencies**: with a bare `pip install`, you may need to install
  optional dependencies (e.g. `drmaa`, `gevent`, `ruffus`) manually.
- **`DRMAA_LIBRARY_PATH` not set**: jobs will silently fall back to local
  execution unless the CLI fallback is configured
  (`cluster.queue_manager` in `.cgat.yml`).
