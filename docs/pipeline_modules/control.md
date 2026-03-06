# Pipeline Control Module

The control module (`cgatcore/pipeline/control.py`) manages the overall execution
flow of a cgatcore pipeline.  It parses command-line arguments, initialises logging
and the DRMAA session, and drives ruffus to execute pipeline tasks.

## Entry point: `P.main()`

Every cgatcore pipeline script should call `P.main()` at the bottom:

```python
import cgatcore.pipeline as P

# ... pipeline task definitions ...

if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
```

`P.main()` calls `run_workflow()` internally, which handles all supported pipeline
actions (see below).

## Pipeline actions

Run a pipeline script with one of the following actions:

| Action | Description |
|--------|-------------|
| `make <task>` | Execute `<task>` and all its dependencies |
| `show <task>` | Print which tasks would run without executing them |
| `touch <task>` | Mark output files as up-to-date without running tasks |
| `config` | Write a default `pipeline.yml` configuration file to the current directory |
| `svg` | Render the pipeline dependency graph as an SVG |
| `state` | Print the state (up-to-date / out-of-date) of all tasks |
| `printconfig` | Print all active parameter values |

Example:

```bash
python my_pipeline.py make all -v 5
python my_pipeline.py config
python my_pipeline.py show all
```

## Common command-line options

| Option | Description |
|--------|-------------|
| `-v / --loglevel` | Verbosity level (0 = errors only, 1 = info, 2+ = debug) |
| `-p / --multiprocess` | Number of parallel ruffus workers (default: half CPU count locally, 40 on cluster) |
| `--local` | Run all jobs locally, ignoring cluster settings |
| `--without-cluster` | Alias for `--local` |
| `--log / --pipeline-logfile` | Path to the pipeline log file |
| `--checksums` | Ruffus checksum level for determining out-of-date tasks |

## `run_workflow()`

`run_workflow()` is the internal function that:

1. Creates an `Executor` instance (for signal handling and job tracking)
2. Starts the DRMAA session if available (`start_session()`)
3. Calls `ruffus.pipeline_run()` with the appropriate options
4. Handles errors by summarising ruffus exceptions and optionally cleaning up jobs

## `initialize()`

Called automatically by `P.main()` on first invocation.  Reads configuration files
and sets up logging.  Configuration is loaded from (in priority order):

1. `/etc/cgat/pipeline.yml` (site-wide defaults)
2. `~/.cgat.yml` (user defaults)
3. `pipeline.yml` in the current working directory
4. Command-line arguments

## Signal handling

The pipeline installs a SIGTERM/SIGINT handler in the main process only.  When the
signal is received, any tracked active jobs are cleaned up before exit.  Ruffus
worker subprocesses (forked from the main process) ignore these signals so that
cleanup runs exactly once.
