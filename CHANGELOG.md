# Version 0.6.21

## Fixes

- Restore DRMAA (GridExecutor) as the primary cluster execution path; CLI-based
  executors (SlurmExecutor etc.) are now a fallback for environments without DRMAA
- Fix SlurmExecutor job ID parsing: sbatch outputs "Submitted batch job <id>" but
  only the numeric ID was being extracted and validated correctly
- Fix SlurmExecutor monitoring: use squeue to poll while the job is alive, then
  sacct for final exit status, avoiding "Bad job/step specified" errors when sacct
  is queried before the job is in the accounting database
- Fix signal handler flood on pipeline exit: ruffus worker processes inherited the
  SIGTERM handler via fork, causing every worker to log "Received signal 15" and
  run cleanup when the process group exited normally
- Fix get_caller() frame depth in run_workflow() so pipeline config files are
  resolved relative to the calling pipeline module, not cgatcore itself
- will_run_on_cluster() no longer raises when DRMAA is absent; returns False so
  CLI executor fallback proceeds cleanly

# Version 0.6.20

- Fix pipeline parallelism: pass `multiprocess` to `ruffus.pipeline_run()` so multiple jobs run in parallel again

# Version 0.6.19

- Update GitHub Actions workflows to use latest action versions
- Fix mkdocs configuration for compatibility with newer mkdocstrings versions

# Version 0.6.18

- Python 3.13 compatibility: Replace deprecated `pipes.quote()` with `shlex.quote()` (PEP 594)

# Version 0.6.15

- additional py3.10 compatibility [contributed by @tschoonj]

# Version 0.6.10

## Fixes

- py3.10 compatibility: import from collections.abc
- read cluster configuration from cgat.yml as well as from pipeline.yml
- remove paramiko dependency (move remote_file_exits function into test code)
