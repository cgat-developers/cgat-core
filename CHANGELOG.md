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
