#!/bin/bash

set -xue

# Run code style checks
pycodestyle

# Run the template pipeline script with the `make all` command
python tests/template_pipeline.py make all

# Run tests using pytest
pytest -v tests/test_import.py
pytest -v tests/test_iotools.py
pytest -v tests/test_pipeline_cluster.py
pytest -v tests/test_pipeline_control.py
pytest -v tests/test_pipeline_execution.py
pytest -v tests/test_pipeline_cli.py
pytest -v tests/test_pipeline_actions.py
pytest -v tests/test_execution_cleanup.py
pytest -v tests/test_s3_decorators.py
