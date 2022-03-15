#! /bin/bash

set -xue

pycodestyle
python tests/template_pipeline.py make all
nosetests -v tests/test_import.py
nosetests -v tests/test_iotools.py
nosetests -v tests/test_pipeline_cluster.py
nosetests -v tests/test_pipeline_control.py
nosetests -v tests/test_pipeline_execution.py
pytest tests/test_pipeline_cli.py
pytest tests/test_pipeline_actions.py
