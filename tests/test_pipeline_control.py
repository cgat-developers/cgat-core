"""Test cases for the pipeline.control module."""

import os
import shutil
import subprocess
import tempfile
import pytest

import cgatcore.pipeline as P
import cgatcore.iotools as iotools
import cgatcore.experiment as E


ROOT = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def work_dir():
    """Fixture to create and clean up a temporary work directory."""
    # Set default value for shared_tmpdir if it is missing
    if 'shared_tmpdir' not in P.get_params():
        P.get_params()['shared_tmpdir'] = tempfile.mkdtemp()
    temp_dir = P.get_temp_dir(shared=True)
    yield temp_dir
    shutil.rmtree(temp_dir)


def run_command(statement, work_dir, **kwargs):
    """Run a shell command in a specified working directory."""
    print(statement)
    proc = subprocess.Popen(statement, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, cwd=work_dir, **kwargs)
    stdout, stderr = proc.communicate()
    stdout = stdout.decode("utf-8")
    stderr = stderr.decode("utf-8")
    assert proc.returncode == 0, f"stderr = {stderr}"
    return proc.returncode, stdout, stderr


@pytest.fixture
def expected_output_files():
    """Fixture for expected output files."""
    return [f"sample_{x:02}.mean" for x in range(10)] + [f"sample_{x:02}.txt" for x in range(10)]


def check_files(work_dir, present=None, absent=None):
    """Check for the presence and absence of files."""
    if present is None:
        present = []
    if absent is None:
        absent = []

    for fn in present:
        path = os.path.join(work_dir, fn)
        assert os.path.exists(path), f"file {fn} does not exist"

    for fn in absent:
        path = os.path.join(work_dir, fn)
        assert not os.path.exists(path), f"file {fn} exists but was not expected"


def test_basic_configuration_produces_expected_files(work_dir, expected_output_files):
    retval, stdout, stderr = run_command(
        f"python {ROOT}/template_pipeline.py make all --local", work_dir)

    check_files(
        work_dir,
        present=expected_output_files + ["pipeline.log"],
        absent=["shell.log"]
    )


def test_shell_log_is_created_in_workdir(work_dir, expected_output_files):
    retval, stdout, stderr = run_command(
        f"python {ROOT}/template_pipeline.py make all --local --shell-logfile=shell.log", work_dir)

    check_files(
        work_dir,
        present=expected_output_files + ["pipeline.log", "shell.log"]
    )


def test_shell_log_is_created_at_absolute_path(work_dir, expected_output_files):
    shell_file = os.path.join(work_dir, "test_shell", "shell.log")

    retval, stdout, stderr = run_command(
        f"python {ROOT}/template_pipeline.py make all --local --shell-logfile={shell_file}", work_dir)

    check_files(
        work_dir,
        present=expected_output_files,
        absent=["shell.log"]
    )

    assert os.path.exists(shell_file)


def test_logging_can_be_configured_from_file(work_dir, expected_output_files):
    log_config = os.path.join(work_dir, "logging.yml")

    with open(log_config, "w") as outf:
        outf.write("""
version: 1
formatters:
  default:
    '()': cgatcore.experiment.MultiLineFormatter
    format: '# %(asctime)s %(levelname)s %(module)s - %(message)s'
  with_app:
    '()': cgatcore.experiment.MultiLineFormatter
    format: '%(asctime)s %(levelname)s %(app_name)s %(module)s - %(message)s'
filters:
  name_filter:
    '()': cgatcore.pipeline.control.LoggingFilterpipelineName
    name: mypipeline_name
handlers:
  console:
    class: logging.StreamHandler
    formatter: default
    stream: ext://sys.stdout
    level: INFO
  second_stream:
    class: logging.FileHandler
    formatter: with_app
    filename: "extra.log"
    level: DEBUG
root:
  handlers: [console]
  level: INFO
loggers:
  cgatcore.pipeline:
    handlers: [second_stream]
    filters: [name_filter]
    level: DEBUG
""")

    retval, stdout, stderr = run_command(
        f"python {ROOT}/template_pipeline.py make all --local --log-config-filename={log_config}", work_dir)

    check_files(
        work_dir,
        present=expected_output_files + ["extra.log"],
        absent=["pipeline.log", "shell.log"]
    )

    extra_log_path = os.path.join(work_dir, "extra.log")
    assert not iotools.is_empty(extra_log_path)

    with open(extra_log_path) as inf:
        assert "DEBUG" in inf.read()

    assert "DEBUG" not in stdout
