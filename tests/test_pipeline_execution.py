"""Test cases for the pipeline.execution module."""

import os
import shutil
import subprocess
import pytest
import socket
import getpass
import cgatcore.pipeline as P
import cgatcore.iotools as iotools
from unittest import mock

# Check for DRMAA without importing it
HAVE_DRMAA = False
try:
    import drmaa
    drmaa.Session()  # Try to create a session to verify DRMAA is properly configured
    HAVE_DRMAA = True
except (ImportError, RuntimeError):
    pass

try:
    import paramiko
    HAVE_PARAMIKO = True
except ImportError:
    HAVE_PARAMIKO = False

QUEUE_MANAGER = P.get_parameters().get("cluster", {}).get("queue_manager", None)


def remote_file_exists(filename, hostname=None, expect=False):
    if not HAVE_PARAMIKO:
        return expect
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname, username=getpass.getuser())
    except (paramiko.SSHException, TimeoutError):
        return expect
    stdin, stdout, _ = ssh.exec_command(f"ls -d {filename}")
    return stdout.read().decode("utf-8").strip() == filename


@pytest.fixture
def work_dir():
    """Create and yield a temporary working directory."""
    temp_dir = P.get_temp_dir(shared=True)
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def expected_output_files():
    """Fixture for expected output files."""
    return [f"sample_{x:02}.mean" for x in range(10)] + [f"sample_{x:02}.txt" for x in range(10)]


def validate_benchmark_data(data, statement):
    assert data.percent_cpu >= 0
    assert data.max_rss >= 0
    assert data.max_vmem >= 0
    assert data.slots == 1
    assert len(data.hostname) > 0
    assert len(data.task) > 0
    assert data.total_t >= 0
    assert data.wall_t >= 0
    assert data.user_t >= 0
    assert data.sys_t >= 0
    assert data.start_time < data.end_time
    assert data.submission_time < data.end_time
    assert data.statement == statement


def test_single_job_returns_runtime_information():
    """Test that a single job returns runtime information when run locally."""
    statement = "echo 'test' && sleep 1"
    benchmark_data = P.run(statement, to_cluster=False)
    assert isinstance(benchmark_data, list)
    assert len(benchmark_data) == 1
    validate_benchmark_data(benchmark_data.pop(), statement)


def test_multiple_jobs_return_runtime_information():
    """Test that multiple jobs return runtime information when run locally."""
    statements = ["echo 'test' && sleep 1"] * 3
    benchmark_data = P.run(statements, to_cluster=False)
    assert isinstance(benchmark_data, list)
    assert len(benchmark_data) == len(statements)
    for data, stmt in zip(benchmark_data, statements):
        validate_benchmark_data(data, stmt)


def test_array_job_returns_runtime_information():
    """Test that array jobs return runtime information when run locally."""
    statements = ["echo 'test' && sleep 1"] * 3
    benchmark_data = P.run(statements, job_array=True, to_cluster=False)
    assert isinstance(benchmark_data, list)
    assert len(benchmark_data) == len(statements)
    for data, stmt in zip(benchmark_data, statements):
        validate_benchmark_data(data, stmt)


def test_job_should_fail_if_wrong_arguments():
    """Test that jobs fail with incorrect arguments."""
    with pytest.raises(OSError):
        P.run("ls -z", to_cluster=False)


def test_job_should_pass_if_unlimited_memory_required(work_dir):
    """Test that jobs pass with unlimited memory."""
    statement = "python -c 'import numpy; a = numpy.random.rand(10,10)'"
    P.run(statement, to_cluster=False, job_memory="unlimited")


if __name__ == "__main__":
    pytest.main()
