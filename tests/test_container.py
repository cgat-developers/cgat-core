import pytest
from unittest.mock import MagicMock, patch
from cgatcore.pipeline.execution import run, Executor

mocked_params = {
    "cluster": {
        "options": "",
        "queue": None,
        "memory_default": "4G",
        "tmpdir": "/tmp",
        "monitor_interval_queued_default": 30,
        "monitor_interval_running_default": 30,
    },
    "cluster_tmpdir": "/tmp",
    "tmpdir": "/tmp",
    "work_dir": "/tmp",
    "os": "Linux",
    "without_cluster": True,  # Explicitly disable cluster execution
    "to_cluster": False,  # Explicitly disable cluster execution
}


@patch("cgatcore.pipeline.execution.get_params", return_value=mocked_params)
def test_run_with_container_support(mock_get_params):
    """Test running a command with container support."""
    with patch("cgatcore.pipeline.execution.subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"Hello from Docker", b"")
        mock_process.returncode = 0
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        # Use Executor instance
        executor = Executor(to_cluster=False, without_cluster=True)

        # Mock the method that collects benchmark data
        with patch.object(executor, "collect_benchmark_data", return_value=None) as mock_collect_benchmark:
            executor.run(
                statement_list=["echo Hello from Docker"],
                container_runtime="docker",
                image="ubuntu:20.04",
            )

            mock_popen.assert_called_once()
            actual_call = mock_popen.call_args[0][0]
            print(f"Actual call to subprocess.Popen: {actual_call}")
            assert "docker run --rm" in actual_call
            assert "ubuntu:20.04" in actual_call
            assert "echo Hello from Docker" in actual_call

            # Validate that collect_benchmark_data was called
            mock_collect_benchmark.assert_called_once()


@patch("cgatcore.pipeline.execution.get_params", return_value=mocked_params)
def test_run_without_container_support(mock_get_params):
    """Test running a command without container support."""
    with patch("cgatcore.pipeline.execution.subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"Hello from local execution", b"")
        mock_process.returncode = 0
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        # Use Executor instance
        executor = Executor(to_cluster=False, without_cluster=True)

        # Mock the method that collects benchmark data
        with patch.object(executor, "collect_benchmark_data", return_value=None) as mock_collect_benchmark:
            executor.run(statement_list=["echo Hello from local execution"])

            mock_popen.assert_called_once()
            actual_call = mock_popen.call_args[0][0]
            print(f"Actual call to subprocess.Popen: {actual_call}")
            assert "echo Hello from local execution" in actual_call

            # Validate that collect_benchmark_data was called
            mock_collect_benchmark.assert_called_once()


@patch("cgatcore.pipeline.execution.get_params", return_value=mocked_params)
def test_invalid_container_runtime(mock_get_params):
    """Test handling of invalid container runtime."""
    with pytest.raises(ValueError, match="Container runtime must be 'docker' or 'singularity'"):
        executor = Executor(to_cluster=False, without_cluster=True)
        executor.run(statement_list=["echo Test"], container_runtime="invalid_runtime")


@patch("cgatcore.pipeline.execution.get_params", return_value=mocked_params)
def test_missing_required_params(mock_get_params):
    """Test handling of missing required parameters."""
    with pytest.raises(ValueError, match="An image must be specified when using a container runtime"):
        executor = Executor(to_cluster=False, without_cluster=True)
        executor.run(statement_list=["echo Test"], container_runtime="docker")


@patch("cgatcore.pipeline.execution.get_params", return_value=mocked_params)
@patch("cgatcore.pipeline.execution.Executor.cleanup_failed_job")
def test_cleanup_on_failure(mock_cleanup, mock_get_params):
    """Test cleanup logic when a job fails."""
    from cgatcore.pipeline.execution import Executor  # Ensure proper import

    # Create an instance of Executor
    executor = Executor(to_cluster=False, without_cluster=True)

    with patch("cgatcore.pipeline.execution.subprocess.Popen") as mock_popen:
        # Mock a process failure
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"Some error occurred")
        mock_process.returncode = 1  # Simulate failure
        mock_popen.return_value = mock_process

        # Attempt to run a failing command
        with pytest.raises(OSError, match="Job failed with return code"):
            executor.run(
                statement_list=["echo This will fail"],
                container_runtime="docker",  # Pass a valid container_runtime
                image="ubuntu:20.04"  # Add a valid image since container_runtime is provided
            )

        # Ensure cleanup_failed_job was called
        mock_cleanup.assert_called_once()
        print(f"Arguments to cleanup_failed_job: {mock_cleanup.call_args}")

        # Check subprocess was invoked
        mock_popen.assert_called_once()
        print(f"Subprocess call: {mock_popen.call_args_list}")


@patch("cgatcore.pipeline.execution.get_params", return_value=mocked_params)
def test_job_tracking(mock_get_params):
    """Test job tracking lifecycle."""
    with patch("cgatcore.pipeline.execution.subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"output", b"")
        mock_process.returncode = 0
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        run(statement=["echo Job tracking test"], to_cluster=False, without_cluster=True)

        mock_popen.assert_called_once()
