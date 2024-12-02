import pytest
from unittest.mock import patch, MagicMock
import cgatcore.pipeline as P


@pytest.mark.parametrize("container_runtime, image, volumes, env_vars, expected_command", [
    ("docker", "ubuntu:20.04", ["/data:/data"], {"MY_VAR": "value"},
     "docker run --rm -v /data:/data -e MY_VAR=value ubuntu:20.04 /bin/bash -c 'echo Hello from Docker'"),
    ("singularity", "/path/to/image.sif", ["/data:/data"], {"MY_VAR": "value"},
     "singularity exec --bind /data:/data --env MY_VAR=value /path/to/image.sif /bin/bash -c 'echo Hello from Singularity'"),
])
@patch("subprocess.Popen")
def test_run_with_container_support(mock_popen, container_runtime, image, volumes, env_vars, expected_command):
    # Mocking the Popen object and its return values
    mock_process = MagicMock()
    mock_process.communicate.return_value = (b"output", b"")
    mock_process.returncode = 0
    mock_popen.return_value = mock_process

    # Running P.run with parameters
    P.run(
        statement_list=["echo Hello from Docker" if container_runtime == "docker" else "echo Hello from Singularity"],
        container_runtime=container_runtime,
        image=image,
        volumes=volumes,
        env_vars=env_vars
    )

    # Assert that Popen was called with the correct command
    mock_popen.assert_called_once_with(expected_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def test_run_without_container_support():
    # Running P.run without container runtime
    with patch("subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        P.run(statement_list=["echo 'Hello, World!'"], container_runtime=None)

        # Ensure subprocess is called without container wrapping
        mock_popen.assert_called_once_with("echo 'Hello, World!'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@pytest.mark.parametrize("invalid_runtime, expected_exception", [
    ("podman", ValueError),
    ("docker-compose", ValueError),
    (None, None),
])
def test_invalid_container_runtime(invalid_runtime, expected_exception):
    # Running P.run with invalid container runtime
    if expected_exception:
        with pytest.raises(expected_exception):
            P.run(
                statement_list=["echo 'Invalid runtime'"],
                container_runtime=invalid_runtime,
                image="ubuntu:20.04"
            )
    else:
        # No exception expected, should run without container support
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = (b"output", b"")
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            P.run(statement_list=["echo 'No container'"], container_runtime=invalid_runtime)
            mock_popen.assert_called_once_with("echo 'No container'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@pytest.mark.parametrize("missing_param, expected_exception", [
    ("image", ValueError),
    ("container_runtime", ValueError),
])
def test_missing_required_params(missing_param, expected_exception):
    kwargs = {
        "statement_list": ["echo 'Missing parameter test'"],
        "container_runtime": "docker",
        "image": "ubuntu:20.04",
    }

    # Remove the required parameter based on the test case
    kwargs.pop(missing_param)

    with pytest.raises(expected_exception):
        P.run(**kwargs)


def test_cleanup_on_failure():
    # Test that cleanup is properly handled when a job fails
    with patch("subprocess.Popen") as mock_popen, patch("P.cleanup_failed_job") as mock_cleanup:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"Some error occurred")
        mock_process.returncode = 1  # Simulate failure
        mock_popen.return_value = mock_process

        with pytest.raises(OSError):
            P.run(statement_list=["echo 'This will fail'"])

        # Assert that cleanup was called
        mock_cleanup.assert_called_once()


def test_job_tracking():
    # Ensure job tracking is handled correctly
    with patch("subprocess.Popen") as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"output", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        executor = P()
        executor.run(statement_list=["echo 'Track this job'"])

        # Assert that the active jobs list was modified correctly
        assert len(executor.active_jobs) == 0  # Job should be removed after completion
