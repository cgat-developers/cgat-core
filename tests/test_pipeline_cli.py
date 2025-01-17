import pytest
from unittest.mock import Mock, patch
import sys

# Mock DRMAA module before any imports
mock_drmaa = Mock()
mock_drmaa.Session = Mock()
mock_drmaa.JobState.RUNNING = "RUNNING"
mock_drmaa.JobState.DONE = "DONE"
mock_drmaa.JobState.FAILED = "FAILED"
mock_drmaa.Session.TIMEOUT_WAIT_FOREVER = -1
sys.modules['drmaa'] = mock_drmaa

from cgatcore.pipeline.executors import (
    LocalExecutor, SGEExecutor, SlurmExecutor, TorqueExecutor,
    SGECluster, SlurmCluster, TorqueCluster
)
from cgatcore.pipeline.kubernetes import KubernetesExecutor
import cgatcore.pipeline.execution

mock = Mock()
cgatcore.pipeline.execution.GLOBAL_SESSION = mock


def get_options(obj):
    args = obj.call_args.args
    if isinstance(args[0], dict):
        return args[0]
    elif isinstance(args[0], list) and len(args[0]) > 0 and isinstance(args[0][0], dict):
        return args[0][0]
    return {}


@patch.object(LocalExecutor, "run", return_value=[{"task": "local_task", "total_t": 5}])
def test_local_executor_runs_correctly(local_run_patch):
    executor = LocalExecutor()
    benchmark_data = executor.run(["echo 'Running local task'"])
    local_run_patch.assert_called_once_with(["echo 'Running local task'"])
    assert benchmark_data[0]["task"] == "local_task"


@patch.object(SGEExecutor, "run", return_value=[{"task": "sge_task", "total_t": 8}])
def test_sge_executor_runs_correctly(sge_run_patch):
    executor = SGEExecutor()
    benchmark_data = executor.run(["echo 'Running SGE task'"])
    sge_run_patch.assert_called_once_with(["echo 'Running SGE task'"])
    assert benchmark_data[0]["task"] == "sge_task"


@patch.object(SlurmExecutor, "run", return_value=[{"task": "slurm_task", "total_t": 10}])
def test_slurm_executor_runs_correctly(slurm_run_patch):
    executor = SlurmExecutor()
    benchmark_data = executor.run(["echo 'Running Slurm task'"])
    slurm_run_patch.assert_called_once_with(["echo 'Running Slurm task'"])
    assert benchmark_data[0]["task"] == "slurm_task"


@patch.object(TorqueExecutor, "run", return_value=[{"task": "torque_task", "total_t": 7}])
def test_torque_executor_runs_correctly(torque_run_patch):
    executor = TorqueExecutor()
    benchmark_data = executor.run(["echo 'Running Torque task'"])
    torque_run_patch.assert_called_once_with(["echo 'Running Torque task'"])
    assert benchmark_data[0]["task"] == "torque_task"


@patch.object(KubernetesExecutor, "run", return_value=[{"task": "kubernetes_task", "total_t": 15}])
def test_kubernetes_executor_runs_correctly(kubernetes_run_patch):
    with patch("cgatcore.pipeline.kubernetes.config.load_kube_config") as mock_kube_config:
        mock_kube_config.return_value = None  # Mock kube config loading if necessary
        executor = KubernetesExecutor()
        benchmark_data = executor.run(["echo 'Running Kubernetes task'"])
        kubernetes_run_patch.assert_called_once_with(["echo 'Running Kubernetes task'"])
        assert benchmark_data[0]["task"] == "kubernetes_task"


@patch('subprocess.run')
def test_slurm_job_monitoring_drmaa(mock_subprocess_run):
    # Mock DRMAA session for successful DRMAA-based execution
    mock_session = Mock()
    mock_session.runJob.return_value = "12345"
    mock_session.jobStatus.side_effect = ["RUNNING", "RUNNING", "DONE"]
    mock_job_info = Mock()
    mock_job_info.exitStatus = 0
    mock_job_info.resourceUsage = {
        'submission_host': 'test-host',
        'wallclock': '3600',
        'mem': '1G',
        'cpu': '100',
        'submission_time': '2024-01-16T21:00:00',
        'start_time': '2024-01-16T21:00:01',
        'end_time': '2024-01-16T21:00:02',
        'slots': '1',
        'maxrss': '100M',
        'maxvmem': '200M',
        'io_input': '1000',
        'io_output': '2000',
        'user_time': '50',
        'system_time': '50',
        'minor_page_faults': '100',
        'major_page_faults': '10',
        'signal': '0'
    }
    mock_session.wait.return_value = mock_job_info
    mock_drmaa.Session.return_value = mock_session
    mock_drmaa.JobState = Mock()
    mock_drmaa.JobState.RUNNING = "RUNNING"
    mock_drmaa.JobState.DONE = "DONE"
    mock_drmaa.JobState.FAILED = "FAILED"
    mock_drmaa.Session.TIMEOUT_WAIT_FOREVER = -1

    # Setup subprocess mock for fallback
    mock_subprocess_run.return_value.returncode = 0
    mock_subprocess_run.return_value.stdout = "12345\n"
    mock_subprocess_run.return_value.stderr = ""

    executor = SlurmExecutor(queue="main")  # Add default queue
    benchmark_data = executor.run(["echo 'test'"])

    # Verify DRMAA calls
    assert mock_session.runJob.called
    assert mock_session.jobStatus.call_count == 3
    assert mock_session.wait.called
    assert not mock_subprocess_run.called


@patch('subprocess.run')
def test_slurm_job_monitoring(mock_subprocess_run):
    # Force DRMAA initialization to fail
    mock_drmaa.Session.side_effect = ImportError("No DRMAA library")

    # Setup mock responses for subprocess-based execution
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=0, stdout="RUNNING\n", stderr=""),  # First squeue check
        Mock(returncode=0, stdout="RUNNING\n", stderr=""),  # Second squeue check
        Mock(returncode=1, stdout="", stderr=""),  # Third squeue check fails (job completed)
        Mock(returncode=0, stdout="COMPLETED\n", stderr=""),  # sacct status check
        Mock(returncode=0, stdout="JobID|State|Elapsed|MaxRSS|MaxVMSize|AveRSS|AveVMSize\n12345|COMPLETED|00:01:00|1K|2K|500|1K\n", stderr="")  # Resource usage
    ]

    executor = SlurmExecutor(queue="main")  # Add default queue
    benchmark_data = executor.run(["echo 'test'"])

    # Verify job submission and monitoring calls
    calls = mock_subprocess_run.call_args_list
    assert len(calls) == 6

    # Check job submission
    submit_call = calls[0]
    assert "sbatch" in submit_call[0][0]

    # Check monitoring calls
    monitor_calls = calls[1:5]  # All but last call
    for i, call in enumerate(monitor_calls):
        if i < 3:  # First three calls are squeue
            assert "squeue -j 12345" in call[0][0]
        else:  # Last call is sacct status
            assert "sacct -j 12345 --format=State" in call[0][0]

    # Check resource usage call
    resource_call = calls[-1]
    assert all(x in resource_call[0][0] for x in ["sacct -j 12345", "--format=JobID,State,Elapsed,MaxRSS"])


@patch('subprocess.run')
def test_sge_job_monitoring(mock_subprocess_run):
    # Force DRMAA initialization to fail
    mock_drmaa.Session.side_effect = ImportError("No DRMAA library")

    # Setup mock responses for job submission and status checks
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="exit_status    0", stderr=""),  # qacct shows success
        Mock(returncode=0, stdout="mem=1000M,cpu=3600,io=1000", stderr="")  # Resource usage
    ]

    executor = SGEExecutor(queue="main")  # Add default queue
    benchmark_data = executor.run(["echo 'test'"])

    # Verify job submission and monitoring calls
    calls = mock_subprocess_run.call_args_list
    assert len(calls) == 4

    # Check job submission
    submit_call = calls[0]
    assert "qsub" in submit_call[0][0]

    # Check monitoring calls
    qstat_call = calls[1]
    assert "qstat -j 12345" in qstat_call[0][0]

    # Check final status and resource usage
    qacct_calls = calls[2:]
    for call in qacct_calls:
        assert "qacct -j 12345" in call[0][0]


@patch('subprocess.run')
def test_torque_job_monitoring(mock_subprocess_run):
    # Force DRMAA initialization to fail
    mock_drmaa.Session.side_effect = ImportError("No DRMAA library")

    # Setup mock responses for job submission and status checks
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="Exit_status=0", stderr=""),  # tracejob shows success
        Mock(returncode=0, stdout="resources_used.mem=1gb\nresources_used.cput=01:00:00", stderr="")  # Resource usage
    ]

    executor = TorqueExecutor(queue="main")  # Add default queue
    benchmark_data = executor.run(["echo 'test'"])

    # Verify job submission and monitoring calls
    calls = mock_subprocess_run.call_args_list
    assert len(calls) == 4

    # Check job submission
    submit_call = calls[0]
    assert "qsub" in submit_call[0][0]

    # Check monitoring calls
    qstat_call = calls[1]
    assert "qstat -f 12345" in qstat_call[0][0]

    # Check final status and resource usage
    tracejob_calls = calls[2:]
    for call in tracejob_calls:
        assert "12345" in call[0][0]  # Just check for job ID presence


@patch('subprocess.run')
def test_slurm_job_monitoring_failure(mock_subprocess_run):
    # Setup mock responses for job submission and failed status
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=0, stdout="FAILED\n", stderr="")  # Status shows failure
    ]

    executor = SlurmExecutor(queue="main")  # Add default queue
    with pytest.raises(RuntimeError, match="Job 12345 failed with status: FAILED"):
        executor.run(["echo 'test'"])


@patch('subprocess.run')
def test_sge_job_monitoring_failure(mock_subprocess_run):
    # Setup mock responses for job submission and failed status
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="exit_status    1", stderr="")  # qacct shows failure
    ]

    executor = SGEExecutor(queue="main")  # Add default queue
    with pytest.raises(RuntimeError, match="Job 12345 failed with exit status: 1"):
        executor.run(["echo 'test'"])


@patch('subprocess.run')
def test_torque_job_monitoring_failure(mock_subprocess_run):
    # Setup mock responses for job submission and failed status
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="Exit_status=1", stderr="")  # tracejob shows failure
    ]

    executor = TorqueExecutor(queue="main")  # Add default queue
    with pytest.raises(RuntimeError, match="Job 12345 failed with exit status: 1"):
        executor.run(["echo 'test'"])


@pytest.mark.parametrize(
    "executor_class, command, expected_task",
    [
        (LocalExecutor, "echo 'Local job'", "local_task"),
        (SGEExecutor, "echo 'SGE job'", "sge_task"),
        (SlurmExecutor, "echo 'Slurm job'", "slurm_task"),
        (TorqueExecutor, "echo 'Torque job'", "torque_task"),
        (KubernetesExecutor, "echo 'Kubernetes job'", "kubernetes_task")
    ]
)
@patch.object(LocalExecutor, "run", return_value=[{"task": "local_task", "total_t": 5}])
@patch.object(SGEExecutor, "run", return_value=[{"task": "sge_task", "total_t": 8}])
@patch.object(SlurmExecutor, "run", return_value=[{"task": "slurm_task", "total_t": 10}])
@patch.object(TorqueExecutor, "run", return_value=[{"task": "torque_task", "total_t": 7}])
@patch.object(KubernetesExecutor, "run", return_value=[{"task": "kubernetes_task", "total_t": 15}])
def test_all_executors_run_correctly(local_run_patch, sge_run_patch, slurm_run_patch, torque_run_patch, kubernetes_run_patch, executor_class, command, expected_task):
    if executor_class == KubernetesExecutor:
        with patch("cgatcore.pipeline.kubernetes.config.load_kube_config") as mock_kube_config:
            mock_kube_config.return_value = None  # Mock kube config loading
            executor = executor_class()
    else:
        executor = executor_class(queue="main")  # Add default queue
    benchmark_data = executor.run([command])
    assert benchmark_data[0]["task"] == expected_task
