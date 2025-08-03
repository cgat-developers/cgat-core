from unittest.mock import patch, Mock
import pytest
from cgatcore.pipeline.executors import (
    SlurmExecutor,
    SGEExecutor,
    LocalExecutor,
    TorqueExecutor
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
def test_slurm_job_monitoring(mock_subprocess_run):
    # Setup mock responses for job submission and status checks
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=0, stdout="RUNNING\n", stderr=""),  # First status check
        Mock(returncode=0, stdout="COMPLETED\n", stderr="")  # Second status check
    ]
    
    executor = SlurmExecutor()
    # Mock the time to avoid actual sleeps in tests
    with patch('time.sleep'):
        benchmark_data = executor.run(["echo 'test'"])
    
    # Verify job submission and monitoring calls
    calls = mock_subprocess_run.call_args_list
    assert len(calls) == 3
    
    # Check job submission
    submit_call = calls[0]
    assert "sbatch" in submit_call.args[0]
    
    # Check monitoring calls
    monitor_calls = calls[1:]
    for call in monitor_calls:
        assert "sacct -j 12345" in call.args[0]


@patch('subprocess.run')
def test_slurm_job_monitoring_failure(mock_subprocess_run):
    # Setup mock responses for job submission and failed status
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=0, stdout="FAILED\n", stderr="")  # Status shows failure
    ]
    
    executor = SlurmExecutor()
    with patch('time.sleep'), pytest.raises(RuntimeError, match="Job 12345 failed with status: FAILED"):
        executor.run(["echo 'test'"])


@patch('subprocess.run')
def test_sge_job_monitoring(mock_subprocess_run):
    # Setup mock responses for job submission and status checks
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="exit_status    0", stderr="")  # qacct shows success
    ]
    
    executor = SGEExecutor()
    with patch('time.sleep'):
        benchmark_data = executor.run(["echo 'test'"])
    
    # Verify job submission and monitoring calls
    calls = mock_subprocess_run.call_args_list
    assert len(calls) == 3
    
    # Check monitoring calls
    assert "qstat -j 12345" in calls[1].args[0]
    assert "qacct -j 12345" in calls[2].args[0]


@patch('subprocess.run')
def test_sge_job_monitoring_failure(mock_subprocess_run):
    # Setup mock responses for job submission and failed status
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="exit_status    1", stderr="")  # qacct shows failure
    ]
    
    executor = SGEExecutor()
    with patch('time.sleep'), pytest.raises(RuntimeError, match="Job 12345 failed with exit status: 1"):
        executor.run(["echo 'test'"])


@patch('subprocess.run')
def test_torque_job_monitoring(mock_subprocess_run):
    # Setup mock responses for job submission and status checks
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="Exit_status=0", stderr="")  # tracejob shows success
    ]
    
    executor = TorqueExecutor()
    with patch('time.sleep'):
        benchmark_data = executor.run(["echo 'test'"])
    
    # Verify job submission and monitoring calls
    calls = mock_subprocess_run.call_args_list
    assert len(calls) == 3
    
    # Check monitoring calls
    assert "qstat -f 12345" in calls[1].args[0]
    assert "tracejob 12345" in calls[2].args[0]


@patch('subprocess.run')
def test_torque_job_monitoring_failure(mock_subprocess_run):
    # Setup mock responses for job submission and failed status
    mock_subprocess_run.side_effect = [
        Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
        Mock(returncode=1, stdout="", stderr=""),  # qstat fails (job completed)
        Mock(returncode=0, stdout="Exit_status=1", stderr="")  # tracejob shows failure
    ]
    
    executor = TorqueExecutor()
    with patch('time.sleep'), pytest.raises(RuntimeError, match="Job 12345 failed with exit status: 1"):
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
        executor = executor_class()
    benchmark_data = executor.run([command])
    assert benchmark_data[0]["task"] == expected_task
