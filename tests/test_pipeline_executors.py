import pytest
from unittest.mock import Mock, patch
from cgatcore.pipeline.executors import (
    SlurmExecutor, SGEExecutor, TorqueExecutor,
    DRMAAExecutorStrategy, SubprocessExecutorStrategy
)

# Test DRMAA Strategy
@pytest.mark.parametrize("executor_class,queue_manager", [
    (SlurmExecutor, "SlurmCluster"),
    (SGEExecutor, "SGECluster"),
    (TorqueExecutor, "TorqueCluster"),
])
def test_drmaa_strategy(executor_class, queue_manager):
    with patch('drmaa.Session') as mock_session:
        # Setup mock DRMAA session
        mock_session.return_value.runJob.return_value = "12345"
        mock_session.return_value.wait.return_value = Mock(
            resourceUsage={'cpu': '100', 'mem': '1GB'})
        
        # Create executor with DRMAA strategy
        executor = executor_class(cluster_executor="drmaa")
        result = executor.run(["echo 'test'"])
        
        # Verify DRMAA was used
        assert mock_session.return_value.runJob.called
        assert len(result) == 1
        assert 'resourceUsage' in result[0]

# Test Subprocess Strategy
@pytest.mark.parametrize("executor_class,submit_cmd", [
    (SlurmExecutor, "sbatch"),
    (SGEExecutor, "qsub"),
    (TorqueExecutor, "qsub"),
])
def test_subprocess_strategy(executor_class, submit_cmd):
    with patch('subprocess.run') as mock_run:
        # Setup mock subprocess responses
        mock_run.side_effect = [
            Mock(returncode=0, stdout="12345\n", stderr=""),  # Job submission
            Mock(returncode=0, stdout="COMPLETED\n", stderr=""),  # Status check
            Mock(returncode=0, stdout="CPU=100|MEM=1GB\n", stderr="")  # Resource check
        ]
        
        # Create executor with subprocess strategy
        executor = executor_class(cluster_executor="subprocess")
        result = executor.run(["echo 'test'"])
        
        # Verify subprocess was used
        assert mock_run.call_count >= 2  # At least submit and status check
        assert submit_cmd in mock_run.call_args_list[0][0][0]
        assert len(result) == 1

# Test Strategy Selection
def test_drmaa_fallback():
    with patch('drmaa.Session', side_effect=ImportError):
        executor = SlurmExecutor(cluster_executor="drmaa")
        # Should fall back to subprocess
        assert isinstance(executor.executor, SubprocessExecutorStrategy)

# Test Error Handling
@pytest.mark.parametrize("executor_class", [
    SlurmExecutor, SGEExecutor, TorqueExecutor
])
def test_submission_failure(executor_class):
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = Mock(returncode=1, stderr="Submit failed")
        
        executor = executor_class(cluster_executor="subprocess")
        with pytest.raises(RuntimeError, match="job submission failed"):
            executor.run(["echo 'test'"])

@pytest.mark.parametrize("executor_class", [
    SlurmExecutor, SGEExecutor, TorqueExecutor
])
def test_job_failure(executor_class):
    with patch('subprocess.run') as mock_run:
        mock_run.side_effect = [
            Mock(returncode=0, stdout="12345\n", stderr=""),  # Submit success
            Mock(returncode=0, stdout="FAILED\n", stderr="")  # Job failed
        ]
        
        executor = executor_class(cluster_executor="subprocess")
        with pytest.raises(RuntimeError, match="failed with status"):
            executor.run(["echo 'test'"])
