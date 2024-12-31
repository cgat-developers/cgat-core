"""Unit tests for cgatcore.pipeline.cluster.py"""

import pytest
from unittest import mock
import os
import logging


# Create a mock Session class

class MockSession:
    def __init__(self):
        self.runJob = mock.MagicMock(return_value="123")
        self.jobStatus = mock.MagicMock()
        self.createJobTemplate = mock.MagicMock()
        self.control = mock.MagicMock()
        self.wait = mock.MagicMock()


# Create a mock drmaa module

class MockDrmaa:
    def __init__(self):
        self.Session = MockSession
        self.JobState = mock.MagicMock()
        self.JobState.RUNNING = "RUNNING"
        self.JobState.DONE = "DONE"
        self.JobState.FAILED = "FAILED"
        self.JobState.QUEUED = "QUEUED"
        self.JobState.SUSPENDED = "SUSPENDED"
        self.JobState.UNDETERMINED = "UNDETERMINED"
        self.DeniedByDrmException = Exception
        self.JobControlAction = mock.MagicMock()
        self.JobControlAction.TERMINATE = "TERMINATE"
        self.JobControlAction.SUSPEND = "SUSPEND"
        self.JobControlAction.RESUME = "RESUME"
        self.JobControlAction.HOLD = "HOLD"
        self.JobControlAction.RELEASE = "RELEASE"
        self.Session.TIMEOUT_WAIT_FOREVER = -1
        self.Session.TIMEOUT_NO_WAIT = 0


# Mock the entire drmaa module before importing cluster
mock_drmaa = MockDrmaa()
with mock.patch.dict('sys.modules', {'drmaa': mock_drmaa}):
    import cgatcore.pipeline.cluster as cluster


@pytest.fixture
def mock_drmaa_session():
    """Mock DRMAA session for testing cluster functionality"""
    return MockSession()


@pytest.fixture
def mock_sge_cluster(mock_drmaa_session):
    """Create a mocked SGE cluster instance"""
    cluster_instance = cluster.SGECluster(mock_drmaa_session)
    cluster_instance.logger = logging.getLogger("test_logger")
    return cluster_instance


@pytest.fixture
def mock_slurm_cluster(mock_drmaa_session):
    """Create a mocked SLURM cluster instance"""
    cluster_instance = cluster.SlurmCluster(mock_drmaa_session)
    cluster_instance.logger = logging.getLogger("test_logger")
    return cluster_instance


def test_sge_job_submission(mock_sge_cluster):
    """Test job submission with mocked SGE DRMAA session"""
    # Setup mock job template
    job_template = mock.MagicMock()
    mock_sge_cluster.session.createJobTemplate.return_value = job_template
    
    # Test submitting a job
    statement = "echo test"
    job_name = "test_job"
    job_threads = 1
    job_memory = "1G"
    working_directory = "/tmp"
    
    # Test the setup_drmaa_job_template method with required kwargs
    jt = mock_sge_cluster.setup_drmaa_job_template(
        mock_sge_cluster.session,
        job_name,
        job_memory,
        job_threads,
        working_directory,
        memory_resource="virtual_free",  # Required by SGECluster
        parallel_environment="smp",  # Required by SGECluster
        queue="NONE"  # Required by SGECluster
    )
    
    assert jt is not None
    assert jt.workingDirectory == working_directory
    assert isinstance(jt.nativeSpecification, str)
    assert "-l virtual_free=1G" in jt.nativeSpecification


def test_slurm_job_submission(mock_slurm_cluster):
    """Test job submission with mocked SLURM DRMAA session"""
    # Setup mock job template
    job_template = mock.MagicMock()
    mock_slurm_cluster.session.createJobTemplate.return_value = job_template
    
    # Test submitting a job
    statement = "echo test"
    job_name = "test_job"
    job_threads = 1
    job_memory = "1G"
    working_directory = "/tmp"
    
    # Test the setup_drmaa_job_template method with SLURM-specific kwargs
    jt = mock_slurm_cluster.setup_drmaa_job_template(
        mock_slurm_cluster.session,
        job_name,
        job_memory,
        job_threads,
        working_directory,
        queue="main",  # SLURM partition
        memory_resource="mem",  # SLURM uses mem as memory resource
    )
    
    assert jt is not None
    assert jt.workingDirectory == working_directory
    assert isinstance(jt.nativeSpecification, str)
    
    # Check SLURM-specific options
    native_spec = jt.nativeSpecification
    assert "-J test_job" in native_spec  # Job name
    assert "--cpus-per-task=1" in native_spec  # Number of threads
    assert "--mem-per-cpu=1000" in native_spec  # Memory per CPU in MB
    assert "--partition=main" in native_spec  # SLURM partition


def test_job_status_checking(mock_sge_cluster):
    """Test job status checking with mocked DRMAA session"""
    job_id = "123"
    stdout_path = "/tmp/job.stdout"
    stderr_path = "/tmp/job.stderr"
    job_path = "/tmp/job.sh"
    statement = "echo test"
    
    # Mock wait() to return a JobInfo-like object
    retval = mock.MagicMock()
    retval.exitStatus = 0
    retval.wasAborted = False
    retval.hasSignal = False
    retval.hasExited = True
    retval.resourceUsage = {}
    mock_sge_cluster.session.wait.return_value = retval
    
    # Create temporary files with proper format
    # The last few lines should be:
    # <arbitrary output>
    # hostname
    # <empty line>
    for path in [stdout_path, stderr_path]:
        with open(path, 'w') as f:
            f.write("test output\ntest output\nlocalhost\n\n")
    
    # Create job path file to avoid unlink error
    with open(job_path, 'w') as f:
        f.write("#!/bin/bash\necho test")
    
    stdout, stderr, resource_usage = mock_sge_cluster.collect_single_job_from_cluster(
        job_id, statement, stdout_path, stderr_path, job_path
    )
    
    assert stdout[-2].strip() == "localhost"  # Changed from -3 to -2 since we added an empty line
    assert isinstance(resource_usage, list)
    assert len(resource_usage) > 0
    
    # Clean up temporary files
    for path in [stdout_path, stderr_path, job_path]:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_job_failure_handling(mock_sge_cluster):
    """Test handling of job submission failures"""
    job_id = "123"
    stdout_path = "/tmp/job.stdout"
    stderr_path = "/tmp/job.stderr"
    job_path = "/tmp/job.sh"
    statement = "echo test"
    
    # Mock wait() to return a failed job
    retval = mock.MagicMock()
    retval.exitStatus = 1
    retval.wasAborted = False
    retval.hasSignal = False
    retval.hasExited = True
    retval.resourceUsage = {}
    mock_sge_cluster.session.wait.return_value = retval
    
    # Create temporary files with proper format
    for path in [stdout_path, stderr_path]:
        with open(path, 'w') as f:
            f.write("test output\ntest output\nlocalhost\n\n")
            
    # Create job path file to avoid unlink error
    with open(job_path, 'w') as f:
        f.write("#!/bin/bash\necho test")
    
    with pytest.raises(OSError):
        mock_sge_cluster.collect_single_job_from_cluster(
            job_id, statement, stdout_path, stderr_path, job_path
        )
    
    # Clean up temporary files
    for path in [stdout_path, stderr_path, job_path]:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_kill_job(mock_sge_cluster):
    """Test job termination through the DRMAA session control method"""
    job_id = "123"
    mock_sge_cluster.session.control(job_id, mock_drmaa.JobControlAction.TERMINATE)
    mock_sge_cluster.session.control.assert_called_once_with(
        job_id, mock_drmaa.JobControlAction.TERMINATE)


def check_slurm_parsing(data, expected):
    """Helper function to perform Slurm value parsing and check the output."""
    resource_usage = cluster.JobInfo(1, {})
    c = cluster.SlurmCluster.parse_accounting_data(data, resource_usage)
    assert c[0].resourceUsage == expected


def test_parsing_short_job():
    sacct = ["host1|267088.batch|2019-02-27T02:58:15|2019-02-27T02:58:15|"
             "2019-02-27T05:05:32|1|0:0|7637|7637|01:54:55|01:11.853|"
             "0.10M|0.00M|10060K|9292520K|12087040K|150280K|77K|9292520K||77K"]
    check_slurm_parsing(sacct[-1], {
        'NodeList': 'host1',
        'JobID': '267088.batch',
        'Submit': 1551232695,  # 2019-02-27T02:58:15 UTC
        'Start': 1551232695,   # 2019-02-27T02:58:15 UTC
        'End': 1551240332,     # 2019-02-27T05:05:32 UTC
        'NCPUS': 1,
        'ExitCode': 0,
        'ElapsedRaw': 7637,
        'CPUTimeRaw': 7637,
        'UserCPU': 6895,
        'SystemCPU': 71,
        'MaxDiskRead': 100000,
        'MaxDiskWrite': 0,
        'AveVMSize': 10060000,
        'AveRSS': 9292520000,
        'MaxRSS': 12087040000,
        'MaxVMSize': 150280000,
        'AvePages': 77000,
        'DerivedExitCode': '',
        'MaxPages': 77000})


def test_parsing_longer_than_24h_job():
    sacct = ["host2|267087.batch|2019-02-27T02:58:08|2019-02-27T02:58:08|"
             "2019-02-28T04:38:50|1|0:0|92442|92442|1-01:12:52|19:36.307|"
             "0.10M|0.00M|10060K|26253156K|33016300K|150280K|580K|26253156K||580K"]

    check_slurm_parsing(sacct[-1], {
        'NodeList': 'host2',
        'JobID': '267087.batch',
        'Submit': 1551232688,  # 2019-02-27T02:58:08 UTC
        'Start': 1551232688,   # 2019-02-27T02:58:08 UTC
        'End': 1551325130,     # 2019-02-28T04:38:50 UTC
        'NCPUS': 1,
        'ExitCode': 0,
        'ElapsedRaw': 92442,
        'CPUTimeRaw': 92442,
        'UserCPU': 90772,
        'SystemCPU': 1176,
        'MaxDiskRead': 100000,
        'MaxDiskWrite': 0,
        'AveVMSize': 10060000,
        'AveRSS': 26253156000,
        'MaxRSS': 33016300000,
        'MaxVMSize': 150280000,
        'AvePages': 580000,
        'DerivedExitCode': '',
        'MaxPages': 580000})
