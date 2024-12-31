"""Unit tests for cgatcore.pipeline.cluster.py"""

import pytest
from unittest import mock
import drmaa
import cgatcore.pipeline.cluster as cluster


@pytest.fixture
def mock_drmaa_session():
    """Mock DRMAA session for testing cluster functionality"""
    with mock.patch('drmaa.Session') as mock_session:
        session = mock.MagicMock()
        mock_session.return_value = session
        yield session


@pytest.fixture
def mock_cluster(mock_drmaa_session):
    """Create a mocked cluster instance"""
    return cluster.DRMAACluster(mock_drmaa_session)


def test_job_submission(mock_cluster):
    """Test job submission with mocked DRMAA session"""
    # Setup mock job template
    job_template = mock.MagicMock()
    mock_cluster.session.createJobTemplate.return_value = job_template
    
    # Test submitting a job
    statement = "echo test"
    job_name = "test_job"
    job_threads = 1
    
    mock_cluster.session.runJob.return_value = "123"
    
    job_id = mock_cluster.submit(statement, job_name, job_threads)
    
    assert job_id == "123"
    mock_cluster.session.runJob.assert_called_once()


def test_job_status_checking(mock_cluster):
    """Test job status checking with mocked DRMAA session"""
    job_id = "123"
    
    # Test running status
    mock_cluster.session.jobStatus.return_value = drmaa.JobState.RUNNING
    status = mock_cluster.get_job_status(job_id)
    assert status == "running"
    
    # Test completed status
    mock_cluster.session.jobStatus.return_value = drmaa.JobState.DONE
    status = mock_cluster.get_job_status(job_id)
    assert status == "completed"


def test_job_failure_handling(mock_cluster):
    """Test handling of job submission failures"""
    mock_cluster.session.runJob.side_effect = drmaa.DeniedByDrmException
    
    with pytest.raises(OSError):
        mock_cluster.submit("echo test", "test_job", 1)


def test_kill_job(mock_cluster):
    """Test job termination"""
    job_id = "123"
    mock_cluster.kill_job(job_id)
    mock_cluster.session.control.assert_called_once_with(
        job_id, drmaa.JobControlAction.TERMINATE)


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
        'Submit': 1551236295,
        'Start': 1551236295,
        'End': 1551243932,
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
        'Submit': 1551236288,
        'Start': 1551236288,
        'End': 1551328730,
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
