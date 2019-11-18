from unittest.mock import patch, Mock
import cgatcore.pipeline
from cgatcore.pipeline import GridExecutor
import cgatcore.pipeline as P
import pytest

mock = Mock()
cgatcore.pipeline.execution.GLOBAL_SESSION = mock()


@patch.object(GridExecutor, "setup_job")
def test_default_queue_arguments(grid_run_patch):
    P.initialize()
    with patch("cgatcore.pipeline.execution.will_run_on_cluster", return_value=True):
        # fails with NameError if drmaa not configured
        # and import drmaa has failed
        with pytest.raises(NameError):
            P.run("echo here")
        grid_run_patch.assert_called_once()
        options = grid_run_patch.call_args_list[0].args[0]
        assert options["queue"] == "all.q"
        assert options["queue_manager"] == "sge"


@patch.object(GridExecutor, "setup_job")        
def test_default_queue_can_be_overridden(grid_run_patch):
    P.initialize(argv=["mytool", "--cluster-queue=test.q"])
    with patch("cgatcore.pipeline.execution.will_run_on_cluster", return_value=True):
        # fails with NameError if drmaa not configured
        # and import drmaa has failed
        with pytest.raises(NameError):
            P.run("echo here")
        grid_run_patch.assert_called_once()
        options = grid_run_patch.call_args_list[0].args[0]
        assert options["queue"] == "test.q"
        assert options["queue_manager"] == "sge"


@patch.object(GridExecutor, "setup_job")
@pytest.mark.parametrize(
    "option,field,value",
    [("--cluster-queue-manager", "queue_manager", "slurm"),
     ("--cluster-queue", "queue", "test.q"),
     ("--cluster-num-jobs", "num_jobs", 4),
     ("--cluster-priority", "priority", -100),
     ("--cluster-parallel-environment", "parallel_environment", "smp"),
     ("--cluster-memory-resource", "memory_resource", "vmem"),
     ("--cluster-options", "options", "-n test.name")])
def test_all_cluster_parameters_can_be_set(grid_run_patch, option, field, value):
    P.initialize(argv=["mytool", "{}={}".format(option, value)])
    with patch("cgatcore.pipeline.execution.will_run_on_cluster", return_value=True):
        # fails with NameError if drmaa not configured
        # and import drmaa has failed
        with pytest.raises(NameError):
            P.run("echo here")
        grid_run_patch.assert_called_once()
        options = grid_run_patch.call_args_list[0].args[0]
        assert options[field] == value
        
