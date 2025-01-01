from unittest.mock import patch, Mock
import cgatcore.experiment as E
import cgatcore.pipeline as P
from cgatcore.pipeline.wrappers import PassThroughRunner, EmptyRunner
import ruffus
import pytest


@pytest.fixture(scope="module")
def build_pipeline():
    pipeline = ruffus.Pipeline(name="test1")
    start_tasks = pipeline.originate(
        task_func=PassThroughRunner(name="dummy_f1",
                                    f=lambda x: None),
        output=["a.1", "b.1"])

    pipeline.merge(
        task_func=EmptyRunner(name="all"),
        input=start_tasks,
        output="all")

    yield pipeline


def test_pipeline_action_show(capsys, build_pipeline):
    P.initialize(argv=["toolname", "show", "all", "--local"])  # Force local execution
    args = E.get_args()
    args.without_cluster = True  # Ensure we run without cluster
    args.to_cluster = False
    P.run_workflow(args, pipeline=build_pipeline)
    captured = capsys.readouterr()
    assert "Tasks which will be run" in captured.out


def test_pipeline_action_state(capsys, build_pipeline):
    P.initialize(argv=["toolname", "state", "--local"])  # Force local execution
    args = E.get_args()
    args.without_cluster = True  # Ensure we run without cluster
    args.to_cluster = False
    P.run_workflow(args, pipeline=build_pipeline)
    captured = capsys.readouterr()
    assert captured.out.startswith("function\tactive")
