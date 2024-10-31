# tests/test_execution_cleanup.py

import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import tempfile
import shutil
import types
from ruffus.ruffus_exceptions import RethrownJobError
from ruffus import pipeline_run, follows, Pipeline


class MockParams(dict):
    def __getitem__(self, key):
        if key in self:
            value = super().__getitem__(key)
            if isinstance(value, dict):
                return MockParams(value)
            else:
                return value
        else:
            raise KeyError(f"Missing parameter accessed: '{key}'")


class TestExecutionCleanup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Patch sys.modules with dummy scripts for 'cgat_check_deps'
        cls.dummy_scripts = types.ModuleType('scripts')
        cls.dummy_cgat_check_deps = types.ModuleType('scripts.cgat_check_deps')

        # Assign dummy function `checkDepedencies` to 'scripts.cgat_check_deps'
        cls.dummy_cgat_check_deps.checkDepedencies = MagicMock(return_value=([], []))  # Adjust the return value as needed

        # Patch 'sys.modules' to include both 'scripts' and 'scripts.cgat_check_deps'
        cls.patcher_scripts = patch.dict('sys.modules', {
            'scripts': cls.dummy_scripts,
            'scripts.cgat_check_deps': cls.dummy_cgat_check_deps
        })
        cls.patcher_scripts.start()

    @classmethod
    def tearDownClass(cls):
        cls.patcher_scripts.stop()

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

        # Patches for params and args
        self.patcher_execution = patch(
            'cgatcore.pipeline.execution.get_params',
            return_value=MockParams({
                "work_dir": self.test_dir,
                "tmpdir": self.test_dir,
                "cluster": {"memory_default": "4G"},
                "pipeline_logfile": "pipeline.log",
                "without_cluster": True,
            })
        )
        self.patcher_execution.start()

        self.patcher_control = patch(
            'cgatcore.pipeline.control.get_params',
            return_value=MockParams({
                "work_dir": self.test_dir,
                "tmpdir": self.test_dir,
                "cluster": {"memory_default": "4G"},
                "pipeline_logfile": "pipeline.log",
                "without_cluster": True,
            })
        )
        self.patcher_control.start()

        # Initialize Executor instance here
        from cgatcore.pipeline.execution import Executor
        self.executor = Executor()

    def tearDown(self):
        self.patcher_execution.stop()
        self.patcher_control.stop()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    @follows()
    def target_task():
        pass  # This is a minimal task, no operation needed

    @patch('ruffus.pipeline_run')
    def test_error_handling_calls_cleanup(self, mock_pipeline_run):
        mock_pipeline_run.side_effect = RethrownJobError([("target_task", "job1", "error1", "msg1", "traceback1")])

        mock_args = MagicMock()
        mock_args.debug = False
        mock_args.pipeline_action = "make"
        mock_args.input_validation = False  # Disable input validation
        mock_args.pipeline_targets = ["target_task"]  # Reference the dummy task
        mock_args.without_cluster = True
        mock_args.multiprocess = 1
        mock_args.loglevel = 5
        mock_args.log_exceptions = True
        mock_args.exceptions_terminate_immediately = False
        mock_args.stdout = sys.stdout
        mock_args.stderr = sys.stderr
        mock_args.timeit_file = None
        mock_args.timeit_header = True
        mock_args.timeit_name = 'test_timeit'

        with patch('cgatcore.experiment.get_args', return_value=mock_args):
            from cgatcore.pipeline import control

            with patch('cgatcore.pipeline.control.logging.getLogger') as mock_logger:
                mock_logger.return_value = MagicMock()
                print("About to call control.run_workflow() and expecting ValueError")

                # Use assertRaises to capture the expected ValueError
                with self.assertRaises(ValueError) as context:
                    control.run_workflow(mock_args, pipeline=None)

                print("Caught expected ValueError:", str(context.exception))

    def test_cleanup_failed_job_single_file(self):
        test_file = os.path.join(self.test_dir, "test_output.txt")
        with open(test_file, "w") as f:
            f.write("Test content")
        job_info = {"outfile": test_file}
        self.executor.cleanup_failed_job(job_info)
        self.assertFalse(os.path.exists(test_file))

    def test_cleanup_failed_job_multiple_files(self):
        test_files = [os.path.join(self.test_dir, f"test_output_{i}.txt") for i in range(3)]
        for file in test_files:
            with open(file, "w") as f:
                f.write("Test content")
        job_info = {"outfiles": test_files}
        self.executor.cleanup_failed_job(job_info)
        for file in test_files:
            self.assertFalse(os.path.exists(file))

    def test_cleanup_failed_job_nonexistent_file(self):
        non_existent_file = os.path.join(self.test_dir, "non_existent.txt")
        job_info = {"outfile": non_existent_file}
        try:
            self.executor.cleanup_failed_job(job_info)
        except Exception as e:
            self.fail(f"cleanup_failed_job raised an exception unexpectedly: {e}")

    def test_cleanup_failed_job_no_outfiles(self):
        job_info = {"job_name": "test_job"}
        with self.assertLogs('cgatcore.pipeline', level='WARNING') as cm:
            self.executor.cleanup_failed_job(job_info)
        self.assertIn("No output files found for job test_job", cm.output[0])

    def test_start_job(self):
        job_info = {"job_name": "test_job"}
        self.executor.start_job(job_info)
        self.assertIn(job_info, self.executor.active_jobs)

    def test_finish_job(self):
        job_info = {"job_name": "test_job"}
        self.executor.start_job(job_info)
        self.executor.finish_job(job_info)
        self.assertNotIn(job_info, self.executor.active_jobs)

    def test_cleanup_all_jobs(self):
        test_files = [os.path.join(self.test_dir, f"test_output_{i}.txt") for i in range(3)]
        for file in test_files:
            with open(file, "w") as f:
                f.write("Test content")
        job_infos = [{"outfile": file} for file in test_files]
        for job_info in job_infos:
            self.executor.start_job(job_info)
        self.executor.cleanup_all_jobs()
        for file in test_files:
            self.assertFalse(os.path.exists(file))
        self.assertEqual(len(self.executor.active_jobs), 0)
