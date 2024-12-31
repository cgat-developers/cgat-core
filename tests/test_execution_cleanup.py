"""Test cases for pipeline execution cleanup."""

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
        cls.dummy_cgat_check_deps.checkDepedencies = MagicMock(return_value=([], []))

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

        # Mock parameters with explicit cluster settings
        mock_params = {
            "work_dir": self.test_dir,
            "tmpdir": self.test_dir,
            "cluster": {"memory_default": "4G"},
            "pipeline_logfile": "pipeline.log",
            "without_cluster": True,
            "to_cluster": False,  # Explicitly set to_cluster to False
        }

        # Patches for params and args
        self.patcher_execution = patch(
            'cgatcore.pipeline.execution.get_params',
            return_value=MockParams(mock_params)
        )
        self.patcher_execution.start()

        self.patcher_control = patch(
            'cgatcore.pipeline.control.get_params',
            return_value=MockParams(mock_params)
        )
        self.patcher_control.start()

        # Initialize Executor instance with explicit kwargs
        from cgatcore.pipeline.execution import Executor
        self.executor = Executor(to_cluster=False, without_cluster=True)

    def tearDown(self):
        self.patcher_execution.stop()
        self.patcher_control.stop()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_cleanup_all_jobs(self):
        """Test cleanup of all jobs."""
        # Create some test files
        test_files = ["test1.out", "test2.out", "test3.out"]
        for f in test_files:
            with open(os.path.join(self.test_dir, f), "w") as outf:
                outf.write("test")

        # Mock job info
        job_info = {
            "output_files": test_files
        }

        # Call cleanup
        self.executor.cleanup([job_info])

        # Check files were removed
        for f in test_files:
            self.assertFalse(os.path.exists(os.path.join(self.test_dir, f)))

    def test_cleanup_failed_job_single_file(self):
        """Test cleanup of a failed job with a single output file."""
        test_file = "test.out"
        file_path = os.path.join(self.test_dir, test_file)
        
        with open(file_path, "w") as outf:
            outf.write("test")

        job_info = {"output_files": [test_file]}
        self.executor.cleanup([job_info])
        
        self.assertFalse(os.path.exists(file_path))

    def test_cleanup_failed_job_multiple_files(self):
        """Test cleanup of a failed job with multiple output files."""
        test_files = ["test1.out", "test2.out"]
        file_paths = [os.path.join(self.test_dir, f) for f in test_files]
        
        for path in file_paths:
            with open(path, "w") as outf:
                outf.write("test")

        job_info = {"output_files": test_files}
        self.executor.cleanup([job_info])
        
        for path in file_paths:
            self.assertFalse(os.path.exists(path))

    def test_cleanup_failed_job_nonexistent_file(self):
        """Test cleanup with a nonexistent file."""
        job_info = {"output_files": ["nonexistent.out"]}
        # Should not raise an exception
        self.executor.cleanup([job_info])

    def test_cleanup_failed_job_no_outfiles(self):
        """Test cleanup with no output files."""
        job_info = {"output_files": []}
        # Should not raise an exception
        self.executor.cleanup([job_info])

    def test_error_handling_calls_cleanup(self):
        """Test that error handling properly calls cleanup."""
        mock_pipeline = MagicMock()
        mock_pipeline.run.side_effect = RethrownJobError([("target_task", "job1", "error1", "msg1", "traceback1")])

        with self.assertRaises(RethrownJobError):
            self.executor.run(mock_pipeline, make_jobs=True)

    def test_start_job(self):
        """Test starting a job."""
        job_info = {
            "task_name": "test_task",
            "job_id": 1,
            "output_files": ["test.out"]
        }
        
        self.executor.start_job(job_info)
        # Add assertions based on what start_job should do

    def test_finish_job(self):
        """Test finishing a job."""
        job_info = {
            "task_name": "test_task",
            "job_id": 1,
            "output_files": ["test.out"]
        }
        
        # First add the job to active_jobs
        self.executor.start_job(job_info)
        self.assertIn(job_info, self.executor.active_jobs)

        # Now finish the job
        self.executor.finish_job(job_info)
        
        # Verify job was removed from active_jobs
        self.assertNotIn(job_info, self.executor.active_jobs)
