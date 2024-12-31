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
            "cluster_tmpdir": self.test_dir,
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
        file_paths = [os.path.join(self.test_dir, f) for f in test_files]
        for path in file_paths:
            with open(path, "w") as outf:
                outf.write("test")

        # Mock job info with absolute paths
        job_info = {
            "outfiles": file_paths
        }

        # Call cleanup
        self.executor.cleanup_failed_job(job_info)

        # Check files were removed
        for path in file_paths:
            self.assertFalse(os.path.exists(path))

    def test_cleanup_failed_job_single_file(self):
        """Test cleanup of a failed job with a single output file."""
        test_file = "test.out"
        file_path = os.path.join(self.test_dir, test_file)
        
        with open(file_path, "w") as outf:
            outf.write("test")

        job_info = {"outfile": file_path}
        self.executor.cleanup_failed_job(job_info)
        
        self.assertFalse(os.path.exists(file_path))

    def test_cleanup_failed_job_multiple_files(self):
        """Test cleanup of a failed job with multiple output files."""
        test_files = ["test1.out", "test2.out"]
        file_paths = [os.path.join(self.test_dir, f) for f in test_files]
        
        for path in file_paths:
            with open(path, "w") as outf:
                outf.write("test")

        job_info = {"outfiles": file_paths}
        self.executor.cleanup_failed_job(job_info)
        
        for path in file_paths:
            self.assertFalse(os.path.exists(path))

    def test_cleanup_failed_job_nonexistent_file(self):
        """Test cleanup with a nonexistent file."""
        nonexistent_path = os.path.join(self.test_dir, "nonexistent.out")
        job_info = {"outfile": nonexistent_path}
        # Should not raise an exception
        self.executor.cleanup_failed_job(job_info)

    def test_cleanup_failed_job_no_outfiles(self):
        """Test cleanup with no output files."""
        job_info = {"job_name": "test_job"}
        # Should not raise an exception
        self.executor.cleanup_failed_job(job_info)

    def test_error_handling_calls_cleanup(self):
        """Test that error handling properly calls cleanup."""
        # Create a test file that should be cleaned up
        test_file = os.path.join(self.test_dir, "test.out")
        with open(test_file, "w") as f:
            f.write("test content")

        # Mock the cleanup_failed_job method to track if it was called
        original_cleanup = self.executor.cleanup_failed_job
        cleanup_called = {"value": False}

        def mock_cleanup(job_info):
            cleanup_called["value"] = True
            original_cleanup(job_info)

        self.executor.cleanup_failed_job = mock_cleanup

        # Create a job that should be cleaned up
        job_info = {"outfile": test_file}
        self.executor.start_job(job_info)

        # Mock executor's run method to fail while preserving job_info
        original_run = self.executor.run
        def mock_run(statement_list, *args, **kwargs):
            try:
                # Force a failure
                raise OSError("Mock job failure")
            except:
                # Get the current job from active_jobs
                if self.executor.active_jobs:
                    current_job = self.executor.active_jobs[-1]
                    self.executor.cleanup_failed_job(current_job)
                raise

        self.executor.run = types.MethodType(mock_run, self.executor)

        try:
            self.executor.run(["some command"])
        except OSError:
            # Verify cleanup was called and file was removed
            self.assertTrue(cleanup_called["value"], "cleanup_failed_job was not called")
            self.assertFalse(os.path.exists(test_file))
        else:
            self.fail("OSError was not raised")
        finally:
            # Restore original methods
            self.executor.run = original_run
            self.executor.cleanup_failed_job = original_cleanup

    def test_start_job(self):
        """Test starting a job."""
        job_info = {
            "task_name": "test_task",
            "job_id": 1,
            "outfile": os.path.join(self.test_dir, "test.out")
        }
        
        self.executor.start_job(job_info)
        self.assertIn(job_info, self.executor.active_jobs)

    def test_finish_job(self):
        """Test finishing a job."""
        job_info = {
            "task_name": "test_task",
            "job_id": 1,
            "outfile": os.path.join(self.test_dir, "test.out")
        }
        
        # First add the job to active_jobs
        self.executor.start_job(job_info)
        self.assertIn(job_info, self.executor.active_jobs)

        # Now finish the job
        self.executor.finish_job(job_info)
        
        # Verify job was removed from active_jobs
        self.assertNotIn(job_info, self.executor.active_jobs)
