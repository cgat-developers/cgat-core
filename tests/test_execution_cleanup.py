# tests/test_execution_cleanup.py

import unittest
from unittest.mock import patch, MagicMock
import os
import tempfile
import shutil
import ruffus
import sys
import types
import signal  # Import signal to fix the NameError

# Import the control module
from cgatcore.pipeline import control
from ruffus.ruffus_exceptions import RethrownJobError  # Import the exception directly


class MockParams(dict):
    """
    A mock params dictionary that raises a detailed KeyError when a missing key is accessed.
    """
    def __getitem__(self, key):
        if key in self:
            value = super().__getitem__(key)
            if isinstance(value, dict):
                return MockParams(value)
            else:
                return value
        else:
            raise KeyError(f"Missing parameter accessed: '{key}'")
    
    def iteritems(self):
        """Provide iteritems compatibility for Python 2."""
        return iter(self.items())


class TestExecutionCleanup(unittest.TestCase):
    """
    Test suite for the Executor class's cleanup functionalities within the cgatcore.pipeline.control module.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up dummy modules for 'scripts' and 'scripts.cgat_check_deps' to prevent ModuleNotFoundError.
        """
        import sys

        # Create dummy 'scripts' module
        cls.dummy_scripts = types.ModuleType('scripts')
        # Create dummy 'cgat_check_deps' submodule
        cls.dummy_cgat_check_deps = types.ModuleType('scripts.cgat_check_deps')
        # Assign the dummy 'cgat_check_deps' to the 'scripts' module
        cls.dummy_scripts.cgat_check_deps = cls.dummy_cgat_check_deps

        # Add a mocked 'checkDepedencies' function to 'cgat_check_deps' module
        cls.dummy_cgat_check_deps.checkDepedencies = MagicMock(return_value=([], []))  # Adjust return values as needed

        # Patch 'sys.modules' to include both 'scripts' and 'scripts.cgat_check_deps'
        cls.patcher_scripts = patch.dict('sys.modules', {
            'scripts': cls.dummy_scripts,
            'scripts.cgat_check_deps': cls.dummy_cgat_check_deps
        })
        cls.patcher_scripts.start()

    @classmethod
    def tearDownClass(cls):
        """
        Stop patching 'sys.modules' after all tests are done.
        """
        cls.patcher_scripts.stop()

    def setUp(self):
        """
        Set up the test environment before each test method.
        """
        # Create a temporary directory for testing
        self.test_dir = tempfile.mkdtemp()

        # Patch 'cgatcore.pipeline.execution.get_params'
        self.patcher_execution = patch(
            'cgatcore.pipeline.execution.get_params',
            return_value=MockParams({
                "work_dir": self.test_dir,
                "tmpdir": self.test_dir,
                "cluster": {
                    "memory_default": "4G",
                    "monitor_interval_queued_default": 30,
                    "monitor_interval_running_default": 30,
                    "options": "",
                    "queue": "default_queue",
                    "cluster_tmpdir": "/tmp"
                },
                "testing": False,
                "start_dir": self.test_dir,
                "pipeline_logfile": "pipeline.log",
                "pipeline_name": "main",
                "pipelinedir": self.test_dir,
                "loglevel": 5,
                "input_globs": {},
                "without_cluster": True,
                "pipeline_yml": [],
                "mount_point": "/mnt",
                "os": "Linux"
            })
        )
        self.mock_execution = self.patcher_execution.start()

        # Patch 'cgatcore.pipeline.control.get_params'
        self.patcher_control = patch(
            'cgatcore.pipeline.control.get_params',
            return_value=MockParams({
                "work_dir": self.test_dir,
                "tmpdir": self.test_dir,
                "cluster": {
                    "memory_default": "4G",
                    "monitor_interval_queued_default": 30,
                    "monitor_interval_running_default": 30,
                    "options": "",
                    "queue": "default_queue",
                    "cluster_tmpdir": "/tmp"
                },
                "testing": False,
                "start_dir": self.test_dir,
                "pipeline_logfile": "pipeline.log",
                "pipeline_name": "main",
                "pipelinedir": self.test_dir,
                "loglevel": 5,
                "input_globs": {},
                "without_cluster": True,
                "pipeline_yml": [],
                "mount_point": "/mnt",
                "os": "Linux"
            })
        )
        self.mock_control = self.patcher_control.start()

        # Patch 'cgatcore.experiment.get_args'
        self.patcher_experiment_args = patch(
            'cgatcore.experiment.get_args',
            return_value=MagicMock(
                loglevel=5,
                timeit_name='test_timeit',
                timeit_file='test_timeit.log',
                timeit_header=True,
                stdout=MagicMock(),
                stderr=MagicMock(),
                stdlog=MagicMock()
            )
        )
        self.mock_experiment_args = self.patcher_experiment_args.start()

        # Mock 'global_benchmark' and 'global_options' in 'cgatcore.experiment'
        self.patcher_global_benchmark = patch(
            'cgatcore.experiment.global_benchmark',
            new=MagicMock()
        )
        self.mock_global_benchmark = self.patcher_global_benchmark.start()

        self.patcher_global_options = patch(
            'cgatcore.experiment.global_options',
            new=MagicMock(
                timeit_name='test_timeit',
                timeit_file='test_timeit.log',
                # Add other necessary attributes if needed
            )
        )
        self.mock_global_options = self.patcher_global_options.start()

        # Patch 'cgatcore.experiment.stop' to prevent it from interfering with the test
        self.patcher_experiment_stop = patch(
            'cgatcore.experiment.stop',
            return_value=None
        )
        self.mock_experiment_stop = self.patcher_experiment_stop.start()

        # Import Executor after patching to ensure it uses the mocked get_params
        from cgatcore.pipeline.execution import Executor
        self.Executor = Executor
        self.executor = Executor()

    def tearDown(self):
        """
        Clean up after each test method.
        """
        self.patcher_execution.stop()
        self.patcher_control.stop()
        self.patcher_experiment_args.stop()
        self.patcher_global_benchmark.stop()
        self.patcher_global_options.stop()
        self.patcher_experiment_stop.stop()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_error_handling_calls_cleanup(self):
        """
        Test that cleanup is called correctly when a job error occurs.
        """
        # Prepare mock arguments
        mock_args = MagicMock()
        mock_args.debug = False
        mock_args.pipeline_logfile = "test_logfile.log"
        mock_args.pipeline_action = "make"
        mock_args.force_run = None
        mock_args.pipeline_targets = ["target"]
        mock_args.without_cluster = True
        mock_args.multiprocess = 1
        mock_args.ruffus_checksums_level = 0
        mock_args.loglevel = 5
        mock_args.log_exceptions = True
        mock_args.exceptions_terminate_immediately = False

        # Ensure 'control.ruffus.pipeline_run' exists
        self.assertTrue(hasattr(control.ruffus, 'pipeline_run'),
                        "control.ruffus does not have 'pipeline_run' attribute")

        # Patch 'cgatcore.pipeline.control.ruffus.pipeline_run' to raise RethrownJobError
        with patch('cgatcore.pipeline.control.ruffus.pipeline_run',
                   side_effect=RethrownJobError(
                       ("task1", "job1", "error1", "msg1", "traceback1")
                   )), \
             patch('cgatcore.pipeline.execution.Executor.cleanup_failed_job') as mock_cleanup:
    
            # Execute and assert that ValueError is raised
            with self.assertRaises(ValueError):
                control.run_workflow(mock_args, pipeline=None)

            # Verify that cleanup_failed_job was called with the correct job info
            mock_cleanup.assert_called_once_with({"job_name": "job1"})

    def test_error_handling_calls_cleanup_object_patching(self):
        """
        Alternative test using patch.object to mock 'pipeline_run'.
        """
        mock_args = MagicMock()
        mock_args.debug = False
        mock_args.pipeline_logfile = "test_logfile.log"
        mock_args.pipeline_action = "make"
        mock_args.force_run = None
        mock_args.pipeline_targets = ["target"]
        mock_args.without_cluster = True
        mock_args.multiprocess = 1
        mock_args.ruffus_checksums_level = 0
        mock_args.loglevel = 5
        mock_args.log_exceptions = True
        mock_args.exceptions_terminate_immediately = False

        # Patch 'pipeline_run' using patch.object
        with patch.object(control.ruffus, 'pipeline_run',
                          side_effect=RethrownJobError(
                              ("task1", "job1", "error1", "msg1", "traceback1")
                          )), \
             patch('cgatcore.pipeline.execution.Executor.cleanup_failed_job') as mock_cleanup:
    
            # Execute and assert that ValueError is raised
            with self.assertRaises(ValueError):
                control.run_workflow(mock_args, pipeline=None)
    
            # Verify that cleanup_failed_job was called with the correct job info
            mock_cleanup.assert_called_once_with({"job_name": "job1"})

    def test_signal_handler(self):
        """
        Test the signal handler for SIGINT (Ctrl+C).
        """
        # Patch 'Executor.cleanup_all_jobs' to mock the cleanup
        with patch('cgatcore.pipeline.execution.Executor.cleanup_all_jobs') as mock_cleanup_all:
            self.executor.setup_signal_handlers()

            # Simulate SIGINT
            with self.assertRaises(SystemExit):
                os.kill(os.getpid(), signal.SIGINT)

            mock_cleanup_all.assert_called_once_with()

    def test_cleanup_failed_job_single_file(self):
        """
        Test cleanup of a failed job with a single output file.
        """
        test_file = os.path.join(self.test_dir, "test_output.txt")
        with open(test_file, "w") as f:
            f.write("Test content")

        job_info = {"outfile": test_file}
        self.executor.cleanup_failed_job(job_info)

        self.assertFalse(os.path.exists(test_file), "Failed to clean up the single output file.")

    def test_cleanup_failed_job_multiple_files(self):
        """
        Test cleanup of a failed job with multiple output files.
        """
        test_files = [
            os.path.join(self.test_dir, f"test_output_{i}.txt")
            for i in range(3)
        ]
        for file in test_files:
            with open(file, "w") as f:
                f.write("Test content")

        job_info = {"outfiles": test_files}
        self.executor.cleanup_failed_job(job_info)

        for file in test_files:
            self.assertFalse(os.path.exists(file), f"Failed to clean up the output file: {file}")

    def test_cleanup_failed_job_nonexistent_file(self):
        """
        Test cleanup of a failed job where the output file does not exist.
        """
        non_existent_file = os.path.join(self.test_dir, "non_existent.txt")
        job_info = {"outfile": non_existent_file}
        
        # This should not raise an exception
        try:
            self.executor.cleanup_failed_job(job_info)
        except Exception as e:
            self.fail(f"cleanup_failed_job raised an exception unexpectedly: {e}")

    def test_cleanup_failed_job_no_outfiles(self):
        """
        Test cleanup of a failed job with no output files specified.
        """
        # Adjusted to capture the warning log with correct logger name
        job_info = {"job_name": "test_job"}
        with self.assertLogs('cgatcore.pipeline', level='WARNING') as cm:
            self.executor.cleanup_failed_job(job_info)
        
        # Verify the expected warning message is in the logs
        self.assertIn("No output files found for job test_job", cm.output[0])

    def test_start_job(self):
        """
        Test starting a job.
        """
        job_info = {"job_name": "test_job"}
        self.executor.start_job(job_info)
        self.assertIn(job_info, self.executor.active_jobs, "Job was not added to active_jobs.")

    def test_finish_job(self):
        """
        Test finishing a job.
        """
        job_info = {"job_name": "test_job"}
        self.executor.start_job(job_info)
        self.executor.finish_job(job_info)
        self.assertNotIn(job_info, self.executor.active_jobs, "Job was not removed from active_jobs.")

    def test_cleanup_all_jobs(self):
        """
        Test cleanup of all active jobs.
        """
        test_files = [
            os.path.join(self.test_dir, f"test_output_{i}.txt")
            for i in range(3)
        ]
        for file in test_files:
            with open(file, "w") as f:
                f.write("Test content")

        job_infos = [{"outfile": file} for file in test_files]
        for job_info in job_infos:
            self.executor.start_job(job_info)

        self.executor.cleanup_all_jobs()

        for file in test_files:
            self.assertFalse(os.path.exists(file), f"Failed to clean up the output file: {file}")
        self.assertEqual(len(self.executor.active_jobs), 0, "Not all active jobs were cleaned up.")


# Run the tests
if __name__ == '__main__':
    unittest.main()
