"""Test cases for the Pipeline.Control module."""

import unittest
import os
import shutil
import subprocess

import cgatcore
import cgatcore.experiment as E

import cgatcore.Pipeline as P
import cgatcore.iotools as iotools


ROOT = os.path.abspath(os.path.dirname(__file__))


class BaseTest(unittest.TestCase):

    def setUp(self):
        self.work_dir = P.get_temp_dir(shared=True)

    def tearDown(self):
        shutil.rmtree(self.work_dir)

    def run_command(self, statement, **kwargs):
        print(statement)
        proc = subprocess.Popen(statement,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                cwd=self.work_dir,
                                **kwargs)
        stdout, stderr = proc.communicate()
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        self.assertEqual(proc.returncode, 0, msg="stderr = {}".format(stderr))
        return proc.returncode, stdout, stderr


class TestExecutionControl(BaseTest):

    expected_output_files = ["sample_{:02}.mean".format(x) for x in range(10)] +\
                            ["sample_{:02}.txt".format(x) for x in range(10)]

    def setUp(self):
        P.get_parameters()
        BaseTest.setUp(self)

    def check_files(self, present=[], absent=[]):
        for fn in present:
            path = os.path.join(self.work_dir, fn)
            self.assertTrue(os.path.exists(path),
                            "file {} does not exist".format(path))

        for fn in absent:
            path = os.path.join(self.work_dir, fn)
            self.assertFalse(os.path.exists(path),
                             "file {} does exist but not expected".format(path))

    def test_basic_configuration_produces_expected_files(self):

        retval, stdout, stderr = self.run_command(
            "python {}/template_pipeline.py make all".format(ROOT))

        self.check_files(
            present=self.expected_output_files + ["pipeline.log"],
            absent=["shell.log"])

    def test_shell_log_is_created_in_workdir(self):

        retval, stdout, stderr = self.run_command(
            "python {}/template_pipeline.py make all --shell-logfile=shell.log".format(ROOT))

        self.check_files(
            present=self.expected_output_files + ["pipeline.log", "shell.log"])

    def test_shell_log_is_created_at_absolute_path(self):

        shell_file = os.path.join(self.work_dir, "test_shell", "shell.log")

        retval, stdout, stderr = self.run_command(
            "python {}/template_pipeline.py make all --shell-logfile={}".format(ROOT, shell_file))

        self.check_files(
            present=self.expected_output_files,
            absent=["shell.log"])

        self.assertTrue(os.path.exists(shell_file))

    def test_logging_can_be_configured_from_file(self):

        log_config = os.path.join(self.work_dir, "logging.yml")

        with open(log_config, "w") as outf:
            outf.write("""
version: 1
formatters:
  default:
    '()': cgatcore.experiment.MultiLineFormatter
    format: '# %(asctime)s %(levelname)s %(module)s - %(message)s'
  with_app:
    '()': cgatcore.experiment.MultiLineFormatter
    format: '%(asctime)s %(levelname)s %(app_name)s %(module)s - %(message)s'
filters:
  name_filter:
    '()': cgatcore.Pipeline.Control.LoggingFilterPipelineName
    name: mypipeline_name
handlers:
  console:
    class: logging.StreamHandler
    formatter: default
    stream: ext://sys.stdout
    level: INFO
  second_stream:
    class: logging.FileHandler
    formatter: with_app
    filename: "extra.log"
    level: DEBUG
root:
  handlers: [console]
  level: INFO
loggers:
  cgatcore.pipeline:
    handlers: [second_stream]
    filters: [name_filter]
    level: DEBUG
""")

        retval, stdout, stderr = self.run_command(
            "python {}/template_pipeline.py make all --log-config-filename={}".format(ROOT, log_config))

        self.check_files(
            present=self.expected_output_files + ["extra.log"],
            absent=["pipeline.log", "shell.log"])

        self.assertFalse(
            iotools.is_empty(os.path.join(self.work_dir, "extra.log")))

        with open(os.path.join(self.work_dir, "extra.log")) as inf:
            self.assertTrue("DEBUG" in inf.read())

        self.assertTrue("DEBUG" not in stdout)


if __name__ == "__main__":
    unittest.main()
