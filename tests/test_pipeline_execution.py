"""Test cases for the pipeline.execution module."""

import shutil
import unittest
import contextlib
import socket
import os
import cgatcore.pipeline as P
import cgatcore.iotools as iotools


@contextlib.contextmanager
def run_on_cluster(to_cluster):
    if to_cluster:
        P.start_session()
        try:
            yield
        finally:
            P.close_session()
    else:
        yield


class BaseTest(unittest.TestCase):

    def setUp(self):
        # ignore command line arguments for pytest
        P.initialize(argv=["test"])
        self.work_dir = P.get_temp_dir(shared=True)

    def tearDown(self):
        shutil.rmtree(self.work_dir)


class TestExecutionRun(BaseTest):

    def setUp(self):
        P.get_parameters()
        BaseTest.setUp(self)

    def test_cluster_job_should_run_on_cluster(self):
        # note that this task requires to be run on a shared
        # drive to work because of outfile using work_dir
        # TODO: use as shared temporary directory
        outfile = os.path.join(self.work_dir, "out")
        with run_on_cluster(True):
            P.run(
                "hostname > {outfile}".format(outfile=outfile))
            has_cluster = P.will_run_on_cluster({"to_cluster": True})

        with iotools.open_file(outfile) as outf:
            execution_hostname = outf.read().strip()

        hostname = socket.gethostname()
        if has_cluster:
            self.assertNotEqual(hostname, execution_hostname)
        else:
            self.assertEqual(hostname, execution_hostname)

    def test_local_job_should_not_run_on_cluster(self):
        outfile = os.path.join(self.work_dir, "out")

        P.run(
            "hostname > {outfile}".format(
                outfile=outfile),
            to_cluster=False)

        with iotools.open_file(outfile) as outf:
            execution_hostname = outf.read().strip()

        hostname = socket.gethostname()
        self.assertEqual(hostname, execution_hostname)


class TestExecutionRunLocal(unittest.TestCase):

    test_memory_size = 100000000
    base_memory_size = 3000000000
    to_cluster = False

    # this command runs about 15s for the openssl
    # the lsof uses up sys time.
    cmd_work = "lsof > /dev/null && openssl speed md5"

    def setUp(self):
        BaseTest.setUp(self)
        P.get_parameters()

    def test_job_should_fail_with_missing_command(self):
        outfile = os.path.join(self.work_dir, "out")

        self.assertRaises(OSError,
                          P.run,
                          "unknown_command > {outfile}".format(
                              outfile=outfile),
                          to_cluster=self.to_cluster)

    def test_job_should_pass_if_no_error_in_sequence_of_commands(self):
        outfile = os.path.join(self.work_dir, "out")

        benchmark_data = P.run(
            "ls; ls; ls".format(
                outfile=outfile),
            to_cluster=self.to_cluster)
        self.assertTrue(benchmark_data)

    def test_job_should_fail_if_error_in_sequence_of_commands(self):
        outfile = os.path.join(self.work_dir, "out")

        self.assertRaises(OSError,
                          P.run,
                          "ls; unknown_command; ls".format(
                              outfile=outfile),
                          to_cluster=self.to_cluster)

    def test_job_should_pass_if_no_error_in_pipe_of_commands(self):
        outfile = os.path.join(self.work_dir, "out")

        benchmark_data = P.run(
            "ls | cat | cat".format(
                outfile=outfile),
            to_cluster=self.to_cluster)
        self.assertTrue(benchmark_data)

    def test_job_should_fail_if_error_in_pipe_of_commands(self):
        outfile = os.path.join(self.work_dir, "out")

        self.assertRaises(OSError,
                          P.run,
                          "ls | unknown_command | cat".format(
                              outfile=outfile),
                          to_cluster=self.to_cluster)

    def test_job_should_pass_if_error_in_pipe_of_commands_but_ignore_pipe_error_set(self):
        outfile = os.path.join(self.work_dir, "out")

        benchmark_data = P.run(
            "ls | unknown_command | cat".format(
                outfile=outfile),
            to_cluster=self.to_cluster,
            ignore_pipe_errors=True)
        self.assertTrue(benchmark_data)

    def test_job_should_fail_with_wrong_arguments(self):
        outfile = os.path.join(self.work_dir, "out")
        self.assertRaises(OSError,
                          P.run,
                          "hostname -z".format(
                              outfile=outfile),
                          to_cluster=self.to_cluster)

    def test_job_should_fail_if_too_little_memory_required(self):

        outfile = os.path.join(self.work_dir, "out")

        if P.get_parameters()['os'] == 'Linux':
            self.assertRaises(
                OSError,
                P.run,
                "python -c 'import numpy; "
                "a = numpy.array(numpy.arange(0, {memory}), numpy.int8); "
                "out = open(\"{outfile}\", \"w\"); "
                "out.write(str(len(a)) + \"\\n\"); "
                "out.close()'".format(
                    memory=self.test_memory_size,
                    outfile=outfile),
                to_cluster=self.to_cluster,
                cluster_memory_ulimit=True,                
                job_memory="{}G".format(
                    0.5 * self.test_memory_size / 10**9))
        else:
            pass

    def test_job_should_fail_if_too_little_memory_required_in_second_statement(self):

        outfile = os.path.join(self.work_dir, "out")
        infile = "arv=by_id/glon1-4zz18-3cbje7tmr0nitut/study_list.txt"

        if P.get_parameters()['os'] == 'Linux':
            self.assertRaises(
                OSError,
                P.run,
                "hostname > {outfile}; "
                "python -c 'import numpy; "
                "a = numpy.array(numpy.arange(0, {memory}), numpy.int8); "
                "out = open(\"{outfile}\", \"w\"); "
                "out.write(str(len(a)) + \"\\n\"); "
                "out.close()'".format(
                    memory=self.test_memory_size,
                    infile=infile,
                    outfile=outfile),
                to_cluster=self.to_cluster,
                cluster_memory_ulimit=True,
                job_memory="{}G".format(
                    0.5 * self.test_memory_size / 10**9))
        else:
            pass

    def test_job_should_pass_if_enough_memory_required(self):
        outfile = os.path.join(self.work_dir, "out")
        benchmark_data = P.run(
            "python -c 'import numpy; "
            "a = numpy.array(numpy.arange(0, {memory}), numpy.int8); "
            "out = open(\"{outfile}\", \"w\"); "
            "out.write(str(len(a)) + \"\\n\"); "
            "out.close()'".format(
                memory=self.test_memory_size,
                outfile=outfile),
            to_cluster=self.to_cluster,
            cluster_memory_ulimit=True,
            job_memory="{}G".format(
                (self.base_memory_size + self.test_memory_size) / 10**9))

        self.assertTrue(benchmark_data)

        with iotools.open_file(outfile) as outf:
            memory_used = int(outf.read().strip())

        self.assertEqual(memory_used, self.test_memory_size)

    def test_job_should_pass_if_unlimited_memory_required(self):
        outfile = os.path.join(self.work_dir, "out")

        benchmark_data = P.run(
            "python -c 'import numpy; "
            "a = numpy.array(numpy.arange(0, {memory}), numpy.int8); "
            "out = open(\"{outfile}\", \"w\"); "
            "out.write(str(len(a)) + \"\\n\"); "
            "out.close()'".format(
                memory=self.test_memory_size,
                outfile=outfile),
            to_cluster=self.to_cluster,
            cluster_memory_ulimit=True,
            job_memory="unlimited".format())
        self.assertTrue(benchmark_data)

        with iotools.open_file(outfile) as outf:
            memory_used = int(outf.read().strip())

        self.assertEqual(memory_used, self.test_memory_size)

    def test_job_should_write_to_explicit_temp_and_not_clean_up(self):

        outfile = os.path.join(self.work_dir, "out")
        tmpfile = P.get_temp_filename(clear=True)
        P.run("hostname > {outfile}; "
              "echo {tmpfile} > {tmpfile}; "
              "cat {tmpfile} >> {outfile}".format(
                  outfile=outfile,
                  tmpfile=tmpfile),
              to_cluster=False)

        with iotools.open_file(outfile) as outf:
            hostname = outf.readline().strip()
            tmpfile_read = outf.readline().strip()

        self.assertEqual(tmpfile,
                         tmpfile_read)
        self.assertTrue(self.file_exists(tmpfile,
                                         hostname,
                                         expect=True))
        os.unlink(tmpfile)

    def test_job_should_use_TMPDIR_and_clean_up(self):

        outfile = os.path.join(self.work_dir, "out")
        P.run("hostname > {outfile}; "
              "echo $TMPDIR > $TMPDIR/tmpfile; "
              "cat $TMPDIR/tmpfile >> {outfile}".format(
                  outfile=outfile),
              to_cluster=False)

        with iotools.open_file(outfile) as outf:
            hostname = outf.readline().strip()
            tmpdir = outf.readline().strip()

        self.assertFalse(self.file_exists(
            os.path.join(tmpdir, "tmpfile"),
            hostname))
        self.assertFalse(self.file_exists(
            tmpdir,
            hostname))

    def test_job_should_use_TMPDIR_and_clean_up_after_fail(self):

        outfile = os.path.join(self.work_dir, "out")
        self.assertRaises(
            OSError,
            P.run,
            "hostname > {outfile}; "
            "echo $TMPDIR >> {outfile}; "
            "unknown_command; "
            "cat $TMPDIR/tmpfile > {outfile}".format(
                outfile=outfile),
            to_cluster=False)

        with iotools.open_file(outfile) as outf:
            hostname = outf.readline().strip()
            tmpdir = outf.readline().strip()

        self.assertFalse(self.file_exists(
            os.path.join(tmpdir, "tmpfile"),
            hostname))
        self.assertFalse(self.file_exists(
            tmpdir,
            hostname))

    def file_exists(self, filename, hostname, expect=False):
        return os.path.exists(filename)

    def validate_benchmark_data(self, data, statement):
        self.assertGreaterEqual(data.percent_cpu, 0)
        self.assertGreaterEqual(data.max_rss, 0)
        self.assertGreaterEqual(data.max_vmem, 0)
        self.assertEqual(data.slots, 1)
        self.assertGreater(len(data.hostname), 0)
        self.assertGreater(len(data.task), 0)
        self.assertGreaterEqual(data.total_t, 0)
        self.assertGreaterEqual(data.wall_t, 0)
        self.assertGreaterEqual(data.user_t, 0)
        self.assertGreaterEqual(data.sys_t, 0)
        self.assertLess(data.start_time, data.end_time)
        self.assertLess(data.submission_time, data.end_time)
        self.assertEqual(data.statement, statement)

    def test_single_job_returns_runtime_information(self):

        statement = self.cmd_work
        benchmark_data = P.run(
            statement,
            to_cluster=self.to_cluster)

        self.assertIsInstance(benchmark_data, list)
        self.assertEqual(len(benchmark_data), 1)
        d = benchmark_data.pop()
        self.validate_benchmark_data(d, statement)

    def test_multiple_jobs_return_runtime_information(self):

        statements = [self.cmd_work] * 3

        benchmark_data = P.run(
            statements,
            to_cluster=self.to_cluster)

        self.assertIsInstance(benchmark_data, list)
        self.assertEqual(len(benchmark_data), len(statements))

        for d, s in zip(benchmark_data, statements):
            self.validate_benchmark_data(d, s)

    def test_array_job_returns_runtime_information(self):

        statements = [self.cmd_work] * 3

        benchmark_data = P.run(
            statements,
            job_array=True,
            to_cluster=self.to_cluster)

        self.assertIsInstance(benchmark_data, list)
        self.assertEqual(len(benchmark_data), len(statements))

        for d, s in zip(benchmark_data, statements):
            self.validate_benchmark_data(d, s)


class TestExecutionRuncluster(TestExecutionRunLocal):
    to_cluster = True

    def setUp(self):
        TestExecutionRunLocal.setUp(self)
        P.start_session()

    def tearDown(self):
        TestExecutionRunLocal.tearDown(self)
        P.close_session()

    def file_exists(self, filename, hostname=None, expect=False):
        return iotools.remote_file_exists(filename, hostname, expect)


if __name__ == "__main__":
    unittest.main()
