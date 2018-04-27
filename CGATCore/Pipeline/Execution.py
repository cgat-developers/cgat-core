"""Execution.py - Job control for ruffus pipelines
=========================================================

Session
-------

This module manages a DRMAA session. :func:`start_session`
starts a session and :func:`close_session` closes it.

Reference
---------

"""

import collections
import importlib
import os
import pickle
import re
import json
import stat
import logging
import subprocess
import sys
import time
import math
import shutil
import gevent
import CGATCore.Experiment as E
import CGATCore.IOTools as IOTools

from CGATCore.Pipeline.Utils import get_caller_locals, get_caller, get_calling_function
from CGATCore.Pipeline.Files import get_temp_filename, get_temp_dir
from CGATCore.Pipeline.Parameters import substitute_parameters, get_params
from CGATCore.Pipeline.Cluster import setup_drmaa_job_template, \
    get_drmaa_job_stdout_stderr, \
    set_drmaa_job_paths


# talking to a cluster
try:
    import drmaa
    HAS_DRMAA = True
except RuntimeError:
    HAS_DRMAA = False
except ImportError:
    HAS_DRMAA = False

# global drmaa session
GLOBAL_SESSION = None

# Timeouts for event loop
GEVENT_TIMEOUT_STARTUP = 5
GEVENT_TIMEOUT_WAIT = 1


def get_logger():
    return logging.getLogger("CGATCore.pipeline")


def _pickle_args(args, kwargs):
    ''' Pickle a set of function arguments. Removes any kwargs that are
    arguements to submit first. Returns a tuple, the first member of which
    is the key word arguements to submit, the second is a file name
    with the picked call arguements '''

    use_args = ["to_cluster",
                "logfile",
                "job_options",
                "job_queue",
                "job_threads",
                "job_memory"]

    submit_args = {}

    for arg in use_args:
        if arg in kwargs:
            submit_args[arg] = kwargs[arg]
            del kwargs[arg]

    args_file = get_temp_filename(shared=True)
    pickle.dump([args, kwargs], open(args_file, "wb"))
    return (submit_args, args_file)


def start_session():
    """start and initialize the global DRMAA session."""
    global GLOBAL_SESSION

    if HAS_DRMAA and GLOBAL_SESSION is None:
        GLOBAL_SESSION = drmaa.Session()
        try:
            GLOBAL_SESSION.initialize()
        except drmaa.errors.InternalException as ex:
            get_logger().warn("could not initialize global drmaa session: {}".format(
                ex))
            GLOBAL_SESSION = None
        return GLOBAL_SESSION


def close_session():
    """close the global DRMAA session."""
    global GLOBAL_SESSION

    if GLOBAL_SESSION is not None:
        GLOBAL_SESSION.exit()
        GLOBAL_SESSION = None


def shellquote(statement):
    '''shell quote a string to be used as a function argument.

    from http://stackoverflow.com/questions/967443/
    python-module-to-shellquote-unshellquote
    '''
    _quote_pos = re.compile('(?=[^-0-9a-zA-Z_./\n])')

    if statement:
        return _quote_pos.sub('\\\\', statement).replace('\n', "'\n'")
    else:
        return "''"


def file_is_mounted(filename):
    """return True if filename is mounted.

    A file is likely to be mounted if it is located
    inside a subdirectory of the local scratch directory.
    """
    if get_params()["mount_point"]:
        return os.path.abspath(filename).startswith(get_params()["mount_point"])
    else:
        return False


def get_mounted_location(filename):
    """return location of filename within mounted directory

    """
    return os.path.abspath(filename)[len(get_params()["mount_point"]):]


@E.cached_function
def get_conda_environment_directory(env_name):
    stdout = E.run("conda env list", return_stdout=True).strip()
    env_map = {}
    for line in stdout.splitlines():
        if line.startswith("#"):
            continue
        parts = re.split(" +", line)
        if len(parts) == 2:
            env_map[parts[0]] = parts[1]
        elif len(parts) == 3:
            env_map[parts[0]] = parts[2]
    if env_name not in env_map:
        raise IOError("conda environment {} does not exist, found {}".format(
            env_name, sorted(env_map.keys())))
    return env_map[env_name]


def execute(statement, **kwargs):
    '''execute a statement locally.

    This method implements the same parameter interpolation
    as the function :func:`run`.

    Arguments
    ---------
    statement : string
        Command line statement to run.

    Returns
    -------
    stdout : string
        Data sent to standard output by command
    stderr : string
        Data sent to standard error by command
    '''

    if not kwargs:
        kwargs = get_caller_locals()

    kwargs = dict(list(get_params().items()) + list(kwargs.items()))

    logger = get_logger()
    logger.debug("running %s" % (statement % kwargs))

    if "cwd" not in kwargs:
        cwd = get_params()["workingdir"]
    else:
        cwd = kwargs["cwd"]

    # cleaning up of statement
    # remove new lines and superfluous spaces and tabs
    statement = " ".join(re.sub("\t+", " ", statement).split("\n")).strip()
    if statement.endswith(";"):
        statement = statement[:-1]

    # always use bash
    os.environ.update({'BASH_ENV': os.path.join(os.environ['HOME'], '.bashrc')})
    process = subprocess.Popen(statement % kwargs,
                               cwd=cwd,
                               shell=True,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               env=os.environ.copy(),
                               executable="/bin/bash")

    # process.stdin.close()
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        raise OSError(
            "Child was terminated by signal %i: \n"
            "The stderr was: \n%s\n%s\n" %
            (-process.returncode, stderr, statement))

    return stdout, stderr


def interpolate_statement(statement, kwargs):
    '''interpolate command line statement with parameters

    The skeleton of the statement should be defined in kwargs.  The
    method then applies string interpolation using a dictionary built
    from the global configuration dictionary PARAMS, but augmented by
    `kwargs`. The latter takes precedence.

    Arguments
    ---------
    statement: string
        Command line statement to be interpolated.
    kwargs : dict
        Keyword arguments that are used for parameter interpolation.

    Returns
    -------
    statement : string
        The command line statement with interpolated parameters.

    Raises
    ------
    KeyError
        If ``statement`` contains unresolved references.

    '''

    local_params = substitute_parameters(**kwargs)

    # build the statement
    try:
        statement = statement % local_params
    except KeyError as msg:
        raise KeyError(
            "Error when creating command: could not "
            "find %s in dictionaries" % msg)
    except ValueError as msg:
        raise ValueError(
            "Error when creating command: %s, statement = %s" % (
                msg, statement))

    # cleaning up of statement
    # remove new lines and superfluous spaces and tabs
    statement = " ".join(re.sub("\t+", " ", statement).split("\n")).strip()
    if statement.endswith(";"):
        statement = statement[:-1]

    # mark arvados mount points in statement
    if get_params().get("mount_point", None):
        statement = re.sub(get_params()["mount_point"], "arv=", statement)

    return statement


def join_statements(statements, infile, outfile=None):
    '''join a chain of statements into a single statement.

    Each statement contains an @IN@ or a @OUT@ placeholder or both.
    These will be replaced by the names of successive temporary files.

    In the first statement, @IN@ is replaced with `infile` and, if given,
    the @OUT@ is replaced by outfile in the last statement.

    Arguments
    ---------
    statements : list
        A list of command line statements.
    infile : string
        Filename of the first data set.
    outfile : string
        Filename of the target data set.

    Returns
    -------
    last_file : string
        Filename of last file created, outfile, if given.
    statement : string
        A command line statement built from merging the statements
    cleanup : string
        A command line statement for cleaning up.

    '''

    prefix = get_temp_filename()

    pattern = "%s_%%i" % prefix

    result = []
    for x, statement in enumerate(statements):
        s = statement
        if x == 0:
            if infile is not None:
                s = re.sub("@IN@", infile, s)
        else:
            s = re.sub("@IN@", pattern % x, s)
            if x > 2:
                s = re.sub("@IN-2@", pattern % (x - 2), s)
            if x > 1:
                s = re.sub("@IN-1@", pattern % (x - 1), s)

        s = re.sub("@OUT@", pattern % (x + 1), s).strip()

        if s.endswith(";"):
            s = s[:-1]
        result.append(s)

    result = "; ".join(result)
    last_file = pattern % (x + 1)
    if outfile:
        result = re.sub(last_file, outfile, result)
        last_file = outfile

    assert prefix != ""
    return last_file, result, "rm -f %s*" % prefix


BENCHMARK_DATA2TYPE = [
    ("task", str),
    ("statement", str),
    ("hostname", str),
    ("job_id", str),
    ("engine", str),
    ("submit_time", float),
    ("start_time", float),
    ("end_time", float),
    ("slots", float),
    ("exit_status", float),
    ("total_t", float),
    ("cpu_t", float),
    ("wall_t", float),
    ("user_t", float),
    ("sys_t", float),
    ("child_user_t", float),
    ("child_sys_t", float),
    ("shared_data", float),
    ("io_input", float),
    ("io_output", float),
    ("average_memory_total", float),
    ("percent_cpu", float),
    ("average_rss", float),
    ("max_rss", float),
    ("max_vmem", float),
    ("minor_page_faults", float),
    ("swapped", float),
    ("context_switches_involuntarily", float),
    ("context_switches_voluntarily", float),
    ("average_uss", float),
    ("signal", float),
    ("socket_received", float),
    ("socket_sent", float),
    ("major_page_faults", float),
    ("unshared_data", float)]


MAP_DRMAA2BENCHMARK_DATA = {
    "task": "task",
    "statement": "statement",
    "hostname": "hostname",
    "job_id": "job_id",
    "engine": "engine",
    "submit_time": "submit_time",
    "start_time": "start_time",
    "end_time": "end_time",
    "slots": "slots",
    "exit_status": "exit_status",
    "total_t": "total_t",
    "wall_t": "ru_wallclock",
    "cpu_t": "cpu",
    "user_t": "ru_utime",
    "sys_t": "ru_stime",
    # "child_user_t":
    # "child_sys_t",
    "shared_data": "ru_ixrss",
    "io_input": "ru_inblock",
    "io_output": "ru_oublock",
    # SGE this is integral memory usage (memory * time)
    "average_memory_total": "mem",
    # needs to be set manually as user + system times divided by the total running time
    "percent_cpu": "percent_cpu",
    # SGE this is integral memory usage
    "average_rss": "mem",
    "max_rss": "ru_maxrss",
    "max_vmem": "maxvmem",
    "minor_page_faults": "ru_minflt",
    "swapped": "ru_nswap",
    "context_switches_involuntarily": "ru_nvcsw",
    "context_switches_voluntarily": "ru_nivcsw",
    "average_uss": "ru_isrss",
    "signal": "signal",
    "socket_received": "ru_msgrcv",
    "socket_sent": "ru_msgsnd",
    "major_page_faults": "ru_majflt",
    "unshared_data": "ru_idrss"}


def will_run_on_cluster(options):
    run_on_cluster = options.get("to_cluster", True) and \
        not options.get("without_cluster", False) and \
        HAS_DRMAA and \
        GLOBAL_SESSION

    return run_on_cluster


class Executor(object):

    def __init__(self, **kwargs):

        self.logger = get_logger()

        self.job_threads = kwargs.get("job_threads", 1)

        if "job_memory" in kwargs and "job_total_memory" in kwargs:
            raise ValueError("both job_memory and job_total_memory have been given")

        if "job_total_memory" in kwargs:
            self.job_total_memory = kwargs['job_total_memory']
            self.job_memory = IOTools.bytes2human(
                IOTools.human2bytes(self.job_total_memory) / self.job_threads)
        elif 'job_memory' in kwargs and kwargs["job_memory"]:
            # job_memory takes precedence over job_total_memory
            self.job_memory = kwargs['job_memory']
            self.job_total_memory = self.job_memory * self.job_threads
        else:
            self.job_memory = get_params()["cluster"].get("memory_default", "4G")
            self.job_total_memory = self.job_memory * self.job_threads

        self.ignore_pipe_errors = kwargs.get('ignore_pipe_errors', False)
        self.ignore_errors = kwargs.get('ignore_errors', False)

        self.job_name = kwargs.get("job_name", "unknow_job_name")
        self.task_name = kwargs.get("task_name", "unknown_task_name")

        # deduce output directory/directories, requires somewhat
        # consistent naming in the calling function.
        outfiles = []
        if "outfile" in kwargs:
            outfiles.append(kwargs["outfile"])
        if "outfiles" in kwargs:
            outfiles.extend(kwargs["outfiles"])

        self.output_directories = set(sorted(
            [os.path.dirname(x) for x in outfiles]))

        self.options = kwargs

        self.workingdir = get_params()["workingdir"]

        self.shellfile = kwargs.get("shell_logfile", "shell.log")
        if self.shellfile:
            if not self.shellfile.startswith(os.sep):
                self.shellfile = os.path.join(self.workingdir, "shell.log")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def expand_statement(self, statement):
        '''add generic commands before and after statement.

        The method scans the statement for arvados mount points and
        inserts appropriate prefixes to make sure that the mount point
        exists.

        Arguments
        ---------
        statement : string
            Command line statement to expand

        Returns
        -------
        statement : string
            The expanded statement.

        '''

        setup_cmds = []
        teardown_cmds = []
        cleanup_funcs = []

        # create local scratch if it does not already exists. Note that
        # directory itself will be not deleted while its contents should
        # be cleaned up.
        setup_cmds.append("umask 002")
        setup_cmds.append("mkdir -p {}".format(get_params()["tmpdir"]))

        if "arv=" in statement:

            # Todo: permit setting this in params
            arvados_api_token = os.environ.get("ARVADOS_API_TOKEN", None)
            arvados_api_host = os.environ.get("ARVADOS_API_HOST", None)
            if not arvados_api_token:
                raise ValueError(
                    "arvados mount encountered in statement {}, "
                    "but ARVADOS_API_TOKEN not defined".format(statement))

            if not arvados_api_host:
                raise ValueError(
                    "arvados mount encountered in statement {}, "
                    "but ARVADOS_API_HOST not defined".format(statement))

            mountpoint = get_temp_filename(clear=True)

            arvados_options = "--disable-event-listening --read-only"
            setup_cmds.append("\n".join(
                ('export ARVADOS_API_TOKEN="{arvados_api_token}"',
                 'export ARVADOS_API_HOST="{arvados_api_host}"',
                 'export ARVADOS_API_HOST_INSECURE=true',
                 'export ARVADOS_MOUNT_POINT="{mountpoint}"',
                 'mkdir -p "{mountpoint}"',
                 'arv-mount {arvados_options} "{mountpoint}" 2>> /dev/null')).format(**locals()))

            statement = re.sub("arv=", mountpoint + "/", statement)

            # "arv-mount --unmount {mountpoint}" not available in newer
            # arvados installs (0.1.20170707152712), so keep using
            # fusermount. However, do not fail if you can't clean up, as
            # there are arvados racing issues.
            cleanup_funcs.append(("unmount_arvados",
                                  '''{{
                                  set +e &&
                                  fusermount -u {mountpoint} &&
                                  rm -rf {mountpoint} &&
                                  set -e
                                  }}'''.format(**locals())))

        if "job_condaenv" in self.options:
            # In conda < 4.4 there is an issue with parallel activations,
            # see https://github.com/conda/conda/issues/2837 .
            # This has been fixed in conda 4.4, but we are on conda
            # 4.3, presumably because we are still on py35. A work-around
            # to source activate is to add the explicit path of the environment
            # in version >= 4.4, do
            # setup_cmds.append(
            #     "conda activate {}".format(self.options["job_condaenv"]))
            # For old conda versions (note this will not work for tools that require
            # additional environment variables)
            setup_cmds.append(
                "export PATH={}:$PATH".format(
                    os.path.join(
                        get_conda_environment_directory(self.options["job_condaenv"]),
                        "bin")))

        statement = "\n".join((
            "\n".join(setup_cmds),
            statement,
            "\n".join(teardown_cmds)))

        return statement, cleanup_funcs

    def build_job_script(self,
                         statement):
        '''build job script from statement.

        returns (name_of_script, stdout_path, stderr_path)
        '''
        tmpfilename = get_temp_filename(dir=self.workingdir, clear=True)
        tmpfilename = tmpfilename + ".sh"

        tmpdir = get_temp_dir(clear=True)

        expanded_statement, cleanup_funcs = self.expand_statement(statement)

        with open(tmpfilename, "w") as tmpfile:
            # disabled: -l -O expand_aliases\n" )

            # make executable
            tmpfile.write("#!/bin/bash -eu\n")
            if not self.ignore_pipe_errors:
                tmpfile.write("set -o pipefail\n")

            os.chmod(tmpfilename, stat.S_IRWXG | stat.S_IRWXU)

            tmpfile.write("\ncd {}\n".format(self.workingdir))
            if self.output_directories is not None:
                for outdir in self.output_directories:
                    if outdir:
                        tmpfile.write("\nmkdir -p {}\n".format(outdir))

            # create and set system scratch dir for temporary files
            tmpfile.write("umask 002\n")
            tmpfile.write("mkdir -p {}\n".format(tmpdir))
            tmpfile.write("export TMPDIR={}\n".format(tmpdir))
            cleanup_funcs.append(
                ("clean_temp",
                 "{{ rm -rf {}; }}".format(tmpdir)))

            # output times whenever script exits, preserving
            # return status
            cleanup_funcs.append(("info",
                                  "{ echo 'benchmark'; hostname; times; }"))
            for cleanup_func, cleanup_code in cleanup_funcs:
                tmpfile.write("\n{}() {}\n".format(cleanup_func, cleanup_code))

            tmpfile.write("\nclean_all() {{ {}; }}\n".format(
                "; ".join([x[0] for x in cleanup_funcs])))

            tmpfile.write("\ntrap clean_all EXIT\n\n")

            if self.job_memory != "unlimited" and self.job_memory != "etc" and get_params()["cluster_memory_ulimit"]:
                # restrict virtual memory
                # Note that there are resources in SGE which could do this directly
                # such as v_hmem.
                # Note that limiting resident set sizes (RSS) with ulimit is not
                # possible in newer kernels.
                # -v and -m accept memory in kb
                requested_memory_kb = max(
                    1000,
                    int(math.ceil(
                        IOTools.human2bytes(self.job_memory) / 1024 * self.job_threads)))
                # unsetting error exit as often not permissions
                tmpfile.write("set +e\n")
                tmpfile.write("ulimit -v {} > /dev/null \n".format(
                    requested_memory_kb))
                tmpfile.write("ulimit -m {} > /dev/null \n".format(
                    requested_memory_kb))
                # set as hard limit
                tmpfile.write("ulimit -H -v > /dev/null \n")
                tmpfile.write("set -e\n")

            if self.shellfile:

                # make sure path exists that we want to write to
                tmpfile.write("mkdir -p $(dirname \"{}\")\n".format(
                    self.shellfile))

                # output low-level debugging information to a shell log file
                tmpfile.write(
                    'echo "%s : START -> %s" >> %s\n' %
                    (self.job_name, tmpfilename, self.shellfile))
                # disabled - problems with quoting
                # tmpfile.write( '''echo 'statement=%s' >> %s\n''' %
                # (shellquote(statement), self.shellfile) )
                tmpfile.write("set | sed 's/^/%s : /' >> %s\n" %
                              (self.job_name, self.shellfile))
                tmpfile.write("pwd | sed 's/^/%s : /' >> %s\n" %
                              (self.job_name, self.shellfile))
                tmpfile.write("hostname | sed 's/^/%s: /' >> %s\n" %
                              (self.job_name, self.shellfile))
                # cat /proc/meminfo is Linux specific
                if get_params()['os'] == 'Linux':
                    tmpfile.write("cat /proc/meminfo | sed 's/^/%s: /' >> %s\n" %
                                  (self.job_name, self.shellfile))
                elif get_params()['os'] == 'Darwin':
                    tmpfile.write("vm_stat | sed 's/^/%s: /' >> %s\n" %
                                  (self.job_name, self.shellfile))
                tmpfile.write(
                    'echo "%s : END -> %s" >> %s\n' %
                    (self.job_name, tmpfilename, self.shellfile))
                tmpfile.write("ulimit | sed 's/^/%s: /' >> %s\n" %
                              (self.job_name, self.shellfile))

            job_path = os.path.abspath(tmpfilename)

            tmpfile.write(expanded_statement)
            tmpfile.write("\n\n")
            tmpfile.close()

        return statement, job_path

    def collect_benchmark_data(self,
                               stdout,
                               statements,
                               start_time,
                               end_time,
                               time_data_file=None,
                               resource_usage=None):
        """collect benchmark data from a job's stdout.

        If time_data is given, read output from time command.
        """

        def parse_time(t):
            minutes, seconds = re.search(r"(\d+)m(\S*)s", t).groups()
            return int(minutes) * 60 + float(seconds)

        benchmark_data = []

        BenchmarkData = collections.namedtuple(
            "BenchmarkData", ([x[0] for x in BENCHMARK_DATA2TYPE]))

        logger = get_logger()

        # todo: this is obsolete, instead rely on time_data_file
        if len(stdout) < 4 or not stdout[-4].startswith("benchmark"):
            logger.warn("could not retrieve performance metrics "
                        "from stdout: {}".format(stdout))
            hostname = "unknown"
        else:
            hostname = stdout[-3][:-1]
            user_t, sys_t = [parse_time(t) for t in stdout[-2].split()]
            child_user_t, child_sys_t = [parse_time(t) for t in stdout[-1].split()]
            cpu_t = user_t + sys_t + child_user_t + child_sys_t

        if resource_usage is None:
            # if no time_data file is present, default values will be 0
            data = collections.defaultdict(int)

            if time_data_file is not None and os.path.exists(time_data_file):
                with open(time_data_file) as inf:
                    data = [x[:-1].split("\t") for x in inf if x]
                    # remove any non-key-value pairs
                    data = dict([x for x in data if len(x) == 2])
                # remove % sign
                data.update(
                    {"percent_cpu": int(re.sub("%", "", data.get("percent_cpu", 0))),
                     "cpu_t": float(data["user_t"]) + float(data["sys_t"])})

            data.update(
                {"task": self.task_name,
                 "statement": ";".join(statements),
                 "hostname": hostname,
                 "engine": self.__class__.__name__,
                 "start_time": start_time,
                 "end_time": end_time,
                 "submit_time": start_time,
                 "total_t": end_time - start_time,
                 "slots": self.job_threads})

            # a local job
            benchmark_data.append(BenchmarkData._make((
                (data.get(x, "0") for x in BenchmarkData._fields))))

        elif resource_usage is not None:
            # save data from DRMAA return values
            for jobinfo, statement in zip(resource_usage, statements):
                # augment return values with additional data
                data = jobinfo.resourceUsage
                data.update(
                    {"task": self.task_name,
                     "hostname": hostname,
                     "statement": statement,
                     "job_id": jobinfo.jobId,
                     "engine": self.__class__.__name__,
                     "slots": self.job_threads,
                     "total_t": end_time - start_time,
                     # avoid division by 0 error
                     "percent_cpu": (
                         100.0 * float(data.get("cpu", 0)) /
                         max(1.0, float(data.get("end_time", 0)) - float(data.get("start_time", 0))) /
                         self.job_threads)})
                # map fields to BenchmarkData fields
                benchmark_data.append(
                    BenchmarkData._make(
                        [data.get(MAP_DRMAA2BENCHMARK_DATA.get(key, None), 0)
                         for key in BenchmarkData._fields]))

        # type conversion
        data2type = dict(BENCHMARK_DATA2TYPE)
        for x, d in enumerate(benchmark_data):
            benchmark_data[x] = BenchmarkData._make(
                [data2type[key](val) for key, val in zip(
                    BenchmarkData._fields, d)])

        return benchmark_data


class GridExecutor(Executor):

    def __init__(self, **kwargs):
        Executor.__init__(self, **kwargs)
        self.session = GLOBAL_SESSION
        if self.session is None:
            raise ValueError("no Grid Session found")

        # if running on cluster, use a working directory on shared drive
        self.workingdir_is_local = IOTools.is_local(self.workingdir)

        # connect to global session
        pid = os.getpid()
        self.logger.debug('task: pid={}, grid-session={}, workingdir={}'.format(
            pid, str(self.session), self.workingdir))

    def __enter__(self):
        # for cluster execution, the working directory can not be
        # local.  Use a temporary shared location instead and copy
        # files over after job has completed.
        if self.workingdir_is_local:
            self.original_dir = self.workingdir
            self.workingdir = get_temp_dir(shared=True)

        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if self.workingdir_is_local:
            destdir = self.original_dir
            self.logger.debug("moving files from {} to {}".format(
                self.workingdir, destdir))

            for root, dirs, files in os.walk(self.workingdir):
                for d in dirs:
                    if not os.path.exists(os.path.join(destdir, d)):
                        os.makedirs(d)
                for fn in files:
                    shutil.move(os.path.join(root, fn),
                                os.path.join(self.workingdir, root, fn))

            shutil.rmtree(self.workingdir)

    def collect_single_job_from_cluster(self,
                                        job_id,
                                        statement,
                                        stdout_path, stderr_path,
                                        job_path):
        '''runs a single job on the cluster.'''
        try:
            retval = self.session.wait(job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        except Exception as msg:
            # ignore message 24, indicates jobs that have been qdel'ed
            if not str(msg).startswith("code 24"):
                raise
            retval = None

        stdout, stderr = get_drmaa_job_stdout_stderr(stdout_path, stderr_path)
        resource_usage = None

        if retval is not None and not self.ignore_errors:
            if retval.exitStatus != 0:
                raise OSError(
                    "---------------------------------------\n"
                    "Job {} exited with error code {}: \n"
                    "The stderr was: \n{}\n{}\n"
                    "-----------------------------------------".format(
                        job_id, retval.exitStatus, "".join(stderr), statement))

            elif retval.hasSignal:
                raise OSError(
                    "---------------------------------------\n"
                    "Job {} was terminated by signal {}: \n"
                    "The stderr was: \n{}\n{}\n"
                    "-----------------------------------------".format(
                        job_id, retval.terminatedSignal, "".join(stderr), statement))

            elif retval.hasExited is False or retval.wasAborted is True:
                raise OSError(
                    "-------------------------------------------------\n"
                    "Cluster job was aborted (%s) and/or failed to exit (%s) "
                    "while running the following statement:\n"
                    "\n%s\n"
                    "(Job may have been cancelled by the user or the scheduler)\n"
                    "----------------------------------------------------------\n"
                    .format(retval.wasAborted, not retval.hasExited, statement))

            try:
                resource_usage = [retval]
            except AttributeError:
                pass

        try:
            os.unlink(job_path)
        except OSError:
            self.logger.warn(
                ("temporary job file %s not present for "
                 "clean-up - ignored") % job_path)

        return stdout, stderr, resource_usage

    def run(self, statement_list):

        # submit statements to cluster individually.
        benchmark_data = []
        jt = self.setup_job(self.options["cluster"])

        self.logger.debug("job-options: %s" % jt.nativeSpecification)

        start_time = time.time()
        job_ids, filenames = [], []
        for statement in statement_list:
            self.logger.debug("running statement:\n%s" % statement)

            full_statement, job_path = self.build_job_script(statement)

            stdout_path, stderr_path = set_drmaa_job_paths(jt, job_path)

            job_id = self.session.runJob(jt)
            job_ids.append(job_id)
            filenames.append((job_path, stdout_path, stderr_path))
            self.logger.debug("job has been submitted with job_id %s" % str(job_id))
            # give back control for bulk submission
            gevent.sleep(GEVENT_TIMEOUT_STARTUP)

        self.wait_for_job_completion(job_ids)

        end_time = time.time()

        # collect and clean up
        for job_id, statement, paths in zip(job_ids,
                                            statement_list,
                                            filenames):
            job_path, stdout_path, stderr_path = paths
            # TODO: collect timings from individual jobs
            stdout, stderr, resource_usage = self.collect_single_job_from_cluster(
                job_id,
                statement,
                stdout_path,
                stderr_path,
                job_path)

            # end time is meaningless
            end_time = time.time()
            benchmark_data.extend(
                self.collect_benchmark_data(stdout,
                                            [statement],
                                            start_time,
                                            end_time,
                                            resource_usage=resource_usage))

        self.session.deleteJobTemplate(jt)
        return benchmark_data

    def setup_job(self, options):

        return setup_drmaa_job_template(self.session,
                                        job_name=self.job_name,
                                        job_memory=self.job_memory,
                                        job_threads=self.job_threads,
                                        working_directory=self.workingdir,
                                        **options)

    def wait_for_job_completion(self, job_ids):

        self.logger.debug("waiting for %i jobs to finish " % len(job_ids))
        running_job_ids = set(job_ids)
        while running_job_ids:
            for job_id in list(running_job_ids):
                status = self.session.jobStatus(job_id)
                if status in (drmaa.JobState.DONE, drmaa.JobState.FAILED):
                    running_job_ids.remove(job_id)
                else:
                    gevent.sleep(GEVENT_TIMEOUT_WAIT)
                    break


class GridArrayExecutor(GridExecutor):

    def run(self, statement_list):

        benchmark_data = []
        # run statements through array interface
        jobsfile = get_temp_filename(dir=self.workingdir,
                                     clear=True) + ".jobs"

        with open(jobsfile, "w") as outf:
            outf.write("\n".join(statement_list))

        master_statement = (
            "CMD=$(awk \"NR==$SGE_TASK_ID\" {jobsfile}); "
            "eval $CMD".format(**locals()))

        full_statement, job_path = self.build_job_script(master_statement)

        jt = self.setup_job(self.options["cluster"])

        stdout_path, stderr_path = set_drmaa_job_paths(jt, job_path)

        start_time = time.time()
        job_id, stdout, stderr, resource_usage = self.run_array_job(
            self.session, jt, stdout_path, stderr_path,
            full_statement, start=0, end=len(statement_list),
            increment=1)
        end_time = time.time()

        benchmark_data.extend(
            self.collect_benchmark_data(stdout,
                                        statement_list,
                                        start_time,
                                        end_time,
                                        resource_usage=resource_usage))
        try:
            os.unlink(jobsfile)
        except OSError:
            pass

        return benchmark_data

    def run_array_job(self, session, jt, stdout_path, stderr_path,
                      statement, start, end, increment):

        logger = get_logger()
        logger.debug("starting an array job: %i-%i,%i" %
                     (start, end, increment))

        jt.outputPath = ":" + stdout_path
        jt.errorPath = ":" + stderr_path

        logger.debug("job submitted with %s" % jt.nativeSpecification)

        # sge works with 1-based, closed intervals
        job_ids = session.runBulkJobs(jt, start + 1, end, increment)
        logger.debug("%i array jobs have been submitted as job_id %s" %
                     (len(job_ids), job_ids[0]))

        self.wait_for_job_completion(job_ids)

        logger.debug("%i array jobs for job_id %s have completed" %
                     (len(job_ids), job_ids[0]))

        resource_usage = []
        for job in job_ids:
            r = session.wait(job, drmaa.Session.TIMEOUT_WAIT_FOREVER)
            resource_usage.append(r)

        stdout, stderr = get_drmaa_job_stdout_stderr(stdout_path, stderr_path)
        job_id = job_ids[0]
        return job_id, stdout, stderr, resource_usage


class LocalExecutor(Executor):

    def run(self, statement_list):

        benchmark_data = []
        for statement in statement_list:
            self.logger.debug("running statement:\n%s" % statement)

            full_statement, job_path = self.build_job_script(statement)

            # max_vmem is set to max_rss, not available by /usr/bin/time
            full_statement = (
                "\\time --output=%s.times "
                "-f '"
                "exit_status\t%%x\n"
                "user_t\t%%U\n"
                "sys_t\t%%S\n"
                "wall_t\t%%e\n"
                "shared_data\t%%D\n"
                "io_input\t%%I\n"
                "io_output\t%%O\n"
                "average_memory_total\t%%K\n"
                "percent_cpu\t%%P\n"
                "average_rss\t%%t\n"
                "max_rss\t%%M\n"
                "max_vmem\t%%M\n"
                "minor_page_faults\t%%R\n"
                "swapped\t%%W\n"
                "context_switches_involuntarily\t%%c\n"
                "context_switches_voluntarily\t%%w\n"
                "average_uss\t%%p\n"
                "signal\t%%k\n"
                "socket_received\t%%r\tn"
                "socket_sent\t%%s\n"
                "major_page_fault\t%%F\n"
                "unshared_data\t%%D\n' "
                "%s") % (job_path, job_path)

            while 1:
                start_time = time.time()

                os.environ.update({'BASH_ENV': os.path.join(os.environ['HOME'], '.bashrc')})
                process = subprocess.Popen(
                    full_statement,
                    cwd=self.workingdir,
                    shell=True,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=os.environ.copy(),
                    close_fds=True,
                    executable="/bin/bash")

                # process.stdin.close()
                stdout, stderr = process.communicate()

                end_time = time.time()

                if process.returncode == 126:
                    self.logger.warn("repeating execution: message={}".format(stderr))
                    time.sleep(1)
                    continue

                break

            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            if process.returncode != 0 and not self.ignore_errors:
                raise OSError(
                    "---------------------------------------\n"
                    "Child was terminated by signal %i: \n"
                    "The stderr was: \n%s\n%s\n"
                    "-----------------------------------------" %
                    (-process.returncode, stderr, statement))

            benchmark_data.extend(
                self.collect_benchmark_data(
                    stdout.splitlines(True),
                    [statement],
                    start_time,
                    end_time,
                    time_data_file=job_path + ".times"))

            try:
                os.unlink(job_path)
                os.unlink(job_path + ".times")
            except OSError:
                pass

        return benchmark_data


class LocalArrayExecutor(LocalExecutor):
    pass


def make_runner(**kwargs):
    """factory function returning an object capable of executing
    a list of command line statements.
    """

    run_as_array = "job_array" in kwargs and kwargs["job_array"] is not None

    # run on cluster if:
    # * to_cluster is not defined or set to True
    # * command line option without_cluster is set to False
    # * an SGE session is present
    run_on_cluster = will_run_on_cluster(kwargs)

    if run_on_cluster:
        if run_as_array:
            runner = GridArrayExecutor(**kwargs)
        else:
            runner = GridExecutor(**kwargs)
    else:
        if run_as_array:
            runner = LocalArrayExecutor(**kwargs)
        else:
            runner = LocalExecutor(**kwargs)

    return runner


def run(statement, **kwargs):
    """run a command line statement.

    This function runs a single or multiple statements either locally
    or on the cluster using drmaa. How a statement is executed or how
    it is modified depends on the context.

    The context is provided by keyword arguments provided as named
    function arguments ('kwargs') but also from defaults (see
    below). The following keyword arguments are recognized:

    job_memory
        memory to use for the job per thread. Memory specification should be in a
        format that is accepted by the job scheduler. Note that memory
        is per thread. If you have 6 threads and the total memory is
        6Gb, use 1G as job_memory.
    job_total_memory
        total memory to use for a job. This will be divided by the number of
        threads.
    job_threads
        number of threads to request for the job.
    job_options
        options to the job scheduler.
    job_condaenv
        conda environment to use for the job.
    job_array
        if set, run statement as an array job. Job_array should be
        tuple with start, end, and increment.

    In addition, any additional variables will be used to interpolate
    the command line string using python's '%' string interpolation
    operator.

    The context is build in a hierarchical manner with successive
    operations overwriting previous values.

    1. Global variables
       The context is initialized
       with system-wide defaults stored in the global PARAMS
       singleton.
    2. Context of caller
       The context of the calling function is examined
       and any local variables defined in this context are added.
    3. kwargs
       Any options given explicitely as options to the run() method
       are added.
    4. params
       If the context of the calling function contains a params
       variable, its contents are added to the context. This permits
       setting variables in configuration files in TaskLibrary
       functions.

    By default, a job is sent to the cluster, unless:

        * ``to_cluster`` is present and set to None.

        * ``without_cluster`` is True.

        * ``--local`` has been specified on the command line
          and the option ``without_cluster`` has been set as
          a result.

        * no libdrmaa is present

        * the global session is not initialized (GLOBAL_SESSION is
          None)

    Troubleshooting:

       1. DRMAA creates sessions and their is a limited number
          of sessions available. If there are two many or sessions
          become not available after failed jobs, use ``qconf -secl``
          to list sessions and ``qconf -kec #`` to delete sessions.

       2. Memory: 1G of free memory can be requested using the job_memory
          variable: ``job_memory = "1G"``
          If there are error messages like "no available queue", then the
          problem could be that a particular complex attribute has
          not been defined (the code should be ``hc`` for ``host:complex``
          and not ``hl`` for ``host:local``. Note that qrsh/qsub directly
          still works.

    The job will be executed within PARAMS["workingdir"], unless
    PARAMS["workingdir"] is not local. In that case, the job will
    be executed in a shared temporary directory.

    Arguments
    ---------
    statement : string or list of strings
        A command line statement or a list of command line statements
        to be executed.
    kwargs : dictionary
        Context for job. The context is used to interpolate the command
        line statement.

    """
    logger = get_logger()

    # combine options using priority
    options = dict(list(get_params().items()))
    caller_options = get_caller_locals()

    options.update(list(caller_options.items()))

    if "self" in options:
        del options["self"]
    options.update(list(kwargs.items()))

    # inject params dictionary from Task functions into option dict.
    # This allows passing options from the config file.
    if "params" in options:
        options.update(options["params"]._asdict())

    # insert parameters supplied through simplified interface such
    # as job_memory, job_options, job_queue
    options['cluster']['options'] = options.get(
        'job_options', options['cluster']['options'])
    options['cluster']['queue'] = options.get(
        'job_queue', options['cluster']['queue'])
    options['without_cluster'] = options.get('without_cluster')

    # SGE compatible job_name
    name_substrate = str(options.get("outfile", "CGATCore"))
    if os.path.basename(name_substrate).startswith("result"):
        name_substrate = os.path.basename(os.path.dirname(name_substrate))
    else:
        name_substrate = os.path.basename(name_substrate)

    options["job_name"] = re.sub("[:]", "_", name_substrate)
    try:
        calling_module = get_caller().__name__
    except AttributeError:
        calling_module = "unknown"

    options["task_name"] = calling_module + "." + get_calling_function()

    # build statements using parameter interpolation
    if isinstance(statement, list):
        statement_list = []
        for stmt in statement:
            statement_list.append(interpolate_statement(stmt, options))
    else:
        statement_list = [interpolate_statement(statement, options)]

    if len(statement_list) == 0:
        logger.warn("no statements found - no execution")
        return []

    if options.get("dryrun", False):
        for statement in statement_list:
            logger.info("dry-run: {}".format(statement))
        return []

    # execute statement list
    runner = make_runner(**options)

    with runner as r:
        benchmark_data = r.run(statement_list)

    # log benchmark_data
    for data in benchmark_data:
        logger.info(json.dumps(data._asdict()))

    return benchmark_data


def submit(module,
           function,
           args=None,
           infiles=None,
           outfiles=None,
           to_cluster=True,
           logfile=None,
           job_options="",
           job_threads=1,
           job_memory=False):
    '''submit a python *function* as a job to the cluster.

    This method runs the script :file:`run_function` using the
    :func:`run` method in this module thus providing the same
    control options as for command line tools.

    Arguments
    ---------
    module : string
        Module name that contains the function. If `module` is
        not part of the PYTHONPATH, an absolute path can be given.
    function : string
        Name of function to execute
    infiles : string or list
        Filenames of input data
    outfiles : string or list
        Filenames of output data
    logfile : filename
        Logfile to provide to the ``--log`` option
    job_options : string
        String for generic job options for the queuing system
    job_threads : int
        Number of slots (threads/cores/CPU) to use for the task
    job_memory : string
        Amount of memory to reserve for the job.

    '''

    if not job_memory:
        job_memory = get_params().get("cluster_memory_default", "2G")

    if type(infiles) in (list, tuple):
        infiles = " ".join(["--input=%s" % x for x in infiles])
    else:
        infiles = "--input=%s" % infiles

    if type(outfiles) in (list, tuple):
        outfiles = " ".join(["--output-section=%s" % x for x in outfiles])
    else:
        outfiles = "--output-section=%s" % outfiles

    if logfile:
        logfile = "--log=%s" % logfile
    else:
        logfile = ""

    if args:
        args = "--args=%s" % ",".join(args)
    else:
        args = ""

    statement = (
        "python -m CGATCore.Pipeline.run_function "
        "--module=%(module)s "
        "--function=%(function)s "
        "%(logfile)s "
        "%(infiles)s "
        "%(outfiles)s "
        "%(args)s")
    run(statement)


def cluster_runnable(func):
    '''A dectorator that allows a function to be run on the cluster.

    The decorated function now takes extra arguments. The most important
    is *submit*. If set to true, it will submit the function to the cluster
    via the Pipeline.submit framework. Arguments to the function are
    pickled, so this will only work if arguments are picklable. Other
    arguments to submit are also accepted.

    Note that this allows the unusal combination of *submit* false,
    and *to_cluster* true. This will submit the function as an external
    job, but run it on the local machine.

    Note: all arguments in the decorated function must be passed as
    key-word arguments.
    '''

    # MM: when decorating functions with cluster_runnable, provide
    # them as kwargs, else will throw attribute error

    function_name = func.__name__

    def submit_function(*args, **kwargs):

        if "submit" in kwargs and kwargs["submit"]:
            del kwargs["submit"]
            submit_args, args_file = _pickle_args(args, kwargs)
            module_file = os.path.abspath(
                sys.modules[func.__module__].__file__)
            submit(IOTools.snip(__file__),
                   "run_pickled",
                   args=[IOTools.snip(module_file), function_name, args_file],
                   **submit_args)
        else:
            # remove job contral options before running function
            for x in ("submit", "job_options", "job_queue"):
                if x in kwargs:
                    del kwargs[x]
            return func(*args, **kwargs)

    return submit_function


def run_pickled(params):
    ''' run a function whose arguments have been pickled.

    expects that params is [module_name, function_name, arguments_file] '''

    module_name, func_name, args_file = params
    location = os.path.dirname(module_name)
    if location != "":
        sys.path.append(location)

    module_base_name = os.path.basename(module_name)
    logger = get_logger()
    logger.info("importing module '%s' " % module_base_name)
    logger.debug("sys.path is: %s" % sys.path)

    module = importlib.import_module(module_base_name)
    try:
        function = getattr(module, func_name)
    except AttributeError as msg:
        raise AttributeError(msg.message +
                             "unknown function, available functions are: %s" %
                             ",".join([x for x in dir(module)
                                       if not x.startswith("_")]))

    args, kwargs = pickle.load(open(args_file, "rb"))
    logger.info("arguments = %s" % str(args))
    logger.info("keyword arguments = %s" % str(kwargs))

    function(*args, **kwargs)

    os.unlink(args_file)
