"""
execution.py - Task execution for CGAT pipelines
==============================================

This module handles the execution of pipeline tasks, providing support for:

1. Job Execution
   - Local execution via subprocess
   - Cluster job submission
   - Python function execution
   - Container-based execution

2. Resource Management
   - Memory monitoring and limits
   - CPU allocation
   - Runtime constraints
   - Working directory management

3. Error Handling
   - Job failure detection
   - Retry mechanisms
   - Error logging and reporting
   - Clean-up procedures

4. Execution Modes
   - Synchronous (blocking) execution
   - Asynchronous job submission
   - Parallel task execution
   - Dependency-aware scheduling

Usage Examples
-------------
1. Submit a command to the cluster:

.. code-block:: python

    statement = "samtools sort input.bam -o output.bam"
    job_options = "-l mem_free=4G"
    job_threads = 4

    execution.run(statement,
                 job_options=job_options,
                 job_threads=job_threads)

2. Execute a Python function:

.. code-block:: python

    def process_data(infile, outfile):
        # Processing logic here
        pass

    execution.submit(module="my_module",
                    function="process_data",
                    infiles="input.txt",
                    outfiles="output.txt",
                    job_memory="4G")

For detailed documentation, see: https://cgat-core.readthedocs.io/
"""

import collections
import importlib
import os
import pickle
import re
import json
import stat
import socket
import logging
import subprocess
import sys
import time
import math
import shutil
import gevent
import signal
import cgatcore.experiment as E
import cgatcore.iotools as iotools

from cgatcore.pipeline.utils import get_caller_locals, get_caller, get_calling_function
from cgatcore.pipeline.files import get_temp_filename, get_temp_dir
from cgatcore.pipeline.parameters import substitute_parameters, get_params
from cgatcore.pipeline.cluster import get_queue_manager, JobInfo
from cgatcore.pipeline.executors import SGEExecutor, SlurmExecutor, TorqueExecutor, LocalExecutor
try:
    from cgatcore.pipeline.kubernetes import KubernetesExecutor
except ImportError:
    KubernetesExecutor = None  # Fallback if Kubernetes is not available


# talking to a cluster
try:
    import drmaa
    HAS_DRMAA = True
except (ImportError, RuntimeError, OSError):
    HAS_DRMAA = False
    drmaa = None

# global drmaa session - initialized lazily only when needed
GLOBAL_SESSION = None


def initialize_drmaa_session():
    """Initialize DRMAA session only when needed.
    Returns True if session initialization was successful, False otherwise.
    """
    global GLOBAL_SESSION
    if GLOBAL_SESSION is not None:
        return True  # Already initialized
        
    if not HAS_DRMAA:
        return False  # DRMAA not available
    
    try:
        get_logger().debug("Initializing DRMAA session")
        GLOBAL_SESSION = drmaa.Session()
        GLOBAL_SESSION.initialize()
        return True
    except Exception as e:
        get_logger().warning(f"Could not initialize DRMAA session: {str(e)}")
        GLOBAL_SESSION = None
        return False


# Timeouts for event loop
GEVENT_TIMEOUT_STARTUP = 5
GEVENT_TIMEOUT_WAIT = 30


# dictionary mapping job metrics to data type
DATA2TYPE = dict([
    ("task", str),
    ("statement", str),
    ("hostname", str),
    ("job_id", str),
    ("engine", str),
    ("submission_time", float),
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
    ("signal", str),
    ("socket_received", float),
    ("socket_sent", float),
    ("major_page_faults", float),
    ("unshared_data", float)])


def get_logger():
    return logging.getLogger("cgatcore.pipeline")


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


class ContainerConfig:
    """Container configuration for pipeline execution."""
    
    def __init__(self, image=None, volumes=None, env_vars=None, runtime="docker"):
        """
        Args:
            image (str): Container image (e.g., "ubuntu:20.04").
            volumes (list): Volume mappings (e.g., ['/data:/data']).
            env_vars (dict): Environment variables for the container.
            runtime (str): Container runtime ("docker" or "singularity").
        """
        self.image = image
        self.volumes = volumes or []
        self.env_vars = env_vars or {}
        self.runtime = runtime.lower()  # Normalise to lowercase

        if self.runtime not in ["docker", "singularity"]:
            raise ValueError("Unsupported container runtime: {}".format(self.runtime))

    def get_container_command(self, statement):
        """Convert a statement to run inside a container."""
        if not self.image:
            return statement

        if self.runtime == "docker":
            return self._get_docker_command(statement)
        elif self.runtime == "singularity":
            return self._get_singularity_command(statement)
        else:
            raise ValueError("Unsupported container runtime: {}".format(self.runtime))

    def _get_docker_command(self, statement):
        """Generate a Docker command."""
        volume_args = [f"-v {volume}" for volume in self.volumes]
        env_args = [f"-e {key}={value}" for key, value in self.env_vars.items()]
        
        return " ".join([
            "docker", "run", "--rm",
            *volume_args, *env_args, self.image,
            "/bin/bash", "-c", f"'{statement}'"
        ])

    def _get_singularity_command(self, statement):
        """Generate a Singularity command."""
        volume_args = [f"--bind {volume}" for volume in self.volumes]
        env_args = [f"--env {key}={value}" for key, value in self.env_vars.items()]
        
        return " ".join([
            "singularity", "exec",
            *volume_args, *env_args, self.image,
            "bash", "-c", f"'{statement}'"
        ])


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
    if "CONDA_EXE" in os.environ:
        stdout = E.run("{} env list".format(os.environ["CONDA_EXE"]),
                       return_stdout=True).strip()
    else:
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

        # IMS: conda envs can be located at abitrary locations, and not be
        # registered with conda. NB this can't tellif the directory contains a
        # valid env.
        if os.path.exists(env_name) and os.path.isdir(env_name):
            return env_name

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
    logger.info("running %s" % (statement % kwargs))

    if "cwd" not in kwargs:
        cwd = get_params()["work_dir"]
    else:
        cwd = kwargs["cwd"]

    # cleaning up of statement
    # remove new lines and superfluous spaces and tabs
    statement = " ".join(re.sub("\t+", " ", statement).split("\n")).strip()
    if statement.endswith(";"):
        statement = statement[:-1]

    # always use bash
    os.environ.update(
        {'BASH_ENV': os.path.join(os.environ['HOME'], '.bashrc')})
    process = subprocess.Popen(statement % kwargs,
                               cwd=cwd,
                               shell=True,
                               stdin=sys.stdin,
                               stdout=sys.stdout,
                               stderr=sys.stderr,
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


def get_executor(options=None):
    """
    Return an executor instance based on the specified queue manager in options.
    
    Parameters:
    - options (dict): Dictionary containing execution options, 
                      including "cluster_queue_manager".
                      
    Returns:
    - Executor instance appropriate for the specified queue manager.
    """
    if options is None:
        options = get_params()

    # Always use LocalExecutor for testing
    if options.get("testing", False):
        get_logger().debug("Using LocalExecutor because testing=True")
        return LocalExecutor(**options)

    # First check: --local flag sets engine to "local" in params
    # This must be the highest priority
    try:
        params = get_params()
        engine = params.get("engine", None)
        if engine == "local":
            get_logger().debug("Using LocalExecutor because engine=local in params (--local flag)")
            return LocalExecutor(**options)
    except Exception as e:
        get_logger().debug(f"Error checking engine parameter: {str(e)}")

    # Second check: to_cluster is explicitly set to False in options
    if not options.get("to_cluster", True):
        get_logger().debug("Using LocalExecutor because to_cluster=False in options")
        return LocalExecutor(**options)

    # Third check: without_cluster is True (another way --local can be specified)
    if options.get("without_cluster", False):
        get_logger().debug("Using LocalExecutor because without_cluster=True in options")
        return LocalExecutor(**options)
        
    # Only check for cluster executors if we've determined we should use the cluster
    queue_manager = options.get("cluster_queue_manager", None)

    # Check for KubernetesExecutor
    if queue_manager == "kubernetes" and KubernetesExecutor is not None:
        return KubernetesExecutor(**options)
    
    # Check for SGEExecutor (Sun Grid Engine)
    elif queue_manager == "sge" and shutil.which("qsub") is not None:
        return SGEExecutor(**options)
    
    # Check for SlurmExecutor
    elif queue_manager == "slurm" and shutil.which("sbatch") is not None:
        return SlurmExecutor(**options)
    
    # Check for TorqueExecutor
    elif queue_manager == "torque" and shutil.which("qsub") is not None:
        return TorqueExecutor(**options)
    
    # Fallback to LocalExecutor, not sure if this should raise an error though, feels like it should
    else:
        return LocalExecutor(**options)


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


def will_run_on_cluster(options):
    """Helper function to determine whether job will be executed on cluster

    Arguments
    ---------
    options : dict
        Dictionary of options.
    """
    
    # Check global params to see if we're using local engine
    # This ensures --local flag is properly respected
    try:
        params = get_params()
        engine = params.get("engine", None)
        if engine == "local":
            return False
    except Exception:
        # If we can't get params, proceed with other checks
        pass
    
    # First check if we're explicitly running without cluster
    if options.get("without_cluster", False):
        return False
    
    # Check if this specific task should run on cluster
    wants_cluster = options.get("to_cluster", True)
    
    # Skip DRMAA check if we don't want to use cluster
    if not wants_cluster:
        return False
    
    # Only raise error if DRMAA is required but not available
    if not HAS_DRMAA:
        # Rather than fail, just run locally when DRMAA is unavailable
        return False
        
    return wants_cluster


class Executor(object):

    def __init__(self, **kwargs):

        self.logger = get_logger()
        self.queue_manager = None
        self.run_on_cluster = will_run_on_cluster(kwargs)
        self.job_threads = kwargs.get("job_threads", 1)
        self.active_jobs = []  # List to track active jobs

        if "job_memory" in kwargs and "job_total_memory" in kwargs:
            raise ValueError(
                "both job_memory and job_total_memory have been given")

        self.job_total_memory = kwargs.get('job_total_memory', None)
        self.job_memory = kwargs.get('job_memory', None)

        if self.job_total_memory == "unlimited" or self.job_memory == "unlimited":
            self.job_total_memory = self.job_memory = "unlimited"
        else:
            if self.job_total_memory:
                self.job_memory = iotools.bytes2human(
                    iotools.human2bytes(self.job_total_memory) / self.job_threads)
            elif self.job_memory:
                self.job_total_memory = self.job_memory * self.job_threads
            else:
                params = get_params()
                if "cluster" in params:
                    self.job_memory = params["cluster"].get("memory_default", "4G")
                else:
                    self.job_memory = "4G"
                if self.job_memory == "unlimited":
                    self.job_total_memory = "unlimited"
                else:
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

        # Get work_dir with fallbacks to handle missing parameter more gracefully
        try:
            params = get_params()
            # Try to get work_dir from params
            self.work_dir = params.get("work_dir", None)
            # If not found, use current directory
            if self.work_dir is None:
                self.work_dir = os.getcwd()
                self.logger.debug(f"work_dir parameter not found, using current directory: {self.work_dir}")
            else:
                self.logger.debug(f"Using work_dir from params: {self.work_dir}")
        except Exception as e:
            self.logger.warning(f"Error getting work_dir parameter: {str(e)}")
            self.work_dir = os.getcwd()
            self.logger.debug(f"Using current directory as work_dir: {self.work_dir}")
            
        self.shellfile = kwargs.get("shell_logfile", None)
        if self.shellfile:
            if not self.shellfile.startswith(os.sep):
                self.shellfile = os.path.join(
                    self.work_dir, os.path.basename(self.shellfile))
            
            # Ensure directory exists for shell log file
            shell_dir = os.path.dirname(self.shellfile)
            if shell_dir and not os.path.exists(shell_dir):
                try:
                    os.makedirs(shell_dir, exist_ok=True)
                    self.logger.debug(f"Created directory for shell log: {shell_dir}")
                except OSError as e:
                    self.logger.warning(f"Could not create directory for shell log: {str(e)}")
                    
            # Create an empty shell log file to ensure it exists
            try:
                with open(self.shellfile, 'a'):
                    pass
                self.logger.debug(f"Initialized shell log file: {self.shellfile}")
            except OSError as e:
                self.logger.warning(f"Could not initialize shell log file: {str(e)}")

        self.monitor_interval_queued = kwargs.get('monitor_interval_queued', None)
        if self.monitor_interval_queued is None:
            params = get_params()
            if "cluster" in params:
                self.monitor_interval_queued = params["cluster"].get(
                    'monitor_interval_queued_default', GEVENT_TIMEOUT_WAIT)
            else:
                self.monitor_interval_queued = GEVENT_TIMEOUT_WAIT
                
        self.monitor_interval_running = kwargs.get('monitor_interval_running', None)
        if self.monitor_interval_running is None:
            params = get_params()
            if "cluster" in params:
                self.monitor_interval_running = params["cluster"].get(
                    'monitor_interval_running_default', GEVENT_TIMEOUT_WAIT)
            else:
                self.monitor_interval_running = GEVENT_TIMEOUT_WAIT
        # Set up signal handlers for clean-up on interruption
        self.setup_signal_handlers()

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

        setup_cmds.append("umask 002")

        for var in ["MKL_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                    "OMP_NUM_THREADS"]:
            setup_cmds.append("export {}={}".format(
                var, self.options.get("job_threads", 1)))

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
                        get_conda_environment_directory(
                            self.options["job_condaenv"]),
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
        tmpfilename = get_temp_filename(dir=self.work_dir, clear=True)
        tmpfilename = tmpfilename + ".sh"

        expanded_statement, cleanup_funcs = self.expand_statement(statement)

        with open(tmpfilename, "w") as tmpfile:
            # disabled: -l -O expand_aliases\n" )

            # make executable
            tmpfile.write("#!/bin/bash -eu\n")
            if not self.ignore_pipe_errors:
                tmpfile.write("set -o pipefail\n")

            os.chmod(tmpfilename, stat.S_IRWXG | stat.S_IRWXU)

            tmpfile.write("\ncd {}\n".format(self.work_dir))
            if self.output_directories is not None:
                for outdir in self.output_directories:
                    if outdir:
                        tmpfile.write("\nmkdir -p {}\n".format(outdir))

            # create and set system scratch dir for temporary files
            tmpfile.write("umask 002\n")
            
            try:
                # Get parameters safely
                params = get_params()
                # Check if cluster_tmpdir exists in params
                cluster_tmpdir = params.get("cluster_tmpdir", None)
                tmpdir = None
                
                if self.run_on_cluster and cluster_tmpdir:
                    # Use cluster temp directory
                    tmpdir = cluster_tmpdir
                    tmpfile.write("TMPDIR=`mktemp -d -p {}`\n".format(tmpdir))
                    tmpfile.write("export TMPDIR\n")
                else:
                    # Use local temp directory
                    tmpdir_param = params.get("tmpdir", "/tmp")
                    tmpdir = get_temp_dir(dir=tmpdir_param, clear=True)
                    tmpfile.write("mkdir -p {}\n".format(tmpdir))
                    tmpfile.write("export TMPDIR={}\n".format(tmpdir))
            except Exception as e:
                # Fallback to a local directory if there's any error
                self.logger.warning(f"Error accessing tmpdir parameters: {str(e)}, using local directory")
                tmpdir = get_temp_filename(root=".", suffix="")
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

            if self.job_memory not in ("unlimited", "etc") and \
               self.options.get("cluster_memory_ulimit", False):
                # restrict virtual memory
                # Note that there are resources in SGE which could do this directly
                # such as v_hmem.
                # Note that limiting resident set sizes (RSS) with ulimit is not
                # possible in newer kernels.
                # -v and -m accept memory in kb
                requested_memory_kb = max(
                    1000,
                    int(math.ceil(
                        iotools.human2bytes(self.job_memory) / 1024 * self.job_threads)))
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
                               statements,
                               resource_usage):
        """collect benchmark data from a job's stdout and any resource usage
        information that might be present.

        If time_data is given, read output from time command.
        """

        benchmark_data = []

        def get_val(d, v, alt):
            val = d.get(v, alt)
            if val == "unknown" or val is None:
                val = alt
            return val

        # build resource usage data structure - part native, part
        # mapped to common fields
        for jobinfo, statement in zip(resource_usage, statements):

            if resource_usage is None:
                E.warn("no resource usage for {}".format(self.task_name))
                continue

            # add some common fields
            data = {"task": self.task_name,
                    "engine": self.__class__.__name__,
                    "statement": statement,
                    "job_id": jobinfo.jobId,
                    "slots": self.job_threads}

            # native specs
            data.update(jobinfo.resourceUsage)

            # translate specs
            if self.queue_manager:
                data.update(
                    self.queue_manager.map_resource_usage(data, DATA2TYPE))

            cpu_time = float(get_val(data, "cpu_t", 0))
            start_time = float(get_val(data, "start_time", 0))
            end_time = float(get_val(data, "end_time", 0))
            data.update({
                # avoid division by 0 error
                "percent_cpu": (
                    100.0 * cpu_time / max(1.0, (end_time - start_time)) / self.job_threads),
                "total_t": end_time - start_time
            })
            benchmark_data.append(data)

        return benchmark_data

    def set_container_config(self, image, volumes=None, env_vars=None, runtime="docker"):
        """Set container configuration for all tasks executed by this executor."""

        if not image:
            raise ValueError("An image must be specified for the container configuration.")
        self.container_config = ContainerConfig(image=image, volumes=volumes, env_vars=env_vars, runtime=runtime)
        
    def start_job(self, job_info):
        """Add a job to active_jobs list when it starts."""
        self.active_jobs.append(job_info)
        self.logger.info(f"Job started: {job_info}")

    def finish_job(self, job_info):
        """Remove a job from active_jobs list when it finishes."""
        if job_info in self.active_jobs:
            self.active_jobs.remove(job_info)
            self.logger.info(f"Job completed: {job_info}")

    def cleanup_all_jobs(self):
        """Clean up all remaining active jobs on interruption."""
        self.logger.info("Cleaning up all job outputs due to pipeline interruption")
        for job_info in self.active_jobs:
            should_cleanup = self._should_cleanup_job(job_info)
            if should_cleanup:
                self.cleanup_failed_job(job_info)
            else:
                job_name = job_info.get('job_name', 'unknown')
                self.logger.info(f"Skipping cleanup of job '{job_name}' - appears to have completed successfully")
        
        self.active_jobs.clear()  # Clear the list after cleanup

    def _should_cleanup_job(self, job_info):
        """Determine if a job should be cleaned up based on multiple criteria.
        
        Returns True if job should be cleaned up (failed/incomplete), False if it should be preserved.
        """
        import time
        
        # Get output files for this job
        if "outfile" in job_info:
            outfiles = [job_info["outfile"]]
        elif "outfiles" in job_info:
            outfiles = job_info["outfiles"]
        else:
            # No output files specified, safe to cleanup
            return True
        
        # Check if job has an explicit completion status
        job_status = job_info.get('status', None)
        if job_status == 'completed':
            return False  # Don't cleanup completed jobs
        elif job_status in ['failed', 'cancelled', 'timeout']:
            return True   # Cleanup explicitly failed jobs
        
        # For jobs without explicit status, use file-based heuristics
        job_start_time = job_info.get('start_time', None)
        current_time = time.time()
        job_command = job_info.get('statement', '').lower()
        
        for outfile in outfiles:
            if not os.path.exists(outfile):
                # Output file doesn't exist, job likely didn't complete
                return True
            
            file_stat = os.stat(outfile)
            file_mtime = file_stat.st_mtime
            file_size = file_stat.st_size
            
            # Check if file was created/modified recently (within job timeframe)
            if job_start_time and file_mtime < job_start_time:
                # File is older than job start, likely from previous run
                return True
            
            # For very recent files (less than 30 seconds old), be more permissive
            # This handles cases where cleanup happens immediately after job completion
            if (current_time - file_mtime) < 30:
                # Recent file, likely just created - don't cleanup
                continue
            
            # Handle empty files with enhanced logic
            if file_size == 0:
                # Check if this is intentionally empty based on multiple factors
                if self._is_intentionally_empty_file(outfile, job_command):
                    # File is supposed to be empty, don't cleanup
                    continue
                else:
                    # Empty file not expected, likely failed job
                    return True
        
        # If we get here, all output files exist and pass validation
        return False

    def _is_intentionally_empty_file(self, filepath, job_command):
        """Determine if an empty file is intentionally empty based on context."""
        
        # Get file extension and basename
        _, ext = os.path.splitext(filepath.lower())
        basename = os.path.basename(filepath.lower())
        
        # 1. Known empty-file extensions
        empty_ok_extensions = [
            '.log', '.txt', '.marker', '.done', '.flag', '.sentinel', 
            '.checkpoint', '.lock', '.tmp', '.temp'
        ]
        if ext in empty_ok_extensions:
            return True
        
        # 2. Common marker/dummy file patterns
        marker_patterns = [
            'done', 'complete', 'finished', 'marker', 'flag', 'sentinel',
            'checkpoint', 'touch', 'dummy', 'placeholder'
        ]
        if any(pattern in basename for pattern in marker_patterns):
            return True
        
        # 3. Command-based detection
        if job_command:
            # Check if command uses touch, which creates empty files intentionally
            if 'touch' in job_command and filepath.split('/')[-1] in job_command:
                return True
            
            # Check for other commands that might create empty files
            empty_file_commands = ['touch', 'truncate', '> ', 'echo "" >', 'cat /dev/null >']
            if any(cmd in job_command for cmd in empty_file_commands):
                return True
            
            # Check for redirection that might create empty files
            if f'> {filepath}' in job_command or f'>{filepath}' in job_command:
                return True
        
        # 4. Directory-based hints (some directories commonly contain empty marker files)
        parent_dir = os.path.dirname(filepath).lower()
        if any(marker in parent_dir for marker in ['checkpoint', 'marker', 'flag', 'done']):
            return True
        
        # If none of the above, assume empty file indicates failure
        return False

    def setup_signal_handlers(self):
        """Set up signal handlers to clean up jobs on SIGINT and SIGTERM."""

        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}. Starting clean-up.")
            self.cleanup_all_jobs()
            exit(1)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def cleanup_failed_job(self, job_info):
        """Clean up files generated by a failed job."""
        if "outfile" in job_info:
            outfiles = [job_info["outfile"]]
        elif "outfiles" in job_info:
            outfiles = job_info["outfiles"]
        else:
            self.logger.warning(f"No output files found for job {job_info.get('job_name', 'unknown')}")
            return

        for outfile in outfiles:
            if os.path.exists(outfile):
                try:
                    os.remove(outfile)
                    self.logger.info(f"Removed failed job output file: {outfile}")
                except OSError as e:
                    self.logger.error(f"Error removing file {outfile}: {str(e)}")
            else:
                self.logger.info(f"Output file not found (already removed or not created): {outfile}")

    def run(
            self,
            statement_list,
            job_memory=None,
            job_threads=None,
            container_runtime=None,
            image=None,
            volumes=None,
            env_vars=None,
            **kwargs,):
        
        """
        Execute a list of statements with optional container support.

            Args:
                statement_list (list): List of commands to execute.
                job_memory (str): Memory requirements (e.g., "4G").
                job_threads (int): Number of threads to use.
                container_runtime (str): Container runtime ("docker" or "singularity").
                image (str): Container image to use.
                volumes (list): Volume mappings (e.g., ['/data:/data']).
                env_vars (dict): Environment variables for the container.
                **kwargs: Additional arguments.
        """
        # Validation checks
        if container_runtime and container_runtime not in ["docker", "singularity"]:
            self.logger.error(f"Invalid container_runtime: {container_runtime}")
            raise ValueError("Container runtime must be 'docker' or 'singularity'")

        if container_runtime and not image:
            self.logger.error(f"Container runtime specified without an image: {container_runtime}")
            raise ValueError("An image must be specified when using a container runtime")

        benchmark_data = []

        for statement in statement_list:
            job_info = {"statement": statement}
            self.start_job(job_info)

            try:
                # Prepare containerized execution
                if container_runtime:
                    self.set_container_config(image=image, volumes=volumes, env_vars=env_vars, runtime=container_runtime)
                    statement = self.container_config.get_container_command(statement)

                # Add memory and thread environment variables
                if job_memory:
                    env_vars = env_vars or {}
                    env_vars["JOB_MEMORY"] = job_memory
                if job_threads:
                    env_vars = env_vars or {}
                    env_vars["JOB_THREADS"] = job_threads

                # Debugging: Log the constructed command
                self.logger.info(f"Executing command: {statement}")

                # Build and execute the statement
                full_statement, job_path = self.build_job_script(statement)
                process = subprocess.Popen(
                    full_statement, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                stdout, stderr = process.communicate()

                if process.returncode != 0:
                    raise OSError(
                        f"Job failed with return code {process.returncode}.\n"
                        f"stderr: {stderr.decode('utf-8')}\ncommand: {statement}"
                    )

                # Collect benchmark data for successful jobs
                benchmark_data.append(
                    self.collect_benchmark_data(
                        statement, resource_usage={"job_id": process.pid}
                    )
                )
                self.finish_job(job_info)

            except Exception as e:
                self.logger.error(f"Job failed: {e}")
                if self._should_cleanup_job(job_info):
                    self.cleanup_failed_job(job_info)
                else:
                    self.logger.info(f"Preserving output files for job {job_info.get('job_name', 'unknown')} despite exception")
                if not self.ignore_errors:
                    raise

        return benchmark_data


class GridExecutor(Executor):

    def __init__(self, **kwargs):
        Executor.__init__(self, **kwargs)
        
        # Check if we're explicitly set to run locally
        try:
            params = get_params()
            engine = params.get("engine", None)
            if engine == "local":
                self.logger.warning("Local engine specified with GridExecutor. This is an error in executor selection.")
                raise ValueError("GridExecutor used with local engine setting")
        except Exception as e:
            # Log but continue since this isn't the main DRMAA check
            self.logger.debug(f"Error checking engine parameter: {str(e)}")
        
        # Initialize DRMAA session - required for cluster execution
        if not initialize_drmaa_session():
            # This is a hard error - we need DRMAA for cluster execution
            error_msg = "Could not initialize DRMAA session. To run locally, use the --local flag."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Get the global session now that it's initialized
        self.session = GLOBAL_SESSION

        # if running on cluster, use a working directory on shared drive
        self.work_dir_is_local = iotools.is_local(self.work_dir)

        # connect to global session
        pid = os.getpid()
        self.logger.info('task: pid={}, grid-session={}, work_dir={}'.format(
            pid, str(self.session), self.work_dir))

        # Set up queue manager
        self.queue_manager = get_queue_manager(
            kwargs.get("cluster_queue_manager"),
            self.session,
            ignore_errors=self.ignore_errors)

    def __enter__(self):
        # for cluster execution, the working directory can not be
        # local.  Use a temporary shared location instead and copy
        # files over after job has completed. Note that this assumes
        # that all paths of input files are absolute and reside in a
        # shared directory.
        if self.work_dir_is_local:
            self.original_dir = self.work_dir
            self.work_dir = get_temp_dir(shared=True)

        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if self.work_dir_is_local:
            destdir = self.original_dir
            self.logger.info("moving files from {} to {}".format(
                self.work_dir, destdir))

            for root, dirs, files in os.walk(self.work_dir):
                for d in dirs:
                    if not os.path.exists(os.path.join(destdir, d)):
                        os.makedirs(d)
                for fn in files:
                    shutil.move(os.path.join(root, fn),
                                os.path.join(self.work_dir, root, fn))

            shutil.rmtree(self.work_dir)

    def run(self, statement_list):
        # At this point, self.session should be a valid DRMAA session
        # The __init__ method should have already checked for local execution mode
        # and raised an error if DRMAA was not available
        
        # Make sure we have a valid session
        if self.session is None:
            error_msg = "No DRMAA session available. Cannot execute on cluster. Use --local flag to run locally."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Regular cluster execution path
        try:
            # submit statements to cluster individually.
            benchmark_data = []
            # Handle missing 'cluster' key
            cluster_options = self.options.get("cluster", {})
            jt = self.setup_job(cluster_options)
            
            job_ids, filenames = [], []
            for statement in statement_list:
                self.logger.info("running statement:\n%s" % statement)

                full_statement, job_path = self.build_job_script(statement)

                stdout_path, stderr_path = self.queue_manager.set_drmaa_job_paths(
                    jt, job_path)

                job_id = self.session.runJob(jt)
                job_ids.append(job_id)
                filenames.append((job_path, stdout_path, stderr_path))
                self.logger.info(
                    "job has been submitted with job_id %s" % str(job_id))
                # give back control for bulk submission
                gevent.sleep(GEVENT_TIMEOUT_STARTUP)

            self.wait_for_job_completion(job_ids)

            # collect and clean up
            for job_id, statement, paths in zip(job_ids,
                                                statement_list,
                                                filenames):
                job_path, stdout_path, stderr_path = paths
                stdout, stderr, resource_usage = self.queue_manager.collect_single_job_from_cluster(
                    job_id,
                    statement,
                    stdout_path,
                    stderr_path,
                    job_path)

                benchmark_data.extend(
                    self.collect_benchmark_data([statement],
                                                resource_usage))
            self.session.deleteJobTemplate(jt)
            return benchmark_data
            
        except Exception as e:
            # Provide a clear error message
            error_msg = f"Cluster execution failed: {str(e)}. To run locally, use the --local flag."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def setup_job(self, options):
        # Ensure options is a dictionary even if None was passed
        if options is None:
            options = {}
            
        # Log what we're doing to help debug cluster issues
        self.logger.debug(f"Setting up job with name={self.job_name}, memory={self.job_memory}, threads={self.job_threads}")
        
        try:
            jt = self.queue_manager.setup_drmaa_job_template(
                self.session,
                job_name=self.job_name,
                job_memory=self.job_memory,
                job_threads=self.job_threads,
                working_directory=self.work_dir,
                **options)
            self.logger.info("job-options: %s" % jt.nativeSpecification)
            return jt
        except Exception as e:
            # If we encounter any issues, log them but don't crash
            self.logger.warning(f"Error setting up job template: {str(e)}, using default settings")
            # Create minimal job template with required fields only
            jt = self.queue_manager.setup_drmaa_job_template(
                self.session,
                job_name=self.job_name,
                working_directory=self.work_dir)
            return jt

    def wait_for_job_completion(self, job_ids):
        # Set safe defaults in case intervals weren't properly initialized
        monitor_interval_running = getattr(self, 'monitor_interval_running', GEVENT_TIMEOUT_WAIT)
        monitor_interval_queued = getattr(self, 'monitor_interval_queued', GEVENT_TIMEOUT_WAIT)

        self.logger.info("waiting for %i jobs to finish " % len(job_ids))
        running_job_ids = set(job_ids)
        while running_job_ids:
            for job_id in list(running_job_ids):
                try:
                    status = self.session.jobStatus(job_id)
                    if status in (drmaa.JobState.DONE, drmaa.JobState.FAILED):
                        running_job_ids.remove(job_id)
                    if status in (drmaa.JobState.RUNNING):
                        # The drmaa documentation is unclear as to what gets returned
                        # for a job array when tasks can be in different states
                        gevent.sleep(monitor_interval_running)
                    else:
                        gevent.sleep(monitor_interval_queued)
                        break
                except Exception as e:
                    self.logger.warning(f"Error checking job status for {job_id}: {str(e)}")
                    running_job_ids.remove(job_id)  # Remove to avoid infinite loop
                    break


class GridArrayExecutor(GridExecutor):

    def run(self, statement_list):

        benchmark_data = []
        # run statements through array interface
        jobsfile = get_temp_filename(dir=self.work_dir,
                                     clear=True) + ".jobs"

        with open(jobsfile, "w") as outf:
            outf.write("\n".join(statement_list))

        master_statement = (
            "CMD=$(awk \"NR==$SGE_TASK_ID\" {jobsfile}); "
            "eval $CMD".format(**locals()))

        full_statement, job_path = self.build_job_script(master_statement)

        # Handle missing 'cluster' key
        cluster_options = self.options.get("cluster", {})
        jt = self.setup_job(cluster_options)
        stdout_path, stderr_path = self.queue_manager.set_drmaa_job_paths(
            jt, job_path)

        job_id, stdout, stderr, resource_usage = self.run_array_job(
            self.session, jt, stdout_path, stderr_path,
            full_statement, start=0, end=len(statement_list),
            increment=1)

        benchmark_data.extend(
            self.collect_benchmark_data(statement_list,
                                        resource_usage=resource_usage))
        try:
            os.unlink(jobsfile)
        except OSError:
            pass

        return benchmark_data

    def run_array_job(self, session, jt, stdout_path, stderr_path,
                      statement, start, end, increment):

        logger = get_logger()
        logger.info("starting an array job: %i-%i,%i" %
                    (start, end, increment))

        jt.outputPath = ":" + stdout_path
        jt.errorPath = ":" + stderr_path

        logger.info("job submitted with %s" % jt.nativeSpecification)

        # sge works with 1-based, closed intervals
        job_ids = session.runBulkJobs(jt, start + 1, end, increment)
        logger.info("%i array jobs have been submitted as job_id %s" %
                    (len(job_ids), job_ids[0]))

        self.wait_for_job_completion(job_ids)

        logger.info("%i array jobs for job_id %s have completed" %
                    (len(job_ids), job_ids[0]))

        resource_usage = []
        for job in job_ids:
            r = session.wait(job, drmaa.Session.TIMEOUT_WAIT_FOREVER)
            r.resourceUsage["hostname"] = "unknown"
            resource_usage.append(r)

        stdout, stderr = self.queue_manager.get_drmaa_job_stdout_stderr(
            stdout_path, stderr_path)
        job_id = job_ids[0]
        return job_id, stdout, stderr, resource_usage


class LocalExecutor(Executor):

    def collect_metric_data(self, process, start_time, end_time, time_data_file):
        data = {"start_time": start_time,
                "end_time": end_time,
                "submission_time": start_time,
                "hostname": socket.gethostname(),
                "total_t": end_time - start_time}

        if time_data_file is not None and os.path.exists(time_data_file):
            with open(time_data_file) as inf:
                pairs = [x[:-1].split("\t") for x in inf if x]

            def _convert(key, v):
                return iotools.str2val(v)

            # remove any non-key-value pairs
            data.update(dict([(x[0], _convert(x[0], x[1]))
                              for x in pairs if len(x) == 2]))

            cpu_value = data.get("percent_cpu", "0")
            # Strip potential '%' symbols, handle non-numeric cases gracefully
            try:
                percent_cpu = int(re.sub("%", "", cpu_value)) if cpu_value.replace("%", "").strip().isdigit() else 0
            except ValueError:
                percent_cpu = 0  # Default or fallback value, adjust as necessary

            data.update(
                {"percent_cpu": percent_cpu,
                 "cpu_t": float(data.get("user_t", 0)) + float(data.get("sys_t", 0))})

        return JobInfo(jobId=process.pid, resourceUsage=data)

    def run(self, statement_list):

        benchmark_data = []
        for statement in statement_list:
            self.logger.info("running statement:\n%s" % statement)

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

                os.environ.update(
                    {'BASH_ENV': os.path.join(os.environ['HOME'], '.bashrc')})
                process = subprocess.Popen(
                    full_statement,
                    cwd=self.work_dir,
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
                    self.logger.warn(
                        "repeating execution: message={}".format(stderr))
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

            resource_usage = self.collect_metric_data(process,
                                                      start_time,
                                                      end_time,
                                                      time_data_file=job_path + ".times")

            benchmark_data.extend(
                self.collect_benchmark_data(
                    [statement],
                    resource_usage=[resource_usage]))

            try:
                os.unlink(job_path)
                os.unlink(job_path + ".times")
            except OSError:
                pass

        return benchmark_data


class LocalArrayExecutor(LocalExecutor):
    pass


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

    The job will be executed within PARAMS["work_dir"], unless
    PARAMS["work_dir"] is not local. In that case, the job will
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

    # Combine options using priority
    options = dict(list(get_params().items()))
    caller_options = get_caller_locals()
    options.update(list(caller_options.items()))

    if "self" in options:
        del options["self"]
    options.update(list(kwargs.items()))

    # Inject params named tuple from TaskLibrary functions into option
    # dict. This allows overriding options set in the code with options set
    # in a .yml file
    if "params" in options:
        try:
            options.update(options["params"]._asdict())
        except AttributeError:
            pass

    # Insert parameters supplied through simplified interface such
    # as job_memory, job_options, job_queue
    # Handle case where 'cluster' key might not exist in options
    if 'cluster' in options:
        options['cluster']['options'] = options.get(
            'job_options', options['cluster']['options'])
        options['cluster']['queue'] = options.get(
            'job_queue', options['cluster']['queue'])
    options['without_cluster'] = options.get('without_cluster')

    # SGE compatible job_name
    name_substrate = str(options.get("outfile", "cgatcore"))
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

    # Build statements using parameter interpolation
    if isinstance(statement, list):
        statement_list = [interpolate_statement(stmt, options) for stmt in statement]
    else:
        statement_list = [interpolate_statement(statement, options)]

    if len(statement_list) == 0:
        logger.warn("No statements found - no execution")
        return []

    if options.get("dryrun", False):
        for statement in statement_list:
            logger.info("Dry-run: {}".format(statement))
        return []

    # CRITICAL CHECK: If engine is set to local, ensure we don't try to use cluster
    # This is a double-safety check since get_executor should already handle this
    try:
        params = get_params()
        if params.get("engine", None) == "local":
            logger.debug("Using LocalExecutor (from run) because engine=local in params")
            executor = LocalExecutor(**options)
        elif options.get("without_cluster", False):
            logger.debug("Using LocalExecutor (from run) because without_cluster=True")
            executor = LocalExecutor(**options)
        else:
            # Only call get_executor if we're not explicitly set to run locally
            logger.debug("Selecting executor via get_executor")
            executor = get_executor(options)
    except Exception as e:
        logger.warning(f"Error checking execution mode, defaulting to get_executor: {str(e)}")
        executor = get_executor(options)
    
    # Execute statement list within the context of the executor
    with executor as e:
        benchmark_data = e.run(statement_list)

    # Log benchmark data
    for data in benchmark_data:
        logger.info(json.dumps(data))

    BenchmarkData = collections.namedtuple('BenchmarkData', sorted(benchmark_data[0]))
    return [BenchmarkData(**d) for d in benchmark_data]


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
        "python -m cgatcore.pipeline.run_function "
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
    via the pipeline.submit framework. Arguments to the function are
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
            submit(iotools.snip(__file__),
                   "run_pickled",
                   args=[iotools.snip(module_file), function_name, args_file],
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
        raise AttributeError(msg.message
                             + "unknown function, available functions are: %s" %
                             ",".join([x for x in dir(module)
                                       if not x.startswith("_")]))

    args, kwargs = pickle.load(open(args_file, "rb"))
    logger.info("arguments = %s" % str(args))
    logger.info("keyword arguments = %s" % str(kwargs))

    function(*args, **kwargs)

    os.unlink(args_file)
