'''cluster.py - cluster utility functions for ruffus pipelines
==============================================================

This module abstracts the DRMAA native specification and provides
convenience functions for running Drmaa jobs.

Currently SGE, SLURM, Torque and PBSPro are supported.

Reference
---------

'''

import re
import math
import collections
import os
import stat
import time
import datetime
import logging
import gevent
import cgatcore.experiment as E

try:
    import drmaa
except (ImportError, RuntimeError):
    pass


def get_logger():
    return logging.getLogger("cgatcore.pipeline")


JobInfo = collections.namedtuple("JobInfo", ("jobId", "resourceUsage"))


# Timeouts for event loop
GEVENT_TIMEOUT_SACCT = 5
GEVENT_TIMEOUT_WAIT = 1


class DRMAACluster(object):

    # dictionary mapping resource usage fields returned by DRMAA
    # to a common set of names.
    map_drmaa2benchmark_data = {}

    def __init__(self, session, ignore_errors=False):
        self.session = session
        self.ignore_errors = ignore_errors

    def get_resource_usage(self, job_id, retval, hostname):
        retval.resourceUsage["hostname"] = hostname
        return [retval]

    def setup_drmaa_job_template(self,
                                 drmaa_session,
                                 job_name,
                                 job_memory,
                                 job_threads,
                                 working_directory,
                                 **kwargs):
        '''Sets up a Drmma job template. Currently SGE, SLURM, Torque and PBSPro are
        supported'''
        if not job_memory:
            raise ValueError("Job memory must be specified when running"
                             "DRMAA jobs")

        jt = drmaa_session.createJobTemplate()
        jt.workingDirectory = working_directory
        jt.jobEnvironment = {'BASH_ENV': '~/.bashrc'}
        jt.args = []
        if not re.match("[a-zA-Z]", job_name[0]):
            job_name = "_" + job_name

        spec = self.get_native_specification(job_name,
                                             job_memory,
                                             job_threads,
                                             **kwargs)

        jt.nativeSpecification = " ".join(spec)

        # keep stdout and stderr separate
        jt.joinFiles = False

        self.update_template(jt)
        return jt

    def update_template(self, jt):
        pass

    def collect_single_job_from_cluster(self,
                                        job_id,
                                        statement,
                                        stdout_path, stderr_path,
                                        job_path):
        '''collects a single job on the cluster.

        This method waits until a job has completed and returns
        stdout, stderr and resource usage.
        '''
        try:
            retval = self.session.wait(job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        except Exception as msg:
            # ignore message 24, indicates jobs that have been qdel'ed
            if not str(msg).startswith("code 24"):
                raise
            retval = None

        stdout, stderr = self.get_drmaa_job_stdout_stderr(stdout_path, stderr_path)
        if retval is not None:
            if retval.exitStatus == 0:
                if retval.wasAborted is True:
                    get_logger().warning(
                        "Job {} marked as hasAborted=True but completed successfully, hasExited={} "
                        "(Job may have been cancelled by the user or the scheduler due to memory constraints)"
                        "The stderr was \n{}\nstatement = {}".format(
                            job_id, retval.hasExited, "".join(stderr), statement))
                
            else:
                msg = ("Job {} has non-zero exitStatus {}: hasExited={},  wasAborted={}"
                       "hasSignal={}, terminatedSignal='{}' "
                       "\nstatement = {}".format(
                           job_id, retval.exitStatus, retval.hasExited, retval.wasAborted,
                           retval.hasSignal, retval.terminatedSignal,
                           statement))
                if stderr:
                    msg += "\n stderr = {}".format("".join(stderr))
                if self.ignore_errors:
                    get_logger().warning(msg)
                else:
                    raise OSError(msg)

        # get hostname from job script
        hostname = stdout[-3][:-1]
        try:
            resource_usage = self.get_resource_usage(job_id, retval, hostname)
        except (ValueError, KeyError, TypeError, IndexError) as ex:
            E.warn("could not collect resource usage for job {}: {}".format(job_id, ex))
            resource_usage = [None]

        try:
            os.unlink(job_path)
        except OSError:
            self.logger.warn(
                ("temporary job file %s not present for "
                 "clean-up - ignored") % job_path)

        return stdout, stderr, resource_usage

    def get_drmaa_job_stdout_stderr(self, stdout_path, stderr_path,
                                    tries=5, encoding="utf-8"):
        '''get stdout/stderr allowing for some lag.

        Try at most *tries* times. If unsuccessfull, throw OSError

        Removes the files once they are read.

        Returns tuple of stdout and stderr as unicode strings.
        '''
        x = tries
        while x >= 0:
            if os.path.exists(stdout_path):
                break
            gevent.sleep(GEVENT_TIMEOUT_WAIT)
            x -= 1

        x = tries
        while x >= 0:
            if os.path.exists(stderr_path):
                break
            gevent.sleep(GEVENT_TIMEOUT_WAIT)
            x -= 1

        try:
            with open(stdout_path, "r", encoding=encoding) as inf:
                stdout = inf.readlines()
        except IOError as msg:
            get_logger().warning("could not open stdout: %s" % msg)
            stdout = []

        try:
            with open(stderr_path, "r", encoding=encoding) as inf:
                stderr = inf.readlines()
        except IOError as msg:
            get_logger().warning("could not open stdout: %s" % msg)
            stderr = []

        try:
            os.unlink(stdout_path)
            os.unlink(stderr_path)
        except OSError as msg:
            pass

        return stdout, stderr

    def set_drmaa_job_paths(self, job_template, job_path):
        '''Adds the job_path, stdout_path and stderr_paths
           to the job_template.
        '''
        job_path = os.path.abspath(job_path)
        os.chmod(job_path, stat.S_IRWXG | stat.S_IRWXU)

        stdout_path = job_path + ".stdout"
        stderr_path = job_path + ".stderr"

        job_template.remoteCommand = job_path
        job_template.outputPath = ":" + stdout_path
        job_template.errorPath = ":" + stderr_path

        return stdout_path, stderr_path

    def map_resource_usage(self, resource_usage, data2type):
        """return job metrics mapped to common name and converted to right type."""
        def _convert(key, v, tpe):
            if v is None:
                return None
            else:
                try:
                    return tpe(v)
                except ValueError as ex:
                    E.warning("could not convert {} with value '{}' to {}: {}".format(
                        key, v, tpe, ex))
                    return v

        return dict([(key,
                      _convert(key, resource_usage.get(self.map_drmaa2benchmark_data.get(key, key), None), tpe))
                     for key, tpe in data2type.items()])


class SGECluster(DRMAACluster):

    map_drmaa2benchmark_data = {
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

    def get_native_specification(self,
                                 job_name,
                                 job_memory,
                                 job_threads,
                                 **kwargs):
        # see: ? cannot find documentation on the SGE native spec
        spec = ["-V",
                "-notify",  # required for signal handling
                "-N %s" % job_name]

        spec.append("-p {}".format(kwargs.get("priority", 0)))

        spec.append(kwargs.get("options", ""))

        if not kwargs["memory_resource"]:
            raise ValueError("cluster memory resource not specified")

        if job_memory != "unlimited":
            for resource in kwargs.get("memory_resource", "").split(","):
                spec.append("-l {}={}".format(resource, job_memory))

        if job_threads > 1:
            if not kwargs["parallel_environment"]:
                raise ValueError("parallel environment not specified")
            spec.append("-pe {} {} -R y".format(
                kwargs.get("parallel_environment", "smp"), job_threads))

        if "pe_queue" in kwargs and job_threads > 1:
            spec.append("-q {}".format(kwargs["pe_queue"]))
        elif kwargs['queue'] != "NONE":
            spec.append("-q {}".format(kwargs["queue"]))

        return spec


class SlurmCluster(DRMAACluster):

    map_drmaa2benchmark_data = {
        "hostname": "NodeList",
        "job_id": "JobID",
        "submission_time": "Submit",
        "start_time": "Start",
        "end_time": "End",
        "slots": "NCPUS",
        "exit_status": "ExitCode",
        "wall_t": "ElapsedRaw",
        "cpu_t": "CPUTimeRaw",
        "user_t": "UserCPU",
        "sys_t": "SystemCPU",
        "io_input": "MaxDiskRead",
        "io_output": "MaxDiskWrite",
        "average_memory_total": "AveVMSize",
        "average_rss": "AveRSS",
        "max_rss": "MaxRSS",
        "max_vmem": "MaxVMSize",
        "minor_page_faults": "AvePages",
        "average_uss": "AveRSS",
        "signal": "DerivedExitCode",
        "major_page_faults": "MaxPages"}

    def get_native_specification(self,
                                 job_name,
                                 job_memory,
                                 job_threads,
                                 **kwargs):
        # SLURM DOCS:
        # http://apps.man.poznan.pl/trac/slurm-drmaa
        # https://computing.llnl.gov/linux/slurm/cons_res_share.html
        #
        # The SLURM Consumable Resource plugin is required
        # The "CR_CPU_Memory" resource must be specified
        #
        # i.e. in slurm.conf:
        # SelectType=select/cons_res
        # SelectTypeParameters=CR_CPU_Memory
        #
        # * Note that --cpus-per-task will actually refer to cores
        #   with the appropriate Node configuration
        #
        # SLURM-DRMAA DOCS - Note that version 1.2 (SVN) is required
        # http://apps.man.poznan.pl/trac/slurm-drmaa
        #
        # Not implemented:
        # -V: SLURM automatically passess the environment variables
        # -p: does not appear to be part of the slurm drmaa native spec
        #
        # TODO: add "--account" (not sure the best way to fill param).

        spec = ["-J %s" % job_name]

        spec.append(kwargs.get("options", ""))
        spec.append("--cpus-per-task={}".format(job_threads))

        # Note the that the specified memory must be per CPU
        # for consistency with the implemented SGE approach

        if job_memory != "unlimited":
            if job_memory.endswith("G"):
                job_memory_per_cpu = int(math.ceil(float(job_memory[:-1]) * 1000))
            elif job_memory.endswith("M"):
                job_memory_per_cpu = int(math.ceil(float(job_memory[:-1])))
            else:
                raise ValueError('job memory unit not recognised for SLURM, '
                                 'must be either "M" (for Mb) or "G" (for Gb),'
                                 ' e.g. 1G or 1000M for 1 Gigabyte of memory')

            spec.append("--mem-per-cpu={}".format(job_memory_per_cpu))

        # set the partition to use (equivalent of SGE queue)
        spec.append("--partition={}".format(kwargs["queue"]))

        return spec

    @classmethod
    def parse_accounting_data(cls, data, retval):

        def convert_value(*pair):
            k, v = pair[0]
            if k in ("UserCPU", "SystemCPU"):
                n = 0
                for x, f in zip(re.split("[-:]", v)[::-1], (1, 60, 3600, 86400)):
                    n += float(x) * f
                v = n
            elif k in ("Start", "End", "Submit"):
                v = time.mktime(datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S").timetuple())
            elif k in ("ExitCode", ):
                v = int(v.split(":")[0])
            elif v.endswith("K"):
                v = float(v[:-1]) * 1000
            elif v.endswith("M"):
                v = float(v[:-1]) * 1000000
            elif v.endswith("G"):
                v = float(v[:-1]) * 1000000000
            try:
                v = int(v)
            except ValueError:
                pass
            return k, v

        d = dict(map(convert_value, zip(cls.map_drmaa2benchmark_data.values(), data.split("|"))))
        retval = retval._replace(resourceUsage=d)
        return [retval]

    def get_resource_usage(self, job_id, retval, hostname):
        # delay to help with sync'ing of book-keeping
        gevent.sleep(GEVENT_TIMEOUT_SACCT)
        statement = "sacct --noheader --units=K --parsable2 --format={} -j {} ".format(
            ",".join(self.map_drmaa2benchmark_data.values()), job_id)

        stdout = E.run(statement, return_stdout=True).splitlines()
        if len(stdout) != 2:
            E.warn("expected 2 lines in {}, but got {}".format(statement, len(stdout)))

        return self.parse_accounting_data(stdout[-1], retval)


class TorqueCluster(DRMAACluster):

    def get_native_specification(self,
                                 job_name,
                                 job_memory,
                                 job_threads,
                                 **kwargs):
        # PBS Torque native specifictation:
        # http://apps.man.poznan.pl/trac/pbs-drmaa

        spec = ["-N {}".format(job_name), ]

        # again, I don't know if mem is same across all sites, or just a
        # common default, so allow to be set via memory_resource, with "mem"
        # default (for backwards compatibility).

        resource_requests = list()

        if job_memory != "unlimited":
            for resource in kwargs.get("memory_resource", "mem").split(","):
                resource_requests.append("{}={}".format(resource, job_memory))

        if job_threads > 1:
            # don't know if this is standard resource names or if
            # it varies site to site. For now, I will assume "nodes" is
            # standard and name of resource for multiple CPUs is stored in
            # "parrellel environment"
            pe = kwargs.get("parallel_environment", "ppn")
            resource_requests.append("nodes=1")
            resource_requests.append("{}={}".format(pe, job_threads))

        if resource_requests:
            spec.append("-l " + ":".join(resource_requests))

        if "pe_queue" in kwargs and job_threads > 1:
            spec.append("-q {}".format(kwargs["pe_queue"]))
        elif kwargs['queue'] != "NONE":
            spec.append("-q {}".format(kwargs["queue"]))

        spec.append(kwargs.get("options", ""))

        return spec

    def update_template(self, jt):
        # There is no equivalent to sge -V option for pbs-drmaa
        # recreating this...
        jt.jobEnvironment = os.environ
        jt.jobEnvironment.update({'BASH_ENV': os.path.join(os.path.expanduser("~"),
                                                           '.bashrc')})


class PBSProCluster(DRMAACluster):

    def get_native_specification(self,
                                 job_name,
                                 job_memory,
                                 job_threads,
                                 **kwargs):

        # PBS Pro docs
        # http://www.pbsworks.com/PBSProduct.aspx?n=PBS-Professional&c=Overview-and-Capabilities
        # http://technion.ac.il/usg/tamnun/PBSProUserGuide12.1.pdf

        # DRMAA for PBS Pro is the same as for torque:
        # http://apps.man.poznan.pl/trac/pbs-drmaa
        # Webpages with some examples:
        # https://wiki.galaxyproject.org/Admin/Config/Performance/cluster#PBS
        # https://sites.google.com/a/case.edu/hpc-upgraded-cluster/home/Software-Guide/pbs-drmaa
        # https://albertsk.files.wordpress.com/2011/12/pbs.pdf

        # PBS Pro has some differences with torque so separating

        # Set environment variables in .bashrc:
        # PBS_DRMAA_CONF to eg ~/.pbs_drmaa.conf
        # DRMAA_LIBRARY_PATH to eg /xxx/libdrmaa.so

        # PBSPro only takes the first 15 characters, throws
        # uninformative error if longer.  mem is maximum amount of RAM
        # used by job; mem_free doesn't seem to be available.
        spec = ["-N {}".format(job_name[0:15]), ]

        if "mem" not in kwargs["options"]:
            spec.append("-l mem={}".format(job_memory))

        # Leaving walltime to be specified by user as difficult to set
        # dynamically and depends on site/admin configuration of
        # default values. Likely means setting for longest job with
        # trade-off of longer waiting times for resources to be
        # available for other jobs.

        if job_threads > 1:
            # TO DO 'select=1' determines de number of nodes. Should
            # go in a config file.  mem is per node and maximum memory
            # Site dependent but in general setting '#PBS -l
            # select=NN:ncpus=NN:mem=NN{gb|mb}' is sufficient for
            # parallel jobs (OpenMP, MPI).  Also architecture
            # dependent, jobs could be hanging if resource doesn't
            # exist.
            # mem outside of -l select errors (ie passing -l mem=4G errors)
            # TO DO: Kill if long waiting time?
            spec.append("-l select=1:ncpus=%s:mem=%s".format(job_threads,
                                                             job_memory
                                                             )
                        )

        if "pe_queue" in kwargs and job_threads > 1:
            spec.append("-q {}".format(kwargs["pe_queue"]))
        elif kwargs['queue'] != "NONE":
            spec.append("-q {}".format(kwargs["queue"]))

        spec.append(kwargs.get("options", ""))
        return spec

    def update_template(self, jt):
        # The directive #PBS -V exists and works in a qsub script but errors here
        # so using the following as for torque:
        jt.jobEnvironment = os.environ
        jt.jobEnvironment.update(
            {'BASH_ENV': os.path.join(os.path.expanduser("~"), '.bashrc')})


def get_queue_manager(queue_manager, *args, **kwargs):

    qm = queue_manager.lower()

    if qm == "sge":
        return SGECluster(*args, **kwargs)
    elif qm == "slurm":
        return SlurmCluster(*args, **kwargs)
    elif qm == "torque":
        return TorqueCluster(*args, **kwargs)
    elif qm == "pbspro":
        return PBSProCluster(*args, **kwargs)
    else:
        raise ValueError("Queue manager {} not supported".format(queue_manager))


MAP_QACCT2BENCHMARK_DATA = {
    "task": "task",
    "statement": "statement",
    "hostname": "hostname",
    "started": "start_time",
    "completed": "end_time",
    "total_t": "total_t",
    "wall_t": "cpu",
    "user_t": "ru_utime",
    "sys_t": "ru_stime",
    # "child_user_t",
    # "child_sys_t",
    "shared_data": "ru_ixrss",
    "io_input": "ru_inblock",
    "io_output": "ru_oublock",
    # SGE this is integral memory usage
    "average_memory_total": "mem",
    "percent_cpu": "cpu",
    # SGE this is integral memory usage
    "average_rss": "mem",
    "max_rss": "ru_maxrss",
    "minor_page_faults": "ru_minflt",
    "swapped": "ru_nswap",
    "context_switches_involuntarily": "ru_nvcsw",
    "context_switches_voluntarily": "ru_nivcsw",
    "average_uss": "ru_isrss"
}
