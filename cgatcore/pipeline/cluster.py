'''cluster.py - cluster utility functions for ruffus pipelines
==============================================================

This module abstracts the DRMAA native specification and provides
convenience functions for running Drmaa jobs.


Reference
---------

'''

import re
import math
import os
import stat
import time
import logging


def get_logger():
    return logging.getLogger("cgatcore.pipeline")


def setup_drmaa_job_template(drmaa_session,
                             queue_manager,
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

    # if process has multiple threads, use a parallel environment
    multithread = job_threads > 1

    jt = drmaa_session.createJobTemplate()
    jt.workingDirectory = working_directory
    jt.jobEnvironment = {'BASH_ENV': '~/.bashrc'}
    jt.args = []
    if not re.match("[a-zA-Z]", job_name[0]):
        job_name = "_" + job_name

    # queue manager specific configuration options
    if queue_manager.lower() == "sge":

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

        if multithread:
            if not kwargs["parallel_environment"]:
                raise ValueError("parallel environment not specified")
            spec.append("-pe {} {} -R y".format(
                kwargs.get("parallel_environment", "smp"), job_threads))

        if "pe_queue" in kwargs and multithread:
            spec.append("-q {}".format(kwargs["pe_queue"]))
        elif kwargs['queue'] != "NONE":
            spec.append("-q {}".format(kwargs["queue"]))

    elif queue_manager.lower() == "slurm":

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

    elif queue_manager.lower() == "torque":

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

        if multithread:
            # don't know if this is standard resource names or if
            # it varies site to site. For now, I will assume "nodes" is
            # standard and name of resource for multiple CPUs is stored in
            # "parrellel environment"
            pe = kwargs.get("parallel_environment", "ppn")
            resource_requests.append("nodes=1")
            resource_requests.append("{}={}".format(pe, job_threads))

        if resource_requests:
            spec.append("-l " + ":".join(resource_requests))
            
        if "pe_queue" in kwargs and multithread:
            spec.append("-q {}".format(kwargs["pe_queue"]))
        elif kwargs['queue'] != "NONE":
            spec.append("-q {}".format(kwargs["queue"]))
                                    
        spec.append(kwargs.get("options", ""))

        # There is no equivalent to sge -V option for pbs-drmaa
        # recreating this...
        jt.jobEnvironment = os.environ
        jt.jobEnvironment.update({'BASH_ENV': os.path.join(os.path.expanduser("~"),
                                                           '.bashrc')})

    elif queue_manager.lower() == "pbspro":

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
        spec = ["-N {}".format(job_name[0:15])]

        if "mem" not in kwargs["options"]:
            spec.append("-l mem={}".format(job_memory))

        # Leaving walltime to be specified by user as difficult to set
        # dynamically and depends on site/admin configuration of
        # default values. Likely means setting for longest job with
        # trade-off of longer waiting times for resources to be
        # available for other jobs.
        spec.append(kwargs["options"], "")

        if multithread:
            # TO DO 'select=1' determines de number of nodes. Should
            # go in a config file.  mem is per node and maximum memory
            # Site dependent but in general setting '#PBS -l
            # select=NN:ncpus=NN:mem=NN{gb|mb}' is sufficient for
            # parallel jobs (OpenMP, MPI).  Also architecture
            # dependent, jobs could be hanging if resource doesn't
            # exist.  TO DO: Kill if long waiting time?
            spec = ["-N {}".format(job_name[0:15]),
                    "-l select=1:ncpus=%s:mem=%s".format(job_threads, job_memory)]

        if "pe_queue" in kwargs and multithread:
            spec.append("-q {}".format(kwargs["pe_queue"]))
        elif kwargs['queue'] != "NONE":
            spec.append("-q {}".format(kwargs["queue"]))

        # As for torque, there is no equivalent to sge -V option for pbs-drmaa:
        jt.jobEnvironment = os.environ
        jt.jobEnvironment.update(
            {'BASH_ENV': os.path.join(os.path.expanduser("~"), '.bashrc')})

    else:
        raise ValueError("Queue manager %s not supported" % queue_manager)

    jt.nativeSpecification = " ".join(spec)

    # keep stdout and stderr separate
    jt.joinFiles = False

    return jt


def set_drmaa_job_paths(job_template, job_path):
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


def get_drmaa_job_stdout_stderr(stdout_path, stderr_path, tries=5, encoding="utf-8"):
    '''get stdout/stderr allowing for some lag.

    Try at most *tries* times. If unsuccessfull, throw OSError

    Removes the files once they are read.

    Returns tuple of stdout and stderr as unicode strings.
    '''
    x = tries
    while x >= 0:
        if os.path.exists(stdout_path):
            break
        time.sleep(1)
        x -= 1

    x = tries
    while x >= 0:
        if os.path.exists(stderr_path):
            break
        time.sleep(1)
        x -= 1

    try:
        with open(stdout_path, "r", encoding=encoding) as inf:
            stdout = inf.readlines()
    except IOError as msg:
        get_logger().warn("could not open stdout: %s" % msg)
        stdout = []

    try:
        with open(stderr_path, "r", encoding=encoding) as inf:
            stderr = inf.readlines()
    except IOError as msg:
        get_logger().warn("could not open stdout: %s" % msg)
        stderr = []

    try:
        os.unlink(stdout_path)
        os.unlink(stderr_path)
    except OSError as msg:
        pass

    return stdout, stderr
