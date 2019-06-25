.. _defining_workflow-run_parameters:

==============
Run parameters
==============

Our workflows are executed using defaults that specify parameters for
setting requirements for memory, threads, environment, e.c.t. Each of these
parameters can be modified within the pipeline.

Modifiable run parameters
-------------------------

- job_memory: Number of slots (threads/cores/CPU) to use for the task.
              Default: "4G"
- job_total_memory: Total memory to use for a job.
- to_cluster: Send the job to the cluster.
              Default: True
- without_cluster: When this is set to True the job is ran locally.
              Default: False
- cluster_memory_ulimit: Restrict virtual memory.
              Default: False
- job_condaenv: Name of the conda environment to use for each job.
              Default: will use the one specified in bashrc
job_array: If set True, run statement as an array job. Job_array should be
              tuple with start, end, and increment.
	      Default: False
	      
Specifying parameters to job
----------------------------

Parameters can be set within a pipeline task as follows::

  @transform( '*.unsorted', suffix('.unsorted'), '.sorted')
  def sortFile( infile, outfile ):

    statement = '''sort -t %(tmpdir)s %(infile)s > %(outfile)s'''

    without_cluster = False
    job_total_memory = 50G

    P.run(statement,
          job_condaenv="sort_environment",
	  job_memory=30G,
	  job_threads=2)
