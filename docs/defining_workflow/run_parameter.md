# Setting run parameters

Our workflows are executed using default settings that specify parameters for requirements such as memory, threads, environment, etc. Each of these parameters can be modified within the pipeline as needed.

## Modifiable run parameters

- **`job_memory`**: Number of slots (threads/cores/CPU) to use for the task. Default: "4G".
- **`job_total_memory`**: Total memory to use for a job.
- **`to_cluster`**: Send the job to the cluster. Default: `True`.
- **`without_cluster`**: Run the job locally when set to `True`. Default: `False`.
- **`cluster_memory_ulimit`**: Restrict virtual memory. Default: `False`.
- **`job_condaenv`**: Name of the conda environment to use for each job. Default: will use the one specified in `bashrc`.
- **`job_array`**: If set to `True`, run the statement as an array job. `job_array` should be a tuple with start, end, and increment values. Default: `False`.

## Specifying parameters to a job

Parameters can be set within a pipeline task as follows:

```python
@transform('*.unsorted', suffix('.unsorted'), '.sorted')
def sortFile(infile, outfile):
    statement = '''sort -t %(tmpdir)s %(infile)s > %(outfile)s'''
    P.run(statement,
          job_condaenv="sort_environment",
          job_memory="30G",
          job_threads=2,
          without_cluster=False,
          job_total_memory="50G")
```

In this example, the `sortFile` function sorts an unsorted file and saves it as a new sorted file. The `P.run()` statement is used to specify various parameters:

- `job_condaenv="sort_environment"`: This specifies that the task should use the `sort_environment` conda environment.
- `job_memory="30G"`: This sets the memory requirement for the task to 30GB.
- `job_threads=2`: The task will use 2 threads.
- `without_cluster=False`: This ensures the job is sent to the cluster.
- `job_total_memory="50G"`: The total memory allocated for the job is 50GB.

These parameters allow fine-tuning of job execution to fit specific computational requirements, such as allocating more memory or running on a local machine rather than a cluster.