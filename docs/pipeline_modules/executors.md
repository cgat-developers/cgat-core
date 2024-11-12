# Executors for job scheduling

## Overview

This documentation describes several executor classes for job scheduling in computational pipelines. Each of these classes inherits from the `BaseExecutor` and is responsible for submitting jobs to a different type of cluster system or local machine. The following executors are available:

- `SGEExecutor`: Submits jobs to an SGE (Sun Grid Engine) cluster.
- `SlurmExecutor`: Submits jobs to a Slurm cluster.
- `TorqueExecutor`: Submits jobs to a Torque cluster.
- `LocalExecutor`: Executes jobs locally.
- `KubernetesExecutor`: Submits jobs to a Kubernetes cluster.

Each executor has specific methods and logging functionality that enable it to handle job submission, monitoring, and error management effectively.

## `SGEExecutor`

The `SGEExecutor` is responsible for running jobs on an SGE cluster. It extends the `BaseExecutor` class.

### Methods

#### `__init__(self, **kwargs)`
Initialises the `SGEExecutor` and sets up a logger for the instance.

#### `run(self, statement_list)`
Runs the provided list of statements using SGE.

- **Arguments**:
  - `statement_list`: A list of shell command statements to be executed.

- **Workflow**:
  - Builds an SGE job submission command for each statement.
  - Uses `subprocess.run()` to submit jobs using the `qsub` command.
  - Handles job submission errors, logs relevant information, and monitors job completion.

#### `build_job_script(self, statement)`
Builds a job script for SGE based on the provided statement.

- **Overrides**: This method is an override of the `BaseExecutor.build_job_script()`.

## `SlurmExecutor`

The `SlurmExecutor` is responsible for running jobs on a Slurm cluster. It also extends the `BaseExecutor` class.

### Methods

#### `__init__(self, **kwargs)`
Initialises the `SlurmExecutor` and sets up a logger for the instance.

#### `run(self, statement_list)`
Runs the provided list of statements using Slurm.

- **Arguments**:
  - `statement_list`: A list of shell command statements to be executed.

- **Workflow**:
  - Builds a Slurm job submission command for each statement using `sbatch`.
  - Uses `subprocess.run()` to submit jobs to the Slurm scheduler.
  - Monitors the job submission status, logs relevant information, and handles any errors.

#### `build_job_script(self, statement)`
Builds a job script for submission on Slurm.

- **Overrides**: This method is an override of the `BaseExecutor.build_job_script()`.

## `TorqueExecutor`

The `TorqueExecutor` class runs jobs on a Torque cluster, using `qsub` for job submissions.

### Methods

#### `__init__(self, **kwargs)`
Initialises the `TorqueExecutor` and sets up a logger for the instance.

#### `run(self, statement_list)`
Runs the provided list of statements using Torque.

- **Arguments**:
  - `statement_list`: A list of shell command statements to be executed.

- **Workflow**:
  - Builds a job script and submits it using the `qsub` command.
  - Uses `subprocess.run()` to handle the submission and logs all related information.
  - Handles job submission errors and monitors job completion.

#### `build_job_script(self, statement)`
Builds a job script for submission on a Torque cluster.

- **Overrides**: This method is an override of the `BaseExecutor.build_job_script()`.

## `LocalExecutor`

The `LocalExecutor` runs jobs on the local machine without the need for cluster scheduling. This is useful for development, testing, or when the jobs are small enough to run locally.

### Methods

#### `__init__(self, **kwargs)`
Initialises the `LocalExecutor` and sets up a logger for the instance.

#### `run(self, statement_list)`
Runs the provided list of statements locally.

- **Arguments**:
  - `statement_list`: A list of shell command statements to be executed.

- **Workflow**:
  - Builds the job script and runs it locally using `subprocess.Popen()`.
  - Monitors the output and logs the job status.
  - Handles any runtime errors by logging them and raising exceptions as needed.

#### `build_job_script(self, statement)`
Builds a job script for local execution.

- **Overrides**: This method is an override of the `BaseExecutor.build_job_script()`.

## `KubernetesExecutor`

The `KubernetesExecutor` is used for running jobs on a Kubernetes cluster.

### Methods

#### `__init__(self, **kwargs)`
Initialises the `KubernetesExecutor`.

- **Workflow**:
  - Loads the Kubernetes configuration and sets up both Core and Batch API clients for job management.
  - Logs information about successful or failed configuration loads.

#### `run(self, statement, job_path, job_condaenv)`
Runs a job using Kubernetes.

- **Arguments**:
  - `statement`: The shell command to be executed within a Kubernetes job.
  - `job_path`: Path for the job files.
  - `job_condaenv`: Conda environment to be used within the job container.

- **Workflow**:
  - Defines the Kubernetes job specification, including container image, command, and job parameters.
  - Submits the job using `create_namespaced_job()` and waits for its completion.
  - Collects job logs and benchmark data for analysis.
  - Cleans up the Kubernetes job once it is complete.

#### `_wait_for_job_completion(self, job_name)`
Waits for the Kubernetes job to complete.

- **Arguments**:
  - `job_name`: The name of the job.

- **Workflow**:
  - Repeatedly queries the job status using `read_namespaced_job_status()` until it succeeds or fails.

#### `_get_pod_logs(self, job_name)`
Retrieves the logs of the pod associated with the specified Kubernetes job.

- **Arguments**:
  - `job_name`: The name of the job.

#### `_cleanup_job(self, job_name)`
Deletes the Kubernetes job and its associated pods.

- **Arguments**:
  - `job_name`: The name of the job to be deleted.

#### `collect_benchmark_data(self, job_name, resource_usage_file)`
Collects benchmark data such as CPU and memory usage from the job's pod(s).

- **Arguments**:
  - `job_name`: Name of the job for which benchmark data is being collected.
  - `resource_usage_file`: Path to a file where resource usage data will be saved.

#### `collect_metric_data(self, process, start_time, end_time, time_data_file)`
Collects and saves metric data related to job duration.

- **Arguments**:
  - `process`: The name of the process.
  - `start_time`: Timestamp when the job started.
  - `end_time`: Timestamp when the job ended.
  - `time_data_file`: Path to a file where timing data will be saved.

## Logging and Error Handling

All executor classes use the Python `logging` module to log different stages of job submission, execution, and monitoring. Logging levels like `INFO`, `ERROR`, and `WARNING` are used to provide information on job progress and errors. Executors also make use of exception handling to raise `RuntimeError` when job submission or execution fails.

## Notes
- The job script generation is handled by the `build_job_script()` function, which is customised per executor but is based on the implementation from `BaseExecutor`.
- Job monitoring and benchmark data collection are placeholder implementations in some of the executors. Users should consider implementing job-specific monitoring and resource management tailored to their requirements.

## Summary

The executor classes provide a modular way to submit jobs to different cluster systems or run them locally. Each executor manages the nuances of the corresponding cluster scheduler, allowing seamless integration with cgatcore pipelines. They provide functionalities such as job submission, logging, monitoring, and benchmarking, ensuring a streamlined and customisable workflow for distributed computing environments.

