# CGATcore Kubernetes Module

## KubernetesExecutor.run

Submits a job to the Kubernetes cluster to run the specified command.

This method creates a Kubernetes Job object and submits it to the cluster. The job runs the specified command in a container, using the provided Conda environment.

### Args:
- **statement (str)**: The command to execute in the job.
- **job_path (str)**: The path to the job script.
- **job_condaenv (str)**: The name of the Conda environment to use.

### Example Usage:
```python
executor = KubernetesExecutor(namespace='default')
logs = executor.run(statement='echo Hello World', job_path='path/to/job/script', job_condaenv='my_conda_env')
print(logs)
```

### Returns:
- Logs from the job execution.
