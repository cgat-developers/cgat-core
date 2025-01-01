# Cluster configuration

Currently, cgatcore supports the following workload managers: SGE, SLURM and Torque. The default cluster options are set for SunGrid Engine (SGE). If you are using a different workload manager, you need to configure your cluster settings accordingly by creating a `.cgat.yml` file in your home directory.

This configuration file allows you to override the default settings. To view the hardcoded parameters for cgatcore, refer to the [parameters.py file](https://github.com/cgat-developers/cgat-core/blob/eb6d29e5fe1439de2318aeb5cdfa730f36ec3af4/cgatcore/pipeline/parameters.py#L67).

For an example of configuring a PBSPro workload manager, see the provided [config example](https://github.com/AntonioJBT/pipeline_example/blob/master/Docker_and_config_file_examples/cgat.yml).

The `.cgat.yml` file in your home directory will take precedence over the default cgatcore settings. For instance, adding the following configuration to `.cgat.yml` will implement cluster settings for SLURM:

```yaml
memory_resource: mem

options: --time=00:10:00 --cpus-per-task=8 --mem=1G

queue_manager: slurm

queue: NONE

parallel_environment: "dedicated"
```

This setup specifies memory resource allocation (`mem`), runtime limits (`walltime`), selection of CPU and memory resources, and the use of the PBSPro queue manager, among other settings. Make sure to adjust the parameters according to your cluster environment to optimise the workload manager for your pipeline runs.

## Default Parameters

The following are some of the default parameters in `cgatcore` that can be overridden in your `.cgat.yml` file:

- **memory_resource**: Defines the memory resource name (e.g., `mem` for PBSPro).
- **options**: Specifies additional options for job submission (e.g., `-l walltime=00:10:00`).
- **queue_manager**: The queue manager to be used (e.g., `pbspro`, `slurm`).
- **queue**: The default queue for job submission.
- **parallel_environment**: Specifies the parallel environment settings.

## Additional Parameters

The following additional parameters can also be configured in your `.cgat.yml` file:

- **cluster_queue**: Specifies the cluster queue to use (default: `all.q`).
- **cluster_priority**: Sets the priority of jobs in the cluster queue (default: `-10`).
- **cluster_num_jobs**: Limits the number of jobs to submit to the cluster queue (default: `100`).
- **cluster_memory_resource**: Name of the consumable resource to request memory (default: `mem_free`).
- **cluster_memory_default**: Default amount of memory allocated for each job (default: `4G`).
- **cluster_memory_ulimit**: Ensures requested memory is not exceeded via ulimit (default: `False`).
- **cluster_options**: General cluster options for job submission.
- **cluster_parallel_environment**: Parallel environment for multi-threaded jobs (default: `dedicated`).
- **cluster_queue_manager**: Specifies the cluster queue manager (default: `sge`).
- **cluster_tmpdir**: Directory specification for temporary files on cluster nodes. If set to `False`, the general `tmpdir` parameter is used.

These parameters allow you to customize the cluster environment to better suit your pipeline's needs.

## Example Configurations

### SLURM Configuration

```yaml
memory_resource: mem

options: --time=00:10:00 --cpus-per-task=8 --mem=1G

queue_manager: slurm

queue: NONE

parallel_environment: "dedicated"
```

### Torque Configuration

```yaml
memory_resource: mem

options: -l walltime=00:10:00 -l nodes=1:ppn=8

queue_manager: torque

queue: NONE

parallel_environment: "dedicated"
```

These configurations specify memory allocation, runtime limits, and other settings specific to each workload manager. Adjust these parameters to suit your cluster environment.