# Cluster configuration

Currently, cgatcore supports the following workload managers: SGE, SLURM, Torque, and PBSPro. The default cluster options are set for SunGrid Engine (SGE). If you are using a different workload manager, you need to configure your cluster settings accordingly by creating a `.cgat.yml` file in your home directory.

This configuration file allows you to override the default settings. To view the hardcoded parameters for cgatcore, refer to the [parameters.py file](https://github.com/cgat-developers/cgat-core/blob/eb6d29e5fe1439de2318aeb5cdfa730f36ec3af4/cgatcore/pipeline/parameters.py#L67).

For an example of configuring a PBSPro workload manager, see the provided [config example](https://github.com/AntonioJBT/pipeline_example/blob/master/Docker_and_config_file_examples/cgat.yml).

The `.cgat.yml` file in your home directory will take precedence over the default cgatcore settings. For instance, adding the following configuration to `.cgat.yml` will implement cluster settings for PBSPro:

```yaml
memory_resource: mem

options: -l walltime=00:10:00 -l select=1:ncpus=8:mem=1gb

queue_manager: pbspro

queue: NONE

parallel_environment: "dedicated"
```

This setup specifies memory resource allocation (`mem`), runtime limits (`walltime`), selection of CPU and memory resources, and the use of the PBSPro queue manager, among other settings. Make sure to adjust the parameters according to your cluster environment to optimise the workload manager for your pipeline runs.