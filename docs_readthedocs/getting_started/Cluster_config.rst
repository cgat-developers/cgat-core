.. _getting_started-Config:


=====================
Cluster configuration
=====================

Currently SGE, SLURM, Torque and PBSPro workload managers are supported. The default cluster options for
cgatcore are set for SunGrid Engine (SGE). Therefore, if you would like to run an alternative workload manager
then you will need to configure your settings for your cluster. In order to do this you will need to
create a :file:`.cgat.yml` within the user`s home directory. 

This will allow you to overide the default configurations. To view the hardcoded parameters for cgatcore
please see the `parameters.py <https://github.com/cgat-developers/cgat-core/blob/eb6d29e5fe1439de2318aeb5cdfa730f36ec3af4/cgatcore/pipeline/parameters.py#L67>`_ 
file. 

For an example of how to configure a PBSpro workload manager see this link to this `config example <https://github.com/AntonioJBT/pipeline_example/blob/master/Docker_and_config_file_examples/cgat.yml>`_.

The .cgat.yml is placed in your home directory and when a pipeline is executed it will automatically prioritise the 
:file:`.cgat.yml` parameters over the cgatcore hard coded parameters. For example, adding the following to the
.cgat.yml file will implement cluster settings for PBSpro::


	memory_resource: mem
    
    options: -l walltime=00:10:00 -l select=1:ncpus=8:mem=1gb
    
    queue_manager: pbspro
    
    queue: NONE
    
    parallel_environment:  "dedicated"
    

 

