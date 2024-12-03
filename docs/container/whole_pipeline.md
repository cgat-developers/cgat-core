# Container Configuration for Entire Pipeline

This document describes how to use the `Pipeline` class from `cgatcore.pipeline` to configure container settings **for the entire pipeline**. Unlike configuring individual jobs with container support, this method allows you to set up a consistent execution environment for all tasks across the entire workflow. This is useful for ensuring reproducibility and simplifying pipeline management.

## Overview

The `Pipeline` class from `cgatcore.pipeline` allows you to:
- Configure container support for tasks.
- Set up Docker or Singularity containers with environment variables and volume mappings.
- Seamlessly execute multiple tasks inside containers.
- Configure container settings for the entire pipeline, ensuring consistent execution environments across all tasks.

By configuring the container support at the pipeline level, all commands that are run through `P.run()` will automatically use the specified container settings.

---

## Usage Examples

### Setting Docker as the Default Runtime for the Entire Pipeline

Below is an example of how to use the `Pipeline` class to configure and execute all tasks in the pipeline within a Docker container:

```python
from cgatcore.pipeline import Pipeline

# Create a pipeline instance
P = Pipeline()

# Configure container support for Docker for the entire pipeline
P.set_container_config(
    image="ubuntu:20.04",
    volumes=["/data:/data", "/reference:/reference"],
    env_vars={"THREADS": "4", "PATH": "/usr/local/bin:$PATH"},
    runtime="docker"
)

# Define and run tasks - these will all run in the specified Docker container
P.run([
    "bwa mem /reference/genome.fa /data/sample1.fastq > /data/sample1.bam",
    "bwa mem /reference/genome.fa /data/sample2.fastq > /data/sample2.bam"
])
```

### Setting Singularity as the Default Runtime for the Entire Pipeline

Similarly, the following example shows how to use Singularity for all tasks in the pipeline:

```python
from cgatcore.pipeline import Pipeline

# Create a pipeline instance
P = Pipeline()

# Configure container support for Singularity for the entire pipeline
P.set_container_config(
    image="/path/to/ubuntu.sif",
    volumes=["/data:/data", "/reference:/reference"],
    env_vars={"THREADS": "4", "PATH": "/usr/local/bin:$PATH"},
    runtime="singularity"
)

# Define and run tasks - these will all run in the specified Singularity container
P.run([
    "bwa mem /reference/genome.fa /data/sample1.fastq > /data/sample1.bam",
    "bwa mem /reference/genome.fa /data/sample2.fastq > /data/sample2.bam"
])
```

## When to Use This Approach

This configuration approach is ideal when:
- You want **all tasks in the pipeline** to run in the same controlled container environment without having to configure container support repeatedly for each individual command.
- Consistency and reproducibility are essential, as this ensures that all tasks use the same software versions, dependencies, and environment.
- You are managing complex workflows where each step depends on a well-defined environment, avoiding any variations that may arise if each step had to be configured separately.

## Differences from Per-Command Containerisation

- **Pipeline-Level Configuration**: Use `P.set_container_config()` to set the container settings for the entire pipeline. Every task executed through `P.run()` will use this configuration by default.
- **Per-Command Containerisation**: Use container-specific arguments in `P.run()` for each task individually, which allows different tasks to use different container settings if needed. This is covered in the separate documentation titled **Containerised Execution in `P.run()`**.

---

## Conclusion

The `Pipeline` class provides an efficient way to standardise the execution environment across all pipeline tasks. By setting container configurations at the pipeline level:
- **All tasks** will use the same Docker or Singularity environment.
- **Configuration is centralised**, reducing redundancy and the risk of errors.
- **Portability** and **reproducibility** are enhanced, making this approach particularly useful for workflows requiring a consistent environment across multiple stages.

With these examples, users can set up a fully containerised workflow environment for all stages of their pipeline, ensuring robust and repeatable results.

