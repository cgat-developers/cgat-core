# Containerised Execution in `P.run()`


The `P.run()` method supports executing jobs within container environments using **Docker** or **Singularity**. This functionality enables seamless integration of containerisation for computational workflows.

## Features

- **Container Runtime Support**: Execute jobs using either Docker or Singularity.
- **Environment Variables**: Pass custom environment variables to the container.
- **Volume Mapping**: Bind directories between the host system and the container.
- **Container-Specific Command Construction**: Automatically builds the appropriate command for Docker or Singularity.

---

## API Documentation

### `P.run()`

The `P.run()` method executes a list of commands with optional support for containerisation via Docker or Singularity.

### Parameters

| Parameter          | Type      | Description                                                                                                                                                                   | Default          |
|---------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `statement_list`    | `list`    | List of commands (statements) to execute.                                                                                                                                     | Required         |
| `job_memory`        | `str`     | Memory requirements for the job (e.g., `"4G"`).                                                                                                                              | `None`           |
| `job_threads`       | `int`     | Number of threads to use.                                                                                                                                                     | `None`           |
| `container_runtime` | `str`     | Container runtime to use. Must be `"docker"` or `"singularity"`.                                                                                                              | `None`           |
| `image`             | `str`     | The container image to use (e.g., `"ubuntu:20.04"` for Docker or `/path/to/image.sif` for Singularity).                                                                      | `None`           |
| `volumes`           | `list`    | List of volume mappings (e.g., `"/host/path:/container/path"`).                                                                                                             | `None`           |
| `env_vars`          | `dict`    | Dictionary of environment variables to pass to the container (e.g., `{"VAR": "value"}`).                                                                                      | `None`           |
| `**kwargs`          | `dict`    | Additional arguments passed to the executor.                                                                                                                                 | `None`           |

### Returns

- **`list`**: A list of benchmark data collected from executed jobs.

### Raises

- **`ValueError`**: If invalid arguments are provided (e.g., container runtime is missing or invalid, or required arguments for container execution are not supplied).
- **`OSError`**: If the job fails during execution.

---

## Examples

### Running a Job with Docker

To execute a job using Docker, specify the `container_runtime` as `"docker"` and provide an image. Optionally, bind host directories to container directories using `volumes`, and pass environment variables with `env_vars`.

```python
P.run(
    statement_list=["echo 'Hello from Docker'"],
    container_runtime="docker",
    image="ubuntu:20.04",
    volumes=["/data:/data"],
    env_vars={"MY_VAR": "value"}
)
```

This will construct and execute the following Docker command:

```bash
docker run --rm -v /data:/data -e MY_VAR=value ubuntu:20.04 /bin/bash -c 'echo Hello from Docker'
```

### Running a Job with Singularity

To execute a job using Singularity, specify the `container_runtime` as `"singularity"` and provide a Singularity Image File (SIF). Similarly, you can bind host directories and set environment variables.

```python
P.run(
    statement_list=["echo 'Hello from Singularity'"],
    container_runtime="singularity",
    image="/path/to/image.sif",
    volumes=["/data:/data"],
    env_vars={"MY_VAR": "value"}
)
```

This will construct and execute the following Singularity command:

```bash
singularity exec --bind /data:/data --env MY_VAR=value /path/to/image.sif /bin/bash -c 'echo Hello from Singularity'
```

---

## Usage Notes

1. **Container Runtime Selection**:
   - Use `"docker"` for Docker-based container execution.
   - Use `"singularity"` for Singularity-based container execution.
   - Ensure the appropriate runtime is installed and available on the system.

2. **Environment Variables**:
   - Use the `env_vars` argument to pass environment variables to the container.

3. **Volume Mapping**:
   - Use the `volumes` argument to bind directories between the host system and the container.
     - Docker: Use `["/host/path:/container/path"]`.
     - Singularity: Use `["/host/path:/container/path"]`.

4. **Validation**:
   - If `container_runtime` is not specified, container-specific arguments such as `volumes`, `env_vars`, and `image` cannot be used.
   - A valid container image must be provided if `container_runtime` is specified.

---

## Error Handling

- **Invalid Configurations**:
  - Raises `ValueError` for invalid configurations, such as:
    - Missing container runtime.
    - Missing or invalid container image.
    - Incompatible arguments (e.g., volumes provided without a container runtime).

- **Job Failures**:
  - Automatically cleans up failed jobs, including temporary files and job outputs.

---

## Implementation Details

Internally, `P.run()` constructs the appropriate command based on the specified runtime and arguments:

### Docker

For Docker, the command is constructed as follows:
```bash
docker run --rm -v /host/path:/container/path -e VAR=value image /bin/bash -c 'statement'
```

### Singularity

For Singularity, the command is constructed as follows:
```bash
singularity exec --bind /host/path:/container/path --env VAR=value image /bin/bash -c 'statement'
```

Both commands ensure proper execution and clean-up after the job completes.

---

## Contributing

To add or enhance containerisation functionality, ensure:
1. New features or fixes support both Docker and Singularity.
2. Unit tests cover all edge cases for container runtime usage.
3. Updates to this documentation reflect changes in functionality.

---

## Adding to MkDocs

Save this content in a markdown file (e.g., `docs/container_execution.md`) and add it to the `mkdocs.yml` navigation:

```yaml
nav:
  - Home: index.md
  - P.run:
      - Docker and Singularity: container_execution.md
```

This provides a clear, accessible reference for users leveraging containerisation with `P.run()`.
