# Pipeline Control Module

The Control module in CGAT-core is responsible for managing the overall execution flow of the pipeline. It provides functions and classes for running the pipeline, handling command-line arguments, and controlling the pipeline's behaviour.

## Key Functions

### `run_pipeline()`

This function is the main entry point for executing a pipeline. It sets up the pipeline, processes command-line arguments, and runs the specified tasks.

```python
from cgatcore import pipeline as P

def my_pipeline():
    # Define your pipeline tasks here
    pass

if __name__ == "__main__":
    P.run_pipeline(pipeline_func=my_pipeline)
```

### `get_parameters()`

Retrieves pipeline parameters from configuration files and command-line arguments.

```python
PARAMS = P.get_parameters("pipeline.yml")
```

## `Pipeline` Class

The `Pipeline` class is the core class for managing pipeline execution. It provides methods for adding tasks, running the pipeline, and handling dependencies.

```python
pipeline = P.Pipeline()
pipeline.add_task(my_task)
pipeline.run()
```

## Command-line Interface

The Control module provides a command-line interface for running pipelines. Common options include:

- `--pipeline-action`: Specify the action to perform (e.g., `show`, `plot`, `run`)
- `--local`: Run the pipeline locally instead of on a cluster
- `--multiprocess`: Specify the number of processes to use for local execution

### Example usage:

```sh
python my_pipeline.py --pipeline-action run --local
```

For more detailed information on pipeline control and execution, refer to the Pipeline Execution documentation.

## Next Steps

These new pages provide more comprehensive documentation for the CGAT-core pipeline modules and S3 integration. You should create similar pages for the other modules (Database, Files, Cluster, Execution, Utils, Parameters) and S3-related topics (S3 Decorators, Configuring S3).

Remember to include code examples, explanations of key concepts, and links to other relevant parts of the documentation. As you continue to develop and expand the CGAT-core functionality, make sure to update the documentation accordingly.
