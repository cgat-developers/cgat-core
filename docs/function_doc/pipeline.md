# CGATcore Pipeline Module

The `pipeline` module is the core component of CGAT-core, providing essential functionality for building and executing computational pipelines.

## Core Functions

### Pipeline Decorators

```python
@transform(input_files, suffix(".input"), ".output")
def task_function(infile, outfile):
    """Transform a single input file to an output file."""
    pass

@merge(input_files, "output.txt")
def merge_task(infiles, outfile):
    """Merge multiple input files into a single output."""
    pass

@split(input_file, "*.split")
def split_task(infile, outfiles):
    """Split a single input file into multiple outputs."""
    pass

@follows(previous_task)
def dependent_task():
    """Execute after previous_task completes."""
    pass
```

### S3-Aware Decorators

```python
@s3_transform("s3://bucket/input.txt", suffix(".txt"), ".processed")
def process_s3_file(infile, outfile):
    """Process files directly from S3."""
    pass

@s3_merge(["s3://bucket/*.txt"], "s3://bucket/merged.txt")
def merge_s3_files(infiles, outfile):
    """Merge multiple S3 files."""
    pass
```

## Configuration Functions

### Pipeline Setup
```python
# Initialize pipeline
pipeline.initialize(options)

# Get pipeline parameters
params = pipeline.get_params()

# Configure cluster execution
pipeline.setup_cluster()
```

### Resource Management
```python
# Set memory requirements
pipeline.set_job_memory("4G")

# Set CPU requirements
pipeline.set_job_threads(4)

# Configure temporary directory
pipeline.set_tmpdir("/path/to/tmp")
```

## Execution Functions

### Running Tasks
```python
# Execute a command
pipeline.run("samtools sort input.bam")

# Submit a Python function
pipeline.submit(
    module="my_module",
    function="process_data",
    infiles="input.txt",
    outfiles="output.txt"
)
```

### Job Control
```python
# Check job status
pipeline.is_running(job_id)

# Wait for job completion
pipeline.wait_for_jobs()

# Clean up temporary files
pipeline.cleanup()
```

## Error Handling

```python
try:
    pipeline.run("risky_command")
except pipeline.PipelineError as e:
    pipeline.handle_error(e)
```

## Best Practices

1. **Resource Management**
   - Always specify memory and CPU requirements
   - Use appropriate cluster queue settings
   - Clean up temporary files

2. **Error Handling**
   - Implement proper error checking
   - Use pipeline.log for logging
   - Handle temporary file cleanup

3. **Performance**
   - Use appropriate chunk sizes for parallel processing
   - Monitor resource usage
   - Optimize cluster settings

For more details, see the [Pipeline Overview](../pipeline_modules/overview.md) and [Writing Workflows](../defining_workflow/writing_workflows.md) guides.

::: cgatcore.pipeline
    :members:
    :show-inheritance:
