# Running a pipeline - Tutorial

Before beginning this tutorial, ensure that `cgat-core` is installed correctly. Refer to the [installation instructions](#installation) for guidance.

As a tutorial example of how to run a CGAT workflow, we will use the `cgat-showcase` pipeline. You will also need to install `cgat-showcase` (see the [instructions](https://cgat-showcase.readthedocs.io/en/latest/getting_started/Tutorial.html)).

The aim of this pipeline is to perform pseudoalignment using `kallisto`. The pipeline can be run locally or distributed across a cluster. This tutorial will explain the steps required to run the pipeline. Further documentation on `cgat-showcase` can be found [here](https://cgat-showcase.readthedocs.io/en/latest/).

The `cgat-showcase` pipeline highlights some of the functionality of `cgat-core`. Additionally, more advanced workflows for next-generation sequencing analysis are available in the [cgat-flow repository](https://github.com/cgat-developers/cgat-flow).

## Tutorial start {#tutorial-start}

### Step 1: Download the tutorial data {#download-data}

Create a new directory, navigate to it, and download the test data:

```bash
mkdir showcase
cd showcase
wget https://www.cgat.org/downloads/public/showcase/showcase_test_data.tar.gz
tar -zxvf showcase_test_data.tar.gz
```

### Step 2: Generate a configuration YAML file {#generate-config}

Navigate to the test data directory and generate a configuration file for the pipeline:

```bash
cd showcase_test_data
cgatshowcase transdiffexpres config
```

Alternatively, you can call the workflow file directly:

```bash
python /path/to/file/pipeline_transdiffexpres.py config
```

This will generate a `pipeline.yml` file containing configuration parameters that can be used to modify the pipeline output. For this tutorial, you do not need to modify the parameters to run the pipeline. In the [Modify Config](#modify-config) section below, you will find details on how to adjust the config file to change the pipeline's output.

### Step 3: Run the pipeline {#run-pipeline}

To run the pipeline, execute the following command in the directory containing the `pipeline.yml` file:

```bash
cgatshowcase transdiffexpres make full -v5 --no-cluster
```

The `--no-cluster` flag will run the pipeline locally if you do not have access to a cluster. If you have access to a cluster, you can remove the `--no-cluster` option, and the pipeline will distribute the jobs across the cluster.

**Note**: There are many command line options available to run the pipeline. To see the available options, run:

```bash
cgatshowcase --help
```

This will start the pipeline execution. Monitor the output for any errors or warnings.

### Step 4: Review Results {#review-results}

Once the pipeline completes, review the output files generated in the `showcase_test_data` directory. These files contain the results of the pseudoalignment.

### Troubleshooting {#troubleshooting}

- **Common Issues**: If you encounter errors during execution, ensure that all dependencies are installed and paths are correctly set.
- **Logs**: Check the log files generated during the pipeline run for detailed error messages.
- **Support**: For further assistance, refer to the [CGAT-core documentation](https://cgat-core.readthedocs.io/en/latest/) or raise an issue on our [GitHub repository](https://github.com/cgat-developers/cgat-core/issues).

### Step 5: Generate a report {#generate-report}

The final step is to generate a report to display the output of the pipeline. We recommend using `MultiQC` for generating reports from commonly used bioinformatics tools (such as mappers and pseudoaligners) and `Rmarkdown` for generating custom reports.

To generate these reports, run the following command:

```bash
cgatshowcase transdiffexprs make build_report -v 5 --no-cluster
```

This will generate a `MultiQC` report in the folder `MultiQC_report.dir/` and an `Rmarkdown` report in `R_report.dir/`.

## Core Concepts {#core-concepts}

### Pipeline Structure {#pipeline-structure}

A CGAT pipeline typically consists of:
1. **Tasks**: Individual processing steps
2. **Dependencies**: Relationships between tasks
3. **Configuration**: Pipeline settings
4. **Execution**: Running the pipeline

### Task Types {#task-types}

1. **@transform**: One-to-one file transformation
```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    pass
```

2. **@merge**: Many-to-one operation
```python
@merge("*.counts", "final_counts.txt")
def merge_counts(infiles, outfile):
    pass
```

3. **@split**: One-to-many operation
```python
@split("input.txt", "chunk_*.txt")
def split_file(infile, outfiles):
    pass
```

### Resource Management {#resource-management}

Control resource allocation:
```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    job_memory = "8G"
    job_threads = 4
    statement = """
    samtools sort -@ %(job_threads)s -m %(job_memory)s 
    %(infile)s > %(outfile)s
    """
    P.run(statement)
```

### Error Handling {#error-handling}

Implement robust error handling:
```python
try:
    P.run(statement)
except P.PipelineError as e:
    L.error("Task failed: %s" % e)
    raise
```

## Advanced Topics {#advanced-topics}

### 1. Pipeline Parameters {#pipeline-parameters}

Access configuration parameters:
```python
# Get parameter with default
threads = PARAMS.get("threads", 1)

# Required parameter
input_dir = PARAMS["input_dir"]
```

### 2. Logging {#logging}

Use the logging system:
```python
# Log information
L.info("Processing %s" % infile)

# Log warnings
L.warning("Low memory condition")

# Log errors
L.error("Task failed: %s" % e)
```

### 3. Temporary Files {#temporary-files}

Manage temporary files:
```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    # Get temp directory
    tmpdir = P.get_temp_dir()
    
    statement = """
    samtools sort -T %(tmpdir)s/sort 
    %(infile)s > %(outfile)s
    """
    P.run(statement)
```

## Best Practices {#best-practices}

### Code Organization {#code-organization}

#### 1. Task Structure
- Use meaningful task names
- Group related tasks together
- Keep tasks focused and single-purpose
- Document task dependencies clearly

#### 2. File Management
- Use consistent file naming patterns
- Organize output directories logically
- Clean up temporary files
- Handle file paths safely

#### 3. Documentation
- Add docstrings to all tasks
- Document configuration parameters
- Include usage examples
- Maintain a clear README

### Resource Management {#resource-management-best-practices}

#### 1. Memory Usage
- Set appropriate memory limits
- Scale memory with input size
- Monitor memory consumption
- Handle memory errors gracefully

```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    """Sort BAM file with memory scaling."""
    # Scale memory based on input size
    infile_size = os.path.getsize(infile)
    job_memory = "%dG" % max(4, infile_size // (1024**3) + 2)
    
    statement = """
    samtools sort -m %(job_memory)s %(infile)s > %(outfile)s
    """
    P.run(statement)
```

#### 2. CPU Allocation
- Set appropriate thread counts
- Consider cluster limitations
- Scale threads with task needs
- Monitor CPU usage

```python
@transform("*.fa", suffix(".fa"), ".indexed")
def index_genome(infile, outfile):
    """Index genome with appropriate thread count."""
    # Set threads based on system
    job_threads = min(4, os.cpu_count())
    
    statement = """
    bwa index -t %(job_threads)s %(infile)s
    """
    P.run(statement)
```

#### 3. Temporary Files
- Use proper temporary directories
- Clean up after task completion
- Handle cleanup in error cases
- Monitor disk usage

```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_with_temp(infile, outfile):
    """Sort using managed temporary directory."""
    tmpdir = P.get_temp_dir()
    try:
        statement = """
        samtools sort -T %(tmpdir)s/sort %(infile)s > %(outfile)s
        """
        P.run(statement)
    finally:
        P.cleanup_tmpdir()
```

### Error Handling {#error-handling-best-practices}

#### 1. Task Failures
- Implement proper error checking
- Log informative error messages
- Clean up on failure
- Provide recovery options

```python
@transform("*.txt", suffix(".txt"), ".processed")
def process_with_errors(infile, outfile):
    """Process files with error handling."""
    try:
        statement = """
        process_data %(infile)s > %(outfile)s
        """
        P.run(statement)
    except P.PipelineError as e:
        L.error("Processing failed: %s" % e)
        # Cleanup and handle error
        cleanup_and_notify()
        raise
```

#### 2. Input Validation
- Check input file existence
- Validate input formats
- Verify parameter values
- Handle missing data

```python
@transform("*.bam", suffix(".bam"), ".stats")
def calculate_stats(infile, outfile):
    """Calculate statistics with input validation."""
    # Check input file
    if not os.path.exists(infile):
        raise ValueError("Input file not found: %s" % infile)
    
    # Verify file format
    if not P.is_valid_bam(infile):
        raise ValueError("Invalid BAM file: %s" % infile)
    
    statement = """
    samtools stats %(infile)s > %(outfile)s
    """
    P.run(statement)
```

#### 3. Logging
- Use appropriate log levels
- Include relevant context
- Log progress and milestones
- Maintain log rotation

```python
@transform("*.data", suffix(".data"), ".processed")
def process_with_logging(infile, outfile):
    """Process with comprehensive logging."""
    L.info("Starting processing of %s" % infile)
    
    try:
        statement = """
        process_data %(infile)s > %(outfile)s
        """
        P.run(statement)
        L.info("Successfully processed %s" % infile)
    except Exception as e:
        L.error("Failed to process %s: %s" % (infile, e))
        raise
```

### Pipeline Configuration {#pipeline-configuration}

#### 1. Parameter Management
- Use configuration files
- Set sensible defaults
- Document parameters
- Validate parameter values

```yaml
# pipeline.yml
pipeline:
    name: example_pipeline
    version: 1.0.0

# Resource configuration
cluster:
    memory_default: 4G
    threads_default: 1
    queue: main

# Processing parameters
params:
    min_quality: 20
    max_threads: 4
    chunk_size: 1000
```

#### 2. Environment Setup
- Use virtual environments
- Document dependencies
- Version control configuration
- Handle platform differences

```bash
# Create virtual environment
python -m venv pipeline-env

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export PIPELINE_CONFIG=/path/to/pipeline.yml
```

#### 3. Testing
- Write unit tests
- Test with sample data
- Verify outputs
- Monitor performance

```python
def test_pipeline():
    """Test pipeline with sample data."""
    # Run pipeline
    statement = """
    python pipeline.py make all --local
    """
    P.run(statement)
    
    # Verify outputs
    assert os.path.exists("expected_output.txt")
    assert check_output_validity("expected_output.txt")
```

### Troubleshooting {#troubleshooting-best-practices}

If you encounter issues:

1. **Check Logs**
   - Review pipeline logs
   - Check cluster logs
   - Examine error messages
   - Monitor resource usage

2. **Common Issues**
   - Memory allocation errors
   - File permission problems
   - Cluster queue issues
   - Software version conflicts

3. **Getting Help**
   - Check documentation
   - Search issue tracker
   - Ask on forums
   - Contact support team

For more detailed information, see:
- [Pipeline Overview](../pipeline_modules/overview.md)
- [Cluster Configuration](../pipeline_modules/cluster.md)
- [Error Handling](../pipeline_modules/execution.md)

## Next Steps {#next-steps}

- Review the [Examples](examples.md) section
- Learn about [Cluster Configuration](../pipeline_modules/cluster.md)
- Explore [Cloud Integration](../s3_integration/configuring_s3.md)

For more advanced topics, see the [Pipeline Modules](../pipeline_modules/overview.md) documentation.

## Conclusion {#conclusion}

This completes the tutorial for running the `transdiffexprs` pipeline for `cgat-showcase`. We hope you find it as useful as we do for writing workflows in Python.