# Pipeline Modules Overview

CGAT-core provides a comprehensive set of modules for building and executing computational pipelines. This document provides an overview of the core modules and their functionality.

## Core Modules

### 1. Pipeline Control (`control.py`)
- Pipeline initialization and configuration
- Command-line interface
- Parameter management
- Logging setup

```python
from cgatcore import pipeline as P

# Initialize pipeline
P.initialize(argv)

# Get parameters
PARAMS = P.get_parameters()

# Setup logging
L = P.get_logger()
```

### 2. Task Execution (`execution.py`)
- Job submission and monitoring
- Resource management
- Error handling
- Cleanup procedures

```python
# Run command
P.run("samtools sort input.bam")

# Submit Python function
P.submit(module="my_module",
         function="process_data",
         infiles="input.txt",
         outfiles="output.txt")
```

### 3. Cluster Integration (`cluster.py`)
- Cluster job management
- Resource allocation
- Queue selection
- Job monitoring

```python
# Configure cluster
P.setup_cluster()

# Submit cluster job
statement = """samtools sort input.bam"""
job_threads = 4
job_memory = "8G"
P.run(statement)
```

### 4. File Management (`files.py`)
- File path handling
- Temporary file management
- File type detection
- Pattern matching

```python
# Get temporary directory
tmpdir = P.get_temp_dir()

# Clean up temporary files
P.cleanup_tmpdir()
```

## Advanced Features

### 1. Parameter Management (`parameters.py`)
- Configuration file parsing
- Parameter validation
- Default value handling
- Environment integration

```python
# Load parameters
PARAMS = P.get_parameters([
    "pipeline.yml",
    "cluster.yml"
])

# Access parameters
input_dir = PARAMS["input_dir"]
threads = PARAMS.get("threads", 1)
```

### 2. Database Integration (`database.py`)
- SQLite database support
- Table creation and updates
- Query execution
- Result caching

```python
# Connect to database
db = P.connect()

# Execute query
results = db.execute("SELECT * FROM stats")
```

### 3. Cloud Integration (`s3_integration.py`)
- AWS S3 support
- Cloud storage access
- File transfer
- Credential management

```python
# Configure S3
P.configure_s3()

# Use S3-aware decorators
@P.s3_transform("s3://bucket/input.txt",
                suffix(".txt"), ".processed")
def process_s3_file(infile, outfile):
    pass
```

## Pipeline Development

### 1. Task Definition

```python
@transform("*.fastq.gz", suffix(".fastq.gz"), ".bam")
def map_reads(infile, outfile):
    """Map reads to reference genome."""
    job_threads = 4
    job_memory = "8G"
    
    statement = """
    bwa mem -t %(job_threads)s 
        reference.fa 
        %(infile)s > %(outfile)s
    """
    P.run(statement)
```

### 2. Pipeline Flow

```python
@follows(map_reads)
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    """Sort BAM files."""
    statement = """
    samtools sort %(infile)s > %(outfile)s
    """
    P.run(statement)

@follows(sort_bam)
@merge("*.sorted.bam", "final_report.txt")
def create_report(infiles, outfile):
    """Generate final report."""
    statement = """
    multiqc . -o %(outfile)s
    """
    P.run(statement)
```

## Best Practices

### 1. Code Organization
- Group related tasks
- Use meaningful task names
- Document pipeline steps
- Implement error handling

### 2. Resource Management
- Specify memory requirements
- Set appropriate thread counts
- Use temporary directories
- Clean up intermediate files

### 3. Error Handling
```python
try:
    P.run(statement)
except P.PipelineError as e:
    L.error("Task failed: %s" % e)
    # Implement recovery or cleanup
    cleanup_and_notify()
```

### 4. Documentation
- Add docstrings to tasks
- Document configuration options
- Include usage examples
- Maintain README files

## Pipeline Examples

### Basic Pipeline
```python
"""Example pipeline demonstrating core functionality."""

from cgatcore import pipeline as P
import logging as L

# Initialize
P.initialize()

@transform("*.txt", suffix(".txt"), ".processed")
def process_files(infile, outfile):
    """Process input files."""
    statement = """
    process_data %(infile)s > %(outfile)s
    """
    P.run(statement)

@follows(process_files)
@merge("*.processed", "report.txt")
def create_report(infiles, outfile):
    """Generate summary report."""
    statement = """
    cat %(infiles)s > %(outfile)s
    """
    P.run(statement)

if __name__ == "__main__":
    P.main()
```

For more detailed information about specific modules, see:
- [Cluster Configuration](cluster.md)
- [Task Execution](execution.md)
- [Parameter Management](parameters.md)
- [Database Integration](database.md)