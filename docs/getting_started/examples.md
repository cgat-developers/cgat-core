# Running a pipeline

This section provides a tutorial-like introduction to running CGAT pipelines. For an example of how to build simple computational pipelines, refer to [cgat-showcase](https://github.com/cgat-developers/cgat-showcase). To see how `cgatcore` is used to build more complex computational pipelines, refer to the [cgat-flow repository](https://github.com/cgat-developers/cgat-flow).

## Introduction

A pipeline takes input data and performs a series of automated steps to produce output data. Each pipeline is usually coupled with a report (such as MultiQC or Rmarkdown) to summarise and visualise the results.

It helps if you are familiar with:

- The Unix command line to run and debug the pipeline
- [Python](https://www.python.org/) to understand what happens in the pipeline
- [Ruffus](http://www.ruffus.org.uk/) to understand the pipeline code
- SGE (or any other workload manager) to monitor jobs
- [Git](https://git-scm.com/) to keep code up-to-date

## Setting up a pipeline

**Step 1**: Install `cgat-showcase` (a toy example of a `cgatcore` pipeline).

Ensure your computing environment is appropriate and follow `cgat-showcase` installation instructions (see [Installation instructions](https://cgat-showcase.readthedocs.io/en/latest/getting_started/Installation.html)).

**Step 2**: Clone the repository

To inspect the code and layout, clone the repository:

```bash
git clone https://github.com/cgat-developers/cgat-showcase.git
```

When inspecting the repository:

- The source directory will contain the pipeline master script named `cgatshowcase/pipeline_<name>.py`.
- The default configuration files are in `cgatshowcase/pipeline<Name>/`.
- Associated module files are typically named `cgatshowcase/Module<Name>.py` and contain code required to run pipeline tasks.

**Step 3**: Create a working directory

To run a pipeline, create a working directory and navigate to it:

```bash
mkdir version1
cd version1/
```

The pipeline will execute and generate files in this directory.

To use test data for `cgat-showcase`, download it with:

```bash
wget https://www.cgat.org/downloads/public/showcase/showcase_test_data.tar.gz
tar -zxvf showcase_test_data.tar.gz
cd showcase_test_data
```

**Step 4**: Configure the cluster

Running pipelines on a cluster requires configuring DRMAA API settings for `cgatcore`. The default cluster engine is SGE, but SLURM and Torque/PBSpro are also supported. To use a non-SGE cluster, create a `.cgat.yml` file in your home directory and configure the parameters (see the [cluster configuration documentation](https://cgat-core.readthedocs.io/en/latest/getting_started/Cluster_config.html)).

**Step 5**: Generate a configuration file

Our pipelines are written with minimal hard-coded options. To run a pipeline, generate an initial configuration file:

```bash
cgatshowcase <name> config
```

For example, to run the `transdiffexprs` pipeline, run:

```bash
cgatshowcase transdiffexprs config
```

This will create a new `pipeline.yml` file. **You must edit this file** as the default values are unlikely to suit your data. The configuration file format is simple and well-documented. For more information, see the [ConfigParser documentation](http://docs.python.org/library/configparser.html).

**Step 6**: Add input files

The required input is specific to each pipeline. Check the pipeline documentation to determine which files are needed and where to place them. Typically, input files are linked into the working directory and follow pipeline-specific naming conventions.

**Step 7**: Check dependencies

Check if all external dependencies, such as tools and R packages, are satisfied by running:

```bash
cgatshowcase <name> check
```

## Running a pipeline

Pipelines are controlled by a Python script named `pipeline_<name>.py` in the source directory. Command line usage information is available by running:

```bash
cgatshowcase <name> --help
```

Alternatively, call the Python script directly:

```bash
python /path/to/code/cgatshowcase/pipeline_<name>.py --help
```

The basic syntax for `pipeline_<name>.py` is:

```bash
cgatshowcase <name> [workflow options] [workflow arguments]
```

For example, to run the `readqc` pipeline:

```bash
cgatshowcase readqc make full
```

### Workflow options

- **make `<task>`**: Run all tasks required to build `<task>`.
- **show `<task>`**: Show tasks required to build `<task>` without executing them.
- **plot `<task>`**: Plot an image of workflow (requires [Inkscape](http://inkscape.org/)) of pipeline state for `<task>`.
- **touch `<task>`**: Touch files without running `<task>` or prerequisites, setting timestamps so files appear up-to-date.
- **config**: Write a new configuration file (`pipeline.ini`) with default values (won't overwrite existing files).
- **clone `<srcdir>`**: Clone a pipeline from `<srcdir>` into the current directory.

To run a long pipeline appropriately:

```bash
nice -19 nohup cgatshowcase <name> make full -v5 -c1
```

This command will keep the pipeline running if you close the terminal.

### Fastq naming convention

Most of our pipelines assume input FASTQ files follow this naming convention:

```
sample1-condition.fastq.1.gz
sample1-condition.fastq.2.gz
```

This convention ensures regular expressions do not need to account for the read within the name, making it more explicit.

### Additional pipeline options

Running the pipeline with `--help` will show additional workflow arguments that modify the pipeline's behaviour:

- **--no-cluster**: Run the pipeline locally.
- **--input-validation**: Check `pipeline.ini` file for missing values before starting.
- **--debug**: Add debugging information to the console (not the logfile).
- **--dry-run**: Perform a dry run (do not execute shell commands).
- **--exceptions**: Echo exceptions immediately as they occur.
- **-c --checksums**: Set the level of Ruffus checksums.

## Building pipeline reports

We associate some form of reporting with our pipelines to display summary information as nicely formatted HTML pages. Currently, CGAT supports three types of reports:

- **MultiQC**: For general alignment and tool reporting.
- **R Markdown**: For bespoke reporting.
- **Jupyter Notebook**: For bespoke reporting.

Refer to the specific pipeline documentation at the beginning of the script to determine which type of reporting is implemented.

Reports are generated using the following command once a workflow has completed:

```bash
cgatshowcase <name> make build_report
```

### MultiQC report

[MultiQC](https://multiqc.info/) is a Python framework for automating reporting, implemented in most workflows to generate QC stats for commonly used tools.

### R Markdown

R Markdown report generation is useful for creating custom reports. This is implemented in the `bamstats` workflow.

### Jupyter Notebook

Jupyter Notebook is another approach we use for bespoke reports, also implemented in the `bamstats` workflow.

## Troubleshooting

Many things can go wrong while running the pipeline:

- **Bad input format**: The pipeline does not perform sanity checks on input formats, which may lead to missing or incorrect results.
- **Pipeline disruptions**: Issues with the cluster, file system, or terminal may cause the pipeline to abort.
- **Bugs**: Changes in program versions or inputs can cause unexpected issues.

If the pipeline aborts, read the log files and error messages (e.g., `nohup.out`) to locate the error. Attempt to fix the error, remove the output files from the step in which the error occurred, and restart the pipeline.

**Note**: Look out for upstream errors. For example, if a geneset filtering by specific contigs does not match, the geneset may be empty, causing errors in downstream steps. To resolve this, fix the initial error and delete the files from the geneset-building step, not just the step that threw the error.

### Common pipeline errors

One common error is:

```text
GLOBAL_SESSION = drmaa.Session()
NameError: name 'drmaa' is not defined
```

This occurs because you are not connected to the cluster. Alternatively, run the pipeline in local mode by adding `--no-cluster` as a command line option.

### Updating to the latest code version

To get the latest bug fixes, navigate to the source directory and run:

```bash
git pull
```

This command retrieves the latest changes from the master repository and updates your local version.

## Using qsub commands

We recommend using `cgat-core` to perform job submissions, as this is handled automatically. However, if you wish to use `qsub` manually, you can do so. Since the statements passed to `P.run()` are essentially command-line scripts, you can write the `qsub` commands as needed. For example:

```python
statement = "qsub [commands] echo 'This is where you would put commands you want ran' "
P.run(statement)
```

When running the pipeline, make sure to specify `--no-cluster` as a command line option.

### Troubleshooting

- **Common Issues**: If you encounter errors during pipeline execution, ensure that all dependencies are installed and paths are correctly set.
- **Logs**: Check the log files generated during the pipeline run for detailed error messages.
- **Support**: For further assistance, refer to the [CGAT-core documentation](https://cgat-developers.github.io/cgat-core/) or raise an issue on our [GitHub repository](https://github.com/cgat-developers/cgat-core/issues).

## CGAT-core Examples

This guide provides practical examples of CGAT-core pipelines for various use cases, from basic file processing to complex genomics workflows.

## Quick Start Examples

### Hello World Pipeline

```python
"""hello_world.py - Simple CGAT pipeline example

This pipeline demonstrates the basic structure of a CGAT pipeline:
1. Task definition
2. Pipeline flow
3. Command execution
"""

from ruffus import *
from cgatcore import pipeline as P
import sys

# ------------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------------
@originate("hello.txt")
def create_file(outfile):
    """Create a simple text file."""
    statement = """echo "Hello, CGAT!" > %(outfile)s"""
    P.run(statement)

@transform(create_file, suffix(".txt"), ".upper.txt")
def convert_to_upper(infile, outfile):
    """Convert text to uppercase."""
    statement = """cat %(infile)s | tr '[:lower:]' '[:upper:]' > %(outfile)s"""
    P.run(statement)

# ------------------------------------------------------------------------
# Pipeline Running
# ------------------------------------------------------------------------
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
```

### Configuration Example

```yaml
# pipeline.yml
pipeline:
    name: hello_world
    author: Your Name

# Cluster configuration
cluster:
    queue_manager: slurm
    queue: main
    memory_resource: mem
    memory_default: 1G
```

## Real-World Examples

### 1. Genomics Pipeline

This example demonstrates a typical RNA-seq analysis pipeline:

```python
"""rnaseq_pipeline.py - RNA-seq analysis pipeline

Features:
- FastQ quality control
- Read alignment
- Expression quantification
- Differential expression analysis
"""

from ruffus import *
from cgatcore import pipeline as P
import logging as L
import sys
import os

# ------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------
P.get_parameters([
    "%s/pipeline.yml" % os.path.splitext(__file__)[0],
    "pipeline.yml"])

# ------------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------------
@transform("*.fastq.gz", suffix(".fastq.gz"), ".fastqc.done")
def run_fastqc(infile, outfile):
    """Quality control of sequencing reads."""
    job_threads = 1
    job_memory = "2G"
    
    statement = """
    fastqc --outdir=fastqc %(infile)s &&
    touch %(outfile)s
    """
    P.run(statement)

@transform("*.fastq.gz", suffix(".fastq.gz"), ".bam")
def align_reads(infile, outfile):
    """Align reads to reference genome."""
    job_threads = 8
    job_memory = "32G"
    
    statement = """
    STAR 
        --runThreadN %(job_threads)s
        --genomeDir %(genome_dir)s
        --readFilesIn %(infile)s
        --readFilesCommand zcat
        --outFileNamePrefix %(outfile)s
        --outSAMtype BAM SortedByCoordinate
    """
    P.run(statement)

@transform(align_reads, suffix(".bam"), ".counts.tsv")
def count_features(infile, outfile):
    """Count reads in genomic features."""
    job_threads = 4
    job_memory = "8G"
    
    statement = """
    featureCounts
        -T %(job_threads)s
        -a %(annotations)s
        -o %(outfile)s
        %(infile)s
    """
    P.run(statement)

@merge(count_features, "deseq2_results")
def run_deseq2(infiles, outfile):
    """Differential expression analysis."""
    job_memory = "16G"
    
    statement = """
    Rscript scripts/run_deseq2.R
        --counts=%(infiles)s
        --design=%(design_file)s
        --outdir=%(outfile)s
    """
    P.run(statement)

if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
```

### 2. Data Processing Pipeline

Example of a data processing pipeline with S3 integration:

```python
"""data_pipeline.py - Data processing with S3 integration

Features:
- S3 input/output
- Parallel processing
- Error handling
- Resource management
"""

from ruffus import *
from cgatcore import pipeline as P
import logging as L
import sys
import os

# ------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------
P.get_parameters([
    "%s/pipeline.yml" % os.path.splitext(__file__)[0],
    "pipeline.yml"])

# Configure S3
P.configure_s3()

# ------------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------------
@P.s3_transform("s3://bucket/data/*.csv", 
                suffix(".csv"), 
                ".processed.csv")
def process_data(infile, outfile):
    """Process CSV files from S3."""
    job_memory = "4G"
    
    statement = """
    python scripts/process_data.py
        --input=%(infile)s
        --output=%(outfile)s
        --config=%(processing_config)s
    """
    try:
        P.run(statement)
    except P.PipelineError as e:
        L.error("Processing failed: %s" % e)
        # Cleanup temporary files
        cleanup_temp_files()
        raise
    finally:
        # Always clean up
        P.cleanup_tmpdir()

@P.s3_merge(process_data, 
           "s3://bucket/results/report.html")
def create_report(infiles, outfile):
    """Generate analysis report."""
    job_memory = "8G"
    
    statement = """
    python scripts/create_report.py
        --input=%(infiles)s
        --output=%(outfile)s
        --template=%(report_template)s
    """
    P.run(statement)

if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
```

### 3. Image Processing Pipeline

Example of an image processing pipeline:

```python
"""image_pipeline.py - Image processing pipeline

Features:
- Batch image processing
- Feature extraction
- Analysis reporting
"""

from ruffus import *
from cgatcore import pipeline as P
import sys
import os

# ------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------
P.get_parameters([
    "%s/pipeline.yml" % os.path.splitext(__file__)[0],
    "pipeline.yml"])

# ------------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------------
@transform("*.png", suffix(".png"), ".processed.png")
def preprocess_images(infile, outfile):
    """Image preprocessing."""
    statement = """
    python scripts/preprocess.py
        --input=%(infile)s
        --output=%(outfile)s
        --params=%(preprocessing_params)s
    """
    P.run(statement)

@transform(preprocess_images, 
          suffix(".processed.png"), 
          ".features.json")
def extract_features(infile, outfile):
    """Feature extraction."""
    statement = """
    python scripts/extract_features.py
        --input=%(infile)s
        --output=%(outfile)s
        --model=%(feature_model)s
    """
    P.run(statement)

@merge(extract_features, "analysis_report.html")
def analyze_results(infiles, outfile):
    """Generate analysis report."""
    statement = """
    python scripts/analyze.py
        --input=%(infiles)s
        --output=%(outfile)s
        --config=%(analysis_config)s
    """
    P.run(statement)

if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
```

## Best Practices

### 1. Resource Management

```python
@transform("*.bam", suffix(".bam"), ".sorted.bam")
def sort_bam(infile, outfile):
    """Example of proper resource management."""
    # Calculate memory based on input size
    infile_size = os.path.getsize(infile)
    job_memory = "%dG" % max(4, infile_size // (1024**3) + 2)
    
    # Set threads based on system
    job_threads = min(4, os.cpu_count())
    
    # Use temporary directory
    tmpdir = P.get_temp_dir()
    
    statement = """
    samtools sort 
        -@ %(job_threads)s 
        -m %(job_memory)s 
        -T %(tmpdir)s/sort 
        %(infile)s > %(outfile)s
    """
    P.run(statement)
```

### 2. Error Handling

```python
@transform("*.txt", suffix(".txt"), ".processed")
def robust_processing(infile, outfile):
    """Example of proper error handling."""
    try:
        statement = """
        process_data %(infile)s > %(outfile)s
        """
        P.run(statement)
    except P.PipelineError as e:
        L.error("Processing failed: %s" % e)
        # Cleanup temporary files
        cleanup_temp_files()
        raise
    finally:
        # Always clean up
        P.cleanup_tmpdir()
```

### 3. Configuration Management

```yaml
# pipeline.yml - Example configuration

# Pipeline metadata
pipeline:
    name: example_pipeline
    version: 1.0.0
    author: Your Name

# Input/Output
io:
    input_dir: /path/to/input
    output_dir: /path/to/output
    temp_dir: /tmp/pipeline

# Processing parameters
processing:
    threads: 4
    memory: 8G
    chunk_size: 1000

# Cluster configuration
cluster:
    queue_manager: slurm
    queue: main
    memory_resource: mem
    parallel_environment: smp
    max_jobs: 100

# S3 configuration (if needed)
s3:
    bucket: my-pipeline-bucket
    region: us-west-2
    transfer:
        multipart_threshold: 8388608
        max_concurrency: 10
```

## Running the Examples

1. **Setup Configuration**
   ```bash
   # Copy and edit pipeline configuration
   cp pipeline.yml.example pipeline.yml
   ```

2. **Run Pipeline**
   ```bash
   # Show pipeline tasks
   python pipeline.py show full

   # Run specific task
   python pipeline.py make task_name

   # Run entire pipeline
   python pipeline.py make full
   ```

3. **Cluster Execution**
   ```bash
   # Run on cluster
   python pipeline.py make full --cluster-queue=main
   ```

For more information, see:
- [Pipeline Overview](../pipeline_modules/overview.md)
- [Cluster Configuration](../pipeline_modules/cluster.md)
- [S3 Integration](../s3_integration/configuring_s3.md)
