# Writing a workflow

## Our workflow philosophy

The explicit aim of CGAT-core is to allow users to quickly and easily build their own computational pipelines, which will ultimately accelerate their analysis workflows.

When building pipelines, it is useful to keep the following principles in mind:

### Flexibility
There are always new tools and insights that could be incorporated into a pipeline. A good pipeline should be flexible, ensuring that the code does not constrain the implementation of new features.

### Scriptability
The pipeline should be scriptable, meaning it can be executed within another pipeline. Additionally, sections of a pipeline should be easy to duplicate to process several data streams in parallel. This is crucial for genomic studies, where a single analysis often cannot provide sufficient insight alone. Typically, when we write a pipeline, we aim to create a command-line script (which we include in the CGAT-apps repository) that can be run as part of the pipeline.

### Reproducibility
The pipeline should be fully automated so that the same inputs and configuration will produce identical outputs every time it is run.

### Reusability
The pipeline should be reusable for similar data, ideally only requiring minor modifications to a configuration file (`pipeline.yml`).

### Archivability
Once a project is complete, the entire project should be archivable without significant dependencies on external data. This implies that all project data should be self-contained, avoiding extensive searching through directories or databases to determine what is necessary.

## Building a pipeline

The best way to build a pipeline is to start from an example. In [cgat-showcase](https://cgat-showcase.readthedocs.io/en/latest/index.html), we provide a toy example of an RNA-seq analysis pipeline that demonstrates how to generate simple workflows with minimal code. For more complex workflows, the [cgat-flow repository](https://github.com/cgat-developers/cgat-flow) provides several examples.

For a step-by-step tutorial on how to run these pipelines, please refer to our [getting started tutorial](#getting_started-Tutorial). For help on constructing pipelines from scratch, please continue reading for more details.

To start building a pipeline, create an empty directory and then add a Python file named appropriately. For instance:

```sh
mkdir test && touch pipeline_test.py
```

All pipelines require a configuration file in YAML format that allows the user to modify parameter values to adjust the pipeline's behaviour. This configuration file should be placed in a directory named after the pipeline's main Python file:

```sh
touch test/pipeline.yml
```

Our recommendation for pipeline organization is to create a main pipeline task file containing Ruffus tasks and to call functions from an associated module file. This modular structure keeps the code readable and manageable.

If you wish to create a module file, you can save it as, for instance, `ModuleTest.py`, and import it into the main pipeline task file (`pipeline_test.py`) as follows:

```python
import ModuleTest
```

Pipelines can be constructed using the `pipeline` module from cgat-core, which contains a variety of useful functions to facilitate this.

## Pipeline input

Pipelines are executed in a dedicated working directory and usually require the following files:

- A pipeline configuration file (`pipeline.yml`)
- Input data files, typically specified in the documentation of each pipeline

Other potential files include external data, such as genomes, referenced by their full path name.

The pipeline typically processes files in the working directory, identified by their suffixes. For instance, a mapping pipeline may look for `*.fastq.gz` files, run quality control on them, and subsequently map the reads to a genome sequence.

## Pipeline output

The pipeline generates files and database tables in the working directory. You can organise the file structure in whatever way best suits your needs, whether a flat directory hierarchy or a deeply nested structure.

## Guidelines

To save disk space, it's best to use compressed files wherever possible. Most data files compress efficiently. For instance, fastq files typically compress by 80% or more, reducing a 10GB file to only 2GB.

Working with compressed files is straightforward using Unix pipes and commands like `gzip`, `gunzip`, or `zcat`. If you require random access, consider loading the file into a database or indexing it appropriately. For example, genomic interval files can be indexed using tabix.

## Import statements

To run cgat-core pipelines, you will need to import several Python modules into your pipeline script. We recommend importing the following for every CGAT pipeline:

```python
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
```

You can import additional modules as needed.

## Selecting the appropriate Ruffus decorator

Before writing the code for a pipeline, it is helpful to visualise the steps and flow on a whiteboard. Doing so will help you identify the input and output of each task, which allows you to determine which Ruffus decorator is most appropriate for each part of the pipeline. Documentation for Ruffus decorators can be found in the [Ruffus documentation](http://www.ruffus.org.uk/decorators/decorators.html).

## Running commands within tasks

To run a command-line program in a pipeline task, build a `statement` and use the `pipeline.run()` method:

```python
@transform('*.unsorted', suffix('.unsorted'), '.sorted')
def sortFile(infile, outfile):
    statement = '''sort %(infile)s > %(outfile)s'''
    P.run(statement)
```

When you call the `pipeline.run()` method, the environment of the caller is examined for a `statement` variable. This variable undergoes string substitution with variables in the local namespace. For example, `%(infile)s` and `%(outfile)s` will be replaced by the respective values of `infile` and `outfile`.

Configuration parameters can also be included in this mechanism:

```python
@transform('*.unsorted', suffix('.unsorted'), '.sorted')
def sortFile(infile, outfile):
    statement = '''sort -t %(tmpdir)s %(infile)s > %(outfile)s'''
    P.run(statement)
```

In this example, `tmpdir` is a configuration parameter that is automatically substituted into the command.

The pipeline will halt if a command exits with an error code. If multiple commands are chained together, only the return value of the last command is used to detect errors. To detect errors from upstream commands, chain them with `&&`:

```python
@transform('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted')
def sortFile(infile, outfile):
    statement = '''gunzip %(infile)s %(infile)s.tmp &&
                  sort -t %(tmpdir)s %(infile)s.tmp > %(outfile)s &&
                  rm -f %(infile)s.tmp'''
    P.run(statement)
```

Alternatively, using pipes might be more efficient:

```python
@transform('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
def sortFile(infile, outfile):
    statement = '''gunzip < %(infile)s \
                  | sort -t %(tmpdir)s \
                  | gzip > %(outfile)s'''
    P.run(statement)
```

Pipelines will automatically insert code to check for errors when multiple commands are combined in a pipeline.

## Running commands on the cluster

To run commands on a cluster, set `to_cluster=True`.

For instance, to run the previous example on a cluster:

```python
@files('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
def sortFile(infile, outfile):
    to_cluster = True
    statement = '''gunzip < %(infile)s \
                  | sort -t %(tmpdir)s \
                  | gzip > %(outfile)s'''
    P.run(statement)
```

The pipeline will automatically create job submission files, submit the job to the cluster, and wait for it to finish.

You can set global job options using command-line options, such as `--cluster-queue` and `--cluster-priority`. To change a job's priority when starting the pipeline, for example:

```sh
python <pipeline_script.py> --cluster-priority=-20
```

To specify options for a particular task, define additional variables in the task definition:

```python
@files('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
def sortFile(infile, outfile):
    to_cluster = True
    job_queue = 'longjobs.q'
    job_priority = -10
    job_options = "-pe dedicated 4 -R y"
    
    statement = '''gunzip < %(infile)s \
                  | sort -t %(tmpdir)s \
                  | gzip > %(outfile)s'''
    P.run(statement)
```

The above code runs in the queue `longjobs.q` at a priority of `-10`. Additionally, it will be executed in the parallel environment `dedicated` with at least four cores.

Array jobs can be controlled using the `job_array` variable:

```python
@files('*.in', suffix('.in'), '.out')
def myGridTask(infile, outfile):
    job_array = (0, nsnps, stepsize)
    
    statement = '''grid_task.bash %(infile)s %(outfile)s
                   > %(outfile)s.$SGE_TASK_ID 2> %(outfile)s.err.$SGE_TASK_ID'''
    P.run(statement)
```

The above example uses Grid Engine environment variables such as `SGE_TASK_ID` to determine which chunk of data each task should process.

The temporary job submission files (`tmp*`) in the working directory are deleted automatically unless a run is interrupted, in which case manual cleanup may be necessary.

