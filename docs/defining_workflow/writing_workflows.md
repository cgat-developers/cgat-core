# Writing a workflow

## Our workflow philosophy

The explicit aim of CGAT-core is to allow users to quickly and easily build their own computational pipelines, speeding up their analysis workflow.

When building pipelines, it is often useful to keep in mind the following guiding principles:

### Flexibility

There are always new tools and insights that could be incorporated into a pipeline. Ultimately, a pipeline should be flexible, and the code should not constrain you when implementing new features.

### Scriptability

The pipeline should be scriptable, i.e., the entire pipeline can be run within another pipeline. Similarly, parts of a pipeline should be easily duplicated to process several data streams in parallel. This is crucial in genome studies, as a single analysis will not always permit making inferences by itself. When writing a pipeline, we typically create a command line script (included in the CGAT-apps repository) and then run this script as a command line statement in the pipeline.

### Reproducibility

The pipeline should be fully automated so that the same inputs and configuration produce the same outputs.

### Reusability

The pipeline should be reusable on similar data, preferably requiring only changes to a configuration file (such as `pipeline.yml`).

### Archivability

Once finished, the whole project should be archivable without relying heavily on external data. This process should be simple; all project data should be self-contained, without needing to go through various directories or databases to determine dependencies.

## Building a pipeline

The best way to build a pipeline is to start from an example. The [CGAT Showcase](https://cgat-showcase.readthedocs.io/en/latest/index.html) contains a toy example of an RNA-seq analysis pipeline, demonstrating how simple workflows can be generated with minimal code. For more complex workflows, you can refer to [CGAT-Flow](https://github.com/cgat-developers/cgat-flow).

For a step-by-step tutorial on running pipelines, refer to our [Getting Started Tutorial](#).

To construct a pipeline from scratch, continue reading below.

In an empty directory, create a new directory and then a Python file with the same name. For example:

```bash
mkdir test && touch pipeline_test.py
```

All pipelines require a `.yml` configuration file that allows you to modify the behaviour of your code. This file is placed in the `test/` directory and should have the same name as the pipeline Python file:

```bash
touch test/pipeline.yml
```

To facilitate debugging and reading, our pipelines are designed so that the pipeline task file contains Ruffus tasks, while the code to transform and analyse data is in an associated module file.

If you wish to create a module file, it is conventionally named using the format `ModuleTest.py`. You can import it into the main pipeline task file (`pipeline_test.py`) as follows:

```python
import ModuleTest
```

The [pipeline module](https://github.com/cgat-developers/cgat-core/tree/master/cgatcore/pipeline) in CGAT-core provides many useful functions for pipeline construction.

## Pipeline input

Pipelines are executed within a dedicated working directory, which usually contains:

- A pipeline configuration file: `pipeline.yml`
- Input data files, typically specified in the pipeline documentation

Other files that might be used include external data files, such as genomes, referred to by their full path.

Pipelines work with input files in the working directory, usually identified by their suffix. For instance, a mapping pipeline might look for any `.fastq.gz` files in the directory, perform QC on them, and map the reads to a genome sequence.

## Pipeline output

The pipeline will generate files and database tables in the working directory. You can structure your files/directories in any way that fits your needs—some prefer a flat structure with many files, while others use deeper hierarchies.

To save disk space, compressed files should be used wherever possible. Most data files compress well; for example, `fastq` files often compress by up to 80%. Working with compressed files is straightforward using Unix pipes (`gzip`, `gunzip`, `zcat`).

If you need random access to a file, load it into a database and index it appropriately. Genomic interval files can be indexed with `tabix` to allow random access.

## Import statements

To run our pipelines, you need to import the CGAT-core Python modules into your pipeline. We recommend importing the following modules for every CGAT pipeline:

```python
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
```

Additional modules can be imported as needed.

## Selecting the appropriate Ruffus decorator

Before starting a pipeline, it is helpful to map out the steps and flow of your potential pipeline on a whiteboard. This helps identify the inputs and outputs of each task. Once you have a clear picture, determine which Ruffus decorator to use for each task. For more information on each decorator, refer to the [Ruffus documentation](http://www.ruffus.org.uk/decorators/decorators.html).

## Running commands within tasks

To run a command line program within a pipeline task, build a statement and call the `P.run()` method:

```python
@transform('*.unsorted', suffix('.unsorted'), '.sorted')
def sortFile(infile, outfile):
    statement = '''sort %(infile)s > %(outfile)s'''
    P.run(statement)
```

In the `P.run()` method, the environment of the caller is examined for a variable called `statement`, which is then subjected to string substitution from other variables in the local namespace. In the example above, `%(infile)s` and `%(outfile)s` are replaced with the values of `infile` and `outfile`, respectively.

The same mechanism also allows configuration parameters to be set, as shown here:

```python
@transform('*.unsorted', suffix('.unsorted'), '.sorted')
def sortFile(infile, outfile):
    statement = '''sort -t %(tmpdir)s %(infile)s > %(outfile)s'''
    P.run(statement)
```

In this case, the configuration parameter `tmpdir` is substituted into the command.

### Chaining commands with error checking

If you need to chain multiple commands, you can use `&&` to ensure that errors in upstream commands are detected:

```python
@transform('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted')
def sortFile(infile, outfile):
    statement = '''gunzip %(infile)s %(infile)s.tmp &&
                  sort -t %(tmpdir)s %(infile)s.tmp > %(outfile)s &&
                  rm -f %(infile)s.tmp'''
    P.run(statement)
```

Alternatively, you can achieve this more efficiently using pipes:

```python
@transform('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
def sortFile(infile, outfile):
    statement = '''gunzip < %(infile)s | sort -t %(tmpdir)s | gzip > %(outfile)s'''
    P.run(statement)
```

The pipeline automatically inserts code to check for error return codes when multiple commands are combined in a pipe.

## Running commands on a cluster

To run commands on a cluster, set `to_cluster=True`:

```python
@files('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
def sortFile(infile, outfile):
    to_cluster = True
    statement = '''gunzip < %(infile)s | sort -t %(tmpdir)s | gzip > %(outfile)s'''
    P.run(statement)
```

Pipelines will use command line options such as `--cluster-queue` and `--cluster-priority` for global job control. For instance, to change the priority when starting the pipeline:

```bash
python <pipeline_script.py> --cluster-priority=-20
```

To set job-specific options, you can define additional variables:

```python
@files('*.unsorted.gz', suffix('.unsorted.gz'), '.sorted.gz')
def sortFile(infile, outfile):
    to_cluster = True
    job_queue = 'longjobs.q'
    job_priority = -10
    job_options = "-pe dedicated 4 -R y"
    statement = '''gunzip < %(infile)s | sort -t %(tmpdir)s | gzip > %(outfile)s'''
    P.run(statement)
```

The statement above will run in the queue `longjobs.q` with a priority of `-10`. It will also be executed in the parallel environment `dedicated`, using at least four cores.

## Combining commands

To combine commands, use `&&` to ensure they execute in the intended order:

```python
statement = """
module load cutadapt &&
cutadapt ...
"""

P.run(statement)
```

Without `&&`, the command would fail because the `cutadapt` command would execute as part of the `module load` statement.

## Useful information regarding decorators

For a full list of Ruffus decorators that control pipeline flow, see the [Ruffus documentation](http://www.ruffus.org.uk/decorators/decorators.html).

Here are some examples of modifying an input file name to transform it into the output filename:

### Using Suffix

```python
@transform(pairs, suffix('.fastq.gz'), ('_trimmed.fastq.gz', '_trimmed.fastq.gz'))
```

This will transform an input `<name_of_file>.fastq.gz` into an output `<name_of_file>_trimmed.fastq.gz`.

### Using Regex

```python
@follows(mkdir("new_folder.dir"))
@transform(pairs, regex('(\S+).fastq.gz'), ('new_folder.dir/\1_trimmed.fastq.gz', 'new_folder.dir/\1_trimmed.fastq.gz'))
```

### Using Formatter

```python
@follows(mkdir("new_folder.dir"))
@transform(pairs, formatter('(\S+).fastq.gz'), ('new_folder.dir/{SAMPLE[0]}_trimmed.fastq.gz', 'new_folder.dir/{SAMPLE[0]}_trimmed.fastq.gz'))
```

This documentation aims to provide a comprehensive guide to writing your own workflows and pipelines. For more advanced usage, please refer to the original CGAT-core and Ruffus documentation.

