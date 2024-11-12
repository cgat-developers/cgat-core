# Writing a workflow - Tutorial

The explicit aim of cgat-core is to allow users to quickly and easily build their own computational pipelines that will speed up your analysis workflow.

## Installation of cgat-core

In order to begin writing a pipeline, you will need to install the cgat-core code (see installation instructions in the "Getting Started" section).

## Tutorial start

### Setting up the pipeline

**1.** First, navigate to a directory where you want to start building your code:

```bash
mkdir test && cd test && mkdir configuration && touch configuration/pipeline.yml && touch pipeline_test.py && touch ModuleTest.py
```

This command will create a directory called `test` in the current directory with the following layout:

```
|-- configuration
|   \-- pipeline.yml
|-- pipeline_test.py
|-- ModuleTest.py
```

The layout has the following components:

- **pipeline_test.py**: This is the file that will contain all of the ruffus workflows. The file needs to be named in the format `pipeline_<name>.py`.
- **test/**: Directory containing the configuration `.yml` file. The directory needs to have the same name as the `pipeline_<name>.py` file. This folder will contain the `pipeline.yml` configuration file.
- **ModuleTest.py**: This file will contain functions that will be imported into the main ruffus workflow file (`pipeline_test.py`).

**2.** View the source code within `pipeline_test.py`

This is where the ruffus tasks will be written. The code begins with a docstring detailing the pipeline functionality. You should use this section to document your pipeline:

```python
'''This pipeline is a test and this is where the documentation goes '''
```

The pipeline then needs a few utility functions to help with executing the pipeline.

- **Import statements**: You will need to import ruffus and cgatcore utilities:

```python
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
```

Importing `ruffus` allows ruffus decorators to be used within the pipeline.
Importing `experiment` from `cgatcore` provides utility functions for argument parsing, logging, and record-keeping within scripts.
Importing `pipeline` from `cgatcore` provides utility functions for interfacing CGAT ruffus pipelines with an HPC cluster, uploading data to a database, and parameterisation.

You'll also need some Python modules:

```python
import os
import sys
```

- **Config parser**: This code helps with parsing the `pipeline.yml` file:

```python
# Load options from the config file
PARAMS = P.get_parameters([
    "%s/pipeline.yml" % os.path.splitext(__file__)[0],
    "../pipeline.yml",
    "pipeline.yml"])
```

- **Pipeline configuration**: We will add configurable variables to our `pipeline.yml` file so that we can modify the output of our pipeline. Open `pipeline.yml` and add the following:

```yaml
database:
   name: "csvdb"
```

When you run the pipeline, the configuration variables (in this case `csvdb`) can be accessed in the pipeline by `PARAMS["database_name"]`.

- **Database connection**: This code helps with connecting to an SQLite database:

```python
def connect():
    '''Utility function to connect to the database.

    Use this method to connect to the pipeline database.
    Additional databases can be attached here as well.

    Returns an sqlite3 database handle.
    '''
    dbh = sqlite3.connect(PARAMS["database_name"])
    return dbh
```

- **Commandline parser**: This code allows the pipeline to parse arguments:

```python
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)

if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
```

### Running the test pipeline

You now have the bare bones layout of the pipeline, and you need some code to execute. Below is example code that you can copy and paste into your `pipeline_test.py` file. The code includes two ruffus `@transform` tasks that parse `pipeline.yml`. The first function, called `countWords`, contains a statement that counts the number of words in the file. The statement is then executed using the `P.run()` function.

The second ruffus `@transform` function called `loadWordCounts` takes as input the output of the function `countWords` and loads the number of words into an SQLite database using `P.load()`.

The third function, `full()`, is a dummy task that runs the entire pipeline. It has an `@follows` decorator that takes the `loadWordCounts` function, completing the pipeline chain.

The following code should be pasted just before the **Commandline parser** arguments and after the **database connection** code:

```python
# ---------------------------------------------------
# Specific pipeline tasks
@transform("pipeline.yml",
           regex("(.*)\.(.*)"),
           r"\1.counts")
def countWords(infile, outfile):
    '''Count the number of words in the pipeline configuration files.'''

    # The command line statement we want to execute
    statement = '''awk 'BEGIN { printf("word\tfreq\n"); }
    {for (i = 1; i <= NF; i++) freq[$i]++}
    END { for (word in freq) printf "%s\t%d\n", word, freq[word] }'
    < %(infile)s > %(outfile)s'''

    # Execute the command in the variable statement.
    P.run(statement)

@transform(countWords,
           suffix(".counts"),
           "_counts.load")
def loadWordCounts(infile, outfile):
    '''Load results of word counting into database.'''
    P.load(infile, outfile, "--add-index=word")

# ---------------------------------------------------
# Generic pipeline tasks
@follows(loadWordCounts)
def full():
    pass
```

To run the pipeline, navigate to the working directory and then run the pipeline:

```bash
python /location/to/code/pipeline_test.py config
python /location/to/code/pipeline_test.py show full -v 5
```

This will place the `pipeline.yml` in the folder. Then run:

```bash
python /location/to/code/pipeline_test.py make full -v5 --local
```

The pipeline will then execute and count the words in the `yml` file.

### Modifying the test pipeline to build your own workflows

The next step is to modify the basic code in the pipeline to fit your particular NGS workflow needs. For example, suppose you want to convert a SAM file into a BAM file, then perform flag stats on that output BAM file. The code and layout that we just wrote can be easily modified to perform this.

The pipeline will have two steps:
1. Identify all SAM files and convert them to BAM files.
2. Take the output of step 1 and perform flag stats on that BAM file.

The first step would involve writing a function to identify all `sam` files in a `data.dir/` directory and convert them to BAM files using `samtools view`. The second function would then take the output of the first function, perform `samtools flagstat`, and output the results as a flat `.txt` file. This would be written as follows:

```python
@transform("data.dir/*.sam",
           regex("data.dir/(\S+).sam"),
           r"\1.bam")
def bamConvert(infile, outfile):
    '''Convert a SAM file into a BAM file using samtools view.'''
    
    statement = '''samtools view -bT /ifs/mirror/genomes/plain/hg19.fasta \
                   %(infile)s > %(outfile)s'''
    P.run(statement)

@transform(bamConvert,
           suffix(".bam"),
           "_flagstats.txt")
def bamFlagstats(infile, outfile):
    '''Perform flagstats on a BAM file.'''
    
    statement = '''samtools flagstat %(infile)s > %(outfile)s'''
    P.run(statement)
```

To run the pipeline:

```bash
python /path/to/file/pipeline_test.py make full -v5
```

The BAM files and flagstats outputs should be generated.

### Parameterising the code using the `.yml` file

As a philosophy, we try and avoid any hardcoded parameters, so that any variables can be easily modified by the user without changing the code.

Looking at the code above, the hardcoded link to the `hg19.fasta` file can be added as a customisable parameter, allowing users to specify any FASTA file depending on the genome build used. In the `pipeline.yml`, add:

```yaml
genome:
    fasta: /ifs/mirror/genomes/plain/hg19.fasta
```

In the `pipeline_test.py` code, the value can be accessed via `PARAMS["genome_fasta"]`.
Therefore, the code for parsing BAM files can be modified as follows:

```python
@transform("data.dir/*.sam",
           regex("data.dir/(\S+).sam"),
           r"\1.bam")
def bamConvert(infile, outfile):
    '''Convert a SAM file into a BAM file using samtools view.'''

    genome_fasta = PARAMS["genome_fasta"]

    statement = '''samtools view -bT %(genome_fasta)s \
                   %(infile)s > %(outfile)s'''
    P.run(statement)

@transform(bamConvert,
           suffix(".bam"),
           "_flagstats.txt")
def bamFlagstats(infile, outfile):
    '''Perform flagstats on a BAM file.'''
    
    statement = '''samtools flagstat %(infile)s > %(outfile)s'''
    P.run(statement)
```

Running the code again will generate the same output. However, if you had BAM files that came from a different genome build, the parameter in the `yml` file can be easily modified, the output files deleted, and the pipeline run again with the new configuration values.

