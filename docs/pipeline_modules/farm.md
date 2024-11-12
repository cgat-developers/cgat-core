# farm.py - Documentation

This document provides an overview of the `farm.py` script, which is used to split a data stream into independent chunks for parallel processing on a cluster. This is particularly useful for large-scale computational tasks where dividing the workload into smaller, independent parts can significantly reduce processing time.

## Table of Contents
- [Purpose](#purpose)
- [Usage](#usage)
- [Documentation](#documentation)
  - [Input and Output Handling](#input-and-output-handling)
  - [Chunking Methods](#chunking-methods)
  - [Error Handling](#error-handling)
  - [Examples](#examples)
- [Classes and Functions](#classes-and-functions)
  - [Chunk Iterator Functions](#chunk-iterator-functions)
  - [Mapper Classes](#mapper-classes)
  - [ResultBuilder Classes](#resultbuilder-classes)
  - [build_command Function](#build_command-function)
  - [hasFinished Function](#hasfinished-function)
  - [get_option_parser Function](#get_option_parser-function)
  - [main Function](#main-function)

## Purpose

The `farm.py` script is designed to process a data stream in parallel on a cluster by splitting it into smaller, independent chunks. This approach is suitable for "embarrassingly parallel" jobs, where the computations for each chunk can be executed independently without requiring communication between them.

The script reads data from `stdin`, splits the data, executes user-specified commands on each chunk, and writes the output to `stdout`. The results are returned in the same order as they are submitted.

## Usage

The script can be run from the command line. Below are two basic examples:

- Split the input data by the first column and execute a Perl command on each split:
  ```sh
  cat go.txt | farm.py --split-at-column=1 perl -p -e "s/GO/gaga/"
  ```

- Split a fasta file at each sequence entry and compute an approximate sequence length:
  ```sh
  cat genome.fasta | farm.py --split-at-regex="^>(\S+)" "wc -c"
  ```

Run `python farm.py --help` to get detailed command-line options.

## Documentation

### Input and Output Handling

The input to `farm.py` is provided via `stdin` and processed in parallel. The output is written to `stdout` with results combined in the same order they were processed. The script ensures that duplicate headers are avoided and can also handle jobs that output multiple files.

### Chunking Methods

The script provides multiple ways to split (or "chunk") the input data:
- **Split at lines**: Divide the input data by the number of lines.
- **Split by column**: Split based on the unique values in a specified column.
- **Split using regex**: Use a regular expression to define how to split data.
- **Group using regex**: Group entries together if they match a regular expression.

### Error Handling

If an error occurs during the execution of a job, the error messages are printed, and the temporary directory used for processing is not deleted, allowing manual recovery. The script also implements a retry mechanism for failed jobs and can log errors into separate files for analysis.

### Examples

1. **Basic Example**: Split the file "go" at the first column and replace `GO` with `gaga` in each chunk:
   ```sh
   cat go | farm.py --split-at-column=1 perl -p -e "s/GO/gaga/"
   ```

2. **FASTA File Processing**: Split a fasta file at each sequence and calculate length:
   ```sh
   cat genome.fasta | farm.py --split-at-regex="^>(\S+)" "wc -c"
   ```

3. **Chunk by Sequence Count**: Split a fasta file at every 10 sequences:
   ```sh
   cat genome.fasta | farm.py --split-at-regex="^>(\S+)" --chunk-size=10 "wc -c"
   ```

## Classes and Functions

### Chunk Iterator Functions

The script includes various functions to handle chunking of the input data:

- **`chunk_iterator_lines`**: Splits input data by a specific number of lines.
- **`chunk_iterator_column`**: Splits input based on values in a specified column.
- **`chunk_iterator_regex_group`**: Groups input lines based on a regex match.
- **`chunk_iterator_regex_split`**: Splits input whenever a regex matches.

These functions yield filenames containing the chunks, which are then processed independently.

### Mapper Classes

Mappers are used to rename or manage output IDs:
- **`MapperGlobal`**: Maps IDs globally with a given pattern.
- **`MapperLocal`**: Maps IDs locally, associating a unique ID to each key within a specific file.
- **`MapperEmpty`**: Passes through the original ID without modification.

### ResultBuilder Classes

The `ResultBuilder` classes handle the output from processed chunks:

- **`ResultBuilder`**: Merges results from table-formatted output.
- **`ResultBuilderFasta`**: Handles results from fasta-formatted output.
- **`ResultBuilderBinary`**: Concatenates binary output files.
- **`ResultBuilderCopies`**: Creates indexed copies of output files.
- **`ResultBuilderLog`**: Aggregates log files from multiple jobs.

### build_command Function
```python
def build_command(data):
    # Function code...
```
This function constructs the shell command to execute each chunk, including logging and managing temporary directories. It replaces placeholders (e.g., `%STDIN%` and `%DIR%`) with appropriate values.

### hasFinished Function
```python
def hasFinished(retcode, filename, output_tag, logfile):
    # Function code...
```
The `hasFinished()` function checks if a run has finished successfully by inspecting the return code and looking for a completion tag in the log file.

### get_option_parser Function
```python
def get_option_parser():
    # Function code...
```
The `get_option_parser()` function sets up and returns an argument parser with various command-line options for specifying how the input should be split, how output should be handled, and other behaviours (e.g., memory requirements, logging).

### main Function
```python
def main(argv=None):
    # Function code...
```
The `main()` function is the entry point of the script. It parses the command-line arguments, prepares the input data for processing, builds commands for each chunk, runs these commands (using a specified method: multiprocessing, threading, etc.), and finally collects and processes the results.

- **Key Steps in Main**:
  - **Argument Parsing**: Uses `get_option_parser()` to parse command-line options.
  - **Chunking Input**: Chooses the appropriate chunking method and splits the input accordingly.
  - **Job Execution**: Executes each chunk using the specified method (e.g., `multiprocessing`, `threads`, or `drmaa` for cluster management).
  - **Result Collection**: Collects and combines the results using `ResultBuilder` classes.
  - **Error Handling**: Logs any failed jobs and, if all jobs succeed, cleans up the temporary directories.

## Conclusion

The `farm.py` script is a powerful utility for dividing data streams into smaller tasks, running them in parallel on a cluster, and collating the output. It is well-suited for "embarrassingly parallel" tasks, such as processing large tabular datasets or fasta files, and integrates seamlessly with cluster environments for distributed computation.

With flexible options for chunking data, managing output, and handling errors, this script is a useful tool for bioinformatics pipelines and other data-intensive workflows.

