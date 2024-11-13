# run_function.py - Documentation

This document provides an overview of the `run_function.py` script, which is used to execute a function from a specified Python module remotely on a cluster. This utility allows functions from Python modules to be executed with user-defined parameters, input files, and output files, which is useful for running scripts as part of a computational pipeline.

## Table of Contents
- [Purpose](#purpose)
- [Usage](#usage)
- [Command Line Options](#command-line-options)
- [Workflow](#workflow)
  - [Parsing Options](#parsing-options)
  - [Module Importing](#module-importing)
  - [Function Invocation](#function-invocation)
- [Examples](#examples)
- [Error Handling](#error-handling)

## Purpose

The `run_function.py` script allows the execution of a specified function from a Python module with given input and output files, and other parameters. It can be used within a cluster environment to facilitate the remote execution of Python functions for parallel processing tasks or batch jobs.

## Usage

The script is typically used in conjunction with other pipeline tools. Here is an example:

```python
statement = """python %(scriptsdir)s/run_function.py \
              -p infile,outfile,additional_param1 \
              -m modulefile \
              -f function"""

P.run()
```

If the module to be used is within the `$PYTHONPATH`, it can be directly named (e.g., "pipeline" would refer to `pipeline.py`). The script is mainly tested for cases involving single input/output pairs.

## Command Line Options

- `-p, --params, --args`: Comma-separated list of additional parameter strings to be passed to the function.
- `-m, --module`: The full path to the module file from which the function will be imported.
- `-i, --input`: Input filename(s). Can be specified multiple times for multiple inputs.
- `-o, --output-section`: Output filename(s). Can be specified multiple times for multiple outputs.
- `-f, --function`: The name of the function to be executed from the specified module.

## Workflow

The workflow of the `run_function.py` script includes:

### Parsing Options

The script begins by parsing command-line arguments using `OptionParser`. The user must specify the module file and the function to run, along with any input and output files or additional parameters.

- **Mandatory Parameters**:
  - Module (`-m`) and Function (`-f`): Both must be specified.
- **Optional Parameters**:
  - Input and output files (`-i`, `-o`) are optional depending on the function requirements.
  - Additional parameters (`-p`) are optional but can be specified to provide custom arguments.

### Module Importing

After parsing the arguments, the script imports the specified module using `importlib`. This is necessary for dynamically loading the module that contains the function to be executed.

- **Adding Path**: If a full path is provided, the script appends that path to `sys.path` to ensure that Python can locate the module.
- **Module Import**: The `importlib.import_module()` function is used to import the module by its basename. The script also handles cases where the `.py` file extension is included.
- **Function Mapping**: The specified function is retrieved from the module using `getattr()`. If the function cannot be found, an error is raised, indicating the available functions within the module.

### Function Invocation

The function is invoked with the appropriate arguments, depending on which input, output, and parameter combinations are specified.

- **Handling Inputs and Outputs**:
  - The script manages cases where there are multiple or single input/output files, converting them into the expected formats for the function.
  - The `infiles` and `outfiles` arguments are handled to ensure they are passed appropriately, either as lists or as single file paths.
- **Parameter Parsing**: If additional parameters are provided, they are split into a list and passed as arguments to the function.
- **Function Call**: Based on the presence of inputs, outputs, and parameters, the function is called with different argument combinations.

## Examples

1. **Basic Function Execution**
   ```sh
   python run_function.py -m mymodule.py -f my_function -i input.txt -o output.txt -p param1,param2
   ```
   In this example, `my_function` from `mymodule.py` is executed with `input.txt` as the input file, `output.txt` as the output file, and `param1` and `param2` as additional parameters.

2. **Executing a Function without Input/Output**
   ```sh
   python run_function.py -m utilities.py -f simple_function -p param1,param2
   ```
   This runs `simple_function` from `utilities.py` with the specified parameters, but without any input or output files.

## Error Handling

- **Missing Module or Function**: If either the module (`-m`) or function (`-f`) options are missing, the script raises a `ValueError`, indicating that both must be provided.
- **Import Errors**: The script checks if the module exists at the specified location, and if the function is present within the module. It provides debug information (`sys.path`) to help locate import issues.
- **Attribute Errors**: If the specified function is not found in the module, an `AttributeError` is raised, and the script lists all available functions within the module.
- **Invalid Argument Combinations**: If the expected combination of input, output, and parameters is not provided, the script raises a `ValueError`, clarifying what is expected.

## Conclusion

The `run_function.py` script is a versatile tool for remotely executing functions from Python modules on a cluster. It supports input/output file handling and passing of additional parameters, making it suitable for use in complex computational pipelines. With its flexible argument parsing and dynamic module importing, it provides an easy way to run Python functions in distributed environments, aiding in the modularisation and parallelisation of tasks.

