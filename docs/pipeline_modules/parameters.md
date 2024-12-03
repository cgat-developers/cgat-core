# Parameter handling for cgatcore Pipelines

This document provides an overview of the `parameters.py` module used in cgatcore pipelines to handle configuration and parameter management. It includes functions for loading, validating, and handling parameters, as well as managing global configurations. This module is essential for customising and controlling cgatcore pipelines' behaviour, allowing the user to flexibly specify parameters via configuration files, command-line arguments, or hard-coded defaults.

## Table of Contents
- [Overview](#overview)
- [Global Constants and Initial Setup](#global-constants-and-initial-setup)
- [Functions Overview](#functions-overview)
  - [get_logger Function](#get_logger-function)
  - [get_parameters Function](#get_parameters-function)
  - [config_to_dictionary Function](#config_to_dictionary-function)
  - [nested_update Function](#nested_update-function)
  - [input_validation Function](#input_validation-function)
  - [match_parameter Function](#match_parameter-function)
  - [substitute_parameters Function](#substitute_parameters-function)
  - [as_list Function](#as_list-function)
  - [is_true Function](#is_true-function)
  - [check_parameter Function](#check_parameter-function)
  - [get_params Function](#get_params-function)
  - [get_parameters_as_namedtuple Function](#get_parameters_as_namedtuple-function)
  - [get_param_section Function](#get_param_section-function)

## Overview
The `parameters.py` module is designed to facilitate the management of configuration values for cgatcore pipelines. The configuration values are read from a variety of sources, including YAML configuration files, hard-coded dictionaries, and user-specific configuration files. The module also provides tools for parameter interpolation, validation, and nested dictionary handling.

### Global Constants and Initial Setup
The module begins by defining some constants and setting up paths:
- `SCRIPTS_ROOT_DIR` and `SCRIPTS_SCRIPTS_DIR`: Defines the root directory of scripts used within the pipeline.
- `HAVE_INITIALIZED`: A boolean variable used to indicate if the global parameters have been loaded.
- `PARAMS`: A global dictionary for parameter interpolation. This dictionary can be switched between `defaultdict` and standard dictionary behaviour to facilitate handling missing parameters.

### Functions Overview

#### get_logger Function
```python
def get_logger():
    return logging.getLogger("cgatcore.pipeline")
```
This function returns a logger instance for use in the pipeline, allowing consistent logging across the module.

#### get_parameters Function
```python
def get_parameters(filenames=None, defaults=None, site_ini=True, user=True, only_import=None):
    # Function code...
```
The `get_parameters` function reads one or more configuration files to build the global `PARAMS` dictionary. It can read from various configuration files (e.g., `pipeline.yml`, `cgat.yml`), and merge configurations from user, site-specific, and default sources.

- **Arguments**:
  - `filenames (list or str)`: A list of filenames for configuration files.
  - `defaults (dict)`: A dictionary of default values.
  - `site_ini (bool)`: If `True`, configuration files from `/etc/cgat/pipeline.yml` are also read.
  - `user (bool)`: If `True`, reads configuration from a user's home directory.
  - `only_import (bool)`: If set, the parameter dictionary will default to a collection type.

- **Returns**:
  - `dict`: A global configuration dictionary (`PARAMS`).

#### config_to_dictionary Function
```python
def config_to_dictionary(config):
    # Function code...
```
This function converts the contents of a `ConfigParser` object into a dictionary. Section names are prefixed with an underscore for clarity.

- **Returns**:
  - `dict`: A dictionary containing all configuration values, with nested sections appropriately handled.

#### nested_update Function
```python
def nested_update(old, new):
    # Function code...
```
The `nested_update` function updates nested dictionaries. If both `old[x]` and `new[x]` are dictionaries, they are recursively merged; otherwise, `old[x]` is updated with `new[x]`.

#### input_validation Function
```python
def input_validation(PARAMS, pipeline_script=""):
    # Function code...
```
The `input_validation` function inspects the `PARAMS` dictionary to check for problematic values, such as missing or placeholder inputs.

- **Validations**:
  - Checks for missing parameters (`?` placeholders).
  - Ensures that all required tools are available on the system PATH.
  - Verifies input file paths are readable.

#### match_parameter Function
```python
def match_parameter(param):
    # Function code...
```
This function attempts to find an exact or prefix match in the global `PARAMS` dictionary for the given parameter. If no match is found, a `KeyError` is raised.

- **Returns**:
  - `str`: The full name of the parameter if found.

#### substitute_parameters Function
```python
def substitute_parameters(**kwargs):
    # Function code...
```
This function returns a dictionary of parameter values for a specific task. It substitutes global parameter values and task-specific configuration values.

- **Example**:
  - If `PARAMS` has `"sample1.bam.gz_tophat_threads": 6` and `outfile = "sample1.bam.gz"`, it returns `{ "tophat_threads": 6 }`.

#### as_list Function
```python
def as_list(value):
    # Function code...
```
This function converts a given value to a list. If the value is a comma-separated string, it splits the string into a list.

- **Returns**:
  - `list`: The input value as a list.

#### is_true Function
```python
def is_true(param, **kwargs):
    # Function code...
```
This function checks if a parameter has a truthy value. Values like `0`, `''`, `false`, and `False` are considered as `False`.

- **Returns**:
  - `bool`: Whether the parameter is truthy or not.

#### check_parameter Function
```python
def check_parameter(param):
    # Function code...
```
The `check_parameter` function checks if the given parameter is set in the global `PARAMS` dictionary. If it is not set, a `ValueError` is raised.

#### get_params Function
```python
def get_params():
    # Function code...
```
This function returns a handle to the global `PARAMS` dictionary.

#### get_parameters_as_namedtuple Function
```python
def get_parameters_as_namedtuple(*args, **kwargs):
    # Function code...
```
The `get_parameters_as_namedtuple` function returns the `PARAMS` dictionary as a namedtuple, allowing for more convenient and attribute-based access to parameters.

#### get_param_section Function
```python
def get_param_section(section):
    # Function code...
```
This function returns all configuration values within a specific section of the `PARAMS` dictionary. Sections are defined by common prefixes.

- **Returns**:
  - `list`: A list of tuples containing section-specific parameters.

### Summary
The `parameters.py` module is designed to facilitate flexible and powerful parameter management for cgatcore pipelines. The functions provided allow for seamless integration of configuration from multiple sources, validation, and management of parameters, while also offering tools for introspection and nested dictionary handling. These utilities help create more robust and maintainable cgatcore pipelines, allowing for greater customisation and scalability.

