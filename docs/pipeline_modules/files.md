# Files Module

`files.py` - Working with files in cgatcore pipelines

====================================================

## Reference

This module provides a collection of functions to facilitate file handling within cgatcore pipelines, including generating temporary files, temporary directories, and checking for required executables or scripts.

## Import Statements

```python
import os
import tempfile

import cgatcore.iotools as iotools
import cgatcore.experiment as E
from cgatcore.pipeline.parameters import get_params
```

## Key Functions

### `get_temp_file(dir=None, shared=False, suffix="", mode="w+", encoding="utf-8")`

Gets a temporary file. The file is created and opened in text mode by default (mode `w+`), with UTF-8 encoding.

Arguments:
- `dir` (string, optional): Directory of the temporary file. If not provided, defaults to the temporary directory defined in the global configuration.
- `shared` (bool, optional): If set to `True`, creates the file in a shared temporary location.
- `suffix` (string, optional): Suffix for the filename.
- `mode` (string, optional): File mode (e.g., `w+` for reading and writing). Defaults to `w+`.
- `encoding` (string, optional): Encoding for the file. Defaults to `utf-8`.

Returns:
- `file` (File object): A temporary file object that the caller must close and delete once it's no longer needed.

### `get_temp_filename(dir=None, shared=False, clear=True, suffix="")`

Returns a temporary filename. The file is created and then optionally deleted if `clear` is set to `True`.

Arguments:
- `dir` (string, optional): Directory for the temporary file. Defaults to the configuration's temporary directory.
- `shared` (bool, optional): If set to `True`, places the file in a shared temporary location.
- `clear` (bool, optional): If set to `True`, deletes the file after creation. Defaults to `True`.
- `suffix` (string, optional): Suffix for the filename.

Returns:
- `filename` (string): Absolute path to the temporary file.

### `get_temp_dir(dir=None, shared=False, clear=False)`

Creates and returns a temporary directory.

Arguments:
- `dir` (string, optional): Directory for the temporary directory. Defaults to the configuration's temporary directory.
- `shared` (bool, optional): If set to `True`, places the directory in a shared temporary location.
- `clear` (bool, optional): If set to `True`, removes the directory after creation.

Returns:
- `filename` (string): Absolute path to the temporary directory.

### `check_executables(filenames)`

Checks for the presence of required executables in the system's PATH.

Arguments:
- `filenames` (list of strings): List of executables to check for.

Raises:
- `ValueError`: If any executable is missing.

```python
def check_executables(filenames):
    missing = []
    for filename in filenames:
        if not iotools.which(filename):
            missing.append(filename)
    if missing:
        raise ValueError("missing executables: %s" % ",".join(missing))
```

### `check_scripts(filenames)`

Checks for the presence of specified scripts in the filesystem.

Arguments:
- `filenames` (list of strings): List of script filenames to check for.

Raises:
- `ValueError`: If any script is missing.

```python
def check_scripts(filenames):
    missing = []
    for filename in filenames:
        if not os.path.exists(filename):
            missing.append(filename)
    if missing:
        raise ValueError("missing scripts: %s" % ",".join(missing))
```

## Usage

These functions are useful for managing temporary files and ensuring all necessary executables or scripts are available when running Ruffus pipelines. They help maintain clean temporary storage and facilitate proper error checking to prevent missing dependencies.

## Notes

- Temporary files and directories are created in either a shared or a default temporary location, based on the provided arguments.
- The `get_temp_file` function provides a safe way to generate temporary files, with customisable directory, mode, and encoding options.
- Always handle temporary files and directories appropriately, ensuring they are deleted after use to avoid cluttering the filesystem.

As you continue to expand the functionality of CGAT-core, make sure to keep this module up to date with new helper functions or improvements.
