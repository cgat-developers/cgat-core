# Database Module

`database.py` - Database upload for cgatcore pipelines

=========================================================

## Reference

This module contains functions to facilitate data upload into a database using CGAT-core. It is particularly useful for integrating with cgatcore pipelines for automating and managing complex workflows.

## Import Statements

```python
import re
import os
import sqlite3
import sqlalchemy
from cgatcore import database as database
import cgatcore.experiment as E
from cgatcore.iotools import snip, touch_file
from cgatcore.pipeline.files import get_temp_file
from cgatcore.pipeline.execution import run
from cgatcore.pipeline.parameters import get_params
```

## Key Functions

### `tablequote(track)`

Quotes a track name to make it suitable as a table name.

```python
def tablequote(track):
    '''quote a track name such that it is suitable as a table name.'''
    return re.sub(r"[-(),\[\].]", "_", track)
```

### `to_table(outfile)`

Converts a filename from a load statement into a table name. Checks if the filename ends with `.load`, removes the suffix, and then quotes it.

Arguments:
- `outfile` (string): Filename ending in `.load`.

Returns:
- `tablename` (string): A suitable table name derived from the file.

```python
def to_table(outfile):
    '''convert a filename from a load statement into a table name.'''
    assert outfile.endswith(".load")
    name = os.path.basename(outfile[:-len(".load")])
    return tablequote(name)
```

### `build_load_statement(tablename, retry=True, options="")`

Builds a command line statement to upload data to the database via the `csv2db` script.

Arguments:
- `tablename` (string): Tablename for upload.
- `retry` (bool): If `True`, add the `--retry` option to `csv2db.py`.
- `options` (string): Command line options to be passed on to `csv2db.py`.

Returns:
- `string`: A command line statement for uploading data.

```python
def build_load_statement(tablename, retry=True, options=""):
    opts = []
    if retry:
        opts.append(" --retry ")
    params = get_params()
    opts.append("--database-url={}".format(params["database"]["url"]))
    db_options = " ".join(opts)
    load_statement = (
        "python -m cgatcore.csv2db {db_options} {options} --table={tablename}".format(**locals()))
    return load_statement
```

### `load(...)`

Imports data from a tab-separated file into the database.

Arguments:
- `infile` (string): Filename of the input data.
- `outfile` (string): Output filename containing logging information.
- Various additional arguments to control the loading behaviour.

Typical usage within Ruffus:

```python
@transform("*.tsv.gz", suffix(".tsv.gz"), ".load")
def loadData(infile, outfile):
    P.load(infile, outfile)
```

### `concatenate_and_load(...)`

Concatenates multiple tab-separated files and uploads the result to the database.

Arguments:
- `infiles` (list): List of input filenames.
- `outfile` (string): Output filename.
- Various additional arguments for concatenation and loading.

Typical usage within Ruffus:

```python
@merge("*.tsv.gz", ".load")
def loadData(infiles, outfile):
    P.concatenate_and_load(infiles, outfile)
```

### `merge_and_load(...)`

Merges multiple categorical tables and loads them into the database.

Arguments:
- `infiles` (list): List of input files.
- `outfile` (string): Output filename.
- Various additional arguments to control the merging and loading behaviour.

### `connect()`

Connects to the SQLite database used in the pipeline. Currently only implemented for SQLite databases.

Returns:
- `dbh`: A database handle.

### `create_view(...)`

Creates a database view for a list of tables by performing a join across them.

Arguments:
- `dbhandle`: Database handle.
- `tables`: List of tuples containing table names and fields to join.
- `tablename` (string): Name of the view or table to be created.
- `view_type` (string): Type of view, either `VIEW` or `TABLE`.

### `get_database_name()`

Returns the database name associated with the pipeline. Implemented for backwards compatibility.

## Utility Functions

These functions assist in interacting with a database in various ways:

- **`load_from_iterator(...)`**: Imports data from an iterator into a database.
- **`apsw_connect(dbname, modname="tsv")`**: Attempts to connect to an APSW database, creating a virtual table from a TSV file.

## Database Utility Functions

`database.py` - Database utility functions

===========================================

This module contains convenience functions to work with a relational database.

### `executewait(dbhandle, statement, regex_error="locked", retries=-1, wait=5)`

Repeatedly executes an SQL statement until it succeeds.

Arguments:
- `dbhandle`: A DB-API conform database handle.
- `statement`: SQL statement to execute.
- `regex_error`: Regex to match error messages to ignore.

Returns:
- `Cursor`: A cursor object for further database operations.

### `getColumnNames(dbhandle, table)`

Returns the column names of a table from the database.

### `getTables(dbhandle)`

Gets a list of tables in an SQLite database.

### `toTSV(dbhandle, outfile, statement, remove_none=True)`

Executes a statement and saves the result as a TSV file to disk.

### Database Interaction Functions

- **`connect(dbhandle=None, attach=None, url=None)`**: Attempts to connect to a database, returning a database handle.
- **`execute(queries, dbhandle=None, attach=False)`**: Executes one or more SQL statements against a database.
- **`fetch(query, dbhandle=None, attach=False)`**: Fetches all query results and returns them.
- **`fetch_with_names(query, dbhandle=None, attach=False)`**: Fetches query results and returns them as an array of row arrays, including field names.
- **`fetch_DataFrame(query, dbhandle=None, attach=False)`**: Fetches query results and returns them as a pandas DataFrame.
- **`write_DataFrame(dataframe, tablename, dbhandle=None, index=False, if_exists='replace')`**: Writes a pandas DataFrame to an SQLite database.

### Virtual Table Creation with APSW

- **`apsw_connect(dbname=None, modname="tsv")`**: Connects to an APSW database and creates a virtual table from a TSV file.
- **`_VirtualTable` and `_Table` classes**: Defines the structure and methods to support virtual tables in APSW.

## Notes and Recommendations

- As you continue to expand and develop the CGAT-core functionality, ensure to update the database module documentation accordingly.
- This module heavily utilises `csv2db` to facilitate data upload and management.
- Always consider potential SQL locking issues and use retry mechanisms where applicable.
