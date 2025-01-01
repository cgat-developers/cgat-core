'''test_import - test importing all modules
===========================================

:Author: Adam Cribbs
:Release: $Id$
:Date: |today|
:Tags: Python

Purpose
-------

This script attempts to import all the python code in the cgat-core
repository.

Importing a script/module is a pre-requisite for building
documentation with sphinx. A script/module that can not be imported
will fail within sphinx.

'''
import os
import glob
import traceback
import importlib.util
import pytest
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# DIRECTORIES to examine
EXPRESSIONS = (
    ('FirstLevel', 'cgatcore/*.py'),
    ('SecondLevel', 'cgatcore/pipeline/*.py')
)

# Code to exclude
EXCLUDE = set()


def check_import(filename, outfile):
    """Attempt to import a module and handle errors."""
    module_name = os.path.splitext(os.path.basename(filename))[0]

    if module_name in EXCLUDE:
        return

    if os.path.exists(filename + "c"):
        os.remove(filename + "c")

    pyxfile = os.path.join(os.path.dirname(filename), "_") + module_name + "x"
    if os.path.exists(pyxfile):
        return

    try:
        spec = importlib.util.spec_from_file_location(module_name, filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except ImportError as msg:
        outfile.write(f"FAIL {module_name}\n{msg}\n")
        outfile.flush()
        traceback.print_exc(file=outfile)
        pytest.fail(f"{module_name} - ImportError: {msg}")
    except Exception as msg:
        outfile.write(f"FAIL {module_name}\n{msg}\n")
        outfile.flush()
        traceback.print_exc(file=outfile)
        pytest.fail(f"{module_name} - Exception: {msg}")


@pytest.mark.parametrize("label, expression", EXPRESSIONS)
def test_import(label, expression):
    """Test importing all modules in the specified expressions."""
    with open('test_import.log', 'a') as outfile:
        files = sorted(glob.glob(expression))

        for filename in files:
            if not os.path.isdir(filename):
                check_import(filename, outfile)
