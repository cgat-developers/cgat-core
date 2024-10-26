"""Test cases for the cgatcore.iotools module."""

import os
import shutil
import tempfile
import time
import cgatcore.iotools as iotools
import pytest


@pytest.fixture
def temp_file():
    """Fixture to create and clean up a temporary file."""
    tempdir = tempfile.mkdtemp()
    filename = os.path.join(tempdir, "test_iotools_touch_file.txt")
    yield filename
    shutil.rmtree(tempdir)


@pytest.fixture
def temp_file_compressed():
    """Fixture to create and clean up a temporary compressed file."""
    tempdir = tempfile.mkdtemp()
    filename = os.path.join(tempdir, "test_iotools_touch_file.txt.gz")
    yield filename
    shutil.rmtree(tempdir)


def test_touch_file_creates_empty_file(temp_file):
    assert not os.path.exists(temp_file)
    iotools.touch_file(temp_file)
    assert os.path.exists(temp_file)
    if temp_file.endswith(".gz"):
        assert not iotools.is_empty(temp_file)
    else:
        assert iotools.is_empty(temp_file)

    with iotools.open_file(temp_file) as inf:
        data = inf.read()
    assert len(data) == 0


def test_touch_file_updates_existing_file(temp_file):
    with iotools.open_file(temp_file, "w") as outf:
        outf.write("some data\n")
    created = os.stat(temp_file).st_mtime
    time.sleep(1)
    iotools.touch_file(temp_file)
    modified = os.stat(temp_file).st_mtime
    assert modified > created
    with iotools.open_file(temp_file) as inf:
        data = inf.read()
    assert data == "some data\n"


def test_touch_file_compressed_creates_empty_file(temp_file_compressed):
    assert not os.path.exists(temp_file_compressed)
    iotools.touch_file(temp_file_compressed)
    assert os.path.exists(temp_file_compressed)
    if temp_file_compressed.endswith(".gz"):
        assert not iotools.is_empty(temp_file_compressed)
    else:
        assert iotools.is_empty(temp_file_compressed)

    with iotools.open_file(temp_file_compressed) as inf:
        data = inf.read()
    assert len(data) == 0


def test_is_nested_with_dict():
    test_data = {
        "key1": {
            "nested_key1": "nested_key1"
        }
    }
    assert iotools.is_nested(test_data)


def test_nested_iter_with_dict_of_dicts():
    test_data = {
        "key1": {
            "nested_key1": "nested_key1"
        }
    }
    list(iotools.nested_iter(test_data))


def test_nested_iter_with_list_of_dicts():
    test_data = [
        {
            "nested_key1": "nested_key1"
        }
    ]
    list(iotools.nested_iter(test_data))
