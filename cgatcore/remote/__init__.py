# cgatcore/remote/__init__.py

import os
import sys
import importlib
from abc import abstractmethod
from .abstract import AbstractRemoteObject
from .file_handler import S3Pipeline, S3Mapper, s3_path_to_local, suffix
from unittest.mock import patch


# Define the path to `tests` and ensure it's added to `sys.path`
tests_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../tests"))
if os.path.isdir(tests_path) and tests_path not in sys.path:
    sys.path.insert(0, tests_path)
print(f"sys.path after adding tests path: {sys.path}")


def load_mock_s3_remote():
    """Dynamically load MockS3RemoteObject from tests.mocks."""
    try:
        # Try importing the mocks module explicitly
        mocks_module = importlib.import_module("mocks")
        return getattr(mocks_module, "MockS3RemoteObject")
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(
            f"Failed to import `tests.mocks`. Ensure 'tests' and 'tests/mocks.py' exist at: {tests_path}\n"
            f"sys.path: {sys.path}"
        ) from e


# Check if running in a test environment and load MockS3RemoteObject if so
if os.getenv("PYTEST_CURRENT_TEST"):
    MockS3RemoteObject = load_mock_s3_remote()
    # Patch S3RemoteObject with the mock version for testing
    with patch("cgatcore.remote.aws.S3RemoteObject", new=MockS3RemoteObject):
        s3_mapper = S3Mapper()  # Use MockS3RemoteObject during tests

__all__ = ['S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix']
