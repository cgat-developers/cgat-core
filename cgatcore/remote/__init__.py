# cgatcore/remote/__init__.py

import os
import importlib
from abc import abstractmethod
from .abstract import AbstractRemoteObject
from .file_handler import S3Pipeline, S3Mapper, s3_path_to_local, suffix
from unittest.mock import patch


# Helper function for loading the mock, only needed for testing
def load_mock_s3_remote():
    try:
        mocks_module = importlib.import_module("tests.mocks")
        return mocks_module.MockS3RemoteObject
    except ImportError:
        raise ImportError(
            "Failed to import `tests.mocks`. Ensure 'tests' and 'tests/mocks.py' exist at the correct path."
        )


# Initialize s3_mapper and conditionally replace S3RemoteObject during testing
s3_mapper = S3Mapper()

# Apply mock for S3RemoteObject in testing mode
if os.getenv("PYTEST_CURRENT_TEST"):
    try:
        MockS3RemoteObject = load_mock_s3_remote()
        with patch("cgatcore.remote.aws.S3RemoteObject", new=MockS3RemoteObject):
            s3_mapper = S3Mapper()  # Use MockS3RemoteObject during tests
    except ImportError as e:
        raise ImportError(
            f"MockS3RemoteObject could not be imported. Ensure 'tests/mocks.py' exists and is accessible."
        ) from e

__all__ = ['S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix']
