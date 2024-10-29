# cgatcore/remote/__init__.py

import os
from abc import abstractmethod
from .abstract import AbstractRemoteObject
from .file_handler import S3Pipeline, S3Mapper, s3_path_to_local, suffix

# Create an instance of S3Mapper
s3_mapper = S3Mapper()

# Conditional import for testing
if os.getenv("PYTEST_CURRENT_TEST"):
    from tests.mocks import MockS3RemoteObject
    from unittest.mock import patch
    with patch("cgatcore.remote.aws.S3RemoteObject", new=MockS3RemoteObject):
        s3_mapper = S3Mapper()  # Use MockS3RemoteObject during tests

__all__ = ['S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix']
