# cgatcore/remote/__init__.py

import os
import sys
from abc import abstractmethod


class AbstractRemoteObject():
    '''This is an abstract class that all RemoteObjects will
       inherit from. This is an abstract class to rigidly define
       the abstract methods of this RemoteObject class'''

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def upload(self):
        pass

    @abstractmethod
    def delete_file(self):
        pass


# Import S3-specific functionality
try:
    from .file_handler import (
        s3_transform,
        s3_merge,
        s3_split,
        s3_originate,
        s3_follows,
        S3Mapper,
        s3_aware
    )
except ImportError as e:
    import warnings

    warnings.warn(f"Failed to import S3 functionality from file_handler: {str(e)}. S3 features will be unavailable.")

    # If the file_handler module is not available, create dummy functions
    def dummy_decorator(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    s3_transform = s3_merge = s3_split = s3_originate = s3_follows = dummy_decorator
    s3_aware = lambda func: func

    class S3Mapper:
        def __init__(self):
            pass

# Create an instance of S3Mapper
s3_mapper = S3Mapper()
