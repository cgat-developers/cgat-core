# cgatcore/pipeline/__init__.py

"""
pipeline.py - Tools for CGAT Ruffus Pipelines
=============================================

This module provides a comprehensive set of tools to facilitate the creation and management
of data processing pipelines using CGAT Ruffus. It includes functionalities for pipeline control,
logging, parameterization, task execution, database uploads, temporary file management, and
integration with AWS S3.
"""

# Import existing pipeline functionality
from cgatcore.pipeline.control import *
from cgatcore.pipeline.database import *
from cgatcore.pipeline.files import *
from cgatcore.pipeline.cluster import *
from cgatcore.pipeline.execution import *
from cgatcore.pipeline.utils import *
from cgatcore.pipeline.parameters import *

# Import original Ruffus decorators
from ruffus import (
    transform,
    merge,
    split,
    originate,
    follows
)

# Lazy-load S3-related classes and functions through the cgatcore instance
from cgatcore import cgatcore

# Helper function to access S3Pipeline instance lazily
def get_s3_pipeline():
    """Instantiate and return the S3Pipeline instance, lazy-loaded to avoid circular imports."""
    return cgatcore.remote.file_handler.S3Pipeline()

# Define S3-aware decorators as properties, accessed only when needed
s3_pipeline = None
def s3_transform(*args, **kwargs):
    global s3_pipeline
    if s3_pipeline is None:
        s3_pipeline = get_s3_pipeline()
    return s3_pipeline.s3_transform(*args, **kwargs)

def s3_merge(*args, **kwargs):
    global s3_pipeline
    if s3_pipeline is None:
        s3_pipeline = get_s3_pipeline()
    return s3_pipeline.s3_merge(*args, **kwargs)

def s3_split(*args, **kwargs):
    global s3_pipeline
    if s3_pipeline is None:
        s3_pipeline = get_s3_pipeline()
    return s3_pipeline.s3_split(*args, **kwargs)

def s3_originate(*args, **kwargs):
    global s3_pipeline
    if s3_pipeline is None:
        s3_pipeline = get_s3_pipeline()
    return s3_pipeline.s3_originate(*args, **kwargs)

def s3_follows(*args, **kwargs):
    global s3_pipeline
    if s3_pipeline is None:
        s3_pipeline = get_s3_pipeline()
    return s3_pipeline.s3_follows(*args, **kwargs)

# Expose S3Mapper and configuration function through lazy loading
def s3_mapper():
    return get_s3_pipeline().s3

def configure_s3(*args, **kwargs):
    return get_s3_pipeline().configure_s3(*args, **kwargs)

# Update __all__ to include both standard and S3-aware decorators and functions
__all__ = [
    'transform', 'merge', 'split', 'originate', 'follows',
    's3_transform', 's3_merge', 's3_split', 's3_originate', 's3_follows',
    'S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix',
    's3_mapper', 'configure_s3'
]

# Module docstring
__doc__ = """
This module provides pipeline functionality for cgat-core, including support for AWS S3.

It includes both standard Ruffus decorators and S3-aware decorators. The S3-aware decorators
can be used to seamlessly work with both local and S3-based files in your pipelines.

Example usage:

from cgatcore import pipeline as P

# Using standard Ruffus decorator (works as before)
@P.transform("input.txt", suffix(".txt"), ".processed")
def process_local_file(infile, outfile):
    # Your processing logic here
    pass

# Using S3-aware decorator
@P.s3_transform("s3://my-bucket/input.txt", suffix(".txt"), ".processed")
def process_s3_file(infile, outfile):
    # Your processing logic here
    pass

# Configure S3 credentials if needed
P.configure_s3(aws_access_key_id="YOUR_KEY", aws_secret_access_key="YOUR_SECRET")
"""
