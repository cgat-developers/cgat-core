# cgatcore/pipeline/__init__.py

"""pipeline.py - Tools for CGAT Ruffus Pipelines
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

# Import S3-related classes and functions
from cgatcore.remote.file_handler import S3Pipeline, S3Mapper, s3_path_to_local, suffix

# Create a global instance of S3Pipeline
s3_pipeline = S3Pipeline()

# Expose S3-aware decorators via the S3Pipeline instance
s3_transform = s3_pipeline.s3_transform
s3_merge = s3_pipeline.s3_merge
s3_split = s3_pipeline.s3_split
s3_originate = s3_pipeline.s3_originate
s3_follows = s3_pipeline.s3_follows

# Expose S3Mapper instance if needed elsewhere
s3_mapper = s3_pipeline.s3

# Expose S3 configuration function
configure_s3 = s3_pipeline.configure_s3

# Update __all__ to include both standard and S3-aware decorators and functions
__all__ = [
    'transform', 'merge', 'split', 'originate', 'follows',
    's3_transform', 's3_merge', 's3_split', 's3_originate', 's3_follows',
    'S3Pipeline', 'S3Mapper', 's3_path_to_local', 'suffix',
    's3_mapper', 'configure_s3'
]

# Add a docstring for the module
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
