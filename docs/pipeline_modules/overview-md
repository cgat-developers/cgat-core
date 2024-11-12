# Pipeline Modules Overview

CGAT-core provides a comprehensive set of modules to facilitate the creation and management of data processing pipelines. These modules offer various functionalities, from pipeline control and execution to database management and file handling.

## Available Modules

1. [Control](control.md): Manages the overall pipeline execution flow.
2. [Database](database.md): Handles database operations and uploads.
3. [Files](files.md): Provides utilities for file management and temporary file handling.
4. [Cluster](cluster.md): Manages job submission and execution on compute clusters.
5. [Execution](execution.md): Handles task execution and logging.
6. [Utils](utils.md): Offers various utility functions for pipeline operations.
7. [Parameters](parameters.md): Manages pipeline parameters and configuration.

## Integration with Ruffus

CGAT-core builds upon the Ruffus pipeline library, extending its functionality and providing additional features. It includes the following Ruffus decorators:

- `@transform`
- `@merge`
- `@split`
- `@originate`
- `@follows`
- `@suffix`

These decorators can be used to define pipeline tasks and their dependencies.

## S3 Integration

CGAT-core also provides S3-aware decorators and functions for seamless integration with AWS S3:

- `@s3_transform`
- `@s3_merge`
- `@s3_split`
- `@s3_originate`
- `@s3_follows`

For more information on working with S3, see the [S3 Integration](../s3_integration/s3_pipeline.md) section.

By leveraging these modules and decorators, you can build powerful, scalable, and efficient data processing pipelines using CGAT-core.