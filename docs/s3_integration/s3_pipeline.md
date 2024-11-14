# S3 Pipeline

The `S3Pipeline` class is part of the integration with AWS S3, enabling seamless data handling in CGAT pipelines that use both local files and S3 storage. This is particularly useful when working with large datasets that are better managed in cloud storage or when collaborating across multiple locations.

## Overview

`S3Pipeline` provides the following functionalities:

- Integration of AWS S3 into CGAT pipeline workflows
- Lazy-loading of S3-specific classes and functions to avoid circular dependencies
- Facilitating operations on files that reside on S3, making it possible to apply transformations and merges without copying everything locally

### Example Usage

The `S3Pipeline` class can be accessed through the `get_s3_pipeline()` function, which returns an instance that is lazy-loaded to prevent issues related to circular imports. Below is an example of how to use it:

```python
from cgatcore.pipeline import get_s3_pipeline

# Instantiate the S3 pipeline
s3_pipeline = get_s3_pipeline()

# Use methods from s3_pipeline as needed
s3_pipeline.s3_transform(...)
```

### Building a Function Using `S3Pipeline`

To build a function that utilises `S3Pipeline`, you can follow a few simple steps. Below is a guide on how to create a function that uses the `s3_transform` method to process data from S3:

1. **Import the required modules**: First, import `get_s3_pipeline` from `cgatcore.pipeline`.
2. **Instantiate the pipeline**: Use `get_s3_pipeline()` to create an instance of `S3Pipeline`.
3. **Define your function**: Use the S3-aware methods like `s3_transform()` to perform the desired operations on your S3 files.

#### Example Function

```python
from cgatcore.pipeline import get_s3_pipeline

# Instantiate the S3 pipeline
s3_pipeline = get_s3_pipeline()

# Define a function that uses s3_transform
def process_s3_data(input_s3_path, output_s3_path):
    @s3_pipeline.s3_transform(input_s3_path, suffix(".txt"), output_s3_path)
    def transform_data(infile, outfile):
        # Add your processing logic here
        with open(infile, 'r') as fin:
            data = fin.read()
            # Example transformation
            processed_data = data.upper()
        with open(outfile, 'w') as fout:
            fout.write(processed_data)

    # Run the transformation
    transform_data()
```

### Methods in `S3Pipeline`

- **`s3_transform(*args, **kwargs)`**: Perform a transformation on data stored in S3, similar to Ruffus `transform()` but adapted for S3 files.
- **`s3_merge(*args, **kwargs)`**: Merge multiple input files into one, allowing the files to be located on S3.
- **`s3_split(*args, **kwargs)`**: Split input data into smaller chunks, enabling parallel processing, even when the input resides on S3.
- **`s3_originate(*args, **kwargs)`**: Create new files directly in S3.
- **`s3_follows(*args, **kwargs)`**: Indicate a dependency on another task, ensuring correct task ordering even for S3 files.

These methods are intended to be directly equivalent to standard Ruffus methods, allowing pipelines to easily mix and match S3-based and local operations.

## Why Use `S3Pipeline`?

- **Scalable Data Management**: Keeps large datasets in the cloud, reducing local storage requirements.
- **Seamless Integration**: Provides a drop-in replacement for standard decorators, enabling hybrid workflows involving both local and cloud files.
- **Lazy Loading**: Optimised to initialise S3 components only when they are needed, minimising overhead and avoiding unnecessary dependencies.

