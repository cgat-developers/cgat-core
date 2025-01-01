# CGAT-core Documentation

![Licence](https://img.shields.io/github/license/cgat-developers/cgat-core.svg)
![Conda](https://img.shields.io/conda/v/bioconda/cgatcore.svg)
![Build Status](https://github.com/cgat-developers/cgat-core/actions/workflows/cgatcore_python.yml/badge.svg)

Welcome to the CGAT-core documentation! CGAT-core is a powerful Python framework for building and executing computational pipelines, with robust support for cluster environments and cloud integration.

## Key Features

- **Pipeline Management**: Build and execute complex computational workflows using Ruffus
- **Cluster Integration**: Support for multiple cluster platforms (SLURM, SGE, PBS/Torque)
- **Cloud Support**: Native integration with AWS S3, Google Cloud, and Azure
- **Resource Management**: Intelligent handling of compute resources and job distribution
- **Container Support**: Execute pipeline tasks in containers for reproducibility

## Getting Started

1. [Installation Guide](getting_started/installation.md)
   - System requirements
   - Installation methods
   - Verification steps

2. [Tutorial](getting_started/tutorial.md)
   - Basic pipeline concepts
   - Running your first pipeline
   - Troubleshooting tips

3. [Examples](getting_started/examples.md)
   - Common use cases
   - Pipeline patterns
   - Best practices

## Core Components

### Pipeline Development

- [Writing Workflows](defining_workflow/writing_workflows.md): Create custom pipeline workflows
- [Run Parameters](getting_started/run_parameters.md): Configure pipeline execution
- [Pipeline Modules](pipeline_modules/overview.md): Core pipeline components

### Execution Environments

- [Cluster Configuration](pipeline_modules/cluster.md): Set up cluster execution
- [Container Support](container/whole_pipeline.md): Run pipelines in containers
- [Cloud Integration](s3_integration/configuring_s3.md): Work with cloud storage

### Advanced Features

- [Parameter Management](pipeline_modules/parameters.md): Handle pipeline parameters
- [Execution Control](pipeline_modules/execution.md): Manage task execution
- [Database Integration](pipeline_modules/database.md): Work with databases

## Project Information

- [How to Contribute](project_info/how_to_contribute.md)
- [Citations](project_info/citations.md)
- [License](project_info/license.md)
- [FAQ](project_info/faq.md)

## Additional Resources

- [API Documentation](function_doc/pipeline.md)
- [GitHub Repository](https://github.com/cgat-developers/cgat-core)
- [Issue Tracker](https://github.com/cgat-developers/cgat-core/issues)

## Need Help?

If you need help or have questions:

1. Check our [FAQ](project_info/faq.md)
2. Search existing [GitHub Issues](https://github.com/cgat-developers/cgat-core/issues)
3. Create a new issue if your problem isn't already addressed

## Overview

CGAT-core has been continuously developed over the past decade to serve as a Next Generation Sequencing (NGS) workflow management system. By combining CGAT-core with [CGAT-apps](https://github.com/cgat-developers/cgat-apps), users can create diverse computational workflows. For a practical demonstration, refer to the [cgat-showcase](https://github.com/cgat-developers/cgat-showcase), which features a simple RNA-seq pipeline.

For advanced usage examples, explore the [cgat-flow](https://github.com/cgat-developers/cgat-flow) repository, which contains production-ready pipelines for automating NGS data analysis. Note that it is under active development and may require additional software dependencies.

## Citation

If you use CGAT-core, please cite our publication in F1000 Research:

**Cribbs AP, Luna-Valero S, George C et al. CGAT-core: a python framework for building scalable, reproducible computational biology workflows [version 1; peer review: 1 approved, 1 approved with reservations].**  
F1000Research 2019, 8:377  
[https://doi.org/10.12688/f1000research.18674.1](https://doi.org/10.12688/f1000research.18674.1)

## Support

- For frequently asked questions, visit the [FAQ](project_info/faq.md).
- To report bugs or issues, raise an issue on our [GitHub repository](https://github.com/cgat-developers/cgat-core).
- To contribute, see the [contributing guidelines](project_info/contributing.md) and refer to the [GitHub source code](https://github.com/cgat-developers/cgat-core).

## Example Workflows

### cgat-showcase
A simple example of workflow development using CGAT-core. Visit the [GitHub page](https://github.com/cgat-developers/cgat-showcase) or view the [documentation](https://cgat-showcase.readthedocs.io/en/latest/).

### cgat-flow
This repository demonstrates CGAT-core's flexibility through fully tested production pipelines. For details on usage and installation, see the [GitHub page](https://github.com/cgat-developers/cgat-flow).

### Single-Cell RNA-seq
- **Cribbs Lab**: Uses CGAT-core for pseudoalignment pipelines in single-cell [Drop-seq](https://github.com/Acribbs/single-cell) methods.
- **Sansom Lab**: Develops single-cell sequencing analysis workflows using the CGAT-core workflow engine ([TenX workflows](https://github.com/sansomlab/tenx)).

## Pipeline Modules Overview

CGAT-core provides a comprehensive set of modules to facilitate the creation and management of data processing pipelines. These modules offer various functionalities, from pipeline control and execution to database management and file handling.

### Available Modules

1. [Control](pipeline_modules/control.md): Manages the overall pipeline execution flow.
2. [Database](pipeline_modules/database.md): Handles database operations and uploads.
3. [Files](pipeline_modules/files.md): Provides utilities for file management and temporary file handling.
4. [Cluster](pipeline_modules/cluster.md): Manages job submission and execution on compute clusters.
5. [Execution](pipeline_modules/execution.md): Handles task execution and logging.
6. [Utils](pipeline_modules/utils.md): Offers various utility functions for pipeline operations.
7. [Parameters](pipeline_modules/parameters.md): Manages pipeline parameters and configuration.

### Integration with Ruffus

CGAT-core builds upon the Ruffus pipeline library, extending its functionality and providing additional features. It includes the following Ruffus decorators:

- `@transform`
- `@merge`
- `@split`
- `@originate`
- `@follows`
- `@suffix`

These decorators can be used to define pipeline tasks and their dependencies.

### S3 Integration

CGAT-core also provides S3-aware decorators and functions for seamless integration with AWS S3:

- `@s3_transform`
- `@s3_merge`
- `@s3_split`
- `@s3_originate`
- `@s3_follows`

For more information on working with S3, see the [S3 Integration](s3_integration/s3_pipeline.md) section.

By leveraging these modules and decorators, you can build powerful, scalable, and efficient data processing pipelines using CGAT-core.

---

## Quick Links

- [Getting Started](getting_started/installation.md)
- [Building a Workflow](defining_workflow/writing_workflow.md)
- [Pipeline Modules Overview](pipeline_modules/overview.md)
- [S3 Integration](s3_integration/s3_pipeline.md)
- [Working with Remote Files](remote/s3.md)
- [Core Functions](function_doc/pipeline.md)
- [Project Info](project_info/contributing.md)
