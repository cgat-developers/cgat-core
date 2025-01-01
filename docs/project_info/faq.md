# Frequently Asked Questions (FAQ)

## General Questions

### What is CGAT-core?
CGAT-core is a Python framework for building and executing computational pipelines, particularly suited for bioinformatics and data analysis workflows. It provides robust support for cluster environments and cloud integration.

### Why use CGAT-core instead of other workflow managers?
- Built on the proven Ruffus framework
- Native support for multiple cluster platforms
- Integrated cloud storage support
- Extensive resource management capabilities
- Container support for reproducibility

### What platforms are supported?
- **Operating Systems**: Linux, macOS
- **Cluster Systems**: SLURM, SGE, PBS/Torque
- **Cloud Platforms**: AWS S3, Google Cloud, Azure

## Installation

### What are the system requirements?
- Python 3.7 or later
- Compatible cluster system (optional)
- Sufficient disk space for pipeline execution
- Memory requirements depend on specific pipeline needs

### How do I install CGAT-core?
```bash
# Using pip
pip install cgatcore

# Using conda
conda install -c bioconda cgatcore
```

### How do I verify my installation?
```bash
# Check installation
cgat --help

# Run test pipeline
cgat showcase make all
```

## Pipeline Development

### How do I create a new pipeline?
1. Create a new Python file
2. Import required modules
3. Define pipeline tasks using decorators
4. Add command-line interface

Example:
```python
from cgatcore import pipeline as P

@P.transform("*.txt", suffix(".txt"), ".processed")
def process_files(infile, outfile):
    # Processing logic here
    pass

if __name__ == "__main__":
    P.main()
```

### How do I configure cluster settings?
Create a `.cgat.yml` file in your home directory:
```yaml
cluster:
    queue_manager: slurm
    queue: main
    memory_resource: mem
    memory_default: 4G
```

## Troubleshooting

### Common Issues

#### Pipeline fails with memory error
- Check available memory with `free -h`
- Adjust memory settings in `.cgat.yml`
- Use `job_memory` parameter in task decorators

#### Cluster jobs not starting
- Verify cluster configuration
- Check queue availability
- Ensure proper permissions

#### Temporary files filling disk space
- Set appropriate tmpdir in configuration
- Enable automatic cleanup
- Monitor disk usage during execution

### Getting Help

1. Check documentation at [readthedocs](https://cgat-core.readthedocs.io/)
2. Search [GitHub Issues](https://github.com/cgat-developers/cgat-core/issues)
3. Join our community discussions
4. Create a new issue with:
   - System information
   - Error messages
   - Minimal reproducible example

## Best Practices

### Resource Management
- Always specify memory requirements
- Use appropriate number of threads
- Clean up temporary files

### Error Handling
- Implement proper error checking
- Use logging effectively
- Handle cleanup in failure cases

### Performance
- Optimize chunk sizes
- Monitor resource usage
- Use appropriate cluster settings

For more detailed information, refer to our [documentation](https://cgat-core.readthedocs.io/).