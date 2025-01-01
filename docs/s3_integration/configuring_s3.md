# Configuring AWS S3 Integration

CGAT-core provides native support for working with AWS S3 storage, allowing pipelines to read from and write to S3 buckets seamlessly.

## Prerequisites

1. **AWS Account Setup**
   - Active AWS account
   - IAM user with S3 access
   - Access key and secret key

2. **Required Packages**
   ```bash
   pip install boto3
   pip install cgatcore[s3]
   ```

## Configuration

### 1. AWS Credentials

Configure AWS credentials using one of these methods:

#### a. Environment Variables
```bash
export AWS_ACCESS_KEY_ID='your_access_key'
export AWS_SECRET_ACCESS_KEY='your_secret_key'
export AWS_DEFAULT_REGION='your_region'
```

#### b. AWS Credentials File
Create `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
region = your_region
```

#### c. Pipeline Configuration
In `pipeline.yml`:
```yaml
s3:
    access_key: your_access_key
    secret_key: your_secret_key
    region: your_region
    bucket: your_default_bucket
```

### 2. S3 Pipeline Configuration

Configure S3-specific settings in `pipeline.yml`:
```yaml
s3:
    # Default bucket for pipeline
    bucket: my-pipeline-bucket
    
    # Temporary directory for downloaded files
    local_tmpdir: /tmp/s3_cache
    
    # File transfer settings
    transfer:
        multipart_threshold: 8388608  # 8MB
        max_concurrency: 10
        multipart_chunksize: 8388608  # 8MB
    
    # Retry configuration
    retry:
        max_attempts: 5
        mode: standard
```

## Usage Examples

### 1. Basic S3 Operations

#### Reading from S3
```python
from cgatcore import pipeline as P

@P.s3_transform("s3://bucket/input.txt", suffix(".txt"), ".processed")
def process_s3_file(infile, outfile):
    """Process a file from S3."""
    statement = """
    cat %(infile)s | process_data > %(outfile)s
    """
    P.run(statement)
```

#### Writing to S3
```python
@P.s3_transform("input.txt", suffix(".txt"), 
                "s3://bucket/output.processed")
def write_to_s3(infile, outfile):
    """Write results to S3."""
    statement = """
    process_data %(infile)s > %(outfile)s
    """
    P.run(statement)
```

### 2. Advanced Operations

#### Working with Multiple Files
```python
@P.s3_merge(["s3://bucket/*.txt"], "s3://bucket/merged.txt")
def merge_s3_files(infiles, outfile):
    """Merge multiple S3 files."""
    statement = """
    cat %(infiles)s > %(outfile)s
    """
    P.run(statement)
```

#### Conditional S3 Usage
```python
@P.transform("*.txt", suffix(".txt"), 
             P.s3_path_if("use_s3", ".processed"))
def conditional_s3(infile, outfile):
    """Use S3 based on configuration."""
    statement = """
    process_data %(infile)s > %(outfile)s
    """
    P.run(statement)
```

## Best Practices

### 1. Performance Optimization

- **Batch Operations**: Group small files for transfers
- **Multipart Uploads**: Configure for large files
- **Concurrent Transfers**: Set appropriate concurrency
- **Local Caching**: Use temporary directory efficiently

```yaml
s3:
    transfer:
        multipart_threshold: 100_000_000  # 100MB
        max_concurrency: 20
        multipart_chunksize: 10_000_000  # 10MB
    local_tmpdir: /fast/local/disk/s3_cache
```

### 2. Cost Management

- **Data Transfer**: Minimize cross-region transfers
- **Storage Classes**: Use appropriate storage tiers
- **Cleanup**: Remove temporary files
- **Lifecycle Rules**: Configure bucket lifecycle

### 3. Error Handling

```python
@P.s3_transform("s3://bucket/input.txt", suffix(".txt"), ".processed")
def robust_s3_processing(infile, outfile):
    """Handle S3 operations with proper error checking."""
    try:
        statement = """
        process_data %(infile)s > %(outfile)s
        """
        P.run(statement)
    except P.S3Error as e:
        L.error("S3 operation failed: %s" % e)
        raise
    finally:
        # Clean up local temporary files
        P.cleanup_tmpdir()
```

## Troubleshooting

### Common Issues

1. **Access Denied**
   - Check AWS credentials
   - Verify IAM permissions
   - Ensure bucket policy allows access

2. **Transfer Failures**
   - Check network connectivity
   - Verify file permissions
   - Monitor transfer logs

3. **Performance Issues**
   - Adjust multipart settings
   - Check network bandwidth
   - Monitor memory usage

### Debugging

Enable detailed S3 logging:
```python
import logging
logging.getLogger('boto3').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.DEBUG)
```

## Security Considerations

1. **Credentials Management**
   - Use IAM roles when possible
   - Rotate access keys regularly
   - Never commit credentials

2. **Data Protection**
   - Enable bucket encryption
   - Use HTTPS endpoints
   - Configure appropriate bucket policies

3. **Access Control**
   - Implement least privilege
   - Use bucket policies
   - Enable access logging

For more examples of using S3 in your pipelines, see the [S3 Pipeline Examples](s3_pipeline.md#examples) section.
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)