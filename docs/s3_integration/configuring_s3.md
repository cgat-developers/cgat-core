# Configuring S3 for Pipeline Execution

To integrate AWS S3 into your CGAT pipeline, you need to configure S3 access to facilitate file handling for reading and writing data. This document explains how to set up S3 configuration for the CGAT pipelines.

## Overview

`configure_s3()` is a utility function provided by the CGATcore pipeline tools to handle authentication and access to AWS S3. This function allows you to provide credentials, specify regions, and set up other configurations that enable seamless integration of S3 into your workflow.

### Basic Configuration

To get started, you will need to import and use the `configure_s3()` function. Here is a basic example:

```python
from cgatcore.pipeline import configure_s3

configure_s3(aws_access_key_id="YOUR_AWS_ACCESS_KEY", aws_secret_access_key="YOUR_AWS_SECRET_KEY")
```

### Configurable Parameters

- **`aws_access_key_id`**: Your AWS access key, used to authenticate and identify the user.
- **`aws_secret_access_key`**: Your secret key, corresponding to your access key.
- **`region_name`** (optional): AWS region where your S3 bucket is located. Defaults to the region set in your environment, if available.
- **`profile_name`** (optional): Name of the AWS profile to use if you have multiple profiles configured locally.

### Using AWS Profiles

If you have multiple AWS profiles configured locally, you can use the `profile_name` parameter to select the appropriate one without hardcoding the access keys in your code:

```python
configure_s3(profile_name="my-profile")
```

### Configuring Endpoints

To use custom endpoints, such as when working with MinIO or an AWS-compatible service:

```python
configure_s3(
    aws_access_key_id="YOUR_AWS_ACCESS_KEY",
    aws_secret_access_key="YOUR_AWS_SECRET_KEY",
    endpoint_url="https://custom-endpoint.com"
)
```

### Security Recommendations

1. **Environment Variables**: Use environment variables to set credentials securely rather than hardcoding them in your scripts. This avoids potential exposure of credentials:
   
   ```bash
   export AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
   export AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY
   ```

2. **AWS IAM Roles**: If you are running the pipeline on AWS infrastructure (such as EC2 instances), it's recommended to use IAM roles. These roles provide temporary security credentials that are automatically rotated by AWS.

### Example Pipeline Integration

After configuring S3, you can seamlessly use the S3-aware methods within your pipeline. Below is an example:

```python
from cgatcore.pipeline import get_s3_pipeline

# Configure S3 access
configure_s3(profile_name="my-profile")

# Instantiate the S3 pipeline
s3_pipeline = get_s3_pipeline()

# Use S3-aware methods in the pipeline
@s3_pipeline.s3_transform("s3://my-bucket/input.txt", suffix(".txt"), ".processed")
def process_s3_file(infile, outfile):
    # Processing logic
    with open(infile, 'r') as fin:
        data = fin.read()
        processed_data = data.upper()
    with open(outfile, 'w') as fout:
        fout.write(processed_data)
```

### Summary

- Use the `configure_s3()` function to set up AWS credentials and S3 access.
- Options are available to use IAM roles, profiles, or custom endpoints.
- Use the S3-aware decorators to integrate S3 files seamlessly in your pipeline.

## Additional Resources

- [AWS IAM Roles Documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)
- [AWS CLI Configuration and Credential Files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)