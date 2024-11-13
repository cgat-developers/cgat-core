# Azure Blob Storage

This section describes how to interact with Microsoft's Azure Blob Storage, which is used for storing data in containers (similar to buckets in other cloud services). We use the [`azure-storage-blob`](https://pypi.org/project/azure-storage-blob/) package for interacting with Azure storage in Python.

This documentation is a work in progress. If you find any bugs or want to request extra features, please report them as [issues on GitHub](https://github.com/cgat-developers/cgat-core/issues).

## Setting up credentials

To use Azure Blob Storage, you need an **account name** and an **account key** from Azure. These credentials can be found in the Azure Portal under "Access keys" for the storage account. You will need to use these credentials to interact with the Azure containers through the `AzureRemoteObject` class.

## Using Azure Blob Storage with `AzureRemoteObject`

The `AzureRemoteObject` class allows you to interact with Azure Blob Storage in your Python scripts or workflows. The operations supported by this class include checking for the existence of containers, downloading blobs, uploading blobs, and deleting blobs.

First, initiate the class as follows:

```python
from cgatcore.remote.azure import AzureRemoteObject

account_name = "your_account_name"
account_key = "your_account_key"

azure_obj = AzureRemoteObject(account_name=account_name, account_key=account_key)
```

### Check if a Container Exists

To check whether a container exists in Azure Blob Storage:

```python
azure_obj.exists('my-container')
```

If the container does not exist, a `KeyError` will be raised.

### Download from Azure Blob Storage

To download a file (blob) from a container, use the `download` method:

```python
azure_obj.download('my-container', 'my-blob.txt', './local-dir/my-blob.txt')
```

This command will download the file named `my-blob.txt` from the container `my-container` and save it locally to `./local-dir/my-blob.txt`.

### Upload to Azure Blob Storage

To upload a file to an Azure container, use the `upload` method:

```python
azure_obj.upload('my-container', 'my-blob.txt', './local-dir/my-blob.txt')
```

This will upload the local file `./local-dir/my-blob.txt` to the container `my-container`, where it will be saved as `my-blob.txt`.

### Delete a File from Azure Blob Storage

To delete a blob from an Azure container, use the `delete_file` method:

```python
azure_obj.delete_file('my-container', 'my-blob.txt')
```

This command will delete the blob named `my-blob.txt` from the container `my-container`.

## Functional Example

Below is an example demonstrating the usage of the `AzureRemoteObject` class within a data processing pipeline:

```python
from ruffus import *
import sys
import os
import cgatcore.experiment as E
from cgatcore import pipeline as P
from cgatcore.remote.azure import AzureRemoteObject

# Set up credentials
account_name = "your_account_name"
account_key = "your_account_key"

azure_obj = AzureRemoteObject(account_name=account_name, account_key=account_key)

# Load options from the config file
PARAMS = P.get_parameters([
    "%s/pipeline.yml" % os.path.splitext(__file__)[0],
    "../pipeline.yml",
    "pipeline.yml"
])

@transform(azure_obj.download('my-container', 'input.txt', './input.txt'),
           regex(r"(.*)\.(.*)"),
           r"\1.counts")
def countWords(infile, outfile):
    '''Count the number of words in the input file.'''

    # Upload file to Azure Blob Storage
    azure_obj.upload('my-container', 'output.txt', './input.txt')

    # The command line statement we want to execute
    statement = '''awk 'BEGIN { printf("word\tfreq\n"); } 
    {for (i = 1; i <= NF; i++) freq[$i]++}
    END { for (word in freq) printf "%%s\t%%d\n", word, freq[word] }'
    < %(infile)s > %(outfile)s'''

    P.run(statement)

    # Delete file from Azure Blob Storage
    azure_obj.delete_file('my-container', 'output.txt')

@follows(countWords)
def full():
    pass
```

In this example:

1. **Download**: The `countWords` function downloads `input.txt` from the container `my-container` to a local path `./input.txt`.
2. **Word Count**: The function then counts the number of words in the file using the `awk` command.
3. **Upload**: The output is uploaded to Azure Blob Storage.
4. **Delete**: Finally, the uploaded file is deleted from the container.

This example demonstrates how Azure Blob Storage can be integrated seamlessly into a data pipeline using the `AzureRemoteObject` class.