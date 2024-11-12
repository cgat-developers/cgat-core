# Google Cloud Storage

This section describes how to interact with Google Cloud Storage, specifically dealing with buckets and blobs (files). To interact with the cloud resource, we use the [`google.cloud` API](https://googleapis.dev/python/google-api-core/latest/index.html) for Python.

This documentation is a work in progress. We welcome any feedback for extra features or, if you find any bugs, please report them as [issues on GitHub](https://github.com/cgat-developers/cgat-core/issues).

## Setting up credentials

To use Google Cloud Storage features, you need to configure your credentials. This is quite easy with the `gcloud` tool. You need to run the following command before executing a workflow:

```bash
gcloud auth application-default login
```

This command sets up a JSON file with all of the credentials in your home folder, typically located at `.config/gcloud/application_default_credentials.json`.

Next, you need to specify which Google Cloud project you are using. Projects are created in the Google Cloud Console and each has a unique ID. This ID needs to be passed into CGAT-core. You can achieve this in two ways:

1. **Passing the project ID into the JSON file**:

    ```json
    {
      "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
      "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
      "refresh_token": "1/d8JxxulX84r3jiJVlt-xMrpDLcIp3RHuxLHtieDu8uA",
      "type": "authorized_user",
      "project_id": "extended-cache-163811"
    }
    ```

2. **Setting the project ID in `.bashrc`**:

    ```bash
    export GCLOUD_PROJECT=extended-cache-163811
    ```

## Download from Google Cloud Storage

Using remote files with Google Cloud can be achieved easily by using the `download`, `upload`, and `delete_file` functions that are part of the `GCRemoteObject` class.

First, initiate the class as follows:

```python
from cgatcore.remote.google_cloud import GCRemoteObject

GC = GCRemoteObject()
```

To download a file and use it within the decorator, follow this example:

```python
@transform(GC.download('gc-test', 'pipeline.yml', './pipeline.yml'),
           regex(r"(.*)\.(.*)"),
           r"\1.counts")
```

This will download the file `pipeline.yml` from the Google Cloud bucket `gc-test` to `./pipeline.yml`, and it will be picked up by the decorator function as normal.

## Upload to Google Cloud Storage

To upload files to Google Cloud, run:

```python
GC.upload('gc-test', 'pipeline2.yml', './pipeline.yml')
```

This command will upload the local file `./pipeline.yml` to the `gc-test` Google Cloud bucket, where it will be saved as `pipeline2.yml`.

## Delete a File from Google Cloud Storage

To delete a file from a Google Cloud bucket, run:

```python
GC.delete_file('gc-test', 'pipeline2.yml')
```

This command will delete the file `pipeline2.yml` from the `gc-test` bucket.

## Functional Example

As a simple example, the following one-function pipeline demonstrates how you can interact with Google Cloud:

```python
from ruffus import *
import sys
import os
import cgatcore.experiment as E
from cgatcore import pipeline as P
from cgatcore.remote.google_cloud import GCRemoteObject

# Load options from the config file
PARAMS = P.get_parameters([
    "%s/pipeline.yml" % os.path.splitext(__file__)[0],
    "../pipeline.yml",
    "pipeline.yml"
])

GC = GCRemoteObject()

@transform(GC.download('gc-test', 'pipeline.yml', './pipeline.yml'),
           regex(r"(.*)\.(.*)"),
           r"\1.counts")
def countWords(infile, outfile):
    '''Count the number of words in the pipeline configuration file.'''

    # Upload file to Google Cloud
    GC.upload('gc-test', 'pipeline2.yml', '/ifs/projects/adam/test_remote/data/pipeline.yml')

    # The command line statement we want to execute
    statement = '''awk 'BEGIN { printf("word\tfreq\n"); }
    {for (i = 1; i <= NF; i++) freq[$i]++}
    END { for (word in freq) printf "%%s\t%%d\n", word, freq[word] }'
    < %(infile)s > %(outfile)s'''

    P.run(statement)

    # Delete file from Google Cloud
    GC.delete_file('gc-test', 'pipeline2.yml')

@follows(countWords)
def full():
    pass
```

In this example:

1. **Download**: The `countWords` function downloads `pipeline.yml` from the `gc-test` bucket to `./pipeline.yml`.
2. **Word Count**: The function counts the number of words in the file using the `awk` command.
3. **Upload**: The processed file is then uploaded to Google Cloud.
4. **Delete**: Finally, the uploaded file is deleted from the bucket.

This functional example provides a simple illustration of how Google Cloud integration can be achieved within a CGAT pipeline.

