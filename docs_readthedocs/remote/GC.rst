.. _GC:

==============
Google storage
==============

This section describes how to interact with the google cloud storage
bucket and blob (files). In order to interact with the cloud 
resource we use the `google.cloud` API for python.

This is a work in progress and we would really like feedback for extra features or if there
are any bugs then please report them as `issues on github <https://github.com/cgat-developers/cgat-core/issues>`_.

Setting up credentials
----------------------

In order to use google cloud storage feature you will need to conigure
your credentials. This is quite easy with the `gcloud` tool. This tool
is ran before exectuing a workflow in the following way::

    gcloud auth application-default login

This sets up a JSON file with all of the credentiaals on your home
folder, usually in the file `.config/gcloud/application_default_credentials.json`

Next you will also need to tell the API which project you are using. 
Projects are usually set in the google console and all have a unique 
ID. This ID needs to be passed into cgat-core.

This can be achieved in the following ways:

* passing project_id into the JASON file::

    {
    "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
    "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
    "refresh_token": "1/d8JxxulX84r3jiJVlt-xMrpDLcIp3RHuxLHtieDu8uA",
    "type": "authorized_user",
    "project_id": "extended-cache-163811"
    }

* project_id can be set in the `bashrc`::

    export GCLOUD_PROJECT=extended-cache-163811

Download from google storage
----------------------------

Using remote files with google cloud can be acieved easily by using `download`, `upload` and `delete_file` functions that are written into a RemoteClass.

Firstly you will need to initiate the class as follows::

    from cgatcore.remote.google_cloud import *
    GC = GCRemoteObject()

In order to download a file and use it within the decorator you can follows the example::

    @transform(GC.download('gc-test',"pipeline.yml", "./pipeline.yml"),
           regex("(.*)\.(.*)"),
           r"\1.counts")

This will download the file `pipeline.yml` from the google cloud bucket `gc-test` locally to `./pipeline.yml` 
and it will be picked up by the decoratory function as normal.

Upload to google cloud
----------------------

In order to upload files to google cloud you simply need to run::

    GC.upload('gc-test',"pipeline2.yml", "./pipeline.yml")

This will upload to the `gc-test` google cloud bucket the `./pipeline.yml` file and it will be saved as
`pipeline2.yml` in that bucket.

Delete file from AWS S3
-----------------------

In order to delete a file from the AWS S3 bucket then you simply run::

    S3.delete_file('aws-test-boto',"pipeline2.yml")

This will delete the `pipeline2.yml` file from the `aws-test-boto` bucket.

Functional example
------------------

As a simple example, the following one function pipeline demonstrates the way you can interact with the google cloud::

    from ruffus import *
    import sys
    import os
    import cgatcore.experiment as E
    from cgatcore import pipeline as P
    from cgatcore.remote.google_cloud import *

    # load options from the config file
    PARAMS = P.get_parameters(
        ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
    	 "../pipeline.yml",
     	 "pipeline.yml"])

    GC = GCRemoteObject()


    @transform(GC.download('gc-test',"pipeline.yml", "./pipeline.yml"),
           regex("(.*)\.(.*)"),
           r"\1.counts")
    def countWords(infile, outfile):
        '''count the number of words in the pipeline configuration files.'''

    	# Upload file to google cloud
    	GC.upload('gc-test',"pipeline2.yml", "/ifs/projects/adam/test_remote/data/pipeline.yml")

    	# the command line statement we want to execute
    	statement = '''awk 'BEGIN { printf("word\\tfreq\\n"); } 
    	{for (i = 1; i <= NF; i++) freq[$i]++}
    	END { for (word in freq) printf "%%s\\t%%d\\n", word, freq[word] }'
    	< %(infile)s > %(outfile)s'''

   	 P.run(statement)

   	  # Delete file from google cloud
    	  GC.delete_file('gc-test',"pipeline2.yml")

    	  @follows(countWords)
    	  def full():
              pass
