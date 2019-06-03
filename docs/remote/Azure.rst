.. _Azure:

=======================
Microsoft Azure Storage
=======================

This section describes how to interact with Microsoft Azure cloud storage. In order to interact with the
Azure cloud storage resource we use the `azure <https://github.com/Azure/azure-sdk-for-python>`_ SDK.

Like all of our remote connection functionality, this is a work in progress and we are currently in the
process of adding extra features. If you have bug reports or comments then please raise them as an issue
on `github <https://github.com/cgat-developers/cgat-core/issues>`_ 


Setting up credentials
----------------------

Unlike other remote access providers, the credentials are set up by passing them directly into the initial class
as variables as follows::

    Azure = AzureRemoteObject(account_name = "firstaccount", account_key = "jbiuebcjubncjklncjkln........")

These access keys can be found in the Azure portal and locating the storage account. In the settings of the storage account
there is a selection "Access keys". The account name and access keys are listed here.

Download from Azure
-------------------

Using remote files with Azure can be acieved easily by using `download`, `upload` and `delete_file` functions that are written into a RemoteClass.

Firstly you will need to initiate the class as follows::

    from cgatcore.remote.azure import *
    Azure = AzureRemoteObject(account_name = "firstaccount", account_key = "jbiuebcjubncjklncjkln........")

In order to download a file and use it within the decorator you can follows the example::

    @transform(Azure.download('test-azure',"pipeline.yml", "./pipeline.yml"),
           regex("(.*)\.(.*)"),
           r"\1.counts")

This will download the file `pipeline.yml` in the Azure container `test-azure` locally to `./pipeline.yml` 
and it will be picked up by the decoratory function as normal.

Upload to Azure
---------------

In order to upload files to Azure you simply need to run::

    Azure.upload('test-azure',"pipeline2.yml", "./pipeline.yml")

This will upload to the `test-azure` Azure container the `./pipeline.yml` file and it will be saved as
`pipeline2.yml` in that bucket.

Delete file from Azure
----------------------

In order to delete a file from the Azure container then you simply run::

    Azure.delete_file('test-azure',"pipeline2.yml")

This will delete the `pipeline2.yml` file from the `test-azure` container.


Functional example
------------------

As a simple example, the following one function pipeline demonstrates the way you can interact with AWS S3::

    from ruffus import *
    import sys
    import os
    import cgatcore.experiment as E
    from cgatcore import pipeline as P
    from cgatcore.remote.azure import *

    # load options from the config file
    PARAMS = P.get_parameters(
        ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
    	 "../pipeline.yml",
     	 "pipeline.yml"])

    Azure = AzureRemoteObject(account_name = "firstaccount", account_key = "jbiuebcjubncjklncjkln........")


    @transform(Azure.download('test-azure',"pipeline.yml", "./pipeline.yml"),
           regex("(.*)\.(.*)"),
           r"\1.counts")
    def countWords(infile, outfile):
        '''count the number of words in the pipeline configuration files.'''

    	# Upload file to Azure
    	Azure.upload('test-azure',"pipeline2.yml", "/ifs/projects/adam/test_remote/data/pipeline.yml")

    	# the command line statement we want to execute
    	statement = '''awk 'BEGIN { printf("word\\tfreq\\n"); } 
    	{for (i = 1; i <= NF; i++) freq[$i]++}
    	END { for (word in freq) printf "%%s\\t%%d\\n", word, freq[word] }'
    	< %(infile)s > %(outfile)s'''

   	 P.run(statement)

   	  # Delete file from Azure
    	  Azure.delete_file('test-azure',"pipeline2.yml")

    	  @follows(countWords)
    	  def full():
              pass
