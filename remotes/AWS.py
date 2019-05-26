import os
import sys

try:
    import boto3
    import botocore
except ImportError as e:
    raise WorkflowError("The boto3 package needs to be installed. %s" % e.msg)


class S3RemoteObject():
    '''This is a class that will interact with an AWS object store.'''

    def __init__(self, *args, **kwargs):

        self._S3object = S3Connect(*args, **kwargs)



class S3Connection():
    '''This is a connection to a remote S3 bucket for AWS
    server using the boto3 API.'''

    def __init__(self, *args, **kwargs):
        # 

        self.S3 = boto3.resource("s3", **kwargs)

    def bucket_exists(self, bucket_name):
        try:
            self.S3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except:
            return False

    def remote_download(self,
                          bucket_name,
                          file_dir):
        '''Download data/file from an S3 bucket.'''


    def remote_upload(self,
                        bucket_name,
                        file_dir):
        '''Upload data/file to an S3 bucket.'''

    def delete_object_bucket(self,
                               bucket_name,
                               key):
        '''delete data/file from an S3 bucket '''

    def exists_object_bucket(self,
                             bucket_name,
                             key):
        '''Returns True if data/file exists in an S3 bucket.'''


