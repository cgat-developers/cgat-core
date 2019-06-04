import os
import sys

try:
    import boto3
    import botocore
except ImportError as e:
    raise WorkflowError("The boto3 package needs to be installed. %s" % (e.msg))

from cgatcore.remote import AbstractRemoteObject


class S3RemoteObject(AbstractRemoteObject):
    '''This is a class that will interact with an AWS object store.'''

    def __init__(self, *args, **kwargs):
        super(S3RemoteObject, self).__init__(*args, **kwargs)

        self._S3object = S3Connection(*args, **kwargs)

    def exists(self, bucket_name):
        
        try:
            self._S3object.bucket_exists(bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise S3FileException("The file cannot be parsed as an s3 path in form 'bucket/key': %s" % self.local_file())

        return True

    def download(self, bucket_name, key, file_dir):
        self._S3object.remote_download(bucket_name, key, file_dir)
        os.sync() # ensure flush to disk
        return file_dir

    def upload(self, bucket_name, key, file_dir):
        self._S3object.remote_upload(bucket_name, file_dir, key)
        return file_dir

    def delete_file(self, bucket_name, key):
        self._S3object.remote_delete_file(bucket_name, key)
        return key

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
                        key,
                        dest_dir):
        '''Download data/file from an S3 bucket.'''

        if not bucket_name:
            raise ValueError("Bucket name must be specified to download file")
        if not key:
            raise ValueError("Key must be specified to download file")

        if dest_dir:
            dest_path = os.path.realpath(os.path.expanduser(dest_dir))

        f = self.S3.Object(bucket_name, key)

        try:
            f.download_file(dest_path)
        except:
            raise Exception('''no file was downloaded, make sure the correct
                            file or path is specified. It currently is: {}'''.format(dest_path))

    def remote_upload(self,
                      bucket_name,
                      file_dir,
                      key=None):
        '''Upload data/file to an S3 bucket.'''

        file_path = os.path.realpath(os.path.expanduser(file_dir))

        if not bucket_name:
            raise ValueError("Bucket name must be specified to upload file")
        if not os.path.exists(file_dir):
            raise ValueError("File path specified does not exitis: {}".format(file_path))
        if not os.path.isfile(file_dir):
            raise ValueError("File path specified is not a file: {}".format(file_path))

        if not self.bucket_exists(bucket_name):
            self.S3.create_bucket(Bucket=bucket_name) # Implement other features fuch as CreateBucketConfiguration

        f = self.S3.Object(bucket_name, key)

        try:
            f.upload_file(file_path)
        except:
            raise Exception("filename is not correctly specified: {}".format(file_dir))

    def remote_delete_file(self, bucket_name, key):
        '''Will remove the object from the remote S3 bucket'''

        if not bucket_name:
            raise ValueError("Bucket name must be specified to download file")
        if not key:
            raise ValueError("Key must be specified to download file")

        f = self.S3.Object(bucket_name, key)
        f_delete = f.delete()

        return f_delete
