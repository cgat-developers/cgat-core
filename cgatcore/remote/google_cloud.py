import os
import sys

try:
    import google.cloud
    from google.cloud import storage
except ImportError as e:
    raise WorkFlowError("The google cloud SDK needs to be installed. %s"
                        % e.msg)

from cgatcore.remote import AbstractRemoteObject


class GCRemoteObject(AbstractRemoteObject):
    '''This is a class that will interact with an Google cloud object store.'''

    def __init__(self, *args, **kwargs):
        super(GCRemoteObject, self).__init__(*args, **kwargs)

        self._GCobject = GCConnection(*args, **kwargs)

    def exists(self, bucket):
        try:
            self._S3object.bucket_exists(bucket_name)
        except:
            raise KeyError("The bucket does not exist in google storage")

    def download(self, bucket_name, key, file_dir):
        self._GCobject.remote_download(bucket_name, key, file_dir)
        os.sync()
        return file_dir

    def upload(self, bucket_name, key, file_dir):
        self._GCobject.remote_upload(bucket_name, file_dir, key)
        return file_dir

    def delete_file(self, bucket_name, key):
        self._GCobject.remote_delete_file(bucket_name, key)
        return key

class GCConnection():
    '''This is a connection to a remote google cloud bucket
       using the goggle.cloud SDK.'''

    def __init__(self, *args, **kwargs):
        # 

        self.GC = storage.Client(*args, **kwargs)
        self.blob = None

    def bucket_exists(self, bucket_name):
        try:
            self.GC.get_bucket(bucket_name)
            return True
        except KeyError as e:
            print("%s bucket does not exist in google cloud"% (e))

    def remote_download(self,
                        bucket_name,
                        key,
                        dest_dir):
        '''Download data/file (blobs) from an google storage bucket.'''

        if not bucket_name:
            raise ValueError("Bucket name must be specified to download file")
        if not key:
            raise ValueError("Key must be specified to download file")

        if dest_dir:
            dest_path = os.path.realpath(os.path.expanduser(dest_dir))

        b = self.GC.get_bucket(bucket_name)
        blob = b.blob(key)

        try:
            blob.download_to_filename(dest_path)
        except:
            raise Exception('''no file was downloaded, make sure the correct
                            file or path is specified. It currently is: {}'''.format(dest_path))

    def remote_upload(self,
                      bucket_name,
                      file_dir,
                      key=None):
        '''Upload data/file (blob) to a google cloud  bucket.'''

        file_path = os.path.realpath(os.path.expanduser(file_dir))

        if not bucket_name:
            raise ValueError("Bucket name must be specified to upload file")
        if not os.path.exists(file_dir):
            raise ValueError("File path specified does not exitis: {}".format(file_path))
        if not os.path.isfile(file_dir):
            raise ValueError("File path specified is not a file: {}".format(file_path))

        if not self.bucket_exists(bucket_name):
            self.GC.create_bucket(bucket_name)

        b = self.GC.get_bucket(bucket_name)
        blob = b.blob(key)

        try:
            blob.upload_from_filename(file_path)
        except:
            raise Exception("filename is not correctly specified: {}".format(file_dir))

    def remote_delete_file(self, bucket_name, key):
        '''Will remove the object from the remote S3 bucket'''

        if not bucket_name:
            raise ValueError("Bucket name must be specified to download file")
        if not key:
            raise ValueError("Key must be specified to download file")

        b = self.GC.get_bucket(bucket_name)
        blob = b.blob(key)
        f_delete = blob.delete()

        return key
