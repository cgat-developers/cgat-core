import os
import sys

try:
    import azure
except:
    raise ImportError("The azure-batch and azure-mgmt-storage packages needs to be installed.")

from azure.storage.blob import BlockBlobService, PublicAccess
from cgatcore.remote import AbstractRemoteObject


class AzureRemoteObject(AbstractRemoteObject):
    '''This is a class that will interact with an AWS object store.'''

    def __init__(self, account_name, account_key, *args, **kwargs):
        super(AzureRemoteObject, self).__init__(*args, **kwargs)

        
        self.account_name = account_name
        self.account_key = account_key

        self._Azureobject = AzureConnection(self.account_name, self.account_key, *args, **kwargs)

    def exists(self, bucket_name):
        
        try:
            self._Azureobject.bucket_exists(bucket_name)
        except:
            raise KeyError("No bucket exists, please specify a valid bucket")
        return True

    def download(self, container_name, blob_name, file_dir):
        self._Azureobject.remote_download(container_name, blob_name, file_dir)
        os.sync() # ensure flush to disk
        return file_dir

    def upload(self, container_name, blob_name, file_dir):
        self._Azureobject.remote_upload(container_name, file_dir, blob_name)
        return file_dir

    def delete_file(self, container_name, blob_name):
        self._Azureobject.remote_delete_file(container_name, blob_name)
        return blob_name


class AzureConnection():
    '''This is a connection to a remote Azure bucket for Microsoft Azure
    platform using the azure SDK.'''

    def __init__(self, account_name, account_key, *args, **kwargs):


        self.Azure = BlockBlobService(account_name=account_name, account_key=account_key, **kwargs)

    def bucket_exists(self, container_name):
        'In microsoft buckets are called containers'
        # To Do: rename function because of discrep[ancy between bucket and containers - may need to alter the name to object storage?
        try:
            self.Azure.exists(container_name)
            return True
        except:
            return False

    def remote_download(self,
                        container_name,
                        blob_name,
                        dest_dir):
        '''Download data/file from an Azure container.'''

        if not container_name:
            raise ValueError("Container name must be specified to download file")
        if not blob_name:
            raise ValueError("Blob name must be specified to download file")

        if dest_dir:
            dest_path = os.path.realpath(os.path.expanduser(dest_dir))
        try:
            self.Azure.get_blob_to_path(container_name, blob_name, dest_path)
        except:
            raise Exception('''No file was downloaded, make sure the correct
                            file or path is specified. It currently is: {}'''.format(dest_path))

    def remote_upload(self,
                      container_name,
                      file_dir,
                      blob_name=None):
        '''Upload data/file to an Azure container.'''

        file_path = os.path.realpath(os.path.expanduser(file_dir))

        if not container_name:
            raise ValueError("Container name must be specified to upload file")
        if not os.path.exists(file_dir):
            raise ValueError("File path specified does not exitis: {}".format(file_path))
        if not os.path.isfile(file_dir):
            raise ValueError("File path specified is not a file: {}".format(file_path))

        if not self.bucket_exists(container_name):
            self.Azure.create_container(container_name)

        try:
            self.Azure.create_blob_from_path(container_name, blob_name, file_dir)
        except:
            raise Exception("filename is not correctly specified: {}".format(file_dir))

    def remote_delete_file(self, container_name, blob_name):
        '''Will remove the blob from the remote Azure container'''

        if not container_name:
            raise ValueError("Bucket name must be specified to download file")
        if not blob_name:
            raise ValueError("Key must be specified to download file")

        try:
            self.Azure.delete_blob(container_name, blob_name)
        except:
            raise Exception("No deletion of blob took place, please make sure the container_name and/or blob_name are correct")
