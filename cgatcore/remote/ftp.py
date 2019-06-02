import os
import sys

try:
    import pysftp
except ImportError as e:
    raise WorkflowError("The pysftp package needs to be installed. %s" % (e.msg))

from cgatcore.remote import AbstractRemoteObject


class S3RemoteObject(AbstractRemoteObject):
    '''This is a class that will interact with an an ftp server.'''

    def __init__(self, *args, **kwargs):
        super(FTPRemoteObject, self).__init__(*args, **kwargs)

        self._FTPobject = FTPConnection(*args, **kwargs)

    def exists(self):
        
        try:
            self._FTPobject.bucket_exists(bucket_name)
        except botocore.exceptions.ClientError as e:
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

class FTPConnection():
    '''This is a connection to a remote ftp server using the
       pysftp project.'''

    def __init__(self, hostname, username, password, *args, **kwargs):

        if hostname is None:
            pass
        else:
            self.hostname = hostname
        self.usersname = username
        self.password = password

        self.FTP = pysftp.Connection(self.hostname, username=self.username, password=self.password)
