# This implimentation was inspired by the SFTP in smakemake remotes repo
import os
import sys
from contextlib import contextmanager

try:
    import pysftp
except ImportError as e:
    raise WorkflowError("The pysftp package needs to be installed. %s" % (e.msg))

from cgatcore.remote import AbstractRemoteObject


class SFTPRemoteObject(AbstractRemoteObject):
    '''This is a class that will interact with an a secure ftp server.'''

    def __init__(self, *args, keep_local=False, provider=None, **kwargs):
        super(SFTPRemoteObject, self).__init__(*args, keep_local=keep_local, provider=provider, **kwargs)

    @contextmanager
    def sftpc(self):
        
        args_use = self.provider.args
        if len(self.args):
            args_use = self.args

        kwargs_use = {}
        kwargs_use['host'] = self.host
        kwargs_use['port'] = int(self.port) if self.port else 22
        for k, v in self.provider.kwargs.items():
            kwargs_use[k] = v
        for k, v in self.kwargs.items():
            kwargs_use[k] = v

        conn = pysftp.Connection(*args_use, **kwargs_use)
        yield conn
        conn.close()

    def exists(self):        
        if self._matched_address:
            with self.sftpc as sftpc:
                return sftpc.exists(self.remote_path)
                if sftpc.exists(self.remote_path):
                    return sftpc.isfile(self.remote_path)
                return False
        else:
            raise SFTPFileException("The file cannot be parsed as an STFP path in form 'host:port/path/to/file': %s" % self.local_file())

    def download(self, make_dest_dir=True):
        with self.sftpc() as sftpc:
            if self.exists():
                # if the dest path does not exist
                if make_dest_dirs:
                    os.makedirs(os.path.dirname(self.local_path, exists_ok=True))

                sftpc.get(remotepath=self.remote_path, localpath=self.local_path, preserve_mtime=True)
                os.sync()
            else:
                raise SFTPFileException("The file cannot be parsed as an STFP path in form 'host:port/path/to/file': %s" % self.local_file())

    def upload(self):
        with self.sftpc() as sftpc:
            sftpc.put(localpath=self.local_path, remotepath=self.remote_path, confirm=True, preserve_mtime=True)

    def delete_file(self):
        raise NotImplementedError("Cannot delete files from an SFTP server") 
