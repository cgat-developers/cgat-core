# This implimentation was inspired/copied in parts from the FTP in smakemake remotes/FTP.py 
import os
import sys
import ftplib
from contextlib import contextmanager

try:
    import ftputil
    import ftputil.session
except ImportError as e:
    raise WorkflowError("The ftputil package needs to be installed. %s" % (e.msg))

from cgatcore.remote import AbstractRemoteObject


class FTPRemoteObject(AbstractRemoteObject):
    '''This is a class that will interact with an a secure ftp server.'''

    def __init__(self, *args, stay_on_remote=False, immediate_close=False, **kwargs):
        super(FTPRemoteObject, self).__init__(*args, stay_on_remote=stay_on_remote, **kwargs)

    @contextmanager
    def ftpc(self):
        
        if (not hasattr(self, "conn") or (hasattr(self, "conn") and not isinstance(self.conn, ftputil.FTPHost))) or self.immediate_close:
            args_use = self.provider.args
            if len(self.args):
                args_use = self.args

                kwargs_use = {}
                kwargs_use['host'] = self.host
                kwargs_use['password'] = None
                kwargs_use['username'] = None
                kwargs_use['port'] = int(self.port) if self.port else 21
                kwargs_use['encrypt_data_channel'] = self.encrypt_data_channel
            
                for k, v in self.provider.kwargs.items():
                    kwargs_use[k] = v
                for k, v in self.kwargs.items():
                    kwargs_use[k] = v


                ftp_base_class = ftplib.FTP_LTS if kwargs_use['encrypt_data_channel'] else ftplib.FTP

                ftp_session_factory = ftputil.session.session_factory(
                        base_class=ftp_base_class,
                        port=kwargs_use['port'],
                        encrypt_data_channel=kwargs_use['encrypt_data_channel'],
                        debug_level=None)


                conn = ftputil.FTPHost(kwargs_use['host'], kwargs_use['username'], kwargs_use['password'], session_factory=ftp_session_factory)

                if self.immediate_close:
                    yield conn
                else:
                    self.conn = conn
                    yield self.conn
            elif not self.immediate_close:
                yield self.conn

        if self.immediate_close:
            try:
                conn.seep_alive()
                conn.close()
            except:
                pass

    def exists(self):        
        if self._matched_address:
            with self.ftpc as ftpc:
                return ftpc.path.exists(self.remote_path)
            return False
        else:
            raise FTPFileException("The file cannot be parsed as an TFP path in form 'host:port/path/to/file': %s" % self.local_file())

    def download(self, make_dest_dir=True):
        with self.ftpc() as ftpc:
            if self.exists():
                # if the dest path does not exist
                if make_dest_dirs:
                    os.makedirs(os.path.dirname(self.local_path, exists_ok=True))
                try:
                    ftpc.syncronize_times()
                except:
                    pass

                ftpc.download(source=self.remote_path, target=self.local_path)
                os.sync()
            else:
                raise FTPFileException("The file does not exist remotely: %s" % self.local_file())

    def upload(self):
        with self.ftpc() as ftpc:
            ftpc.synchronize_times()
            ftpc.upload(source=self.local_path, target=self.remote_path)

    def delete_file(self):
        raise NotImplementedError("Cannot delete files from an FTP server") 
