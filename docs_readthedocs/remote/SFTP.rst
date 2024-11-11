.. _SFTP:


========================
File transfer using SFTP
========================

Cgat-core can access files on a remote server vis SFTP. This functionality is provided
by the `pysftp <https://pysftp.readthedocs.io/en/release_0.2.9/>`_ python library. 

Given that you have already set up your SSH key pairs correctly for your server then 
accessing the server is easy:::

    from cgatcore.remote.sftp import *
    sftp = SFTPRemoteObject()


Download from SFTP
------------------

Using remote files with SFTP can be achieved easily by using `download` function that
is written into a RemoteClass.

In order to download a file and use it within the decorator you can follows the example::

    from cgatcore.remote.SFTP import *
    sftp = SFTPRemoteObject()

    @transform(sftp.download('example.com/path/to/file.txt'),
           regex("(.*)\.txt"),
           r"\1.counts")


The remote address must be specified with the host (domain or IP address) and the absolute
path to the file on the remote server. A port may be specified if the SSH daemon on the server
is listening on a port other than 22.::

    from cgatcore.remote.SFTP import *
    sftp = SFTPRemoteObject(port=4040)

    @transform(sftp.download('example.com/path/to/file.txt'),
           regex("(.*)\.txt"),
           r"\1.counts")

You can specify standard arguments used by `pysftp <https://pysftp.readthedocs.io/en/release_0.2.9/pysftp.html#pysftp.Connection>`_. For
example::

    from cgatcore.remote.SFTP import *
    sftp = SFTPRemoteObject(username= "cgatpassword", password="cgatpassword")

    @transform(sftp.download('example.com/path/to/file.txt'),
           regex("(.*)\.txt"),
           r"\1.counts")
