"""Files.py - Working with files in ruffus pipelines
====================================================

Reference
---------

"""
import os
import tempfile

import cgatcore.iotools as iotools
import cgatcore.experiment as E
from cgatcore.pipeline.parameters import get_params


def get_temp_file(dir=None, shared=False, suffix="", mode="w+", encoding="utf-8"):
    '''get a temporary file.

    The file is created and the caller needs to close and delete the
    temporary file once it is not used any more. By default, the file
    is opened as a text file (mode ``w+``) with encoding ``utf-8``
    instead of the default mode ``w+b`` used in
    :class:`tempfile.NamedTemporaryFile`

    If dir does not exist, it will be created.

    Arguments
    ---------
    dir : string
        Directory of the temporary file and if not given is set to the
        default temporary location in the global configuration dictionary.
    shared : bool
        If set, the tempory file will be in a shared temporary
        location (given by the global configuration directory).
    suffix : string
        Filename suffix

    Returns
    -------
    file : File
        A file object of the temporary file.

    '''
    if dir is None:
        if shared:
            dir = get_params()['shared_tmpdir']
        else:
            dir = get_params()['tmpdir']

    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
        except OSError:
            # avoid race condition when several processes try to create
            # temporary directory.
            pass
        if not os.path.exists(dir):
            raise OSError("temporary directory {} could not be created".format(dir))

    return tempfile.NamedTemporaryFile(dir=dir, delete=False, prefix="ctmp",
                                       mode=mode,
                                       encoding=encoding, suffix=suffix)


def get_temp_filename(dir=None, shared=False, clear=True, suffix=""):
    '''return a temporary filename.

    The file is created and the caller needs to delete the temporary
    file once it is not used any more (unless `clear` is set`).

    If dir does not exist, it will be created.

    Arguments
    ---------
    dir : string
        Directory of the temporary file and if not given is set to the
        default temporary location in the global configuration dictionary.
    shared : bool
        If set, the tempory file will be in a shared temporary
        location.
    clear : bool
        If set, remove the temporary file after creation.
    suffix : string
        Filename suffix

    Returns
    -------
    filename : string
        Absolute pathname of temporary file.

    '''
    tmpfile = get_temp_file(dir=dir, shared=shared, suffix=suffix)
    tmpfile.close()
    if clear:
        os.unlink(tmpfile.name)
    return tmpfile.name


def get_temp_dir(dir=None, shared=False, clear=False):
    '''get a temporary directory.

    The directory is created and the caller needs to delete the temporary
    directory once it is not used any more.

    If dir does not exist, it will be created.

    Arguments
    ---------
    dir : string
        Directory of the temporary directory and if not given is set to the
        default temporary location in the global configuration dictionary.
    shared : bool
        If set, the tempory directory will be in a shared temporary
        location.

    Returns
    -------
    filename : string
        Absolute pathname of temporary file.

    '''
    if dir is None:
        if shared:
            dir = get_params()['shared_tmpdir']
        else:
            dir = get_params()['tmpdir']

    if not os.path.exists(dir):
        os.makedirs(dir)

    tmpdir = tempfile.mkdtemp(dir=dir, prefix="ctmp")
    if clear:
        os.rmdir(tmpdir)
    return tmpdir


def check_executables(filenames):
    """check for the presence/absence of executables"""

    missing = []

    for filename in filenames:
        if not iotools.which(filename):
            missing.append(filename)

    if missing:
        raise ValueError("missing executables: %s" % ",".join(missing))


def check_scripts(filenames):
    """check for the presence/absence of scripts"""
    missing = []
    for filename in filenames:
        if not os.path.exists(filename):
            missing.append(filename)

    if missing:
        raise ValueError("missing scripts: %s" % ",".join(missing))
