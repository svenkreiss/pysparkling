from __future__ import absolute_import

from .file_system import FileSystem
from .gs import GS
from .hdfs import Hdfs
from .http import Http
from .local import Local
from .s3 import S3


__all__ = ['FileSystem', 'GS', 'Hdfs', 'Http', 'Local', 'S3']


FILE_EXTENSIONS = [
    (('file', ''), Local),
    (('s3', 's3n'), S3),
    (('gs'), GS),
    (('http', 'https'), Http),
    (('hdfs'), Hdfs),
]


def get_fs(path):
    """Find the file system implementation for this path."""
    scheme = ''
    if '://' in path:
        scheme = path.partition('://')[0]

    for schemes, fs_class in FILE_EXTENSIONS:
        if scheme in schemes:
            return fs_class

    return FileSystem
