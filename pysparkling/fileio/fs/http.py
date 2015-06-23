from __future__ import absolute_import

import logging
from io import BytesIO, StringIO

from .file_system import FileSystem
from ...exceptions import ConnectionException, FileSystemNotSupported

log = logging.getLogger(__name__)

try:
    import requests
    SUPPORTED = True
except ImportError:
    SUPPORTED = False


class Http(FileSystem):
    def __init__(self, file_name):
        if not SUPPORTED:
            raise FileSystemNotSupported(
                'http not supported. Install "requests".'
            )

        FileSystem.__init__(self, file_name)
        self.headers = None

    @staticmethod
    def exists(path):
        r = requests.head(path, allow_redirects=True)
        return r.status_code == 200

    @staticmethod
    def resolve_filenames(expr):
        if Http.exists(expr):
            return [expr]
        return []

    def load(self):
        log.debug('Http GET request for {0}.'.format(self.file_name))
        r = requests.get(self.file_name, headers=self.headers)
        if r.status_code != 200:
            raise ConnectionException
        return BytesIO(r.content)

    def load_text(self):
        log.debug('Http GET request for {0}.'.format(self.file_name))
        r = requests.get(self.file_name, headers=self.headers)
        if r.status_code != 200:
            raise ConnectionException
        return StringIO(r.text)

    def dump(self, stream):
        log.debug('Dump to {0} with http PUT.'.format(self.file_name))
        requests.put(self.file_name, data=b''.join(stream))
        return self
