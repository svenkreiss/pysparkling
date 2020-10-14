from __future__ import absolute_import

import logging
from io import BytesIO, StringIO

from .file_system import FileSystem
from ...exceptions import ConnectionException, FileSystemNotSupported

log = logging.getLogger(__name__)

try:
    import requests
except ImportError:
    requests = None


class Http(FileSystem):
    """:class:`.FileSystem` implementation for HTTP."""

    def __init__(self, file_name):
        if requests is None:
            raise FileSystemNotSupported(
                'http not supported. Install "requests".'
            )

        super(Http, self).__init__(file_name)
        self.headers = None

    @staticmethod
    def resolve_filenames(expr):
        if Http(expr).exists():
            return [expr]
        return []

    def exists(self):
        r = requests.head(self.file_name, allow_redirects=True)
        return r.status_code == 200

    def load(self):
        log.debug('Http GET request for {0}.'.format(self.file_name))
        r = requests.get(self.file_name, headers=self.headers)
        if r.status_code != 200:
            raise ConnectionException()
        return BytesIO(r.content)

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        # warning: encoding and encoding_errors are ignored
        log.debug('Http GET request for {0}.'.format(self.file_name))
        r = requests.get(self.file_name, headers=self.headers)
        if r.status_code != 200:
            raise ConnectionException()
        return StringIO(r.text)

    def dump(self, stream):
        log.debug('Dump to {0} with http PUT.'.format(self.file_name))
        requests.put(self.file_name, data=b''.join(stream))
        return self
