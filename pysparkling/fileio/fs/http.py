from __future__ import absolute_import

import logging
import requests
from io import BytesIO

from .file_system import FileSystem
from ...exceptions import ConnectionException

log = logging.getLogger(__name__)


class Http(FileSystem):
    def __init__(self, file_name):
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

    def load(self, f_range=None, delimiter=None):
        if f_range and f_range[0] != 0.0:
            log.warn('HTTP does not support partial file downloads. Skipping.')
            return BytesIO()
        log.info('Http GET request for {0}.'.format(self.file_name))
        r = requests.get(self.file_name, headers=self.headers)
        if r.status_code != 200:
            raise ConnectionException
        return BytesIO(r.content)

    def dump(self, stream):
        log.info('Dump to {0} with http PUT.'.format(self.file_name))
        requests.put(self.file_name, data=b''.join(stream))
        return self
