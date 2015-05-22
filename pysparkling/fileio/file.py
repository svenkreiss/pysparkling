from __future__ import absolute_import

import bz2
import gzip
import logging
from io import BytesIO

from . import fs

log = logging.getLogger(__name__)


class File(object):
    def __init__(self, file_name):
        self.file_name = file_name
        self.fs = fs.get_fs(file_name)(file_name)

    @staticmethod
    def resolve_filenames(all_expr):
        files = []
        for expr in all_expr.split(','):
            expr = expr.strip()
            files += fs.get_fs(expr).resolve_filenames(expr)
        log.debug('Filenames: {0}'.format(files))
        return files

    @staticmethod
    def exists(path):
        """Checks both for a file at this location or a directory."""
        return fs.get_fs(path).exists(path)

    def load(self):
        stream = self.fs.load()

        # decompress
        if self.file_name.endswith('.gz'):
            log.info('Using gzip decompression: {0}'
                     ''.format(self.file_name))
            stream = gzip.GzipFile(fileobj=stream, mode='rb')
        if self.file_name.endswith('.bz2'):
            log.info('Using bz2 decompression: {0}'.format(self.file_name))
            stream = BytesIO(bz2.decompress(stream.read()))

        return stream

    def dump(self, stream=None):
        """stream could be a BytesIO instance."""
        if stream is None:
            stream = BytesIO()

        # compress
        if self.file_name.endswith('.gz'):
            log.debug('Compressing with gzip: {0}'.format(self.file_name))
            compressed = BytesIO()
            with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
                for x in stream:
                    f.write(x)
            stream = BytesIO(compressed.getvalue())
        elif self.file_name.endswith('.bz2'):
            log.debug('Compressing with bz2: {0}'.format(self.file_name))
            stream = BytesIO(bz2.compress(b''.join(stream)))

        self.fs.dump(stream)
        return self

    def make_public(self, recursive=False):
        self.fs.make_public(recursive)
        return self
