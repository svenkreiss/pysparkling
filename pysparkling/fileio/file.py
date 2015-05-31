from __future__ import absolute_import

import logging
from io import BytesIO

from . import fs
from . import codec

log = logging.getLogger(__name__)


class File(object):
    def __init__(self, file_name):
        self.file_name = file_name
        self.fs = fs.get_fs(file_name)(file_name)
        self.codec = codec.get_codec(file_name)()

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

    def load(self, f_range=None, delimiter=None):
        stream = self.fs.load(f_range, delimiter)
        stream = self.codec.decompress(stream)
        return stream

    def dump(self, stream=None):
        """stream could be a BytesIO instance."""
        if stream is None:
            stream = BytesIO()

        stream = self.codec.compress(stream)
        self.fs.dump(stream)

        return self

    def make_public(self, recursive=False):
        self.fs.make_public(recursive)
        return self
