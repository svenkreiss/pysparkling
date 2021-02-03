from __future__ import absolute_import, unicode_literals

from fnmatch import fnmatch
import glob
import io
import logging
import os

from ...utils import Tokenizer
from .file_system import FileSystem

log = logging.getLogger(__name__)


class Local(FileSystem):
    """:class:`.FileSystem` implementation for the local file system."""

    @staticmethod
    def resolve_filenames(expr):
        if expr.startswith('file://'):
            expr = expr[7:]
        if os.path.isfile(expr):
            return [expr]
        if '/' not in expr:
            expr = '.' + os.path.sep + expr

        t = Tokenizer(expr)
        prefix = t.get_next(['*', '?'])
        if not prefix.endswith('/') and '/' in prefix:
            prefix = os.path.dirname(prefix)
        files = []
        for root, _, filenames in os.walk(prefix):
            for filename in filenames:
                path = os.path.join(root, filename)
                if fnmatch(path, expr) or fnmatch(path, expr + '/part*'):
                    files.append(path)
        return files

    @staticmethod
    def resolve_content(expr):
        if expr.startswith('file://'):
            expr = expr[7:]
        matches = glob.glob(expr)
        file_paths = []
        for match in matches:
            if os.path.isfile(match):
                file_paths.append(match)
            else:
                file_paths += [
                    os.path.join(root, f)
                    for root, _, files in os.walk(match)
                    for f in files
                    if not f.startswith(("_", "."))
                ]
        return file_paths

    @property
    def file_path(self):
        if self.file_name.startswith('file://'):
            return self.file_name[7:]
        return self.file_name

    def exists(self):
        return os.path.exists(self.file_path)

    def load(self):
        with io.open(self.file_path, 'rb') as f:
            return io.BytesIO(f.read())

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        with io.open(self.file_path, 'r',
                     encoding=encoding, errors=encoding_errors) as f:
            return io.StringIO(f.read())

    def dump(self, stream):
        file_path = self.file_path  # caching

        # making sure directory exists
        dirname = os.path.dirname(file_path)
        if dirname and not os.path.exists(dirname):
            log.debug('creating local directory {0}'.format(dirname))
            os.makedirs(dirname)

        log.debug('writing file {0}'.format(file_path))
        with io.open(file_path, 'wb') as f:
            for c in stream:
                f.write(c)
        return self
