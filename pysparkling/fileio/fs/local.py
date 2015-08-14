from __future__ import absolute_import, unicode_literals

import io
import os
import fnmatch
import logging

from ...utils import Tokenizer
from .file_system import FileSystem

log = logging.getLogger(__name__)


class Local(FileSystem):
    def __init__(self, file_name):
        FileSystem.__init__(self, file_name)

    @staticmethod
    def resolve_filenames(expr):
        if expr.startswith('file://'):
            expr = expr[7:]

        if os.path.isfile(expr):
            return [expr]

        files = []
        t = Tokenizer(expr)
        prefix = t.next(['*', '?'])
        for root, dirnames, filenames in os.walk(prefix):
            root_wo_slash = root[:-1] if root.endswith('/') else root
            for filename in filenames:
                if fnmatch.fnmatch(root_wo_slash+'/'+filename,
                                   expr) or \
                   fnmatch.fnmatch(root_wo_slash+'/'+filename,
                                   expr+'/part*'):
                    files.append(root_wo_slash+'/'+filename)
        # files += glob.glob(expr)+glob.glob(expr+'/part*')

        return files

    def exists(self):
        path_local = self.file_name
        if path_local.startswith('file://'):
            path_local = path_local[7:]
        return os.path.exists(path_local) or os.path.exists(path_local+'/')

    def load(self):
        f_name_local = self.file_name
        if f_name_local.startswith('file://'):
            f_name_local = f_name_local[7:]
        with io.open(f_name_local, 'rb') as f:
            return io.BytesIO(f.read())

    def load_text(self):
        f_name_local = self.file_name
        if f_name_local.startswith('file://'):
            f_name_local = f_name_local[7:]
        with io.open(f_name_local, 'r') as f:
            return io.StringIO(f.read())

    def dump(self, stream):
        path_local = self.file_name
        if path_local.startswith('file://'):
            path_local = path_local[7:]

        # making sure directory exists
        dirname = os.path.dirname(path_local)
        if not os.path.exists(dirname):
            log.debug('creating local directory {0}'.format(dirname))
            os.makedirs(dirname)

        log.debug('writing file {0}'.format(path_local))
        with io.open(path_local, 'wb') as f:
            for c in stream:
                f.write(c)
        return self
