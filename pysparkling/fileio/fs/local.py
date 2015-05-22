from __future__ import absolute_import

import os
import fnmatch
import logging
from io import BytesIO

from ...utils import Tokenizer
from .file_system import FileSystem

log = logging.getLogger(__name__)


class Local(FileSystem):
    def __init__(self, file_name):
        FileSystem.__init__(self, file_name)

    @staticmethod
    def exists(path):
        path_local = path
        if path_local.startswith('file://'):
            path_local = path_local[7:]
        return os.path.exists(path_local) or os.path.exists(path_local+'/')

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

    def load(self):
        f_name_local = self.file_name
        if f_name_local.startswith('file://'):
            f_name_local = f_name_local[7:]
        with open(f_name_local, 'rb') as f:
            return BytesIO(f.read())

    def dump(self, stream, make_public=False):
        path_local = self.file_name
        if path_local.startswith('file://'):
            path_local = path_local[7:]

        # making sure directory exists
        dirname = os.path.dirname(path_local)
        if not os.path.exists(dirname):
            log.info('creating local dir {0}/'.format(dirname))
            os.makedirs(dirname)

        log.info('writing file {0}'.format(path_local))
        with open(path_local, 'wb') as f:
            for c in stream:
                f.write(c)
        return self
