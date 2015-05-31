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

    def load(self, f_range=None, delimiter=None):
        f_name_local = self.file_name
        if f_name_local.startswith('file://'):
            f_name_local = f_name_local[7:]

        if not f_range:
            with open(f_name_local, 'rb') as f:
                log.debug('Reading entire file {0}'.format(f_name_local))
                return BytesIO(f.read())

        f_size = os.path.getsize(f_name_local)
        f_range = (
            int(f_range[0]*f_size),
            int(f_range[1]*f_size),
        )

        if f_range[0] == f_range[1]:
            log.warn('Empty range for {0} given: {1}'
                     ''.format(f_name_local, f_range))
            return BytesIO()

        with open(f_name_local, 'rb') as f:
            log.debug(
                'Reading file {0} of size {1} in range {2} with delimiter {3}.'
                ''.format(f_name_local, str(f_size), str(f_range),
                          str(delimiter).encode('string_escape'))
            )

            # seek to beginning of range
            # but check whether the char before range start is a delimiter
            start_pos = f_range[0]
            if f_range[0] == 0.0 or not delimiter:
                f.seek(start_pos)
            else:
                f.seek(start_pos-1)
                c = f.read(1)
                while c and c != delimiter:
                    start_pos += 1
                    if start_pos >= f_range[1]:
                        log.debug('Did not find delimiter in chunk.')
                        return BytesIO()
                    c = f.read(1)

            # read the requested chunk
            data = f.read(f_range[1]-start_pos)
            if delimiter:
                while data[-1] != delimiter:
                    c = f.read(1)
                    if not c:
                        break
                    data += c

            return BytesIO(data)

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
