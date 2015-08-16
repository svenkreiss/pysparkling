from __future__ import absolute_import, unicode_literals

import fnmatch
import logging
from io import BytesIO, StringIO

from ...utils import Tokenizer
from .file_system import FileSystem
from ...exceptions import FileSystemNotSupported

log = logging.getLogger(__name__)

hdfs = None
try:
    import hdfs
except ImportError:
    pass


class Hdfs(FileSystem):
    _conn = {}

    def __init__(self, file_name):
        if hdfs is None:
            raise FileSystemNotSupported(
                'hdfs not supported. Install the python package "hdfs".'
            )

        FileSystem.__init__(self, file_name)

    @staticmethod
    def client_and_path(path):
        # obtain key
        t = Tokenizer(path)
        t.next('://')  # skip scheme
        domain = t.next('/')
        path = t.next()

        if ':' not in domain:
            port = 50070
        else:
            domain, port = domain.split(':')
            port = int(port)
        cache_id = domain+'__'+str(port)

        if cache_id not in Hdfs._conn:
            Hdfs._conn[cache_id] = hdfs.InsecureClient(
                'http://{0}:{1}'.format(domain, port)
            )
        return (Hdfs._conn[cache_id], '/'+path)

    def exists(self):
        c, p = Hdfs.client_and_path(self.file_name)
        try:
            c.status(p)
        except hdfs.util.HdfsError:
            return False
        return True

    @staticmethod
    def resolve_filenames(expr):
        files = []

        c, expr_path = Hdfs.client_and_path(expr)

        t = Tokenizer(expr)
        scheme = t.next('://')
        domain = t.next('/')
        fixed_path = t.next(['*', '?'])
        file_expr = t.next()

        if '/' in fixed_path:
            fixed_path = fixed_path[:fixed_path.rfind('/')]
            file_expr = fixed_path[fixed_path.rfind('/')+1:]+file_expr

        for fn, file_meta in c.list('/'+fixed_path):
            print(fn)
            print(type(fn))
            if fnmatch.fnmatch(fn, expr_path) or \
               fnmatch.fnmatch(fn, expr_path+'/part*'):
                files.append(scheme+'://'+domain+fn)
        return files

    def load(self):
        log.debug('Hdfs read for {0}.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)

        reader = c.read(path)
        r = BytesIO(b''.join(reader))
        reader.close()

        return r

    def load_text(self):
        log.debug('Hdfs text read for {0}.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)

        reader = c.read(path)
        r = StringIO(''.join(reader))
        reader.close()

        return r

    def dump(self, stream):
        log.debug('Dump to {0} with hdfs write.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)
        c.write(path, stream)
        return self
