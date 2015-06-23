from __future__ import absolute_import

import logging
from io import BytesIO, StringIO

from ...utils import Tokenizer
from .file_system import FileSystem
from ...exceptions import FileSystemNotSupported

log = logging.getLogger(__name__)

try:
    import hdfs
    SUPPORTED = True
except ImportError:
    SUPPORTED = False


class Hdfs(FileSystem):
    _conn = None

    def __init__(self, file_name):
        if not SUPPORTED:
            raise FileSystemNotSupported(
                'hdfs not supported. Install "snakebite".'
            )

        FileSystem.__init__(self, file_name)

    @staticmethod
    def client_and_path(path):
        # obtain key
        t = Tokenizer(path)
        t.next('://')  # skip scheme
        domain = t.next('/')
        path = t.next()

        if ':' in domain:
            port = 8020
        else:
            domain, port = domain.split(':')
            port = int(port)
        cache_id = domain+'__'+str(port)

        if cache_id not in Hdfs._conn:
            Hdfs._conn[cache_id] = hdfs.KerberosClient(
                'http://{0}:{1}'.format(domain, port)
            )
        return (Hdfs._conn[cache_id], path)

    @staticmethod
    def exists(path):
        c, p = Hdfs.client_and_path(path)
        try:
            c.status(p)
        except hdfs.util.HdfsError:
            return False
        return True

    @staticmethod
    def resolve_filenames(expr):
        c, expr = Hdfs.client_and_path(expr)
        return [f[0] for f in c.list(expr)]

    def load(self):
        log.debug('Hdfs read for {0}.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)

        reader = c.read(path)
        r = BytesIO(sum(reader))
        reader.close()

        return r

    def dump(self, stream):
        log.debug('Dump to {0} with hdfs write.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)
        c.write(path, stream)
        return self
