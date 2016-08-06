from __future__ import absolute_import, unicode_literals

from fnmatch import fnmatch
import logging
from io import BytesIO, StringIO

from ...exceptions import FileSystemNotSupported
from ...utils import Tokenizer
from .file_system import FileSystem

log = logging.getLogger(__name__)

try:
    import hdfs
except ImportError:
    hdfs = None


class Hdfs(FileSystem):
    """:class:`.FileSystem` implementation for HDFS."""

    _conn = {}

    def __init__(self, file_name):
        if hdfs is None:
            raise FileSystemNotSupported(
                'hdfs not supported. Install the python package "hdfs".'
            )

        super(Hdfs, self).__init__(file_name)

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
        cache_id = domain + '__' + str(port)

        if cache_id not in Hdfs._conn:
            Hdfs._conn[cache_id] = hdfs.InsecureClient(
                'http://{0}:{1}'.format(domain, port)
            )
        return Hdfs._conn[cache_id], '/' + path

    def exists(self):
        c, p = Hdfs.client_and_path(self.file_name)
        try:
            c.status(p)
        except hdfs.util.HdfsError:
            return False
        return True

    @staticmethod
    def resolve_filenames(expr):
        c, _ = Hdfs.client_and_path(expr)

        t = Tokenizer(expr)
        scheme = t.next('://')
        domain = t.next('/')
        fixed_path = t.next(['*', '?'])
        file_expr = t.next()

        if file_expr and '/' in fixed_path:
            file_expr = fixed_path[fixed_path.rfind('/') + 1:] + file_expr
            fixed_path = fixed_path[:fixed_path.rfind('/')]
        # file_expr is only the actual file expression if there was a * or ?.
        # Handle this case.
        if not file_expr:
            if '/' in fixed_path:
                file_expr = fixed_path[fixed_path.rfind('/') + 1:]
                fixed_path = fixed_path[:fixed_path.rfind('/')]
            else:
                file_expr = fixed_path
                fixed_path = ''

        files = []
        for fn in c.list('{}/'.format(fixed_path), status=False):
            if fnmatch(fn, file_expr) or fnmatch(fn, file_expr + '/part*'):
                files.append('{0}://{1}/{2}/{3}'.format(
                    scheme, domain, fixed_path, fn))
        return files

    def load(self):
        log.debug('Hdfs read for {0}.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)

        with c.read(path) as reader:
            r = BytesIO(reader.read())

        return r

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        log.debug('Hdfs text read for {0}.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)

        with c.read(path) as reader:
            r = StringIO(reader.read().decode(encoding, encoding_errors))

        return r

    def dump(self, stream):
        log.debug('Dump to {0} with hdfs write.'.format(self.file_name))
        c, path = Hdfs.client_and_path(self.file_name)
        c.write(path, stream)
        return self
