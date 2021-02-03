from __future__ import absolute_import, unicode_literals

from fnmatch import fnmatch
from io import BytesIO, StringIO
import logging

from ...exceptions import FileSystemNotSupported
from ...utils import format_file_uri, parse_file_uri
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

        super().__init__(file_name)

    @staticmethod
    def client_and_path(path):
        _, domain, folder_path, file_pattern = parse_file_uri(path)

        if ':' not in domain:
            port = 50070
        else:
            domain, port = domain.split(':')
            port = int(port)
        cache_id = domain + '__' + str(port)

        if cache_id not in Hdfs._conn:
            if hdfs is None:
                raise FileSystemNotSupported(
                    'hdfs not supported. Install the python package "hdfs".'
                )
            Hdfs._conn[cache_id] = hdfs.InsecureClient(  # pylint: disable=no-member
                f'http://{domain}:{port}'
            )
        return Hdfs._conn[cache_id], folder_path + file_pattern

    def exists(self):
        c, p = Hdfs.client_and_path(self.file_name)
        try:
            c.status(p)
        except hdfs.util.HdfsError:  # pylint: disable=no-member
            return False
        return True

    @staticmethod
    def resolve_filenames(expr):
        c, _ = Hdfs.client_and_path(expr)

        scheme, domain, folder_path, _ = parse_file_uri(expr)

        files = []
        for fn, file_status in c.list(folder_path, status=True):
            file_local_path = f'{folder_path}{fn}'
            file_path = format_file_uri(scheme, domain, file_local_path)
            part_file_expr = expr + ("" if expr.endswith("/") else "/") + 'part*'

            if fnmatch(file_path, expr):
                if file_status["type"] != "DIRECTORY":
                    files.append(file_path)
                else:
                    files += Hdfs._get_folder_part_files(
                        c,
                        scheme,
                        domain,
                        file_local_path,
                        part_file_expr
                    )
            elif fnmatch(file_path, part_file_expr):
                files.append(file_path)
        return files

    @staticmethod
    def _get_folder_part_files(c, scheme, domain, folder_local_path, expr_with_part):
        files = []
        for fn, file_status in c.list(folder_local_path, status=True):
            sub_file_path = format_file_uri(scheme, domain, folder_local_path, fn)
            if fnmatch(sub_file_path, expr_with_part) and file_status["type"] != "DIRECTORY":
                files.append(sub_file_path)
        return files

    @classmethod
    def _get_folder_files_by_expr(cls, c, scheme, domain, folder_path, expr=None):
        """
        Using client c, retrieves all files located in the folder `folder_path` that matches `expr`

        :param c: An HDFS client
        :param scheme: a scheme such as hdfs
        :param domain: a DFS web server
        :param folder_path: a folder path without patterns
        :param expr: a pattern

        :return: list of matching files absolute paths prefixed with the scheme and domain
        """
        file_paths = []
        for fn, file_status in c.list(folder_path, status=True):
            file_local_path = f'{folder_path}{fn}'
            if expr is None or fnmatch(file_local_path, expr):
                if file_status["type"] == "DIRECTORY":
                    file_paths += cls._get_folder_files_by_expr(
                        c,
                        scheme,
                        domain,
                        file_local_path + "/",
                        expr=None
                    )
                else:
                    file_path = format_file_uri(scheme, domain, file_local_path)
                    file_paths.append(file_path)
            elif file_status["type"] == "DIRECTORY":
                file_paths += cls._get_folder_files_by_expr(
                    c, scheme, domain, file_local_path + "/", expr
                )
        return file_paths

    @classmethod
    def resolve_content(cls, expr):
        c, _ = cls.client_and_path(expr)

        scheme, domain, folder_path, pattern = parse_file_uri(expr)

        expr = folder_path + pattern

        return cls._get_folder_files_by_expr(c, scheme, domain, folder_path, expr)

    def load(self):
        log.debug(f'Hdfs read for {self.file_name}.')
        c, path = Hdfs.client_and_path(self.file_name)

        with c.read(path) as reader:
            r = BytesIO(reader.read())

        return r

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        log.debug(f'Hdfs text read for {self.file_name}.')
        c, path = Hdfs.client_and_path(self.file_name)

        with c.read(path) as reader:
            r = StringIO(reader.read().decode(encoding, encoding_errors))

        return r

    def dump(self, stream):
        log.debug(f'Dump to {self.file_name} with hdfs write.')
        c, path = Hdfs.client_and_path(self.file_name)
        c.write(path, stream)
        return self
