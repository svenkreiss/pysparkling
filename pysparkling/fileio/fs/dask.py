from __future__ import absolute_import, unicode_literals

from .file_system import FileSystem
from .local import Local
import logging

log = logging.getLogger(__name__)


class Dask(FileSystem):
    """:class:`.FileSystem` implementation for distributed dask files."""

    client = None

    def __init__(self, file_name):
        super(Dask, self).__init__(file_name)

    @staticmethod
    def resolve_filenames(expr):
        if expr.startswith('dask://'):
            expr = expr[7:]

        workers = None
        prefix = 'dask://'
        if ':' in expr:
            worker_name, _, expr = expr.partition(':')
            prefix += '{}:'.format(worker_name)
            workers = [worker_name]

        submission = Dask.client.submit(Local.resolve_filenames, expr,
                                        workers=workers)
        filenames = submission.result()
        return [prefix + file_name for file_name in filenames]

    def workers_and_path(self):
        if self.file_name.startswith('dask://'):
            path = self.file_name[7:]

        workers = None
        if ':' in path:
            worker_name, _, path = path.partition(':')
            workers = [worker_name]

        return workers, path

    def exists(self):
        workers, path = self.workers_and_path()
        return Dask.client.submit(lambda p: Local(p).exists(), path,
                                  workers=workers).result()

    def load(self):
        workers, path = self.workers_and_path()
        return Dask.client.submit(lambda p: Local(p).load(), path,
                                  workers=workers).result()

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        workers, path = self.workers_and_path()
        return Dask.client.submit(
            lambda p: Local(p).load_text(encoding, encoding_errors),
            path,
            workers=workers,
        ).result()

    def dump(self, stream):
        workers, path = self.workers_and_path()
        return Dask.client.submit(
            lambda p: Local(p).dump(stream),
            path,
            workers=workers,
        ).result()
