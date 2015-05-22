from __future__ import absolute_import

import logging

from . import fs

log = logging.getLogger(__name__)


class File(object):
    def __init__(self):
        """This is an abstract class. Use WholeFile or any other future
        implementation."""
        pass

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
