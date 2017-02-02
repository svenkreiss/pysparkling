from __future__ import absolute_import

import logging

from ..fileio import File
from ..rdd import EmptyRDD

log = logging.getLogger(__name__)


class FileStreamDeserializer(object):
    def __init__(self, context):
        self.context = context

    def __call__(self, path):
        if path is None:
            return EmptyRDD(self.context)

        return self.context.textFile(path)


class FileTextStream(object):
    def __init__(self, path):
        self.path = path
        self.files_done = set()

    def get(self):
        files = [fn for fn in File.resolve_filenames(self.path)
                 if fn not in self.files_done]
        if not files:
            return None

        self.files_done |= set(files)
        return ','.join(files)

    def stop(self):
        pass
