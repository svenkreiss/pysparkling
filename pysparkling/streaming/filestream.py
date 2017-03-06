from __future__ import absolute_import

import logging

from ..fileio import File
from ..rdd import EmptyRDD

log = logging.getLogger(__name__)


class FileTextStreamDeserializer(object):
    def __init__(self, context):
        self.context = context

    def __call__(self, path):
        if path is None:
            return EmptyRDD(self.context)

        return self.context.textFile(path)


class FileBinaryStreamDeserializer(object):
    def __init__(self, context, recordLength=None):
        self.context = context
        self.record_length = recordLength

    def __call__(self, path):
        if path is None:
            return EmptyRDD(self.context)

        return self.context.binaryRecords(
            path, recordLength=self.record_length)


class FileStream(object):
    def __init__(self, path, process_all=False):
        self.path = path
        self.files_done = set()
        if not process_all:
            self.files_done = set(File.resolve_filenames(self.path))

    def get(self):
        files = [fn for fn in File.resolve_filenames(self.path)
                 if fn not in self.files_done]
        if not files:
            return None

        self.files_done |= set(files)
        return ','.join(files)

    def stop(self):
        pass
