from __future__ import absolute_import

import bz2
import gzip
import logging
from io import BytesIO

from .file import File
from ..utils import Tokenizer

log = logging.getLogger(__name__)


class WholeFile(File):
    def __init__(self, file_name):
        """context is optional"""
        File.__init__(self)

        self.file_name = file_name

    def open_read(self):
        file_obj = None

        # read
        if self.file_name.startswith(('s3://', 's3n://')):
            t = Tokenizer(self.file_name)
            t.next('//')  # skip scheme
            bucket_name = t.next('/')
            key_name = t.next()
            conn = File._get_s3_conn()
            bucket = conn.get_bucket(bucket_name, validate=False)
            key = bucket.get_key(key_name)
            file_obj = BytesIO(key.get_contents_as_string())
        else:
            f_name_local = self.file_name
            if f_name_local.startswith('file://'):
                f_name_local = f_name_local[7:]
            file_obj = open(f_name_local, 'rb')

        # decompress
        if self.file_name.endswith('.gz') or '.gz/part-' in self.file_name:
            log.info('Using gzip decompression: {0}'
                     ''.format(self.file_name))
            file_obj = gzip.GzipFile(fileobj=file_obj, mode='rb')
        if self.file_name.endswith('.bz2') or '.bz2/part-' in self.file_name:
            log.info('Using bz2 decompression: {0}'.format(self.file_name))
            file_obj = BytesIO(bz2.decompress(file_obj.read()))

        return file_obj

    def open_write(self, stream):
        """Stream could be a BytesIO instance."""

        # compress
        if self.file_name.endswith('.gz') or '.gz/part-' in self.file_name:
            log.debug('Compressing with gzip: {0}'.format(self.file_name))
            compressed = BytesIO()
            with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
                for x in stream:
                    f.write(x)
            stream = BytesIO(compressed.getvalue())
        elif self.file_name.endswith('.bz2') or '.bz2/part-' in self.file_name:
            log.debug('Compressing with bz2: {0}'.format(self.file_name))
            stream = BytesIO(bz2.compress(b''.join(stream)))

        # write
        if self.file_name.startswith(('s3://', 's3n://')):
            t = Tokenizer(self.file_name)
            t.next('//')  # skip scheme
            bucket_name = t.next('/')
            key_name = t.next()
            conn = self.context._get_s3_conn()
            bucket = conn.get_bucket(bucket_name, validate=False)
            key = bucket.new_key(key_name)
            key.set_contents_from_string(b''.join(stream))
        else:
            path_local = self.file_name
            if path_local.startswith('file://'):
                path_local = path_local[7:]
            log.info('writing file {0}'.format(path_local))
            with open(path_local, 'wb') as f:
                for c in stream:
                    f.write(c)
