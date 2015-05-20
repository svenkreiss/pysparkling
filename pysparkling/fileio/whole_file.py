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
        self.path_type = File.path_type(file_name)

    def load(self):
        stream = None

        # read
        if self.path_type == 's3':
            t = Tokenizer(self.file_name)
            t.next('//')  # skip scheme
            bucket_name = t.next('/')
            key_name = t.next()
            conn = File._get_s3_conn()
            bucket = conn.get_bucket(bucket_name, validate=False)
            key = bucket.get_key(key_name)
            stream = BytesIO(key.get_contents_as_string())
        elif self.path_type == 'local':
            f_name_local = self.file_name
            if f_name_local.startswith('file://'):
                f_name_local = f_name_local[7:]
            with open(f_name_local, 'rb') as f:
                stream = BytesIO(f.read())

        # decompress
        if self.file_name.endswith('.gz'):
            log.info('Using gzip decompression: {0}'
                     ''.format(self.file_name))
            stream = gzip.GzipFile(fileobj=stream, mode='rb')
        if self.file_name.endswith('.bz2'):
            log.info('Using bz2 decompression: {0}'.format(self.file_name))
            stream = BytesIO(bz2.decompress(stream.read()))

        return stream

    def dump(self, stream=None):
        """Stream could be a BytesIO instance."""
        if stream is None:
            stream = BytesIO()

        # compress
        if self.file_name.endswith('.gz'):
            log.debug('Compressing with gzip: {0}'.format(self.file_name))
            compressed = BytesIO()
            with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
                for x in stream:
                    f.write(x)
            stream = BytesIO(compressed.getvalue())
        elif self.file_name.endswith('.bz2'):
            log.debug('Compressing with bz2: {0}'.format(self.file_name))
            stream = BytesIO(bz2.compress(b''.join(stream)))

        # write
        if self.path_type == 's3':
            t = Tokenizer(self.file_name)
            t.next('//')  # skip scheme
            bucket_name = t.next('/')
            key_name = t.next()
            conn = File._get_s3_conn()
            bucket = conn.get_bucket(bucket_name, validate=False)
            key = bucket.new_key(key_name)
            key.set_contents_from_string(b''.join(stream))
        elif self.path_type == 'local':
            path_local = self.file_name
            if path_local.startswith('file://'):
                path_local = path_local[7:]
            log.info('writing file {0}'.format(path_local))
            with open(path_local, 'wb') as f:
                for c in stream:
                    f.write(c)

        return self

    def make_public(self, recursive=False):
        if self.path_type == 's3':
            t = Tokenizer(self.file_name)
            t.next('//')  # skip scheme
            bucket_name = t.next('/')
            key_name = t.next()
            conn = File._get_s3_conn()
            bucket = conn.get_bucket(bucket_name, validate=False)
            key = bucket.get_key(key_name)
            key.make_public(recursive)
        else:
            log.error('Cannot make this file public.')

        return self
