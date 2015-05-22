from __future__ import absolute_import

import boto
import fnmatch
from io import BytesIO

from ...utils import Tokenizer
from .file_system import FileSystem


class S3(FileSystem):
    _conn = None

    def __init__(self, file_name):
        FileSystem.__init__(self, file_name)

        # obtain key
        t = Tokenizer(self.file_name)
        t.next('//')  # skip scheme
        bucket_name = t.next('/')
        key_name = t.next()
        conn = S3._get_conn()
        bucket = conn.get_bucket(bucket_name, validate=False)
        self.key = bucket.get_key(key_name)
        if not self.key:
            self.key = bucket.new_key(key_name)

    @staticmethod
    def _get_conn():
        if not S3._conn:
            S3._conn = boto.connect_s3()
        return S3._conn

    @staticmethod
    def exists(path):
        t = Tokenizer(path)
        t.next('//')  # skip scheme
        bucket_name = t.next('/')
        key_name = t.next()
        conn = S3._get_conn()
        bucket = conn.get_bucket(bucket_name, validate=False)
        return (bucket.get_key(key_name) or
                any(True for _ in bucket.list(prefix=key_name+'/')))

    @staticmethod
    def resolve_filenames(expr):
        files = []

        t = Tokenizer(expr)
        scheme = t.next('://')
        bucket_name = t.next('/')
        prefix = t.next(['*', '?'])

        bucket = S3._get_conn().get_bucket(
            bucket_name,
            validate=False
        )
        expr_after_bucket = expr[len(scheme)+3+len(bucket_name)+1:]
        for k in bucket.list(prefix=prefix):
            if fnmatch.fnmatch(k.name, expr_after_bucket) or \
               fnmatch.fnmatch(k.name, expr_after_bucket+'/part*'):
                files.append(scheme+'://'+bucket_name+'/'+k.name)
        return files

    def load(self):
        return BytesIO(self.key.get_contents_as_string())

    def dump(self, stream):
        self.key.set_contents_from_string(b''.join(stream))
        return self

    def make_public(self, recursive=False):
        self.key.make_public(recursive)
        return self
