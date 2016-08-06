from __future__ import absolute_import

from fnmatch import fnmatch
from io import BytesIO, StringIO
import logging

from ...exceptions import FileSystemNotSupported
from ...utils import Tokenizer
from .file_system import FileSystem

log = logging.getLogger(__name__)

try:
    import boto
except ImportError:
    boto = None


class S3(FileSystem):
    """:class:`.FileSystem` implementation for S3.

    Use environment variables ``AWS_SECRET_ACCESS_KEY`` and
    ``AWS_ACCESS_KEY_ID`` for auth and use file paths of the form
    ``s3://bucket_name/filename.txt``.
    """

    #: Keyword arguments for new connections.
    #: Example: set to `{'anon': True}` for anonymous connections.
    connection_kwargs = {}

    _conn = None

    def __init__(self, file_name):
        if boto is None:
            raise FileSystemNotSupported('S3 not supported. Install "boto".')

        super(S3, self).__init__(file_name)

        # obtain key
        t = Tokenizer(self.file_name)
        t.next('://')  # skip scheme
        bucket_name = t.next('/')
        key_name = t.next()
        conn = self._get_conn()
        bucket = conn.get_bucket(bucket_name, validate=False)
        self.key = bucket.get_key(key_name)
        if not self.key:
            self.key = bucket.new_key(key_name)

    @classmethod
    def _get_conn(cls):
        if not cls._conn:
            cls._conn = boto.connect_s3(**cls.connection_kwargs)
        return cls._conn

    @classmethod
    def resolve_filenames(cls, expr):
        files = []

        t = Tokenizer(expr)
        scheme = t.next('://')
        bucket_name = t.next('/')
        prefix = t.next(['*', '?'])

        bucket = cls._get_conn().get_bucket(
            bucket_name,
            validate=False
        )
        expr = expr[len(scheme) + 3 + len(bucket_name) + 1:]
        for k in bucket.list(prefix=prefix):
            if fnmatch(k.name, expr) or fnmatch(k.name, expr + '/part*'):
                files.append('{0}://{1}/{2}'.format(
                    scheme,
                    bucket_name,
                    k.name,
                ))
        return files

    def exists(self):
        t = Tokenizer(self.file_name)
        t.next('//')  # skip scheme
        bucket_name = t.next('/')
        key_name = t.next()
        conn = self._get_conn()
        bucket = conn.get_bucket(bucket_name, validate=False)
        return (bucket.get_key(key_name) or
                bucket.list(prefix='{}/'.format(key_name)))

    def load(self):
        log.debug('Loading {0} with size {1}.'
                  ''.format(self.key.name, self.key.size))
        return BytesIO(self.key.get_contents_as_string())

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        log.debug('Loading {0} with size {1}.'
                  ''.format(self.key.name, self.key.size))
        return StringIO(
            self.key.get_contents_as_string().decode(encoding, encoding_errors)
        )

    def dump(self, stream):
        log.debug('Dumping to {0}.'.format(self.key.name))
        self.key.set_contents_from_file(stream)
        return self

    def make_public(self, recursive=False):
        self.key.make_public(recursive)
        return self
