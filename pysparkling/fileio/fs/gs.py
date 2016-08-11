from __future__ import absolute_import

from fnmatch import fnmatch
import logging
from io import BytesIO, StringIO

from ...exceptions import FileSystemNotSupported
from ...utils import Tokenizer
from .file_system import FileSystem

log = logging.getLogger(__name__)

try:
    from gcloud import storage
except ImportError:
    storage = None


class GS(FileSystem):
    """:class:`.FileSystem` implementation for Google Storage.

    Paths are of the form `gs://bucket_name/file_path` or
    `gs://project_name:bucket_name/file_path`.
    """

    #: Set a default project name.
    project_name = None

    #: Default mime type.
    mime_type = 'text/plain'

    _clients = {}

    def __init__(self, file_name):
        if storage is None:
            raise FileSystemNotSupported(
                'Google Storage is not supported. Install "gcloud".'
            )

        super(GS, self).__init__(file_name)

        # obtain key
        t = Tokenizer(self.file_name)
        t.next('://')  # skip scheme
        bucket_name = t.next('/')
        if ':' in bucket_name:
            project_name, _, bucket_name = bucket_name.partition(':')
        else:
            project_name = GS.project_name
        blob_name = t.next()

        client = GS._get_client(project_name)
        bucket = client.get_bucket(bucket_name)
        self.blob = bucket.get_blob(blob_name)
        if not self.blob:
            self.blob = bucket.blob(blob_name)

    @staticmethod
    def _get_client(project_name):
        if project_name not in GS._clients:
            GS._clients[project_name] = storage.Client(project_name)
        return GS._clients[project_name]

    @staticmethod
    def resolve_filenames(expr):
        files = []

        t = Tokenizer(expr)
        scheme = t.next('://')
        bucket_name = t.next('/')
        if ':' in bucket_name:
            project_name, _, bucket_name = bucket_name.partition(':')
        else:
            project_name = GS.project_name
        prefix = t.next(['*', '?'])

        bucket = GS._get_client(project_name).get_bucket(bucket_name)
        expr_s = len(scheme) + 3 + len(project_name) + 1 + len(bucket_name) + 1
        expr = expr[expr_s:]
        for k in bucket.list_blobs(prefix=prefix):
            if fnmatch(k.name, expr) or fnmatch(k.name, expr + '/part*'):
                files.append('{0}://{1}:{2}/{3}'.format(
                    scheme, project_name, bucket_name, k.name))
        return files

    def exists(self):
        t = Tokenizer(self.file_name)
        t.next('//')  # skip scheme
        bucket_name = t.next('/')
        if ':' in bucket_name:
            project_name, _, bucket_name = bucket_name.partition(':')
        else:
            project_name = GS.project_name
        blob_name = t.next()
        bucket = GS._get_client(project_name).get_bucket(bucket_name)
        return (bucket.get_blob(blob_name) or
                list(bucket.list_blobs(prefix='{}/'.format(blob_name))))

    def load(self):
        log.debug('Loading {0} with size {1}.'
                  ''.format(self.blob.name, self.blob.size))
        return BytesIO(self.blob.download_as_string())

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        log.debug('Loading {0} with size {1}.'
                  ''.format(self.blob.name, self.blob.size))
        return StringIO(
            self.blob.download_as_string().decode(
                encoding, encoding_errors
            )
        )

    def dump(self, stream):
        log.debug('Dumping to {0}.'.format(self.blob.name))
        self.blob.upload_from_string(stream.read(),
                                     content_type=self.mime_type)
        return self

    def make_public(self, recursive=False):
        self.blob.make_public(recursive)
        return self
