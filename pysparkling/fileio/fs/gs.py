from __future__ import absolute_import

from fnmatch import fnmatch
import logging
from io import BytesIO, StringIO

from ...exceptions import FileSystemNotSupported
from ...utils import Tokenizer, parse_file_uri
from .file_system import FileSystem

log = logging.getLogger(__name__)

try:
    from gcloud import storage
except ImportError as e:
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
            if storage is None:
                raise FileSystemNotSupported(
                    'Google Storage is not supported. Install "gcloud".'
                )
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
                files.append(f'{scheme}://{project_name}:{bucket_name}/{k.name}')
        return files

    @staticmethod
    def resolve_content(expr):
        scheme, raw_bucket_name, folder_path, pattern = parse_file_uri(expr)

        if ':' in raw_bucket_name:
            project_name, _, bucket_name = raw_bucket_name.partition(':')
        else:
            project_name = GS.project_name
            bucket_name = raw_bucket_name

        folder_path = folder_path[1:]  # Remove leading slash

        expr = f"{folder_path}{pattern}"
        # Match all files inside folders that match expr
        pattern_expr = f"{expr}{'' if expr.endswith('/') else '/'}*"

        bucket = GS._get_client(project_name).get_bucket(bucket_name)

        files = []
        for k in bucket.list_blobs(prefix=folder_path):
            if not k.name.endswith("/") and (
                    fnmatch(k.name, expr) or fnmatch(k.name, pattern_expr)
            ):
                files.append(
                    f'{scheme}://{raw_bucket_name}/{k.name}'
                )
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
        return (bucket.get_blob(blob_name)
                or list(bucket.list_blobs(prefix=f'{blob_name}/')))

    def load(self):
        log.debug(f'Loading {self.blob.name} with size {self.blob.size}.')
        return BytesIO(self.blob.download_as_string())

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        log.debug(f'Loading {self.blob.name} with size {self.blob.size}.')
        return StringIO(
            self.blob.download_as_string().decode(
                encoding, encoding_errors
            )
        )

    def dump(self, stream):
        log.debug(f'Dumping to {self.blob.name}.')
        self.blob.upload_from_string(stream.read(),
                                     content_type=self.mime_type)
        return self

    def make_public(self, recursive=False):
        self.blob.make_public(recursive)
        return self
