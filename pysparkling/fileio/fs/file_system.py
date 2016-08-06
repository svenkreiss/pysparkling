import logging

log = logging.getLogger(__name__)


class FileSystem(object):
    """Interface class for the file system.

    :param str file_name: File name.
    """
    def __init__(self, file_name):
        self.file_name = file_name

    @staticmethod
    def resolve_filenames(expr):
        """Resolve the given glob-like expression to filenames.

        :rtype: list
        """
        log.error('Cannot resolve: {0}'.format(expr))

    def exists(self):
        """Check whether the given file_name exists.

        :rtype: bool
        """
        log.warning('Could not determine whether {0} exists due to '
                    'unhandled scheme.'.format(self.file_name))

    def load(self):
        """Load a file to a stream.

        :rtype: io.BytesIO
        """
        log.error('Cannot load: {0}'.format(self.file_name))

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        """Load a file to a stream.

        :param str encoding: Text encoding.
        :param str encoding_errors: How to handle encoding errors.

        :rtype: io.StringIO
        """
        log.error('Cannot load: {0}'.format(self.file_name))

    def dump(self, stream):
        """Dump a stream to a file.

        :param io.BytesIO stream: Input tream.
        """
        log.error('Cannot dump: {0}'.format(self.file_name))

    def make_public(self, recursive=False):
        """Make the file public (only on some file systems).

        :param bool recursive: Recurse.
        :rtype: FileSystem
        """
        log.warning('Cannot make {0} public.'.format(self.file_name))
