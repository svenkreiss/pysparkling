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
        log.error(f'Cannot resolve: {expr}')

    @staticmethod
    def resolve_content(expr):
        """Return all the files matching expr or in a folder matching expr

        :rtype: list
        """
        log.error(f'Cannot resolve: {expr}')

    def exists(self):
        """Check whether the given file_name exists.

        :rtype: bool
        """
        log.warning(f'Could not determine whether {self.file_name} exists due to unhandled scheme.')

    def load(self):
        """Load a file to a stream.

        :rtype: io.BytesIO
        """
        log.error(f'Cannot load: {self.file_name}')

    def load_text(self, encoding='utf8', encoding_errors='ignore'):
        """Load a file to a stream.

        :param str encoding: Text encoding.
        :param str encoding_errors: How to handle encoding errors.

        :rtype: io.StringIO
        """
        log.error(f'Cannot load: {self.file_name}')

    def dump(self, stream):
        """Dump a stream to a file.

        :param io.BytesIO stream: Input tream.
        """
        log.error(f'Cannot dump: {self.file_name}')

    def make_public(self, recursive=False):
        """Make the file public (only on some file systems).

        :param bool recursive: Recurse.
        :rtype: FileSystem
        """
        log.warning(f'Cannot make {self.file_name} public.')
