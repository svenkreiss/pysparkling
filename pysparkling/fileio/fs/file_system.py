import io
import logging
import typing as t

log = logging.getLogger(__name__)


class FileSystem:
    """Interface class for the file system.

    :param str file_name: File name.
    """
    def __init__(self, file_name: str):
        self.file_name: str = file_name

    @staticmethod
    def resolve_filenames(expr: str) -> t.List[str]:
        """Resolve the given glob-like expression to filenames.

        :rtype: list
        """
        log.error('Cannot resolve: %s', expr)
        raise NotImplementedError

    @staticmethod
    def resolve_content(expr: str) -> t.List[str]:
        """Return all the files matching expr or in a folder matching expr

        :rtype: list
        """
        log.error('Cannot resolve: %s', expr)
        raise NotImplementedError

    def exists(self) -> bool:
        """Check whether the given file_name exists.

        :rtype: bool
        """
        log.warning('Could not determine whether %s exists due to unhandled scheme.', self.file_name)
        raise NotImplementedError

    def load(self) -> io.BytesIO:
        """Load a file to a stream."""
        log.error('Cannot load: %s', self.file_name)
        raise NotImplementedError

    def load_text(self, encoding: str = 'utf8', encoding_errors: str = 'ignore') -> io.StringIO:
        """Load a file to a stream.

        :param str encoding: Text encoding.
        :param str encoding_errors: How to handle encoding errors.
        """
        log.error('Cannot load: %s', self.file_name)
        raise NotImplementedError

    def dump(self, stream: io.BytesIO):
        """Dump a stream to a file.

        :param io.BytesIO stream: Input tream.
        """
        log.error('Cannot dump: %s', self.file_name)
        raise NotImplementedError

    def make_public(self, recursive=False):
        """Make the file public (only on some file systems).

        :param bool recursive: Recurse.
        :rtype: FileSystem
        """
        log.warning('Cannot make %s public.', self.file_name)
        raise NotImplementedError
