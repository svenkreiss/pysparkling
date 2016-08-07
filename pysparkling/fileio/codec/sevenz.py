from __future__ import absolute_import

try:
    import py7zlib
except ImportError:
    py7zlib = None

from io import BytesIO
import logging

from .codec import Codec

log = logging.getLogger(__name__)


class SevenZ(Codec):
    """Implementation of :class:`.Codec` for 7z compression.

    Needs the `pylzma` module.
    """

    def __init__(self):
        if py7zlib is None:
            log.warn('py7zlib could not be imported. To read 7z files, '
                     'install the library with "pip install pylzma".')
        super(SevenZ, self).__init__()

    def compress(self, stream):
        log.warn('Writing of 7z compressed archives is not supported.')
        return stream

    def decompress(self, stream):
        if py7zlib is None:
            return Codec.decompress(self, stream)

        uncompressed = BytesIO()

        f = py7zlib.Archive7z(file=stream)
        for f_name in f.getnames():
            uncompressed.write(f.getmember(f_name).read())

        uncompressed.seek(0)
        return uncompressed
