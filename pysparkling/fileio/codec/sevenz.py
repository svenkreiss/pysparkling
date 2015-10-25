from __future__ import absolute_import

try:
    import py7zlib
except ImportError:
    pass

import logging
from io import BytesIO

from .codec import Codec

LOGGER = logging.getLogger(__name__)


class SevenZ(Codec):
    def __init__(self):
        if 'py7zlib' not in globals():
            LOGGER.warn('py7zlib could not be imported. To read 7z files, '
                        'install the library with "pip install pylzma".')

    def compress(self, stream):
        LOGGER.warn('Writing of 7z compressed archives is not supported.')
        return stream

    def decompress(self, stream):
        if 'py7zlib' not in globals():
            return Codec.decompress(self, stream)

        uncompressed = BytesIO()

        f = py7zlib.Archive7z(file=stream)
        for f_name in f.getnames():
            uncompressed.write(f.getmember(f_name).read())

        uncompressed.seek(0)
        return uncompressed
