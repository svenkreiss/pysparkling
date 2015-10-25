from __future__ import absolute_import

import py7zlib
import logging
from io import BytesIO

from .codec import Codec

LOGGER = logging.getLogger(__name__)


class SevenZ(Codec):
    def __init__(self):
        pass

    def compress(self, stream):
        LOGGER.warn('Writing of 7z compressed archives is not supported.')
        return stream

    def decompress(self, stream):
        uncompressed = BytesIO()

        f = py7zlib.Archive7z(file=stream)
        for f_name in f.getnames():
            uncompressed.write(f.getmember(f_name).read())

        uncompressed.seek(0)
        return uncompressed
