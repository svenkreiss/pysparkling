from __future__ import absolute_import


# lzma only available in Python >= 3.3
try:
    import lzma
except ImportError:
    pass

import logging
from io import BytesIO

from .codec import Codec

LOGGER = logging.getLogger(__name__)


class Lzma(Codec):
    def __init__(self):
        if 'lzma' not in globals():
            LOGGER.warn('LZMA codec not supported. It is only supported '
                        'in Python>=3.3. Not compressing streams.')

    def compress(self, stream):
        if 'lzma' not in globals():
            return Codec.compress(self, stream)

        return BytesIO(lzma.compress(stream.read()))

    def decompress(self, stream):
        if 'lzma' not in globals():
            return Codec.decompress(self, stream)

        return BytesIO(lzma.decompress(stream.read()))
