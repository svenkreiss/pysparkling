import bz2
import logging
from io import BytesIO

from .codec import Codec

log = logging.getLogger(__name__)


class Bz2(Codec):
    """Implementation of :class:`.Codec` for bz2 compression."""

    def compress(self, stream):
        return BytesIO(bz2.compress(b''.join(stream)))

    def decompress(self, stream):
        return BytesIO(bz2.decompress(stream.read()))
