from __future__ import absolute_import

import bz2
from io import BytesIO
import logging

from .codec import Codec

log = logging.getLogger(__name__)


class Bz2(Codec):
    """Implementation of :class:`.Codec` for bz2 compression."""

    def __init__(self):
        super(Bz2, self).__init__()

    def compress(self, stream):
        return BytesIO(bz2.compress(b''.join(stream)))

    def decompress(self, stream):
        return BytesIO(bz2.decompress(stream.read()))
