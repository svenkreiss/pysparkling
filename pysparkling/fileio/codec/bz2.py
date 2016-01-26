from __future__ import absolute_import

import bz2
import logging
from io import BytesIO

from .codec import Codec

log = logging.getLogger(__name__)


class Bz2(Codec):
    def __init__(self):
        super(Bz2, self).__init__()

    def compress(self, stream):
        return BytesIO(bz2.compress(b''.join(stream)))

    def decompress(self, stream):
        return BytesIO(bz2.decompress(stream.read()))
