from __future__ import absolute_import

import gzip
from io import BytesIO
import logging

from .codec import Codec

log = logging.getLogger(__name__)


class Gz(Codec):
    """Implementation of :class:`.Codec` for gz compression."""

    def __init__(self):
        super(Gz, self).__init__()

    def compress(self, stream):
        compressed = BytesIO()

        with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
            f.write(stream.read())

        compressed.seek(0)
        return compressed

    def decompress(self, stream):
        uncompressed = BytesIO()

        with gzip.GzipFile(fileobj=stream, mode='rb') as f:
            uncompressed.write(f.read())

        uncompressed.seek(0)
        return uncompressed
