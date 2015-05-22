from __future__ import absolute_import

import gzip
import logging
from io import BytesIO

from .codec import Codec

log = logging.getLogger(__name__)


class Gz(Codec):
    def __init__(self):
        pass

    def compress(self, stream):
        compressed = BytesIO()
        with gzip.GzipFile(fileobj=compressed, mode='wb') as f:
            for x in stream:
                f.write(x)
        return BytesIO(compressed.getvalue())

    def decompress(self, stream):
        return gzip.GzipFile(fileobj=stream, mode='rb')
