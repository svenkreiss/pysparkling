from __future__ import absolute_import

from io import BytesIO
import logging
import zipfile

from .codec import Codec

log = logging.getLogger(__name__)


class Zip(Codec):
    """Implementation of :class:`.Codec` for zip compression."""

    def __init__(self):
        super(Zip, self).__init__()

    def compress(self, stream):
        compressed = BytesIO()

        with zipfile.ZipFile(file=compressed, mode='w', allowZip64=True) as f:
            f.writestr('data', stream.read())

        compressed.seek(0)
        return compressed

    def decompress(self, stream):
        uncompressed = BytesIO()

        with zipfile.ZipFile(file=stream, mode='r', allowZip64=True) as f:
            for f_name in f.namelist():
                uncompressed.write(f.read(f_name))

        uncompressed.seek(0)
        return uncompressed
