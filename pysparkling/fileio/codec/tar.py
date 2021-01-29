import logging
import tarfile
from io import BytesIO

from .codec import Codec

log = logging.getLogger(__name__)


class Tar(Codec):
    """Implementation of :class:`.Codec` for tar compression."""

    def compress(self, stream):
        compressed = BytesIO()

        with tarfile.open(fileobj=compressed, mode='w') as f:
            s = stream.read()

            t = tarfile.TarInfo('data')
            t.size = len(s)

            f.addfile(t, BytesIO(s))

        compressed.seek(0)
        return compressed

    def decompress(self, stream):
        uncompressed = BytesIO()

        with tarfile.open(fileobj=stream, mode='r') as f:
            for tar_info in f.getmembers():
                if not tar_info.isfile():
                    continue
                uncompressed.write(f.extractfile(tar_info).read())

        uncompressed.seek(0)
        return uncompressed


class TarGz(Codec):
    """Implementation of :class:`.Codec` for .tar.gz compression."""

    def compress(self, stream):
        compressed = BytesIO()

        with tarfile.open(fileobj=compressed, mode='w:gz') as f:
            s = stream.read()

            t = tarfile.TarInfo('data')
            t.size = len(s)

            f.addfile(t, BytesIO(s))

        compressed.seek(0)
        return compressed

    def decompress(self, stream):
        uncompressed = BytesIO()

        with tarfile.open(fileobj=stream, mode='r:gz') as f:
            for tar_info in f.getmembers():
                if not tar_info.isfile():
                    continue
                uncompressed.write(f.extractfile(tar_info).read())

        uncompressed.seek(0)
        return uncompressed


class TarBz2(Codec):
    """Implementation of :class:`.Codec` for .tar.bz2 compression."""

    def compress(self, stream):
        compressed = BytesIO()

        with tarfile.open(fileobj=compressed, mode='w:bz2') as f:
            s = stream.read()

            t = tarfile.TarInfo('data')
            t.size = len(s)

            f.addfile(t, BytesIO(s))

        compressed.seek(0)
        return compressed

    def decompress(self, stream):
        uncompressed = BytesIO()

        with tarfile.open(fileobj=stream, mode='r:bz2') as f:
            for tar_info in f.getmembers():
                if not tar_info.isfile():
                    continue
                uncompressed.write(f.extractfile(tar_info).read())

        uncompressed.seek(0)
        return uncompressed
