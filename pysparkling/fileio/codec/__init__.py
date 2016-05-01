from __future__ import absolute_import

import logging

from .bz2 import Bz2
from .codec import Codec
from .gz import Gz
from .lzma import Lzma
from .sevenz import SevenZ
from .tar import Tar, TarGz, TarBz2
from .zip import Zip

log = logging.getLogger(__name__)

FILE_ENDINGS = [
    (('.tar',), Tar),
    (('.tar.gz',), TarGz),
    (('.tar.bz2',), TarBz2),
    (('.gz',), Gz),
    (('.zip',), Zip),
    (('.bz2',), Bz2),
    (('.lzma', '.xz'), Lzma),
    (('.7z',), SevenZ),
]


def get_codec(path):
    """Find the codec implementation for this path."""
    if '.' not in path or path.rfind('/') > path.rfind('.'):
        return Codec

    for endings, codec_class in FILE_ENDINGS:
        if any(path.endswith(e) for e in endings):
            log.debug('Using {0} codec: {1}'.format(endings, path))
            return codec_class

    return Codec
