from __future__ import absolute_import

import logging

from .codec import Codec
from .gz import Gz
from .zip import Zip
from .bz2 import Bz2
from .lzma import Lzma
from .sevenz import SevenZ

log = logging.getLogger(__name__)

FILE_ENDINGS = [
    (('gz'), Gz),
    (('zip'), Zip),
    (('bz2'), Bz2),
    (('lzma', 'xz'), Lzma),
    (('7z', SevenZ)),
]


def get_codec(path):
    """Find the codec implementation for this path."""
    if '.' not in path or path.rfind('/') > path.rfind('.'):
        return Codec
    ending = path[path.rfind('.')+1:]

    for endings, codec_class in FILE_ENDINGS:
        if ending in endings:
            log.debug('Using {0} codec: {1}'.format(ending, path))
            return codec_class

    return Codec
