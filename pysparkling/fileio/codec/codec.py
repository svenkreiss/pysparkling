import logging

log = logging.getLogger(__name__)


class Codec(object):
    def __init__(self):
        pass

    def compress(self, stream):
        return stream

    def decompress(self, stream):
        return stream
