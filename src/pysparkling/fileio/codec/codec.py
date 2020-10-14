import logging

log = logging.getLogger(__name__)


class Codec(object):
    """Codec."""
    def __init__(self):
        pass

    def compress(self, stream):
        """Compress.

        :param io.BytesIO stream: Uncompressed input stream.
        :rtype: io.BytesIO
        """
        return stream

    def decompress(self, stream):
        """Decompress.

        :param io.BytesIO stream: Compressed input stream.
        :rtype: io.BytesIO
        """
        return stream
