from __future__ import absolute_import

import struct
import logging

from tornado.gen import coroutine
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError

log = logging.getLogger(__name__)


class TCPTextStream(TCPServer):
    def __init__(self, delimiter=b'\n'):
        if TCPServer is False:
            log.error('Run \'pip install tornado\' to use TCPStream.')

        super(TCPTextStream, self).__init__()
        self.delimiter = delimiter
        self.buffer = []

    def get(self):
        r = self.buffer
        self.buffer = []
        return [r] if r else []

    @coroutine
    def handle_stream(self, stream, address):
        try:
            data = yield stream.read_until(self.delimiter)
        except StreamClosedError:
            return
        stream.close()
        self.buffer.append(data[:-1].decode('utf8'))


class TCPBinaryStream(TCPServer):
    """
    :param length:
        An int for fixed message lengths in bytes.

        For variable length messages where the message length is sent right
        before the message itself, ``length`` is
        a format string that can be passed to ``struct.unpack()``.
        For example, use ``length='<I'`` for a little-endian (standard on x86)
        32-bit unsigned int.
    """
    def __init__(self, length=None):
        if TCPServer is False:
            log.error('Run \'pip install tornado\' to use TCPStream.')

        super(TCPBinaryStream, self).__init__()
        self.length = length
        self.buffer = []

        self.prefix_length = None
        if not isinstance(self.length, int):
            self.prefix_length = struct.calcsize(self.length)

    def get(self):
        r = self.buffer
        self.buffer = []
        return [r] if r else []

    @coroutine
    def handle_stream(self, stream, address):
        try:
            if self.prefix_length:
                prefix = yield stream.read_bytes(self.prefix_length)
                message_length = struct.unpack(self.length, prefix)[0]
            else:
                message_length = self.length
            data = yield stream.read_bytes(message_length)
        except StreamClosedError:
            return
        stream.close()
        self.buffer.append(data)
