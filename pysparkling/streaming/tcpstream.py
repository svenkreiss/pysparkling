from __future__ import absolute_import

import logging
import struct
from tornado.gen import coroutine, moment
from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer

from ..rdd import EmptyRDD

log = logging.getLogger(__name__)


class TCPDeserializer(object):
    def __init__(self, context):
        self.context = context

    def __call__(self, data):
        if data is None:
            return EmptyRDD(self.context)

        return self.context.parallelize(data)


class TCPTextStream(TCPServer):
    def __init__(self, delimiter=b'\n'):
        super(TCPTextStream, self).__init__()
        self.delimiter = delimiter
        self.buffer = []

    def get(self):
        if not self.buffer:
            return []

        buffer = self.buffer
        self.buffer = []
        return buffer

    @coroutine
    def handle_stream(self, stream, address):
        try:
            while True:
                for _ in range(100):
                    data = yield stream.read_until(self.delimiter)
                    self.buffer.append(data[:-1].decode('utf8'))
                yield moment
        except StreamClosedError:
            pass


class TCPBinaryStream(TCPServer):
    """Consumes binary messages from a TCP socket.

    :param length: An int or string.
    """

    def __init__(self, length=None):
        super(TCPBinaryStream, self).__init__()
        self.length = length
        self.buffer = []

        self.prefix_length = None
        if not isinstance(self.length, int):
            self.prefix_length = struct.calcsize(self.length)

    def get(self):
        if not self.buffer:
            return []

        buffer = self.buffer
        self.buffer = []
        return buffer

    @coroutine
    def handle_stream(self, stream, address):
        try:
            while True:
                for _ in range(100):
                    if self.prefix_length:
                        prefix = yield stream.read_bytes(self.prefix_length)
                        message_length = struct.unpack(self.length, prefix)[0]
                    else:
                        message_length = self.length
                    data = yield stream.read_bytes(message_length)
                    self.buffer.append(data)
                yield moment
        except StreamClosedError:
            return
