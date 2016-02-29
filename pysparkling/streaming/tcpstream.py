from __future__ import absolute_import

import logging

try:
    from tornado.gen import coroutine
    from tornado.tcpserver import TCPServer
    from tornado.iostream import StreamClosedError
except ImportError:
    coroutine = False
    TCPServer = False
    StreamClosedError = False

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
        while True:
            try:
                data = yield stream.read_until(self.delimiter)
            except StreamClosedError:
                return
            self.buffer.append(data[:-1].decode('utf8'))
