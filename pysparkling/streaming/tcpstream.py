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


class TCPStream(object):
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port

        if TCPServer is False:
            log.error('Run \'pip install tornado\' to use TCPStream.')

        self.server = TCPListener()
        self.server.listen(self.port, self.hostname)
        self.server.start()

    def get(self):
        r = self.server.buffer
        self.server.buffer = []
        return [r] if r else []


class TCPListener(TCPServer):
    def __init__(self):
        super(TCPListener, self).__init__()
        self.buffer = []

    @coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                data = yield stream.read_until(b'\n')
            except StreamClosedError:
                return
            self.buffer.append(data[:-1].decode('utf8'))
