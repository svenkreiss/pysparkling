"""Sends tcp messages."""

from __future__ import absolute_import, division

import sys
import json
import time
import random
import struct
import argparse
from contextlib import closing
from tornado import gen
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError


class Emitter(object):
    def __init__(self, port, n=1000, duration=3.0):
        self.port = port
        self.n = n
        self.duration = duration
        self.message = self.hello
        self.i = 0

        self.pcb = None
        self.client = None

    def start(self):
        self.client = TCPClient()

        self.pcb = PeriodicCallback(self.send, 1000.0/self.n)
        self.pcb.start()

        IOLoop.current().call_later(self.duration+0.5, self.stop)
        IOLoop.current().start()

    def stop(self):
        if self.pcb is not None:
            self.pcb.stop()
        if self.client is not None:
            self.client.close()
        IOLoop.current().stop()

    @gen.coroutine
    def send(self):
        if self.i >= self.duration*self.n:
            self.pcb.stop()
            return

        try:
            stream = yield self.client.connect('127.0.0.1', self.port)
            with closing(stream):
                stream.write(self.message())
                self.i += 1
        except StreamClosedError:
            return

    def hello(self):
        return b'hello\n'

    def r(self):
        s = random.randint(1, 10)
        v = s/10.0 + (1.5 - s/10.0)*random.random()
        return (s, v)

    def text(self):
        return 'sensor{}|{}\n'.format(*self.r()).encode('utf8')

    def json(self):
        s, v = self.r()
        return (json.dumps({
            'sensor{}'.format(s): v,
        })+'\n').encode('utf8')

    def bello(self):
        # 5 bytes
        return b'bello'

    def struct(self):
        # 8 bytes
        return struct.pack('If', *self.r())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-n', type=int, default=1000,
                        help='messages per second')
    parser.add_argument('--port', type=int, default=8123,
                        help='target port number')
    parser.add_argument('--format', default='hello',
                        help='format of the messages: hello (default), '
                             'text, json, bello (binary hello), '
                             'struct (binary)')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='wait before start sending messages')
    args = parser.parse_args()

    time.sleep(args.delay)
    e = Emitter(args.port, args.n)
    e.message = getattr(e, args.format)
    e.start()
    print('{} sent {} messages'.format(sys.argv[0], e.i))


if __name__ == '__main__':
    main()
