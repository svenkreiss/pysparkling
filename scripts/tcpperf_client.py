"""Sends tcp messages."""
import argparse
from contextlib import closing
import json
import random
import struct
import sys
import time

from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient


class Emitter:
    def __init__(self, port, n=1000, values=1, duration=3.0):
        self.port = port
        self.n = n
        self.values = values
        self.duration = duration
        self.message = self.hello
        self.i = 0

        self.pcb = None
        self.client = None

    def start(self):
        self.client = TCPClient()

        self.pcb = PeriodicCallback(self.send, 1000.0 / self.n)
        self.pcb.start()

        IOLoop.current().call_later(self.duration + 0.5, self.stop)
        IOLoop.current().start()
        IOLoop.clear_current()

    def stop(self):
        if self.pcb is not None:
            self.pcb.stop()
        if self.client is not None:
            self.client.close()
        IOLoop.current().stop()

    @gen.coroutine
    def send(self):
        if self.i >= self.duration * self.n * self.values:
            self.pcb.stop()
            return

        try:
            stream = yield self.client.connect('127.0.0.1', self.port)
            with closing(stream):
                messages = b''.join(self.message() for _ in range(self.values))
                stream.write(messages)
                self.i += self.values
        except StreamClosedError:
            return

    def hello(self):
        return b'hello\n'

    def r(self):
        s = random.randint(1, 10)
        v = s / 10.0 + (1.5 - s / 10.0) * random.random()
        return (s, v)

    def text(self):
        return 'sensor{}|{}\n'.format(*self.r()).encode('utf8')

    def json(self):
        s, v = self.r()
        return (json.dumps({
            f'sensor{s}': v,
        }) + '\n').encode('utf8')

    def bello(self):
        # 5 bytes
        return b'bello'

    def struct(self):
        # 8 bytes
        return struct.pack('If', *self.r())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-n', type=int, default=1000,
                        help='number of connections')
    parser.add_argument('--values', type=int, default=1,
                        help='number of values per connection')
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
    e = Emitter(args.port, args.n, args.values)
    e.message = getattr(e, args.format)
    e.start()
    print(f'{sys.argv[0]} sent {e.i} messages')


if __name__ == '__main__':
    main()
