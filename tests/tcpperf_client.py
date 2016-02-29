"""Sends tcp messages."""

from __future__ import absolute_import, division

import sys
import time
import random
import argparse
from contextlib import closing
from tornado import gen
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop, PeriodicCallback


class Emitter(object):
    def __init__(self, port, n=1000, duration=3.0):
        self.port = port
        self.n = n
        self.duration = duration
        self.cb = self.text
        self.i = 0

        self.pcb = None
        self.client = None

    def start(self):
        self.client = TCPClient()

        self.pcb = PeriodicCallback(self.cb, 1000.0/self.n)
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
    def text(self):
        if self.i >= self.duration*self.n:
            self.pcb.stop()
            return

        stream = yield self.client.connect('127.0.0.1', self.port)
        with closing(stream):
            stream.write('{}\n'.format(random.random()).encode('utf8'))
            self.i += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-n', type=int, default=1000,
                        help='messages per second')
    parser.add_argument('--port', type=int, default=8123,
                        help='target port number')
    parser.add_argument('--format', default='text',
                        help='format of the messages')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='wait before start sending messages')
    args = parser.parse_args()

    time.sleep(args.delay)
    e = Emitter(args.port, args.n)
    e.start()
    print('{} sent {} messages'.format(sys.argv[0], e.i))


if __name__ == '__main__':
    main()
