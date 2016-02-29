import logging
import unittest
import pysparkling

from contextlib import closing
from tornado.testing import AsyncTestCase, gen_test
from tornado.tcpclient import TCPClient


class StreamingTestCase(unittest.TestCase):

    def setUp(self):
        self.c = pysparkling.Context()
        self.stream_c = pysparkling.streaming.StreamingContext(self.c, 0.001)

    def tearDown(self):
        result = []
        self.r.foreachRDD(lambda _, rdd: result.append(rdd.collect()))

        self.stream_c.start()
        self.stream_c.awaitTermination(timeout=0.01)

        print(result)
        assert result == self.expect


class TCPClientTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPClientTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        # self.stop_server()
        super(TCPClientTest, self).tearDown()

    @gen_test
    def test_connect(self):
        t = self.stream_c.socketTextStream('localhost', 8123)

        stream = yield self.client.connect('localhost', 8123)
        with closing(stream):
            for v in range(20):
                stream.write('{}\n'.format(v).encode('utf8'))
            stream.write(b'a\nb\n')
            stream.write(b'c\n')

        self.r = t.count()
        self.expect = [[23]]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
