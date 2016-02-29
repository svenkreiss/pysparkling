import struct
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


class TCPTextTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPTextTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        super(TCPTextTest, self).tearDown()

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


class TCPBinaryFixedLengthTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPBinaryFixedLengthTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        super(TCPBinaryFixedLengthTest, self).tearDown()

    @gen_test
    def test_main(self):
        t = self.stream_c.socketBinaryStream_('localhost', 8123, length=5)

        stream = yield self.client.connect('localhost', 8123)
        with closing(stream):
            stream.write(b'hello')

        self.r = t
        self.expect = [[b'hello']]


class TCPBinaryUIntLengthTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPBinaryUIntLengthTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        super(TCPBinaryUIntLengthTest, self).tearDown()

    @gen_test
    def test_main(self):
        t = self.stream_c.socketBinaryStream_('localhost', 8123, length='<I')

        stream = yield self.client.connect('localhost', 8123)
        with closing(stream):
            stream.write(struct.pack('<I', 10)+b'hellohello')

        self.r = t
        self.expect = [[b'hellohello']]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
