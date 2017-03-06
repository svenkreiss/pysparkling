from __future__ import print_function

from contextlib import closing
import struct
from tornado.testing import AsyncTestCase, gen_test
from tornado.tcpclient import TCPClient

from .streaming_test_case import StreamingTestCase


class TCPTextTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPTextTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        super(TCPTextTest, self).tearDown()

    @gen_test
    def test_connect(self):
        self.result = 0
        self.expect = 20
        (
            self.stream_c.socketTextStream('localhost', 8123)
            .count()
            .foreachRDD(lambda rdd: self.incr_result(rdd.collect()[0]))
        )

        for v in range(20):
            stream = yield self.client.connect('localhost', 8123)
            with closing(stream):
                stream.write('{}\n'.format(v).encode('utf8'))


class TCPBinaryFixedLengthTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPBinaryFixedLengthTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        super(TCPBinaryFixedLengthTest, self).tearDown()

    @gen_test
    def test_main(self):
        self.result = []
        self.expect = [[b'hello']]
        (
            self.stream_c.socketBinaryStream('localhost', 8123, length=5)
            .foreachRDD(lambda rdd: self.append_result(rdd.collect()))
        )

        stream = yield self.client.connect('localhost', 8123)
        with closing(stream):
            stream.write(b'hello')


class TCPBinaryUIntLengthTest(AsyncTestCase, StreamingTestCase):
    def setUp(self):
        super(TCPBinaryUIntLengthTest, self).setUp()
        self.client = TCPClient()

    def tearDown(self):
        self.client.close()
        super(TCPBinaryUIntLengthTest, self).tearDown()

    @gen_test
    def test_main(self):
        self.result = []
        self.expect = [[b'hellohello']]
        (
            self.stream_c.socketBinaryStream('localhost', 8123, length='<I')
            .foreachRDD(lambda rdd: self.append_result(rdd.collect()))
        )

        stream = yield self.client.connect('localhost', 8123)
        with closing(stream):
            stream.write(struct.pack('<I', 10) + b'hellohello')
