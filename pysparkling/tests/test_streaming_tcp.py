from collections import Counter
from contextlib import closing
import struct

import tornado.gen
import tornado.tcpclient
import tornado.testing

import pysparkling


class TCPTextTest(tornado.testing.AsyncTestCase):
    @tornado.gen.coroutine
    def client(self):
        client = tornado.tcpclient.TCPClient()
        for v in range(20):
            stream = yield client.connect('127.0.0.1', 8123)
            with closing(stream):
                stream.write('a = {}\n'.format(v).encode('utf8'))
        client.close()

    def test_connect(self):
        sc = pysparkling.Context()
        ssc = pysparkling.streaming.StreamingContext(sc, 0.1)

        counter = Counter()
        (
            ssc.socketTextStream('127.0.0.1', 8123)
            .foreachRDD(lambda rdd:
                        counter.update(''.join(rdd.collect()))
                        if rdd.collect() else None)
        )
        self.client()

        ssc.start()
        ssc.awaitTermination(timeout=0.3)
        self.assertEqual(counter['a'], 20)


class TCPBinaryFixedLengthTest(tornado.testing.AsyncTestCase):
    @tornado.gen.coroutine
    def client(self):
        client = tornado.tcpclient.TCPClient()
        stream = yield client.connect('127.0.0.1', 8124)
        with closing(stream):
            stream.write(b'hello')
        client.close()

    def test_main(self):
        sc = pysparkling.Context()
        ssc = pysparkling.streaming.StreamingContext(sc, 0.1)

        counter = Counter()
        (
            ssc.socketBinaryStream('127.0.0.1', 8124, length=5)
            .foreachRDD(lambda rdd: counter.update(rdd.collect()))
        )
        self.client()

        ssc.start()
        ssc.awaitTermination(timeout=0.3)
        self.assertEqual(counter[b'hello'], 1)


class TCPBinaryUIntLengthTest(tornado.testing.AsyncTestCase):
    @tornado.gen.coroutine
    def client(self):
        client = tornado.tcpclient.TCPClient()
        stream = yield client.connect('127.0.0.1', 8125)
        with closing(stream):
            stream.write(struct.pack('<I', 10) + b'hellohello')
        client.close()

    def test_main(self):
        sc = pysparkling.Context()
        ssc = pysparkling.streaming.StreamingContext(sc, 0.1)

        counter = Counter()
        (
            ssc.socketBinaryStream('127.0.0.1', 8125, length='<I')
            .foreachRDD(lambda rdd: counter.update(rdd.collect()))
        )
        self.client()

        ssc.start()
        ssc.awaitTermination(timeout=0.3)
        self.assertEqual(counter[b'hellohello'], 1)
