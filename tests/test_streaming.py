import logging
import unittest
import pysparkling


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


class TestCount(StreamingTestCase):

    def test_count(self):
        t = self.stream_c.queueStream([range(20), ['a', 'b'], ['c']])
        self.r = t.count()
        self.expect = [[20], [2], [1]]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
