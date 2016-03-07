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

    def test_groupByKey(self):
        t = self.stream_c.queueStream([[('a', 5), ('b', 8), ('a', 2)],
                                       [('a', 2), ('b', 3)]])
        self.r = t.groupByKey().mapValues(sorted)
        self.expect = [[('a', [2, 5]), ('b', [8])], [('a', [2]), ('b', [3])]]

    def test_mapValues(self):
        t = self.stream_c.queueStream([[('a', [5, 8, 2]), ('b', [6, 3, 8])]])
        self.r = t.mapValues(sorted)
        self.expect = [[('a', [2, 5, 8]), ('b', [3, 6, 8])]]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
