from __future__ import print_function

from .streaming_test_case import StreamingTestCase


class TestCount(StreamingTestCase):

    def test_count(self):
        self.result = 0
        self.expect = 23
        (
            self.stream_c.queueStream([range(20), ['a', 'b'], ['c']])
            .count()
            .foreachRDD(lambda rdd: self.incr_result(rdd.collect()[0]))
        )

    def test_groupByKey(self):
        self.result = []
        self.expect = [[('a', [2, 5]), ('b', [8])], [('a', [2]), ('b', [3])]]
        (
            self.stream_c.queueStream([[('a', 5), ('b', 8), ('a', 2)],
                                       [('a', 2), ('b', 3)]])
            .groupByKey().mapPartitions(sorted).mapValues(sorted)
            .foreachRDD(lambda rdd: self.append_result(rdd.collect()))
        )

    def test_mapValues(self):
        self.result = []
        self.expect = [[('a', [2, 5, 8]), ('b', [3, 6, 8])]]
        (
            self.stream_c.queueStream([[('a', [5, 8, 2]), ('b', [6, 3, 8])]])
            .mapValues(sorted)
            .foreachRDD(lambda rdd: self.append_result(rdd.collect()))
        )
