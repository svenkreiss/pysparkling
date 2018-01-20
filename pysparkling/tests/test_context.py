from __future__ import print_function

import logging
import pysparkling
import unittest


class Context(unittest.TestCase):
    def test_broadcast(self):
        b = pysparkling.Context().broadcast([1, 2, 3])
        self.assertEqual(b.value[0], 1)

    def test_lock1(self):
        """Should not be able to create a new RDD inside a map operation."""
        sc = pysparkling.Context()
        self.assertRaises(
            pysparkling.exceptions.ContextIsLockedException,
            lambda: (sc
                     .parallelize(range(5))
                     .map(lambda _: sc.parallelize([1]))
                     .collect())
        )

    def test_lock2(self):
        """Should not be able to create RDDs containing RDDs."""
        sc = pysparkling.Context()

        def parallelize_in_parallelize():
            o = sc.parallelize(sc.parallelize(range(x)) for x in range(5))
            print(o.map(lambda x: x.collect()).collect())

        self.assertRaises(
            pysparkling.exceptions.ContextIsLockedException,
            parallelize_in_parallelize
        )

    def test_parallelize_single_element(self):
        my_rdd = pysparkling.Context().parallelize([7], 100)
        self.assertEqual(my_rdd.collect(), [7])

    def test_parallelize_matched_elements(self):
        my_rdd = pysparkling.Context().parallelize([1, 2, 3, 4, 5], 5)
        self.assertEqual(my_rdd.collect(), [1, 2, 3, 4, 5])

    def test_parallelize_empty_partitions_at_end(self):
        my_rdd = pysparkling.Context().parallelize(range(3529), 500)
        print(my_rdd.getNumPartitions())
        my_rdd.foreachPartition(lambda p: print(sum(1 for _ in p)))
        self.assertEqual(my_rdd.getNumPartitions(), 500)
        self.assertEqual(my_rdd.count(), 3529)

    def test_retry(self):

        class EverySecondCallFails(object):
            def __init__(self):
                self.attempt = 0

            def __call__(self, value):
                self.attempt += 1
                if self.attempt % 2 == 1:
                    raise Exception
                return value

        data = list(range(6))
        rdd = pysparkling.Context().parallelize(data, 3)
        result = rdd.mapPartitions(EverySecondCallFails()).collect()
        self.assertEqual(result, data)

    def test_union(self):
        sc = pysparkling.Context()
        rdd1 = sc.parallelize(['Hello'])
        rdd2 = sc.parallelize(['World'])
        union = sc.union([rdd1, rdd2]).collect()
        print(union)
        self.assertEqual(union, ['Hello', 'World'])

    def test_version(self):
        self.assertTrue(isinstance(pysparkling.Context().version, str))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    Context().test_retry()
