from concurrent import futures
import logging
import math
import multiprocessing
import os
import pickle
import pprint
import random
import time
import timeit
import unittest

import cloudpickle

import pysparkling


class Processor:
    """This modifies lines but also keeps track whether it was executed."""
    def __init__(self):
        self.executed = False

    def indent_line(self, line):
        self.executed = True
        return f'--- {line}'


class LazyTestInjection:
    def lazy_execution_test(self):
        r = self.sc.textFile(__file__)  # pylint: disable=no-member

        processor = Processor()

        r = r.map(processor.indent_line)
        self.assertFalse(processor.executed)  # pylint: disable=no-member
        r = r.map(processor.indent_line).cache()
        self.assertFalse(processor.executed)  # pylint: disable=no-member
        r = r.map(processor.indent_line)
        r.collect()
        self.assertTrue(processor.executed)  # pylint: disable=no-member


class Multiprocessing(unittest.TestCase):
    def setUp(self):
        self.pool = multiprocessing.Pool(4)
        self.sc = pysparkling.Context(pool=self.pool,
                                      serializer=cloudpickle.dumps,
                                      deserializer=pickle.loads)

    def test_basic(self):
        my_rdd = self.sc.parallelize([1, 3, 4])
        r = my_rdd.map(lambda x: x ** 2).collect()
        self.assertIn(16, r)

    def test_first(self):
        my_rdd = self.sc.parallelize([1, 2, 2, 4, 1, 3, 5, 9], 3)
        self.assertEqual(my_rdd.first(), 1)

    def tearDown(self):
        self.pool.close()


def square_op(x):
    return x ** 2


class MultiprocessingWithoutCloudpickle(unittest.TestCase):
    def setUp(self):
        self.pool = multiprocessing.Pool(4)
        self.sc = pysparkling.Context(pool=self.pool)

    def test_basic(self):
        my_rdd = self.sc.parallelize([1, 3, 4])
        r = my_rdd.map(square_op).collect()
        self.assertIn(16, r)

    def tearDown(self):
        self.pool.close()


class NotParallel(unittest.TestCase, LazyTestInjection):
    """Test cases in the spirit of the parallel test cases for reference."""

    def setUp(self):
        self.sc = pysparkling.Context()


class ThreadPool(unittest.TestCase, LazyTestInjection):
    def setUp(self):
        self.pool = futures.ThreadPoolExecutor(4)
        self.sc = pysparkling.Context(pool=self.pool)

    def tearDown(self):
        self.pool.shutdown()

    def test_basic(self):
        r = self.sc.parallelize([1, 3, 4]).map(math.sqrt).collect()
        self.assertIn(2, r)


class ProcessPool(unittest.TestCase):  # cannot work here: LazyTestInjection):
    def setUp(self):
        self.pool = futures.ProcessPoolExecutor(4)
        self.sc = pysparkling.Context(pool=self.pool,
                                      serializer=cloudpickle.dumps,
                                      deserializer=pickle.loads)

    def tearDown(self):
        self.pool.shutdown()

    def test_basic(self):
        r = self.sc.parallelize([1, 3, 4]).map(math.sqrt).collect()
        self.assertIn(2, r)

    def test_zipWithIndex(self):
        """Prevent regression in zipWithIndex().

        Test the case of parallelizing data directly form toLocalIterator()
        in the multiprocessing case.
        """
        r = (self.sc
             .parallelize([1, 3, 4, 9, 15, 25, 50, 75, 100], 3)
             .zipWithIndex()
             .collect())
        self.assertIn((4, 2), r)

    def test_cache(self):
        to_check = list(range(5))
        r = self.sc.parallelize(to_check, 3)

        def sleep05(v):
            time.sleep(0.5)
            return v

        # Spin up the pool: --> Mind you, not caching here yet!
        r.map(sleep05).collect()

        # And continue with the procedure
        r = r.map(sleep05).cache()
        self.assertCountEqual(r.collect(), to_check)

        start = time.time()
        r.collect()

        self.assertLess(time.time() - start, 0.5)


class ProcessPoolIdlePerformance(unittest.TestCase):
    """Idle performance tests.

    The "load" on these tests are sleeps.
    """
    @staticmethod
    def _sub_procedure(pool, n):
        sc = pysparkling.Context(pool=pool,
                                 serializer=cloudpickle.dumps,
                                 deserializer=pickle.loads)
        rdd = sc.parallelize(range(n), 10)
        rdd.map(lambda _: time.sleep(0.01)).collect()

    def runtime(self, n=10, processes=1):
        with futures.ProcessPoolExecutor(processes) as pool:
            # 1: go through everything... Just to spin up the pool
            self._sub_procedure(pool, n)

            # 2: now we're going to time it.
            start = time.time()
            self._sub_procedure(pool, n)
            stop = time.time()  # Don't calculate the stopping of the pool as well.

        return stop - start

    def test_basic(self):
        t1 = self.runtime(processes=1)
        t10 = self.runtime(processes=10)

        self.assertLess(t10, t1 / 2)


# pickle-able map function
def map1(ft):
    return [random.choice(ft[1].split()) for _ in range(1000)]


def map_pi(n):
    return sum((
        1 for x in (random.random() ** 2 + random.random() ** 2
                    for _ in range(n))
        if x < 1.0
    ))


@unittest.skipIf(os.getenv('PERFORMANCE') is None,
                 'PERFORMANCE env variable not set')
def test_performance():
    # not pickle-able map function
    # def map2(ft):
    #     return [random.choice(ft[1].split()) for _ in range(1000)]

    def create_context(n_processes=0):
        if not n_processes:
            return pysparkling.Context()

        pool = futures.ProcessPoolExecutor(n_processes)
        return pysparkling.Context(pool=pool,
                                   serializer=cloudpickle.dumps,
                                   # serializer=pickle.dumps,
                                   deserializer=pickle.loads)

    def test(n_processes):
        sc = create_context(n_processes)
        timed = timeit.Timer(
            lambda: sc.parallelize(
                [1000 for _ in range(100)],
                100,
            ).map(map_pi).collect()
        ).timeit(number=10)
        return (timed, sc._stats)

    print('starting processing')
    n_cpu = multiprocessing.cpu_count()
    test_results = {}
    for n in range(int(n_cpu * 1.5 + 1)):
        test_results[n] = test(n)
        print(n, test_results[n][0])
    print('results where running on one core with full serialization is 1.0:')
    pprint.pprint({
        n: 1.0 / (v[0] / test_results[1][0]) for n, v in test_results.items()
    })
    print('time spent where:')
    pprint.pprint({
        n: {k: f'{t / v[1]["map_exec"]:.1%}' for k, t in v[1].items()}
        for n, v in test_results.items()
    })

    return (n_cpu, test_results)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # test_performance()
    t = ProcessPool()
    t.setUp()
    t.test_cache()
    t.tearDown()
