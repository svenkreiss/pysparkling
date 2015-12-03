import os
import math
import time
import pickle
import pprint
import random
import timeit
import logging
import unittest
import cloudpickle
import multiprocessing
from concurrent import futures
from pysparkling import Context


def test_multiprocessing():
    p = multiprocessing.Pool(4)
    c = Context(pool=p, serializer=cloudpickle.dumps,
                deserializer=pickle.loads)
    my_rdd = c.parallelize([1, 3, 4])
    r = my_rdd.map(lambda x: x*x).collect()
    print(r)
    assert 16 in r


def test_concurrent():
    with futures.ThreadPoolExecutor(4) as p:
        my_rdd = Context(pool=p).parallelize([1, 3, 4])
        r = my_rdd.map(math.sqrt).collect()
        print(r)
        assert 2 in r


def test_first_mp():
    p = multiprocessing.Pool(4)
    c = Context(pool=p, serializer=cloudpickle.dumps,
                deserializer=pickle.loads)
    my_rdd = c.parallelize([1, 2, 2, 4, 1, 3, 5, 9], 3)
    print(my_rdd.first())
    assert my_rdd.first() == 1


def test_lazy_execution():

    class I(object):
        def __init__(self):
            self.executed = False

        def indent_line(self, l):
            # global indent_was_executed
            self.executed = True
            return '--- '+l

    r = Context().textFile('tests/test_multiprocessing.py')
    i = I()

    r = r.map(i.indent_line)
    exec_before_collect = i.executed
    # at this point, no map() or foreach() should have been executed
    r = r.map(i.indent_line).cache()
    print(r.collect())
    r = r.map(i.indent_line)
    r.collect()
    exec_after_collect = i.executed
    print((exec_before_collect, exec_after_collect))
    assert not exec_before_collect and exec_after_collect


def test_lazy_execution_threadpool():
    def indent_line(l):
        return '--- '+l

    with futures.ThreadPoolExecutor(4) as p:
        r = Context(pool=p).textFile('tests/test_multiprocessing.py')
        r = r.map(indent_line).cache()
        r.collect()
        r = r.map(indent_line)
        r = r.collect()
        # ThreadPool is not lazy although it returns generators.
        print(r)
        assert '--- --- from pysparkling import Context' in r


def test_lazy_execution_processpool():
    def indent_line(l):
        return '--- '+l

    with futures.ProcessPoolExecutor(4) as p:
        r = Context(
            pool=p,
            serializer=cloudpickle.dumps,
            deserializer=pickle.loads,
        ).textFile('tests/test_multiprocessing.py')  # .take(10)
        print(r.collect())
        r = r.map(indent_line)
        print(r.collect())
        r = r.cache()
        print(r.collect())
        r = r.map(indent_line)
        r = r.collect()
        # ProcessPool is not lazy although it returns generators.
        print(r)
        assert '--- --- from pysparkling import Context' in r


def test_processpool_distributed_cache():
    with futures.ProcessPoolExecutor(4) as p:
        r = Context(
            pool=p,
            serializer=cloudpickle.dumps,
            deserializer=pickle.loads,
        ).parallelize(range(3), 3)
        r = r.map(lambda _: time.sleep(0.1)).cache()
        r.collect()

        time_start = time.time()
        print(r.collect())
        time_end = time.time()
        assert time_end - time_start < 0.3


# pickle-able map function
def map1(ft):
    return [random.choice(ft[1].split()) for _ in range(1000)]


def map_pi(n):
    return len(filter(
        lambda x: x < 1.0,
        [random.random()**2 + random.random()**2 for _ in range(n)]
    ))


@unittest.skipIf(os.getenv('TRAVIS', False) is not False,
                 "skip performance test on Travis")
def test_performance():
    # not pickle-able map function
    def map2(ft):
        return [random.choice(ft[1].split()) for _ in range(1000)]

    def create_context(n_processes=0):
        if not n_processes:
            return Context()

        p = futures.ProcessPoolExecutor(n_processes)
        return Context(
            pool=p,
            serializer=cloudpickle.dumps,
            # serializer=pickle.dumps,
            deserializer=pickle.loads,
        )

    def test(n_processes):
        c = create_context(n_processes)
        t = timeit.Timer(
            # lambda: c.wholeTextFiles('tests/*.py').map(map1).collect()
            lambda: c.parallelize(
                [10000 for _ in range(100)],
                100,
            ).map(map_pi).collect()
        ).timeit(number=100)
        return (t, c._stats)

    print('starting processing')
    n_cpu = multiprocessing.cpu_count()
    test_results = {}
    for n in range(int(n_cpu*1.5 + 1)):
        test_results[n] = test(n)
        print(n, test_results[n][0])
    print('results where running on one core with full serialization is 1.0:')
    pprint.pprint({
        n: 1.0 / (v[0]/test_results[1][0]) for n, v in test_results.items()
    })
    print('time spent where:')
    pprint.pprint({
        n: {k: t/v[1]['map_exec'] for k, t in v[1].items()}
        for n, v in test_results.items()
    })

    # running on two cores takes less than 70% of the time running on one
    assert test_results[2][0]/test_results[1][0] < 0.7

    return (n_cpu, test_results)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_performance()
