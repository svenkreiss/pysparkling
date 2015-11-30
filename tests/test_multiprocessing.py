import math
import time
import pickle
import timeit
import logging
import cloudpickle
import multiprocessing
from concurrent import futures
from pysparkling import Context

import pysparkling


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


def map1(ft):
    return {
        'first line': ft[1].lower().split('\n')[0],
        'count import': ft[1].lower().count('import'),
        'count print': ft[1].lower().count('print'),
        'reversed': str(reversed([ft[1]])),
        'number of lines': len(ft[1].split('\n')),
    }


def test_performance():
    def map2(ft):
        return {
            'first line': ft[1].lower().split('\n')[0],
            'count import': ft[1].lower().count('import'),
            'count print': ft[1].lower().count('print'),
            'reversed': str(reversed([ft[1]])),
            'number of lines': len(ft[1].split('\n')),
        }

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
            lambda: c.wholeTextFiles('tests/*.py').map(map2).collect()
        ).timeit(number=100)
        print(dict(c._stats))
        return t

    print('starting processing')
    for n in range(8):
        print(n, test(n))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_performance()
