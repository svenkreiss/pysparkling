import math
import pysparkling
import multiprocessing
from concurrent import futures


def test_multiprocessing():
    p = multiprocessing.Pool(4)
    my_rdd = pysparkling.Context(pool=p).parallelize([1, 3, 4])
    r = my_rdd.foreach(math.sqrt).collect()
    print(r)
    assert 2 in r


def test_concurrent():
    with futures.ProcessPoolExecutor(4) as p:
        my_rdd = pysparkling.Context(pool=p).parallelize([1, 3, 4])
        r = my_rdd.foreach(math.sqrt).collect()
        print(r)
        assert 2 in r


INDENT_WAS_EXECUTED = False


def indent_line(l):
    global INDENT_WAS_EXECUTED
    INDENT_WAS_EXECUTED = True
    return '--- '+l


def test_lazy_execution():
    r = pysparkling.Context().textFile('tests/test_simple.py')
    r = r.map(indent_line)
    r.foreach(indent_line)
    exec_before_collect = INDENT_WAS_EXECUTED
    # at this point, no map() or foreach() should have been executed
    r.collect()
    exec_after_collect = INDENT_WAS_EXECUTED
    assert not exec_before_collect and exec_after_collect


def test_lazy_execution_threadpool():
    with futures.ThreadPoolExecutor(4) as p:
        r = pysparkling.Context(pool=p).textFile('tests/test_simple.py')
        r = r.map(indent_line)
        r.foreach(indent_line)
        exec_before_collect = INDENT_WAS_EXECUTED
        # at this point, no map() or foreach() should have been executed
        r.collect()
        exec_after_collect = INDENT_WAS_EXECUTED
        # ThreadPool is not lazy although it returns generators.
        # assert not exec_before_collect and exec_after_collect


if __name__ == '__main__':
    # test_multiprocessing()
    # test_concurrent()
    test_lazy_execution()
    # test_lazy_execution_threadpool()
