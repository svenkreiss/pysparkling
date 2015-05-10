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
    with futures.ProcessPoolExecutor(4) as executor:
        my_rdd = pysparkling.Context(pool=executor).parallelize([1, 3, 4])
        r = my_rdd.foreach(math.sqrt).collect()
        print(r)
        assert 2 in r


if __name__ == '__main__':
    test_multiprocessing()
    test_concurrent()
