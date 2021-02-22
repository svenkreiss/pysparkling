import logging
import time

import pysparkling


class Manip:
    def __init__(self):
        self.count = 0

    def trivial_manip_with_debug(self, e):
        self.count += 1
        print('manipulating {0}'.format(e))
        return e


def test_cache_empty_partition():
    m = Manip()

    c = pysparkling.Context()
    rdd = c.parallelize(range(10), 2)
    rdd = rdd.map(m.trivial_manip_with_debug)
    rdd = rdd.filter(lambda e: e > 6).cache()
    print(rdd.collect())
    print(rdd.collect())

    print('count of map executions: {}'.format(m.count))
    assert m.count == 10


def test_timed_cache():
    m = Manip()

    # create a timed cache manager
    cm = pysparkling.TimedCacheManager(timeout=1.0)

    # create a cache entry
    c = pysparkling.Context(cache_manager=cm)
    rdd = c.parallelize(range(10), 2)
    rdd = rdd.map(m.trivial_manip_with_debug).cache()
    print(rdd.collect())
    # make sure the cache is working
    count_before = m.count
    print(rdd.collect())
    count_after = m.count
    assert count_before == count_after

    # wait to have the cache expire
    time.sleep(1.5)
    cm.gc()
    print(rdd.collect())
    assert m.count > count_after


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # test_cache_empty_partition()
    test_timed_cache()
