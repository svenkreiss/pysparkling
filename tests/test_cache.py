from __future__ import print_function

import logging
import pysparkling


class Manip(object):
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


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_cache_empty_partition()
