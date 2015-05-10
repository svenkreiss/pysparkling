"""Imitates SparkContext."""

from .tld import TLD


class Context(object):
    def __init__(self, pool=None):
        if not pool:
            pool = DummyPool()

        self.ctx = {
            'pool': pool
        }

    def parallelize(self, x):
        return TLD(x, self.ctx)

    def textFile(self, filename):
        with open(filename, 'r') as f:
            return self.parallelize(f.readlines())
        return self.parallelize([])


class DummyPool(object):
    def __init__(self):
        pass

    def map(self, f, input_list):
        return [f(x) for x in input_list]
