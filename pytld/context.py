"""Imitates SparkContext."""

from .tld import TLD


class Context(object):
    def __init__(self, pool=None):
        self.ctx = {
            'pool': pool
        }

    def parallelize(self, x):
        return TLD(x, self.ctx)

    def textFile(self, filename):
        with open(filename, 'r') as f:
            return self.parallelize(f.readlines())
        return self.parallelize([])
