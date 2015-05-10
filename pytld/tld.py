"""TLD implementation."""

import random
import functools


class TLD(object):
    """methods starting with underscore are not in the Spark interface"""

    def __init__(self, x, ctx):
        self.x = x
        self.ctx = ctx

    def _flatten(self):
        self.x = [xx for x in self.x for xx in x]
        return self

    def _flattenValues(self):
        self.x = [(e[0], v) for e in self.x for v in e[1]]
        return self

    def cache(self):
        return self

    def coalesce(self):
        return self

    def collect(self):
        return self.x

    def context(self):
        return self.ctx

    def count(self):
        return len(self.x)

    def countApprox(self):
        return self.count()

    def countByKey(self):
        keys = set(k for k, v in self.x)
        return dict((k, sum(v for kk, v in self.x if kk == k)) for k in keys)

    def countByValue(self):
        keys = set(self.x)
        return dict((k, self.x.count(k)) for k in keys)

    def distinct(self, numPartitions=None):
        return TLD(list(set(self.x)), self.ctx)

    def filter(self, f):
        return TLD([x for x in self.x if f(x)], self.ctx)

    def first(self):
        return self.x[0]

    def flatMap(self, f, preservesPartitioning=False):
        return self.map(f)._flatten()

    def flatMapValues(self, f):
        return self.mapValues(f)._flattenValues()

    def fold(self, zeroValue, op):
        return functools.reduce(op, self.x, zeroValue)

    def foldByKey(self, zeroValue, op):
        keys = set(k for k, v in self.x)
        return dict(
            (
                k,
                functools.reduce(
                    op,
                    (e[1] for e in self.x if e[0] == k),
                    zeroValue
                )
            )
            for k in keys
        )

    def foreach(self, f):
        self.x = self.ctx['pool'].map(f, self.x)
        return self

    def foreachPartition(self, f):
        self.foreach(f)
        return self

    def groupBy(self, f):
        f_applied = self.ctx['pool'].map(f, self.x)
        keys = set(f_applied)
        return TLD([
            (k, [vv for kk, vv in zip(f_applied, self.x) if kk == k])
            for k in keys
        ], self.ctx)

    def map(self, f):
        return TLD(self.ctx['pool'].map(f, self.x), self.ctx)

    def mapValues(self, f):
        return TLD(zip(
            (e[0] for e in self.x),
            self.ctx['pool'].map(f, (e[1] for e in self.x))
        ), self.ctx)

    def take(self, n):
        return self.x[:n]

    def takeSample(self, n):
        return random.sample(self.x, n)
