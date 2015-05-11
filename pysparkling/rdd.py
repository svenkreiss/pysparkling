"""RDD implementation."""

import random
import functools
import itertools


class RDD(object):
    """methods starting with underscore are not in the Spark interface"""

    def __init__(self, x, ctx):
        self._x = x
        self.ctx = ctx

    def x(self):
        self._x, r = itertools.tee(self._x, 2)
        return r

    def _flatten(self):
        self._x = (xx for x in self.x() for xx in x)
        return self

    def _flattenValues(self):
        self._x = ((e[0], v) for e in self.x() for v in e[1])
        return self

    def cache(self):
        # This cache is not lazy, but it will guarantee that previous
        # steps are only executed once.
        self._x = list(self._x)
        return self

    def coalesce(self):
        return self

    def collect(self):
        return list(self.x())

    def context(self):
        return self.ctx

    def count(self):
        return sum(1 for _ in self.x())

    def countApprox(self):
        return self.count()

    def countByKey(self):
        keys = set(k for k, v in self.x())
        return dict((k, sum(v for kk, v in self.x() if kk == k)) for k in keys)

    def countByValue(self):
        as_list = list(self.x())
        keys = set(as_list)
        return dict((k, as_list.count(k)) for k in keys)

    def distinct(self, numPartitions=None):
        return RDD(list(set(self.x())), self.ctx)

    def filter(self, f):
        return RDD((x for x in self.x() if f(x)), self.ctx)

    def first(self):
        return next(self.x())

    def flatMap(self, f, preservesPartitioning=False):
        return self.map(f)._flatten()

    def flatMapValues(self, f):
        return self.mapValues(f)._flattenValues()

    def fold(self, zeroValue, op):
        return functools.reduce(op, self.x(), zeroValue)

    def foldByKey(self, zeroValue, op):
        keys = set(k for k, v in self.x())
        return dict(
            (
                k,
                functools.reduce(
                    op,
                    (e[1] for e in self.x() if e[0] == k),
                    zeroValue
                )
            )
            for k in keys
        )

    def foreach(self, f):
        self._x = self.ctx['pool'].map(f, self.x())
        return self

    def foreachPartition(self, f):
        self.foreach(f)
        return self

    def groupBy(self, f):
        as_list = list(self.x())
        f_applied = list(self.ctx['pool'].map(f, as_list))
        keys = set(f_applied)
        return RDD([
            (k, [vv for kk, vv in zip(f_applied, as_list) if kk == k])
            for k in keys
        ], self.ctx)

    def groupByKey(self):
        as_list = list(self.x())
        keys = set([e[0] for e in as_list])
        return RDD([
            (k, [e[1] for e in as_list if e[0] == k])
            for k in keys
        ], self.ctx)

    def histogram(self, buckets):
        if isinstance(buckets, int):
            num_buckets = buckets
            min_v = self.min()
            max_v = self.max()
            buckets = [min_v + float(i)*(max_v-min_v)/num_buckets
                       for i in range(num_buckets+1)]
        h = [0 for _ in buckets]
        for x in self.x():
            for i, b in enumerate(zip(buckets[:-1], buckets[1:])):
                if x >= b[0] and x < b[1]:
                    h[i] += 1
            # make the last bin inclusive on the right
            if x == buckets[-1]:
                h[-1] += 1
        return (buckets, h)

    def id(self):
        # not implemented yet
        return None

    def intersection(self, other):
        return RDD(list(set(self.collect()) & set(other.collect())), self.ctx)

    def isCheckpointed(self):
        return False

    def join(self, other, numPartitions=None):
        d1 = dict(self.x())
        d2 = dict(other.x())
        keys = set(d1.keys()) & set(d2.keys())
        return RDD(((k, (d1[k], d2[k])) for k in keys), self.ctx)

    def keyBy(self, f):
        return RDD(((f(e), e) for e in self.x()), self.ctx)

    def keys(self):
        return RDD((e[0] for e in self.x()), self.ctx)

    def leftOuterJoin(self, other):
        d1 = dict(self.x())
        d2 = dict(other.x())
        return RDD(((k, (d1[k], d2[k] if k in d2 else None))
                    for k in d1.iterkeys()), self.ctx)

    def lookup(self, key):
        return [e[1] for e in self.x() if e[0] == key]

    def map(self, f):
        return RDD(self.ctx['pool'].map(f, self.x()), self.ctx)

    def mapValues(self, f):
        return RDD(zip(
            (e[0] for e in self.x()),
            self.ctx['pool'].map(f, (e[1] for e in self.x()))
        ), self.ctx)

    def max(self):
        return max(self.x())

    def min(self):
        return min(self.x())

    def rightOuterJoin(self, other):
        d1 = dict(self.x())
        d2 = dict(other.x())
        return RDD(((k, (d1[k] if k in d1 else None, d2[k]))
                    for k in d2.iterkeys()), self.ctx)

    def take(self, n):
        i = self.x()
        return [next(i) for _ in range(n)]

    def takeSample(self, n):
        return random.sample(list(self.x()), n)
