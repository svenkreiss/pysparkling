"""RDD implementation."""

from __future__ import division, absolute_import, print_function

import sys
import copy
import random
import logging
import functools
import itertools
import subprocess
from collections import defaultdict

from . import utils
from .fileio import File
from .partition import PersistedPartition
from .exceptions import FileAlreadyExistsException

log = logging.getLogger(__name__)


class RDD(object):
    """methods starting with underscore are not in the Spark interface"""

    def __init__(self, partitions, ctx):
        self._p = partitions
        self.context = ctx
        self._name = None

    def __getstate__(self):
        r = dict((k, v) for k, v in self.__dict__.items())
        r['_p'] = list(self.partitions())
        r['context'] = None
        return r

    def compute(self, split, task_context):
        """split is a partition. This function is used in derived RDD
        classes. To add smarter behavior for specific cases."""
        return split.x()

    def partitions(self):
        self._p, r = itertools.tee(self._p, 2)
        return r

    """

    Public API
    ----------
    """

    def aggregate(self, zeroValue, seqOp, combOp):
        """[distributed]"""
        return self.context.runJob(
            self,
            lambda tc, i: functools.reduce(
                seqOp, i, copy.deepcopy(zeroValue)
            ),
            resultHandler=lambda l: functools.reduce(
                combOp, l, copy.deepcopy(zeroValue)
            ),
        )

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None):
        def seqFuncByKey(tc, i):
            r = defaultdict(lambda: copy.deepcopy(zeroValue))
            for k, v in i:
                r[k] = seqFunc(r[k], v)
            return r

        def combFuncByKey(l):
            r = defaultdict(lambda: copy.deepcopy(zeroValue))
            for p in l:
                for k, v in p.items():
                    r[k] = combFunc(r[k], v)
            return r

        return self.context.runJob(self, seqFuncByKey,
                                   resultHandler=combFuncByKey)

    def cache(self):
        return self.persist()

    def cartesian(self, other):
        v1 = self.collect()
        v2 = self.collect()
        return self.context.parallelize([(a, b) for a in v1 for b in v2])

    def coalesce(self, numPartitions, shuffle=False):
        return self.context.parallelize(self.collect(), numPartitions)

    def collect(self):
        """[distributed]"""
        return self.context.runJob(
            self, lambda tc, i: list(i),
            resultHandler=lambda l: [x for p in l for x in p],
        )

    def count(self):
        """[distributed]"""
        return self.context.runJob(self, lambda tc, i: sum(1 for _ in i),
                                   resultHandler=sum)

    def countApprox(self):
        return self.count()

    def countByKey(self):
        """[distributed]"""
        def map_func(tc, x):
            r = defaultdict(int)
            for k, v in x:
                r[k] += v
            return r
        return self.context.runJob(self, map_func,
                                   resultHandler=utils.sum_counts_by_keys)

    def countByValue(self):
        """[distributed]"""
        def map_func(tc, x):
            r = defaultdict(int)
            for v in x:
                r[v] += 1
            return r
        return self.context.runJob(self, map_func,
                                   resultHandler=utils.sum_counts_by_keys)

    def distinct(self, numPartitions=None):
        return self.context.parallelize(list(set(self.collect())),
                                        numPartitions)

    def filter(self, f):
        """[distributed]"""
        def map_func(tc, i, x):
            return (xx for xx in x if f(xx))
        return MapPartitionsRDD(self, map_func, preservesPartitioning=True)

    def first(self):
        """[distributed]"""
        return self.context.runJob(
            self,
            lambda tc, i: next(i) if tc.partition_id == 0 else None,
            resultHandler=lambda l: next(l),
        )

    def flatMap(self, f, preservesPartitioning=False):
        """[distributed]"""
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (e for xx in x for e in f(xx)),
            preservesPartitioning=True,
        )

    def flatMapValues(self, f):
        """[distributed]"""
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: ((xx[0], e) for xx in x for e in f(xx[1])),
            preservesPartitioning=True,
        )

    def fold(self, zeroValue, op):
        return self.aggregate(zeroValue, op, op)

    def foldByKey(self, zeroValue, op):
        return self.aggregateByKey(zeroValue, op, op)

    def foreach(self, f):
        self.context.runJob(self, lambda tc, x: [f(xx) for xx in x],
                            resultHandler=None)

    def foreachPartition(self, f):
        self.context.runJob(self, lambda tc, x: f(x),
                            resultHandler=None)

    def getNumPartitions(self):
        return sum(1 for _ in self.partitions())

    def getPartitions(self):
        return self.partitions()

    def groupBy(self, f, numPartitions=None):
        return self.context.parallelize((
            (k, [gg[1] for gg in g]) for k, g in itertools.groupby(
                sorted(self.keyBy(f).collect()),
                lambda e: e[0],
            )
        ), numPartitions)

    def groupByKey(self, numPartitions=None):
        return self.context.parallelize((
            (k, [gg[1] for gg in g]) for k, g in itertools.groupby(
                sorted(self.collect()),
                lambda e: e[0],
            )
        ), numPartitions)

    def histogram(self, buckets):
        if isinstance(buckets, int):
            num_buckets = buckets
            min_v = self.min()
            max_v = self.max()
            buckets = [min_v + float(i)*(max_v-min_v)/num_buckets
                       for i in range(num_buckets+1)]
        h = [0 for _ in buckets]
        for x in self.collect():
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
        return self.context.parallelize(
            list(set(self.collect()) & set(other.collect()))
        )

    def isCheckpointed(self):
        return False

    def join(self, other, numPartitions=None):
        d1 = dict(self.collect())
        d2 = dict(other.collect())
        keys = set(d1.keys()) & set(d2.keys())
        return self.context.parallelize((
            (k, (d1[k], d2[k]))
            for k in keys
        ), numPartitions)

    def keyBy(self, f):
        return self.map(lambda e: (f(e), e))

    def keys(self):
        return self.map(lambda e: e[0])

    def leftOuterJoin(self, other, numPartitions=None):
        d1 = dict(self.collect())
        d2 = dict(other.collect())
        return self.context.parallelize((
            (k, (d1[k], d2[k] if k in d2 else None))
            for k in d1.keys()
        ), numPartitions)

    def lookup(self, key):
        """[distributed]"""
        return self.context.runJob(
            self,
            lambda tc, x: (xx[1] for xx in x if xx[0] == key),
            resultHandler=lambda l: [e for ll in l for e in ll],
        )

    def map(self, f):
        """[distributed]"""
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (f(xx) for xx in x),
            preservesPartitioning=True,
        )

    def mapPartitions(self, f, preservesPartitioning=False):
        """[distributed]"""
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: f(x),
            preservesPartitioning=preservesPartitioning,
        )

    def mapValues(self, f):
        """[distributed]"""
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: ((e[0], f(e[1])) for e in x),
            preservesPartitioning=True,
        )

    def max(self):
        """[distributed]"""
        return self.context.runJob(
            self,
            lambda tc, x: max(x),
            resultHandler=max,
        )

    def mean(self):
        """[distributed]"""
        def map_func(tc, x):
            summed, length = (0.0, 0)
            for xx in x:
                summed += xx
                length += 1
            return (summed, length)

        def reduce_func(l):
            summed, length = zip(*l)
            return sum(summed)/sum(length)

        return self.context.runJob(self, map_func,
                                   resultHandler=reduce_func)

    def min(self):
        """[distributed]"""
        return self.context.runJob(
            self,
            lambda tc, x: min(x),
            resultHandler=min,
        )

    def name(self):
        return self._name

    def persist(self, storageLevel=None):
        """[distributed]"""
        return PersistedRDD(self, storageLevel=storageLevel)

    def pipe(self, command, env={}):
        return self.context.parallelize(subprocess.check_output(
            [command]+x if isinstance(x, list) else [command, x]
        ) for x in self.collect())

    def reduce(self, f):
        """[distributed] f must be a commutative and associative
        binary operator"""
        return self.context.runJob(
            self,
            lambda tc, x: functools.reduce(f, x),
            resultHandler=lambda x: functools.reduce(f, x),
        )

    def reduceByKey(self, f):
        return self.groupByKey().mapValues(lambda x: functools.reduce(f, x))

    def repartition(self, numPartitions):
        return self.context.parallelize(self.collect(), numPartitions)

    def rightOuterJoin(self, other, numPartitions=None):
        d1 = dict(self.collect())
        d2 = dict(other.collect())
        return self.context.parallelize((
            (k, (d1[k] if k in d1 else None, d2[k]))
            for k in d2.keys()
        ), numPartitions)

    def sample(self, withReplacement, fraction, seed=None):
        """[distributed]

        withReplacement is not implemented.
        """
        return PartitionwiseSampledRDD(
            self, fraction,
            preservesPartitioning=True,
            seed=seed,
        )

    def saveAsTextFile(self, path, compressionCodecClass=None):
        if File.exists(path):
            raise FileAlreadyExistsException(
                'Output {0} already exists.'.format(path)
            )

        codec_suffix = ''
        if path.endswith(('.gz', '.bz2', '.lzo')):
            codec_suffix = path[path.rfind('.'):]

        self.context.runJob(
            self,
            lambda tc, x: File(
                path+'/part-{0:05d}{1}'.format(tc.partitionId(), codec_suffix)
            ).dump([
                u'{0}\n'.format(xx).encode('utf-8') for xx in x
            ]),
            resultHandler=lambda l: (
                list(l) and File(path+'/_SUCCESS').dump()
            ),
        )
        return self

    def subtract(self, other, numPartitions=None):
        """[distributed]"""
        list_other = other.collect()
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (e for e in x if e not in list_other),
            preservesPartitioning=True,
        )

    def sum(self):
        """[distributed]"""
        return self.context.runJob(self, lambda tc, x: sum(x),
                                   resultHandler=sum)

    def take(self, n):
        """[distributed]
        Due to lazy execution of maps in result handler, only the first
        partitions that are necessary are evaluated."""

        def res_handler(l):
            r = []
            for p in l:
                for x in p:
                    if len(r) >= n:
                        break
                    r.append(x)
                if len(r) >= n:
                    break
            return r

        return self.context.runJob(
            self, lambda tc, i: list(i),
            resultHandler=res_handler,
        )

    def takeSample(self, n):
        """[distributed]

        Assumes samples are evenly distributed between partitions.

        Due to lazy execution of maps in result handler, only the needed
        partitions are evaluated."""

        rnd_entries = sorted([random.random() for _ in range(n)])
        num_partitions = sum(1 for _ in self.partitions())

        rnd_entries = [
            (
                int(e*num_partitions),  # partition number
                e*num_partitions-int(e*num_partitions),  # element in partition
            )
            for e in rnd_entries
        ]
        partition_indices = [i for i, e in rnd_entries]
        partitions = [p for i, p in enumerate(self.partitions())
                      if i in partition_indices]

        def res_handler(l):
            map_results = list(l)
            entries = itertools.groupby(rnd_entries, lambda e: e[0])
            r = []
            for i, e_list in enumerate(entries):
                p_result = map_results[i]
                if not p_result:
                    continue
                for p_num, e in e_list[1]:
                    e_num = int(e*len(p_result))
                    r.append(p_result[e_num])
            return r

        return self.context.runJob(
            self, lambda tc, i: list(i), partitions=partitions,
            resultHandler=res_handler,
        )


class MapPartitionsRDD(RDD):
    def __init__(self, prev, f, preservesPartitioning=False):
        """prev is the previous RDD.

        f is a function with the signature
        (task_context, partition index, iterator over elements).
        """
        RDD.__init__(self, prev.partitions(), prev.context)

        self.prev = prev
        self.f = f
        self.preservesPartitioning = preservesPartitioning

    def compute(self, split, task_context):
        return self.f(task_context, split.index,
                      self.prev.compute(split, task_context._create_child()))

    def partitions(self):
        return self.prev.partitions()


class PartitionwiseSampledRDD(RDD):
    def __init__(self, prev, fraction, preservesPartitioning=False, seed=None):
        """prev is the previous RDD.

        f is a function with the signature
        (task_context, partition index, iterator over elements).
        """
        RDD.__init__(self, prev.partitions(), prev.context)

        if not seed:
            seed = random.randint(0, sys.maxint)

        self.prev = prev
        self.fraction = fraction
        self.preservesPartitioning = preservesPartitioning
        self.seed = seed

    def compute(self, split, task_context):
        random.seed(self.seed+split.index)
        return (
            x for x in self.prev.compute(split, task_context._create_child())
            if random.random() < self.fraction
        )

    def partitions(self):
        return self.prev.partitions()


class PersistedRDD(RDD):
    def __init__(self, prev, storageLevel=None):
        """prev is the previous RDD.

        """
        RDD.__init__(
            self,
            (
                PersistedPartition(
                    p.x(),
                    p.index,
                    storageLevel,
                ) for p in prev.partitions()
            ),
            prev.context,
        )

        self.prev = prev

    def compute(self, split, task_context):
        if split.cache_x is None:
            split.set_cache_x(
                self.prev.compute(split, task_context._create_child())
            )
        return split.x()
