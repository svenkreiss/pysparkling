"""
Provides a Python implementation of RDDs.

"""

from __future__ import (division, absolute_import, print_function,
                        unicode_literals)

from builtins import range, zip
from collections import defaultdict
import copy
import functools
import io
import itertools
import logging
from operator import itemgetter
import os
import pickle
import random
import subprocess
import sys

try:
    import numpy
except ImportError:
    numpy = None

from . import fileio
from .cache_manager import CacheManager
from .exceptions import FileAlreadyExistsException
from .samplers import (BernoulliSampler, PoissonSampler,
                       BernoulliSamplerPerKey, PoissonSamplerPerKey)
from .stat_counter import StatCounter

maxint = sys.maxint if hasattr(sys, 'maxint') else sys.maxsize

log = logging.getLogger(__name__)


def _hash(v):
    return hash(v) & 0xffffffff


class RDD(object):
    """RDD

    In Spark's original form, RDDs are Resilient, Distributed Datasets.
    This class reimplements the same interface with the goal of being
    fast on small data at the cost of being resilient and distributed.

    :param list partitions:
        A list of instances of :class:`Partition`.

    :param Context ctx:
        An instance of the applicable :class:`Context`.

    """

    def __init__(self, partitions, ctx):
        self._p = list(partitions)
        self.context = ctx
        self._name = None
        self._rdd_id = ctx.newRddId()

    def __getstate__(self):
        r = {k: v
             for k, v in self.__dict__.items()
             if k not in ('_p', 'context')}
        return r

    def compute(self, split, task_context):
        """interface to extend behavior for specific cases

        :param Partition split: a partition
        """
        return split.x()

    def partitions(self):
        return self._p

    """

    Public API
    ----------
    """

    def aggregate(self, zeroValue, seqOp, combOp):
        """aggregate

        [distributed]

        :param zeroValue:
            The initial value to an aggregation, for example ``0`` or ``0.0``
            for aggregating ``int`` s and ``float`` s, but any Python object is
            possible. Can be ``None``.

        :param seqOp:
            A reference to a function that combines the current state with a
            new value. In the first iteration, the current state is zeroValue.

        :param combOp:
            A reference to a function that combines outputs of seqOp.
            In the first iteration, the current state is zeroValue.

        :returns:
            Output of ``combOp`` operations.


        Example:

        >>> from pysparkling import Context
        >>> seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
        >>> combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
        >>> Context().parallelize(
        ...     [1, 2, 3, 4], 2
        ... ).aggregate((0, 0), seqOp, combOp)
        (10, 4)
        """
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
        """aggregate by key

        :param zeroValue:
            The initial value to an aggregation, for example ``0`` or ``0.0``
            for aggregating ``int`` s and ``float`` s, but any Python object is
            possible. Can be ``None``.

        :param seqFunc:
            A reference to a function that combines the current state with a
            new value. In the first iteration, the current state is zeroValue.

        :param combFunc:
            A reference to a function that combines outputs of seqFunc.
            In the first iteration, the current state is zeroValue.

        :param int numPartitions: (optional)
            Not used.

        :returns: An RDD with the output of ``combOp`` operations.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> seqOp = (lambda x, y: x + y)
        >>> combOp = (lambda x, y: x + y)
        >>> r = Context().parallelize(
        ...     [('a', 1), ('b', 2), ('a', 3), ('c', 4)]
        ... ).aggregateByKey(0, seqOp, combOp).collectAsMap()
        >>> (r['a'], r['b'])
        (4, 2)
        """
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

        local_result = self.context.runJob(self, seqFuncByKey,
                                           resultHandler=combFuncByKey)

        return self.context.parallelize(local_result.items())

    def cache(self):
        """Once a partition is computed, cache the result.

        Alias for :func:`RDD.persist`.


        Example:

        >>> from pysparkling import Context
        >>> from pysparkling import CacheManager
        >>>
        >>> n_exec = 0
        >>>
        >>> def _map(e):
        ...     global n_exec
        ...     n_exec += 1
        ...     return e*e
        >>>
        >>> my_rdd = Context().parallelize([1, 2, 3, 4], 2)
        >>> my_rdd = my_rdd.map(_map).cache()
        >>>
        >>> logging.info('no exec until here')
        >>> f = my_rdd.first()
        >>> logging.info('available caches in {1}: {0}'.format(
        ...     CacheManager.singleton().stored_idents(),
        ...     CacheManager.singleton(),
        ... ))
        >>>
        >>> logging.info('executed map on first partition only so far')
        >>> a = my_rdd.collect()
        >>> logging.info('available caches in {1}: {0}'.format(
        ...     CacheManager.singleton().stored_idents(),
        ...     CacheManager.singleton(),
        ... ))
        >>>
        >>> logging.info('now _map() was executed on all partitions and should'
        ...              'not be executed again')
        >>> logging.info('available caches in {1}: {0}'.format(
        ...     CacheManager.singleton().stored_idents(),
        ...     CacheManager.singleton(),
        ... ))
        >>> (my_rdd.collect(), n_exec)
        ([1, 4, 9, 16], 4)
        """
        return self.persist()

    def cartesian(self, other):
        """cartesian product of this RDD with ``other``

        :param RDD other: Another RDD.
        :rtype: RDD

        .. note::
            This is currently implemented as a local operation requiring
            all data to be pulled on one machine.


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([1, 2])
        >>> sorted(rdd.cartesian(rdd).collect())
        [(1, 1), (1, 2), (2, 1), (2, 2)]
        """
        v1 = self.toLocalIterator()
        v2 = other.collect()
        return self.context.parallelize([(a, b) for a in v1 for b in v2])

    def coalesce(self, numPartitions, shuffle=False):
        """coalesce

        :param int numPartitions: Number of partitions in the resulting RDD.
        :param shuffle: (optional) Not used.
        :rtype: RDD

        .. note::
            This is currently implemented as a local operation requiring
            all data to be pulled on one machine.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3], 2).coalesce(1).getNumPartitions()
        1
        """
        return self.context.parallelize(self.toLocalIterator(), numPartitions)

    def cogroup(self, other, numPartitions=None):
        """Groups keys from both RDDs together. Values are nested iterators.

        :param RDD other: The other RDD.
        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> c = Context()
        >>> a = c.parallelize([('house', 1), ('tree', 2)])
        >>> b = c.parallelize([('house', 3)])
        >>>
        >>> [(k, sorted(list([list(vv) for vv in v])))
        ...  for k, v in sorted(a.cogroup(b).collect())
        ... ]
        [('house', [[1], [3]]), ('tree', [[], [2]])]
        """

        d_self = defaultdict(list, self.groupByKey().collect())
        d_other = defaultdict(list, other.groupByKey().collect())
        return self.context.parallelize([
            (k, [list(d_self[k]), list(d_other[k])])
            for k in set(d_self.keys()) | set(d_other.keys())
        ])

    def collect(self):
        """returns the entire dataset as a list

        :rtype: list


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).collect()
        [1, 2, 3]
        """
        return self.context.runJob(
            self,
            unit_map,
            resultHandler=unit_collect,
        )

    def collectAsMap(self):
        """returns a dictionary for a pair dataset

        :rtype: dict


        Example:

        >>> from pysparkling import Context
        >>> d = Context().parallelize([('a', 1), ('b', 2)]).collectAsMap()
        >>> (d['a'], d['b'])
        (1, 2)
        """
        return dict(self.collect())

    def count(self):
        """number of entries in this dataset

        :rtype: int


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3], 2).count()
        3
        """
        return self.context.runJob(self, lambda tc, i: sum(1 for _ in i),
                                   resultHandler=sum)

    def countApprox(self):
        """same as :func:`RDD.count()`

        :rtype: int
        """
        return self.count()

    def countByKey(self):
        """returns a ``dict`` containing the count for every key

        :rtype: dict


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize(
        ...     [('a', 1), ('b', 2), ('b', 2)]
        ... ).countByKey()['b']
        2
        """
        return self.map(lambda r: r[0]).countByValue()

    def countByValue(self):
        """returns a ``dict`` containing the count for every value

        :rtype: dict


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 2, 4, 1]).countByValue()[2]
        2
        """
        def map_func(tc, x):
            r = defaultdict(int)
            for v in x:
                r[v] += 1
            return r
        return self.context.runJob(self, map_func,
                                   resultHandler=sum_counts_by_keys)

    def distinct(self, numPartitions=None):
        """returns only distinct elements

        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 2, 4, 1]).distinct().count()
        3
        """

        if numPartitions is None:
            numPartitions = self.getNumPartitions()

        return self.context.parallelize(list(set(self.toLocalIterator())),
                                        numPartitions)

    def filter(self, f):
        """filter elements

        :param f: a function that decides whether to keep an element
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize(
        ...     [1, 2, 2, 4, 1, 3, 5, 9], 3,
        ... ).filter(lambda x: x % 2 == 0).collect()
        [2, 2, 4]
        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (xx for xx in x if f(xx)),
            preservesPartitioning=True,
        )

    def first(self):
        """returns the first element in the dataset


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 2, 4, 1, 3, 5, 9], 3).first()
        1

        Works also with empty partitions:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2], 20).first()
        1
        """
        return self.context.runJob(
            self,
            lambda tc, iterable: iterable,
            allowLocal=True,
            resultHandler=lambda l: next(itertools.chain.from_iterable(l)),
        )

    def flatMap(self, f, preservesPartitioning=True):
        """map followed by flatten

        :param f: The map function.
        :param preservesPartitioning: (optional) Preserve the partitioning of
            the original RDD. Default True.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize(['hello', 'world']).flatMap(
        ...     lambda x: [ord(ch) for ch in x]
        ... ).collect()
        [104, 101, 108, 108, 111, 119, 111, 114, 108, 100]
        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (e for xx in x for e in f(xx)),
            preservesPartitioning=preservesPartitioning,
        )

    def flatMapValues(self, f):
        """map operation on values in a (key, value) pair followed by a flatten

        :param f: The map function.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([(1, 'hi'), (2, 'world')]).flatMapValues(
        ...     lambda x: [ord(ch) for ch in x]
        ... ).collect()
        [(1, 104), (1, 105), (2, 119), (2, 111), (2, 114), (2, 108), (2, 100)]
        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: ((xx[0], e) for xx in x for e in f(xx[1])),
            preservesPartitioning=True,
        )

    def fold(self, zeroValue, op):
        """fold

        :param zeroValue: The inital value, for example ``0`` or ``0.0``.
        :param op: The reduce operation.
        :returns: The folded (or aggregated) value.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 7, 2])
        >>> my_rdd.fold(0, lambda a, b: a+b)
        13
        """
        return self.aggregate(zeroValue, op, op)

    def foldByKey(self, zeroValue, op):
        """Fold (or aggregate) value by key.

        :param zeroValue: The inital value, for example ``0`` or ``0.0``.
        :param op: The reduce operation.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([('a', 4), ('b', 7), ('a', 2)])
        >>> my_rdd.foldByKey(0, lambda a, b: a+b).collectAsMap()['a']
        6
        """
        return self.aggregateByKey(zeroValue, op, op)

    def foreach(self, f):
        """applies ``f`` to every element

        It does not return a new RDD like :func:`RDD.map()`.

        :param f: Apply a function to every element.
        :rtype: None


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([1, 2, 3])
        >>> a = []
        >>> my_rdd.foreach(lambda x: a.append(x))
        >>> len(a)
        3
        """
        self.context.runJob(self, lambda tc, x: [f(xx) for xx in x],
                            resultHandler=None)

    def foreachPartition(self, f):
        """applies ``f`` to every partition

        It does not return a new RDD like :func:`RDD.mapPartitions()`.

        :param f: Apply a function to every partition.
        :rtype: None
        """
        self.context.runJob(self, lambda tc, x: f(x),
                            resultHandler=None)

    def fullOuterJoin(self, other, numPartitions=None):
        """returns the full outer join of two RDDs

        The output contains all keys from both input RDDs, with missing
        keys replaced with None.

        :param RDD other: The RDD to join to this one.
        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> sc = Context()
        >>> rdd1 = sc.parallelize([('a', 0), ('b', 1)])
        >>> rdd2 = sc.parallelize([('b', 2), ('c', 3)])
        >>> sorted(
        ...     rdd1.fullOuterJoin(rdd2).collect()
        ... )
        [('a', (0, None)), ('b', (1, 2)), ('c', (None, 3))]
        """

        grouped = self.cogroup(other, numPartitions)

        return grouped.flatMap(lambda kv: [
            (kv[0], (v_self, v_other))
            for v_self in (kv[1][0] if kv[1][0] else [None])
            for v_other in (kv[1][1] if kv[1][1] else [None])
        ])

    def getNumPartitions(self):
        """returns the number of partitions

        :rtype: int
        """
        return len(self.partitions())

    def getPartitions(self):
        """returns the partitions of this RDD"""
        return self.partitions()

    def groupBy(self, f, numPartitions=None):
        """group by f

        :param f: Function returning a key given an element of the dataset.
        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 7, 2])
        >>> my_rdd.groupBy(lambda x: x % 2).mapValues(sorted).collect()
        [(0, [2, 4]), (1, [7])]
        """

        return self.keyBy(f).groupByKey(numPartitions)

    def groupByKey(self, numPartitions=None):
        """group by key

        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.
        """

        if numPartitions is None:
            numPartitions = self.getNumPartitions()

        r = defaultdict(list)
        for key, value in self.toLocalIterator():
            r[key].append(value)

        return self.context.parallelize(r.items(), numPartitions)

    def histogram(self, buckets):
        """histogram

        :param buckets:
            A list of bucket boundaries or an int for the number of buckets.

        :returns:
            A tuple (bucket_boundaries, histogram_values) where
            bucket_boundaries is a list of length n+1 boundaries and
            histogram_values is a list of length n with the values of each
            bucket.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([0, 4, 7, 4, 10])
        >>> b, h = my_rdd.histogram(10)
        >>> h
        [1, 0, 0, 0, 2, 0, 0, 1, 0, 0, 1]
        """
        if isinstance(buckets, int):
            num_buckets = buckets
            stats = self.stats()
            min_v = stats.min()
            max_v = stats.max()
            buckets = [min_v + float(i) * (max_v - min_v) / num_buckets
                       for i in range(num_buckets + 1)]
        h = [0 for _ in buckets]
        for x in self.toLocalIterator():
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
        """intersection of this and other RDD

        :param RDD other: The other dataset to do the intersection with.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([0, 4, 7, 4, 10])
        >>> rdd2 = Context().parallelize([3, 4, 7, 4, 5])
        >>> rdd1.intersection(rdd2).collect()
        [4, 7]
        """
        return self.context.parallelize(
            list(set(self.toLocalIterator()) & set(other.toLocalIterator()))
        )

    def isCheckpointed(self):
        return False

    def join(self, other, numPartitions=None):
        """join

        :param RDD other: The other RDD.
        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([(0, 1), (1, 1)])
        >>> rdd2 = Context().parallelize([(2, 1), (1, 3)])
        >>> rdd1.join(rdd2).collect()
        [(1, (1, 3))]
        """

        if numPartitions is None:
            numPartitions = self.getNumPartitions()

        d1 = dict(self.collect())
        d2 = dict(other.collect())
        keys = set(d1.keys()) & set(d2.keys())
        return self.context.parallelize((
            (k, (d1[k], d2[k]))
            for k in keys
        ), numPartitions)

    def keyBy(self, f):
        """key by f

        :param f: Function that returns a key from a dataset element.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([0, 4, 7, 4, 10])
        >>> rdd.keyBy(lambda x: x % 2).collect()
        [(0, 0), (0, 4), (1, 7), (0, 4), (0, 10)]
        """
        return self.map(lambda e: (f(e), e))

    def keys(self):
        """keys of a pair dataset

        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([(0, 1), (1, 1)]).keys().collect()
        [0, 1]
        """
        return self.map(lambda e: e[0])

    def leftOuterJoin(self, other, numPartitions=None):
        """left outer join

        :param RDD other: The other RDD.
        :param int numPartitions: Number of partitions in the resulting RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([(0, 1), (1, 1)])
        >>> rdd2 = Context().parallelize([(2, 1), (1, 3)])
        >>> rdd1.leftOuterJoin(rdd2).collect()
        [(0, (1, None)), (1, (1, 3))]
        """

        d_other = other.groupByKey().collectAsMap()

        return self.groupByKey().flatMap(lambda kv: [
            (kv[0], (v_self, v_other))
            for v_self in kv[1]
            for v_other in (d_other[kv[0]] if kv[0] in d_other else [None])
        ])

    def lookup(self, key):
        """Return all the (key, value) pairs where the given key matches.

        :param key: The key to lookup.
        :rtype: list


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([(0, 1), (1, 1), (1, 3)]).lookup(1)
        [1, 3]
        """
        return self.filter(lambda x: x[0] == key).values().collect()

    def map(self, f):
        """map

        :param f: map function for elements
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).map(lambda x: x+1).collect()
        [2, 3, 4]
        """
        return MapPartitionsRDD(
            self,
            MapF(f),
            preservesPartitioning=True,
        )

    def mapPartitions(self, f, preservesPartitioning=False):
        """map partitions

        :param f: map function for partitions
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([1, 2, 3, 4], 2)
        >>> def f(iterator):
        ...     yield sum(iterator)
        >>> rdd.mapPartitions(f).collect()
        [3, 7]
        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: f(x),
            preservesPartitioning=preservesPartitioning,
        )

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """map partitions with index

        :param f: map function for (index, partition)
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([9, 8, 7, 6, 5, 4], 3)
        >>> def f(splitIndex, iterator):
        ...     yield splitIndex
        >>> rdd.mapPartitionsWithIndex(f).sum()
        3
        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: f(i, x),
            preservesPartitioning=preservesPartitioning,
        )

    def mapValues(self, f):
        """map values in a pair dataset

        :param f: map function for values
        :rtype: RDD
        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: ((e[0], f(e[1])) for e in x),
            preservesPartitioning=True,
        )

    def max(self):
        """returns the maximum element


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3, 4, 3, 2], 2).max() == 4
        True
        """
        return self.stats().max()

    def mean(self):
        """returns the mean of this dataset


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([0, 4, 7, 4, 10]).mean()
        5.0
        """
        return self.stats().mean()

    def min(self):
        """returns the minimum element"""
        return self.stats().min()

    def name(self):
        """returns the name of the dataset"""
        if self._name is None:
            return 'RDD_{}'.format(self._rdd_id)

        return self._name

    def partitionBy(self, numPartitions, partitionFunc=None):
        """Return a partitioned copy of this key-value RDD.

        :param int numPartitions: Number of partitions.
        :param function partitionFunc: Partition function.
        :rtype: RDD


        Example where even numbers get assigned to partition 0
        and odd numbers to partition 1:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> rdd = sc.parallelize([1, 3, 2, 7, 8, 5], 1)
        >>> keyvalue_rdd = rdd.map(lambda x: (x, x))
        >>> keyvalue_rdd.partitionBy(2).keys().collect()
        [2, 8, 1, 3, 7, 5]
        """

        if partitionFunc is None:
            partitionFunc = _hash

        new_partitions = [[] for _ in range(numPartitions)]
        for key_value in self.toLocalIterator():
            idx = partitionFunc(key_value[0]) % numPartitions
            new_partitions[idx].append(key_value)

        return self.context._parallelize_partitions(new_partitions)

    def persist(self, storageLevel=None):
        """Cache the results of computed partitions.

        :param storageLevel: Not used.
        """
        return PersistedRDD(self, storageLevel=storageLevel)

    def pipe(self, command, env=None):
        """Run a command with the elements in the dataset as argument.

        :param command: Command line command to run.
        :param dict env: environment variables
        :rtype: RDD

        .. warning::
            Unsafe for untrusted data.


        Example:

        >>> from pysparkling import Context
        >>> piped = Context().parallelize(['0', 'hello', 'world']).pipe('echo')
        >>> b'hello\\n' in piped.collect()
        True
        """
        if env is None:
            env = {}

        return self.context.parallelize(subprocess.check_output(
            [command] + x if isinstance(x, list) else [command, x]
        ) for x in self.collect())

    def randomSplit(self, weights, seed=None):
        """Split the RDD into a few RDDs according to the given weights.

        :param list[float] weights: relative lengths of the resulting RDDs
        :param int seed: seed for random number generator
        :returns: a list of RDDs
        :rtype: list

        .. note::
            Creating the new RDDs is currently implemented as a local
            operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize(range(500))
        >>> rdd1, rdd2 = rdd.randomSplit([2, 3], seed=42)
        >>> (rdd1.count(), rdd2.count())
        (199, 301)
        """
        sum_weights = sum(weights)
        boundaries = [0]
        for w in weights:
            boundaries.append(boundaries[-1] + w / sum_weights)
        random.seed(seed)

        lists = [[] for _ in weights]
        for e in self.toLocalIterator():
            r = random.random()
            for i, lbub in enumerate(zip(boundaries[:-1], boundaries[1:])):
                if r >= lbub[0] and r < lbub[1]:
                    lists[i].append(e)

        return [self.context.parallelize(l) for l in lists]

    def reduce(self, f):
        """reduce

        :param f: A commutative and associative binary operator.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([0, 4, 7, 4, 10]).reduce(lambda a, b: a+b)
        25
        """
        return self.context.runJob(
            self,
            lambda tc, x: functools.reduce(f, x),
            resultHandler=lambda x: functools.reduce(f, x),
        )

    def reduceByKey(self, f):
        """reduce by key

        :param f: A commutative and associative binary operator.
        :rtype: RDD

        .. note::
            This operation includes a :func:`pysparkling.RDD.groupByKey()`
            which is a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([(0, 1), (1, 1), (1, 3)])
        >>> rdd.reduceByKey(lambda a, b: a+b).collect()
        [(0, 1), (1, 4)]
        """
        return self.groupByKey().mapValues(lambda x: functools.reduce(f, x))

    def repartition(self, numPartitions):
        """repartition

        :param int numPartitions: Number of partitions in new RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.
        """
        return self.context.parallelize(self.toLocalIterator(), numPartitions)

    def repartitionAndSortWithinPartitions(
        self, numPartitions=None, partitionFunc=None,
        ascending=True, keyfunc=None,
    ):
        """Repartition and sort within each partition.

        :param int numPartitions: Number of partitions in new RDD.
        :param partitionFunc: function that partitions
        :param ascending: Default is True.
        :param keyfunc: Returns the value that will be sorted.
        :rtype: RDD


        Example where even numbers are assigned to partition 0 and odd numbers
        to partition 1 and then the partitions are sorted individually:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> rdd = sc.parallelize([1, 3, 2, 7, 8, 5], 1)
        >>> kv_rdd = rdd.map(lambda x: (x, x))
        >>> processed = kv_rdd.repartitionAndSortWithinPartitions(2)
        >>> processed.keys().collect()
        [2, 8, 1, 3, 5, 7]
        """

        def partition_sort(data):
            return sorted(data, key=keyfunc, reverse=not ascending)

        return (
            self
            .partitionBy(numPartitions, partitionFunc)
            .mapPartitions(partition_sort)
        )

    def rightOuterJoin(self, other, numPartitions=None):
        """right outer join

        :param RDD other: The other RDD.
        :param int numPartitions: Number of partitions in new RDD.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> rdd1 = sc.parallelize([(0, 1), (1, 1)])
        >>> rdd2 = sc.parallelize([(2, 1), (1, 3)])
        >>> sorted(rdd1.rightOuterJoin(rdd2).collect())
        [(1, (1, 3)), (2, (None, 1))]
        """

        d_self = self.groupByKey().collectAsMap()

        return other.groupByKey().flatMap(lambda kv: [
            (kv[0], (v_self, v_other))
            for v_other in kv[1]
            for v_self in (d_self[kv[0]] if kv[0] in d_self else [None])
        ])

    def sample(self, withReplacement, fraction, seed=None):
        """randomly sample

        :param bool withReplacement: sample with replacement
        :param float fraction: probability that an element is sampled
        :param seed: (optional) Seed for random number generator
        :rtype: RDD

        Sampling without replacement uses Bernoulli sampling and ``fraction``
        is the probability that an element is sampled. Sampling with
        replacement uses Poisson sampling where ``fraction`` is the
        expectation.

        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize(range(1000))
        >>> sampled = rdd.sample(False, 0.1, seed=5).collect()
        >>> len(sampled)
        115
        >>> sampled_with_replacement = rdd.sample(True, 5.0, seed=5).collect()
        >>> len(sampled_with_replacement) in (5067, 5111)  # w/o, w/ numpy
        True
        """
        sampler = (PoissonSampler(fraction)
                   if withReplacement else BernoulliSampler(fraction))

        return PartitionwiseSampledRDD(
            self, sampler, preservesPartitioning=True, seed=seed)

    def sampleByKey(self, withReplacement, fractions, seed=None):
        """randomly sample by key

        :param bool withReplacement: sample with replacement
        :param dict fractions: per key sample probabilities
        :param seed: (optional) Seed for random number generator.
        :rtype: RDD

        Sampling without replacement uses Bernoulli sampling and ``fraction``
        is the probability that an element is sampled. Sampling with
        replacement uses Poisson sampling where ``fraction`` is the
        expectation.


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> fractions = {"a": 0.2, "b": 0.1}
        >>> rdd = (sc
        ...        .parallelize(fractions.keys())
        ...        .cartesian(sc.parallelize(range(0, 1000))))
        >>> sample = (rdd
        ...           .sampleByKey(False, fractions, 2)
        ...           .groupByKey().collectAsMap())
        >>> 100 < len(sample["a"]) < 300 and 50 < len(sample["b"]) < 150
        True
        >>> max(sample["a"]) <= 999 and min(sample["a"]) >= 0
        True
        >>> max(sample["b"]) <= 999 and min(sample["b"]) >= 0
        True
        """
        sampler = (PoissonSamplerPerKey(fractions)
                   if withReplacement else BernoulliSamplerPerKey(fractions))

        return PartitionwiseSampledRDD(
            self, sampler, preservesPartitioning=True, seed=seed)

    def sampleStdev(self):
        """sample standard deviation

        :rtype: float


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).sampleStdev()
        1.0
        """
        return self.stats().sampleStdev()

    def sampleVariance(self):
        """sample variance

        :rtype: float


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).sampleVariance()
        1.0
        """
        return self.stats().sampleVariance()

    def saveAsPickleFile(self, path, batchSize=10):
        """save as pickle file

        :returns: ``self``
        :rtype: RDD

        .. warning::
            The output of this function is incompatible with the PySpark
            output as there is no pure Python way to write Sequence files.


        Example:

        >>> from pysparkling import Context
        >>> from tempfile import NamedTemporaryFile
        >>> tmpFile = NamedTemporaryFile(delete=True)
        >>> tmpFile.close()
        >>> d = ['hello', 'world', 1, 2]
        >>> rdd = Context().parallelize(d).saveAsPickleFile(tmpFile.name)
        >>> 'hello' in Context().pickleFile(tmpFile.name).collect()
        True
        """

        if fileio.File(path).exists():
            raise FileAlreadyExistsException(
                'Output {0} already exists.'.format(path)
            )

        codec_suffix = ''
        if path.endswith(tuple('.' + ending
                               for endings, _ in fileio.codec.FILE_ENDINGS
                               for ending in endings)):
            codec_suffix = path[path.rfind('.'):]

        def _map(path, obj):
            stream = io.BytesIO()
            pickle.dump(obj, stream)
            stream.seek(0)
            fileio.File(path).dump(stream)

        if self.getNumPartitions() == 1:
            _map(path, self.collect())
            return self

        self.context.runJob(
            self,
            lambda tc, x: _map(
                os.path.join(path, 'part-{0:05d}{1}'.format(tc.partitionId(),
                                                            codec_suffix)),
                list(x),
            ),
            resultHandler=list,
        )
        fileio.TextFile(os.path.join(path, '_SUCCESS')).dump()
        return self

    def saveAsTextFile(self, path, compressionCodecClass=None):
        """save as text file

        If the RDD has many partitions, the contents will be stored directly
        in the given path. If the RDD has more partitions, the data of the
        partitions are stored in individual files under ``path/part-00000`` and
        so on and once all partitions are written, the file ``path/_SUCCESS``
        is written last.

        :param path: Destination of the text file.
        :param compressionCodecClass: Not used.
        :returns: ``self``
        :rtype: RDD
        """
        if fileio.TextFile(path).exists():
            raise FileAlreadyExistsException(
                'Output {0} already exists.'.format(path)
            )

        codec_suffix = ''
        if path.endswith(tuple('.' + ending
                               for endings, _ in fileio.codec.FILE_ENDINGS
                               for ending in endings)):
            codec_suffix = path[path.rfind('.'):]

        if self.getNumPartitions() == 1:
            fileio.TextFile(
                path
            ).dump(io.StringIO(''.join([
                '{}\n'.format(xx) for xx in self.toLocalIterator()
            ])))
            return self

        self.context.runJob(
            self,
            lambda tc, x: fileio.TextFile(
                os.path.join(path, 'part-{0:05d}{1}'.format(tc.partitionId(),
                                                            codec_suffix))
            ).dump(io.StringIO(''.join([
                '{}\n'.format(xx) for xx in x
            ]))),
            resultHandler=list,
        )
        fileio.TextFile(os.path.join(path, '_SUCCESS')).dump()
        return self

    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        """sort by keyfunc

        :param keyfunc: Returns the value that will be sorted.
        :param ascending: Default is True.
        :param int numPartitions:
            Default is None. None means the output will have the same number of
            partitions as the input.
        :rtype: RDD

        .. note::
            Sorting is currently implemented as a local operation.


        Examples:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([5, 1, 2, 3])
        >>> rdd.sortBy(lambda x: x).collect()
        [1, 2, 3, 5]

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([1, 5, 2, 3])
        >>> rdd.sortBy(lambda x: x, ascending=False).collect()
        [5, 3, 2, 1]
        """

        if numPartitions is None:
            numPartitions = self.getNumPartitions()

        return self.context.parallelize(
            sorted(self.collect(), key=keyfunc, reverse=not ascending),
            numPartitions,
        )

    def sortByKey(self, ascending=True, numPartitions=None,
                  keyfunc=itemgetter(0)):
        """sort by key

        :param ascending: Default is True.
        :param int numPartitions: Default is None. None means the output will
            have the same number of partitions as the input.
        :param keyfunc: Returns the value that will be sorted.
        :rtype: RDD

        .. note::
            Sorting is currently implemented as a local operation.


        Examples:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize(
        ...     [(5, 'a'), (1, 'b'), (2, 'c'), (3, 'd')]
        ... )
        >>> rdd.sortByKey().collect()[0][1] == 'b'
        True

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize(
        ...     [(1, 'b'), (5, 'a'), (2, 'c'), (3, 'd')]
        ... )
        >>> rdd.sortByKey(ascending=False).collect()[0][1] == 'a'
        True
        """
        return self.sortBy(keyfunc, ascending, numPartitions)

    def stats(self):
        """stats

        :rtype: StatCounter


        Example:

        >>> from pysparkling import Context
        >>> d = [1, 4, 9, 16, 25, 36]
        >>> s = Context().parallelize(d, 3).stats()
        >>> sum(d)/len(d) == s.mean()
        True
        """
        return self.aggregate(
            StatCounter(),
            lambda a, b: a.merge(b),
            lambda a, b: a.mergeStats(b),
        )

    def stdev(self):
        """standard deviation

        :rtype: float


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1.5, 2.5]).stdev()
        0.5
        """
        return self.stats().stdev()

    def subtract(self, other, numPartitions=None):
        """subtract

        :param RDD other: The RDD to subtract from the current RDD.
        :param int numPartitions: Currently not used. Partitions are preserved.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([(0, 1), (1, 1)])
        >>> rdd2 = Context().parallelize([(1, 1), (1, 3)])
        >>> rdd1.subtract(rdd2).collect()
        [(0, 1)]
        """
        list_other = other.collect()
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (e for e in x if e not in list_other),
            preservesPartitioning=True,
        )

    def sum(self):
        """sum of all the elements

        :rtype: float


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([0, 4, 7, 4, 10]).sum()
        25
        """
        return self.context.runJob(self, lambda tc, x: sum(x),
                                   resultHandler=sum)

    def take(self, n):
        """Take n elements and return them in a list.

        Only evaluates the partitions that are necessary to return n elements.

        :param int n: Number of elements to return.
        :rtype: list


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([4, 7, 2]).take(2)
        [4, 7]


        Another example where only the first two partitions only are computed
        (check the debug logs):

        >>> from pysparkling import Context
        >>> Context().parallelize([4, 7, 2], 3).take(2)
        [4, 7]
        """

        return self.context.runJob(
            self,
            lambda tc, i: i,
            allowLocal=True,
            resultHandler=lambda l: list(itertools.islice(
                itertools.chain.from_iterable(l),
                n,
            )),
        )

    def takeSample(self, n):
        """take sample

        Assumes samples are evenly distributed between partitions.
        Only evaluates the partitions that are necessary to return n elements.

        :param int n: The number of elements to sample.
        :rtype: list


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([4, 7, 2]).takeSample(1)[0] in [4, 7, 2]
        True


        Another example where only one partition is computed
        (check the debug logs):

        >>> from pysparkling import Context
        >>> d = [4, 9, 7, 3, 2, 5]
        >>> Context().parallelize(d, 3).takeSample(1)[0] in d
        True
        """

        rnd_entries = sorted([random.random() for _ in range(n)])
        num_partitions = self.getNumPartitions()

        rnd_entries = [
            (
                int(e * num_partitions),  # partition number
                e * num_partitions - int(e * num_partitions),  # e in partition
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
                for _, e in e_list[1]:
                    e_num = int(e * len(p_result))
                    r.append(p_result[e_num])
            return r

        return self.context.runJob(
            self, lambda tc, i: list(i), partitions=partitions,
            resultHandler=res_handler,
        )

    def toLocalIterator(self):
        """Returns an iterator over the dataset.


        Example:

        >>> from pysparkling import Context
        >>> sum(Context().parallelize([4, 9, 7, 3, 2, 5], 3).toLocalIterator())
        30
        """
        return self.context.runJob(
            self, lambda tc, i: list(i),
            resultHandler=lambda l: (x for p in l for x in p),
        )

    def top(self, num, key=None):
        """Top N elements in descending order.

        :param int num: number of elements
        :param key: optional key function
        :rtype: list


        Example:

        >>> from pysparkling import Context
        >>> r = Context().parallelize([4, 9, 7, 3, 2, 5], 3)
        >>> r.top(2)
        [9, 7]
        """

        def unit(x):
            return x

        if key is None:
            key = unit

        return self.sortBy(key, ascending=False).take(num)

    def union(self, other):
        """union

        :param RDD other: The other RDD for the union.
        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 9, 7, 3, 2, 5], 3)
        >>> my_rdd.union(my_rdd).count()
        12
        """
        return self.context.union((self, other))

    def values(self):
        """Values of a (key, value) dataset.

        :rtype: RDD
        """
        return self.map(lambda e: e[1])

    def variance(self):
        """The variance of the dataset.

        :rtype: float


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1.5, 2.5]).variance()
        0.25
        """
        return self.stats().variance()

    def zip(self, other):
        """zip

        :param RDD other: Other dataset to zip with.
        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 9, 7, 3, 2, 5], 3)
        >>> my_rdd.zip(my_rdd).collect()
        [(4, 4), (9, 9), (7, 7), (3, 3), (2, 2), (5, 5)]
        """
        return self.context.parallelize(
            zip(self.toLocalIterator(), other.toLocalIterator())
        )

    def zipWithIndex(self):
        """Returns pairs of an original element and its index.

        :rtype: RDD

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 9, 7, 3, 2, 5], 3)
        >>> my_rdd.zipWithIndex().collect()
        [(4, 0), (9, 1), (7, 2), (3, 3), (2, 4), (5, 5)]
        """
        return self.context.parallelize(
            (d, i) for i, d in enumerate(self.toLocalIterator())
        )

    def zipWithUniqueId(self):
        """Zip every entry with a unique index.

        This is a fast operation.

        :rtype: RDD


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([423, 234, 986, 5, 345], 3)
        >>> my_rdd.zipWithUniqueId().collect()
        [(423, 0), (234, 1), (986, 4), (5, 2), (345, 5)]
        """
        num_p = self.getNumPartitions()
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (
                (xx, e * num_p + tc.partition_id) for e, xx in enumerate(x)
            ),
            preservesPartitioning=True,
        )


class MapPartitionsRDD(RDD):
    def __init__(self, prev, f, preservesPartitioning=False):
        """``prev`` is the previous RDD.

        ``f`` is a function with the signature
        ``(task_context, partition index, iterator over elements)``.
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
    def __init__(self, prev, sampler, preservesPartitioning=False,
                 seed=None):
        """RDD with sampled partitions.

        :param RDD prev: previous RDD
        :param sampler: a sampler
        :param bool preservesPartitioning: preserve partitioning (not used)
        :param int seed: random number generator seed (can be None)
        """
        RDD.__init__(self, prev.partitions(), prev.context)

        if seed is None:
            seed = random.randint(0, maxint)

        self.prev = prev
        self.sampler = sampler
        self.preservesPartitioning = preservesPartitioning
        self.seed = seed

    def compute(self, split, task_context):
        random.seed(self.seed + split.index)
        if numpy is not None:
            numpy.random.seed(self.seed + split.index)
        return (
            x
            for x in self.prev.compute(split, task_context._create_child())
            for _ in range(self.sampler(x))
        )

    def partitions(self):
        return self.prev.partitions()


class PersistedRDD(RDD):
    def __init__(self, prev, storageLevel=None):
        """persisted RDD

        :param RDD prev: previous RDD
        """
        RDD.__init__(self, prev.partitions(), prev.context)
        self.prev = prev
        self.storageLevel = storageLevel

    def compute(self, split, task_context):
        if self._rdd_id is None or split.index is None:
            cid = None
        else:
            cid = (self._rdd_id, split.index)

        cm = CacheManager.singleton()
        if not cm.has(cid):
            data = list(self.prev.compute(split, task_context._create_child()))
            cm.add(cid, data, self.storageLevel)
        else:
            data = cm.get(cid)

        return iter(data)


class EmptyRDD(RDD):
    def __init__(self, context):
        RDD.__init__(self, [], context)


# pickle-able helpers

class MapF(object):
    def __init__(self, f):
        self.f = f

    def __call__(self, tc, i, x):
        return (self.f(xx) for xx in x)


def unit_map(task_context, elements):
    return list(elements)


def unit_collect(l):
    return [x for p in l for x in p]


def sum_counts_by_keys(list_of_pairlists):
    r = defaultdict(int)  # calling int results in a zero
    for l in list_of_pairlists:
        for key, count in l.items():
            r[key] += count
    return r
