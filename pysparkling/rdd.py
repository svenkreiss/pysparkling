"""
Provides a Python implementation of RDDs.

"""

from __future__ import (division, absolute_import, print_function,
                        unicode_literals)

import io
import sys
import copy
import pickle
import random
import logging
import functools
import itertools
import subprocess
from collections import defaultdict

from . import utils
from .fileio import File, TextFile
from .stat_counter import StatCounter
from .cache_manager import CacheManager
from .exceptions import FileAlreadyExistsException

try:
    from itertools import izip as zip  # Python 2
except ImportError:
    pass                               # Python 3

log = logging.getLogger(__name__)


class RDD(object):
    """
    In Spark's original form, RDDs are Resilient, Distributed Datasets.
    This class reimplements the same interface with the goal of being
    fast on small data at the cost of being resilient and distributed.

    :param partitions:
        A list of instances of :class:`Partition`.

    :param ctx:
        An instance of the applicable :class:`Context`.

    """

    def __init__(self, partitions, ctx):
        self._p = partitions
        self.context = ctx
        self._name = None
        self._rdd_id = ctx.newRddId()

    def __getstate__(self):
        r = dict((k, v) for k, v in self.__dict__.items())
        r['_p'] = list(self.partitions())
        r['context'] = None
        return r

    def compute(self, split, task_context):
        """split is a partition. This function is used in derived RDD
        classes to add smarter behavior for specific cases."""
        return split.x()

    def partitions(self):
        self._p, r = itertools.tee(self._p, 2)
        return r

    """

    Public API
    ----------
    """

    def aggregate(self, zeroValue, seqOp, combOp):
        """
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
        """
        [distributed]

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

        :param numPartitions: (optional)
            Not used.

        :returns:
            Output of ``combOp`` operations.


        Example:

        >>> from pysparkling import Context
        >>> seqOp = (lambda x, y: x + y)
        >>> combOp = (lambda x, y: x + y)
        >>> r = Context().parallelize(
        ...     [('a', 1), ('b', 2), ('a', 3), ('c', 4)]
        ... ).aggregateByKey(0, seqOp, combOp)
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

        return self.context.runJob(self, seqFuncByKey,
                                   resultHandler=combFuncByKey)

    def cache(self):
        """
        Whenever a partition is computed, cache the result.
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
        """
        :param other:
            Another RDD.

        :returns:
            A new RDD with the cartesian product of this RDD with ``other``.

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
        v2 = self.collect()
        return self.context.parallelize([(a, b) for a in v1 for b in v2])

    def coalesce(self, numPartitions, shuffle=False):
        """
        :param numPartitions:
            Number of partitions in the resulting RDD.

        :param shuffle: (optional)
            Not used.

        :returns:
            A new RDD.

        .. note::
            This is currently implemented as a local operation requiring
            all data to be pulled on one machine.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3], 2).coalesce(1).getNumPartitions()
        1

        """
        return self.context.parallelize(self.toLocalIterator(), numPartitions)

    def collect(self):
        """
        :returns:
            The entire dataset as a list.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).collect()
        [1, 2, 3]

        """
        return self.context.runJob(
            self, lambda tc, i: list(i),
            resultHandler=lambda l: [x for p in l for x in p],
        )

    def count(self):
        """
        :returns:
            Number of entries in this dataset.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3], 2).count()
        3

        """
        return self.context.runJob(self, lambda tc, i: sum(1 for _ in i),
                                   resultHandler=sum)

    def countApprox(self):
        """
        Same as :func:`RDD.count()`.

        """
        return self.count()

    def countByKey(self):
        """
        :returns:
            A ``dict`` containing the count for every key.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize(
        ...     [('a', 1), ('b', 2), ('b', 2)]
        ... ).countByKey()['b']
        2

        """
        return self.map(lambda r: r[0]).countByValue()

    def countByValue(self):
        """
        :returns:
            A ``dict`` containing the count for every value.


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
                                   resultHandler=utils.sum_counts_by_keys)

    def distinct(self, numPartitions=None):
        """
        :param numPartitions:
            The number of partitions of the newly created RDD.

        :returns:
            A new RDD containing only distict elements.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 2, 4, 1]).distinct().count()
        3

        """
        return self.context.parallelize(list(set(self.toLocalIterator())),
                                        numPartitions)

    def filter(self, f):
        """
        :param f:
            A reference to a function that if it evaluates to true when applied
            to an element in the dataset, the element is kept.

        :returns:
            A new dataset.


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
        """
        :returns:
            The first element in the dataset.


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
        """
        A map operation followed by flattening.

        :param f:
            The map function.

        :param preservesPartitioning: (optional)
            Preserve the partitioning of the original RDD. Default True.

        :returns:
            A new RDD.


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
        """
        A map operation on the values in a (key, value) pair followed by a map.

        :param f:
            The map function.

        :returns:
            A new RDD.


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
        """
        :param zeroValue:
            The inital value, for example ``0`` or ``0.0``.

        :param op:
            The reduce operation.

        :returns:
            The folded (or aggregated) value.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 7, 2])
        >>> my_rdd.fold(0, lambda a, b: a+b)
        13

        """
        return self.aggregate(zeroValue, op, op)

    def foldByKey(self, zeroValue, op):
        """
        :param zeroValue:
            The inital value, for example ``0`` or ``0.0``.

        :param op:
            The reduce operation.

        :returns:
            The folded (or aggregated) value by key.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([('a', 4), ('b', 7), ('a', 2)])
        >>> my_rdd.foldByKey(0, lambda a, b: a+b)['a']
        6

        """
        return self.aggregateByKey(zeroValue, op, op)

    def foreach(self, f):
        """
        Applies ``f`` to every element, but does not return a new RDD like
        :func:`RDD.map()`.

        :param f:
            Apply a function to every element.


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
        """
        Applies ``f`` to every partition, but does not return a new RDD like
        :func:`RDD.mapPartitions()`.

        :param f:
            Apply a function to every partition.

        """
        self.context.runJob(self, lambda tc, x: f(x),
                            resultHandler=None)

    def getNumPartitions(self):
        """
        :returns:
            Returns the number of partitions.

        """
        return sum(1 for _ in self.partitions())

    def getPartitions(self):
        """
        :returns:
            The partitions of this RDD.

        """
        return self.partitions()

    def groupBy(self, f, numPartitions=None):
        """
        :param f:
            Function returning a key given an element of the dataset.

        :param numPartitions:
            The number of partitions in the new grouped dataset.

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 7, 2])
        >>> my_rdd.groupBy(lambda x: x % 2).collect()
        [(0, [2, 4]), (1, [7])]

        """
        return self.context.parallelize((
            (k, [gg[1] for gg in g]) for k, g in itertools.groupby(
                sorted(self.keyBy(f).collect()),
                lambda e: e[0],
            )
        ), numPartitions)

    def groupByKey(self, numPartitions=None):
        """
        :param numPartitions:
            The number of partitions in the new grouped dataset.

        .. note::
            Creating the new RDD is currently implemented as a local operation.

        """
        return self.context.parallelize((
            (k, [gg[1] for gg in g]) for k, g in itertools.groupby(
                sorted(self.collect()),
                lambda e: e[0],
            )
        ), numPartitions)

    def histogram(self, buckets):
        """
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
            buckets = [min_v + float(i)*(max_v-min_v)/num_buckets
                       for i in range(num_buckets+1)]
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
        """
        :param other:
            The other dataset to do the intersection with.

        :returns:
            A new RDD containing the intersection of this and the other RDD.

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
        """
        :param other:
            The other RDD.

        :param numPartitions:
            Number of partitions to create in the new RDD.

        :returns:
            A new RDD containing the join.

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([(0, 1), (1, 1)])
        >>> rdd2 = Context().parallelize([(2, 1), (1, 3)])
        >>> rdd1.join(rdd2).collect()
        [(1, (1, 3))]

        """
        d1 = dict(self.collect())
        d2 = dict(other.collect())
        keys = set(d1.keys()) & set(d2.keys())
        return self.context.parallelize((
            (k, (d1[k], d2[k]))
            for k in keys
        ), numPartitions)

    def keyBy(self, f):
        """
        :param f:
            Function that returns a key from a dataset element.

        :returns:
            A new RDD containing the keyed data.


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize([0, 4, 7, 4, 10])
        >>> rdd.keyBy(lambda x: x % 2).collect()
        [(0, 0), (0, 4), (1, 7), (0, 4), (0, 10)]

        """
        return self.map(lambda e: (f(e), e))

    def keys(self):
        """
        :returns:
            A new RDD containing the keys of the current RDD.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([(0, 1), (1, 1)]).keys().collect()
        [0, 1]

        """
        return self.map(lambda e: e[0])

    def leftOuterJoin(self, other, numPartitions=None):
        """
        :param other:
            The other RDD.

        :param numPartitions: (optional)
            Number of partitions of the resulting RDD.

        :returns:
            A new RDD with the result of the join.

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([(0, 1), (1, 1)])
        >>> rdd2 = Context().parallelize([(2, 1), (1, 3)])
        >>> rdd1.leftOuterJoin(rdd2).collect()
        [(0, (1, None)), (1, (1, 3))]

        """
        d1 = dict(self.collect())
        d2 = dict(other.collect())
        return self.context.parallelize((
            (k, (d1[k], d2[k] if k in d2 else None))
            for k in d1.keys()
        ), numPartitions)

    def lookup(self, key):
        """
        Return all the (key, value) pairs where the given key matches.

        :param key:
            The key to lookup.

        :returns:
            A list of matched (key, value) pairs.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([(0, 1), (1, 1), (1, 3)]).lookup(1)
        [1, 3]

        """
        return self.filter(lambda x: x[0] == key).values().collect()

    def map(self, f):
        """
        :param f:
            Map function.

        :returns:
            A new RDD with mapped values.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).map(lambda x: x+1).collect()
        [2, 3, 4]

        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: (f(xx) for xx in x),
            preservesPartitioning=True,
        )

    def mapPartitions(self, f, preservesPartitioning=False):
        """
        :param f:
            Map function.

        :returns:
            A new RDD with mapped partitions.


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

    def mapValues(self, f):
        """
        :param f:
            Map function.

        :returns:
            A new RDD with mapped values.

        """
        return MapPartitionsRDD(
            self,
            lambda tc, i, x: ((e[0], f(e[1])) for e in x),
            preservesPartitioning=True,
        )

    def max(self):
        """
        :returns:
            The maximum element.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3, 4, 3, 2], 2).max() == 4
        True

        """
        return self.stats().max()

    def mean(self):
        """
        :returns:
            The mean of this dataset.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([0, 4, 7, 4, 10]).mean()
        5.0

        """
        return self.stats().mean()

    def min(self):
        """
        :returns:
            The minimum element.

        """
        return self.stats().min()

    def name(self):
        """
        :returns:
            The name of the dataset.

        """
        return self._name

    def persist(self, storageLevel=None):
        """
        Cache the results of computed partitions.

        :param storageLevel:
            Not used.

        """
        return PersistedRDD(self, storageLevel=storageLevel)

    def pipe(self, command, env={}):
        """
        Run a command with the elements in the dataset as argument.

        :param command:
            Command line command to run.

        :param env:
            ``dict`` of environment variables.

        .. warning::
            Unsafe for untrusted data.


        Example:

        >>> from pysparkling import Context
        >>> piped = Context().parallelize(['0', 'hello', 'world']).pipe('echo')
        >>> b'hello\\n' in piped.collect()
        True

        """
        return self.context.parallelize(subprocess.check_output(
            [command]+x if isinstance(x, list) else [command, x]
        ) for x in self.collect())

    def randomSplit(self, weights, seed=None):
        """
        Split the RDD into a few RDDs according to the given weights.

        .. note::
            Creating the new RDDs is currently implemented as a local
            operation.

        :param weights:
            Determines the relative lengths of the resulting RDDs.

        :param seed:
            Seed for random number generator.

        :returns:
            A list of RDDs.


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
            boundaries.append(boundaries[-1] + w/sum_weights)
        random.seed(seed)

        lists = [[] for _ in weights]
        for e in self.toLocalIterator():
            r = random.random()
            for i, lbub in enumerate(zip(boundaries[:-1], boundaries[1:])):
                if r >= lbub[0] and r < lbub[1]:
                    lists[i].append(e)

        return [self.context.parallelize(l) for l in lists]

    def reduce(self, f):
        """
        :param f:
            A commutative and associative binary operator.


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
        """
        :param f:
            A commutative and associative binary operator.

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
        """
        :param numPartitions:
            Number of partitions in new RDD.

        :returns:
            A new RDD.

        .. note::
            Creating the new RDD is currently implemented as a local operation.

        """
        return self.context.parallelize(self.toLocalIterator(), numPartitions)

    def rightOuterJoin(self, other, numPartitions=None):
        """
        :param other:
            The other RDD.

        :param numPartitions: (optional)
            Number of partitions of the resulting RDD.

        :returns:
            A new RDD with the result of the join.

        .. note::
            Creating the new RDD is currently implemented as a local operation.


        Example:

        >>> from pysparkling import Context
        >>> rdd1 = Context().parallelize([(0, 1), (1, 1)])
        >>> rdd2 = Context().parallelize([(2, 1), (1, 3)])
        >>> sorted(rdd1.rightOuterJoin(rdd2).collect())
        [(1, (1, 3)), (2, (None, 1))]

        """
        d1 = dict(self.collect())
        d2 = dict(other.collect())
        return self.context.parallelize((
            (k, (d1[k] if k in d1 else None, d2[k]))
            for k in d2.keys()
        ), numPartitions)

    def sample(self, withReplacement, fraction, seed=None):
        """
        :param withReplacement:
            Not used.

        :param fraction:
            Specifies the probability that an element is sampled.

        :param seed: (optional)
            Seed for random number generator.

        :returns:
            Sampled RDD.


        Example:

        >>> from pysparkling import Context
        >>> rdd = Context().parallelize(range(100))
        >>> sampled = rdd.sample(False, 0.1, seed=5)
        >>> all(s1 == s2 for s1, s2 in zip(sampled.collect(),
        ...                                sampled.collect()))
        True

        """
        return PartitionwiseSampledRDD(
            self, fraction,
            preservesPartitioning=True,
            seed=seed,
        )

    def sampleStdev(self):
        """
        :returns:
            sample standard deviation


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).sampleStdev()
        1.0

        """
        return self.stats().sampleStdev()

    def sampleVariance(self):
        """
        :returns:
            sample variance


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1, 2, 3]).sampleVariance()
        1.0

        """
        return self.stats().sampleVariance()

    def saveAsPickleFile(self, path, batchSize=10):
        """
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

        if File(path).exists():
            raise FileAlreadyExistsException(
                'Output {0} already exists.'.format(path)
            )

        codec_suffix = ''
        if path.endswith(('.gz', '.bz2', '.lzo')):
            codec_suffix = path[path.rfind('.'):]

        def _map(path, obj):
            stream = io.BytesIO()
            pickle.dump(self.collect(), stream)
            stream.seek(0)
            File(path).dump(stream)

        if self.getNumPartitions() == 1:
            _map(path, self.collect())
            return self

        self.context.runJob(
            self,
            lambda tc, x: _map(
                path+'/part-{0:05d}{1}'.format(tc.partitionId(), codec_suffix),
                list(x),
            ),
            resultHandler=lambda l: list(l),
        )
        TextFile(path+'/_SUCCESS').dump()
        return self

    def saveAsTextFile(self, path, compressionCodecClass=None):
        """
        If the RDD has many partitions, the contents will be stored directly
        in the given path. If the RDD has more partitions, the data of the
        partitions are stored in individual files under ``path/part-00000`` and
        so on and once all partitions are written, the file ``path/_SUCCESS``
        is written last.

        :param path:
            Destination of the text file.

        :param compressionCodecClass:
            Not used.

        :returns:
            ``self``

        """
        if TextFile(path).exists():
            raise FileAlreadyExistsException(
                'Output {0} already exists.'.format(path)
            )

        codec_suffix = ''
        if path.endswith(('.gz', '.bz2', '.lzo')):
            codec_suffix = path[path.rfind('.'):]

        if self.getNumPartitions() == 1:
            TextFile(
                path
            ).dump(io.StringIO(''.join([
                str(xx)+'\n' for xx in self.toLocalIterator()
            ])))
            return self

        self.context.runJob(
            self,
            lambda tc, x: TextFile(
                path+'/part-{0:05d}{1}'.format(tc.partitionId(), codec_suffix)
            ).dump(io.StringIO(''.join([
                str(xx)+'\n' for xx in x
            ]))),
            resultHandler=lambda l: list(l),
        )
        TextFile(path+'/_SUCCESS').dump()
        return self

    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        """
        :param keyfunc:
            Returns the value that will be sorted.

        :param ascending:
            Default is True.

        :param numPartitions:
            Default is None. None means the output will have the same number of
            partitions as the input.

        :returns:
            A new sorted RDD.

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
                  keyfunc=lambda x: x[0]):
        """
        :param ascending:
            Default is True.

        :param numPartitions:
            Default is None. None means the output will have the same number of
            partitions as the input.

        :param keyfunc:
            Returns the value that will be sorted.

        :returns:
            A new sorted RDD.

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
        """
        :returns:
            A :class:`pysparkling.StatCounter` instance.


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
        """
        :returns:
            standard deviation


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1.5, 2.5]).stdev()
        0.5

        """
        return self.stats().stdev()

    def subtract(self, other, numPartitions=None):
        """
        :param other:
            The RDD to be subtracted from the current RDD.

        :param numPartitions:
            Currently not used. Partitions are preserved.

        :returns:
            New RDD.


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
        """
        :returns:
            The sum of all the elements.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([0, 4, 7, 4, 10]).sum()
        25

        """
        return self.context.runJob(self, lambda tc, x: sum(x),
                                   resultHandler=sum)

    def take(self, n):
        """
        Only evaluates the partitions that are necessary to return n elements.

        :param n:
            Number of elements to return.

        :returns:
            Elements of the dataset in a list.


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
        """
        Assumes samples are evenly distributed between partitions.

        Only evaluates the partitions that are necessary to return n elements.

        :param n:
            The number of elements to sample.

        :returns:
            Samples from the dataset.


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

    def toLocalIterator(self):
        """
        :returns:
            An iterator over the dataset.


        Example:

        >>> from pysparkling import Context
        >>> sum(Context().parallelize([4, 9, 7, 3, 2, 5], 3).toLocalIterator())
        30

        """
        return self.context.runJob(
            self, lambda tc, i: list(i),
            resultHandler=lambda l: (x for p in l for x in p),
        )

    def union(self, other):
        """
        :param other:
            The other RDD for the union.

        :returns:
            A new RDD.


        Example:

        >>> from pysparkling import Context
        >>> my_rdd = Context().parallelize([4, 9, 7, 3, 2, 5], 3)
        >>> my_rdd.union(my_rdd).count()
        12

        """
        return self.context.union((self, other))

    def values(self):
        """
        :returns:
            Values of a (key, value) dataset.

        """
        return self.map(lambda e: e[1])

    def variance(self):
        """
        :returns:
            The variance of the dataset.


        Example:

        >>> from pysparkling import Context
        >>> Context().parallelize([1.5, 2.5]).variance()
        0.25

        """
        return self.stats().variance()

    def zip(self, other):
        """
        :param other:
            Other dataset to zip with.

        :returns:
            New RDD with zipped entries.

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

    def zipWithUniqueId(self):
        """
        This is a fast operation.

        :returns:
            New RDD where every entry is zipped with a unique index.


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
                (xx, e*num_p+tc.partition_id) for e, xx in enumerate(x)
            ),
            preservesPartitioning=True,
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
        RDD.__init__(self, prev.partitions(), prev.context)
        self.prev = prev
        self.storageLevel = storageLevel

    def compute(self, split, task_context):
        if self._rdd_id is None or split.index is None:
            cid = None
        else:
            cid = '{0}:{1}'.format(self._rdd_id, split.index)

        cm = CacheManager.singleton()
        if not cm.has(cid):
            cm.add(
                cid,
                list(self.prev.compute(split, task_context._create_child())),
                self.storageLevel
            )

        return iter(cm.get(cid))
