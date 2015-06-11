"""
Provides a Python implementation of RDDs.

"""

from __future__ import (division, absolute_import, print_function,
                        unicode_literals)

import io
import sys
import copy
import random
import logging
import functools
import itertools
import subprocess
from collections import defaultdict

from . import utils
from .fileio import TextFile
from .stat_counter import StatCounter
from .partition import PersistedPartition
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

        """
        return self.context.parallelize(self.toLocalIterator(), numPartitions)

    def collect(self):
        """
        :returns:
            The entire dataset as a list.

        """
        return self.context.runJob(
            self, lambda tc, i: list(i),
            resultHandler=lambda l: [x for p in l for x in p],
        )

    def count(self):
        """
        :returns:
            Number of entries in this dataset.

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

        """
        def map_func(tc, x):
            r = defaultdict(int)
            for k, v in x:
                r[k] += v
            return r
        return self.context.runJob(self, map_func,
                                   resultHandler=utils.sum_counts_by_keys)

    def countByValue(self):
        """
        :returns:
            A ``dict`` containing the count for every value.

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

        """
        def map_func(tc, i, x):
            return (xx for xx in x if f(xx))
        return MapPartitionsRDD(self, map_func, preservesPartitioning=True)

    def first(self):
        """
        :returns:
            The first element in the dataset.

        """
        return self.context.runJob(
            self,
            lambda tc, i: i,
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

        """
        return self.aggregateByKey(zeroValue, op, op)

    def foreach(self, f):
        """
        Applies ``f`` to every element, but does not return a new RDD like
        :func:`RDD.map()`.

        :param f:
            Apply a function to every element.

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

        """
        return self.map(lambda e: (f(e), e))

    def keys(self):
        """
        :returns:
            A new RDD containing the keys of the current RDD.

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

        """
        return self.context.runJob(
            self,
            lambda tc, x: (xx[1] for xx in x if xx[0] == key),
            resultHandler=lambda l: [e for ll in l for e in ll],
        )

    def map(self, f):
        """
        :param f:
            Map function.

        :returns:
            A new RDD with mapped values.

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

        """
        return self.stats().max()

    def mean(self):
        """
        :returns:
            The mean of this dataset.

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

        """
        return self.context.parallelize(subprocess.check_output(
            [command]+x if isinstance(x, list) else [command, x]
        ) for x in self.collect())

    def reduce(self, f):
        """
        :param f:
            A commutative and associative binary operator.

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

        """
        return self.stats().sampleStdev()

    def sampleVariance(self):
        """
        :returns:
            sample variance

        """
        return self.stats().sampleVariance()

    def saveAsTextFile(self, path, compressionCodecClass=None):
        """
        :param path:
            Destination of the text file.

        :param compressionCodecClass:
            Not used.

        :returns:
            ``self``

        """
        if TextFile.exists(path):
            raise FileAlreadyExistsException(
                'Output {0} already exists.'.format(path)
            )

        codec_suffix = ''
        if path.endswith(('.gz', '.bz2', '.lzo')):
            codec_suffix = path[path.rfind('.'):]

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

    def stats(self):
        """
        :returns:
            A :class:`pysparkling.StatCounter` instance.

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

        """
        return self.context.parallelize(
            zip(self.toLocalIterator(), other.toLocalIterator())
        )

    def zipWithUniqueId(self):
        """
        This is a fast operation.

        :returns:
            New RDD where every entry is zipped with a unique index.

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
