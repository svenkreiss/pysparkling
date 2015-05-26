"""Context."""

from __future__ import division, print_function

import math
import logging

from .rdd import RDD
from .broadcast import Broadcast
from .partition import Partition
from .task_context import TaskContext
from .fileio import File
from . import __version__ as PYSPARKLING_VERSION

log = logging.getLogger(__name__)


def unit_fn(arg):
    """Used as dummy serializer and deserializer."""
    return arg


def runJob_map(i):
    (deserializer, data_serializer, data_deserializer,
     serialized, serialized_data) = i
    func, rdd = deserializer(serialized)
    partition = data_deserializer(serialized_data)
    log.debug('Worker function {0} is about to get executed with {1}'
              ''.format(func, partition))

    task_context = TaskContext(stage_id=0, partition_id=partition.index)
    return data_serializer(
        func(task_context, rdd.compute(partition, task_context))
    )


class Context(object):
    def __init__(self, pool=None, serializer=None, deserializer=None,
                 data_serializer=None, data_deserializer=None):
        if not pool:
            pool = DummyPool()
        if not serializer:
            serializer = unit_fn
        if not deserializer:
            deserializer = unit_fn
        if not data_serializer:
            data_serializer = unit_fn
        if not data_deserializer:
            data_deserializer = unit_fn

        self._pool = pool
        self._serializer = serializer
        self._deserializer = deserializer
        self._data_serializer = data_serializer
        self._data_deserializer = data_deserializer
        self._s3_conn = None
        self._last_rdd_id = 0

        self.version = PYSPARKLING_VERSION

    def broadcast(self, x):
        return Broadcast(x)

    def newRddId(self):
        self._last_rdd_id += 1
        return self._last_rdd_id

    def parallelize(self, x, numPartitions=None):
        if not numPartitions:
            return RDD([Partition(x, 0)], self)

        stride_size = int(math.ceil(len(x)/numPartitions))

        def partitioned():
            for i in range(numPartitions):
                yield Partition(x[i*stride_size:(i+1)*stride_size], i)

        return RDD(partitioned(), self)

    def runJob(self, rdd, func, partitions=None, allowLocal=False,
               resultHandler=None):
        """func is of the form func(TaskContext, Iterator over elements).

        Note that the maps are only inside generators and the resultHandler
        needs to take care of executing the ones that it needs.
        """

        if not partitions:
            partitions = rdd.partitions()

        # TODO: this is the place to insert proper schedulers
        map_result = (
            self._data_deserializer(d) for d in self._pool.map(runJob_map, [
                (self._deserializer,
                 self._data_serializer,
                 self._data_deserializer,
                 self._serializer((func, rdd)),
                 self._data_serializer(p),
                 )
                for p in partitions
            ])
        )
        log.info('Map jobs generated.')

        if resultHandler is not None:
            return resultHandler(map_result)
        return list(map_result)  # convert to list to execute on all partitions

    def textFile(self, filename, minPartitions=None, use_unicode=True):
        resolved_names = File.resolve_filenames(filename)
        log.info('textFile() resolved "{0}" to {1} files.'
                 ''.format(filename, len(resolved_names)))
        rdd_filenames = self.parallelize(resolved_names)
        rdd = rdd_filenames.flatMap(lambda f_name: [
            l.rstrip('\n')
            for l in File(f_name).load().read().decode('utf-8').splitlines()
        ])
        rdd._name = filename
        return rdd

    def union(self, rdds):
        return self.parallelize(
            (x for rdd in rdds for x in rdd.collect())
        )


class DummyPool(object):
    def __init__(self):
        pass

    def map(self, f, input_list):
        return (f(x) for x in input_list)
