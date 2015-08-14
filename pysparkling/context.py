"""Context."""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pickle
import logging
import itertools

from .rdd import RDD
from .broadcast import Broadcast
from .partition import Partition
from .task_context import TaskContext
from .cache_manager import CacheManager
from .fileio import File, TextFile
from . import __version__ as PYSPARKLING_VERSION

log = logging.getLogger(__name__)


def unit_fn(arg):
    """Used as dummy serializer and deserializer."""
    return arg


def runJob_map(i):
    (deserializer, data_serializer, data_deserializer,
     serialized, serialized_data, cache_manager) = i

    if cache_manager:
        if not CacheManager.singleton__:
            CacheManager.singleton__ = data_deserializer(cache_manager)
        else:
            CacheManager.singleton().join(
                data_deserializer(cache_manager).cache_obj
            )
    cm_state = CacheManager.singleton().stored_idents()
    log.debug('Cache indices available in map: {0}'.format(cm_state))

    func, rdd = deserializer(serialized)
    partition = data_deserializer(serialized_data)
    log.debug('Worker function {0} is about to get executed with {1}'
              ''.format(func, partition))

    task_context = TaskContext(stage_id=0, partition_id=partition.index)
    result = func(task_context, rdd.compute(partition, task_context))

    return data_serializer((
        result,
        CacheManager.singleton().get_not_in(cm_state),
    ))


class Context(object):
    """
    :param pool:
        An instance with a ``map(func, iterable)`` method.

    :param serializer:
        Serializer for functions. Examples are ``pickle.dumps`` and
        ``dill.dumps``.

    :param deserializer:
        Deserializer for functions. Examples are ``pickle.loads`` and
        ``dill.loads``.

    :param data_serializer:
        Serializer for the data.

    :param data_deserializer:
        Deserializer for the data.

    """

    __last_rdd_id = 0

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

        self.version = PYSPARKLING_VERSION

    def broadcast(self, x):
        return Broadcast(x)

    def newRddId(self):
        Context.__last_rdd_id += 1
        return Context.__last_rdd_id

    def parallelize(self, x, numPartitions=None):
        """
        :param x:
            An iterable (e.g. a list) that represents the data.

        :param numPartitions: (optional)
            The number of partitions the data should be split into.
            A partition is a unit of data that is processed at a time.
            Can be ``None``.

        :returns:
            New RDD.

        """
        if not numPartitions:
            return RDD([Partition(x, 0)], self)

        x_len_iter, x = itertools.tee(x, 2)
        len_x = sum(1 for _ in x_len_iter)

        def partitioned():
            for i in range(numPartitions):
                start = int(i * len_x/numPartitions)
                end = int((i+1) * len_x/numPartitions)
                if i+1 == numPartitions:
                    end += 1
                yield Partition(itertools.islice(x, end-start), i)

        return RDD(partitioned(), self)

    def pickleFile(self, name, minPartitions=None):
        """
        Read a pickle file created with :func:`RDD.saveAsPickleFile()`
        into an RDD.

        :param name:
            Location of a file. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :returns:
            New RDD.


        Example with a serialized list:

        >>> import pickle
        >>> from pysparkling import Context
        >>> from tempfile import NamedTemporaryFile
        >>> tmpFile = NamedTemporaryFile(delete=True)
        >>> tmpFile.close()
        >>> with open(tmpFile.name, 'wb') as f:
        ...     pickle.dump(['hello', 'world'], f)
        >>> Context().pickleFile(tmpFile.name).collect()[0] == 'hello'
        True

        """
        resolved_names = File.resolve_filenames(name)
        log.debug('pickleFile() resolved "{0}" to {1} files.'
                  ''.format(name, len(resolved_names)))

        num_partitions = len(resolved_names)
        if minPartitions and minPartitions > num_partitions:
            num_partitions = minPartitions

        rdd_filenames = self.parallelize(resolved_names, num_partitions)
        rdd = rdd_filenames.flatMap(
            lambda f_name: pickle.load(File(f_name).load())
        )
        rdd._name = name
        return rdd

    def runJob(self, rdd, func, partitions=None, allowLocal=False,
               resultHandler=None):
        """
        This function is used by methods in the RDD.

        Note that the maps are only inside generators and the resultHandler
        needs to take care of executing the ones that it needs. In other words,
        if you need everything to be executed, the resultHandler needs to be
        at least ``lambda x: list(x)`` to trigger execution of the generators.

        :param func:
            Map function. The signature is
            func(TaskContext, Iterator over elements).

        :param partitions: (optional)
            List of partitions that are involved. Default is ``None``, meaning
            the map job is applied to all partitions.

        :param allowLocal: (optional)
            Allows for local execution. Default is False.

        :param resultHandler: (optional)
            Process the result from the maps.

        :returns:
            Result of resultHandler.

        """

        if not partitions:
            partitions = rdd.partitions()

        # TODO: this is the place to insert proper schedulers
        if allowLocal:
            def local_map(partition):
                task_context = TaskContext(
                    stage_id=0,
                    partition_id=partition.index,
                )
                return func(task_context, rdd.compute(partition, task_context))
            map_result = (local_map(p) for p in partitions)
        else:
            def map_and_cache():
                cm = CacheManager.singleton()
                for d in self._pool.map(runJob_map, [
                    (self._deserializer,
                     self._data_serializer,
                     self._data_deserializer,
                     self._serializer((func, rdd)),
                     self._data_serializer(p),
                     self._data_serializer(
                        cm.clone_contains(':{0}'.format(p.index))
                     ),
                     )
                    for p in partitions
                ]):
                    map_result, cache_result = self._data_deserializer(d)
                    cm.join(cache_result)
                    yield map_result
            map_result = map_and_cache()
        log.debug('Map jobs generated.')

        if resultHandler is not None:
            return resultHandler(map_result)
        return list(map_result)  # convert to list to execute on all partitions

    def textFile(self, filename, minPartitions=None, use_unicode=True):
        """
        Read a text file into an RDD.

        :param filename:
            Location of a file. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :param use_unicode: (optional)
            Not used.

        :returns:
            New RDD.

        """
        resolved_names = TextFile.resolve_filenames(filename)
        log.debug('textFile() resolved "{0}" to {1} files.'
                  ''.format(filename, len(resolved_names)))

        num_partitions = len(resolved_names)
        if minPartitions and minPartitions > num_partitions:
            num_partitions = minPartitions

        rdd_filenames = self.parallelize(resolved_names, num_partitions)
        rdd = rdd_filenames.flatMap(lambda f_name: [
            l.rstrip('\n')
            for l in TextFile(f_name).load().read().splitlines()
        ])
        rdd._name = filename
        return rdd

    def union(self, rdds):
        """
        :param rdds:
            Iterable of RDDs.

        :returns:
            New RDD.

        """
        return self.parallelize(
            (x for rdd in rdds for x in rdd.collect())
        )

    def version(self):
        """
        :returns:
            Version of pysparkling.

        """
        return self.version

    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        """
        Read text files into an RDD of pairs of file name and file content.

        :param path:
            Location of the files. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :param use_unicode: (optional)
            Not used.

        :returns:
            New RDD.

        """
        resolved_names = TextFile.resolve_filenames(path)
        log.debug('textFile() resolved "{0}" to {1} files.'
                  ''.format(path, len(resolved_names)))

        num_partitions = len(resolved_names)
        if minPartitions and minPartitions > num_partitions:
            num_partitions = minPartitions

        rdd_filenames = self.parallelize(resolved_names, num_partitions)
        rdd = rdd_filenames.map(lambda f_name: (
            f_name,
            TextFile(f_name).load().read(),
        ))
        rdd._name = path
        return rdd


class DummyPool(object):
    def __init__(self):
        pass

    def map(self, f, input_list):
        return (f(x) for x in input_list)
