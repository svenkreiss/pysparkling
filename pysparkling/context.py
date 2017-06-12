"""Context."""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from collections import defaultdict
import itertools
import logging
import pickle
import struct
import time
import traceback

from . import __version__ as PYSPARKLING_VERSION
from .broadcast import Broadcast
from .cache_manager import CacheManager
from .fileio import File, TextFile
from .partition import Partition
from .rdd import RDD, EmptyRDD
from .task_context import TaskContext

log = logging.getLogger(__name__)


def unit_fn(arg):
    """Used as dummy serializer and deserializer."""
    return arg


def _run_task(task_context, rdd, func, partition):
    """Run a task, aka compute a partition.

    :param TaskContext task_context: this task context
    :param RDD rdd: rdd this partition is a part of
    :param func: a function
    :param Partition partition: partition to process
    """
    task_context.attempt_number += 1

    log.debug('Running stage {} for partition {} of {}.'
              ''.format(task_context.stage_id,
                        task_context.partition_id,
                        rdd.name()))

    try:
        return func(task_context, rdd.compute(partition, task_context))
    except Exception:
        log.warn('Attempt {} failed for partition {} of {}.'
                 ''.format(task_context.attempt_number, partition.index,
                           rdd.name()))
        traceback.print_exc()

    if task_context.attempt_number == task_context.max_retries:
        log.error('Partition {} of {} failed.'
                  ''.format(partition.index, rdd.name()))
        return []

    if task_context.retry_wait:
        time.sleep(task_context.retry_wait)
    return _run_task(task_context, rdd, func, partition)


def runJob_map(i):
    (deserializer, data_serializer, data_deserializer,
     serialized_func_rdd, serialized_task_context,
     serialized_data, cache_manager) = i

    t_start = time.clock()
    if cache_manager:
        if not CacheManager.singleton__:
            CacheManager.singleton__ = data_deserializer(cache_manager)
        else:
            CacheManager.singleton().join(
                data_deserializer(cache_manager).cache_obj
            )
    cm_state = CacheManager.singleton().stored_idents()
    t_cache_init = time.clock() - t_start

    t_start = time.clock()
    func, rdd = deserializer(serialized_func_rdd)
    t_deserialize_func = time.clock() - t_start

    t_start = time.clock()
    partition = data_deserializer(serialized_data)
    t_deserialize_data = time.clock() - t_start

    t_start = time.clock()
    task_context = deserializer(serialized_task_context)
    t_deserialize_task_context = time.clock() - t_start

    t_start = time.clock()
    result = _run_task(task_context, rdd, func, partition)
    t_exec = time.clock() - t_start

    return data_serializer((
        result,
        CacheManager.singleton().get_not_in(cm_state),
        {
            'map_cache_init': t_cache_init,
            'map_deserialize_func': t_deserialize_func,
            'map_deserialize_task_context': t_deserialize_task_context,
            'map_deserialize_data': t_deserialize_data,
            'map_exec': t_exec,
        }
    ))


class Context(object):
    """Context object similar to a Spark Context.

    The variable `_stats` contains measured timing information about data and
    function (de)serialization and workload execution to benchmark your jobs.

    :param pool: An instance with a ``map(func, iterable)`` method.
    :param serializer:
        Serializer for functions. Examples are ``pickle.dumps`` and
        ``dill.dumps``.
    :param deserializer:
        Deserializer for functions. Examples are ``pickle.loads`` and
        ``dill.loads``.
    :param data_serializer: Serializer for the data.
    :param data_deserializer: Deserializer for the data.
    :param int max_retries: maximum number a partition is retried
    :param float retry_wait: seconds to wait between retries
    """

    __last_rdd_id = 0

    def __init__(self, pool=None, serializer=None, deserializer=None,
                 data_serializer=None, data_deserializer=None,
                 max_retries=3, retry_wait=0.0):
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
        self.max_retries = max_retries
        self.retry_wait = retry_wait

        self._pool = pool
        self._serializer = serializer
        self._deserializer = deserializer
        self._data_serializer = data_serializer
        self._data_deserializer = data_deserializer
        self._s3_conn = None
        self._stats = defaultdict(float)

        self.version = PYSPARKLING_VERSION

    def broadcast(self, x):
        return Broadcast(x)

    def newRddId(self):
        Context.__last_rdd_id += 1
        return Context.__last_rdd_id

    def parallelize(self, x, numPartitions=None):
        """Parallelize x.

        :param x:
            An iterable (e.g. a list) that represents the data.

        :param int|None numPartitions: (optional)
            The number of partitions the data should be split into.
            A partition is a unit of data that is processed at a time.
            Can be ``None``.

        :rtype: RDD
        """
        if numPartitions is None or numPartitions <= 1:
            return RDD([Partition(x, 0)], self)

        x_len_iter, x = itertools.tee(x, 2)
        len_x = sum(1 for _ in x_len_iter)

        def partitioned():
            for i in range(numPartitions):
                start = int(i * len_x / numPartitions)
                end = int((i + 1) * len_x / numPartitions)
                if i + 1 == numPartitions:
                    end += 1
                yield itertools.islice(x, end - start)

        return self._parallelize_partitions(partitioned())

    def _parallelize_partitions(self, partitions):
        """Helper to parallelize partitions.

        :param partitions: An iterable over the partitioned data.
        :rtype: RDD
        """

        return RDD(
            (Partition(p_data, i) for i, p_data in enumerate(partitions)),
            self,
        )

    def pickleFile(self, name, minPartitions=None):
        """Read a pickle file.

        Reads files created with :func:`RDD.saveAsPickleFile()` into an RDD.

        :param name:
            Location of a file. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :rtype: RDD


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
        """This function is used by methods in the RDD.

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

        # this is the place to insert proper schedulers
        if allowLocal or isinstance(self._pool, DummyPool):
            map_result = self._runJob_local(rdd, func, partitions)
        else:
            map_result = self._runJob_distributed(rdd, func, partitions)

        if resultHandler is not None:
            return resultHandler(map_result)

        return list(map_result)  # convert to list to execute on all partitions

    def _runJob_local(self, rdd, func, partitions):
        for partition in partitions:
            task_context = TaskContext(
                stage_id=0,
                partition_id=partition.index,
                max_retries=self.max_retries,
                retry_wait=self.retry_wait,
            )
            yield _run_task(task_context, rdd, func, partition)

    def _runJob_distributed(self, rdd, func, partitions):
        cm = CacheManager.singleton()
        serialized_func_rdd = self._serializer((func, rdd))

        def prepare(partition):
            t_start = time.clock()
            cm_clone = cm.clone_contains(lambda i: i[1] == partition.index)
            self._stats['driver_cache_clone'] += (time.clock() -
                                                  t_start)

            t_start = time.clock()
            cm_serialized = self._data_deserializer(cm_clone)
            self._stats['driver_cache_serialize'] += (time.clock() -
                                                      t_start)

            t_start = time.clock()
            task_context = TaskContext(
                stage_id=0,
                partition_id=partition.index,
                max_retries=self.max_retries,
                retry_wait=self.retry_wait,
            )
            serialized_task_context = self._serializer(task_context)
            self._stats['driver_serialize_task_context'] += (time.clock() -
                                                             t_start)

            t_start = time.clock()
            serialized_partition = self._data_deserializer(partition)
            self._stats['driver_serialize_data'] += (time.clock() -
                                                     t_start)

            return (
                self._deserializer,
                self._data_serializer,
                self._data_deserializer,
                serialized_func_rdd,
                serialized_task_context,
                serialized_partition,
                cm_serialized,
            )

        for d in self._pool.map(runJob_map,
                                (prepare(p) for p in partitions)):
            t_start = time.clock()
            map_result, cache_result, s = self._data_deserializer(d)
            self._stats['driver_deserialize_data'] += (time.clock() -
                                                       t_start)

            # join cache
            t_start = time.clock()
            cm.join(cache_result)
            self._stats['driver_cache_join'] += time.clock() - t_start

            # collect stats
            for k, v in s.items():
                self._stats[k] += v

            yield map_result

    def binaryFiles(self, path, minPartitions=None):
        """Read a binary file into an RDD.

        :param path:
            Location of a file. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :rtype: RDD

        .. warning::
            Not part of PySpark API.


        Setting up examples:

        >>> import os, pysparkling
        >>> from backports import tempfile
        >>> sc = pysparkling.Context()
        >>> decode = lambda bstring: bstring.decode()


        Example with whole file:

        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     with open(os.path.join(tmp, 'test.b'), 'wb') as f:
        ...         _ = f.write(b'bellobello')
        ...     sc.binaryFiles(tmp+'*').mapValues(decode).collect()
        [('...', 'bellobello')]
        """
        resolved_names = File.resolve_filenames(path)
        log.debug('binaryFile() resolved "{0}" to {1} files.'
                  ''.format(path, len(resolved_names)))

        num_partitions = len(resolved_names)
        if minPartitions and minPartitions > num_partitions:
            num_partitions = minPartitions

        rdd_filenames = self.parallelize(resolved_names, num_partitions)
        rdd = rdd_filenames.map(lambda f_name:
                                (f_name, File(f_name).load().read()))
        rdd._name = path
        return rdd

    def binaryRecords(self, path, recordLength=None):
        """Read a binary file into an RDD.

        :param path:
            Location of a file. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param recordLength:
            If ``None`` every file is a record, ``int`` means fixed length
            records and a ``string`` is used as a format string to ``struct``
            to read the length of variable length binary records.

        :rtype: RDD

        .. warning::
            Only an ``int`` recordLength is part of the PySpark API.


        Setting up examples:

        >>> import os, pysparkling
        >>> from backports import tempfile
        >>> sc = pysparkling.Context()
        >>> decode = lambda bstring: bstring.decode()


        Example with whole file:

        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     with open(os.path.join(tmp, 'test.b'), 'wb') as f:
        ...         _ = f.write(b'bellobello')
        ...     sc.binaryRecords(tmp+'*').map(decode).collect()
        ['bellobello']


        Example with fixed length records:

        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     with open(os.path.join(tmp, 'test.b'), 'wb') as f:
        ...         _ = f.write(b'bellobello')
        ...     sc.binaryRecords(tmp+'*', recordLength=5).map(decode).collect()
        ['bello', 'bello']


        Example with variable length records:

        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     with open(os.path.join(tmp, 'test.b'), 'wb') as f:
        ...         _ = f.write(struct.pack('<I', 5) + b'bello')
        ...         _ = f.write(struct.pack('<I', 10) + b'bellobello')
        ...     (sc.binaryRecords(tmp+'*', recordLength='<I')
        ...      .map(decode).collect())
        ['bello', 'bellobello']
        """

        rdd = self.binaryFiles(path).values()
        if recordLength is None:
            pass
        elif isinstance(recordLength, int):
            chunker = FixedLengthChunker(recordLength)
            rdd = rdd.flatMap(chunker)
        else:
            chunker = VariableLengthChunker(recordLength)
            rdd = rdd.flatMap(chunker)
        rdd._name = path
        return rdd

    def textFile(self, filename, minPartitions=None, use_unicode=True):
        """Read a text file into an RDD.

        :param filename:
            Location of a file. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :param use_unicode: (optional, default=True)
            Use ``utf8`` if ``True`` and ``ascii`` if ``False``.

        :rtype: RDD
        """
        resolved_names = TextFile.resolve_filenames(filename)
        log.debug('textFile() resolved "{0}" to {1} files.'
                  ''.format(filename, len(resolved_names)))

        num_partitions = len(resolved_names)
        if minPartitions and minPartitions > num_partitions:
            num_partitions = minPartitions

        encoding = 'utf8' if use_unicode else 'ascii'

        rdd_filenames = self.parallelize(resolved_names, num_partitions)
        rdd = rdd_filenames.flatMap(
            lambda f_name:
            TextFile(f_name).load(encoding=encoding).read().splitlines()
        )
        rdd._name = filename
        return rdd

    def union(self, rdds):
        """Create a union of rdds.

        :param rdds: Iterable of RDDs.
        :rtype: RDD
        """
        if all(isinstance(rdd, EmptyRDD) for rdd in rdds):
            return EmptyRDD(self)

        return self.parallelize(
            (x for rdd in rdds for x in rdd.collect())
        )

    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        """Read text files into an RDD of pairs of file name and file content.

        :param path:
            Location of the files. Can include schemes like ``http://``,
            ``s3://`` and ``file://``, wildcard characters ``?`` and ``*``
            and multiple expressions separated by ``,``.

        :param minPartitions: (optional)
            By default, every file is a partition, but this option allows to
            split these further.

        :param use_unicode: (optional, default=True)
            Use ``utf8`` if ``True`` and ``ascii`` if ``False``.

        :rtype: RDD
        """
        resolved_names = TextFile.resolve_filenames(path)
        log.debug('wholeTextFiles() resolved "{0}" to {1} files.'
                  ''.format(path, len(resolved_names)))

        num_partitions = len(resolved_names)
        if minPartitions and minPartitions > num_partitions:
            num_partitions = minPartitions

        encoding = 'utf8' if use_unicode else 'ascii'
        rdd_filenames = self.parallelize(
            [(f_name, encoding) for f_name in resolved_names],
            num_partitions,
        )
        rdd = rdd_filenames.map(map_whole_text_file)
        rdd._name = path
        return rdd


class DummyPool(object):
    def __init__(self):
        pass

    def map(self, f, input_list):
        return (f(x) for x in input_list)

    def close(self):
        pass

    def shutdown(self):
        pass


# pickle-able helpers

def map_whole_text_file(f_name__encoding):
    f_name, encoding = f_name__encoding
    return (
        f_name,
        TextFile(f_name).load(encoding=encoding).read(),
    )


class FixedLengthChunker(object):
    def __init__(self, recordLength):
        self.record_length = recordLength

    def __call__(self, data):
        for i in range(0, len(data), self.record_length):
            yield data[i: i + self.record_length]


class VariableLengthChunker(object):
    def __init__(self, recordLength):
        self.length_fmt = recordLength
        self.prefix_length = struct.calcsize(recordLength)

    def __call__(self, data):
        while data:
            prefix, data = data[:self.prefix_length], data[self.prefix_length:]
            length = struct.unpack(self.length_fmt, prefix)[0]
            package, data = data[:length], data[length:]
            yield package
