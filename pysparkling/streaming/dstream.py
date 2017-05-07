from __future__ import print_function

import logging
import operator

from ..rdd import EmptyRDD

log = logging.getLogger(__name__)


class DStream(object):
    """A discrete stream of RDDs.

    Usually a DStream is created by a
    :class:`pysparkling.streaming.StreamingContext` method like
    :func:`pysparkling.streaming.StreamingContext.queueStream` and then
    operated on with the methods below.

    :param jdstream: previous stream
    :param StreamingContext ssc: the streaming context
    :param jrdd_deserializer: a deserializer callable
    """

    def __init__(self, jdstream, ssc, jrdd_deserializer=None):
        self._stream = jdstream
        self._context = ssc
        self._jrdd_deserializer = jrdd_deserializer

        self._current_time = 0.0
        self._current_rdd = None

        ssc._add_dstream(self)

    def _step(self, time_):
        if time_ <= self._current_time:
            return
        self._current_time = time_
        if self._jrdd_deserializer is None:
            self._current_rdd = self._stream.get()
        else:
            self._current_rdd = self._jrdd_deserializer(self._stream.get())

    def cache(self):
        """Cache RDDs.

        :rtype: DStream
        """
        return self.transform(lambda rdd: rdd.cache())

    def cogroup(self, other, numPartitions=None):
        """Apply cogroup to RDDs of this and other DStream.

        :param DStream other: another DStream
        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> s1 = ssc.queueStream([[('a', 4), ('b', 2)], [('c', 7)]])
        >>> s2 = ssc.queueStream([[('a', 1), ('b', 3)], [('c', 8)]])
        >>> s1.cogroup(s2).foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        [('a', [[4], [1]]), ('b', [[2], [3]])]
        [('c', [[7], [8]])]
        """
        return CogroupedDStream(self, other, numPartitions)

    def context(self):
        """Return the StreamContext of this stream.

        :rtype: StreamingContext
        """
        return self._context

    def count(self):
        """Count elements per RDD.

        Creates a new RDD stream where each RDD has a single entry that
        is the count of the elements.

        :rtype: DStream
        """
        return (
            self
            .mapPartitions(lambda p: [sum(1 for _ in p)])
            .reduce(operator.add)
        )

    def countByValue(self):
        """Apply countByValue to every RDD.abs

        :rtype: DStream

        .. warning::
            Implemented as a local operation.


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[1, 1, 5, 5, 5, 2]])
        ...     .countByValue()
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.15)
        [(1, 2), (2, 1), (5, 3)]
        """
        return self.transform(
            lambda rdd: self._context._context.parallelize(
                rdd.countByValue().items()))

    def countByWindow(self, windowDuration, slideDuration=None):
        """Applies count() after window().

        :param float windowDuration: multiple of batch interval
        :param float slideDuration: multiple of batch interval
        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[1, 1, 5], [5, 5, 2, 4], [1, 2]])
        ...     .countByWindow(0.2)
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.35)
        [3]
        [7]
        [6]
        """
        return self.window(windowDuration, slideDuration).count()

    def filter(self, f):
        """Filter elements.

        :param f: filter function
        :rtype: DStream
        """
        return self.transform(lambda rdd: rdd.filter(f))

    def flatMap(self, f, preservesPartitioning=False):
        """Apply function f and flatten.

        :param f: mapping function
        :rtype: DStream
        """
        return self.mapPartitions(
            lambda p: (e for pp in p for e in f(pp)),
            preservesPartitioning,
        )

    def flatMapValues(self, f):
        """Apply f to each value of a key-value pair.

        :param f: map function
        :rtype: DStream
        """
        return self.transform(lambda rdd: rdd.flatMapValues(f))

    def foreachRDD(self, func):
        """Apply func.

        :param func: Function to apply.
        """
        self.transform(func)

    def fullOuterJoin(self, other, numPartitions=None):
        """Apply fullOuterJoin to each pair of RDDs.

        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> s1 = ssc.queueStream([[('a', 4), ('b', 2)], [('c', 7)]])
        >>> s2 = ssc.queueStream([[('a', 1), ('b', 3)], [('c', 8)]])
        >>> (
        ...     s1.fullOuterJoin(s2)
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        [('a', (4, 1)), ('b', (2, 3))]
        [('c', (7, 8))]


        Example with repeated keys:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> s1 = ssc.queueStream([[('a', 4), ('a', 2)], [('c', 7)]])
        >>> s2 = ssc.queueStream([[('b', 1)], [('c', 8)]])
        >>> (
        ...     s1.fullOuterJoin(s2)
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        [('a', (2, None)), ('a', (4, None)), ('b', (None, 1))]
        [('c', (7, 8))]
        """
        return CogroupedDStream(self, other, numPartitions, op='fullOuterJoin')

    def groupByKey(self):
        """group by key

        :rtype: DStream
        """
        return self.transform(lambda rdd: rdd.groupByKey())

    def join(self, other, numPartitions=None):
        """Apply join to each pair of RDDs.

        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> s1 = ssc.queueStream([[('a', 4), ('e', 2)], [('c', 7)]])
        >>> s2 = ssc.queueStream([[('a', 1), ('b', 3)], [('c', 8)]])
        >>> (
        ...     s1.join(s2)
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        [('a', (4, 1))]
        [('c', (7, 8))]
        """
        return CogroupedDStream(self, other, numPartitions, op='join')

    def leftOuterJoin(self, other, numPartitions=None):
        """Apply leftOuterJoin to each pair of RDDs.

        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> s1 = ssc.queueStream([[('a', 4), ('e', 2)], [('c', 7)]])
        >>> s2 = ssc.queueStream([[('a', 1), ('b', 3)], [('c', 8)]])
        >>> (
        ...     s1.leftOuterJoin(s2)
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        [('a', (4, 1)), ('e', (2, None))]
        [('c', (7, 8))]
        """
        return CogroupedDStream(self, other, numPartitions, op='leftOuterJoin')

    def map(self, f, preservesPartitioning=False):
        """Apply function f

        :param f: mapping function
        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[4], [2], [7]])
        ...     .map(lambda e: e + 1)
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.35)
        [5]
        [3]
        [8]
        """
        return self.mapPartitions(
            lambda p: (f(e) for e in p),
            preservesPartitioning,
        )

    def mapPartitions(self, f, preservesPartitioning=False):
        """Map partitions.

        :param f: mapping function
        :rtype: DStream
        """
        return self.mapPartitionsWithIndex(lambda i, p: f(p),
                                           preservesPartitioning)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """Apply a map function that takes an index and the data.

        Map partitions with a function that takes the partition index
        and an iterator over the partition data as arguments.

        :param f: mapping function
        :rtype: DStream
        """
        return self.transform(
            lambda rdd: rdd.mapPartitionsWithIndex(f, preservesPartitioning)
        )

    def mapValues(self, f):
        """Apply ``f`` to every element.

        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[('a', 4)], [('b', 2)], [('c', 7)]])
        ...     .mapValues(lambda e: e + 1)
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.35)
        [('a', 5)]
        [('b', 3)]
        [('c', 8)]
        """
        return self.transform(lambda rdd: rdd.mapValues(f))

    def pprint(self, num=10):
        """Print the first ``num`` elements of each RDD.

        :param int num: Set number of elements to be printed.
        """

        def pprint_map(time_, rdd):
            print('>>> Time: {}'.format(time_))
            data = rdd.take(num + 1)
            for d in data[:num]:
                print(d)
            if len(data) > num:
                print('...')
            print('')

        self.foreachRDD(pprint_map)

    def reduce(self, func):
        """Return a new DStream where each RDD was reduced with ``func``.

        :rtype: DStream
        """

        # avoid RDD.reduce() which does not return an RDD
        return self.transform(
            lambda rdd: (
                rdd
                .map(lambda i: (None, i))
                .reduceByKey(func)
                .map(lambda none_i: none_i[1])
            )
        )

    def reduceByKey(self, func, numPartitions=None):
        """Apply reduceByKey to every RDD.

        :param func: reduce function to apply
        :param int numPartitions: number of partitions
        :rtype: DStream
        """
        return self.transform(lambda rdd: rdd.reduceByKey(func))

    def repartition(self, numPartitions):
        """Repartition every RDD.

        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([['hello', 'world']])
        ...     .repartition(2)
        ...     .foreachRDD(lambda rdd: print(len(rdd.partitions())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        2
        0

        """
        return self.transform(
            lambda rdd: (rdd.repartition(numPartitions)
                         if not isinstance(rdd, EmptyRDD) else rdd)
        )

    def rightOuterJoin(self, other, numPartitions=None):
        """Apply rightOuterJoin to each pair of RDDs.

        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> s1 = ssc.queueStream([[('a', 4), ('e', 2)], [('c', 7)]])
        >>> s2 = ssc.queueStream([[('a', 1), ('b', 3)], [('c', 8)]])
        >>> (
        ...     s1.rightOuterJoin(s2)
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.25)
        [('a', (4, 1)), ('b', (None, 3))]
        [('c', (7, 8))]
        """
        return CogroupedDStream(self, other, numPartitions,
                                op='rightOuterJoin')

    def saveAsTextFiles(self, prefix, suffix=None):
        """Save every RDD as a text file (or sets of text files).

        :param string prefix: path prefix of the output
        :param string suffix: file suffix (e.g. '.gz' to enable compression)


        Example:

        >>> from backports import tempfile
        >>> import os, pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> with tempfile.TemporaryDirectory() as tmp_dir:
        ...     (
        ...         ssc.queueStream([['hello', 'world'], [1, 2]])
        ...         .saveAsTextFiles(os.path.join(tmp_dir, 'textout'))
        ...     )
        ...     ssc.start()
        ...     ssc.awaitTermination(0.25)
        ...     result = sc.textFile(tmp_dir + '*').collect()
        >>> result
        ['hello', 'world', '1', '2']
        """
        self.foreachRDD(
            lambda time_, rdd:
            rdd.saveAsTextFile('{}-{:.0f}{}'.format(
                prefix, time_ * 1000, suffix if suffix is not None else ''))
        )

    def slice(self, begin, end):
        """Filter RDDs to between begin and end.

        :param datetime.datetime|int begin: datetiem or unix timestamp
        :param datetime.datetime|int end: datetiem or unix timestamp
        :rtype: DStream
        """
        return self.transform(lambda time_, rdd:
                              rdd if begin <= time_ <= end
                              else EmptyRDD(self._context._context))

    def transform(self, func):
        """Return a new DStream where each RDD is transformed by ``f``.

        :param f: Function that transforms an RDD.
        :rtype: DStream
        """
        if func.__code__.co_argcount == 1:
            one_arg_func = func
            func = lambda _, rdd: one_arg_func(rdd)

        return TransformedDStream(self, func)

    def transformWith(self, func, other, keepSerializer=False):
        """Return a new DStream where each RDD is transformed by ``f``.

        :param f: transformation function
        :rtype: DStream

        The transformation function can have arguments ``(time, rdd_a, rdd_b)``
        or ``(rdd_a, rdd_b)``.
        """
        if func.__code__.co_argcount == 2:
            two_arg_func = func
            func = lambda _, rdd_a, rdd_b: two_arg_func(rdd_a, rdd_b)

        return TransformedWithDStream(self, func, other)

    def union(self, other):
        """Union of two DStreams.

        :param DStream other: Another DStream.


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> odd = ssc.queueStream([[1], [3], [5]])
        >>> even = ssc.queueStream([[2], [4], [6]])
        >>> (
        ...     odd.union(even)
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.35)
        [1, 2]
        [3, 4]
        [5, 6]
        """
        def union_rdds(rdd_a, rdd_b):
            return self._context._context.union((rdd_a, rdd_b))

        return self.transformWith(union_rdds, other)

    def updateStateByKey(self, func):
        """Process with state.

        :param func: Evaluated per key. Takes list of input_values and a state.
        :rtype: DStream


        This example shows how to return the latest value per key:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.2)
        >>> (
        ...     ssc
        ...     .queueStream([[('a', 1), ('b', 3)], [('a', 2), ('c', 4)]])
        ...     .updateStateByKey(lambda input_values, state:
        ...                       state
        ...                       if not input_values
        ...                       else input_values[-1])
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.5)
        [('a', 1), ('b', 3)]
        [('a', 2), ('b', 3), ('c', 4)]


        This example counts values per key:

        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.2)
        >>> (
        ...     ssc
        ...     .queueStream([[('a', 1)], [('a', 2), ('b', 4), ('b', 3)]])
        ...     .updateStateByKey(lambda input_values, state:
        ...                       (state if state is not None else 0) +
        ...                       sum(input_values))
        ...     .foreachRDD(lambda rdd: print(sorted(rdd.collect())))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.5)
        [('a', 1)]
        [('a', 3), ('b', 7)]
        """
        return StatefulDStream(self, func)

    def window(self, windowDuration, slideDuration=None):
        """Windowed RDD.

        :param float windowDuration: multiple of batching interval
        :param float slideDuration: multiple of batching interval
        :rtype: DStream


        Example:

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.2)
        >>> (
        ...     ssc
        ...     .queueStream([[1], [2], [3], [4], [5], [6]])
        ...     .window(0.6)
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(1.3)
        [1]
        [1, 2]
        [1, 2, 3]
        [2, 3, 4]
        [3, 4, 5]
        [4, 5, 6]
        """
        return WindowedDStream(self, windowDuration, slideDuration)


class TransformedDStream(DStream):
    def __init__(self, prev, func):
        super(TransformedDStream, self).__init__(prev._stream, prev._context)
        self._prev = prev
        self._func = func

    def _step(self, time_):
        if time_ <= self._current_time:
            return

        self._prev._step(time_)
        self._current_time = time_
        self._current_rdd = self._func(time_, self._prev._current_rdd)


class TransformedWithDStream(DStream):
    def __init__(self, prev, func, other_prev):
        super(TransformedWithDStream, self).__init__(
            prev._stream, prev._context)
        self._prev = prev
        self._func = func
        self._other_prev = other_prev

    def _step(self, time_):
        if time_ <= self._current_time:
            return

        self._prev._step(time_)
        self._other_prev._step(time_)
        self._current_time = time_
        self._current_rdd = self._func(time_,
                                       self._prev._current_rdd,
                                       self._other_prev._current_rdd)


class WindowedDStream(DStream):
    def __init__(self, prev, windowDuration, slideDuration=None):
        super(WindowedDStream, self).__init__(prev._stream, prev._context)

        if slideDuration is None:
            slideDuration = self._context.batch_duration

        self._prev = prev
        self._window_duration = int(round(
            windowDuration / self._context.batch_duration))
        self._slide_duration = int(round(
            slideDuration / self._context.batch_duration))
        self._slide_counter = 0
        self._window = []

    def _step(self, time_):
        if time_ <= self._current_time:
            return

        self._prev._step(time_)
        self._window.append(self._prev._current_rdd)

        # window duration
        while len(self._window) > self._window_duration:
            self._window.pop(0)

        # slide duration
        self._slide_counter = (self._slide_counter + 1) % self._slide_duration
        if self._slide_counter != 0:
            return

        self._current_time = time_
        self._current_rdd = self._context._context.union(self._window)


class CogroupedDStream(DStream):
    def __init__(self, prev1, prev2, numPartitions=None, op='cogroup'):
        super(CogroupedDStream, self).__init__(prev1._stream, prev1._context)
        self._prev1 = prev1
        self._prev2 = prev2
        self._num_partitions = numPartitions
        self._op = op

    def _step(self, time_):
        if time_ <= self._current_time:
            return

        self._prev1._step(time_)
        self._prev2._step(time_)
        self._current_time = time_
        self._current_rdd = getattr(self._prev1._current_rdd, self._op)(
            self._prev2._current_rdd, self._num_partitions)


class StatefulDStream(DStream):
    def __init__(self, prev, state_update_fn):
        super(StatefulDStream, self).__init__(prev._stream, prev._context)
        self._prev = prev
        self._func = state_update_fn
        self._state_rdd = EmptyRDD(self._context._context)

    def convert_fn(self, joined):
        input_values, state_list = joined
        state = state_list[-1] if len(state_list) > 0 else None

        return self._func(input_values, state)

    def _step(self, time_):
        if time_ <= self._current_time:
            return

        self._prev._step(time_)
        self._current_time = time_

        combined = self._prev._current_rdd.cogroup(self._state_rdd)
        self._state_rdd = combined.mapValues(self.convert_fn)
        self._current_rdd = self._state_rdd
