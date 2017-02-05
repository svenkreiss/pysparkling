from __future__ import print_function

import logging
import operator

from ..rdd import EmptyRDD

log = logging.getLogger(__name__)


class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer=None):
        self._stream = jdstream
        self._context = ssc
        self._jrdd_deserializer = jrdd_deserializer

        self._current_time = 0.0
        self._current_rdd = None
        self._fn = None

        ssc._add_dstream(self)

    def _step(self, time_):
        if time_ <= self._current_time:
            return
        self._current_time = time_
        if self._jrdd_deserializer is None:
            self._current_rdd = self._stream.get()
        else:
            self._current_rdd = self._jrdd_deserializer(self._stream.get())

    def _apply(self, time_):
        if self._fn is None:
            return
        self._fn(time_, self._current_rdd)

    def context(self):
        """Return the StreamContext of this stream.

        :rtype: StreamContext
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

    def flatMap(self, f, preservesPartitioning=False):
        """Apply function f and flatten.

        :param f: mapping function
        :rtype: DStream
        """
        return self.mapPartitions(
            lambda p: (e for pp in p for e in f(pp)),
            preservesPartitioning,
        )

    def foreachRDD(self, func):
        """Apply func.

        :param func: Function to apply.
        """
        self.transform(func)

    def groupByKey(self):
        """group by key

        :rtype: DStream
        """
        return self.transform(lambda rdd: rdd.groupByKey())

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

    def saveAsTextFiles(self, prefix, suffix=None):
        """Save every RDD as a text file (or sets of text files).

        :param string prefix: path prefix of the output
        :param string suffix: file suffix (e.g. '.gz' to enable compression)
        """
        self.foreachRDD(
            lambda time_, rdd:
            rdd.saveAsTextFile('{}-{:.0f}{}'.format(
                prefix, time_ * 1000, suffix if suffix is not None else ''))
        )

    def transform(self, func):
        """Return a new DStream where each RDD is transformed by ``f``.

        :param f: Function that transforms an RDD.
        :rtype: DStream
        """
        if func.__code__.co_argcount == 1:
            one_arg_func = func
            func = lambda _, rdd: one_arg_func(rdd)

        return TransformedDStream(self, func)

    def window(self, windowDuration, slideDuration=None):
        """Windowed RDD.

        :param float windowDuration: multiple of batching interval
        :param float slideDuration: multiple of batching interval
        :rtype: DStream

        >>> import pysparkling
        >>> sc = pysparkling.Context()
        >>> ssc = pysparkling.streaming.StreamingContext(sc, 0.1)
        >>> (
        ...     ssc
        ...     .queueStream([[1], [2], [3], [4], [5], [6]])
        ...     .window(0.3)
        ...     .foreachRDD(lambda rdd: print(rdd.collect()))
        ... )
        >>> ssc.start()
        >>> ssc.awaitTermination(0.65)
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
