import logging
import operator

log = logging.getLogger(__name__)


class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer=None):
        self._stream = jdstream
        self._context = ssc

        self._current_time = 0.0
        self._current_rdds = []
        self._fn = None

        ssc._add_dstream(self)

    def _step(self, time_):
        if time_ <= self._current_time:
            return
        self._current_time = time_
        self._current_rdds = self._stream.get()

    def _apply(self, time_):
        if self._fn is None:
            return
        self._fn(time_, self._current_rdds)

    def context(self):
        """Return the StreamContext of this stream."""
        return self._context

    def count(self):
        """Creates a new RDD stream where each RDD has a single entry that
        is the count of the elements.
        """
        return self.mapPartitions(
            lambda p: [sum(1 for _ in p)]
        ).reduce(
            operator.add
        )

    def flatMap(self, f, preservesPartitioning=False):
        """Apply function f and flatten.

        :param f:
            mapping function
        """
        return self.mapPartitions(
            lambda p: (e for pp in p for e in f(pp)),
            preservesPartitioning,
        )

    def foreachRDD(self, func):
        """Apply func.

        :param func:
            Function to apply.
        """
        self._fn = lambda time_, rdds: [func(time_, i) for i in rdds]

    def map(self, f, preservesPartitioning=False):
        """Apply function f

        :param f:
            mapping function
        """
        return self.mapPartitions(
            lambda p: (f(e) for e in p),
            preservesPartitioning,
        )

    def mapPartitions(self, f, preservesPartitioning=False):
        """Map partitions.

        :param f:
            mapping function
        """
        return self.mapPartitionsWithIndex(lambda i, p: f(p),
                                           preservesPartitioning)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """Map partitions with a function that takes the partition index
        and an iterator over the partition data as arguments.

        :param f:
            mapping function
        """
        return self.transform(
            lambda rdd: rdd.mapPartitionsWithIndex(f, preservesPartitioning)
        )

    def reduce(self, func):
        """Return a new DStream where each RDD was reduced with func."""

        # avoid RDD.reduce() which does not return an RDD
        return self.transform(
            lambda rdd: rdd.map(
                lambda i: (None, i)
            ).reduceByKey(
                func
            ).map(
                lambda i: i[1]
            )
        )

    def transform(self, func):
        """Return a new DStream where each RDD is transformed by f.

        :param f:
            Function that transforms an RDD.
        """
        return TransformedDStream(self, func)

    def pprint(self, num=10):
        """Print the first ``num`` elements of each RDD.

        :param num:
            Set number of elements to be printed.
        """

        def pprint_map(time_, rdd):
            data = rdd.take(num + 1)
            print('>>> Time: {}'.format(time_))
            for d in data[:num]:
                print(d)
            if len(data) > num:
                print('...')
            print('')

        self.foreachRDD(pprint_map)


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
        self._current_rdds = [self._func(rdd)
                              for rdd in self._prev._current_rdds]
