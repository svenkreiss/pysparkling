from functools import partial

from pyspark import StorageLevel, Row

from pysparkling import RDD


def to_row(cols, record):
    return Row(**dict(zip(cols, record)))


class DataFrameInternal(object):
    def __init__(self, sc, rdd: RDD, cols, convert_to_row=False):
        self._sc = sc
        self._cols = cols
        if convert_to_row:
            rdd = rdd.map(partial(to_row, cols))
        self._rdd = rdd

    def _with_rdd(self, rdd):
        return DataFrameInternal(self._sc, rdd, self._cols)

    def rdd(self):
        return self._rdd

    @staticmethod
    def range(sc, start, end=None, step=1, numPartitions=None):
        if end is None:
            start, end = 0, start

        rdd = sc.parallelize(
            ([i] for i in range(start, end, step)),
            numSlices=numPartitions
        )
        return DataFrameInternal(sc, rdd, ["id"], True)

    def count(self):
        return self._rdd.count()

    def collect(self):
        return self._rdd.collect()

    def toLocalIterator(self):
        return self._rdd.toLocalIterator()

    def limit(self, n):
        jdf = self._sc.parallelize(self._rdd.take(n))
        return self._with_rdd(jdf)

    def take(self, n):
        return self._rdd.take(n)

    def foreach(self, f):
        self._rdd.foreach(f)

    def foreachPartition(self, f):
        self._rdd.foreachPartition(f)

    def cache(self):
        return self._with_rdd(self._rdd.cache())

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        return self._with_rdd(self._rdd.persist(storageLevel))

    def unpersist(self, blocking=False):
        return self._with_rdd(self._rdd.unpersist(blocking))

    def coalesce(self, numPartitions):
        return self._with_rdd(self._rdd.coalesce(numPartitions))

    def repartition(self, numPartitions):
        return self._with_rdd(self._rdd.repartition(numPartitions))

    def distinct(self):
        return self._with_rdd(self._rdd.distinct())

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return self._with_rdd(
            self._rdd.sample(
                withReplacement=withReplacement,
                fraction=fraction,
                seed=seed
            )
        )

    def randomSplit(self, weights, seed):
        return self._with_rdd(
            self._rdd.randomSplit(weights=weights, seed=seed)
        )

    @property
    def storageLevel(self):
        return self._rdd.storageLevel

    def is_cached(self):
        return hasattr(self._rdd, "storageLevel")
