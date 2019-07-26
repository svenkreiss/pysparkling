import sys
import warnings

from pyspark import StorageLevel
# noinspection PyProtectedMember
from pyspark.sql.types import TimestampType, IntegralType, ByteType, ShortType, \
    IntegerType, FloatType, Row, _parse_datatype_json_value

from pysparkling.sql.column import Column
from pysparkling.sql.expressions.fields import FieldAsExpression
from pysparkling.sql.functions import col
from pysparkling.sql.internals import DataFrameInternal, InternalGroupedDataFrame
from pysparkling.sql.readwriter import DataFrameWriter
from pysparkling.sql.streaming import DataStreamWriter

if sys.version >= '3':
    basestring = str
    long = int

_NoValue = object()


# noinspection PyMethodMayBeStatic
class DataFrame(object):
    def __init__(self, jdf: DataFrameInternal, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        # noinspection PyProtectedMember
        self._sc = sql_ctx and sql_ctx._sc
        self._schema = None  # initialized lazily

        # Check whether _repr_html is supported or not, we use it to avoid calling _jdf twice
        # by __repr__ and _repr_html_ while eager evaluation opened.
        self._support_repr_html = False

    @property
    def rdd(self):
        return self._jdf.rdd()

    @property
    def is_cached(self):
        return self._jdf.is_cached()

    @property
    def na(self):
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.
        """
        return DataFrameNaFunctions(self)

    @property
    def stat(self):
        return DataFrameStatFunctions(self)

    def toJSON(self, use_unicode=True):
        """
        Return an RDD containing all items after JSONification

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2)
        >>> df.toJSON().collect()
        ['{"id": 0}', '{"id": 1}']
        """
        return self._jdf.toJSON()

    def createTempView(self, name):
        self._jdf.createTempView(name)

    def createOrReplaceTempView(self, name):
        self._jdf.createOrReplaceTempView(name)

    def createGlobalTempView(self, name):
        self._jdf.createGlobalTempView(name)

    def createOrReplaceGlobalTempView(self, name):
        self._jdf.createOrReplaceGlobalTempView(name)

    @property
    def write(self):
        return DataFrameWriter(self)

    @property
    def writeStream(self):
        return DataStreamWriter(self)

    @property
    def schema(self):
        return self._jdf.schema

    def printSchema(self):
        print(self.schema.treeString())

    def explain(self, extended=False):
        print("Pysparkling does not provide query execution explanation")

    def exceptAll(self, other):
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame` but
        not in another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `EXCEPT ALL` in SQL.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df1 = spark.createDataFrame(
        ...         [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b",  3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])

        >>> df1.exceptAll(df2).show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  a|  1|
        |  a|  2|
        |  c|  4|
        +---+---+

        Also as standard in SQL, this function resolves columns by position (not by name).
        """
        return DataFrame(self._jdf.exceptAll(other._jdf), self.sql_ctx)

    def isLocal(self):
        return True

    def isStreaming(self):
        # todo: Add support of streaming
        return False

    def show(self, n=20, truncate=True, vertical=False):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        ... )
        >>> df.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+
        >>> from pysparkling.sql.functions import map_from_arrays, array
        >>> df = spark.range(3)
        >>> df.select(array(df.id, df.id * 2)).show()
        +-------------------+
        |array(id, (id * 2))|
        +-------------------+
        |             [0, 0]|
        |             [1, 2]|
        |             [2, 4]|
        +-------------------+
        >>> df.select(map_from_arrays(array(df.id), array(df.id))).show()
        +-------------------------------------+
        |map_from_arrays(array(id), array(id))|
        +-------------------------------------+
        |                             [0 -> 0]|
        |                             [1 -> 1]|
        |                             [2 -> 2]|
        +-------------------------------------+
        >>> df.select(map_from_arrays(array(df.id, df.id * 2), array(df.id, df.id * 2))).show()
        +---------------------------------------------------------+
        |map_from_arrays(array(id, (id * 2)), array(id, (id * 2)))|
        +---------------------------------------------------------+
        |                                                 [0 -> 0]|
        |                                         [1 -> 1, 2 -> 2]|
        |                                         [2 -> 2, 4 -> 4]|
        +---------------------------------------------------------+
        """
        if truncate is True:
            print(self._jdf.showString(n, 20, vertical))
        else:
            print(self._jdf.showString(n, int(truncate), vertical))

    def __repr__(self):
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def checkpoint(self, eager=True):
        raise NotImplementedError("Streaming is not supported in PySparkling")

    def localCheckpoint(self, eager=True):
        raise NotImplementedError("Streaming is not supported in PySparkling")

    def withWatermark(self, eventTime, delayThreshold):
        raise NotImplementedError("Streaming is not supported in PySparkling")

    def hint(self, name, *parameters):
        if len(parameters) == 1 and isinstance(parameters[0], list):
            parameters = parameters[0]

        if not isinstance(name, str):
            raise TypeError("name should be provided as str, got {0}".format(type(name)))

        allowed_types = (basestring, list, float, int)
        for p in parameters:
            if not isinstance(p, allowed_types):
                raise TypeError(
                    "all parameters should be in {0}, got {1} of type {2}".format(
                        allowed_types, p, type(p)))

        jdf = self._jdf.hint(name, parameters)
        return DataFrame(jdf, self.sql_ctx)

    def count(self):
        """Returns the number of rows in this :class:`DataFrame`.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2)
        >>> df.count()
        2
        """
        return self._jdf.count()

    def collect(self):
        """Returns the number of rows in this :class:`DataFrame`.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2)
        >>> df.collect()
        [Row(id=0), Row(id=1)]
        """
        return self._jdf.collect()

    def toLocalIterator(self):
        """Returns an iterator on the content of this DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2)
        >>> list(df.toLocalIterator())
        [Row(id=0), Row(id=1)]
        """
        return self._jdf.toLocalIterator()

    def limit(self, n):
        """Restrict the DataFrame to the first n items

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(2).limit(1)
        >>> df.show()
        +---+
        | id|
        +---+
        |  0|
        +---+
        """
        return DataFrame(self._jdf.limit(n), self.sql_ctx)

    def take(self, n):
        """Return a list with the first n items of the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(2).take(1)
        [Row(id=0)]
        """
        return self._jdf.take(n)

    def foreach(self, f):
        """Execute a function for each item of the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> result = spark.range(2).foreach(print)
        Row(id=0)
        Row(id=1)
        >>> result is None
        True
        """
        self._jdf.foreach(f)

    def foreachPartition(self, f):
        """Execute a function for each partition of the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> result = spark.range(4, numPartitions=2).foreachPartition(lambda partition: print(list(partition)))
        [Row(id=0), Row(id=1)]
        [Row(id=2), Row(id=3)]
        >>> result is None
        True
        """
        self._jdf.foreachPartition(f)

    def cache(self):
        """Cache the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(4, numPartitions=2).cache()
        >>> df.is_cached
        True
        """
        return DataFrame(self._jdf.cache(), self.sql_ctx)

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        """Cache the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(4, numPartitions=2).persist()
        >>> df.is_cached
        True
        >>> df.storageLevel == StorageLevel.MEMORY_ONLY
        True
        """
        if storageLevel != StorageLevel.MEMORY_ONLY:
            raise NotImplementedError("Pysparkling currently only supports memory as the storage level")
        return DataFrame(self._jdf.persist(storageLevel), self.sql_ctx)

    @property
    def storageLevel(self):
        """Cache the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(4, numPartitions=2)
        >>> df.storageLevel
        StorageLevel(False, False, False, False, 1)
        >>> persisted_df = df.persist()
        >>> persisted_df.is_cached
        True
        >>> persisted_df.storageLevel
        StorageLevel(False, True, False, False, 1)
        """
        if self.is_cached:
            return self._jdf.storageLevel
        else:
            return StorageLevel(False, False, False, False, 1)

    def unpersist(self, blocking=False):
        """Cache the DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(4, numPartitions=2)
        >>> df.storageLevel
        StorageLevel(False, False, False, False, 1)
        >>> persisted_df = df.persist()
        >>> persisted_df.is_cached
        True
        >>> persisted_df.storageLevel
        StorageLevel(False, True, False, False, 1)
        >>> unpersisted_df = persisted_df.unpersist()
        >>> unpersisted_df.storageLevel
        StorageLevel(False, False, False, False, 1)
        """
        return DataFrame(self._jdf.unpersist(blocking), self.sql_ctx)

    def coalesce(self, numPartitions):
        """Coalesce the dataframe

        :param int numPartitions: Max number of partitions in the resulting dataframe.
        :rtype: DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(4, numPartitions=2).coalesce(1).rdd.getNumPartitions()
        1
        >>> spark.range(4, numPartitions=2).coalesce(4).rdd.getNumPartitions()
        2
        """
        return DataFrame(self._jdf.coalesce(numPartitions), self.sql_ctx)

    def repartition(self, numPartitions, *cols):
        """Repartition the dataframe

        :param int numPartitions: Number of partitions in the resulting dataframe.
        :rtype: DataFrame

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(4, numPartitions=2).repartition(1).rdd.getNumPartitions()
        1
        >>> spark.range(4, numPartitions=2).repartition(4).rdd.getNumPartitions()
        4
        >>> spark.range(4, numPartitions=2).repartition("id").rdd.getNumPartitions()
        200
        >>> spark.createDataFrame(
        ...   [[0], [1], [1], [2]],
        ...   ["v"]
        ... ).repartition(3, "v").rdd.foreachPartition(lambda x: print((list(x))))
        [Row(v=0)]
        [Row(v=1), Row(v=1)]
        [Row(v=2)]
        """
        if isinstance(numPartitions, int):
            if not cols:
                return DataFrame(self._jdf.repartition(numPartitions), self.sql_ctx)
            else:
                def partitioner(row: Row):
                    return sum(hash(row[col]) for col in cols)

                repartitioned_jdf = self._jdf.partitionValues(numPartitions, partitioner)
                return DataFrame(repartitioned_jdf, self.sql_ctx)
        elif isinstance(numPartitions, (basestring, Column)):
            # todo: rely on conf only
            newNumPartitions = self.sql_ctx.sparkSession.conf.get("numShufflePartitions", "200")
            return self.repartition(int(newNumPartitions), numPartitions, *cols)
        else:
            raise TypeError("numPartitions should be an int, str or Column")

    def repartitionByRange(self, numPartitions, *cols):
        """
        Returns a new :class:`DataFrame` partitioned by the given partitioning expressions. The
        resulting DataFrame is range partitioned.

        :param numPartitions:
            can be an int to specify the target number of partitions or a Column.
            If it is a Column, it will be used as the first partitioning column. If not specified,
            the default number of partitions is used.

        At least one partition-by expression must be specified.

        Note that due to performance reasons this method uses sampling to estimate the ranges.
        Hence, the output may not be consistent, since sampling can return different values.

        Sort orders are not supported in this pysparkling implementation

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(4, numPartitions=2).repartitionByRange(1, "id").rdd.getNumPartitions()
        1
        >>> spark.createDataFrame(
        ...   [[0], [1], [1], [2], [4]],
        ...   ["v"]
        ... ).repartitionByRange(3, "v").rdd.foreachPartition(lambda x: print((list(x))))
        [Row(v=0), Row(v=1), Row(v=1)]
        [Row(v=2)]
        [Row(v=4)]

        """
        # todo: support sort orders and assume "ascending nulls first" if needed
        if isinstance(numPartitions, int):
            if not cols:
                raise ValueError("At least one partition-by expression must be specified.")
            else:
                repartitioned_jdf = self._jdf.repartitionByRange(numPartitions, *cols)
                return DataFrame(repartitioned_jdf, self.sql_ctx)
        elif isinstance(numPartitions, (basestring, Column)):
            # todo: rely on conf only
            newNumPartitions = self.sql_ctx.sparkSession.conf.get("numShufflePartitions", "200")
            return self.repartitionByRange(int(newNumPartitions), numPartitions, *cols)
        else:
            raise TypeError("numPartitions should be an int, str or Column")

    def distinct(self):
        return DataFrame(self._jdf.distinct(), self.sql_ctx)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        is_withReplacement_set = type(withReplacement) == bool and isinstance(fraction, float)
        is_withReplacement_omitted_kwargs = withReplacement is None and isinstance(fraction, float)
        is_withReplacement_omitted_args = isinstance(withReplacement, float)

        if not (is_withReplacement_set
                or is_withReplacement_omitted_kwargs
                or is_withReplacement_omitted_args):
            argtypes = [
                str(type(arg))
                for arg in [withReplacement, fraction, seed]
                if arg is not None
            ]
            raise TypeError(
                "withReplacement (optional), fraction (required) and seed (optional)"
                " should be a bool, float and number; however, "
                "got [%s]." % ", ".join(argtypes))

        if is_withReplacement_omitted_args:
            if fraction is not None:
                seed = fraction
            fraction = withReplacement
            withReplacement = None

        seed = long(seed) if seed is not None else None
        args = [arg for arg in [withReplacement, fraction, seed] if arg is not None]
        jdf = self._jdf.sample(*args)
        return DataFrame(jdf, self.sql_ctx)

    def sampleBy(self, col, fractions, seed=None):
        """
        Returns a stratified sample without replacement based on the
        fraction given on each stratum.

        :param col: column that defines strata
        :param fractions:
            sampling fraction for each stratum. If a stratum is not
            specified, we treat its fraction as zero.
        :param seed: random seed
        :return: a new DataFrame that represents the stratified sample

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import count
        >>> spark = SparkSession(Context())
        >>> dataset = spark.createDataFrame(
        ...   [[i % 3] for i in range(100)],
        ...   ["key"]
        ... )
        >>> sampled = dataset.sampleBy("key", fractions={0: 0.5, 1: 0.25}, seed=0)
        >>> sampled.groupBy("key").agg(count(1)).show()
        +---+--------+
        |key|count(1)|
        +---+--------+
        |  0|      14|
        |  1|       3|
        +---+--------+
        >>> sampled.groupBy("key").count().show()
        +---+-----+
        |key|count|
        +---+-----+
        |  0|   14|
        |  1|    3|
        +---+-----+
        >>> sampled.groupBy("key").count().orderBy("key").show()
        +---+-----+
        |key|count|
        +---+-----+
        |  0|    3|
        |  1|    6|
        +---+-----+
        >>> dataset.sampleBy("key", fractions={2: 1.0}, seed=0).count()
        33
        """
        return DataFrame(self._jdf.sampleBy(col, fractions, seed), self.sql_ctx)

    def randomSplit(self, weights, seed=None):
        for w in weights:
            if w < 0.0:
                raise ValueError("Weights must be positive. Found weight value: {}".format(w))
        seed = long(seed) if seed is not None else None
        rdd_array = self._jdf.randomSplit(weights, seed)
        return [DataFrame(rdd, self.sql_ctx) for rdd in rdd_array]

    @property
    def dtypes(self):
        return [(f.name, f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self):
        return [f.name for f in self.schema.fields]

    def colRegex(self, colName):
        if not isinstance(colName, str):
            raise ValueError("colName should be provided as string")

    def alias(self, alias):
        assert isinstance(alias, basestring), "alias should be a string"
        raise NotImplementedError("Pysparkling does not currently support SQL catalog")

    def crossjoin(self, other):
        jdf = self._jdf.crossJoin(other)
        return DataFrame(jdf, self.sql_ctx)

    def join(self, other, on=None, how=None):
        """

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import length
        >>> spark = SparkSession(Context())
        >>> a = spark.createDataFrame([Row(name='o', time=1479441846)])
        >>> b = spark.createDataFrame([["a"],["b"],["o"]]).select(col("_1").alias("n"))
        >>> a.join(b, on=length(a.name) * 2 == length(b.n) + length(a.name)).show()
        +----+----------+---+
        |name|      time|  n|
        +----+----------+---+
        |   o|1479441846|  a|
        |   o|1479441846|  b|
        |   o|1479441846|  o|
        +----+----------+---+
        >>> c = spark.createDataFrame([["a"],["b"],["o"]]).select(col("_1").alias("name"))

        >>> a.join(c, on=(a.name == c.name)).show()
        +----+----------+----+
        |name|      time|name|
        +----+----------+----+
        |   o|1479441846|   o|
        +----+----------+----+
        >>> a.join(c, on=(a.name != c.name)).show()
        +----+----------+----+
        |name|      time|name|
        +----+----------+----+
        |   o|1479441846|   a|
        |   o|1479441846|   b|
        +----+----------+----+
        """
        return DataFrame(self._jdf.join(other._jdf, on, how), self.sql_ctx)

    def sortWithinPartitions(self, *cols, ascending=True):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(4, numPartitions=2)
        >>> df.sortWithinPartitions("id", ascending=False).foreachPartition(lambda p: print(list(p)))
        [Row(id=1), Row(id=0)]
        [Row(id=3), Row(id=2)]
        """
        sorted_jdf = self._jdf.sortWithinPartitions(cols, ascending=ascending)
        return DataFrame(sorted_jdf, self.sql_ctx)

    def sort(self, *cols, ascending=True):
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        :param cols: list of :class:`Column` or column names to sort by.
        :param ascending: boolean or list of boolean (default True).
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, length of the list must equal length of the `cols`.

        # >>> df.orderBy(desc("age"), "name").collect()
        # [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        # >>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
        # [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        ... )
        >>> df.sort("age", ascending=False).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+
        >>> df.sort("age", ascending=False).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+
        >>> df.sort("age").show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        sorted_jdf = self._jdf.sort(cols, ascending)
        return DataFrame(sorted_jdf, self.sql_ctx)

    def orderBy(self, *cols, **kwargs):
        return self.sort(*cols, **kwargs)

    @staticmethod
    def _sort_cols(cols, kwargs):
        """ Return a list of Columns that describes the sort order
        """
        # todo: use this function in sort methods to add support of custom orders
        if not cols:
            raise ValueError("should sort by at least one column")
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

        ascending = kwargs.get('ascending', True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                cols = [jc.desc() for jc in cols]
        elif isinstance(ascending, list):
            cols = [jc if asc else jc.desc() for asc, jc in zip(ascending, cols)]
        else:
            raise TypeError("ascending can only be boolean or list, but got %s" % type(ascending))

        return cols

    def describe(self, *cols):
        """Computes basic statistics for numeric and string columns.

        This include count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical or string columns.

        .. note:: This function is meant for exploratory data analysis, as we make no
            guarantee about the backward compatibility of the schema of the resulting DataFrame.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        ... )
        >>> df.describe(['age']).show()
        +-------+------------------+
        |summary|               age|
        +-------+------------------+
        |  count|                 2|
        |   mean|               3.5|
        | stddev|2.1213203435596424|
        |    min|                 2|
        |    max|                 5|
        +-------+------------------+
        >>> df.describe().show()
        +-------+------------------+-----+
        |summary|               age| name|
        +-------+------------------+-----+
        |  count|                 2|    2|
        |   mean|               3.5| null|
        | stddev|2.1213203435596424| null|
        |    min|                 2|Alice|
        |    max|                 5|  Bob|
        +-------+------------------+-----+

        Use summary for expanded statistics and control over which statistics to compute.
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        if len(cols) == 0:
            cols = ["*"]
        return DataFrame(self._jdf.describe(cols), self.sql_ctx)

    def summary(self, *statistics):
        """Computes specified statistics for numeric and string columns. Available statistics are:
        - count
        - mean
        - stddev
        - min
        - max
        - arbitrary approximate percentiles specified as a percentage (eg, 75%)

        If no statistics are given, this function computes count, mean, stddev, min,
        approximate quartiles (percentiles at 25%, 50%, and 75%), and max.

        .. note:: This function is meant for exploratory data analysis, as we make no
            guarantee about the backward compatibility of the schema of the resulting DataFrame.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        ... )

        >>> df.summary().show()
        +-------+------------------+-----+
        |summary|               age| name|
        +-------+------------------+-----+
        |  count|                 2|    2|
        |   mean|               3.5| null|
        | stddev|2.1213203435596424| null|
        |    min|                 2|Alice|
        |    25%|                 2| null|
        |    50%|                 2| null|
        |    75%|                 5| null|
        |    max|                 5|  Bob|
        +-------+------------------+-----+

        >>> df.summary("count", "min", "25%", "75%", "max").show()
        +-------+---+-----+
        |summary|age| name|
        +-------+---+-----+
        |  count|  2|    2|
        |    min|  2|Alice|
        |    25%|  2| null|
        |    75%|  5| null|
        |    max|  5|  Bob|
        +-------+---+-----+

        To do a summary for specific columns first select them:

        >>> df.select("age", "name").summary("count").show()
        +-------+---+----+
        |summary|age|name|
        +-------+---+----+
        |  count|  2|   2|
        +-------+---+----+

        See also describe for basic statistics.
        """
        if len(statistics) == 1 and isinstance(statistics[0], list):
            statistics = statistics[0]
        jdf = self._jdf.summary(statistics)
        return DataFrame(jdf, self.sql_ctx)

    def head(self, n=None):
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    def first(self):
        return self.head()

    def __getitem__(self, item):
        if isinstance(item, basestring):
            return getattr(self, item)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            return Column(FieldAsExpression(self.schema[item]))
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    def __getattr__(self, name):
        field_position = Column(name).find_position_in_schema(self.schema)
        return self[field_position]

    def select(self, *cols):
        """Projects a set of expressions and returns a new :class:`DataFrame`.

        :param cols: list of column names (string) or expressions (:class:`Column`).
            If one of the column names is '*', that column is expanded to include all columns
            in the current DataFrame.


        # >>> df = spark.createDataFrame(
        # ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        # ... )
        # >>> df.select('*').show()
        # +---+-----+
        # |age| name|
        # +---+-----+
        # |  2|Alice|
        # |  5|  Bob|
        # +---+-----+
        # >>> df.select('name', 'age').show()
        # +-----+---+
        # | name|age|
        # +-----+---+
        # |Alice|  2|
        # |  Bob|  5|
        # +-----+---+
        # >>> df.select('name').show()
        # +-----+
        # | name|
        # +-----+
        # |Alice|
        # |  Bob|
        # +-----+
        # >>> df.select(df.name, (df.age + 10).alias('age')).collect()
        # [Row(name='Alice', age=12), Row(name='Bob', age=15)]


        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> from pysparkling.sql.functions import explode, split
        >>> spark.createDataFrame([["a,b", "1,2"]]).select(explode(split("_1", ",")), "*").show()
        +---+---+---+
        |col| _1| _2|
        +---+---+---+
        |  a|a,b|1,2|
        |  b|a,b|1,2|
        +---+---+---+

        """
        jdf = self._jdf.select(*cols)
        return DataFrame(jdf, self.sql_ctx)

    def selectExpr(self, *expr):
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        # todo: handle this:
        # >>> df.selectExpr("age * 2", "abs(age)").collect()
        # [Row((age * 2)=4, abs(age)=2), Row((age * 2)=10, abs(age)=5)]

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.selectExpr("age").show()
        +---+
        |age|
        +---+
        |  2|
        |  5|
        +---+
        """
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]
        # todo: handle expr like abs(age)
        # with jdf = self._jdf.selectExpr(expr)
        jdf = self._jdf.select(*expr)
        return DataFrame(jdf, self.sql_ctx)

    def filter(self, condition):
        if isinstance(condition, basestring):
            jdf = self._jdf.filter(col(condition))
        elif isinstance(condition, Column):
            jdf = self._jdf.filter(condition)
        else:
            raise TypeError("condition should be string or Column")
        return DataFrame(jdf, self.sql_ctx)

    def groupBy(self, *cols):
        jgd = InternalGroupedDataFrame(self, cols, InternalGroupedDataFrame.GROUP_BY_TYPE)
        from pysparkling.sql.group import GroupedData
        return GroupedData(jgd, self)

    def rollup(self, *cols):
        jgd = InternalGroupedDataFrame(self, cols, InternalGroupedDataFrame.ROLLUP_TYPE)
        from pysparkling.sql.group import GroupedData
        return GroupedData(jgd, self)

    def cube(self, *cols):
        jgd = InternalGroupedDataFrame(self, cols, InternalGroupedDataFrame.CUBE_TYPE)
        from pysparkling.sql.group import GroupedData
        return GroupedData(jgd, self)

    def agg(self, *exprs):
        return self.groupBy().agg(*exprs)

    def union(self, other):

        """ Return a new :class:`DataFrame` containing union of rows in this and another frame.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df1 = spark.createDataFrame([Row(age=5, name='Bob')])
        >>> df2 = spark.createDataFrame([Row(age=2, name='Alice')])
        >>> df1.union(df2).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+
        """
        return DataFrame(self._jdf.union(other._jdf), self.sql_ctx)

    def unionAll(self, other):
        """
        Same as union
        """
        return self.union(other)

    def unionByName(self, other):
        """ Returns a new :class:`DataFrame` containing union of rows in this and another frame.

        This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
        union (that does deduplication of elements), use this function followed by :func:`distinct`.

        The difference between this function and :func:`union` is that this function
        resolves columns by name (not by position):

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df1 = spark.createDataFrame([Row(age=5, name='Bob')])
        >>> df2 = spark.createDataFrame([Row(age=2, name='Alice')])
        >>> df1.union(df2).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+
        """
        return DataFrame(self._jdf.unionByName(other._jdf), self.sql_ctx)

    def intersect(self, other):
        return DataFrame(self._jdf.intersect(other._jdf), self.sql_ctx)

    def intersectAll(self, other):
        return DataFrame(self._jdf.intersectAll(other._jdf), self.sql_ctx)

    def subtract(self, other):
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sql_ctx)

    def dropDuplicates(self, subset=None):
        jdf = self._jdf.dropDuplicates(cols=subset)
        return DataFrame(jdf, self.sql_ctx)

    def dropna(self, how='any', thresh=None, subset=None):
        if how is not None and how not in ['any', 'all']:
            raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if subset is None:
            subset = self.columns
        elif isinstance(subset, basestring):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        if thresh is None:
            thresh = len(subset) if how == 'any' else 1

        return DataFrame(self._jdf.na().drop(thresh, self._jseq(subset)), self.sql_ctx)

    def fillna(self, value, subset=None):
        if not isinstance(value, (float, int, long, basestring, bool, dict)):
            raise ValueError("value should be a float, int, long, string, bool or dict")

        # Note that bool validates isinstance(int), but we don't want to
        # convert bools to floats

        if not isinstance(value, bool) and isinstance(value, (int, long)):
            value = float(value)

        if isinstance(value, dict):
            return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
        elif subset is None:
            return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
        else:
            if isinstance(subset, basestring):
                subset = [subset]
            elif not isinstance(subset, (list, tuple)):
                raise ValueError("subset should be a list or tuple of column names")

            return DataFrame(self._jdf.na().fill(value, self._jseq(subset)), self.sql_ctx)

    def replace(self, to_replace, value=_NoValue, subset=None):
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise TypeError("value argument is required when to_replace is not a dictionary.")

        # Helper functions
        def all_of(types):
            def all_of_(xs):
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(basestring)
        all_of_numeric = all_of((float, int, long))

        # Validate input types
        valid_types = (bool, float, int, long, basestring, list, tuple)
        if not isinstance(to_replace, valid_types + (dict,)):
            raise ValueError(
                "to_replace should be a bool, float, int, long, string, list, tuple, or dict. "
                "Got {0}".format(type(to_replace)))

        if not isinstance(value, valid_types) and value is not None \
                and not isinstance(to_replace, dict):
            raise ValueError("If to_replace is not a dict, value should be "
                             "a bool, float, int, long, string, list, tuple or None. "
                             "Got {0}".format(type(value)))

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise ValueError("to_replace and value lists should be of the same length. "
                                 "Got {0} and {1}".format(len(to_replace), len(value)))

        if not (subset is None or isinstance(subset, (list, tuple, basestring))):
            raise ValueError("subset should be a list or tuple of column names, "
                             "column name or None. Got {0}".format(type(subset)))

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, long, basestring)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, long, basestring)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, value))

        if isinstance(subset, basestring):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(all_of_type(rep_dict.keys())
                   and all_of_type(x for x in rep_dict.values() if x is not None)
                   for all_of_type in [all_of_bool, all_of_str, all_of_numeric]):
            raise ValueError("Mixed type replacements are not supported")

        if subset is None:
            return DataFrame(self._jdf.na().replace('*', rep_dict), self.sql_ctx)
        else:
            return DataFrame(
                self._jdf.na().replace(subset, rep_dict), self.sql_ctx)

    def approxQuantile(self, col, probabilities, relativeError):
        """
        Approximate a list of quantiles (probabilities) for one or a list of columns (col)
        with an error related to relativeError.

        More information in pysparkling.stat_counter.ColumnStatHelper

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.approxQuantile("age", [0.1, 0.5, 0.9], 1/1000)
        [2.0, 2.0, 5.0]
        >>> df.approxQuantile(["age"], [0.1, 0.5, 0.9], 1/1000)
        [[2.0, 2.0, 5.0]]
        """
        if not isinstance(col, (basestring, list, tuple)):
            raise ValueError("col should be a string, list or tuple, but got %r" % type(col))

        isStr = isinstance(col, basestring)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [col]

        for c in col:
            if not isinstance(c, basestring):
                raise ValueError("columns should be strings, but got %r" % type(c))

        if not isinstance(probabilities, (list, tuple)):
            raise ValueError("probabilities should be a list or tuple")
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int, long)) or p < 0 or p > 1:
                raise ValueError("probabilities should be numerical (float, int, long) in [0,1].")

        if not isinstance(relativeError, (float, int, long)) or relativeError < 0:
            raise ValueError("relativeError should be numerical (float, int, long) >= 0.")
        relativeError = float(relativeError)

        jaq = self._jdf.approxQuantile(col, probabilities, relativeError)
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    def corr(self, col1, col2, method=None):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(50).corr('id', 'id')
        1.0
        """
        if not isinstance(col1, basestring):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, basestring):
            raise ValueError("col2 should be a string.")
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise ValueError("Currently only the calculation of the Pearson Correlation " +
                             "coefficient is supported.")
        return self._jdf.corr(col1, col2, method)

    def cov(self, col1, col2):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> spark.range(50).cov('id', 'id')
        212.5
        """
        if not isinstance(col1, basestring):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, basestring):
            raise ValueError("col2 should be a string.")
        return self._jdf.cov(col1, col2)

    def crosstab(self, col1, col2):
        # todo: extra workin here
        # todo: tests on schema
        if not isinstance(col1, basestring):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, basestring):
            raise ValueError("col2 should be a string.")
        return DataFrame(self._jdf.crosstab(self, col1, col2), self.sql_ctx)

    def freqItems(self, cols, support=None):
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise ValueError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame(self._jdf.freqItems(cols, support), self.sql_ctx)

    def withColumn(self, colName, col):
        """

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import length
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        ... )
        >>> df.withColumn("name_length", length("name")).show()
        +---+-----+-----------+
        |age| name|name_length|
        +---+-----+-----------+
        |  5|  Bob|          3|
        |  2|Alice|          5|
        +---+-----+-----------+
        """
        assert isinstance(col, Column), "col should be Column"
        return DataFrame(self._jdf.withColumn(colName, col), self.sql_ctx)

    def withColumnRenamed(self, existing, new):
        return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)

    def drop(self, *cols):
        if len(cols) == 1:
            col = cols[0]
            if isinstance(col, basestring):
                jdf = self._jdf.drop(col)
            elif isinstance(col, Column):
                jdf = self._jdf.drop(col._jc)
            else:
                raise TypeError("col should be a string or a Column")
        else:
            for col in cols:
                if not isinstance(col, basestring):
                    raise TypeError("each col in the param list should be a string")
            jdf = self._jdf.drop(cols)

        return DataFrame(jdf, self.sql_ctx)

    def toDF(self, *cols):
        """Returns a new class:`DataFrame` that with new specified column names

        :param cols: list of new column names (string)

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.toDF('f1', 'f2').show()
        +---+-----+
        | f1|   f2|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        return DataFrame(self._jdf.toDF(cols), self.sql_ctx)

    def transform(self, func):
        result = func(self)
        assert isinstance(result, DataFrame), "Func returned an instance of type [%s], " \
                                              "should have been DataFrame." % type(result)
        return result

    def toPandas(self):
        from pyspark.sql.utils import require_minimum_pandas_version
        require_minimum_pandas_version()

        import pandas as pd

        # noinspection PyProtectedMember
        sql_ctx_conf = self.sql_ctx._conf
        if sql_ctx_conf.pandasRespectSessionTimeZone():
            timezone = sql_ctx_conf.sessionLocalTimeZone()
        else:
            timezone = None

        # todo: Handle sql_ctx_conf.arrowEnabled()
        # Below is toPandas without Arrow optimization.
        pdf = pd.DataFrame.from_records(self.collect(), columns=self.columns)

        dtype = {}
        for field in self.schema:
            pandas_type = _to_corrected_pandas_type(field.dataType)
            # SPARK-21766: if an integer field is nullable and has null values, it can be
            # inferred by pandas as float column. Once we convert the column with NaN back
            # to integer type e.g., np.int16, we will hit exception. So we use the inferred
            # float type, not the corrected type from the schema in this case.
            if pandas_type is not None and \
                    not (isinstance(field.dataType, IntegralType) and field.nullable and
                         pdf[field.name].isnull().any()):
                dtype[field.name] = pandas_type

        for f, t in dtype.items():
            pdf[f] = pdf[f].astype(t, copy=False)

        if timezone is None:
            return pdf
        else:
            from pyspark.sql.types import _check_series_convert_timestamps_local_tz
            for field in self.schema:
                # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
                if isinstance(field.dataType, TimestampType):
                    pdf[field.name] = \
                        _check_series_convert_timestamps_local_tz(pdf[field.name], timezone)
            return pdf

    def groupby(self, *cols):
        return self.groupBy(*cols)

    def drop_duplicates(self, subset=None):
        return self.dropDuplicates(subset)

    def where(self, condition):
        return self.filter(condition)


class DataFrameNaFunctions(object):
    def __init__(self, df: DataFrame):
        self.df = df

    def drop(self, how='any', thresh=None, subset=None):
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    drop.__doc__ = DataFrame.dropna.__doc__

    def fill(self, value, subset=None):
        return self.df.fillna(value=value, subset=subset)

    fill.__doc__ = DataFrame.fillna.__doc__

    def replace(self, to_replace, value=_NoValue, subset=None):
        return self.df.replace(to_replace, value, subset)

    replace.__doc__ = DataFrame.replace.__doc__


class DataFrameStatFunctions(object):
    def __init__(self, df: DataFrame):
        self.df = df

    def approxQuantile(self, col, probabilities, relativeError):
        return self.df.approxQuantile(col, probabilities, relativeError)

    approxQuantile.__doc__ = DataFrame.approxQuantile.__doc__

    def corr(self, col1, col2, method=None):
        return self.df.corr(col1, col2, method)

    corr.__doc__ = DataFrame.corr.__doc__

    def cov(self, col1, col2):
        return self.df.cov(col1, col2)

    cov.__doc__ = DataFrame.cov.__doc__

    def crosstab(self, col1, col2):
        return self.df.crosstab(col1, col2)

    crosstab.__doc__ = DataFrame.crosstab.__doc__

    def freqItems(self, cols, support=None):
        return self.df.freqItems(cols, support)

    freqItems.__doc__ = DataFrame.freqItems.__doc__

    def sampleBy(self, col, fractions, seed=None):
        return self.df.sampleBy(col, fractions, seed)

    sampleBy.__doc__ = DataFrame.sampleBy.__doc__


def _to_corrected_pandas_type(dt):
    """
    When converting Spark SQL records to Pandas DataFrame, the inferred data type may be wrong.
    This method gets the corrected data type for Pandas if that type may be inferred uncorrectly.
    """
    import numpy as np
    if type(dt) == ByteType:
        return np.int8
    elif type(dt) == ShortType:
        return np.int16
    elif type(dt) == IntegerType:
        return np.int32
    elif type(dt) == FloatType:
        return np.float32
    else:
        return None
