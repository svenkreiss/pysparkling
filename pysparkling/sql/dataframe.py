import warnings

from pysparkling import StorageLevel
from pysparkling.sql.column import parse, Column
from pysparkling.sql.expressions.fields import FieldAsExpression
from pysparkling.sql.internal_utils.joins import JOIN_TYPES, CROSS_JOIN
from pysparkling.sql.utils import IllegalArgumentException, AnalysisException

_NoValue = object()


class DataFrame(object):
    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx

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
        return self._jdf.toJSON(use_unicode)

    def createTempView(self, name):
        self._jdf.createTempView(name)

    def createOrReplaceTempView(self, name):
        self._jdf.createOrReplaceTempView(name)

    def createGlobalTempView(self, name):
        self._jdf.createGlobalTempView(name)

    def createOrReplaceGlobalTempView(self, name):
        self._jdf.createOrReplaceGlobalTempView(name)

    @property
    def schema(self):
        return self._jdf.unbound_schema

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
        >>> df1 = spark.createDataFrame([
        ...   ("a", 1),
        ...   ("a", 1),
        ...   ("a", 1),
        ...   ("a", 2),
        ...   ("b", 3),
        ...   ("c", 4)
        ... ], ["C1", "C2"])
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
        # noinspection PyProtectedMember
        return DataFrame(self._jdf.exceptAll(other._jdf), self.sql_ctx)

    def isLocal(self):
        return True

    def isStreaming(self):
        # pylint: disable=fixme
        # todo: Add support of streaming
        return False

    def show(self, n=20, truncate=True, vertical=False):
        """
        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> from pysparkling.sql.functions import col
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
        >>> c = col("id")
        >>> (spark.range(9, 11)
        ...       .select(c, c*2, c**2)
        ...       .show(vertical=True))  # doctest: +NORMALIZE_WHITESPACE
        -RECORD 0-------------
         id           | 9
         (id * 2)     | 18
         POWER(id, 2) | 81.0
        -RECORD 1-------------
         id           | 10
         (id * 2)     | 20
         POWER(id, 2) | 100.0
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

        allowed_types = (str, list, float, int)
        for p in parameters:
            if not isinstance(p, allowed_types):
                raise TypeError(
                    "all parameters should be in {0}, got {1} of type {2}".format(
                        allowed_types, p, type(p)))

        # No hint are supported by pysparkling hence nothing is done here
        jdf = self._jdf
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
        >>> result = (spark.range(4, numPartitions=2)
        ...                .foreachPartition(lambda partition: print(list(partition))))
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
            raise NotImplementedError(
                "Pysparkling currently only supports memory as the storage level"
            )
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
        >>> spark.range(3).coalesce(1).collect()
        [Row(id=0), Row(id=1), Row(id=2)]
        >>> df = spark.range(200).repartition(300)
        >>> df = df.filter(df.id % 2 == 0).select(df.id * 2)
        >>> df = df.coalesce(299)
        >>> df.rdd.getNumPartitions()
        299
        >>> df = df.coalesce(298)
        >>> df.rdd.getNumPartitions()
        298
        >>> df = df.coalesce(174)
        >>> df.rdd.getNumPartitions()
        174
        >>> df = df.coalesce(75)
        >>> df.rdd.getNumPartitions()
        75
        >>> df = df.coalesce(1)
        >>> df.rdd.getNumPartitions()
        1
        >>> df.count()
        100
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
                return DataFrame(self._jdf.simple_repartition(numPartitions), self.sql_ctx)

            cols = [parse(col) for col in cols]
            repartitioned_jdf = self._jdf.repartition(numPartitions, cols)
            return DataFrame(repartitioned_jdf, self.sql_ctx)
        if isinstance(numPartitions, (str, Column)):
            return self.repartition(200, numPartitions, *cols)
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
        # pylint: disable=fixme
        # todo: support sort orders and assume "ascending nulls first" if needed
        if isinstance(numPartitions, int):
            if not cols:
                raise ValueError("At least one partition-by expression must be specified.")
            cols = [parse(col) for col in cols]
            repartitioned_jdf = self._jdf.repartitionByRange(numPartitions, *cols)
            return DataFrame(repartitioned_jdf, self.sql_ctx)
        if isinstance(numPartitions, (str, Column)):
            return self.repartitionByRange(200, numPartitions, *cols)
        raise TypeError("numPartitions should be an int, str or Column")

    def distinct(self):
        return DataFrame(self._jdf.distinct(), self.sql_ctx)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        is_withReplacement_set = isinstance(withReplacement, bool) and isinstance(fraction, float)
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

        seed = int(seed) if seed is not None else None
        args = [arg for arg in [withReplacement, fraction, seed] if arg is not None]
        jdf = self._jdf.sample(*args)
        return DataFrame(jdf, self.sql_ctx)

    def randomSplit(self, weights, seed=None):
        for w in weights:
            if w < 0.0:
                raise ValueError("Weights must be positive. Found weight value: {}".format(w))
        seed = int(seed) if seed is not None else None
        rdd_array = self._jdf.randomSplit(weights, seed)
        return [DataFrame(rdd, self.sql_ctx) for rdd in rdd_array]

    @property
    def dtypes(self):
        return [(f.name, f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self):
        return [f.name for f in self.schema.fields]

    def alias(self, alias):
        assert isinstance(alias, str), "alias should be a string"
        raise NotImplementedError("Pysparkling does not currently support SQL catalog")

    def crossJoin(self, other):
        """
        Returns the cartesian product of self and other

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame([
        ...   Row(age=2, name='Alice'),
        ...   Row(age=5, name='Bob')
        ... ])
        >>> df2 = spark.createDataFrame([
        ...   Row(name='Tom', height=80),
        ...   Row(name='Bob', height=85)
        ... ])
        >>> df.select("age", "name").collect()
        [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        >>> df2.select("name", "height").collect()
        [Row(name='Tom', height=80), Row(name='Bob', height=85)]
        >>> df.crossJoin(df2.select("height")).select("age", "name", "height").show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        |  2|Alice|    80|
        |  2|Alice|    85|
        |  5|  Bob|    80|
        |  5|  Bob|    85|
        +---+-----+------+

        """
        # noinspection PyProtectedMember
        jdf = self._jdf.crossJoin(other._jdf)
        return DataFrame(jdf, self.sql_ctx)

    def join(self, other, on=None, how="inner"):
        # noinspection PyProtectedMember
        if isinstance(on, str):
            return self.join(other=other, on=[on], how=how)

        how = how.lower().replace("_", "")
        if how not in JOIN_TYPES:
            raise IllegalArgumentException("Invalid how argument in join: {0}".format(how))
        how = JOIN_TYPES[how]

        if how == CROSS_JOIN and on is not None:
            raise IllegalArgumentException("`on` must be None for a crossJoin")

        if how != CROSS_JOIN and on is None:
            raise IllegalArgumentException(
                "Join condition is missing. "
                "Use the CROSS JOIN syntax to allow cartesian products"
            )

        return DataFrame(self._jdf.join(other._jdf, on, how), self.sql_ctx)

    def sortWithinPartitions(self, *cols, **kwargs):
        """
        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.range(4, numPartitions=2)
        >>> (df.sortWithinPartitions("id", ascending=False)
        ...    .foreachPartition(lambda p: print(list(p))))
        [Row(id=1), Row(id=0)]
        [Row(id=3), Row(id=2)]
        """
        ascending = kwargs.pop("ascending", True)
        if kwargs:
            raise TypeError("Unrecognized arguments: {0}".format(kwargs))
        sorted_jdf = self._jdf.sortWithinPartitions(cols, ascending=ascending)
        return DataFrame(sorted_jdf, self.sql_ctx)

    def sort(self, *cols, **kwargs):
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        :param cols: list of :class:`Column` or column names to sort by.
        :param ascending: boolean or list of boolean (default True).
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, length of the list must equal length of the `cols`.

        # >>> df.orderBy(desc("age"), "name").collect()
        # [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        # >>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
        # [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        >>> from pysparkling import Context, Row
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
        exprs = [parse(col) for col in cols]
        sorting_cols = self._sort_cols(exprs, kwargs)
        sorted_jdf = self._jdf.sort(sorting_cols)
        return DataFrame(sorted_jdf, self.sql_ctx)

    def orderBy(self, *cols, **kwargs):
        return self.sort(*cols, **kwargs)

    @staticmethod
    def _sort_cols(cols, kwargs):
        """ Return a list of Columns that describes the sort order
        """
        if not cols:
            raise ValueError("should sort by at least one column")
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

        ascending = kwargs.pop('ascending', True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                cols = [jc.desc() for jc in cols]
        elif isinstance(ascending, list):
            cols = [jc if asc else jc.desc() for asc, jc in zip(ascending, cols)]
        else:
            raise TypeError("ascending can only be boolean or list, but got %s" % type(ascending))

        if kwargs:
            raise TypeError("Unrecognized arguments: {0}".format(kwargs))

        return cols

    def describe(self, *cols):
        """Computes basic statistics for numeric and string columns.

        This include count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical or string columns.

        .. note:: This function is meant for exploratory data analysis, as we make no
            guarantee about the backward compatibility of the schema of the resulting DataFrame.

        >>> from pysparkling import Context, Row
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
        if not cols:
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

        >>> from pysparkling import Context, Row
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
        if isinstance(item, str):
            return getattr(self, item)
        if isinstance(item, Column):
            return self.filter(item)
        if isinstance(item, (list, tuple)):
            return self.select(*item)
        if isinstance(item, int):
            return Column(FieldAsExpression(self._jdf.bound_schema[item]))
        raise TypeError("unexpected item type: %s" % type(item))

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        try:
            field_position = Column(name).find_position_in_schema(self.schema)
        except AnalysisException:
            raise AttributeError
        return self[field_position]

    def select(self, *cols):
        """Projects a set of expressions and returns a new :class:`DataFrame`.

        :param cols: list of column names (string) or expressions (:class:`Column`).
            If one of the column names is '*', that column is expanded to include all columns
            in the current DataFrame.

        >>> from pysparkling import Context, Row
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df = spark.createDataFrame(
        ...   [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
        ... )
        >>> df.select('*').show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        >>> df.select('name', 'age').show()
        +-----+---+
        | name|age|
        +-----+---+
        |Alice|  2|
        |  Bob|  5|
        +-----+---+
        >>> df.select('name').show()
        +-----+
        | name|
        +-----+
        |Alice|
        |  Bob|
        +-----+
        >>> df.select(df.name, (df.age + 10).alias('age')).collect()
        [Row(name='Alice', age=12), Row(name='Bob', age=15)]
        """
        jdf = self._jdf.select(*cols)
        return DataFrame(jdf, self.sql_ctx)

    def selectExpr(self, *expr):
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        # >>> df.selectExpr("age * 2", "abs(age)").collect()
        # [Row((age * 2)=4, abs(age)=2), Row((age * 2)=10, abs(age)=5)]

        >>> from pysparkling import Context, Row
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
        # pylint: disable=fixme
        # todo: handle expr like abs(age)
        jdf = self._jdf.select(*expr)
        return DataFrame(jdf, self.sql_ctx)

    def filter(self, condition):
        if isinstance(condition, str):
            jdf = self._jdf.filter(parse(condition))
        elif isinstance(condition, Column):
            jdf = self._jdf.filter(condition)
        else:
            raise TypeError("condition should be string or Column")
        return DataFrame(jdf, self.sql_ctx)

    def union(self, other):

        """ Return a new :class:`DataFrame` containing union of rows in this and another frame.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        >>> from pysparkling import Context, Row
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
        # noinspection PyProtectedMember
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

        >>> from pysparkling import Context, Row
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
        >>> df1.unionByName(df2).select(df1.age).show()
        +---+
        |age|
        +---+
        |  5|
        |  2|
        +---+
        >>> df1.unionByName(df2).select(df2.age).show() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
          ...
        pysparkling.sql.utils.AnalysisException: Unable to find the column 'age#3' \
        among ['age#1', 'name#2']
        """
        # noinspection PyProtectedMember
        return DataFrame(self._jdf.unionByName(other._jdf), self.sql_ctx)

    def intersect(self, other):
        """

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])

        >>> df1.intersect(df2).sort("C1", "C2").show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  b|  3|
        +---+---+
        """
        # noinspection PyProtectedMember
        return DataFrame(self._jdf.intersect(other._jdf), self.sql_ctx)

    def intersectAll(self, other):
        """

        >>> from pysparkling import Context
        >>> from pysparkling.sql.session import SparkSession
        >>> spark = SparkSession(Context())
        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])

        >>> df1.intersectAll(df2).sort("C1", "C2").show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  a|  1|
        |  b|  3|
        +---+---+
        """
        # noinspection PyProtectedMember
        return DataFrame(self._jdf.intersectAll(other._jdf), self.sql_ctx)

    def subtract(self, other):
        # noinspection PyProtectedMember
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sql_ctx)

    def dropDuplicates(self, subset=None):
        """Return a new DataFrame without any duplicate values
        between rows or between rows for a subset of fields

        >>> from pysparkling import Context, Row
        >>> sc = Context()
        >>> df = sc.parallelize([ \\
        ...     Row(name='Alice', age=5, height=80), \\
        ...     Row(name='Alice', age=5, height=80), \\
        ...     Row(name='Alice', age=10, height=80)]).toDF()
        >>> df.dropDuplicates().show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        |  5|    80|Alice|
        | 10|    80|Alice|
        +---+------+-----+

        >>> df.dropDuplicates(['name', 'height']).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        |  5|    80|Alice|
        +---+------+-----+
        """
        jdf = self._jdf.dropDuplicates(cols=subset)
        return DataFrame(jdf, self.sql_ctx)

    def dropna(self, how='any', thresh=None, subset=None):
        if how is not None and how not in ['any', 'all']:
            raise ValueError("how ('" + how + "') should be 'any' or 'all'")

        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        if thresh is None:
            thresh = len(subset) if how == 'any' else 1

        return DataFrame(self._jdf.dropna(thresh, subset), self.sql_ctx)

    def fillna(self, value, subset=None):
        if not isinstance(value, (float, int, str, bool, dict)):
            raise ValueError("value should be a float, int, long, string, bool or dict")

        # Note that bool validates isinstance(int), but we don't want to
        # convert bools to floats

        if not isinstance(value, bool) and isinstance(value, int):
            value = float(value)

        if isinstance(value, dict):
            return DataFrame(self._jdf.fillna(value), self.sql_ctx)
        if subset is None:
            return DataFrame(self._jdf.fillna(value), self.sql_ctx)
        if isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise ValueError("subset should be a list or tuple of column names")

        return DataFrame(self._jdf.fillna(value, subset), self.sql_ctx)

    def replace(self, to_replace, value=_NoValue, subset=None):
        # Helper functions
        def all_of(types):
            def all_of_(xs):
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(str)
        all_of_numeric = all_of((float, int))

        value = self._check_replace_inputs(subset, to_replace, value)

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, str)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, str)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, value))

        if isinstance(subset, str):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(all_of_type(rep_dict.keys())
                   and all_of_type(x for x in rep_dict.values() if x is not None)
                   for all_of_type in [all_of_bool, all_of_str, all_of_numeric]):
            raise ValueError("Mixed type replacements are not supported")

        if subset is None:
            return DataFrame(self._jdf.replace('*', rep_dict), self.sql_ctx)
        return DataFrame(self._jdf.replace(subset, rep_dict), self.sql_ctx)

    def _check_replace_inputs(self, subset, to_replace, value):
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise TypeError("value argument is required when to_replace is not a dictionary.")

        # Validate input types
        valid_types = (bool, float, int, str, list, tuple)
        if not isinstance(to_replace, valid_types) and not isinstance(to_replace, dict):
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
        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise ValueError("subset should be a list or tuple of column names, "
                             "column name or None. Got {0}".format(type(subset)))
        return value

    def approxQuantile(self, col, probabilities, relativeError):
        """
        Approximate a list of quantiles (probabilities) for one or a list of columns (col)
        with an error related to relativeError.

        More information in pysparkling.stat_counter.ColumnStatHelper

        >>> from pysparkling import Context, Row
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
        if not isinstance(col, (str, list, tuple)):
            raise ValueError("col should be a string, list or tuple, but got %r" % type(col))

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [col]

        for c in col:
            if not isinstance(c, str):
                raise ValueError("columns should be strings, but got %r" % type(c))

        if not isinstance(probabilities, (list, tuple)):
            raise ValueError("probabilities should be a list or tuple")
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int)) or p < 0 or p > 1:
                raise ValueError("probabilities should be numerical (float, int, long) in [0,1].")

        if not isinstance(relativeError, (float, int)) or relativeError < 0:
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
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        if not method:
            method = "pearson"
        if method != "pearson":
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
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
            raise ValueError("col2 should be a string.")
        return self._jdf.cov(col1, col2)

    def crosstab(self, col1, col2):
        if not isinstance(col1, str):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, str):
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
        """
        return DataFrame(self._jdf.sampleBy(parse(col), fractions, seed), self.sql_ctx)

    def withColumn(self, colName, col):
        """

        # >>> from pysparkling import Context, Row
        # >>> from pysparkling.sql.session import SparkSession
        # >>> from pysparkling.sql.functions import length
        # >>> spark = SparkSession(Context())
        # >>> df = spark.createDataFrame(
        # ...   [Row(age=5, name='Bob'), Row(age=2, name='Alice')]
        # ... )
        # >>> df.withColumn("name_length", length("name")).show()
        # +---+-----+-----------+
        # |age| name|name_length|
        # +---+-----+-----------+
        # |  5|  Bob|          3|
        # |  2|Alice|          5|
        # +---+-----+-----------+
        """
        assert isinstance(col, Column), "col should be Column"
        return DataFrame(self._jdf.withColumn(colName, col), self.sql_ctx)


class DataFrameNaFunctions(object):
    def __init__(self, df):
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
    def __init__(self, df):
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
