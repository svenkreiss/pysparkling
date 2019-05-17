import sys

from pyspark import StorageLevel
from pyspark.sql import Column
from pyspark.sql.types import TimestampType, IntegralType, _parse_datatype_json_string, ByteType, ShortType, \
    IntegerType, FloatType, Row

from pysparkling import RDD
from pysparkling.sql.internals import DataFrameInternal
from pysparkling.sql.readwriter import DataFrameWriter
from pysparkling.sql.streaming import DataStreamWriter

if sys.version >= '3':
    basestring = str
    long = int

_NoValue = object()


class DataFrame(object):
    def __init__(self, jdf: DataFrameInternal, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc
        self._schema = []  # initialized lazily

        # Check whether _repr_html is supported or not, we use it to avoid calling _jdf twice
        # by __repr__ and _repr_html_ while eager evaluation opened.
        self._support_repr_html = False

    @property
    def rdd(self):
        return self._jdf.rdd()

    @property
    def is_cached(self):
        return self._jdf.is_cached()

    def na(self):
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.
        """
        return DataFrameNaFunctions(self)

    def stat(self):
        return DataFrameStatFunctions(self)

    def toJSON(self, use_unicode=True):
        rdd = self._jdf.toJSON()
        return RDD(rdd.partitions(), ctx=self._sc)

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
        if self._schema is None:
            try:
                self._schema = _parse_datatype_json_string(self._jdf.schema().json())
            except AttributeError as e:
                raise Exception(
                    "Unable to parse datatype from schema. %s" % e)
        return self._schema

    def printSchema(self):
        print(self._jdf.schema().treeString())

    def explain(self, extended=False):
        if extended:
            print(self._jdf.queryExecution().toString())
        else:
            print(self._jdf.queryExecution().simpleString())

    def exceptAll(self, other):
        return DataFrame(self._jdf.exceptAll(other._jdf), self.sql_ctx)

    def isLocal(self):
        return True

    def isStreaming(self):
        return self._jdf.isStreaming()

    def show(self, n=20, truncate=True, vertical=False):
        if isinstance(truncate, bool) and truncate:
            print(self._jdf.showString(n, 20, vertical))
        else:
            print(self._jdf.showString(n, int(truncate), vertical))

    def __repr__(self):
        return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def checkpoint(self, eager=True):
        jdf = self._jdf.checkpoint(eager)
        return DataFrame(jdf, self.sql_ctx)

    def localCheckpoint(self, eager=True):
        jdf = self._jdf.localCheckpoint(eager)
        return DataFrame(jdf, self.sql_ctx)

    def withWatermark(self, eventTime, delayThreshold):
        if not eventTime or type(eventTime) is not str:
            raise TypeError("eventTime should be provided as a string")
        if not delayThreshold or type(delayThreshold) is not str:
            raise TypeError("delayThreshold should be provided as a string interval")
        jdf = self._jdf.withWatermark(eventTime, delayThreshold)
        return DataFrame(jdf, self.sql_ctx)

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

        jdf = self._jdf.hint(name, self._jseq(parameters))
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
        >>> df.collect()
        [Row(id=0)]
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
        2000
        """
        if isinstance(numPartitions, int):
            if not cols:
                return DataFrame(self._jdf.repartition(numPartitions), self.sql_ctx)
            else:
                def partitioner(row: Row):
                    return row  # tuple(row[col] for col in cols)
                return DataFrame(self._jdf.rdd().partitionBy(numPartitions, partitioner), self.sql_ctx)
        elif isinstance(numPartitions, (basestring, Column)):
            # todo: rely on conf only
            newNumPartitions = self.sql_ctx.sparkSession.conf.get("numShufflePartitions", "200")
            return self.repartition(int(newNumPartitions), numPartitions, *cols)
        else:
            raise TypeError("numPartitions should be an int, str or Column")

    def repartitionByRange(self, numPartitions, *cols):
        return self.repartition(numPartitions, *cols)

    def distinct(self):
        return DataFrame(self._jdf.distinct(), self.sql_ctx)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        is_withReplacement_set = \
            type(withReplacement) == bool and isinstance(fraction, float)

        is_withReplacement_omitted_kwargs = \
            withReplacement is None and isinstance(fraction, float)

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
        ...

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
        ...

    def crossjoin(self, other):
        jdf = self._jdf.crossJoin(other)
        return DataFrame(jdf, self.sql_ctx)

    def join(self, other, on=None, how=None):
        ...

    def sortWithinPartitions(self, *cols, **kwargs):
        # todo: check impact of _sort_cols
        jdf = self._jdf.sortWithinPartitions(cols, kwargs)
        return DataFrame(jdf, self.sql_ctx)

    def sort(self, *cols, **kwargs):
        # todo: check impact of _sort_cols
        jdf = self._jdf.sort(cols, kwargs)
        return DataFrame(jdf, self.sql_ctx)

    def orderBy(self, *cols, **kwargs):
        return self.sort(*cols, **kwargs)

    def describe(self, *cols):
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        jdf = self._jdf.describe(cols)
        return DataFrame(jdf, self.sql_ctx)

    def summary(self, *statistics):
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
            jc = self._jdf.apply(item)
            return Column(jc)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            jc = self._jdf.apply(self.columns[item])
            return Column(jc)
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    # def __getattr__(self, name):
    # if name not in self.columns:
    #     raise AttributeError(
    #         "'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
    # jc = self._jdf.apply(name)
    # return Column(jc)

    def select(self, *cols):
        jdf = self._jdf.select(*cols)
        return DataFrame(jdf, self.sql_ctx)

    def selectExpr(self, *expr):
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]
        jdf = self._jdf.selectExpr(expr)
        return DataFrame(jdf, self.sql_ctx)

    def filter(self, condition):
        if isinstance(condition, basestring):
            jdf = self._jdf.filter(condition)
        elif isinstance(condition, Column):
            jdf = self._jdf.filter(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return DataFrame(jdf, self.sql_ctx)

    def groupBy(self, *cols):
        jgd = self._jdf.groupBy(self._jcols(*cols))
        from pysparkling.sql.group import GroupedData
        return GroupedData(jgd, self)

    def rollup(self, *cols):
        jgd = self._jdf.rollup(self._jcols(*cols))
        from pysparkling.sql.group import GroupedData
        return GroupedData(jgd, self)

    def cube(self, *cols):
        jgd = self._jdf.cube(self._jcols(*cols))
        from pysparkling.sql.group import GroupedData
        return GroupedData(jgd, self)

    def agg(self, *exprs):
        return self.groupBy().agg(*exprs)

    def union(self, other):
        return DataFrame(self._jdf.union(other._jdf), self.sql_ctx)

    def unionAll(self, other):
        return self.union(other)

    def unionByName(self, other):
        return DataFrame(self._jdf.unionByName(other._jdf), self.sql_ctx)

    def intersect(self, other):
        return DataFrame(self._jdf.intersect(other._jdf), self.sql_ctx)

    def intersectAll(self, other):
        return DataFrame(self._jdf.intersectAll(other._jdf), self.sql_ctx)

    def subtract(self, other):
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sql_ctx)

    def dropDuplicates(self, subset=None):
        if subset is None:
            jdf = self._jdf.dropDuplicates()
        else:
            jdf = self._jdf.dropDuplicates(self._jseq(subset))
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
                self._jdf.na().replace(self._jseq(subset), self._jmap(rep_dict)), self.sql_ctx)

    def approxQuantile(self, col, probabilities, relativeError):
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

        jaq = self._jdf.stat().approxQuantile(col, probabilities, relativeError)
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    def corr(self, col1, col2, method=None):
        if not isinstance(col1, basestring):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, basestring):
            raise ValueError("col2 should be a string.")
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise ValueError("Currently only the calculation of the Pearson Correlation " +
                             "coefficient is supported.")
        return self._jdf.stat().corr(col1, col2, method)

    def cov(self, col1, col2):
        if not isinstance(col1, basestring):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, basestring):
            raise ValueError("col2 should be a string.")
        return self._jdf.stat().cov(col1, col2)

    def crosstab(self, col1, col2):
        if not isinstance(col1, basestring):
            raise ValueError("col1 should be a string.")
        if not isinstance(col2, basestring):
            raise ValueError("col2 should be a string.")
        return DataFrame(self._jdf.stat().crosstab(col1, col2), self.sql_ctx)

    def freqItems(self, cols, support=None):
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise ValueError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame(self._jdf.stat().freqItems(cols, support), self.sql_ctx)

    def withColumn(self, colName, col):
        assert isinstance(col, Column), "col should be Column"
        return DataFrame(self._jdf.withColumn(colName, col._jc), self.sql_ctx)

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
            jdf = self._jdf.drop(self._jseq(cols))

        return DataFrame(jdf, self.sql_ctx)

    def toDF(self, *cols):
        jdf = self._jdf.toDF(self._jseq(cols))
        return DataFrame(jdf, self.sql_ctx)

    def transform(self, func):
        result = func(self)
        assert isinstance(result, DataFrame), "Func returned an instance of type [%s], " \
                                              "should have been DataFrame." % type(result)
        return result

    def toPandas(self):
        from pyspark.sql.utils import require_minimum_pandas_version
        require_minimum_pandas_version()

        import pandas as pd

        if self.sql_ctx._conf.pandasRespectSessionTimeZone():
            timezone = self.sql_ctx._conf.sessionLocalTimeZone()
        else:
            timezone = None

        if self.sql_ctx._conf.arrowEnabled():
            use_arrow = True
            try:
                from pyspark.sql.types import to_arrow_schema
                from pyspark.sql.utils import require_minimum_pyarrow_version

                require_minimum_pyarrow_version()
                to_arrow_schema(self.schema)
            except Exception as e:
                if self.sql_ctx._conf.arrowFallbackEnabled():
                    msg = (
                            "toPandas attempted Arrow optimization because "
                            "'spark.sql.execution.arrow.enabled' is set to true; however, "
                            "failed by the reason below:\n  %s\n"
                            "Attempting non-optimization as "
                            "'spark.sql.execution.arrow.fallback.enabled' is set to "
                            "true." % _exception_message(e))
                    warnings.warn(msg)
                    use_arrow = False
                else:
                    msg = (
                            "toPandas attempted Arrow optimization because "
                            "'spark.sql.execution.arrow.enabled' is set to true, but has reached "
                            "the error below and will not continue because automatic fallback "
                            "with 'spark.sql.execution.arrow.fallback.enabled' has been set to "
                            "false.\n  %s" % _exception_message(e))
                    warnings.warn(msg)
                    raise

            # Try to use Arrow optimization when the schema is supported and the required version
            # of PyArrow is found, if 'spark.sql.execution.arrow.enabled' is enabled.
            if use_arrow:
                try:
                    from pyspark.sql.types import _arrow_table_to_pandas, \
                        _check_dataframe_localize_timestamps
                    import pyarrow
                    batches = self._collectAsArrow()
                    if len(batches) > 0:
                        table = pyarrow.Table.from_batches(batches)
                        pdf = _arrow_table_to_pandas(table, self.schema)
                        return _check_dataframe_localize_timestamps(pdf, timezone)
                    else:
                        return pd.DataFrame.from_records([], columns=self.columns)
                except Exception as e:
                    # We might have to allow fallback here as well but multiple Spark jobs can
                    # be executed. So, simply fail in this case for now.
                    msg = (
                            "toPandas attempted Arrow optimization because "
                            "'spark.sql.execution.arrow.enabled' is set to true, but has reached "
                            "the error below and can not continue. Note that "
                            "'spark.sql.execution.arrow.fallback.enabled' does not have an effect "
                            "on failures in the middle of computation.\n  %s" % _exception_message(e))
                    warnings.warn(msg)
                    raise

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
