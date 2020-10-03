import warnings

from pysparkling import StorageLevel
from pysparkling.sql.column import parse

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
