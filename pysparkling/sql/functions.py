import math

from pysparkling.sql.column import Column, parse
from pysparkling.sql.expressions.aggregate.collectors import CollectSet, ApproxCountDistinct, CollectList, \
    CountDistinct, First, Last, SumDistinct
from pysparkling.sql.expressions.aggregate.covariance_aggregations import Corr, CovarPop, CovarSamp
from pysparkling.sql.expressions.aggregate.stat_aggregations import Count, Avg, Kurtosis, Max, Min, Skewness, \
    StddevSamp, StddevPop, Sum, VarSamp, VarPop
from pysparkling.sql.expressions.arrays import ArrayColumn, MapFromArraysColumn, MapColumn
from pysparkling.sql.expressions.mappers import CaseWhen, Rand, CreateStruct, Grouping, GroupingID, Coalesce, \
    InputFileName, IsNaN, MonotonicallyIncreasingID, NaNvl, Randn, SparkPartitionID, Sqrt, Abs, Acos, Asin, Atan, Atan2, \
    Bin, Cbrt, Ceil, Conv, Cos, Cosh, Exp, ExpM1, Factorial, Floor, Greatest, Hex, Unhex, Hypot, Least, Log, Log10, \
    Log1p, Log2, Rint, Round, Bround, Signum, Sin
from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.operators import IsNull, BitwiseNot, Pow, Pmod


def col(colName):
    """
    :rtype: Column
    """
    return Column(colName)


def column(colName):
    """
    :rtype: Column
    """
    return col(colName)


def lit(literal):
    """
    :rtype: Column
    """
    return col(typedLit(literal))


def typedLit(literal):
    """
    :rtype: Column
    """
    return Literal(literal)


def asc(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(asc("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  1| null|
    |  3| null|
    |  0|    0|
    |  2|    2|
    |  4|    4|
    +---+-----+
    """
    return parse(columnName).asc()


def asc_nulls_first(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(asc_nulls_first("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  1| null|
    |  3| null|
    |  0|    0|
    |  2|    2|
    |  4|    4|
    +---+-----+
    """
    return parse(columnName).asc_nulls_first()


def asc_nulls_last(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(asc_nulls_last("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  0|    0|
    |  2|    2|
    |  4|    4|
    |  1| null|
    |  3| null|
    +---+-----+
    """
    return parse(columnName).asc_nulls_last()


def desc(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(desc("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  4|    4|
    |  2|    2|
    |  0|    0|
    |  1| null|
    |  3| null|
    +---+-----+
    """
    return parse(columnName).desc()


def desc_nulls_first(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(desc_nulls_first("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  1| null|
    |  3| null|
    |  4|    4|
    |  2|    2|
    |  0|    0|
    +---+-----+
    """
    return parse(columnName).desc_nulls_first()


def desc_nulls_last(columnName):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> from pysparkling.sql.functions import when, col
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5).withColumn(
    ...   "order", when(col('id')%2 == 0, col('id'))
    ... ).orderBy(desc_nulls_last("order")).show()
    +---+-----+
    | id|order|
    +---+-----+
    |  4|    4|
    |  2|    2|
    |  0|    0|
    |  1| null|
    |  3| null|
    +---+-----+
    """
    return parse(columnName).desc_nulls_last()


def approx_count_distinct(e, rsd=0.05):
    """
    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(100).select((col("id")%10).alias("n")).select(approx_count_distinct("n")).show()
    +------------------------+
    |approx_count_distinct(n)|
    +------------------------+
    |                      10|
    +------------------------+
    """
    # NB: This function returns the exact number of distinct values in pysparkling
    # as it does not rely on HyperLogLogPlusPlus or another estimator
    return col(ApproxCountDistinct(column=parse(e)))


def avg(e):
    """
    :rtype: Column
    """
    return col(Avg(column=parse(e)))


def collect_list(e):
    """
    :rtype: Column
    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(5)
    >>> df.repartition(2).select(collect_list("id").alias("all_ids")).show()
    +---------------+
    |        all_ids|
    +---------------+
    |[0, 1, 2, 3, 4]|
    +---------------+
    """
    return col(CollectList(column=parse(e)))


def corr(column1, column2):
    """
    :rtype: Column
    """
    return col(Corr(
        column1=parse(column1),
        column2=parse(column2)
    ))


def when(condition, value):
    """
    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.createDataFrame(
    # ...    [Row(age=2, name='Alice'), Row(age=5, name='Bob'), Row(age=4, name='Lisa')]
    # ... )
    # >>> df.select(df.name, when(df.age > 4, -1).when(df.age < 3, 1).otherwise(0)).show()
    # +-----+------------------------------------------------------------+
    # | name|CASE WHEN (age > 4) THEN -1 WHEN (age < 3) THEN 1 ELSE 0 END|
    # +-----+------------------------------------------------------------+
    # |Alice|                                                           1|
    # |  Bob|                                                          -1|
    # | Lisa|                                                           0|
    # +-----+------------------------------------------------------------+

    :rtype: Column
    """
    return col(CaseWhen([parse(condition)], [parse(value)]))


def rand(seed=None):
    """

    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.range(4, numPartitions=2)
    # >>> df.select((rand(seed=42) * 3).alias("rand")).show()
    # +------------------+
    # |              rand|
    # +------------------+
    # |2.3675439190260485|
    # |1.8992753422855404|
    # |1.5878851952491426|
    # |0.8800146499990725|
    # +------------------+
    """
    return col(Rand(seed))


def randn(seed=None):
    """

    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(4, numPartitions=2)
    >>> df.select((randn(seed=42) * 3).alias("randn")).show()
    +------------------+
    |             randn|
    +------------------+
    | 3.662337823324239|
    |1.6855413955465237|
    |0.7870748709777542|
    |-5.552412872005739|
    +------------------+
    """
    return col(Randn(seed))


def struct(*exprs):
    """
    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.createDataFrame([Row(age=2, name='Alice'), Row(age=5, name='Bob')])
    # >>> df.select(struct("age", col("name")).alias("struct")).collect()
    # [Row(struct=Row(age=2, name='Alice')), Row(struct=Row(age=5, name='Bob'))]
    # >>> df.select(struct("age", col("name"))).show()
    # +----------------------------------+
    # |named_struct(age, age, name, name)|
    # +----------------------------------+
    # |                        [2, Alice]|
    # |                          [5, Bob]|
    # +----------------------------------+

    """
    cols = [parse(e) for e in exprs]
    return col(CreateStruct(cols))


def array(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return col(ArrayColumn(columns))


def map_from_arrays(col1, col2):
    """Creates a new map from two arrays.

    :param col1: name of column containing a set of keys. All elements should not be null
    :param col2: name of column containing a set of values
    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> df = spark.createDataFrame([([2, 5], ['a', 'b'])], ['k', 'v'])
    # >>> df.select(map_from_arrays(df.k, df.v).alias("map")).show()
    # +----------------+
    # |             map|
    # +----------------+
    # |[2 -> a, 5 -> b]|
    # +----------------+
    """
    key_col = parse(col1)
    value_col = parse(col2)
    return col(MapFromArraysColumn(key_col, value_col))


def count(e):
    """
    :rtype: Column

    # >>> from pysparkling import Context, Row
    # >>> from pysparkling.sql.session import SparkSession
    # >>> spark = SparkSession(Context())
    # >>> spark.range(5).select(count("*")).show()
    # +--------+
    # |count(1)|
    # +--------+
    # |       5|
    +--------+
    """
    return col(Count(column=parse(e)))


def countDistinct(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return col(CountDistinct(columns=columns))


def collect_set(e):
    """
    :rtype: Column
    """
    return col(CollectSet(column=parse(e)))


def covar_pop(column1, column2):
    """
    :rtype: Column
    """
    return col(CovarPop(
        column1=parse(column1),
        column2=parse(column2)
    ))


def covar_samp(column1, column2):
    """
    :rtype: Column
    """
    return col(CovarSamp(
        column1=parse(column1),
        column2=parse(column2)
    ))


def first(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    return col(First(parse(e), ignoreNulls))


def last(e, ignoreNulls=False):
    """
    :rtype: Column
    """
    return col(Last(parse(e), ignoreNulls))


def grouping(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([(2, 'Alice'), (5, 'Bob'), (5, 'Carl')], ["age", "name"])
    >>> df.cube("name", df.age).agg(count("*"), grouping(df.age)).orderBy("name", "age").show()
    +-----+----+--------+-------------+
    | name| age|count(1)|grouping(age)|
    +-----+----+--------+-------------+
    | null|null|       3|            1|
    | null|   2|       1|            0|
    | null|   5|       2|            0|
    |Alice|null|       1|            1|
    |Alice|   2|       1|            0|
    |  Bob|null|       1|            1|
    |  Bob|   5|       1|            0|
    | Carl|null|       1|            1|
    | Carl|   5|       1|            0|
    +-----+----+--------+-------------+

    """
    return col(Grouping(parse(e)))


def grouping_id(*exprs):
    """

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame(
    ...     [(2, 'Alice', 3), (5, 'Bob', 4), (5, None, 6)],
    ...     ["age", "name", "id"]
    ... )
    >>> (df.cube("name", df.age)
    ...    .agg(count("*"), grouping_id())
    ...     .orderBy("name", "age", "count(1)")
    ... ).show()
    +-----+----+--------+-------------+
    | name| age|count(1)|grouping_id()|
    +-----+----+--------+-------------+
    | null|null|       1|            1|
    | null|null|       3|            3|
    | null|   2|       1|            2|
    | null|   5|       1|            0|
    | null|   5|       2|            2|
    |Alice|null|       1|            1|
    |Alice|   2|       1|            0|
    |  Bob|null|       1|            1|
    |  Bob|   5|       1|            0|
    +-----+----+--------+-------------+
    >>> (df
    ...   .rollup("name", df.age)
    ...   .agg(count("*"), grouping_id())
    ...   .orderBy("name", "age", "count(1)")
    ...  ).show()
    +-----+----+--------+-------------+
    | name| age|count(1)|grouping_id()|
    +-----+----+--------+-------------+
    | null|null|       1|            1|
    | null|null|       3|            3|
    | null|   5|       1|            0|
    |Alice|null|       1|            1|
    |Alice|   2|       1|            0|
    |  Bob|null|       1|            1|
    |  Bob|   5|       1|            0|
    +-----+----+--------+-------------+

    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(GroupingID(cols))


def kurtosis(e):
    """
    :rtype: Column
    """
    return col(Kurtosis(column=parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def max(e):
    """
    :rtype: Column
    """
    return col(Max(column=parse(e)))


def mean(e):
    """
    :rtype: Column
    """
    # Discrepancy between name and object (mean vs Avg) replicate a discrepancy in PySpark
    return col(Avg(column=parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def min(e):
    """
    :rtype: Column
    """
    return col(Min(column=parse(e)))


def skewness(e):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.range(100, numPartitions=20).select((col("id")**2).alias("n"))
    >>> df.groupBy().agg(skewness("n")).show()
    +------------------+
    |       skewness(n)|
    +------------------+
    |0.6440904335963368|
    +------------------+

    """
    return col(Skewness(column=parse(e)))


def stddev(e):
    """
    :rtype: Column
    """
    return col(StddevSamp(column=parse(e)))


def stddev_samp(e):
    """
    :rtype: Column
    """
    return col(StddevSamp(column=parse(e)))


def stddev_pop(e):
    """
    :rtype: Column
    """
    return col(StddevPop(column=parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def sum(e):
    """
    :rtype: Column
    """
    return col(Sum(column=parse(e)))


def sumDistinct(e):
    """
    :rtype: Column
    """
    return col(SumDistinct(column=parse(e)))


def variance(e):
    """
    :rtype: Column
    """
    return col(VarSamp(column=parse(e)))


def var_samp(e):
    """
    :rtype: Column
    """
    return col(VarSamp(column=parse(e)))


def var_pop(e):
    """
    :rtype: Column
    """
    return col(VarPop(column=parse(e)))


# //////////////////////////////////////////////////////////////////////////////////////////////
# // Window functions
# //////////////////////////////////////////////////////////////////////////////////////////////

def cume_dist():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def dense_rank():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def lag(e, offset, defaultValue=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def lead(e, offset, defaultValue=None):
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def ntile(n):
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def percent_rank():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def rank():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def row_number():
    """
    :rtype: Column
    """
    raise NotImplementedError("window functions are not yet supported by pysparkling")


def create_map(*exprs):
    """Creates a new map column.

    :param exprs: list of column names (string) or list of :class:`Column` expressions that are
        grouped as key-value pairs, e.g. (key1, value1, key2, value2, ...).

    :rtype: Column

    >>> from pysparkling import Context, Row
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> df = spark.createDataFrame([Row(age=2, name='Alice'), Row(age=5, name='Bob')])
    >>> df.select(create_map('name', 'age').alias("map")).collect()
    [Row(map={'Alice': 2}), Row(map={'Bob': 5})]
    >>> df.select(create_map([df.name, df.age]).alias("map")).collect()
    [Row(map={'Alice': 2}), Row(map={'Bob': 5})]
    """
    if len(exprs) == 1 and isinstance(exprs[0], (list, set)):
        exprs = exprs[0]
    cols = [parse(e) for e in exprs]

    return col(MapColumn(cols))


def broadcast(df):
    """
    :rtype: Column
    """
    # Broadcast is not implemented as Pysparkling is not distributed
    return df


def coalesce(*exprs):
    """
    :rtype: Column
    """
    columns = [parse(e) for e in exprs]
    return col(Coalesce(columns))


def input_file_name():
    return col(InputFileName())


def isnan(e):
    """
    :rtype: Column
    """
    return col(IsNaN(parse(e)))


def isnull(e):
    """
    :rtype: Column
    """
    return col(IsNull(parse(e)))


def monotonically_increasing_id():
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(10).repartition(1).select("id", monotonically_increasing_id()).show()
    +---+-----------------------------+
    | id|monotonically_increasing_id()|
    +---+-----------------------------+
    |  0|                            0|
    |  1|                            1|
    |  2|                            2|
    |  3|                            3|
    |  4|                            4|
    |  5|                            5|
    |  6|                            6|
    |  7|                            7|
    |  8|                            8|
    |  9|                            9|
    +---+-----------------------------+
    >>> spark.range(3).repartition(5).select("id", monotonically_increasing_id()).show()
    +---+-----------------------------+
    | id|monotonically_increasing_id()|
    +---+-----------------------------+
    |  0|                   8589934592|
    |  1|                  25769803776|
    |  2|                  34359738368|
    +---+-----------------------------+

    """
    return col(MonotonicallyIncreasingID())


def nanvl(col1, col2):
    """
    :rtype: Column
    """
    return col(NaNvl(parse(col1), parse(col2)))


def spark_partition_id():
    """
    :rtype: Column
    """
    return col(SparkPartitionID())


def sqrt(e):
    """
    :rtype: Column
    """
    return col(Sqrt(parse(e)))


def bitwiseNOT(e):
    """
    :rtype: Column
    """
    return col(BitwiseNot(parse(e)))


def expr(expression):
    """
    :rtype: Column
    """
    return parse(expression)


# noinspection PyShadowingBuiltins
def abs(e):
    """
    :rtype: Column
    """
    return col(Abs(parse(e)))


def acos(e):
    """
    :rtype: Column
    """
    return col(Acos(parse(e)))


def asin(e):
    """
    :rtype: Column
    """
    return col(Asin(parse(e)))


def atan(e):
    """
    :rtype: Column
    """
    return col(Atan(parse(e)))


def atan2(y, x):
    """
    :rtype: Column
    """
    return col(Atan2(parse(y), parse(x)))


# noinspection PyShadowingBuiltins
def bin(e):
    """
    :rtype: Column
    """
    return col(Bin(parse(e)))


def cbrt(e):
    """
    :rtype: Column
    """
    return col(Cbrt(parse(e)))


def ceil(e):
    """
    :rtype: Column
    """
    return col(Ceil(parse(e)))


def conv(num, fromBase, toBase):
    """
    :rtype: Column
    """
    return col(Conv(parse(num), fromBase, toBase))


def cos(e):
    """
    :rtype: Column
    """
    return col(Cos(parse(e)))


def cosh(e):
    """
    :rtype: Column
    """
    return col(Cosh(parse(e)))


def exp(e):
    """
    :rtype: Column
    """
    return col(Exp(parse(e)))


def expm1(e):
    """
    :rtype: Column
    """
    return col(ExpM1(parse(e)))


def factorial(e):
    """
    :rtype: Column
    """
    return col(Factorial(parse(e)))


def floor(e):
    """
    :rtype: Column
    """
    return col(Floor(parse(e)))


def greatest(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Greatest(cols))


# noinspection PyShadowingBuiltins
# pylint: disable=W0621
def hex(column):
    """
    :rtype: Column
    """
    return col(Hex(parse(column)))


# noinspection PyShadowingNames
# pylint: disable=W0621
def unhex(column):
    """
    :rtype: Column
    """
    return col(Unhex(parse(column)))


def hypot(l, r):
    """
    :rtype: Column
    """
    return col(Hypot(parse(l), parse(r)))


def least(*exprs):
    """
    :rtype: Column
    """
    cols = [parse(e) for e in exprs]
    return col(Least(cols))


def log(arg1, arg2=None):
    """
    :rtype: Column
    """
    if arg2 is None:
        base, value = math.e, parse(arg1)
    else:
        base, value = arg1, parse(arg2)
    return col(Log(base, value))


def log10(e):
    """
    :rtype: Column
    """
    return col(Log10(parse(e)))


def log1p(e):
    """
    :rtype: Column
    """
    return col(Log1p(parse(e)))


def log2(e):
    """
    :rtype: Column
    """
    return col(Log2(parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def pow(l, r):
    """
    :rtype: Column
    """
    return col(Pow(parse(l), parse(r)))


def pmod(dividend, divisor):
    """
    :rtype: Column
    """
    return col(Pmod(dividend, divisor))


def rint(e):
    """
    :rtype: Column
    """
    return col(Rint(parse(e)))


# noinspection PyShadowingBuiltins
# pylint: disable=W0622
def round(e, scale=0):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(
    ...     round(lit(9.1)),
    ...     round(lit(9.9)),
    ...     round(lit(9.5)),
    ...     round(lit(8.5)),
    ...     round(lit(15), -1),
    ...     round(lit(25), -1)
    ... ).show()
    +-------------+-------------+-------------+-------------+-------------+-------------+
    |round(9.1, 0)|round(9.9, 0)|round(9.5, 0)|round(8.5, 0)|round(15, -1)|round(25, -1)|
    +-------------+-------------+-------------+-------------+-------------+-------------+
    |          9.0|         10.0|         10.0|          9.0|           20|           30|
    +-------------+-------------+-------------+-------------+-------------+-------------+


    """
    return col(Round(parse(e), scale))


def bround(e, scale=0):
    """
    :rtype: Column

    >>> from pysparkling import Context
    >>> from pysparkling.sql.session import SparkSession
    >>> spark = SparkSession(Context())
    >>> spark.range(1).select(
    ...     bround(lit(9.1)),
    ...     bround(lit(9.9)),
    ...     bround(lit(9.5)),
    ...     bround(lit(8.5)),
    ...     bround(lit(15), -1),
    ...     bround(lit(25), -1)
    ... ).show()
    +--------------+--------------+--------------+--------------+--------------+--------------+
    |bround(9.1, 0)|bround(9.9, 0)|bround(9.5, 0)|bround(8.5, 0)|bround(15, -1)|bround(25, -1)|
    +--------------+--------------+--------------+--------------+--------------+--------------+
    |           9.0|          10.0|          10.0|           8.0|            20|            20|
    +--------------+--------------+--------------+--------------+--------------+--------------+
    """
    return col(Bround(parse(e), scale))


def shiftLeft(e, numBits):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def shiftRight(e, numBits):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def shiftRightUnsigned(e, numBits):
    """
    :rtype: Column
    """
    raise NotImplementedError("Pysparkling does not support yet this function")


def signum(e):
    """
    :rtype: Column
    """
    return col(Signum(parse(e)))


def sin(e):
    """
    :rtype: Column
    """
    return col(Sin(parse(e)))
